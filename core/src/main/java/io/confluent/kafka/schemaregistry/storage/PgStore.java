package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.Value;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.WILDCARD;

public class PgStore {
  private static final Logger log = LoggerFactory.getLogger(PgStore.class);
  private Connection conn;
  private RedisClient redisClient;
  private RedisAsyncCommands<String, String> redisCommands;
  private ObjectMapper objectMapper;

  public void init() throws SchemaRegistryException {
    String url = "jdbc:postgresql://localhost:5555/ewu-test";
    Properties props = new Properties();
    props.setProperty("user", "postgres");
    props.setProperty("password", "postgres");
    try {
      this.conn = DriverManager.getConnection(url, props);
      this.conn.setAutoCommit(false);
      this.conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    } catch (SQLException e) {
      throw new SchemaRegistryException(e);
    }
    this.redisClient = RedisClient.create(
        RedisURI.create("redis://localhost:6379"));
    this.redisCommands = redisClient.connect().async();
    this.objectMapper = new ObjectMapper();
  }

  public void commit() throws SQLException {
    conn.commit();
  }

  public void rollback() {
    try {
      conn.rollback();
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void shutdown() {
    // TODO
  }

  public Map<String, Integer[]> getSubjectByHash(QualifiedSubject qs,
                                                 Schema schema, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    Map<String, Integer[]> result = new HashMap<>(); // subject : [schema_id, version]
    String pattern = qs.toQualifiedContext() + ":hash:"
        + Base64.getEncoder().encodeToString(MD5.ofSchema(schema).bytes()) + ":"
        + qs.getSubject() + ":*";
    try {
      List<String> keys = redisCommands.keys(pattern).thenApply(ks ->
          ks.stream().filter(k -> lookupDeletedSchema || !k.endsWith(":deleted:")).collect(Collectors.toList())
      ).toCompletableFuture().get();
      if (keys != null && !keys.isEmpty()) {
        String[] idAndVersion = redisCommands.get(keys.get(0)).get().split(":");
        int id = Integer.parseInt(idAndVersion[0]);
        int version = Integer.parseInt(idAndVersion[1]);
        keys.stream().map(k -> {
          if (k.endsWith(":deleted:")) return k.substring(0, k.length() - ":deleted:".length());
          return k.substring(0, k.length() - 1);
        }).map(k -> k.substring(k.lastIndexOf(':') + 1))
            .forEach(k -> result.put(k, new Integer[]{id, version}));
        log.info("Cache hit");
        return result;
      }
    } catch (Exception e) {
      log.error("Redis error");
    }

    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT sub.subject, s.id, s.version FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND hash = ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted");
      }
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setBytes(3, MD5.ofSchema(schema).bytes());
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          String subject = rs.getString(1);
          int id = rs.getInt(2);
          int version = rs.getInt(3);
          result.put(subject, new Integer[]{id, version});
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetSubjectByHash error", e);
    } finally {
      closeResultSet(rs);
    }

    return result;
  }

  public Schema lookupSchemaBySubject(QualifiedSubject qs, Schema schema, String subject,
                                      boolean lookupDeletedSchema) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT s.id, s.version FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? AND hash = ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted");
      }
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      ps.setBytes(4, MD5.ofSchema(schema).bytes());
      rs = ps.executeQuery();
      if (rs != null) {
        if (rs.next()) {
          Schema matchingSchema = schema.copy();
          matchingSchema.setSubject(subject);
          matchingSchema.setId(rs.getInt(1));
          matchingSchema.setVersion(rs.getInt(2));
          return matchingSchema;
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("LookupSchemaBySubject error", e);
    } finally {
      closeResultSet(rs);
    }

    return null;
  }

  public Schema getSubjectVersion(QualifiedSubject qs, int version,
                                  boolean lookupDeletedSchema) throws SchemaRegistryException {
    String key = qs.toQualifiedSubject() + ":version:" + version + ":";
    Optional<Schema> maybeSchema = fetchFromCache(key, lookupDeletedSchema);
    if (maybeSchema.isPresent()) {
      log.info("Cache hit");
      return maybeSchema.get();
    }

    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT c.tenant, c.context, s.id, sub.subject, s.version, s.type, s.str, sub.id, s.deleted FROM contexts c ")
          .append("JOIN subjects sub ON c.id = sub.context_id ")
          .append("JOIN schemas s ON s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? AND version = ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted");
      }
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      ps.setInt(4, version);
      rs = ps.executeQuery();
      if (rs != null) {
        if (rs.next()) {
          return populateSchema(rs);
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetSubjectVersion error", e);
    } finally {
      closeResultSet(rs);
    }

    return null;
  }

  public Schema getSchemaById(QualifiedSubject qs, int id) throws SchemaRegistryException {
    String pattern = qs.toQualifiedSubject() + "*" + ":id:" + id + ":";
    try {
      String value = redisCommands.keys(pattern).thenApply(ks -> {
        if (ks.isEmpty()) return "";
        Collections.sort(ks);
        return ks.get(0);
      }).thenCompose(k -> redisCommands.get(k)).toCompletableFuture().get();
      if (value != null && !value.isEmpty()) {
        log.info("Cache hit");
        return objectMapper.readValue(value, Schema.class);
      }
    } catch (Exception e) {
      log.error("Redis error");
    }

    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT c.tenant, c.context, s.id, sub.subject, s.version, s.type, s.str, sub.id, s.deleted FROM contexts c ")
          .append("JOIN subjects sub ON c.id = sub.context_id ")
          .append("JOIN schemas s ON s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context like ? AND s.id = ? ");
      if (!qs.getSubject().isEmpty()) {
        sql.append("AND sub.subject = ? ");
      }
      sql.append("ORDER BY c.context LIMIT 1 ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext() + "%");
      ps.setInt(3, id);
      if (!qs.getSubject().isEmpty()) {
        ps.setString(4, qs.getSubject());
      }
      rs = ps.executeQuery();
      if (rs != null) {
        if (rs.next()) {
          return populateSchema(rs);
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetSchemaById error", e);
    } finally {
      closeResultSet(rs);
    }

    return null;
  }

  public boolean subjectExists(QualifiedSubject qs, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    String pattern = qs.toQualifiedSubject() + ":version:*:";
    try {
      List<String> keys = redisCommands.keys(pattern).get();
      if (keys.stream().filter(Objects::nonNull)
          .anyMatch(k -> lookupDeletedSchema || !k.endsWith(":deleted:"))) {
        return true;
      }
    } catch (ExecutionException | InterruptedException e) {
      log.error("Redis error");
    }

    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT s.subject_id FROM contexts c ")
          .append("JOIN subjects sub ON c.id = sub.context_id ")
          .append("JOIN schemas s ON s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted ");
      }
      sql.append("GROUP BY s.subject_id HAVING COUNT(*) > 0 ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      rs = ps.executeQuery();
      if (rs != null) {
        if (rs.next()) {
          return true;
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("SubjectExists error", e);
    } finally {
      closeResultSet(rs);
    }

    return false;
  }

  public void softDeleteSchema(QualifiedSubject qs, Schema schema) throws SchemaRegistryException {
    PreparedStatement ps;

    try {
      String subjectVersionKey = qs.toQualifiedSubject() + ":version:" + schema.getVersion() + ":";
      String hashKey = qs.toQualifiedContext() + ":hash:"
          + Base64.getEncoder().encodeToString(MD5.ofSchema(schema).bytes()) + ":"
          + qs.getSubject() + ":";
      String value = objectMapper.writeValueAsString(schema);
      log.info("Redis set {}", subjectVersionKey);
      redisCommands.multi();
      redisCommands.del(subjectVersionKey, hashKey);
      redisCommands.set(subjectVersionKey + "deleted:", value, SetArgs.Builder.ex(Duration.ofMinutes(5)));
      redisCommands.set(hashKey + "deleted:", schema.getId() + ":" + schema.getVersion(), SetArgs.Builder.ex(Duration.ofMinutes(5)));
      RedisFuture<TransactionResult> exec = redisCommands.exec();

      log.info(exec.get().toString());

      StringBuilder sql = new StringBuilder();
      sql.append("UPDATE schemas SET deleted = true WHERE (subject_id, version) IN ")
          .append("(SELECT sub.id, ? FROM subjects sub ")
          .append("JOIN contexts c ON sub.context_id = c.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ?) ");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, schema.getVersion());
      ps.setString(2, qs.getTenant());
      ps.setString(3, qs.getContext());
      ps.setString(4, qs.getSubject());
      ps.executeUpdate();
    } catch (Exception e) {
      throw new SchemaRegistryException("SoftDeleteSchema error", e);
    }
  }

  public void hardDeleteSchema(QualifiedSubject qs, Schema schema) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;
    Integer subjectId = null, schemaId = null;

    try {
      String subjectVersionKey = qs.toQualifiedSubject() + ":version:" + schema.getVersion() + ":deleted:";
      String hashKey = qs.toQualifiedContext() + ":hash:"
          + Base64.getEncoder().encodeToString(MD5.ofSchema(schema).bytes()) + ":deleted:"
          + qs.getSubject() + ":";
      redisCommands.multi();
      redisCommands.del(subjectVersionKey, hashKey);
      RedisFuture<TransactionResult> exec = redisCommands.exec();

      log.info(exec.get().toString());

      StringBuilder sql = new StringBuilder();

      sql.append("SELECT sub.id, s.id FROM contexts c ")
          .append("JOIN subjects sub ON c.id = sub.context_id ")
          .append("JOIN schemas s ON s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? AND version = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      ps.setInt(4, schema.getVersion());
      rs = ps.executeQuery();
      if (rs != null) {
        if (rs.next()) {
          subjectId = rs.getInt(1);
          schemaId = rs.getInt(2);
        }
      }

      if (subjectId == null)
        return;

      sql.setLength(0);
      sql.append("DELETE FROM refs WHERE subject_id = ? AND schema_id = ?");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, subjectId);
      ps.setInt(2, schemaId);
      ps.executeUpdate();

      sql.setLength(0);
      sql.append("DELETE FROM schemas WHERE subject_id = ? AND id = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, subjectId);
      ps.setInt(2, schemaId);
      ps.executeUpdate();
    } catch (Exception e) {
      throw new SchemaRegistryException("HardDeleteSchema error", e);
    } finally {
      closeResultSet(rs);
    }
  }

  public void softDeleteSubject(QualifiedSubject qs) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;
    Integer subjectId = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT sub.id FROM subjects sub ")
          .append("JOIN contexts c ON sub.context_id = c.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      rs = ps.executeQuery();
      if (rs != null) {
        if (rs.next()) {
          subjectId = rs.getInt(1);
        }
      }

      if (subjectId == null)
        return;

      List<Schema> allVersions = getAllVersions(qs, false, false);
      for (Schema schema : allVersions) {
        String subjectVersionKey = qs.toQualifiedSubject() + ":version:" + schema.getVersion() + ":";
        String hashKey = qs.toQualifiedContext() + ":hash:"
            + Base64.getEncoder().encodeToString(MD5.ofSchema(schema).bytes()) + ":"
            + qs.getSubject() + ":";
        String value = objectMapper.writeValueAsString(schema);
        log.info("Redis set {}", subjectVersionKey);
        redisCommands.multi();
        redisCommands.del(subjectVersionKey, hashKey);
        redisCommands.set(subjectVersionKey + "deleted:", value, SetArgs.Builder.ex(Duration.ofMinutes(5)));
        redisCommands.set(hashKey + "deleted:", schema.getId() + ":" + schema.getVersion(), SetArgs.Builder.ex(Duration.ofMinutes(5)));
        RedisFuture<TransactionResult> exec = redisCommands.exec();

        log.info(exec.get().toString());
      }

      sql.setLength(0);
      sql.append("UPDATE schemas SET deleted = true WHERE subject_id = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, subjectId);
      ps.executeUpdate();
    } catch (Exception e) {
      throw new SchemaRegistryException("SoftDeleteSubject error", e);
    } finally {
      closeResultSet(rs);
    }
  }

  public void hardDeleteSubject(QualifiedSubject qs) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;
    Integer subjectId = null;

    try {
      StringBuilder sql = new StringBuilder();

      sql.append("SELECT sub.id FROM contexts c ")
          .append("JOIN subjects sub ON c.id = sub.context_id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      rs = ps.executeQuery();
      if (rs != null) {
        if (rs.next()) {
          subjectId = rs.getInt(1);
        }
      }

      if (subjectId == null)
        return;

      List<Schema> allVersions = getAllVersions(qs, true, false);
      for (Schema schema : allVersions) {
        String subjectVersionKey = qs.toQualifiedSubject() + ":version:" + schema.getVersion() + ":";
        String hashKey = qs.toQualifiedContext() + ":hash:"
            + Base64.getEncoder().encodeToString(MD5.ofSchema(schema).bytes()) + ":"
            + qs.getSubject() + ":";
        String idKey = qs.toQualifiedSubject() + ":id:" + schema.getId() + ":";
        redisCommands.multi();
        redisCommands.del(subjectVersionKey + "deleted:", hashKey + "deleted:", idKey);
        RedisFuture<TransactionResult> exec = redisCommands.exec();

        log.info(exec.get().toString());
      }

      sql.setLength(0);
      sql.append("DELETE FROM refs WHERE subject_id = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, subjectId);
      ps.executeUpdate();

      sql.setLength(0);
      sql.append("DELETE FROM schemas WHERE subject_id = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, subjectId);
      ps.executeUpdate();

      sql.setLength(0);
      sql.append("DELETE FROM subjects WHERE id = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, subjectId);
      ps.executeUpdate();
    } catch (Exception e) {
      throw new SchemaRegistryException("HardDeleteSubject error", e);
    } finally {
      closeResultSet(rs);
    }
  }

  public List<Schema> getAllVersionsInAllContexts(String tenant, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    List<Schema> list = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT c.tenant, c.context, s.id, sub.subject, s.version, s.type, s.str, sub.id, s.deleted FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted ");
      }
      sql.append("ORDER BY sub.subject, s.version DESC ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, tenant);
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          list.add(populateSchema(rs));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetAllVersionsInAllContexts error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public List<Schema> getLatestVersionsInAllContexts(String tenant, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    List<Schema> list = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT * FROM (")
          .append("SELECT c.tenant, c.context, s.id, sub.subject, s.version, s.type, s.str, sub.id, s.deleted, ")
          .append("ROW_NUMBER() OVER (PARTITION BY s.subject_id ORDER BY sub.subject, s.version DESC) AS r ")
          .append("FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted ");
      }
      sql.append(") t ")
          .append("WHERE t.r <= 1");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, tenant);
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          list.add(populateSchema(rs));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetLatestVersionsInAllContexts error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public List<Schema> getAllVersionsBySubjectPrefix(QualifiedSubject qs, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    List<Schema> list = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT c.tenant, c.context, s.id, sub.subject, s.version, s.type, s.str, sub.id, s.deleted FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject like ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted ");
      }
      sql.append("ORDER BY sub.subject, s.version ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject() + "%");
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          list.add(populateSchema(rs));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetAllVersionsBySubjectPrefix error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public List<Schema> getLatestVersionsBySubjectPrefix(QualifiedSubject qs, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    List<Schema> list = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT * FROM (")
          .append("SELECT c.tenant, c.context, s.id, sub.subject, s.version, s.type, s.str, sub.id, s.deleted, ")
          .append("ROW_NUMBER() OVER (PARTITION BY s.subject_id ORDER BY sub.subject, s.version DESC) AS r ")
          .append("FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject like ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted ");
      }
      sql.append(") t ")
          .append("WHERE t.r <= 1");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject() + "%");
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          list.add(populateSchema(rs));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetLatestVersionsBySubjectPrefix error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public List<String> getAllContexts(String tenant) throws SchemaRegistryException {
    List<String> list = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT context FROM contexts WHERE tenant = ? ORDER BY context ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, tenant);
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          list.add(rs.getString(1));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetAllContexts error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public List<Schema> getAllVersions(QualifiedSubject qs, boolean lookupDeletedSchema,
                                     boolean desc) throws SchemaRegistryException {
    List<Schema> list = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT c.tenant, c.context, s.id, sub.subject, s.version, s.type, s.str, sub.id, s.deleted FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted ");
      }
      if (desc) {
        sql.append("ORDER BY version DESC");
      } else {
        sql.append("ORDER BY version ASC");
      }
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          list.add(populateSchema(rs));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetAllVersionsDesc error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public List<SubjectVersion> getSubjectVersionsForId(QualifiedSubject qs, int id, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    List<SubjectVersion> list = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT sub.subject, s.version FROM schemas s ")
          .append("JOIN subjects sub ON s.subject_id = sub.id ")
          .append("JOIN contexts c ON sub.context_id = c.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND s.id = ? ");
      if (!qs.getSubject().isEmpty()) {
        sql.append("AND sub.subject = ? ");
      }
      if (!lookupDeletedSchema) {
        sql.append("AND NOT s.deleted ");
      }
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setInt(3, id);
      if (!qs.getSubject().isEmpty()) {
        ps.setString(4, qs.getSubject());
      }
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          String subject = new QualifiedSubject(qs.getTenant(), qs.getContext(), rs.getString(1)).toQualifiedSubject();
          int version = rs.getInt(2);
          list.add(new SubjectVersion(subject, version));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetSubjectVersionsForId error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public List<Integer> getReferencedBy(QualifiedSubject qs, Optional<Integer> version)
      throws SchemaRegistryException {
    List<Integer> list = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT s.id FROM schemas s JOIN ")
          .append("(SELECT r.schema_id, r.subject_id FROM refs r ")
          .append("JOIN schemas s ON r.ref_schema_id = s.id AND r.ref_subject_id = s.subject_id ")
          .append("JOIN subjects sub on sub.id = s.subject_id ")
          .append("JOIN contexts c on c.id = sub.context_id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? ");
      if (version.isPresent()) {
        sql.append("AND s.version = ? ");
      }
      sql.append(") t ")
          .append("ON s.id = t.schema_id AND s.subject_id = t.subject_id ")
          .append("WHERE NOT s.deleted ")
          .append("ORDER BY s.id ");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      if (version.isPresent()) {
        ps.setInt(4, version.get());
      }
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          list.add(rs.getInt(1));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetReferencedBy error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> getReferences(
      QualifiedSubject qs, int subjectId, int schemaId) throws SchemaRegistryException {
    List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> list
        = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT r.name, sub.subject, s.version FROM refs r ")
          .append("JOIN schemas s ON r.ref_schema_id = s.id AND r.ref_subject_id = s.subject_id ")
          .append("JOIN subjects sub on r.ref_subject_id = sub.id ")
          .append("WHERE r.subject_id = ? AND r.schema_id = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, subjectId);
      ps.setInt(2, schemaId);
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          list.add(populateSchemaReference(qs, rs));
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetReferences error", e);
    } finally {
      closeResultSet(rs);
    }

    return list;
  }

  public Schema getLatestSubjectVersion(QualifiedSubject qs)
      throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT c.tenant, c.context, s.id, sub.subject, s.version, s.type, s.str, sub.id, s.deleted FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND sub.subject = ? AND NOT s.deleted ");
      if (!qs.getContext().equals(WILDCARD)) {
        sql.append("AND c.context = ? ");
      }
      sql.append("ORDER BY s.version DESC LIMIT 1");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getSubject());
      if (!qs.getContext().equals(WILDCARD)) {
        ps.setString(3, qs.getContext());
      }
      rs = ps.executeQuery();
      if (rs != null) {
        if (rs.next()) {
          return populateSchema(rs);
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetLatestSubjectVersion error", e);
    } finally {
      closeResultSet(rs);
    }

    return null;
  }

  public int getOrCreateContext(QualifiedSubject qs) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;
    int contextId = -1;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT id FROM contexts WHERE tenant = ? AND context = ?");
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          contextId = rs.getInt(1);
        }
      }

      if (contextId < 0) {
        sql.setLength(0);
        sql.append("INSERT INTO contexts VALUES (DEFAULT, ?, ?, ?) ")
            .append("RETURNING id");
        ps = conn.prepareStatement(sql.toString());
        ps.setString(1, qs.getTenant());
        ps.setString(2, qs.getContext());
        ps.setInt(3, 0);
        rs = ps.executeQuery();
        if (rs != null) {
          while (rs.next()) {
            contextId = rs.getInt(1);
          }
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetOrCreateContext error", e);
    } finally {
      closeResultSet(rs);
    }

    return contextId;
  }

  public int getOrCreateSubject(int contextId, QualifiedSubject qs) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;
    int subjectId = -1;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT id FROM subjects WHERE context_id = ? ")
          .append("AND subject = ?");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, contextId);
      ps.setString(2, qs.getSubject());
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          subjectId = rs.getInt(1);
        }
      }

      if (subjectId < 0) {
        sql.setLength(0);
        sql.append("INSERT INTO subjects VALUES (DEFAULT, ?, ?) ")
            .append("RETURNING id");
        ps = conn.prepareStatement(sql.toString());
        ps.setInt(1, contextId);
        ps.setString(2, qs.getSubject());
        rs = ps.executeQuery();
        if (rs != null) {
          while (rs.next()) {
            subjectId = rs.getInt(1);
          }
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetOrCreateSubject error", e);
    } finally {
      closeResultSet(rs);
    }

    return subjectId;
  }

  public int getMaxVersion(int subjectId) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;
    int version = 1;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT MAX(version) FROM schemas WHERE subject_id = ?");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, subjectId);
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          version = rs.getInt(1);
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("GetMaxVersion error", e);
    } finally {
      closeResultSet(rs);
    }

    return version;
  }

  public int createSchema(int contextId, int subjectId, int version, int schemaId,
                          ParsedSchema parsedSchema, byte[] hash) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;
    try {
      StringBuilder sql = new StringBuilder();
      if (schemaId < 0) {
        sql.append("UPDATE contexts SET schemas = schemas + 1 ")
            .append("WHERE id = ? ")
            .append("RETURNING schemas");
        ps = conn.prepareStatement(sql.toString());
        ps.setInt(1, contextId);
        rs = ps.executeQuery();
        if (rs != null) {
          while (rs.next()) {
            schemaId = rs.getInt(1);
          }
        }
      }

      sql.setLength(0);
      sql.append("INSERT INTO schemas VALUES (?, ?, ?, ?, ?, ?, ?) ")
          .append("RETURNING id");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, schemaId);
      ps.setInt(2, subjectId);
      ps.setInt(3, version);
      ps.setString(4, parsedSchema.schemaType());
      ps.setString(5, parsedSchema.canonicalString());
      ps.setBytes(6, hash);
      ps.setBoolean(7, false);
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          schemaId = rs.getInt(1);
        }
      }

      for (io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference r :
          parsedSchema.references()) {
        sql.setLength(0);
        sql.append("INSERT INTO refs (subject_id, schema_id, name, ref_subject_id, ref_schema_id) ")
            .append("SELECT ?, ?, ?, sub.id, s.id ")
            .append("FROM subjects sub JOIN schemas s on sub.id = s.subject_id ")
            .append("WHERE sub.context_id = ? AND sub.subject = ? AND s.version = ? ");
        ps = conn.prepareStatement(sql.toString());
        ps.setInt(1, subjectId);
        ps.setInt(2, schemaId);
        ps.setString(3, r.getName());
        ps.setInt(4, contextId);
        ps.setString(5, QualifiedSubject.create("", r.getSubject()).getSubject());
        ps.setInt(6, r.getVersion());
        ps.executeUpdate();
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("CreateSchema error", e);
    } finally {
      closeResultSet(rs);
    }

    return schemaId;
  }

  public void registerDeleted(QualifiedSubject qs, Schema schema, int newVersion, int subjectId)
      throws SchemaRegistryException {
    int oldVersion = schema.getVersion();
    String oldHashKey = qs.toQualifiedContext() + ":hash:"
        + Base64.getEncoder().encodeToString(MD5.ofSchema(schema).bytes()) + ":"
        + qs.getSubject() + ":";
    schema.setVersion(newVersion);
    String oldSubjectVersionKey = qs.toQualifiedSubject() + ":version:" + oldVersion + ":";
    String hashKey = qs.toQualifiedContext() + ":hash:"
        + Base64.getEncoder().encodeToString(MD5.ofSchema(schema).bytes()) + ":"
        + qs.getSubject() + ":";
    String subjectVersionKey = qs.toQualifiedSubject() + ":version:" + newVersion + ":";

    PreparedStatement ps;

    try {
      String value = objectMapper.writeValueAsString(schema);
      log.info("Redis reset {}", subjectVersionKey);
      redisCommands.multi();
      redisCommands.del(oldSubjectVersionKey + "deleted:", oldHashKey + "deleted:");
      redisCommands.set(subjectVersionKey, value, SetArgs.Builder.ex(Duration.ofMinutes(5)));
      redisCommands.set(hashKey, schema.getId() + ":" + newVersion, SetArgs.Builder.ex(Duration.ofMinutes(5)));
      RedisFuture<TransactionResult> exec = redisCommands.exec();

      log.info(exec.get().toString());

      StringBuilder sql = new StringBuilder();
      sql.append("UPDATE schemas SET deleted = false, version = ? ")
          .append("WHERE id = ? AND subject_id = ? ");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, newVersion);
      ps.setInt(2, schema.getId());
      ps.setInt(3, subjectId);
      ps.executeUpdate();
    } catch (Exception e) {
      throw new SchemaRegistryException("RegisterDeleted error", e);
    }
  }

  private Schema populateSchema(ResultSet rs) throws Exception {
    String tenant = rs.getString(1);
    String context = rs.getString(2);
    int id = rs.getInt(3);
    String subject = rs.getString(4);
    int version = rs.getInt(5);
    String type = rs.getString(6);
    String str = rs.getString(7);
    int subjectId = rs.getInt(8);
    boolean deleted = rs.getBoolean(9);
    QualifiedSubject qs = new QualifiedSubject(tenant, context, subject);
    // TODO not handling N + 1 problem
    Schema schema = new Schema(qs.toQualifiedSubject(), version, id, type, getReferences(qs, subjectId, id), str);

    String subjectVersionKey = qs.toQualifiedSubject() + ":version:" + version + ":";
    String hashKey = qs.toQualifiedContext() + ":hash:"
        + Base64.getEncoder().encodeToString(MD5.ofSchema(schema).bytes()) + ":"
        + qs.getSubject() + ":";
    String value = objectMapper.writeValueAsString(schema);
    log.info("Redis set {}", subjectVersionKey);

    if (deleted) {
      redisCommands.set(subjectVersionKey + "deleted:", value, SetArgs.Builder.ex(Duration.ofMinutes(5)));
      redisCommands.set(hashKey + "deleted:", id + ":" + version, SetArgs.Builder.ex(Duration.ofMinutes(5)));
    } else {
      redisCommands.set(subjectVersionKey, value, SetArgs.Builder.ex(Duration.ofMinutes(5)));
      redisCommands.set(hashKey, id + ":" + version, SetArgs.Builder.ex(Duration.ofMinutes(5)));
    }
    String idKey = qs.toQualifiedSubject() + ":id:" + id + ":";
    redisCommands.set(idKey, value, SetArgs.Builder.ex(Duration.ofMinutes(5)));
    return schema;
  }

  private io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
    populateSchemaReference(QualifiedSubject qs, ResultSet rs) throws SQLException {
    String name = rs.getString(1);
    String subject = rs.getString(2);
    int version = rs.getInt(3);
    QualifiedSubject refQs = new QualifiedSubject(qs.getTenant(), qs.getContext(), subject);
    return new io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference(
        name, refQs.toQualifiedSubject(), version);
  }

  private Optional<Schema> fetchFromCache(String key, boolean lookupDeletedSchema) {
    Optional<Schema> maybeSchema;
    RedisFuture<List<KeyValue<String, String>>> mget;
    if (lookupDeletedSchema) {
      mget = redisCommands.mget(key, key + "deleted:");
    } else {
      mget = redisCommands.mget(key);
    }
    try {
      List<KeyValue<String, String>> keyValues = mget.get();
      maybeSchema = keyValues.stream().filter(Value::hasValue).map(Value::getValue)
          .filter(Objects::nonNull).map(v -> {
            try {
              return objectMapper.readValue(v, Schema.class);
            } catch (JsonProcessingException e) {
              return null;
            }
          }).filter(Objects::nonNull).findFirst();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Redis error");
      maybeSchema = Optional.empty();
    }
    return maybeSchema;
  }

  private void closeResultSet(ResultSet rs) {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException ignored) {
          // ignore
        }
      }
  }
}
