package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.WILDCARD;

public class PgStore {
  private Connection conn;

  public void init() throws SchemaRegistryException {
    String url = "jdbc:postgresql://localhost:5555/ewu";
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

  public Schema lookupSchemaBySubject(QualifiedSubject qs, Schema schema,
                                      String subject, byte[] hash,
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
        sql.append("AND NOT deleted");
      }
      ps = conn.prepareStatement(sql.toString());
      ps.setString(1, qs.getTenant());
      ps.setString(2, qs.getContext());
      ps.setString(3, qs.getSubject());
      ps.setBytes(4, hash);
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
      throw new SchemaRegistryException("LookupSchemaBySubject error");
    } finally {
      closeResultSet(rs);
    }

    return null;
  }

  public Schema getSubjectVersion(QualifiedSubject qs, int version,
                                  boolean lookupDeletedSchema) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT s.id, sub.subject, s.version, s.type, s.str FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND c.context = ? AND sub.subject = ? AND version = ? ");
      if (!lookupDeletedSchema) {
        sql.append("AND NOT deleted");
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
      throw new SchemaRegistryException("LookupSchemaBySubject error");
    } finally {
      closeResultSet(rs);
    }

    return null;
  }

  public Schema getLatestSubjectVersion(QualifiedSubject qs)
      throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;

    try {
      StringBuilder sql = new StringBuilder();
      // TODO not handling refs
      sql.append("SELECT s.id, sub.subject, s.version, s.type, s.str FROM contexts c ")
          .append("JOIN subjects sub on c.id = sub.context_id ")
          .append("JOIN schemas s on s.subject_id = sub.id ")
          .append("WHERE c.tenant = ? AND sub.subject = ? ");
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
      throw new SchemaRegistryException("GetLatestSubjectVersion error");
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
      throw new SchemaRegistryException("GetOrCreateContext error");
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
      throw new SchemaRegistryException("GetOrCreateSubject error");
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
      throw new SchemaRegistryException("GetMaxVersion error");
    } finally {
      closeResultSet(rs);
    }

    return version;
  }

  public int createSchema(int contextId, int subjectId, int version,
                          ParsedSchema parsedSchema, byte[] hash) throws SchemaRegistryException {
    ResultSet rs = null;
    PreparedStatement ps;
    int schemaId = 0;
    try {
      StringBuilder sql = new StringBuilder();
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

      sql.setLength(0);
      sql.append("INSERT INTO schemas VALUES (?, ?, ?, ?, ?, ?, ?, ?) ")
          .append("RETURNING id");
      ps = conn.prepareStatement(sql.toString());
      ps.setInt(1, schemaId);
      ps.setInt(2, subjectId);
      ps.setInt(3, version);
      ps.setString(4, parsedSchema.schemaType());
      ps.setString(5, parsedSchema.canonicalString());
      ps.setObject(6, new int[0]);
      ps.setBytes(7, hash);
      ps.setBoolean(8, false);
      rs = ps.executeQuery();
      if (rs != null) {
        while (rs.next()) {
          schemaId = rs.getInt(1);
        }
      }
    } catch (Exception e) {
      throw new SchemaRegistryException("CreateSchema error");
    } finally {
      closeResultSet(rs);
    }

    return schemaId;
  }

  private Schema populateSchema(ResultSet rs) throws SQLException {
    int id = rs.getInt(1);
    String subject = rs.getString(2);
    int version = rs.getInt(3);
    String type = rs.getString(4);
    String str = rs.getString(5);
    return new Schema(subject, version, id, type, Collections.emptyList(), str);
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
