/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PgSchemaRegistry implements SchemaRegistry {
  private static final Logger log = LoggerFactory.getLogger(PgSchemaRegistry.class);
  private final SchemaRegistryConfig config;
  private final Map<String, SchemaProvider> providers;
  private final MetricsContainer metricsContainer;
  private final LoadingCache<RawSchema, ParsedSchema> schemaCache;
  private final PgStore pgStore;

  public PgSchemaRegistry(SchemaRegistryConfig config) {
    this.config = config;
    this.providers = initProviders(config);
    this.pgStore = new PgStore();
    this.metricsContainer = new MetricsContainer(config, null);
    this.schemaCache = CacheBuilder.newBuilder()
        .maximumSize(config.getInt(SchemaRegistryConfig.SCHEMA_CACHE_SIZE_CONFIG))
        .expireAfterAccess(
            config.getInt(SchemaRegistryConfig.SCHEMA_CACHE_EXPIRY_SECS_CONFIG), TimeUnit.SECONDS)
        .build(new CacheLoader<RawSchema, ParsedSchema>() {
          @Override
          public ParsedSchema load(RawSchema s) throws Exception {
            return loadSchema(s.getSchemaType(), s.getSchema(), s.getReferences(), s.isNew());
          }
        });
  }

  @Override
  public void init() throws SchemaRegistryException {
    pgStore.init();
  }

  @Override
  public Set<String> schemaTypes() {
    return null;
  }

  @Override
  public Set<String> listSubjects(boolean returnDeletedSubjects) throws SchemaRegistryException {
    return null;
  }

  @Override
  public Iterator<Schema> getAllVersions(String subject, boolean returnDeletedSchemas)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public Iterator<Schema> getVersionsWithSubjectPrefix(String prefix, boolean returnDeletedSchemas,
      boolean latestOnly) throws SchemaRegistryException {
    return null;
  }

  @Override
  public Schema getLatestVersion(String subject) throws SchemaRegistryException {
    return null;
  }

  @Override
  public Schema lookUpSchemaUnderSubject(String subject, Schema schema,
                                         boolean normalize, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    MD5 md5 = MD5.ofSchema(schema);
    QualifiedSubject qs = QualifiedSubject.create(tenant(), schema.getSubject());
    if (qs == null) {
      throw new SchemaRegistryException("Invalid QualifiedSubject");
    }

    // TODO not handling the case where a schema is sent with all references resolved
    return pgStore.lookupSchemaBySubject(qs, schema, subject, tenant(),
        md5.bytes(), lookupDeletedSchema);
  }

  @Override
  public List<String> isCompatible(String subject, Schema newSchema, Schema targetSchema)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public List<String> isCompatible(String subject, Schema newSchema, List<Schema> previousSchemas)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public SchemaRegistryConfig config() {
    return config;
  }

  @Override
  public Map<String, Object> properties() {
    return null;
  }

  @Override
  public void updateConfigOrForward(String subject,
                                    CompatibilityLevel compatibilityLevel,
                                    Map<String, String> headerProperties)
      throws SchemaRegistryException {

  }

  @Override
  public CompatibilityLevel getCompatibilityLevelInScope(String subject)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public CompatibilityLevel getCompatibilityLevel(String subject)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public void deleteCompatibilityConfig(String subject, Map<String, String> headerProperties)
      throws SchemaRegistryException {

  }

  @Override
  public List<String> listContexts() throws SchemaRegistryException {
    return null;
  }

  @Override
  public Schema lookUpSchemaUnderSubjectUsingContexts(String subject,
                                                      Schema schema, boolean normalize,
                                                      boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public boolean hasSubjects(String subject, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    return false;
  }

  @Override
  public Set<String> listSubjectsWithPrefix(String prefix, boolean lookupDeletedSubjects)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public List<Integer> deleteSubject(String subject, boolean permanentDelete)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public List<Integer> deleteSubject(Map<String, String> headerProperties,
                                     String subject, boolean permanentDelete)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public Schema get(String subject, int version, boolean returnDeletedSchema)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public SchemaString get(int id, String subject) throws SchemaRegistryException {
    return null;
  }

  @Override
  public SchemaString get(int id, String subject, String format, boolean fetchMaxId)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public Set<String> listSubjectsForId(int id, String subject, boolean returnDeleted)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public List<SubjectVersion> listVersionsForId(int id, String subject, boolean lookupDeleted)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public Schema getUsingContexts(String subject, int versionId, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public List<Integer> getReferencedBy(String subject, VersionId versionId)
      throws SchemaRegistryException {
    return null;
  }

  @Override
  public int register(String subject, Schema schema, boolean normalize)
      throws SchemaRegistryException {
    return 0;
  }

  @Override
  public int register(String subjectName, Schema schema,
                      boolean normalize, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    // TODO skip mode check
    Schema existingSchema = lookUpSchemaUnderSubject(subjectName, schema, normalize, false);
    if (existingSchema != null) {
      if (schema.getId() == null
          || schema.getId() < 0
          || schema.getId().equals(existingSchema.getId())
      ) {
        return existingSchema.getId();
      }
    }

    int schemaId = schema.getId();
    ParsedSchema parsedSchema = canonicalizeSchema(schema, schemaId < 0, normalize);

    try {
      if (normalize) {
        parsedSchema = parsedSchema.normalize();
      }
    } catch (Exception e) {
      String errMsg = "Invalid schema " + schema + ", details: " + e.getMessage();
      log.error(errMsg, e);
      throw new InvalidSchemaException(errMsg, e);
    }
    // Allow schema providers to modify the schema during compatibility checks
    schema.setSchema(parsedSchema.canonicalString());
    schema.setReferences(parsedSchema.references());

    QualifiedSubject qs = QualifiedSubject.create(tenant(), subjectName);
    if (qs != null) {
      try {
        int contextId = pgStore.getOrCreateContext(tenant(), qs);
        int subjectId = pgStore.getOrCreateSubject(contextId, qs);
        int version = pgStore.getMaxVersion(subjectId) + 1;
        schemaId = pgStore.createSchema(contextId, subjectId, version, parsedSchema,
            MD5.ofSchema(schema).bytes());

        pgStore.commit();
      } catch (Exception e) {
        pgStore.rollback();
        throw new SchemaRegistryException("register failed");
      }
    }
    return schemaId;
  }

  @Override
  public void setMode(String subject, Mode mode,
                      boolean force, Map<String, String> headerProperties)
      throws SchemaRegistryException {

  }

  @Override
  public Mode getModeInScope(String subject) throws SchemaRegistryException {
    return null;
  }

  @Override
  public Mode getMode(String subject) throws SchemaRegistryException {
    return null;
  }

  @Override
  public void deleteSubjectMode(String subject, Map<String, String> headerProperties)
      throws SchemaRegistryException {

  }

  @Override
  public boolean schemaVersionExists(String subject,
                                     VersionId versionId, boolean returnDeletedSchema)
      throws SchemaRegistryException {
    return false;
  }

  @Override
  public void deleteSchemaVersion(String subject, Schema schema, boolean permanentDelete)
      throws SchemaRegistryException {

  }

  @Override
  public void deleteSchemaVersion(Map<String, String> headerProperties,
                                  String subject, Schema schema, boolean permanentDelete)
      throws SchemaRegistryException {

  }

  @Override
  public String getKafkaClusterId() {
    return null;
  }

  @Override
  public String getGroupId() {
    return null;
  }

  @Override
  public MetricsContainer getMetricsContainer() {
    return null;
  }

  private Map<String, SchemaProvider> initProviders(SchemaRegistryConfig config) {
    Map<String, Object> schemaProviderConfigs =
        config.originalsWithPrefix(SchemaRegistryConfig.SCHEMA_PROVIDERS_CONFIG + ".");
    schemaProviderConfigs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, this);
    List<SchemaProvider> defaultSchemaProviders = Arrays.asList(
        new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
    );
    for (SchemaProvider provider : defaultSchemaProviders) {
      provider.configure(schemaProviderConfigs);
    }
    Map<String, SchemaProvider> providerMap = new HashMap<>();
    registerProviders(providerMap, defaultSchemaProviders);
    List<SchemaProvider> customSchemaProviders =
        config.getConfiguredInstances(SchemaRegistryConfig.SCHEMA_PROVIDERS_CONFIG,
            SchemaProvider.class,
            schemaProviderConfigs);
    // Allow custom providers to override default providers
    registerProviders(providerMap, customSchemaProviders);
    //metricsContainer.getCustomSchemaProviderCount().record(customSchemaProviders.size());
    return providerMap;
  }

  private void registerProviders(
      Map<String, SchemaProvider> providerMap,
      List<SchemaProvider> schemaProviders
  ) {
    for (SchemaProvider schemaProvider : schemaProviders) {
      log.info("Registering schema provider for {}: {}",
          schemaProvider.schemaType(),
          schemaProvider.getClass().getName()
      );
      providerMap.put(schemaProvider.schemaType(), schemaProvider);
    }
  }

  private ParsedSchema loadSchema(
      String schemaType,
      String schema,
      List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> references,
      boolean isNew)
      throws InvalidSchemaException {
    if (schemaType == null) {
      schemaType = AvroSchema.TYPE;
    }
    SchemaProvider provider = schemaProvider(schemaType);
    if (provider == null) {
      String errMsg = "Invalid schema type " + schemaType;
      log.error(errMsg);
      throw new InvalidSchemaException(errMsg);
    }
    final String type = schemaType;

    try {
      return provider.parseSchemaOrElseThrow(
          new Schema(null, null, null, schemaType, references, schema), isNew);
    } catch (Exception e) {
      throw new InvalidSchemaException("Invalid schema " + schema
          + " with refs " + references + " of type " + type + ", details: " + e.getMessage());
    }
  }

  public SchemaProvider schemaProvider(String schemaType) {
    return providers.get(schemaType);
  }

  public ParsedSchema parseSchema(Schema schema) throws InvalidSchemaException {
    return parseSchema(schema, false);
  }

  public ParsedSchema parseSchema(Schema schema, boolean isNew) throws InvalidSchemaException {
    return parseSchema(schema.getSchemaType(), schema.getSchema(), schema.getReferences(), isNew);
  }

  public ParsedSchema parseSchema(
      String schemaType,
      String schema,
      List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> references)
      throws InvalidSchemaException {
    return parseSchema(schemaType, schema, references, false);
  }

  public ParsedSchema parseSchema(
      String schemaType,
      String schema,
      List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> references,
      boolean isNew) throws InvalidSchemaException {
    try {
      return schemaCache.get(new RawSchema(schemaType, references, schema, isNew));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof InvalidSchemaException) {
        throw (InvalidSchemaException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private ParsedSchema canonicalizeSchema(Schema schema, boolean isNew, boolean normalize)
      throws InvalidSchemaException {
    if (schema == null
        || schema.getSchema() == null
        || schema.getSchema().trim().isEmpty()) {
      log.error("Empty schema");
      throw new InvalidSchemaException("Empty schema");
    }
    ParsedSchema parsedSchema = parseSchema(schema, isNew);
    try {
      parsedSchema.validate();
      if (normalize) {
        parsedSchema = parsedSchema.normalize();
      }
    } catch (Exception e) {
      String errMsg = "Invalid schema " + schema + ", details: " + e.getMessage();
      log.error(errMsg, e);
      throw new InvalidSchemaException(errMsg, e);
    }
    schema.setSchema(parsedSchema.canonicalString());
    schema.setReferences(parsedSchema.references());
    return parsedSchema;
  }

  private static class RawSchema {
    private String schemaType;
    private List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> references;
    private String schema;
    private boolean isNew;

    public RawSchema(
        String schemaType,
        List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> references,
        String schema,
        boolean isNew) {
      this.schemaType = schemaType;
      this.references = references;
      this.schema = schema;
      this.isNew = isNew;
    }

    public String getSchemaType() {
      return schemaType;
    }

    public List<
        io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> getReferences() {
      return references;
    }

    public String getSchema() {
      return schema;
    }

    public boolean isNew() {
      return isNew;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RawSchema that = (RawSchema) o;
      return isNew == that.isNew
          && Objects.equals(schemaType, that.schemaType)
          && Objects.equals(references, that.references)
          && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schemaType, references, schema, isNew);
    }
  }

  public static class SchemeAndPort {
    public int port;
    public String scheme;

    public SchemeAndPort(String scheme, int port) {
      this.port = port;
      this.scheme = scheme;
    }
  }
}
