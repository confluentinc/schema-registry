/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.serializers.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.schemaregistry.rules.RuleResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.rules.RulePhase;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import io.confluent.kafka.serializers.provenance.ProvenanceProjector;
import io.confluent.kafka.serializers.provenance.ReaderSchema;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class AbstractKafkaJsonSchemaDeserializer<T> extends AbstractKafkaSchemaSerDe {
  private static final Logger log =
      LoggerFactory.getLogger(AbstractKafkaJsonSchemaDeserializer.class);

  protected ObjectMapper objectMapper = Jackson.newObjectMapper();
  protected Class<T> type;
  protected String typeProperty;
  protected List<String> allowedTypePackages = Collections.singletonList("*");
  protected volatile boolean validate;
  protected volatile boolean validateBeforeDomainRules;

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  protected void configure(KafkaJsonSchemaDeserializerConfig config, Class<T> type) {
    configureClientProperties(config, new JsonSchemaProvider());
    resetProvenance();
    this.type = type;

    boolean failUnknownProperties =
        config.getBoolean(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES);
    this.objectMapper.configure(
        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
        failUnknownProperties
    );
    this.validate = config.getBoolean(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA);
    this.validateBeforeDomainRules =
        config.getBoolean(KafkaJsonSchemaDeserializerConfig.VALIDATE_BEFORE_DOMAIN_RULES);
    this.typeProperty = config.getString(KafkaJsonSchemaDeserializerConfig.TYPE_PROPERTY);
    this.allowedTypePackages =
        config.getList(KafkaJsonSchemaDeserializerConfig.TYPE_ALLOWED_PACKAGES);
  }

  protected KafkaJsonSchemaDeserializerConfig deserializerConfig(Map<String, ?> props) {
    try {
      return new KafkaJsonSchemaDeserializerConfig(props);
    } catch (ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  protected KafkaJsonSchemaDeserializerConfig deserializerConfig(Properties props) {
    return new KafkaJsonSchemaDeserializerConfig(props);
  }

  public ObjectMapper objectMapper() {
    return objectMapper;
  }

  /**
   * Deserializes the payload without including schema information for primitive types, maps, and
   * arrays. Just the resulting deserialized object is returned.
   *
   * <p>This behavior is the norm for Decoders/Deserializers.
   *
   * @param payload serialized data
   * @return the deserialized object
   */
  protected T deserialize(byte[] payload)
      throws SerializationException, InvalidConfigurationException {
    return (T) deserialize(false, null, isKey, payload);
  }

  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload
  ) throws SerializationException, InvalidConfigurationException {
    return deserialize(includeSchemaAndVersion, topic, isKey, null, payload);
  }

  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, Headers headers, byte[] payload
  ) throws SerializationException, InvalidConfigurationException {
    return deserialize(includeSchemaAndVersion, topic, isKey, headers, payload, null);
  }

  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean key, Headers headers, byte[] payload,
      Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc
  ) throws SerializationException, InvalidConfigurationException {
    return deserialize(
        includeSchemaAndVersion, topic, key, headers, payload, writerToReaderSchemaFunc, false);
  }

  // The Object return type is a bit messy, but this is the simplest way to have
  // flexible decoding and not duplicate deserialization code multiple times for different variants.
  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean key, Headers headers, byte[] payload,
      Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc,
      boolean includeRuleResults
  ) throws SerializationException, InvalidConfigurationException {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException(
          "SchemaRegistryClient not found. You need to configure the deserializer "
              + "or use deserializer constructor with SchemaRegistryClient.");
    }
    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle this case.
    if (payload == null) {
      return null;
    }

    boolean isKey = key != null ? key : this.isKey;
    SchemaId schemaId = new SchemaId(JsonSchema.TYPE);
    List<RuleResult> ruleResults = includeRuleResults ? new ArrayList<>() : null;
    try (SchemaIdDeserializer schemaIdDeserializer = schemaIdDeserializer(isKey)) {
      ByteBuffer buffer =
          schemaIdDeserializer.deserialize(topic, isKey, headers, payload, schemaId);
      String subject = strategyUsesSchema(isKey)
          ? getContextName(topic) : subjectName(topic, isKey, null);
      JsonSchema schema = (JsonSchema) getSchemaBySchemaId(subject, schemaId);
      if (subject == null || strategyUsesSchema(isKey)) {
        subject = subjectName(topic, isKey, schema);
        schema = schemaForDeserialize(schemaId, schema, subject, isKey);
      }
      Object buf = executeRules(
          subject, topic, headers, payload, RulePhase.ENCODING, RuleMode.READ, null,
          schema, buffer, ruleResults
      );
      buffer = buf instanceof byte[] ? ByteBuffer.wrap((byte[]) buf) : (ByteBuffer) buf;

      List<Migration> migrations = Collections.emptyList();
      ParsedSchema readerSchema = writerToReaderSchemaFunc != null
          ? writerToReaderSchemaFunc.apply(schema)
          : null;
      if (readerSchema == null) {
        if (metadata != null) {
          readerSchema = getLatestWithMetadata(subject).getSchema();
        } else if (useLatestVersion) {
          readerSchema = lookupLatestVersion(subject, schema, false).getSchema();
        }
        if (includeSchemaAndVersion || readerSchema != null) {
          Integer version = schemaVersion(topic, isKey, schemaId, subject, schema, null);
          schema = schema.copy(version);
        }
        if (readerSchema != null) {
          migrations = getMigrations(subject, schema, readerSchema);
        }
      }

      int length = buffer.remaining();
      int start = buffer.position() + buffer.arrayOffset();

      JsonNode jsonNode = null;
      if (!migrations.isEmpty()) {
        jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
        jsonNode = (JsonNode) executeMigrations(migrations, subject, topic, headers, jsonNode);
      }

      final JsonSchema writerSchema = schema;
      // Pruned first: validation and the domain rules see only what the reader may, validation's
      // defaults reach a pruned property as one never written, and a rule's value is not undone.
      jsonNode = byProvenance(subject, schemaId, writerSchema,
          provenanceReader(readerSchema, migrations, writerSchema), migrations, jsonNode, buffer,
          start, length);
      if (readerSchema != null) {
        schema = (JsonSchema) readerSchema;
      }
      if (validate && validateBeforeDomainRules) {
        jsonNode = validateJson(jsonNode, buffer, start, length, schema);
      }
      if (schema.ruleSet() != null && schema.ruleSet().hasRules(RulePhase.DOMAIN, RuleMode.READ)) {
        if (jsonNode == null) {
          jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
        }
        jsonNode = (JsonNode) executeRules(
            subject, topic, headers, payload, RulePhase.DOMAIN, RuleMode.READ, null,
            schema, jsonNode, ruleResults
        );
      }

      if (validate && !validateBeforeDomainRules) {
        jsonNode = validateJson(jsonNode, buffer, start, length, schema);
      }

      Object value;
      if (type != null && !Object.class.equals(type)) {
        value = jsonNode != null
            ? objectMapper.convertValue(jsonNode, type)
            : objectMapper.readValue(buffer.array(), start, length, type);
      } else {
        String typeName;
        if (schema.has("oneOf") || schema.has("anyOf") || schema.has("allOf")) {
          if (jsonNode == null) {
            jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
          }
          typeName = getTypeName(schema.rawSchema(), jsonNode);
        } else {
          typeName = schema.getString(typeProperty);
        }
        if (typeName != null) {
          value = jsonNode != null
              ? deriveType(jsonNode, typeName)
              : deriveType(buffer, length, start, typeName);
        } else if (Object.class.equals(type)) {
          value = jsonNode != null
              ? objectMapper.convertValue(jsonNode, type)
              : objectMapper.readValue(buffer.array(), start, length, type);
        } else {
          // Return JsonNode if type is null
          value = jsonNode != null
              ? jsonNode
              : objectMapper.readTree(new ByteArrayInputStream(buffer.array(), start, length));
        }
      }

      if (includeSchemaAndVersion) {
        // Annotate the schema with the version. Note that we only do this if the schema +
        // version are requested, i.e. in Kafka Connect converters. This is critical because that
        // code *will not* rely on exact schema equality. Regular deserializers *must not* include
        // this information because it would return schemas which are not equivalent.
        //
        // Note, however, that we also do not fill in the connect.version field. This allows the
        // Converter to let a version provided by a Kafka Connect source take priority over the
        // schema registry's ordering (which is implicit by auto-registration time rather than
        // explicit from the Connector).

        Integer writerVersion = schemaVersion(topic, isKey, schemaId, subject, writerSchema, null);
        ParsedSchemaAndValue.SchemaInfo writerInfo = new ParsedSchemaAndValue.SchemaInfo(
            subject,
            schemaId.getId(),
            writerVersion,
            schemaId.getGuid());
        List<RuleResult> ruleResultsCopy = ruleResults == null || ruleResults.isEmpty()
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(ruleResults));
        return new JsonSchemaAndValue(schema, value, writerInfo, writerSchema, ruleResultsCopy);
      }

      return value;
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error deserializing JSON message for id " + schemaId, e);
    } catch (IOException | RuntimeException e) {
      throw toDeserializationException(e, "Error deserializing JSON message for id " + schemaId);
    } catch (RestClientException e) {
      throw toKafkaException(e, "Error retrieving JSON schema for id " + schemaId);
    } finally {
      postOp(payload);
    }
  }

  private String getTypeName(Schema schema, JsonNode jsonNode) {
    if (schema instanceof CombinedSchema) {
      for (Schema subschema : ((CombinedSchema) schema).getSubschemas()) {
        boolean valid = false;
        try {
          JsonSchema.validate(subschema, jsonNode);
          valid = true;
        } catch (Exception e) {
          // noop
        }
        if (valid) {
          return getTypeName(subschema, jsonNode);
        }
      }
    } else if (schema instanceof ReferenceSchema) {
      return getTypeName(((ReferenceSchema)schema).getReferredSchema(), jsonNode);
    }
    return (String) schema.getUnprocessedProperties().get(typeProperty);
  }

  private Object deriveType(
      ByteBuffer buffer, int length, int start, String typeName
  ) throws IOException {
    checkTypeAllowed(typeName);
    if (!isJsonContainerPayload(buffer.array(), start, length)) {
      throw new SerializationException("Refusing to resolve javaType " + typeName
          + " for non-object/array JSON payload");
    }
    try {
      Class<?> cls = Class.forName(typeName);
      return objectMapper.readValue(buffer.array(), start, length, cls);
    } catch (ClassNotFoundException e) {
      throw new SerializationException("Class " + typeName + " could not be found.");
    }
  }

  private Object deriveType(JsonNode jsonNode, String typeName) throws IOException {
    checkTypeAllowed(typeName);
    if (!jsonNode.isContainerNode()) {
      throw new SerializationException("Refusing to resolve javaType " + typeName
          + " for non-object/array JSON payload");
    }
    try {
      Class<?> cls = Class.forName(typeName);
      return objectMapper.convertValue(jsonNode, cls);
    } catch (ClassNotFoundException e) {
      throw new SerializationException("Class " + typeName + " could not be found.");
    }
  }

  private void checkTypeAllowed(String typeName) {
    if (allowedTypePackages == null || allowedTypePackages.isEmpty()) {
      throw new SerializationException("javaType resolution is disabled "
          + "(json.type.allowed.packages is empty); refusing to load class " + typeName);
    }
    for (String pkg : allowedTypePackages) {
      if (pkg.isEmpty()) {
        continue;
      }
      if ("*".equals(pkg) || typeName.startsWith(pkg)) {
        return;
      }
    }
    throw new SerializationException(
        "Class " + typeName + " is not in json.type.allowed.packages");
  }

  private static boolean isJsonContainerPayload(byte[] arr, int start, int length) {
    int end = start + length;
    for (int i = start; i < end; i++) {
      byte b = arr[i];
      if (b == ' ' || b == '\t' || b == '\n' || b == '\r') {
        continue;
      }
      return b == '{' || b == '[';
    }
    return false;
  }

  private Integer schemaVersion(
      String topic, boolean isKey, SchemaId schemaId,
      String subject, JsonSchema schema, Object value
  ) throws IOException, RestClientException {
    Integer version = null;
    JsonSchema subjectSchema = (JsonSchema) getSchemaBySchemaId(subject, schemaId);
    Metadata metadata = subjectSchema.metadata();
    if (metadata != null) {
      version = metadata.getConfluentVersionNumber();
    }
    if (version == null) {
      version = schemaRegistry.getVersion(subject, subjectSchema);
    }
    return version;
  }

  private String subjectName(String topic, boolean isKey, JsonSchema schemaFromRegistry) {
    return getSubjectName(topic, isKey, null, schemaFromRegistry);
  }

  private JsonSchema schemaForDeserialize(
      SchemaId schemaId, JsonSchema schemaFromRegistry, String subject, boolean isKey
  ) throws IOException, RestClientException {
    return (JsonSchema) getSchemaBySchemaId(subject, schemaId);
  }

  /**
   * With {@code provenance.algorithm}, the payload parsed with every property provenance marks as
   * new removed; {@code node} unchanged otherwise, as it is after migrations, which provenance
   * does not apply to.
   */
  private JsonNode byProvenance(String subject, SchemaId writerId, JsonSchema writer,
      ParsedSchema reader, List<Migration> migrations, JsonNode node, ByteBuffer buffer,
      int start, int length) throws IOException {
    if (provenanceAlgorithm == null || reader == null || !migrations.isEmpty()) {
      return node;
    }
    JsonProvenancePruner pruner = provenanceProjector()
        .project(subject, writerId, writer, reader, false,
            mapping -> JsonProvenancePruner.plan(mapping, (JsonSchema) reader, writer))
        .orElse(null);
    if (pruner == null || pruner.isEmpty()) {
      return node;
    }
    JsonNode document = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
    pruner.prune(document);
    return document;
  }

  /**
   * {@code readers} as a reader function, with any registered id a reader comes with used for
   * provenance instead of being looked up.
   */
  protected Function<ParsedSchema, ParsedSchema> readerSchemas(
      Function<ParsedSchema, ReaderSchema> readers) {
    return provenanceProjector().readerSchemas(readers);
  }

  private volatile ProvenanceProjector<JsonProvenancePruner> provenanceProjector;

  /**
   * Forgets the projector: it belongs to the configuration it was built under. Under the lock it
   * is built under.
   */
  private synchronized void resetProvenance() {
    provenanceProjector = null;
    classSchemas.clear();
    javaTypes.clear();
  }

  /**
   * The reader provenance prunes for: the reader schema, else a typed read's class, as a generated
   * class is in Avro and Protobuf. None after migrations, which provenance does not apply to.
   */
  private ParsedSchema provenanceReader(ParsedSchema readerSchema, List<Migration> migrations,
      JsonSchema writer) {
    return readerSchema != null || !migrations.isEmpty() ? readerSchema : classSchema(writer);
  }

  // Derived once per class: a class's schema never changes, nor does a class without one.
  private final Map<Class<?>, Optional<JsonSchema>> classSchemas = new ConcurrentHashMap<>();

  /**
   * The schema of the class a typed read converts into, the configured type or else the writer's
   * {@code javaType}, derived as the serializer derives it; null for an untyped read.
   */
  private JsonSchema classSchema(JsonSchema writer) {
    if (provenanceAlgorithm == null) {
      return null;
    }
    Class<?> cls = type != null && !Object.class.equals(type) ? type : javaTypeOf(writer);
    if (cls == null || !isApplicationClass(cls)) {
      return null;
    }
    // The projector first: a load holding the map's lock must not wait for the monitor a reset,
    // clearing the map, holds.
    ProvenanceProjector<JsonProvenancePruner> projector = provenanceProjector();
    return classSchemas.computeIfAbsent(cls,
        c -> Optional.ofNullable(loadClassSchema(c, projector))).orElse(null);
  }

  // A class with properties of its own: not a tree, map, collection, primitive or JDK type.
  private static boolean isApplicationClass(Class<?> cls) {
    if (JsonNode.class.isAssignableFrom(cls) || Map.class.isAssignableFrom(cls)) {
      return false;
    }
    if (Collection.class.isAssignableFrom(cls) || cls.isPrimitive()) {
      return false;
    }
    return !cls.getName().startsWith("java.");
  }

  private JsonSchema loadClassSchema(Class<?> cls,
      ProvenanceProjector<JsonProvenancePruner> projector) {
    try {
      // From the class, as the serializer derives it, without constructing one.
      boolean failUnknown =
          objectMapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
      return (JsonSchema) projector.derivedReader(JsonSchemaUtils.getSchemaOfClass(cls,
          null, null, true, failUnknown, objectMapper, schemaRegistry));
    } catch (Exception | LinkageError e) {
      // Once per class: without its schema, its reads have no provenance.
      log.warn("No schema derived from {}; reading it without provenance: {}", cls.getName(),
          e.toString());
      return null;
    }
  }

  // The class a writer's javaType names, looked up once per name: reads need no class lookup.
  private final Map<String, Optional<Class<?>>> javaTypes = new ConcurrentHashMap<>();

  private Class<?> javaTypeOf(JsonSchema writer) {
    String name = writer.getString(typeProperty);
    if (name == null) {
      return null;
    }
    return javaTypes.computeIfAbsent(name, n -> {
      try {
        checkTypeAllowed(n);
        // Not initialized: the read loads it so only for an object or array payload.
        return Optional.of(Class.forName(n, false,
            AbstractKafkaJsonSchemaDeserializer.class.getClassLoader()));
      } catch (ClassNotFoundException | SerializationException e) {
        return Optional.empty();
      }
    }).orElse(null);
  }

  @Override
  protected boolean readsByProvenance() {
    return true;
  }

  // Created on first use, once the deserializer is configured, and only once: it holds the ids
  // readers were supplied with and which readers a class derived.
  private ProvenanceProjector<JsonProvenancePruner> provenanceProjector() {
    ProvenanceProjector<JsonProvenancePruner> projector = provenanceProjector;
    if (projector == null) {
      synchronized (this) {
        projector = provenanceProjector;
        if (projector == null) {
          projector = new ProvenanceProjector<>(schemaRegistry, provenanceAlgorithm,
              provenanceCacheSize, provenanceCacheTtlSec, provenanceStrategy);
          provenanceProjector = projector;
        }
      }
    }
    return projector;
  }

  protected JsonSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, Headers headers, byte[] payload
  ) throws SerializationException {
    return (JsonSchemaAndValue) deserialize(true, topic, isKey, headers, payload);
  }

  protected JsonSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, Headers headers, byte[] payload,
      Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc
  ) throws SerializationException {
    return deserializeWithSchemaAndVersion(
        topic, isKey, headers, payload, writerToReaderSchemaFunc, false);
  }

  protected JsonSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, Headers headers, byte[] payload,
      Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc,
      boolean includeRuleResults
  ) throws SerializationException {
    return (JsonSchemaAndValue) deserialize(
        true, topic, isKey, headers, payload, writerToReaderSchemaFunc, includeRuleResults);
  }

  protected JsonNode validateJson(JsonNode jsonNode, ByteBuffer buffer, int start, int length,
      JsonSchema schema) throws IOException {
    try {
      if (jsonNode == null) {
        jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
      }
      return schema.validate(jsonNode);
    } catch (JsonProcessingException | ValidationException e) {
      throw new SerializationException("JSON does not match schema of type "
          + schema.schemaType(), e);
    }
  }
}
