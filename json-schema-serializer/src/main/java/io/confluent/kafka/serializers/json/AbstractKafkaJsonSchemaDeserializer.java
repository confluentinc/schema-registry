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
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.rules.RulePhase;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class AbstractKafkaJsonSchemaDeserializer<T> extends AbstractKafkaSchemaSerDe {
  protected ObjectMapper objectMapper = Jackson.newObjectMapper();
  protected Class<T> type;
  protected String typeProperty;
  protected boolean validate;

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  protected void configure(KafkaJsonSchemaDeserializerConfig config, Class<T> type) {
    configureClientProperties(config, new JsonSchemaProvider());
    this.type = type;

    boolean failUnknownProperties =
        config.getBoolean(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES);
    this.objectMapper.configure(
        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
        failUnknownProperties
    );
    this.validate = config.getBoolean(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA);
    this.typeProperty = config.getString(KafkaJsonSchemaDeserializerConfig.TYPE_PROPERTY);
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

  // The Object return type is a bit messy, but this is the simplest way to have
  // flexible decoding and not duplicate deserialization code multiple times for different variants.
  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, Headers headers, byte[] payload
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

    SchemaId schemaId = new SchemaId(JsonSchema.TYPE);
    try (SchemaIdDeserializer schemaIdDeserializer = schemaIdDeserializer(isKey)) {
      ByteBuffer buffer =
          schemaIdDeserializer.deserialize(topic, isKey, headers, payload, schemaId);
      String subject = isKey == null || strategyUsesSchema(isKey)
          ? getContextName(topic) : subjectName(topic, isKey, null);
      JsonSchema schema = (JsonSchema) getSchemaBySchemaId(subject, schemaId);
      if (isKey != null && strategyUsesSchema(isKey)) {
        subject = subjectName(topic, isKey, schema);
        schema = schemaForDeserialize(schemaId, schema, subject, isKey);
      }
      Object buf = executeRules(
          subject, topic, headers, payload, RulePhase.ENCODING, RuleMode.READ, null,
          schema, buffer
      );
      buffer = buf instanceof byte[] ? ByteBuffer.wrap((byte[]) buf) : (ByteBuffer) buf;

      ParsedSchema readerSchema = null;
      if (metadata != null) {
        readerSchema = getLatestWithMetadata(subject).getSchema();
      } else if (useLatestVersion) {
        readerSchema = lookupLatestVersion(subject, schema, false).getSchema();
      }
      if (includeSchemaAndVersion || readerSchema != null) {
        Integer version = schemaVersion(topic, isKey, schemaId, subject, schema, null);
        schema = schema.copy(version);
      }
      List<Migration> migrations = Collections.emptyList();
      if (readerSchema != null) {
        migrations = getMigrations(subject, schema, readerSchema);
      }

      int length = buffer.remaining();
      int start = buffer.position() + buffer.arrayOffset();

      JsonNode jsonNode = null;
      if (!migrations.isEmpty()) {
        jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
        jsonNode = (JsonNode) executeMigrations(migrations, subject, topic, headers, jsonNode);
      }

      if (readerSchema != null) {
        schema = (JsonSchema) readerSchema;
      }
      if (schema.ruleSet() != null && schema.ruleSet().hasRules(RulePhase.DOMAIN, RuleMode.READ)) {
        if (jsonNode == null) {
          jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
        }
        jsonNode = (JsonNode) executeRules(
            subject, topic, headers, payload, RuleMode.READ, null, schema, jsonNode
        );
      }

      if (validate) {
        try {
          if (jsonNode == null) {
            jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
          }
          jsonNode = schema.validate(jsonNode);
        } catch (JsonProcessingException | ValidationException e) {
          throw new SerializationException("JSON "
              + jsonNode
              + " does not match schema "
              + schema.canonicalString(), e);
        }
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

        return new JsonSchemaAndValue(schema, value);
      }

      return value;
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error deserializing JSON message for id " + schemaId, e);
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error deserializing JSON message for id " + schemaId, e);
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
    try {
      Class<?> cls = Class.forName(typeName);
      return objectMapper.readValue(buffer.array(), start, length, cls);
    } catch (ClassNotFoundException e) {
      throw new SerializationException("Class " + typeName + " could not be found.");
    }
  }

  private Object deriveType(JsonNode jsonNode, String typeName) throws IOException {
    try {
      Class<?> cls = Class.forName(typeName);
      return objectMapper.convertValue(jsonNode, cls);
    } catch (ClassNotFoundException e) {
      throw new SerializationException("Class " + typeName + " could not be found.");
    }
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

  protected JsonSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, Headers headers, byte[] payload
  ) throws SerializationException {
    return (JsonSchemaAndValue) deserialize(true, topic, isKey, headers, payload);
  }
}
