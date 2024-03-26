/*
 * Copyright 2020 Confluent Inc.
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


package io.confluent.kafka.serializers.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InterruptedIOException;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
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
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class AbstractKafkaJsonSchemaDeserializer<T> extends AbstractKafkaSchemaSerDe {
  protected static final ObjectMapper objectMapper = Jackson.newObjectMapper();
  protected boolean isKey;
  protected Class<T> type;
  protected String typeProperty;
  protected boolean validate;

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  @SuppressWarnings("unchecked")
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

  // The Object return type is a bit messy, but this is the simplest way to have
  // flexible decoding and not duplicate deserialization code multiple times for different variants.
  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload
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

    int id = -1;
    try {
      ByteBuffer buffer = getByteBuffer(payload);
      id = buffer.getInt();
      String subject = isKey == null || strategyUsesSchema(isKey)
          ? getContextName(topic) : subjectName(topic, isKey, null);
      JsonSchema schema = ((JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id));
      if (includeSchemaAndVersion) {
        subject = subjectName(topic, isKey, schema);
        schema = schemaForDeserialize(id, schema, subject, isKey);
      }

      int length = buffer.limit() - 1 - idSize;
      int start = buffer.position() + buffer.arrayOffset();

      JsonNode jsonNode = null;
      if (validate) {
        try {
          jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
          schema.validate(jsonNode);
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

        Integer version = schemaVersion(topic, isKey, id, subject, schema, value);
        return new JsonSchemaAndValue(schema.copy(version), value);
      }

      return value;
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error deserializing JSON message for id " + id, e);
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error deserializing JSON message for id " + id, e);
    } catch (RestClientException e) {
      throw toKafkaException(e, "Error retrieving JSON schema for id " + id);
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
      String topic, boolean isKey, int id, String subject, JsonSchema schema, Object value
  ) throws IOException, RestClientException {
    Integer version;
    if (isDeprecatedSubjectNameStrategy(isKey)) {
      subject = getSubjectName(topic, isKey, value, schema);
    }
    JsonSchema subjectSchema = (JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
    version = schemaRegistry.getVersion(subject, subjectSchema);
    return version;
  }

  private String subjectName(String topic, boolean isKey, JsonSchema schemaFromRegistry) {
    return isDeprecatedSubjectNameStrategy(isKey)
           ? null
           : getSubjectName(topic, isKey, null, schemaFromRegistry);
  }

  private JsonSchema schemaForDeserialize(
      int id, JsonSchema schemaFromRegistry, String subject, boolean isKey
  ) throws IOException, RestClientException {
    return isDeprecatedSubjectNameStrategy(isKey)
           ? JsonSchemaUtils.copyOf(schemaFromRegistry)
           : (JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
  }

  protected JsonSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, byte[] payload
  ) throws SerializationException {
    return (JsonSchemaAndValue) deserialize(true, topic, isKey, payload);
  }
}
