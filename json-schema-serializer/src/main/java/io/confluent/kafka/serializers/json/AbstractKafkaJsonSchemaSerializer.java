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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import java.io.InterruptedIOException;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.everit.json.schema.ValidationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class AbstractKafkaJsonSchemaSerializer<T> extends AbstractKafkaSchemaSerDe {

  protected boolean normalizeSchema;
  protected boolean autoRegisterSchema;
  protected int useSchemaId = -1;
  protected boolean idCompatStrict;
  protected boolean latestCompatStrict;
  protected static final ObjectMapper objectMapper = Jackson.newObjectMapper();
  protected SpecificationVersion specVersion;
  protected boolean oneofForNullables;
  protected boolean failUnknownProperties;
  protected boolean validate;

  protected void configure(KafkaJsonSchemaSerializerConfig config) {
    configureClientProperties(config, new JsonSchemaProvider());
    this.normalizeSchema = config.normalizeSchema();
    this.autoRegisterSchema = config.autoRegisterSchema();
    this.useSchemaId = config.useSchemaId();
    this.idCompatStrict = config.getIdCompatibilityStrict();
    this.latestCompatStrict = config.getLatestCompatibilityStrict();
    boolean prettyPrint = config.getBoolean(KafkaJsonSchemaSerializerConfig.JSON_INDENT_OUTPUT);
    this.objectMapper.configure(SerializationFeature.INDENT_OUTPUT, prettyPrint);
    boolean writeDatesAsIso8601 = config.getBoolean(
        KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601);
    this.objectMapper.configure(
        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, !writeDatesAsIso8601);
    this.specVersion = SpecificationVersion.get(
        config.getString(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION));
    this.oneofForNullables = config.getBoolean(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES);
    String inclusion = config.getString(KafkaJsonSchemaSerializerConfig.DEFAULT_PROPERTY_INCLUSION);
    if (inclusion != null) {
      this.objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.valueOf(inclusion));
    }
    this.failUnknownProperties =
        config.getBoolean(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES);
    this.validate = config.getBoolean(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA);
  }

  protected KafkaJsonSchemaSerializerConfig serializerConfig(Map<String, ?> props) {
    try {
      return new KafkaJsonSchemaSerializerConfig(props);
    } catch (ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  public ObjectMapper objectMapper() {
    return objectMapper;
  }

  protected byte[] serializeImpl(
      String subject,
      T object,
      JsonSchema schema
  ) throws SerializationException, InvalidConfigurationException {
    return serializeImpl(subject, null, null, object, schema);
  }

  @SuppressWarnings("unchecked")
  protected byte[] serializeImpl(
      String subject,
      String topic,
      Headers headers,
      T object,
      JsonSchema schema
  ) throws SerializationException, InvalidConfigurationException {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException(
          "SchemaRegistryClient not found. You need to configure the serializer "
              + "or use serializer constructor with SchemaRegistryClient.");
    }
    // null needs to treated specially since the client most likely just wants to send
    // an individual null value instead of making the subject a null type. Also, null in
    // Kafka has a special meaning for deletion in a topic with the compact retention policy.
    // Therefore, we will bypass schema registration and return a null value in Kafka, instead
    // of an encoded null.
    if (object == null) {
      return null;
    }
    String restClientErrorMsg = "";
    try {
      int id;
      if (autoRegisterSchema) {
        restClientErrorMsg = "Error registering JSON schema: ";
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema s =
            registerWithResponse(subject, schema, normalizeSchema);
        if (s.getSchema() != null) {
          Optional<ParsedSchema> optSchema = schemaRegistry.parseSchema(s);
          if (optSchema.isPresent()) {
            schema = (JsonSchema) optSchema.get();
            schema = schema.copy(s.getVersion());
          }
        }
        id = s.getId();
      } else if (useSchemaId >= 0) {
        restClientErrorMsg = "Error retrieving schema ID";
        schema = (JsonSchema)
            lookupSchemaBySubjectAndId(subject, useSchemaId, schema, idCompatStrict);
        id = schemaRegistry.getId(subject, schema);
      } else if (metadata != null) {
        restClientErrorMsg = "Error retrieving latest with metadata '" + metadata + "'";
        schema = (JsonSchema) getLatestWithMetadata(subject);
        id = schemaRegistry.getId(subject, schema);
      } else if (useLatestVersion) {
        restClientErrorMsg = "Error retrieving latest version: ";
        schema = (JsonSchema) lookupLatestVersion(subject, schema, latestCompatStrict);
        id = schemaRegistry.getId(subject, schema);
      } else {
        restClientErrorMsg = "Error retrieving JSON schema: ";
        id = schemaRegistry.getId(subject, schema, normalizeSchema);
      }
      object = (T) executeRules(subject, topic, headers, RuleMode.WRITE, null, schema, object);
      if (validate) {
        object = validateJson(object, schema);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putInt(id).array());
      out.write(objectMapper.writeValueAsBytes(object));
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error serializing JSON message", e);
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error serializing JSON message", e);
    } catch (RestClientException e) {
      throw toKafkaException(e, restClientErrorMsg + schema);
    } finally {
      postOp(object);
    }
  }

  @SuppressWarnings("unchecked")
  protected T validateJson(T object,
                           JsonSchema schema)
      throws SerializationException {
    try {
      JsonNode jsonNode = object instanceof JsonNode
          ? (JsonNode) object
          : objectMapper.convertValue(object, JsonNode.class);
      jsonNode = schema.validate(jsonNode);
      return object instanceof JsonNode
          ? object
          : (T) objectMapper.convertValue(jsonNode, object.getClass());
    } catch (JsonProcessingException e) {
      throw new SerializationException("JSON "
          + object
          + " does not match schema "
          + schema.canonicalString(), e);
    } catch (ValidationException e) {
      throw new SerializationException("Validation error in JSON "
          + object
          + ", Error report:\n"
          + e.toJSON().toString(2), e);
    }
  }
}
