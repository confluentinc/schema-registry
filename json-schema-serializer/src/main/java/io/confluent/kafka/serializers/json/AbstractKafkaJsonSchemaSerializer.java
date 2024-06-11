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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
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

  protected boolean autoRegisterSchema;
  protected boolean useLatestVersion;
  protected boolean latestCompatStrict;
  protected ObjectMapper objectMapper = Jackson.newObjectMapper();
  protected SpecificationVersion specVersion;
  protected boolean oneofForNullables;
  protected boolean failUnknownProperties;
  protected boolean validate;

  protected void configure(KafkaJsonSchemaSerializerConfig config) {
    configureClientProperties(config, new JsonSchemaProvider());
    this.autoRegisterSchema = config.autoRegisterSchema();
    this.useLatestVersion = config.useLatestVersion();
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

  protected byte[] serializeImpl(
      String subject,
      T object,
      JsonSchema schema
  ) throws SerializationException, InvalidConfigurationException {
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
        id = schemaRegistry.register(subject, schema);
      } else if (useLatestVersion) {
        restClientErrorMsg = "Error retrieving latest version: ";
        schema = (JsonSchema) lookupLatestVersion(subject, schema, latestCompatStrict);
        id = schemaRegistry.getId(subject, schema);
      } else {
        restClientErrorMsg = "Error retrieving JSON schema: ";
        id = schemaRegistry.getId(subject, schema);
      }
      if (validate) {
        try {
          JsonNode jsonNode = objectMapper.convertValue(object, JsonNode.class);
          schema.validate(jsonNode);
        } catch (JsonProcessingException | ValidationException e) {
          throw new SerializationException("JSON "
              + object
              + " does not match schema "
              + schema.canonicalString(), e);
        }
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putInt(id).array());
      out.write(objectMapper.writeValueAsBytes(object));
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error serializing JSON message", e);
    } catch (RestClientException e) {
      throw toKafkaException(e, restClientErrorMsg + schema);
    }
  }
}
