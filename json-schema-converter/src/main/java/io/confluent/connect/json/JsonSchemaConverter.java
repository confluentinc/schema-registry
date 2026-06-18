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

package io.confluent.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.connect.schema.backup.BackupConverterHelper;
import io.confluent.connect.schema.backup.BackupReferenceResolver;
import io.confluent.connect.schema.backup.BackupWrapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.utils.ExceptionUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.JsonSchemaAndValue;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implementation of Converter that supports JSON with JSON Schema.
 */
public class JsonSchemaConverter extends AbstractKafkaSchemaSerDe implements Converter {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaConverter.class);

  private static final BackupReferenceResolver.ParsedSchemaFactory JSON_SCHEMA_FACTORY =
      (raw, refs, resolvedSchemas) -> !refs.isEmpty()
          ? new JsonSchema(raw, refs, resolvedSchemas, null)
          : new JsonSchema(raw);

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;
  private JsonSchemaData jsonSchemaData;
  private boolean backupEnvelopeMode;
  private BackupConverterHelper backupHelper;
  private final Map<Schema, Schema> wrapperSchemaCache =
      new java.util.concurrent.ConcurrentHashMap<>();

  public JsonSchemaConverter() {
  }

  @VisibleForTesting
  public JsonSchemaConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.backupEnvelopeMode = BackupConverterHelper.isBackupEnabled(configs);
    log.info("JsonSchemaConverter schema.backup.enabled={}, isKey={}", backupEnvelopeMode, isKey);
    JsonSchemaConverterConfig jsonSchemaConverterConfig = new JsonSchemaConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          jsonSchemaConverterConfig.getSchemaRegistryUrls(),
          jsonSchemaConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new JsonSchemaProvider()),
          configs,
          jsonSchemaConverterConfig.requestHeaders()
      );
    }

    serializer = new Serializer(configs, schemaRegistry);
    deserializer = new Deserializer(configs, schemaRegistry);
    jsonSchemaData = new JsonSchemaData(new JsonSchemaDataConfig(configs));

    backupHelper = new BackupConverterHelper(schemaRegistry, wrapperSchemaCache);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return fromConnectData(topic, null, schema, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    if (BackupWrapper.isWrapper(schema) && value instanceof Struct) {
      return restoreFromWrapper(topic, headers, schema, (Struct) value);
    }
    if (schema == null && value == null) {
      return null;
    }
    JsonSchema jsonSchema = jsonSchemaData.fromConnectSchema(schema);
    JsonNode jsonValue = jsonSchemaData.fromConnectData(schema, value);
    try {
      return serializer.serialize(topic, headers, isKey, jsonValue, jsonSchema);
    } catch (TimeoutException e) {
      throw new RetriableException(String.format(
          "Converting Kafka Connect data to byte[] failed "
              + "due to serialization error of topic %s: ",
          topic),
          e
      );
    } catch (SerializationException e) {
      if (ExceptionUtils.isNetworkConnectionException(e.getCause())) {
        throw new NetworkException(
            String.format(
                "Network connection error while serializing Json data for topic %s: %s",
                topic, e.getCause().getMessage()),
            e
        );
      } else {
        throw new DataException(
            String.format("Converting Kafka Connect data to byte[] failed due to "
                    + "serialization error of topic %s: ",
                topic),
            e
        );
      }
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access JSON Schema data from "
                  + "topic %s : %s", topic, e.getMessage())
      );
    }
  }

  private byte[] restoreFromWrapper(
      String topic, Headers headers, Schema wrapperSchema, Struct wrapper) {
    try {
      Object actualData = wrapper.get(BackupWrapper.FIELD_DATA);
      if (wrapperSchema.field(BackupWrapper.FIELD_DATA) == null) {
        throw new DataException("Malformed backup wrapper: missing '"
            + BackupWrapper.FIELD_DATA + "' field");
      }
      Schema actualConnectSchema = wrapperSchema.field(BackupWrapper.FIELD_DATA).schema();
      String rawSchema = wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA);

      BackupReferenceResolver.ResolutionResult resolved =
          backupHelper.getReferenceResolver().resolveFromWrapper(
              wrapperSchema, wrapper, JSON_SCHEMA_FACTORY);

      JsonSchema jsonSchema;
      if (rawSchema != null) {
        jsonSchema = resolved.hasReferences()
            ? new JsonSchema(rawSchema, resolved.getTargetRefs(),
                resolved.getResolvedSchemas(), null)
            : new JsonSchema(rawSchema);
      } else {
        jsonSchema = jsonSchemaData.fromConnectSchema(actualConnectSchema);
      }
      JsonNode jsonValue = jsonSchemaData.fromConnectData(actualConnectSchema, actualData);
      return serializer.serialize(topic, headers, isKey, jsonValue, jsonSchema);
    } catch (Exception e) {
      throw new DataException(
          String.format("Failed to restore JSON Schema data for topic %s", topic), e);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    try {
      Integer schemaId = backupEnvelopeMode
          ? BackupWrapper.extractSchemaId(value) : null;

      JsonSchemaAndValue deserialized =
          deserializer.deserialize(topic, isKey, headers, value);

      if (deserialized == null || deserialized.getValue() == null) {
        return SchemaAndValue.NULL;
      }

      JsonSchema jsonSchema = deserialized.getSchema();
      Schema schema = jsonSchemaData.toConnectSchema(jsonSchema);
      SchemaAndValue result = new SchemaAndValue(schema, jsonSchemaData.toConnectData(schema,
          (JsonNode) deserialized.getValue()));

      if (backupEnvelopeMode && schemaId != null && result.schema() != null) {
        return wrapWithBackupMetadata(result, topic, schemaId);
      }
      return result;
    } catch (java.io.InterruptedIOException e) {
      throw new RetriableException(
          String.format("Timeout fetching JSON Schema for topic %s: ", topic),
          e
      );
    } catch (java.io.IOException e) {
      throw new SerializationException(
          String.format("Error fetching JSON Schema for topic %s: ", topic),
          e
      );
    } catch (io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException e) {
      String msg = String.format(
          "Error fetching JSON Schema for topic %s", topic);
      throw toKafkaException(e, msg);
    } catch (TimeoutException e) {
      throw new RetriableException(String.format(
          "Converting byte[] to Kafka Connect data failed "
              + "due to serialization error of topic %s: ",
          topic),
          e
      );
    } catch (SerializationException e) {
      if (ExceptionUtils.isNetworkConnectionException(e.getCause())) {
        throw new NetworkException(
            String.format("Network connection error while deserializing data for topic %s: %s",
                topic, e.getCause().getMessage()),
            e
        );
      } else {
        throw new DataException(
            String.format("Converting byte[] to Kafka Connect data failed due to "
                    + "serialization error of topic %s: ",
                topic),
            e
        );
      }
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access JSON Schema data from "
                  + "topic %s : %s", topic, e.getMessage())
      );
    }
  }

  private SchemaAndValue wrapWithBackupMetadata(
      SchemaAndValue original, String topic, int schemaId)
      throws IOException, RestClientException {
    return backupHelper.wrapWithBackupMetadata(
        original, topic, schemaId,
        BackupWrapper.SCHEMA_TYPE_JSON_SCHEMA, isKey,
        JSON_SCHEMA_FACTORY, serializer::computeSubjectName);
  }

  static class Serializer extends AbstractKafkaJsonSchemaSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {

      this(client, false);
      configure(new KafkaJsonSchemaSerializerConfig(configs));
    }

    public byte[] serialize(
        String topic, Headers headers, boolean isKey, Object value, JsonSchema schema) {
      if (value == null) {
        return null;
      }
      return serializeImpl(
          getSubjectName(topic, isKey, value, schema), topic, isKey, headers, value, schema);
    }

    public String computeSubjectName(String topic, boolean isKey, ParsedSchema schema) {
      return getSubjectName(topic, isKey, null, schema);
    }
  }

  static class Deserializer extends AbstractKafkaJsonSchemaDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client);
      configure(new KafkaJsonSchemaDeserializerConfig(configs), null);
    }

    public JsonSchemaAndValue deserialize(
        String topic, boolean isKey, Headers headers, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
    }
  }
}
