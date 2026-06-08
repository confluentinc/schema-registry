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
import io.confluent.connect.schema.backup.BackupReferenceResolver;
import io.confluent.connect.schema.backup.BackupSchemaFetcher;
import io.confluent.connect.schema.backup.BackupSchemaFetcher.BackupSchemaInfo;
import io.confluent.connect.schema.backup.BackupSchemaFetcher.RefTreeEntry;
import io.confluent.connect.schema.backup.BackupWrapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of Converter that supports JSON with JSON Schema.
 */
public class JsonSchemaConverter extends AbstractKafkaSchemaSerDe implements Converter {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaConverter.class);
  public static final String BACKUP_MODE_CONFIG = "backup.mode";

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;
  private JsonSchemaData jsonSchemaData;
  private boolean backupEnvelopeMode;
  private BackupSchemaFetcher schemaFetcher;
  private BackupReferenceResolver referenceResolver;
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
    Object backupMode = configs.get(BACKUP_MODE_CONFIG);
    this.backupEnvelopeMode = "envelope".equalsIgnoreCase(
        backupMode != null ? backupMode.toString() : null);
    log.info("JsonSchemaConverter backup.mode={}, envelope={}, isKey={}",
        backupMode, backupEnvelopeMode, isKey);
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

    schemaFetcher = new BackupSchemaFetcher(schemaRegistry);
    referenceResolver = new BackupReferenceResolver(schemaRegistry);
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
      Schema actualConnectSchema = wrapperSchema.field(BackupWrapper.FIELD_DATA).schema();
      String rawSchema = wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA);

      String treeJson = wrapperSchema.field(BackupWrapper.FIELD_REFERENCE_TREE) != null
          ? wrapper.getString(BackupWrapper.FIELD_REFERENCE_TREE) : null;
      String directRefsJson = wrapperSchema.field(BackupWrapper.FIELD_DIRECT_REFS) != null
          ? wrapper.getString(BackupWrapper.FIELD_DIRECT_REFS) : null;

      Map<String, RefTreeEntry> refTree =
          BackupReferenceResolver.parseReferenceTree(treeJson);
      List<SchemaReference> directRefs =
          BackupReferenceResolver.parseDirectRefs(directRefsJson);
      List<SchemaReference> remappedRefs = Collections.emptyList();
      Map<String, String> resolvedTexts = Collections.emptyMap();

      if (!refTree.isEmpty() && !directRefs.isEmpty()) {
        remappedRefs = new ArrayList<>();
        resolvedTexts = new HashMap<>();
        BackupReferenceResolver.ParsedSchemaFactory factory =
            (raw, refs, resolved) -> !refs.isEmpty()
                ? new JsonSchema(raw, refs, resolved, null)
                : new JsonSchema(raw);
        referenceResolver.registerRefsRecursive(
            directRefs, refTree, factory, remappedRefs, resolvedTexts);
      }

      JsonSchema jsonSchema;
      if (rawSchema != null) {
        if (!remappedRefs.isEmpty()) {
          jsonSchema = new JsonSchema(rawSchema, remappedRefs, resolvedTexts, null);
        } else {
          jsonSchema = new JsonSchema(rawSchema);
        }
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
      throws java.io.IOException,
      io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException {
    BackupSchemaInfo info = schemaFetcher.fetchSchemaInfo(schemaId);
    String rawSchema = info.getRawSchema();
    JsonSchema parsed = new JsonSchema(rawSchema);
    String subject = serializer.computeSubjectName(topic, isKey, parsed);

    Integer schemaVersion = info.getVersionForSubject(subject);

    Schema wrapperSchema;
    if (original.schema() == null) {
      wrapperSchema = BackupWrapper.buildSchema(null);
    } else {
      wrapperSchema = wrapperSchemaCache.computeIfAbsent(
          original.schema(), ds -> BackupWrapper.buildSchema(ds));
    }

    Struct wrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.value(),
        schemaId, schemaVersion, "JSON_SCHEMA", subject, rawSchema,
        info.getReferenceTreeJson(),
        info.getDirectRefsJson());

    log.debug("Wrapped backup metadata: topic={}, isKey={}, schemaId={}, hasRefs={}",
        topic, isKey, schemaId, info.getReferenceTreeJson() != null);
    return new SchemaAndValue(wrapperSchema, wrapper);
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
