/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.avro;

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.connect.schema.backup.core.BackupConverterHelper;
import io.confluent.connect.schema.backup.core.BackupReferenceResolver;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.utils.ExceptionUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of Converter that uses Avro schemas and objects.
 */
public class AvroConverter implements Converter {

  private static final Logger log = LoggerFactory.getLogger(AvroConverter.class);

  private static final BackupReferenceResolver.ParsedSchemaFactory AVRO_SCHEMA_FACTORY =
      (raw, refs, resolvedSchemas) -> !refs.isEmpty()
          ? new AvroSchema(raw, refs, resolvedSchemas, null)
          : new AvroSchema(raw);

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;
  private AvroData avroData;
  private AvroData restoreAvroData;
  private boolean backupEnvelopeMode;
  private BackupConverterHelper backupHelper;
  private final Map<Schema, Schema> wrapperSchemaCache =
      new ConcurrentHashMap<>();

  public AvroConverter() {
  }

  // Public only for testing
  public AvroConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.backupEnvelopeMode = BackupConverterHelper.isBackupEnabled(configs);
    log.info("AvroConverter schema.backup.enabled={}, isKey={}", backupEnvelopeMode, isKey);
    AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          avroConverterConfig.getSchemaRegistryUrls(),
          avroConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new AvroSchemaProvider()),
          configs,
          avroConverterConfig.requestHeaders()
      );
    }

    serializer = new Serializer(configs, schemaRegistry);
    deserializer = new Deserializer(configs, schemaRegistry);
    avroData = new AvroData(new AvroDataConfig(configs));

    backupHelper = new BackupConverterHelper(schemaRegistry, wrapperSchemaCache);
    restoreAvroData = avroData;
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return fromConnectData(topic, null, schema, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    if (backupEnvelopeMode
        && BackupWrapper.isWrapper(schema) && value instanceof Struct) {
      return restoreFromWrapper(topic, headers, schema, (Struct) value);
    }
    try {
      org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
      return serializer.serialize(
          topic,
          isKey,
          headers,
          avroData.fromConnectData(schema, avroSchema, value),
          new AvroSchema(avroSchema));
    } catch (TimeoutException e) {
      throw new RetriableException(
          String.format("Failed to serialize Avro data from topic %s :", topic),
          e
      );
    } catch (SerializationException e) {
      if (ExceptionUtils.isNetworkConnectionException(e.getCause())) {
        throw new NetworkException(
            String.format(
                "Network connection error while serializing Avro data for topic %s: %s",
                topic, e.getCause().getMessage()),
            e
        );
      } else {
        throw new DataException(
            String.format("Failed to serialize Avro data from topic %s:", topic),
            e
        );
      }
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Avro data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  private byte[] restoreFromWrapper(
      String topic, Headers headers, Schema wrapperSchema, Struct wrapper) {
    try {
      if (wrapperSchema.field(BackupWrapper.FIELD_DATA) == null) {
        throw new DataException("Malformed backup wrapper: missing '"
            + BackupWrapper.FIELD_DATA + "' field");
      }
      Object actualData = wrapper.get(BackupWrapper.FIELD_DATA);
      Schema actualConnectSchema = wrapperSchema.field(BackupWrapper.FIELD_DATA).schema();
      String rawSchema = wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA);

      BackupReferenceResolver.ResolutionResult resolved =
          backupHelper.getReferenceResolver().resolveFromWrapper(
              wrapperSchema, wrapper, AVRO_SCHEMA_FACTORY);

      org.apache.avro.Schema avroSchema =
          restoreAvroData.fromConnectSchema(actualConnectSchema);
      Object avroValue = restoreAvroData.fromConnectData(
          actualConnectSchema, avroSchema, actualData);

      AvroSchema serializeSchema;
      if (rawSchema != null) {
        serializeSchema = resolved.hasReferences()
            ? new AvroSchema(rawSchema, resolved.getTargetRefs(),
                resolved.getResolvedSchemas(), null)
            : new AvroSchema(rawSchema);
      } else {
        serializeSchema = new AvroSchema(avroSchema);
      }

      return serializer.serialize(topic, isKey, headers, avroValue, serializeSchema);
    } catch (Exception e) {
      throw new DataException(
          String.format("Failed to restore Avro data for topic %s", topic), e);
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

      GenericContainerWithVersion containerWithVersion =
          deserializer.deserialize(topic, isKey, headers, value);
      if (containerWithVersion == null) {
        return SchemaAndValue.NULL;
      }
      GenericContainer deserialized = containerWithVersion.container();
      Integer version = containerWithVersion.version();
      SchemaAndValue result;
      if (deserialized instanceof IndexedRecord) {
        result = avroData.toConnectData(deserialized.getSchema(), deserialized, version);
      } else if (deserialized instanceof NonRecordContainer) {
        result = avroData.toConnectData(
            deserialized.getSchema(), ((NonRecordContainer) deserialized).getValue(), version);
      } else {
        throw new DataException(
            String.format("Unsupported type returned during deserialization of topic %s ", topic)
        );
      }

      if (backupEnvelopeMode && schemaId != null && result.schema() != null) {
        return wrapWithBackupMetadata(result, topic, schemaId);
      }
      return result;
    } catch (TimeoutException e) {
      throw new RetriableException(
          String.format("Failed to deserialize data for topic %s to Avro: ", topic),
          e
      );
    } catch (SerializationException e) {
      if (ExceptionUtils.isNetworkConnectionException(e.getCause())) {
        throw new NetworkException(
            String.format(
                "Network connection error while deserializing data for topic %s: %s",
                topic, e.getCause().getMessage()),
            e
        );
      } else {
        throw new DataException(
            String.format("Failed to deserialize data for topic %s to Avro:", topic),
            e
        );
      }
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Avro data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  private SchemaAndValue wrapWithBackupMetadata(
      SchemaAndValue original, String topic, int schemaId) {
    try {
      return backupHelper.wrapWithBackupMetadata(
          original, topic, schemaId,
          SchemaBackupConfig.TYPE_AVRO, isKey,
          AVRO_SCHEMA_FACTORY, serializer::computeSubjectName);
    } catch (Exception e) {
      throw new DataException(
          String.format("Failed to wrap backup metadata for topic %s", topic), e);
    }
  }

  static class Serializer extends AbstractKafkaAvroSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {

      this(client, false);
      configure(new KafkaAvroSerializerConfig(configs));
    }

    public byte[] serialize(
        String topic, boolean isKey, Headers headers, Object value, AvroSchema schema) {
      if (value == null) {
        return null;
      }
      return serializeImpl(
          getSubjectName(topic, isKey, value, schema),
          topic,
          isKey,
          headers,
          value,
          schema);
    }

    public String computeSubjectName(String topic, boolean isKey, ParsedSchema schema) {
      return getSubjectName(topic, isKey, null, schema);
    }

    @Override
    protected DatumWriter<?> getDatumWriter(
        Object value, org.apache.avro.Schema schema, boolean useLogicalTypes, boolean allowNull) {
      GenericData data = AvroSchemaUtils.getThreadLocalGenericData();
      if (data == null) {
        data = AvroSchemaUtils.getGenericData(useLogicalTypes);
      }
      return new GenericDatumWriter<>(schema, data);
    }
  }

  static class Deserializer extends AbstractKafkaAvroDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client);
      configure(new KafkaAvroDeserializerConfig(configs));
    }

    public GenericContainerWithVersion deserialize(
        String topic, boolean isKey, Headers headers, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
    }

  }
}
