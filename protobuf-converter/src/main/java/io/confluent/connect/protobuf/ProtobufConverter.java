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

package io.confluent.connect.protobuf;

import com.google.protobuf.Message;
import io.confluent.connect.schema.backup.BackupReferenceResolver;
import io.confluent.connect.schema.backup.BackupSchemaFetcher;
import io.confluent.connect.schema.backup.BackupSchemaFetcher.BackupSchemaInfo;
import io.confluent.connect.schema.backup.BackupWrapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.utils.ExceptionUtils;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of Converter that uses Protobuf schemas and objects.
 */
public class ProtobufConverter implements Converter {

  private static final Logger log = LoggerFactory.getLogger(ProtobufConverter.class);

  private static final BackupReferenceResolver.ParsedSchemaFactory PROTOBUF_SCHEMA_FACTORY =
      (raw, refs, resolvedSchemas) -> !refs.isEmpty()
          ? new ProtobufSchema(raw, refs, resolvedSchemas, null, null)
          : new ProtobufSchema(raw);

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;
  private ProtobufData protobufData;
  private boolean backupEnvelopeMode;
  private BackupSchemaFetcher schemaFetcher;
  private BackupReferenceResolver referenceResolver;
  private final Map<Schema, Schema> wrapperSchemaCache =
      new java.util.concurrent.ConcurrentHashMap<>();

  public ProtobufConverter() {
  }

  // Public only for testing
  public ProtobufConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    Object schemaBackup = configs.get(BackupWrapper.SCHEMA_BACKUP_ENABLED_CONFIG);
    this.backupEnvelopeMode = "true".equalsIgnoreCase(
        schemaBackup != null ? schemaBackup.toString() : null);
    log.info("ProtobufConverter schema.backup.enabled={}, isKey={}", backupEnvelopeMode, isKey);
    ProtobufConverterConfig protobufConverterConfig = new ProtobufConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          protobufConverterConfig.getSchemaRegistryUrls(),
          protobufConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new ProtobufSchemaProvider()),
          configs,
          protobufConverterConfig.requestHeaders()
      );
    }

    serializer = new Serializer(configs, schemaRegistry);
    deserializer = new Deserializer(configs, schemaRegistry);
    protobufData = new ProtobufData(new ProtobufDataConfig(configs));

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
    try {
      ProtobufSchemaAndValue schemaAndValue = protobufData.fromConnectData(schema, value);
      Object v = schemaAndValue.getValue();
      if (v == null) {
        return null;
      } else if (v instanceof Message) {
        return serializer.serialize(topic,
            isKey,
            headers,
            (Message) v,
            schemaAndValue.getSchema()
        );
      } else {
        throw new DataException("Unsupported object of class " + v.getClass().getName());
      }
    } catch (TimeoutException e) {
      throw new RetriableException(String.format(
          "Failed to serialize Protobuf data from topic %s :",
          topic
      ), e);
    } catch (SerializationException e) {
      if (ExceptionUtils.isNetworkConnectionException(e.getCause())) {
        throw new NetworkException(
            String.format(
                "Network connection error while serializing Protobuf data for topic %s: %s",
                topic, e.getCause().getMessage()),
            e
        );
      } else {
        throw new DataException(String.format(
            "Failed to serialize Protobuf data from topic %s:",
            topic
        ), e);
      }
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format(
              "Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  private byte[] restoreFromWrapper(
      String topic, Headers headers, Schema wrapperSchema, Struct wrapper) {
    try {
      Object actualData = wrapper.get(BackupWrapper.FIELD_DATA);
      Schema actualConnectSchema = wrapperSchema.field(BackupWrapper.FIELD_DATA).schema();
      String rawSchema = wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA);

      BackupReferenceResolver.ResolutionResult resolved =
          referenceResolver.resolveFromWrapper(wrapperSchema, wrapper, PROTOBUF_SCHEMA_FACTORY);

      ProtobufSchemaAndValue schemaAndValue =
          protobufData.fromConnectData(actualConnectSchema, actualData);
      Object v = schemaAndValue.getValue();

      ProtobufSchema serializeSchema;
      if (rawSchema != null) {
        serializeSchema = resolved.hasReferences()
            ? new ProtobufSchema(rawSchema, resolved.getTargetRefs(),
                resolved.getResolvedSchemas(), null, null)
            : new ProtobufSchema(rawSchema);
      } else {
        serializeSchema = schemaAndValue.getSchema();
      }

      if (v instanceof Message) {
        return serializer.serialize(topic, isKey, headers,
            (Message) v, serializeSchema);
      }
      throw new DataException("Unsupported type during restore: "
          + (v != null ? v.getClass().getName() : "null"));
    } catch (Exception e) {
      throw new DataException(
          String.format("Failed to restore Protobuf data for topic %s", topic), e);
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

      ProtobufSchemaAndValue deserialized =
          deserializer.deserialize(topic, isKey, headers, value);

      if (deserialized == null || deserialized.getValue() == null) {
        return SchemaAndValue.NULL;
      }
      Object object = deserialized.getValue();
      if (!(object instanceof Message)) {
        throw new DataException(String.format(
            "Unsupported type %s returned during deserialization of topic %s ",
            object.getClass().getName(),
            topic
        ));
      }

      Message message = (Message) object;
      SchemaAndValue result = protobufData.toConnectData(deserialized.getSchema(), message);

      if (backupEnvelopeMode && schemaId != null && result.schema() != null) {
        return wrapWithBackupMetadata(result, topic, schemaId);
      }
      return result;
    } catch (java.io.InterruptedIOException e) {
      throw new RetriableException(
          String.format("Timeout fetching Protobuf schema for topic %s: ", topic),
          e
      );
    } catch (java.io.IOException e) {
      throw new SerializationException(
          String.format("Error fetching Protobuf schema for topic %s: ", topic),
          e
      );
    } catch (io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException e) {
      String msg = String.format(
          "Error fetching Protobuf schema for topic %s", topic);
      throw Deserializer.toKafka(e, msg);
    } catch (TimeoutException e) {
      throw new RetriableException(String.format(
          "Failed to deserialize data for topic %s to Protobuf: ",
          topic
      ), e);
    } catch (SerializationException e) {
      if (ExceptionUtils.isNetworkConnectionException(e.getCause())) {
        throw new NetworkException(
            String.format("Network connection error while deserializing data for topic %s: %s",
                topic, e.getCause().getMessage()),
            e
        );
      } else {
        throw new DataException(String.format(
            "Failed to deserialize data for topic %s to Protobuf:",
            topic
        ), e);
      }
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format(
              "Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  private SchemaAndValue wrapWithBackupMetadata(
      SchemaAndValue original, String topic, int schemaId)
      throws java.io.IOException,
      io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException {
    BackupSchemaInfo info = schemaFetcher.fetchSchemaInfo(schemaId);
    String rawSchema = info.getRawSchema();

    ProtobufSchema parsed;
    if (!info.getDirectReferences().isEmpty()) {
      java.util.Map<String, String> resolved = new java.util.HashMap<>();
      for (java.util.Map.Entry<String, BackupSchemaFetcher.RefTreeEntry> e
          : info.getReferenceTree().entrySet()) {
        resolved.put(e.getKey(), e.getValue().getSchema());
      }
      parsed = new ProtobufSchema(rawSchema,
          info.getDirectReferences(), resolved, null, null);
    } else {
      parsed = new ProtobufSchema(rawSchema);
    }
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
        schemaId, schemaVersion, "PROTOBUF", subject, rawSchema,
        info.getReferenceTreeJson(),
        info.getDirectRefsJson());

    log.debug("Wrapped backup metadata: topic={}, isKey={}, schemaId={}, hasRefs={}",
        topic, isKey, schemaId, info.getReferenceTreeJson() != null);
    return new SchemaAndValue(wrapperSchema, wrapper);
  }

  static class Serializer extends AbstractKafkaProtobufSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client, false);
      configure(new KafkaProtobufSerializerConfig(configs));
    }

    public byte[] serialize(
        String topic, boolean isKey, Headers headers, Message value, ProtobufSchema schema) {
      if (value == null) {
        return null;
      }
      return serializeImpl(getSubjectName(topic, isKey, value, schema),
          topic, isKey, headers, value, schema);
    }

    public String computeSubjectName(String topic, boolean isKey, ParsedSchema schema) {
      return getSubjectName(topic, isKey, null, schema);
    }
  }

  static class Deserializer extends AbstractKafkaProtobufDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client);
      configure(new KafkaProtobufDeserializerConfig(configs), null);
    }

    public ProtobufSchemaAndValue deserialize(
        String topic, boolean isKey, Headers headers, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
    }

    static RuntimeException toKafka(
        io.confluent.kafka.schemaregistry.client.rest.exceptions
            .RestClientException e, String msg) {
      return toKafkaException(e, msg);
    }
  }
}
