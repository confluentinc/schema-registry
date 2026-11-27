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
import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.connect.schema.backup.core.BackupCaches;
import io.confluent.connect.schema.backup.core.BackupConverterHelper;
import io.confluent.connect.schema.backup.core.BackupExceptionMapper;
import io.confluent.connect.schema.backup.core.BackupReferenceResolver;
import io.confluent.connect.schema.backup.core.NamedSchemaCache;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.utils.ExceptionUtils;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
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
  private BackupConverterHelper backupHelper;
  private final Map<Schema, Schema> wrapperSchemaCache =
      BackupCaches.newBoundedMap();
  private final NamedSchemaCache<ProtobufSchema> namedProtobufSchemas = new NamedSchemaCache<>(
      (a, b) -> a.equals(b) && a.dependencies().equals(b.dependencies()), ProtobufSchema::copy);

  public ProtobufConverter() {
  }

  // Public only for testing
  public ProtobufConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.backupEnvelopeMode = BackupConverterHelper.isBackupEnabled(configs);
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

    backupHelper = new BackupConverterHelper(schemaRegistry, wrapperSchemaCache);
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
    } catch (AuthenticationException | AuthorizationException e) {
      // Rethrow as-is so Connect's retry-with-tolerance handling can see the
      // original SR RestClientException in the cause chain instead of it being
      // swallowed into a cause-less ConfigException below.
      throw e;
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
      BackupWrapper.requireFields(wrapperSchema,
          BackupWrapper.FIELD_DATA,
          BackupWrapper.FIELD_SCHEMA_TYPE,
          BackupWrapper.FIELD_RAW_SCHEMA);
      BackupWrapper.requireDataEncoding(wrapperSchema, wrapper,
          BackupWrapper.DATA_ENCODING_CONNECT, getClass().getSimpleName());
      String schemaType = wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE);
      if (!SchemaBackupConfig.TYPE_PROTOBUF.equals(schemaType)) {
        throw new DataException(
            "ProtobufConverter received wrapper with schemaType='" + schemaType
            + "', expected '" + SchemaBackupConfig.TYPE_PROTOBUF + "'");
      }
      Object actualData = wrapper.get(BackupWrapper.FIELD_DATA);
      Schema actualConnectSchema = wrapperSchema.field(BackupWrapper.FIELD_DATA).schema();
      String rawSchema = wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA);
      if (rawSchema == null) {
        throw new DataException(
            "Malformed backup wrapper: missing '" + BackupWrapper.FIELD_RAW_SCHEMA
            + "' for topic " + topic + ". Cannot guarantee pristine restore.");
      }

      ProtobufSchema serializeSchema = (ProtobufSchema)
          backupHelper.getReferenceResolver().resolveRestoreSchema(
              wrapperSchema, wrapper, rawSchema, PROTOBUF_SCHEMA_FACTORY);

      ProtobufSchemaAndValue schemaAndValue =
          protobufData.fromConnectData(actualConnectSchema, actualData);
      Object v = schemaAndValue.getValue();

      if (v instanceof Message) {
        // Name the schema after the restored message, so the subject strategy sees it.
        Message message = (Message) v;
        return serializer.serialize(topic, isKey, headers, message,
            namedForMessage(serializeSchema, message));
      }
      throw new DataException("Unsupported type during restore: "
          + (v != null ? v.getClass().getName() : "null"));
    } catch (Exception e) {
      throw BackupExceptionMapper.classify(
          "restore Protobuf backup for topic " + topic, e);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    try {
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

      if (backupEnvelopeMode && deserialized.getWriterSchemaInfo() != null
          && result.schema() != null) {
        return wrapWithBackupMetadata(
            result, topic, deserialized.getWriterSchemaInfo(), message);
      }
      return result;
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
    } catch (AuthenticationException | AuthorizationException e) {
      // Rethrow as-is so Connect's retry-with-tolerance handling can see the
      // original SR RestClientException in the cause chain instead of it being
      // swallowed into a cause-less ConfigException below.
      throw e;
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format(
              "Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  private SchemaAndValue wrapWithBackupMetadata(
      SchemaAndValue original, String topic,
      ParsedSchemaAndValue.SchemaInfo schemaInfo, Message message) {
    try {
      // The subject follows the record's message, as in the Protobuf deserializer.
      return backupHelper.wrapWithBackupMetadata(
          original, topic, schemaInfo,
          SchemaBackupConfig.TYPE_PROTOBUF, isKey,
          PROTOBUF_SCHEMA_FACTORY,
          (t, k, parsed) -> serializer.computeSubjectName(
              t, k, namedForMessage((ProtobufSchema) parsed, message)));
    } catch (Exception e) {
      throw BackupExceptionMapper.classify(
          "wrap Protobuf backup for topic " + topic, e);
    }
  }

  /**
   * Names {@code schema} after the message type of {@code message}. An unnamed schema
   * otherwise reports its first message as its name.
   */
  private ProtobufSchema namedForMessage(ProtobufSchema schema, Message message) {
    return namedProtobufSchemas.named(schema, message.getDescriptorForType().getFullName());
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

  }
}
