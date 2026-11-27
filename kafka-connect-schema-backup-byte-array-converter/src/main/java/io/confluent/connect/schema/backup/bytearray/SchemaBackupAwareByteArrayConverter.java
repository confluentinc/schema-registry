/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.schema.backup.bytearray;

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.connect.schema.backup.core.BackupConverterHelper;
import io.confluent.connect.schema.backup.core.BackupExceptionMapper;
import io.confluent.connect.schema.backup.core.BackupReferenceResolver;
import io.confluent.connect.schema.backup.core.BackupSchemaFetcher;
import io.confluent.connect.schema.backup.core.BackupSchemaFetcher.BackupSchemaInfo;
import io.confluent.connect.schema.backup.core.NamedSchemaCache;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaIdSerializer;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Byte-array converter that backs up and restores Schema Registry data (Avro, Protobuf,
 * JSON Schema) without re-encoding the payload.
 *
 * <p>With {@code schema.backup.enabled=false} (default) it behaves like
 * {@code ByteArrayConverter}.
 *
 * <p>With {@code schema.backup.enabled=true}, backup stores the payload bytes unchanged
 * in a backup wrapper together with the writer schema and its references. Restore
 * registers the schema on the target registry and rewrites only the schema ID framing
 * (prefix or header, per {@code key.schema.id.serializer} /
 * {@code value.schema.id.serializer}).
 */
public class SchemaBackupAwareByteArrayConverter implements Converter {

  private static final Logger log =
      LoggerFactory.getLogger(SchemaBackupAwareByteArrayConverter.class);

  /**
   * Schema factories keyed by the wrapper's {@code schemaType}. JSON Schema is
   * {@code "JSON_SCHEMA"} in the wrapper and {@code "JSON"} in the registry.
   */
  private static final Map<String, BackupReferenceResolver.ParsedSchemaFactory> FACTORIES;

  static {
    Map<String, BackupReferenceResolver.ParsedSchemaFactory> m = new LinkedHashMap<>();
    m.put(SchemaBackupConfig.TYPE_AVRO, (raw, refs, resolved) ->
        !refs.isEmpty()
            ? new AvroSchema(raw, refs, resolved, null)
            : new AvroSchema(raw));
    m.put(SchemaBackupConfig.TYPE_PROTOBUF, (raw, refs, resolved) ->
        !refs.isEmpty()
            ? new ProtobufSchema(raw, refs, resolved, null, null)
            : new ProtobufSchema(raw));
    m.put(SchemaBackupConfig.TYPE_JSON_SCHEMA, (raw, refs, resolved) ->
        !refs.isEmpty()
            ? new JsonSchema(raw, refs, resolved, null)
            : new JsonSchema(raw));
    FACTORIES = Collections.unmodifiableMap(m);
  }

  // SchemaId reads Protobuf message indexes only for the PROTOBUF type; any other type
  // reads the schema ID or GUID alone. Used before the schema type is known.
  private static final String ID_ONLY_SCHEMA_TYPE = SchemaBackupConfig.TYPE_AVRO;

  private final NamedSchemaCache<ProtobufSchema> namedProtobufSchemas = new NamedSchemaCache<>(
      (a, b) -> a.equals(b) && a.dependencies().equals(b.dependencies()), ProtobufSchema::copy);
  private SchemaRegistryClient schemaRegistry;
  private BackupSchemaFetcher schemaFetcher;
  private BackupReferenceResolver referenceResolver;
  private SchemaIdDeserializer schemaIdDeserializer;
  private SchemaIdSerializer schemaIdSerializer;
  private SubjectNameStrategy subjectNameStrategy;
  private ContextNameStrategy contextNameStrategy;
  private boolean autoRegisterSchema;
  private boolean normalizeSchema;
  private boolean propagateSchemaTags;
  // Configured setting that would replace the writer schema on restore, or null.
  private String writerSchemaOverride;
  private boolean backupEnabled;
  private boolean isKey;
  private Schema wrapperSchema;

  public SchemaBackupAwareByteArrayConverter() {
  }

  // Visible for testing.
  public SchemaBackupAwareByteArrayConverter(SchemaRegistryClient client) {
    this.schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.backupEnabled = BackupConverterHelper.isBackupEnabled(configs);
    log.info("SchemaBackupAwareByteArrayConverter schema.backup.enabled={}, isKey={}",
        backupEnabled, isKey);
    if (!backupEnabled) {
      return;
    }

    SchemaBackupAwareByteArrayConverterConfig cfg =
        new SchemaBackupAwareByteArrayConverterConfig(configs);
    if (schemaRegistry == null) {
      List<SchemaProvider> providers = Arrays.asList(
          new AvroSchemaProvider(),
          new ProtobufSchemaProvider(),
          new JsonSchemaProvider());
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          cfg.getSchemaRegistryUrls(),
          cfg.getMaxSchemasPerSubject(),
          providers,
          configs,
          cfg.requestHeaders());
    }
    this.autoRegisterSchema = cfg.autoRegisterSchema();
    this.normalizeSchema = cfg.normalizeSchema();
    this.propagateSchemaTags = cfg.propagateSchemaTags();
    this.contextNameStrategy = cfg.contextNameStrategy();
    this.writerSchemaOverride = findWriterSchemaOverride(cfg);
    this.schemaFetcher = new BackupSchemaFetcher(schemaRegistry);
    this.referenceResolver = new BackupReferenceResolver(schemaRegistry, autoRegisterSchema);
    this.schemaIdDeserializer = isKey
        ? cfg.keySchemaIdDeserializer() : cfg.valueSchemaIdDeserializer();
    this.schemaIdSerializer = isKey
        ? cfg.keySchemaIdSerializer() : cfg.valueSchemaIdSerializer();
    this.subjectNameStrategy = isKey
        ? cfg.keySubjectNameStrategy() : cfg.valueSubjectNameStrategy();
    // As in AbstractKafkaSchemaSerDe: some subject name strategies need the client.
    this.subjectNameStrategy.setSchemaRegistryClient(schemaRegistry);
    this.wrapperSchema = BackupWrapper.buildSchema(Schema.OPTIONAL_BYTES_SCHEMA);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    if (value == null) {
      if (backupEnabled) {
        // Tombstone: return SchemaAndValue.NULL, as the Schema Registry converters do.
        return SchemaAndValue.NULL;
      }
      // Passthrough mode: same as ByteArrayConverter.
      return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null);
    }
    if (!backupEnabled) {
      return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
    }
    return wrapForBackup(topic, headers, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return fromConnectData(topic, null, schema, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    if (value == null) {
      return null;
    }
    if (backupEnabled && BackupWrapper.isWrapper(schema) && value instanceof Struct) {
      return restoreFromWrapper(topic, headers, (Struct) value);
    }
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer bb = ((ByteBuffer) value).duplicate();
      byte[] out = new byte[bb.remaining()];
      bb.get(out);
      return out;
    }
    throw new DataException(
        "SchemaBackupAwareByteArrayConverter cannot serialize value of type "
        + value.getClass().getName()
        + " [backupEnabled=" + backupEnabled
        + ", isKey=" + isKey
        + ", schemaName=" + (schema != null ? schema.name() : "null")
        + ", schemaType=" + (schema != null ? schema.type() : "null")
        + ", isWrapper=" + BackupWrapper.isWrapper(schema) + "]");
  }

  private SchemaAndValue wrapForBackup(String topic, Headers headers, byte[] bytes) {
    try {
      SchemaId probe = new SchemaId(ID_ONLY_SCHEMA_TYPE);
      schemaIdDeserializer.deserialize(topic, isKey, headers, bytes, probe);
      if (probe.getId() == null && probe.getGuid() == null) {
        throw new DataException(
            "Malformed wire format for topic " + topic
            + ": neither schema ID nor GUID could be extracted from bytes or headers");
      }

      BackupSchemaInfo info = probe.getId() != null
          ? schemaFetcher.fetchSchemaInfo(probe.getId())
          : schemaFetcher.fetchSchemaInfoByGuid(probe.getGuid().toString());

      String wrapperType = toWrapperSchemaType(info.getSchemaType());
      BackupReferenceResolver.ParsedSchemaFactory factory = FACTORIES.get(wrapperType);
      if (factory == null) {
        throw new DataException(
            "Unsupported schemaType from Schema Registry: " + info.getSchemaType()
            + " (topic=" + topic + ")");
      }

      ParsedSchema parsed = withRecordName(info.parse(factory), topic, headers, bytes);
      String subject = subjectName(topic, parsed);
      Integer version = info.getVersionForSubject(subject);

      BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
          probe.getId(), version, wrapperType, subject,
          info.getRawSchema(),
          info.getReferenceTreeJson(),
          info.getDirectRefsJson(),
          probe.getGuid() != null ? probe.getGuid().toString() : null,
          BackupWrapper.DATA_ENCODING_WIRE_BYTES);
      Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, bytes, fields);
      return new SchemaAndValue(wrapperSchema, wrapper);
    } catch (Exception e) {
      throw BackupExceptionMapper.classify("backup for topic " + topic, e);
    }
  }

  private byte[] restoreFromWrapper(String topic, Headers headers, Struct wrapper) {
    if (writerSchemaOverride != null) {
      throw new ConfigException(writerSchemaOverride
          + " is not supported by SchemaBackupAwareByteArrayConverter on restore. It"
          + " restores the original payload bytes unchanged, so they must be labelled"
          + " with the backed-up writer schema; this setting would label them with a"
          + " different schema and consumers would decode them with the wrong schema.");
    }
    try {
      Schema wschema = wrapper.schema();
      if (wschema.field(BackupWrapper.FIELD_DATA) == null) {
        throw new DataException(
            "Malformed backup wrapper: missing '" + BackupWrapper.FIELD_DATA
            + "' field for topic " + topic);
      }
      BackupWrapper.requireDataEncoding(wschema, wrapper,
          BackupWrapper.DATA_ENCODING_WIRE_BYTES, getClass().getSimpleName());
      String schemaType = wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE);
      BackupReferenceResolver.ParsedSchemaFactory factory = FACTORIES.get(schemaType);
      if (factory == null) {
        throw new DataException(
            "Unsupported schemaType in backup wrapper: '" + schemaType
            + "' for topic " + topic + ". Expected one of "
            + FACTORIES.keySet());
      }
      String rawSchema = wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA);
      if (rawSchema == null) {
        throw new DataException(
            "Malformed backup wrapper: missing '" + BackupWrapper.FIELD_RAW_SCHEMA
            + "' for topic " + topic + ". Cannot guarantee pristine restore.");
      }
      byte[] originalBytes = wrapper.getBytes(BackupWrapper.FIELD_DATA);
      if (originalBytes == null) {
        throw new DataException(
            "Malformed backup wrapper: missing '" + BackupWrapper.FIELD_DATA
            + "' bytes for topic " + topic);
      }

      // Framing of the stored bytes, from the wrapper: a schema ID means prefix framing,
      // a GUID alone means header framing.
      SchemaId src = new SchemaId(schemaType);
      Integer srcId = optionalInt32(wrapper, wschema, BackupWrapper.FIELD_SCHEMA_ID);
      String srcGuid = optionalString(wrapper, wschema, BackupWrapper.FIELD_SCHEMA_GUID);
      byte[] payloadBytes;
      if (srcId != null) {
        payloadBytes = stripPrefixFraming(topic, originalBytes, src, srcId);
      } else if (srcGuid != null) {
        // Header framing: the stored bytes are the bare payload.
        payloadBytes = originalBytes;
        if (SchemaBackupConfig.TYPE_PROTOBUF.equals(schemaType)) {
          copyMessageIndexesFromHeader(topic, headers, src);
        }
      } else {
        // Wrapper without ID or GUID: read the framing from the bytes and headers.
        ByteBuffer payloadBuf =
            schemaIdDeserializer.deserialize(topic, isKey, headers, originalBytes, src);
        payloadBytes = new byte[payloadBuf.remaining()];
        payloadBuf.get(payloadBytes);
      }

      ParsedSchema parsedSchema =
          referenceResolver.resolveRestoreSchema(wschema, wrapper, rawSchema, factory);
      List<Integer> targetIndexes = src.getMessageIndexes();
      if (parsedSchema instanceof ProtobufSchema) {
        // Name the schema after the record's message, and compute its indexes as the
        // Protobuf serializer does (normalize.schemas skips map-entry messages).
        ProtobufSchema named =
            namedForMessage((ProtobufSchema) parsedSchema, src.getMessageIndexes());
        parsedSchema = named;
        List<Integer> indexes = named.toMessageIndexes(named.name(), normalizeSchema).indexes();
        targetIndexes = indexes.isEmpty() ? MessageIndexes.DEFAULT_INDEX : indexes;
      }

      // Subject from the restore topic and the configured strategy, as the serializers
      // do. Register when auto.register.schemas=true, otherwise look up.
      String subject = subjectName(topic, parsedSchema);
      RegisterSchemaResponse response = autoRegisterSchema
          ? schemaRegistry.registerWithResponse(
              subject, parsedSchema, normalizeSchema, propagateSchemaTags)
          : schemaRegistry.getIdWithResponse(subject, parsedSchema, normalizeSchema);

      SchemaId tgt = new SchemaId(schemaType, response.getId(), response.getGuid());
      if (!targetIndexes.isEmpty()) {
        tgt.setMessageIndexes(targetIndexes);
      }
      // Remove the source schema ID header; deserializers read it before the prefix.
      if (headers != null) {
        headers.remove(schemaIdHeaderName());
      }
      return schemaIdSerializer.serialize(topic, isKey, headers, payloadBytes, tgt);
    } catch (Exception e) {
      throw BackupExceptionMapper.classify("restore for topic " + topic, e);
    }
  }

  /**
   * Parses the prefix framing of the stored bytes (ignoring headers) and returns the
   * payload. Fails if the bytes do not carry the schema ID the wrapper recorded.
   */
  private static byte[] stripPrefixFraming(
      String topic, byte[] originalBytes, SchemaId src, int expectedId) {
    ByteBuffer payloadBuf;
    try {
      payloadBuf = src.fromBytes(ByteBuffer.wrap(originalBytes));
    } catch (RuntimeException e) {
      throw new DataException(
          "Backup wrapper for topic " + topic + " records schema ID " + expectedId
          + " (prefix framing), but the stored bytes do not start with a valid"
          + " schema ID prefix. Backup may be corrupted.", e);
    }
    if (src.getId() == null || src.getId() != expectedId) {
      throw new DataException(
          "Backup wrapper for topic " + topic + " records schema ID " + expectedId
          + ", but the stored bytes carry schema ID " + src.getId()
          + ". Backup may be corrupted.");
    }
    byte[] payloadBytes = new byte[payloadBuf.remaining()];
    payloadBuf.get(payloadBytes);
    return payloadBytes;
  }

  /**
   * Reads the Protobuf message indexes from the schema ID header (header framing).
   * Fails when the header is missing or unreadable.
   */
  private void copyMessageIndexesFromHeader(String topic, Headers headers, SchemaId src) {
    String headerName = schemaIdHeaderName();
    Header header = headers != null ? headers.lastHeader(headerName) : null;
    if (header == null || header.value() == null) {
      throw new DataException(
          "Protobuf record for topic " + topic + " was backed up with header (GUID)"
          + " framing, but the " + headerName + " header is missing on restore. Its"
          + " message indexes are required. Use"
          + " header.converter=org.apache.kafka.connect.converters.ByteArrayConverter"
          + " on both backup and restore so headers round-trip unchanged.");
    }
    SchemaId fromHeader = new SchemaId(SchemaBackupConfig.TYPE_PROTOBUF);
    try {
      fromHeader.fromBytes(ByteBuffer.wrap(header.value()));
    } catch (RuntimeException e) {
      throw new DataException(
          "Protobuf record for topic " + topic + ": the " + headerName + " header"
          + " cannot be parsed, so its message indexes are unavailable. The header"
          + " may have been altered by a non-ByteArray header.converter.", e);
    }
    src.setMessageIndexes(fromHeader.getMessageIndexes());
  }

  /**
   * For Protobuf, returns the schema named after the record's message, read from its message
   * indexes. Other schema types are returned unchanged.
   */
  private ParsedSchema withRecordName(
      ParsedSchema parsed, String topic, Headers headers, byte[] bytes) {
    if (!(parsed instanceof ProtobufSchema)) {
      return parsed;
    }
    SchemaId withIndexes = new SchemaId(SchemaBackupConfig.TYPE_PROTOBUF);
    schemaIdDeserializer.deserialize(topic, isKey, headers, bytes, withIndexes);
    return namedForMessage((ProtobufSchema) parsed, withIndexes.getMessageIndexes());
  }

  /**
   * Names a Protobuf schema after the message selected by {@code indexes}, as the Protobuf
   * deserializer does. An unnamed schema otherwise reports its first message as its name.
   */
  private ProtobufSchema namedForMessage(ProtobufSchema schema, List<Integer> indexes) {
    return namedProtobufSchemas.named(
        schema, schema.toMessageName(new MessageIndexes(indexes)));
  }

  /**
   * Subject for {@code topic}, qualified by {@code context.name.strategy} when it yields
   * a context. Mirrors AbstractKafkaSchemaSerDe#getSubjectName.
   */
  private String subjectName(String topic, ParsedSchema schema) {
    String subject = subjectNameStrategy.subjectName(topic, isKey, schema);
    String contextName = contextNameStrategy.contextName(topic);
    if (contextName == null) {
      return subject;
    }
    contextName = QualifiedSubject.normalizeContext(contextName);
    return subject != null ? contextName + subject : contextName;
  }

  /**
   * Returns the first configured setting that would make the restored ID differ from
   * the backed-up writer schema, or null when none is set.
   */
  private static String findWriterSchemaOverride(
      SchemaBackupAwareByteArrayConverterConfig cfg) {
    if (cfg.useLatestVersion()) {
      return AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION + "=true";
    }
    if (cfg.getLatestWithMetadataSpec() != null) {
      return AbstractKafkaSchemaSerDeConfig.USE_LATEST_WITH_METADATA;
    }
    if (cfg.useSchemaId() != AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID_DEFAULT) {
      return AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID;
    }
    if (cfg.useSchemaGuid() != null) {
      return AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_GUID;
    }
    return null;
  }

  private String schemaIdHeaderName() {
    return isKey ? SchemaId.KEY_SCHEMA_ID_HEADER : SchemaId.VALUE_SCHEMA_ID_HEADER;
  }

  private static Integer optionalInt32(Struct wrapper, Schema schema, String field) {
    return schema.field(field) != null ? wrapper.getInt32(field) : null;
  }

  private static String optionalString(Struct wrapper, Schema schema, String field) {
    return schema.field(field) != null ? wrapper.getString(field) : null;
  }

  /**
   * Maps the registry schemaType to the wrapper value: {@code "JSON"} becomes
   * {@code "JSON_SCHEMA"}, as written by {@code JsonSchemaConverter}.
   */
  private static String toWrapperSchemaType(String srSchemaType) {
    if (JsonSchema.TYPE.equals(srSchemaType)) {
      return SchemaBackupConfig.TYPE_JSON_SCHEMA;
    }
    return srSchemaType;
  }
}
