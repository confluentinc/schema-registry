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
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
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
 * Byte-array Kafka Connect Converter with optional SR-aware backup/restore mode.
 *
 * <p>When {@code schema.backup.enabled=false} (default), behaves as a pure passthrough:
 * {@code toConnectData} emits {@code SchemaAndValue(OPTIONAL_BYTES, bytes)} and
 * {@code fromConnectData} accepts {@code byte[]} or {@code ByteBuffer}.
 *
 * <p>When {@code schema.backup.enabled=true}, unifies the backup/restore path for all
 * three SR-aware wire formats (Avro, Protobuf, JSON Schema). The payload bytes are stored
 * verbatim in the wrapper's {@code data} field; only the wire-format header (magic byte +
 * schema ID/GUID + Protobuf message indexes) is parsed and rewritten. Reference
 * pre-registration on the target SR uses the existing {@link BackupReferenceResolver}
 * unchanged.
 *
 * <p>Wire-header handling is delegated to {@link SchemaId} / {@link SchemaIdDeserializer}
 * / {@link SchemaIdSerializer} from {@code kafka-schema-serializer}. Format choice on
 * restore (prefix ID vs header GUID) follows the standard
 * {@code key.schema.id.serializer} / {@code value.schema.id.serializer} configs.
 */
public class SchemaBackupAwareByteArrayConverter implements Converter {

  private static final Logger log =
      LoggerFactory.getLogger(SchemaBackupAwareByteArrayConverter.class);

  /**
   * Factories keyed by the wrapper's {@code schemaType} field.
   * Note: JSON Schema uses {@code "JSON_SCHEMA"} in the wrapper (matches
   * existing {@code JsonSchemaConverter}) even though {@code JsonSchema.schemaType()}
   * returns {@code "JSON"} at the SR level.
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
  // Restore-time rejection: set when a serializer setting would label the original
  // payload bytes with a schema other than the backed-up writer schema.
  private String writerSchemaOverride;
  private boolean backupEnabled;
  private boolean isKey;
  private Schema wrapperSchema;

  public SchemaBackupAwareByteArrayConverter() {
  }

  // Public for testing: injects a preconfigured SR client.
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
    // Same as AbstractKafkaSchemaSerDe: strategies such as the default
    // AssociatedNameStrategy need the client, otherwise they fall back to <topic>-value.
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
        // Match SR converter family (AvroConverter/ProtobufConverter/JsonSchemaConverter):
        // return SchemaAndValue.NULL so BackupWrapperExtractor hits the tombstone()
        // branch and tags the envelope schemaType as TYPE_NONE (canonical tombstone
        // marker). BackupS3SinkTask wraps records before behavior.on.null.values
        // applies, so IGNORE semantics don't drop these.
        return SchemaAndValue.NULL;
      }
      // Passthrough mode: match plain ByteArrayConverter for drop-in parity.
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
      // Placeholder type: SchemaId only needs a non-PROTOBUF value to skip MI parsing.
      // Bytes are stored verbatim in FIELD_DATA, so buffer position doesn't matter here.
      SchemaId probe = new SchemaId(SchemaBackupConfig.TYPE_AVRO);
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

      ParsedSchema parsed = factory.create(
          info.getRawSchema(),
          info.getDirectReferences(),
          flattenTreeToResolvedMap(info));
      String subject = subjectName(topic, parsed);
      Integer version = info.getVersionForSubject(subject);

      BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
          probe.getId(), version, wrapperType, subject,
          info.getRawSchema(),
          info.getReferenceTreeJson(),
          info.getDirectRefsJson(),
          probe.getGuid() != null ? probe.getGuid().toString() : null);
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

      // The wrapper records how the stored bytes were framed at backup time:
      // a schema ID means prefix framing, a GUID alone means header framing.
      // Use that instead of the restore record's headers, which may be missing,
      // damaged, or unrelated to the stored bytes.
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
        // Wrapper without ID or GUID: keep the original header-driven parsing.
        ByteBuffer payloadBuf =
            schemaIdDeserializer.deserialize(topic, isKey, headers, originalBytes, src);
        payloadBytes = new byte[payloadBuf.remaining()];
        payloadBuf.get(payloadBytes);
      }

      BackupReferenceResolver.ResolutionResult resolved =
          referenceResolver.resolveFromWrapper(wschema, wrapper, factory);
      ParsedSchema parsedSchema = factory.create(
          rawSchema,
          resolved.hasReferences()
              ? resolved.getTargetRefs() : Collections.emptyList(),
          resolved.hasReferences()
              ? resolved.getResolvedSchemas() : Collections.emptyMap());

      // Derive the target subject from the RESTORE topic via the configured strategy,
      // matching what AvroConverter's serializer.serialize(topic, ...) does internally.
      // The wrapper's FIELD_SCHEMA_SUBJECT records the SOURCE subject and is not used
      // here on purpose: after a topic rename during restore (e.g. restore.topic.prefix),
      // the target subject follows the target topic.
      // Same choice as AbstractKafkaAvroSerializer.serializeImpl: register when
      // auto.register.schemas=true, otherwise look up the existing ID.
      String subject = subjectName(topic, parsedSchema);
      RegisterSchemaResponse response = autoRegisterSchema
          ? schemaRegistry.registerWithResponse(
              subject, parsedSchema, normalizeSchema, propagateSchemaTags)
          : schemaRegistry.getIdWithResponse(subject, parsedSchema, normalizeSchema);

      SchemaId tgt = new SchemaId(schemaType, response.getId(), response.getGuid());
      if (!src.getMessageIndexes().isEmpty()) {
        tgt.setMessageIndexes(src.getMessageIndexes());
      }
      // Drop the source schema ID header so the target framing is the only one on
      // the record. Consumers read this header before the prefix, so a stale source
      // GUID would make them look up the wrong schema on the target registry.
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
   * For Protobuf with header (GUID) framing, the message indexes live in the schema ID
   * header, not in the stored bytes, so they are read from the restored record's header.
   * Fails loudly when the header is missing or unreadable instead of guessing.
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
   * Map SR's schemaType string to the wrapper convention.
   * {@link JsonSchema#schemaType()} returns {@code "JSON"} but the existing
   * {@code JsonSchemaConverter} writes {@code "JSON_SCHEMA"} in the wrapper; we match
   * that so wrappers are interchangeable.
   */
  private static String toWrapperSchemaType(String srSchemaType) {
    if (JsonSchema.TYPE.equals(srSchemaType)) {
      return SchemaBackupConfig.TYPE_JSON_SCHEMA;
    }
    return srSchemaType;
  }

  private static Map<String, String> flattenTreeToResolvedMap(BackupSchemaInfo info) {
    Map<String, String> resolved = new LinkedHashMap<>();
    for (Map.Entry<String, BackupSchemaFetcher.RefTreeEntry> e
        : info.getReferenceTree().entrySet()) {
      resolved.put(e.getKey(), e.getValue().getSchema());
    }
    return resolved;
  }
}
