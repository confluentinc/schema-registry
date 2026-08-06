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

package io.confluent.connect.backup.bytearray;

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
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaIdSerializer;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
public class BackupAwareByteArrayConverter implements Converter {

  private static final Logger log =
      LoggerFactory.getLogger(BackupAwareByteArrayConverter.class);

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
  private boolean backupEnabled;
  private boolean isKey;
  private Schema wrapperSchema;

  public BackupAwareByteArrayConverter() {
  }

  // Public for testing: injects a preconfigured SR client.
  public BackupAwareByteArrayConverter(SchemaRegistryClient client) {
    this.schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.backupEnabled = BackupConverterHelper.isBackupEnabled(configs);
    log.info("BackupAwareByteArrayConverter schema.backup.enabled={}, isKey={}",
        backupEnabled, isKey);
    if (!backupEnabled) {
      return;
    }

    BackupAwareByteArrayConverterConfig cfg =
        new BackupAwareByteArrayConverterConfig(configs);
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
    this.schemaFetcher = new BackupSchemaFetcher(schemaRegistry);
    this.referenceResolver = new BackupReferenceResolver(schemaRegistry);
    this.schemaIdDeserializer = isKey
        ? cfg.keySchemaIdDeserializer() : cfg.valueSchemaIdDeserializer();
    this.schemaIdSerializer = isKey
        ? cfg.keySchemaIdSerializer() : cfg.valueSchemaIdSerializer();
    this.subjectNameStrategy = isKey
        ? cfg.keySubjectNameStrategy() : cfg.valueSubjectNameStrategy();
    this.wrapperSchema = BackupWrapper.buildSchema(Schema.OPTIONAL_BYTES_SCHEMA);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    if (value == null) {
      // Match plain ByteArrayConverter: non-null schema so downstream sink modes
      // (like BACKUP_FULL_RECORD) can wrap tombstones into an envelope struct.
      // Returning SchemaAndValue.NULL (schema=null) would cause tombstones to be
      // dropped by S3SinkConnector's default behavior.on.null.values=IGNORE path.
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
        "BackupAwareByteArrayConverter cannot serialize value of type "
        + value.getClass().getName());
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
      String subject = subjectNameStrategy.subjectName(topic, isKey, parsed);
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

      // Re-parse with the correct type so PROTOBUF message indexes are extracted.
      SchemaId src = new SchemaId(schemaType);
      ByteBuffer payloadBuf =
          schemaIdDeserializer.deserialize(topic, isKey, headers, originalBytes, src);
      byte[] payloadBytes = new byte[payloadBuf.remaining()];
      payloadBuf.get(payloadBytes);

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
      String subject = subjectNameStrategy.subjectName(topic, isKey, parsedSchema);
      RegisterSchemaResponse response = schemaRegistry.registerWithResponse(
          subject, parsedSchema, false, false);

      SchemaId tgt = new SchemaId(schemaType, response.getId(), response.getGuid());
      if (!src.getMessageIndexes().isEmpty()) {
        tgt.setMessageIndexes(src.getMessageIndexes());
      }
      return schemaIdSerializer.serialize(topic, isKey, headers, payloadBytes, tgt);
    } catch (Exception e) {
      throw BackupExceptionMapper.classify("restore for topic " + topic, e);
    }
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
