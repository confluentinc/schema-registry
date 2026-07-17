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

package io.confluent.connect.schema.backup.core;

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.serializers.schema.id.DualSchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Byte-passthrough converter with schema backup and ID translation.
 *
 * <p>On backup: passes raw bytes through (no deserialization) but extracts
 * the schema ID from the Confluent wire format and wraps in a BackupWrapper
 * so the envelope layer can back up the schema alongside the data.
 *
 * <p>On restore: detects BackupWrapper, resolves and registers schema
 * references in the target SR, rewrites the schema ID in the wire format
 * bytes if the target SR assigned a different ID, and returns the bytes.
 *
 * <p>For records without Confluent wire format (plain strings, JSON, etc.),
 * bytes pass through untouched in both directions — no schema interaction.
 */
public class BackupAwareByteArrayConverter implements Converter {

  private static final Logger log =
      LoggerFactory.getLogger(BackupAwareByteArrayConverter.class);

  private static final byte MAGIC_BYTE = 0x0;
  private static final String KEY_SUBJECT_NAME_STRATEGY =
      "key.subject.name.strategy";
  private static final String VALUE_SUBJECT_NAME_STRATEGY =
      "value.subject.name.strategy";

  private SchemaRegistryClient schemaRegistry;
  private boolean isKey;
  private SubjectNameStrategy subjectNameStrategy;
  private BackupConverterHelper backupHelper;
  private final Map<Schema, Schema> wrapperSchemaCache =
      new ConcurrentHashMap<>();
  private final Map<Integer, Integer> schemaIdTranslation =
      new ConcurrentHashMap<>();

  public BackupAwareByteArrayConverter() {
  }

  public BackupAwareByteArrayConverter(SchemaRegistryClient client) {
    this.schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;

    Object srUrl = configs.get("schema.registry.url");
    if (srUrl != null && schemaRegistry == null) {
      List<SchemaProvider> providers = new ArrayList<>();
      for (SchemaProvider p : ServiceLoader.load(SchemaProvider.class)) {
        providers.add(p);
      }
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          Collections.singletonList(srUrl.toString()),
          1000,
          providers,
          configs,
          Collections.emptyMap()
      );
    }

    backupHelper = schemaRegistry != null
        ? new BackupConverterHelper(schemaRegistry, wrapperSchemaCache)
        : null;

    subjectNameStrategy = createSubjectNameStrategy(configs);

    log.info("BackupAwareByteArrayConverter configured: isKey={}, srAvailable={}, "
        + "subjectStrategy={}",
        isKey, schemaRegistry != null,
        subjectNameStrategy.getClass().getSimpleName());
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return fromConnectData(topic, null, schema, value);
  }

  @Override
  public byte[] fromConnectData(
      String topic, Headers headers, Schema schema, Object value) {
    if (BackupWrapper.isWrapper(schema) && value instanceof Struct) {
      return restoreFromWrapper(topic, schema, (Struct) value);
    }
    return toBytes(value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(
      String topic, Headers headers, byte[] value) {
    if (value == null) {
      return SchemaAndValue.NULL;
    }

    ParsedSchemaAndValue.SchemaInfo schemaInfo = extractSchemaInfo(value, headers);

    if (schemaInfo != null && backupHelper != null) {
      try {
        return backupHelper.wrapWithBackupMetadata(
            new SchemaAndValue(Schema.BYTES_SCHEMA, value),
            topic, schemaInfo,
            detectSchemaType(schemaInfo.id()),
            isKey,
            (raw, refs, resolved) -> null,
            (t, k, parsed) -> t + (k ? "-key" : "-value")
        );
      } catch (Exception e) {
        log.warn("Failed to fetch schema metadata for key={}, "
            + "falling back to raw bytes", schemaInfo.id(), e);
      }
    }

    return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
  }

  private ParsedSchemaAndValue.SchemaInfo extractSchemaInfo(
      byte[] value, Headers headers) {
    try {
      SchemaId schemaId = new SchemaId(null);
      DualSchemaIdDeserializer dual = new DualSchemaIdDeserializer();
      dual.deserialize(null, isKey, headers, value, schemaId);
      if (schemaId.getId() != null || schemaId.getGuid() != null) {
        return new ParsedSchemaAndValue.SchemaInfo(
            null, schemaId.getId(), null, schemaId.getGuid());
      }
    } catch (Exception e) {
      log.debug("No schema ID found in value/headers, treating as raw bytes");
    }
    return null;
  }

  private byte[] restoreFromWrapper(
      String topic, Schema wrapperSchema, Struct wrapper) {
    try {
      Object data = wrapper.get(BackupWrapper.FIELD_DATA);
      byte[] rawBytes = toBytes(data);
      if (rawBytes == null) {
        return null;
      }

      String rawSchema = wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA);
      String schemaType = wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE);
      int originalId = wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID);

      if (rawSchema != null && schemaRegistry != null) {
        int newId = registerAndTranslateSchemaId(
            topic, wrapperSchema, wrapper,
            originalId, rawSchema, schemaType);
        if (newId != originalId) {
          rawBytes = rewriteSchemaId(rawBytes, newId);
        }
      }

      return rawBytes;
    } catch (Exception e) {
      throw new DataException(
          "Failed to restore byte data for topic " + topic, e);
    }
  }

  private int registerAndTranslateSchemaId(
      String topic, Schema wrapperSchema, Struct wrapper,
      int originalId, String rawSchema, String schemaType) {
    Integer cached = schemaIdTranslation.get(originalId);
    if (cached != null) {
      return cached;
    }
    try {
      BackupReferenceResolver.ParsedSchemaFactory schemaFactory =
          createSchemaFactory(schemaType);
      if (schemaFactory != null) {
        backupHelper.getReferenceResolver().resolveFromWrapper(
            wrapperSchema, wrapper, schemaFactory);
      }

      Optional<ParsedSchema> parsed = schemaRegistry.parseSchema(
          schemaType, rawSchema, Collections.emptyList());

      if (parsed.isPresent()) {
        String resolvedSubject = resolveSubject(topic, null, parsed.get());
        int newId = schemaRegistry.register(resolvedSubject, parsed.get());
        schemaIdTranslation.put(originalId, newId);
        log.info("Schema registered: subject={}, originalId={}, newId={}, type={}",
            resolvedSubject, originalId, newId, schemaType);
        return newId;
      }

      log.warn("Could not parse schema type={} for id={}, "
          + "using original ID", schemaType, originalId);
      return originalId;
    } catch (Exception e) {
      log.warn("Failed to register schema id={}, using original",
          originalId, e);
      return originalId;
    }
  }

  private String resolveSubject(
      String restoredTopic, String backupSubject, ParsedSchema schema) {
    return subjectNameStrategy.subjectName(restoredTopic, isKey, schema);
  }

  @SuppressWarnings("unchecked")
  private SubjectNameStrategy createSubjectNameStrategy(Map<String, ?> configs) {
    String strategyKey = isKey
        ? KEY_SUBJECT_NAME_STRATEGY : VALUE_SUBJECT_NAME_STRATEGY;
    Object strategyClass = configs.get(strategyKey);
    if (strategyClass != null) {
      try {
        Class<?> cls = strategyClass instanceof Class
            ? (Class<?>) strategyClass
            : Class.forName(strategyClass.toString());
        SubjectNameStrategy strategy =
            (SubjectNameStrategy) cls.getDeclaredConstructor().newInstance();
        if (schemaRegistry != null) {
          strategy.setSchemaRegistryClient(schemaRegistry);
        }
        return strategy;
      } catch (Exception e) {
        log.warn("Failed to create subject name strategy {}, "
            + "using TopicNameStrategy", strategyClass, e);
      }
    }
    TopicNameStrategy defaultStrategy = new TopicNameStrategy();
    if (schemaRegistry != null) {
      defaultStrategy.setSchemaRegistryClient(schemaRegistry);
    }
    return defaultStrategy;
  }

  private BackupReferenceResolver.ParsedSchemaFactory createSchemaFactory(
      String schemaType) {
    if (schemaType == null) {
      return null;
    }
    return (raw, refs, resolved) -> {
      Optional<ParsedSchema> parsed = schemaRegistry.parseSchema(
          schemaType, raw, refs);
      return parsed.orElse(null);
    };
  }

  private byte[] rewriteSchemaId(byte[] bytes, int newId) {
    if (bytes.length < 5 || bytes[0] != MAGIC_BYTE) {
      return bytes;
    }
    byte[] result = bytes.clone();
    ByteBuffer.wrap(result, 1, 4).putInt(newId);
    return result;
  }

  private byte[] toBytes(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) value;
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      return bytes;
    }
    throw new DataException("Expected byte[] or ByteBuffer but got "
        + value.getClass().getName());
  }

  private String detectSchemaType(int schemaId) {
    if (schemaRegistry == null) {
      return SchemaBackupConfig.TYPE_UNKNOWN;
    }
    try {
      String type = schemaRegistry.getSchemaById(schemaId).schemaType();
      if ("AVRO".equals(type)) {
        return SchemaBackupConfig.TYPE_AVRO;
      }
      if ("PROTOBUF".equals(type)) {
        return SchemaBackupConfig.TYPE_PROTOBUF;
      }
      if ("JSON".equals(type)) {
        return SchemaBackupConfig.TYPE_JSON_SCHEMA;
      }
      return type;
    } catch (Exception e) {
      log.debug("Could not determine schema type for id={}", schemaId, e);
      return SchemaBackupConfig.TYPE_UNKNOWN;
    }
  }
}
