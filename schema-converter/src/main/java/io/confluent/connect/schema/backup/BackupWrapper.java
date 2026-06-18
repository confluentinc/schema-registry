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

package io.confluent.connect.schema.backup;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Single source of truth for the backup Wrapper struct contract.
 *
 * <p>The Wrapper carries schema metadata between converters and the envelope layer
 * during backup and restore. Converters build Wrappers in {@code toConnectData()}
 * and detect them in {@code fromConnectData()}. The envelope layer unwraps them
 * on backup and rebuilds them on restore.
 */
public final class BackupWrapper {

  public static final String NAME = "io.confluent.connect.backup.Wrapper";

  public static final String FIELD_DATA = "data";
  public static final String FIELD_SCHEMA_ID = "schemaId";
  public static final String FIELD_SCHEMA_VERSION = "schemaVersion";
  public static final String FIELD_SCHEMA_TYPE = "schemaType";
  public static final String FIELD_SCHEMA_SUBJECT = "schemaSubject";
  public static final String FIELD_RAW_SCHEMA = "rawSchema";
  public static final String FIELD_REFERENCE_TREE = "referenceTree";
  public static final String FIELD_DIRECT_REFS = "directRefs";

  /**
   * Converter config key that enables schema backup metadata wrapping.
   * Set by the connector via taskConfigs() propagation
   * (e.g., {@code value.converter.schema.backup.enabled=true}).
   *
   * <p>When enabled, the converter captures schema ID, version, subject,
   * raw schema text, and reference metadata in a Wrapper struct so the
   * envelope layer can back them up to object storage.
   */
  public static final String SCHEMA_BACKUP_ENABLED_CONFIG = "schema.backup.enabled";

  // Schema type tags — used in wrapWithBackupMetadata() across converters.
  // Must match BackupEnvelope.TYPE_* in storage-common (cross-repo contract).
  public static final String SCHEMA_TYPE_AVRO = "AVRO";
  public static final String SCHEMA_TYPE_PROTOBUF = "PROTOBUF";
  public static final String SCHEMA_TYPE_JSON_SCHEMA = "JSON_SCHEMA";

  public static final int MAX_REFERENCE_DEPTH = 50;

  // Reference tree JSON field names (cross-repo contract between
  // BackupSchemaFetcher, BackupReferenceResolver, BackupWrapperRebuilder,
  // EnvelopeTransformer, BackupReferenceParser).
  // Must match BackupEnvelope.REF_FIELD_* in storage-common.
  public static final String REF_FIELD_SUBJECT = "subject";
  public static final String REF_FIELD_VERSION = "version";
  public static final String REF_FIELD_GLOBAL_ID = "globalId";
  public static final String REF_FIELD_SCHEMA_TYPE = "schemaType";
  public static final String REF_FIELD_SCHEMA = "schema";
  public static final String REF_FIELD_REFERENCES = "references";
  public static final String REF_FIELD_NAME = "name";

  private static final byte MAGIC_BYTE = 0x0;

  private BackupWrapper() {
  }

  public static Schema buildSchema(Schema dataSchema) {
    Schema safeDataSchema = dataSchema != null ? dataSchema : Schema.OPTIONAL_BYTES_SCHEMA;
    return SchemaBuilder.struct()
        .name(NAME)
        .field(FIELD_DATA, safeDataSchema)
        .field(FIELD_SCHEMA_ID, Schema.INT32_SCHEMA)
        .field(FIELD_SCHEMA_VERSION, Schema.OPTIONAL_INT32_SCHEMA)
        .field(FIELD_SCHEMA_TYPE, Schema.STRING_SCHEMA)
        .field(FIELD_SCHEMA_SUBJECT, Schema.STRING_SCHEMA)
        .field(FIELD_RAW_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .field(FIELD_REFERENCE_TREE, Schema.OPTIONAL_STRING_SCHEMA)
        .field(FIELD_DIRECT_REFS, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
  }

  public static Struct buildWrapper(
      Schema wrapperSchema,
      Object data,
      int schemaId,
      Integer schemaVersion,
      String schemaType,
      String subject,
      String rawSchema,
      String referenceTreeJson,
      String directRefsJson) {
    Struct wrapper = new Struct(wrapperSchema);
    wrapper.put(FIELD_DATA, data);
    wrapper.put(FIELD_SCHEMA_ID, schemaId);
    wrapper.put(FIELD_SCHEMA_VERSION, schemaVersion);
    wrapper.put(FIELD_SCHEMA_TYPE, schemaType);
    wrapper.put(FIELD_SCHEMA_SUBJECT, subject);
    wrapper.put(FIELD_RAW_SCHEMA, rawSchema);
    wrapper.put(FIELD_REFERENCE_TREE, referenceTreeJson);
    wrapper.put(FIELD_DIRECT_REFS, directRefsJson);
    return wrapper;
  }

  public static boolean isWrapper(Schema schema) {
    return schema != null && NAME.equals(schema.name());
  }

  public static Integer extractSchemaId(byte[] value) {
    if (value == null || value.length < 5 || value[0] != MAGIC_BYTE) {
      return null;
    }
    return ByteBuffer.wrap(value, 1, 4).getInt();
  }
}
