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

package io.confluent.connect.schema.backup.api;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Backup Wrapper struct operations for Kafka Connect object storage connectors.
 *
 * <p>The Wrapper is a self-identifying Connect Struct that carries schema
 * metadata between converters and the envelope layer during backup and
 * restore. Converters build Wrappers in {@code toConnectData()} and
 * detect them in {@code fromConnectData()}. The envelope layer unwraps
 * them on backup and rebuilds them on restore.
 *
 * <p>For schema type tags, reference field names, and configuration
 * constants, see {@link SchemaBackupConfig}.
 */
public final class BackupWrapper {

  /**
   * Schema name used to identify Wrapper structs.
   * Detection key for {@link #isWrapper(Schema)}.
   */
  public static final String NAME = "io.confluent.connect.backup.Wrapper";

  // Wrapper struct field names
  public static final String FIELD_DATA = "data";
  public static final String FIELD_SCHEMA_ID = "schemaId";
  public static final String FIELD_SCHEMA_VERSION = "schemaVersion";
  public static final String FIELD_SCHEMA_TYPE = "schemaType";
  public static final String FIELD_SCHEMA_SUBJECT = "schemaSubject";
  public static final String FIELD_RAW_SCHEMA = "rawSchema";
  public static final String FIELD_REFERENCE_TREE = "referenceTree";
  public static final String FIELD_DIRECT_REFS = "directRefs";
  public static final String FIELD_FORMAT_VERSION = "formatVersion";

  public static final int FORMAT_VERSION = 1;

  private static final byte MAGIC_BYTE = 0x0;

  private BackupWrapper() {
  }

  /**
   * Builds the 8-field Wrapper Connect schema for a given data schema.
   *
   * @param dataSchema the original key or value schema, or null for
   *     tombstones (defaults to OPTIONAL_BYTES_SCHEMA)
   * @return a named struct schema with all Wrapper fields
   */
  public static Schema buildSchema(Schema dataSchema) {
    Schema safeDataSchema = dataSchema != null
        ? dataSchema : Schema.OPTIONAL_BYTES_SCHEMA;
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
        .field(FIELD_FORMAT_VERSION, Schema.INT32_SCHEMA)
        .build();
  }

  /**
   * Builds a Wrapper struct with all 8 fields populated.
   *
   * @param wrapperSchema the schema from {@link #buildSchema}
   * @param data the original key or value data
   * @param schemaId SR global schema ID
   * @param schemaVersion SR schema version, or null if unknown
   * @param schemaType schema type tag from {@link SchemaBackupConfig}
   * @param subject SR subject name
   * @param rawSchema pristine schema text from SR
   * @param referenceTreeJson reference tree JSON, or null if no refs
   * @param directRefsJson direct references JSON, or null if no refs
   * @return populated Wrapper struct
   */
  public static Struct buildWrapper(
      Schema wrapperSchema, Object data,
      int schemaId, Integer schemaVersion,
      String schemaType, String subject, String rawSchema,
      String referenceTreeJson, String directRefsJson) {
    Struct wrapper = new Struct(wrapperSchema);
    wrapper.put(FIELD_DATA, data);
    wrapper.put(FIELD_SCHEMA_ID, schemaId);
    wrapper.put(FIELD_SCHEMA_VERSION, schemaVersion);
    wrapper.put(FIELD_SCHEMA_TYPE, schemaType);
    wrapper.put(FIELD_SCHEMA_SUBJECT, subject);
    wrapper.put(FIELD_RAW_SCHEMA, rawSchema);
    wrapper.put(FIELD_REFERENCE_TREE, referenceTreeJson);
    wrapper.put(FIELD_DIRECT_REFS, directRefsJson);
    wrapper.put(FIELD_FORMAT_VERSION, FORMAT_VERSION);
    return wrapper;
  }

  /**
   * Detects whether a Connect schema is a BackupWrapper by its name.
   *
   * @param schema the schema to check
   * @return true if the schema name matches {@link #NAME}
   */
  public static boolean isWrapper(Schema schema) {
    return schema != null && NAME.equals(schema.name());
  }

  /**
   * Extracts the SR schema ID from Confluent wire format bytes.
   * The wire format is: {@code [0x00][4-byte big-endian schema ID][payload]}.
   *
   * @param value the raw Kafka record value bytes
   * @return the schema ID, or null if the bytes are not in wire format
   */
  public static Integer extractSchemaId(byte[] value) {
    if (value == null || value.length < 5 || value[0] != MAGIC_BYTE) {
      return null;
    }
    return ByteBuffer.wrap(value, 1, 4).getInt();
  }
}
