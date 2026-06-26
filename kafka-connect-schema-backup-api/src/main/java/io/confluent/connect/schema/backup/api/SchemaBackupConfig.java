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

/**
 * Configuration constants and schema type definitions for backup/restore
 * in Kafka Connect object storage connectors (S3, GCS, Azure).
 *
 * <p>Defines the schema type tags used to identify converter types,
 * the reference tree JSON field names shared across all backup layers,
 * and the configuration key for enabling schema backup in converters.
 *
 * <p>This is the single source of truth for these constants.
 * No other class should duplicate these values.
 */
public final class SchemaBackupConfig {

  // Schema type tags for SR-backed converters
  public static final String TYPE_AVRO = "AVRO";
  public static final String TYPE_PROTOBUF = "PROTOBUF";
  public static final String TYPE_JSON_SCHEMA = "JSON_SCHEMA";
  public static final String TYPE_JSON = "JSON";

  // Schema type tags for non-SR converters
  public static final String TYPE_JSON_SCHEMALESS = "JSON_SCHEMALESS";
  public static final String TYPE_JSON_EMBEDDED_SCHEMA = "JSON_EMBEDDED_SCHEMA";
  public static final String TYPE_STRING = "STRING";
  public static final String TYPE_INT16 = "INT16";
  public static final String TYPE_INT32 = "INT32";
  public static final String TYPE_INT64 = "INT64";
  public static final String TYPE_FLOAT32 = "FLOAT32";
  public static final String TYPE_FLOAT64 = "FLOAT64";
  public static final String TYPE_BYTES = "BYTES";
  public static final String TYPE_NONE = "NONE";
  public static final String TYPE_UNKNOWN = "UNKNOWN";

  // Converter config key for enabling schema backup.
  // User sets: value.converter.schema.backup.enabled=true
  public static final String SCHEMA_BACKUP_ENABLED_CONFIG = "schema.backup.enabled";

  // Reference tree JSON field names — shared contract between
  // BackupSchemaFetcher, BackupReferenceResolver (backup-core),
  // BackupWrapperRebuilder (object-store-source),
  // EnvelopeTransformer, BackupReferenceParser (storage-common).
  public static final String REF_FIELD_SUBJECT = "subject";
  public static final String REF_FIELD_VERSION = "version";
  public static final String REF_FIELD_GLOBAL_ID = "globalId";
  public static final String REF_FIELD_SCHEMA_TYPE = "schemaType";
  public static final String REF_FIELD_SCHEMA = "schema";
  public static final String REF_FIELD_REFERENCES = "references";
  public static final String REF_FIELD_NAME = "name";

  // Maximum depth for recursive reference traversal.
  // Protects against circular references and deeply nested chains.
  public static final int MAX_REFERENCE_DEPTH = 50;

  private SchemaBackupConfig() {
  }

  /**
   * Returns whether a schema type is Schema Registry-backed
   * (requires schema metadata for pristine backup/restore).
   *
   * @param schemaType the schema type tag
   * @return true if Avro, Protobuf, JSON Schema, or JSON (SR-native)
   */
  public static boolean isSrBackedType(String schemaType) {
    return TYPE_AVRO.equals(schemaType)
        || TYPE_PROTOBUF.equals(schemaType)
        || TYPE_JSON_SCHEMA.equals(schemaType)
        || TYPE_JSON.equals(schemaType);
  }
}
