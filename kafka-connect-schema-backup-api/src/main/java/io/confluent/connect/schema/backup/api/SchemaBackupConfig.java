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
 * Constants and schema type definitions for backup/restore in object storage connectors.
 */
public final class SchemaBackupConfig {

  public static final String TYPE_AVRO = "AVRO";
  public static final String TYPE_PROTOBUF = "PROTOBUF";
  public static final String TYPE_JSON_SCHEMA = "JSON_SCHEMA";
  public static final String TYPE_JSON = "JSON";
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

  public static final String SCHEMA_BACKUP_ENABLED_CONFIG = "schema.backup.enabled";

  public static final String REF_FIELD_SUBJECT = "subject";
  public static final String REF_FIELD_VERSION = "version";
  public static final String REF_FIELD_GLOBAL_ID = "globalId";
  public static final String REF_FIELD_SCHEMA_TYPE = "schemaType";
  public static final String REF_FIELD_SCHEMA = "schema";
  public static final String REF_FIELD_REFERENCES = "references";
  public static final String REF_FIELD_NAME = "name";

  public static final int MAX_REFERENCE_DEPTH = 50;

  private SchemaBackupConfig() {
  }

  public static boolean isSrBackedType(String schemaType) {
    return TYPE_AVRO.equals(schemaType)
        || TYPE_PROTOBUF.equals(schemaType)
        || TYPE_JSON_SCHEMA.equals(schemaType)
        || TYPE_JSON.equals(schemaType);
  }
}
