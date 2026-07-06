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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SchemaBackupConfigTest {

  @Test
  public void isSrBackedType_avro_returnsTrue() {
    assertTrue(SchemaBackupConfig.isSrBackedType("AVRO"));
  }

  @Test
  public void isSrBackedType_protobuf_returnsTrue() {
    assertTrue(SchemaBackupConfig.isSrBackedType("PROTOBUF"));
  }

  @Test
  public void isSrBackedType_jsonSchema_returnsTrue() {
    assertTrue(SchemaBackupConfig.isSrBackedType("JSON_SCHEMA"));
  }

  @Test
  public void isSrBackedType_json_returnsTrue() {
    assertTrue(SchemaBackupConfig.isSrBackedType("JSON"));
  }

  @Test
  public void isSrBackedType_string_returnsFalse() {
    assertFalse(SchemaBackupConfig.isSrBackedType("STRING"));
  }

  @Test
  public void isSrBackedType_bytes_returnsFalse() {
    assertFalse(SchemaBackupConfig.isSrBackedType("BYTES"));
  }

  @Test
  public void isSrBackedType_null_returnsFalse() {
    assertFalse(SchemaBackupConfig.isSrBackedType(null));
  }

  @Test
  public void isSrBackedType_nonSrTypes_allReturnFalse() {
    assertFalse(SchemaBackupConfig.isSrBackedType("JSON_SCHEMALESS"));
    assertFalse(SchemaBackupConfig.isSrBackedType("JSON_EMBEDDED_SCHEMA"));
    assertFalse(SchemaBackupConfig.isSrBackedType("INT16"));
    assertFalse(SchemaBackupConfig.isSrBackedType("INT32"));
    assertFalse(SchemaBackupConfig.isSrBackedType("INT64"));
    assertFalse(SchemaBackupConfig.isSrBackedType("FLOAT32"));
    assertFalse(SchemaBackupConfig.isSrBackedType("FLOAT64"));
    assertFalse(SchemaBackupConfig.isSrBackedType("NONE"));
    assertFalse(SchemaBackupConfig.isSrBackedType("UNKNOWN"));
    assertFalse(SchemaBackupConfig.isSrBackedType(""));
    assertFalse(SchemaBackupConfig.isSrBackedType("avro"));
  }

  @Test
  public void testConstants_haveExpectedValues() {
    assertEquals("AVRO", SchemaBackupConfig.TYPE_AVRO);
    assertEquals("PROTOBUF", SchemaBackupConfig.TYPE_PROTOBUF);
    assertEquals("JSON_SCHEMA", SchemaBackupConfig.TYPE_JSON_SCHEMA);
    assertEquals("JSON", SchemaBackupConfig.TYPE_JSON);
    assertEquals("JSON_SCHEMALESS", SchemaBackupConfig.TYPE_JSON_SCHEMALESS);
    assertEquals("JSON_EMBEDDED_SCHEMA", SchemaBackupConfig.TYPE_JSON_EMBEDDED_SCHEMA);
    assertEquals("STRING", SchemaBackupConfig.TYPE_STRING);
    assertEquals("INT16", SchemaBackupConfig.TYPE_INT16);
    assertEquals("INT32", SchemaBackupConfig.TYPE_INT32);
    assertEquals("INT64", SchemaBackupConfig.TYPE_INT64);
    assertEquals("FLOAT32", SchemaBackupConfig.TYPE_FLOAT32);
    assertEquals("FLOAT64", SchemaBackupConfig.TYPE_FLOAT64);
    assertEquals("BYTES", SchemaBackupConfig.TYPE_BYTES);
    assertEquals("NONE", SchemaBackupConfig.TYPE_NONE);
    assertEquals("UNKNOWN", SchemaBackupConfig.TYPE_UNKNOWN);
    assertEquals("schema.backup.enabled",
        SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG);
    assertEquals(50, SchemaBackupConfig.MAX_REFERENCE_DEPTH);
  }
}
