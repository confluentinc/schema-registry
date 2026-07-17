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

package io.confluent.connect.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class AvroConverterBackupTest {

  private static final String TOPIC = "test-topic";
  private static final String FIELD_NAME = "name";
  private static final String FIELD_COUNT = "count";
  private static final String SCHEMA_TYPE = "AVRO";

  private final SchemaRegistryClient schemaRegistry;
  private final AvroConverter converter;
  private final AvroConverter plainConverter;

  public AvroConverterBackupTest() {
    schemaRegistry = new MockSchemaRegistryClient();
    converter = new AvroConverter(schemaRegistry);
    plainConverter = new AvroConverter(schemaRegistry);
  }

  @Before
  public void setUp() {
    Map<String, Object> backupConfig = new HashMap<>();
    backupConfig.put("schema.registry.url", "http://fake-url");
    backupConfig.put("schema.backup.enabled", "true");
    converter.configure(backupConfig, false);

    plainConverter.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"),
        false);
  }

  @Test
  public void testBackupToConnectDataWrapsMetadata() {
    Schema schema = SchemaBuilder.struct()
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema)
        .put(FIELD_NAME, "Alice")
        .put("age", 30);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    assertNotNull(result);
    assertNotNull(result.schema());
    assertEquals(BackupWrapper.NAME, result.schema().name());

    Struct wrapper = (Struct) result.value();
    assertNotNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertEquals(SCHEMA_TYPE, wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertNotNull(wrapper.get(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testBackupFromConnectDataRestores() {
    Schema schema = SchemaBuilder.struct()
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "Bob");

    // Serialize to get valid wire-format bytes
    byte[] original = plainConverter.fromConnectData(TOPIC, schema, value);
    assertNotNull(original);
    assertTrue(original.length >= 5);

    // Wrap via backup toConnectData
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, original);
    assertEquals(BackupWrapper.NAME, wrapped.schema().name());

    // Restore via backup fromConnectData
    byte[] restored = converter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    assertNotNull(restored);
    // Wire format: magic byte 0x00 followed by 4-byte schema ID
    assertEquals(0x00, restored[0]);
  }

  @Test
  public void testBackupRoundTrip() {
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field(FIELD_COUNT, Schema.INT32_SCHEMA)
        .build();
    Struct original = new Struct(schema)
        .put("id", "order-1")
        .put(FIELD_COUNT, 42);

    // Serialize with plain converter
    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, original);

    // Backup path: toConnectData wraps, fromConnectData restores
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    // Deserialize restored bytes and compare values
    SchemaAndValue restoredData = plainConverter.toConnectData(TOPIC, restoredBytes);
    Struct restored = (Struct) restoredData.value();
    assertEquals("order-1", restored.getString("id"));
    assertEquals(Integer.valueOf(42), restored.getInt32(FIELD_COUNT));
  }

  @Test
  public void testBackupDisabledNoWrapping() {
    Schema schema = Schema.STRING_SCHEMA;
    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, "hello");

    // Use plainConverter (backup disabled)
    SchemaAndValue result = plainConverter.toConnectData(TOPIC, serialized);

    assertNotNull(result.schema());
    assertNotEquals(BackupWrapper.NAME, result.schema().name());
  }

  @Test
  public void testBackupNullValue() {
    SchemaAndValue result = converter.toConnectData(TOPIC, null);
    assertNull(result.schema());
    assertNull(result.value());
  }

  @Test
  public void testBackupPrimitiveType() {
    byte[] serialized = plainConverter.fromConnectData(
        TOPIC, Schema.STRING_SCHEMA, "test-value");

    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    assertEquals(BackupWrapper.NAME, result.schema().name());
    Struct wrapper = (Struct) result.value();
    assertEquals(SCHEMA_TYPE, wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
  }

  @Test
  public void testBackupWrapperFields() {
    Schema schema = SchemaBuilder.struct()
        .field("x", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("x", 1);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    Struct wrapper = (Struct) result.value();
    assertEquals(SCHEMA_TYPE, wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    // Subject should follow TopicNameStrategy: topic-value
    assertTrue(wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT)
        .contains(TOPIC));
  }

  @Test
  public void testBackupSchemaIdExtracted() {
    byte[] serialized = plainConverter.fromConnectData(
        TOPIC, Schema.INT32_SCHEMA, 42);

    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);
    Struct wrapper = (Struct) result.value();

    int wrappedId = wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID);
    // Schema ID extracted from wire format should match what SR assigned
    assertTrue(wrappedId > 0);
  }

  @Test
  public void testBackupNonWrapperSchemaNormalSerialization() {
    Schema schema = SchemaBuilder.struct()
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "direct");

    // Even in backup mode, non-wrapper schemas serialize normally
    byte[] result = converter.fromConnectData(TOPIC, schema, value);
    assertNotNull(result);
    assertTrue(result.length > 5);
    assertEquals(0x00, result[0]);
  }

  @Test
  public void testBackupRestoreMissingDataField() {
    // Build a struct with wrapper name but missing FIELD_DATA
    Schema badSchema = SchemaBuilder.struct()
        .name(BackupWrapper.NAME)
        .field(BackupWrapper.FIELD_SCHEMA_ID, Schema.INT32_SCHEMA)
        .build();
    Struct badWrapper = new Struct(badSchema).put(BackupWrapper.FIELD_SCHEMA_ID, 1);

    try {
      converter.fromConnectData(TOPIC, badSchema, badWrapper);
      fail("Expected DataException");
    } catch (DataException e) {
      assertTrue(e.getMessage().contains("data")
          || e.getMessage().contains("restore")
          || e.getMessage().contains("Failed"));
    }
  }

  @Test
  public void testBackupRoundTripBytesExact() {
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field(FIELD_COUNT, Schema.INT32_SCHEMA)
        .build();
    Struct original = new Struct(schema)
        .put("id", "exact-match")
        .put(FIELD_COUNT, 100);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, original);

    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    // Restored bytes should produce identical data
    assertArrayEquals(originalBytes, restoredBytes);
  }

  @Test
  public void testBackupSchemaVersionPresent() {
    Schema schema = SchemaBuilder.struct()
        .field("x", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("x", 42);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    Struct wrapper = (Struct) result.value();
    // Schema version should be populated for registered schemas
    assertNotNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
  }

  @Test
  public void testBackupRestoreNullRawSchemaFallback() {
    Schema schema = SchemaBuilder.struct()
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "fallback");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);

    // Rebuild wrapper with null rawSchema to exercise fallback branch
    Struct original = (Struct) wrapped.value();
    Schema wrapperSchema = wrapped.schema();
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        original.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        original.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION),
        original.getString(BackupWrapper.FIELD_SCHEMA_TYPE),
        original.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        null, null, null);
    Struct modifiedWrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.get(BackupWrapper.FIELD_DATA), fields);

    byte[] restored = converter.fromConnectData(TOPIC, wrapperSchema, modifiedWrapper);
    assertNotNull(restored);
    assertEquals(0x00, restored[0]);
  }
}
