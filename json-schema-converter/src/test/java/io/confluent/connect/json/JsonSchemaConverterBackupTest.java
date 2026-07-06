/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class JsonSchemaConverterBackupTest {

  private static final String TOPIC = "test-topic";

  private final SchemaRegistryClient schemaRegistry;
  private final JsonSchemaConverter converter;
  private final JsonSchemaConverter plainConverter;

  public JsonSchemaConverterBackupTest() {
    schemaRegistry = new MockSchemaRegistryClient(
        ImmutableList.of(new JsonSchemaProvider()));
    converter = new JsonSchemaConverter(schemaRegistry);
    plainConverter = new JsonSchemaConverter(schemaRegistry);
  }

  @Before
  public void setUp() {
    Map<String, Object> backupConfig = new HashMap<>();
    backupConfig.put("schema.registry.url", "http://fake-url");
    backupConfig.put("schema.backup.enabled", "true");
    converter.configure(backupConfig, false);

    Map<String, String> plainConfig = new HashMap<>();
    plainConfig.put("schema.registry.url", "http://fake-url");
    plainConverter.configure(plainConfig, false);
  }

  @Test
  public void testBackupMode_toConnectData_wrapsWithMetadata() {
    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema)
        .put("name", "Alice")
        .put("age", 30);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    assertNotNull(result);
    assertNotNull(result.schema());
    assertEquals(BackupWrapper.NAME, result.schema().name());

    Struct wrapper = (Struct) result.value();
    assertNotNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertEquals("JSON_SCHEMA",
        wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertNotNull(wrapper.get(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testBackupMode_fromConnectData_restoresFromWrapper() {
    Schema schema = SchemaBuilder.struct()
        .field("text", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("text", "hello");

    byte[] original = plainConverter.fromConnectData(TOPIC, schema, value);
    assertNotNull(original);

    SchemaAndValue wrapped = converter.toConnectData(TOPIC, original);
    assertEquals(BackupWrapper.NAME, wrapped.schema().name());

    byte[] restored = converter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    assertNotNull(restored);
    assertEquals(0x00, restored[0]);
  }

  @Test
  public void testBackupMode_roundTrip() {
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("count", Schema.INT32_SCHEMA)
        .build();
    Struct original = new Struct(schema)
        .put("id", "item-1")
        .put("count", 5);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, original);

    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    SchemaAndValue restoredData = plainConverter.toConnectData(
        TOPIC, restoredBytes);
    Struct restored = (Struct) restoredData.value();
    assertEquals("item-1", restored.getString("id"));
    assertEquals(Integer.valueOf(5), restored.getInt32("count"));
  }

  @Test
  public void testBackupDisabled_toConnectData_noWrapping() {
    Schema schema = SchemaBuilder.struct()
        .field("x", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("x", 1);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = plainConverter.toConnectData(TOPIC, serialized);

    assertNotNull(result.schema());
    assertNotEquals(BackupWrapper.NAME, result.schema().name());
  }

  @Test
  public void testBackupMode_nullValue_returnsNull() {
    SchemaAndValue result = converter.toConnectData(TOPIC, null);
    assertNull(result.schema());
    assertNull(result.value());
  }

  @Test
  public void testBackupMode_primitiveType_wrapsCorrectly() {
    Schema schema = SchemaBuilder.struct()
        .field("val", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("val", "test");

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    assertEquals(BackupWrapper.NAME, result.schema().name());
    Struct wrapper = (Struct) result.value();
    assertEquals("JSON_SCHEMA",
        wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
  }

  @Test
  public void testBackupMode_wrapperFields_correctValues() {
    Schema schema = SchemaBuilder.struct()
        .field("data", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("data", "check");

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    Struct wrapper = (Struct) result.value();
    assertEquals("JSON_SCHEMA",
        wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertTrue(wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT)
        .contains(TOPIC));
  }

  @Test
  public void testBackupMode_schemaIdExtracted() {
    Schema schema = SchemaBuilder.struct()
        .field("n", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("n", 7);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    Struct wrapper = (Struct) result.value();
    int wrappedId = wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID);
    assertTrue(wrappedId > 0);
  }
}
