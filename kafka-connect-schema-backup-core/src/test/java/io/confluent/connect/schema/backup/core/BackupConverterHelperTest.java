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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class BackupConverterHelperTest {

  private static final String TOPIC = "test-topic";
  private static final String VALUE_SUFFIX = "-value";
  private static final String ADDRESS_SUBJECT = "com.example.Address";

  private static final String USER_SCHEMA =
      "{\"type\":\"record\",\"name\":\"User\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"name\",\"type\":\"string\"},"
      + "{\"name\":\"age\",\"type\":\"int\"}"
      + "]}";

  private static final String ADDRESS_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Address\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"street\",\"type\":\"string\"},"
      + "{\"name\":\"city\",\"type\":\"string\"}"
      + "]}";

  private static final String ORDER_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Order\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},"
      + "{\"name\":\"addr\",\"type\":\"Address\"}"
      + "]}";

  private static final BackupReferenceResolver.ParsedSchemaFactory AVRO_FACTORY =
      (raw, refs, resolved) -> !refs.isEmpty()
          ? new AvroSchema(raw, refs, resolved, null)
          : new AvroSchema(raw);

  private static final BackupConverterHelper.SubjectNameComputer SUBJECT_COMPUTER =
      (topic, isKey, schema) -> topic + (isKey ? "-key" : "-value");

  private SchemaRegistryClient schemaRegistry;
  private Map<Schema, Schema> wrapperSchemaCache;
  private BackupConverterHelper helper;

  @Before
  public void setUp() {
    schemaRegistry = new MockSchemaRegistryClient();
    wrapperSchemaCache = new HashMap<>();
    helper = new BackupConverterHelper(schemaRegistry, wrapperSchemaCache);
  }

  @Test
  public void testIsBackupEnabledTrue() {
    Map<String, String> config = new HashMap<>();
    config.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, "true");
    assertTrue(BackupConverterHelper.isBackupEnabled(config));
  }

  @Test
  public void testIsBackupEnabledFalse() {
    Map<String, String> config = new HashMap<>();
    config.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, "false");
    assertFalse(BackupConverterHelper.isBackupEnabled(config));
  }

  @Test
  public void testIsBackupEnabledUppercaseTrue() {
    Map<String, String> config = new HashMap<>();
    config.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, "TRUE");
    assertTrue(BackupConverterHelper.isBackupEnabled(config));
  }

  @Test
  public void testIsBackupEnabledMixedCase() {
    Map<String, String> config = new HashMap<>();
    config.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, "True");
    assertTrue(BackupConverterHelper.isBackupEnabled(config));
  }

  @Test
  public void testIsBackupEnabledMissingKey() {
    assertFalse(BackupConverterHelper.isBackupEnabled(Collections.emptyMap()));
  }

  @Test
  public void testIsBackupEnabledNullValue() {
    Map<String, String> config = new HashMap<>();
    config.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, null);
    assertFalse(BackupConverterHelper.isBackupEnabled(config));
  }

  @Test
  public void testIsBackupEnabledEmptyString() {
    Map<String, String> config = new HashMap<>();
    config.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, "");
    assertFalse(BackupConverterHelper.isBackupEnabled(config));
  }

  @Test
  public void testIsBackupEnabledNonBoolean() {
    Map<String, String> config = new HashMap<>();
    config.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, "yes");
    assertFalse(BackupConverterHelper.isBackupEnabled(config));
  }

  @Test
  public void testGetReferenceResolver() {
    assertNotNull(helper.getReferenceResolver());
  }

  @Test
  public void testGetReferenceResolverSameInstance() {
    assertSame(helper.getReferenceResolver(), helper.getReferenceResolver());
  }

  @Test
  public void testWrapSimpleSchema()
      throws Exception {
    AvroSchema avro = new AvroSchema(USER_SCHEMA);
    int schemaId = schemaRegistry.register(TOPIC + VALUE_SUFFIX, avro);

    Schema connectSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(connectSchema)
        .put("name", "Alice")
        .put("age", 30);

    SchemaAndValue original = new SchemaAndValue(connectSchema, value);
    SchemaAndValue wrapped = helper.wrapWithBackupMetadata(
        original, TOPIC, schemaId,
        SchemaBackupConfig.TYPE_AVRO, false,
        AVRO_FACTORY, SUBJECT_COMPUTER);

    // Verify it's a BackupWrapper
    assertNotNull(wrapped);
    assertNotNull(wrapped.schema());
    assertEquals(BackupWrapper.NAME, wrapped.schema().name());

    // Verify wrapper fields
    Struct wrapper = (Struct) wrapped.value();
    assertEquals(schemaId, (int) wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertEquals("AVRO", wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertEquals(TOPIC + VALUE_SUFFIX,
        wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));

    // Verify data is preserved
    assertNotNull(wrapper.get(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testWrapWithReferences()
      throws Exception {
    // Register the referenced schema first
    AvroSchema addrSchema = new AvroSchema(ADDRESS_SCHEMA);
    schemaRegistry.register(ADDRESS_SUBJECT, addrSchema);

    // Register the main schema with a reference
    SchemaReference ref = new SchemaReference(
        ADDRESS_SUBJECT, ADDRESS_SUBJECT, 1);
    AvroSchema orderSchema = new AvroSchema(
        ORDER_SCHEMA,
        Collections.singletonList(ref),
        Collections.singletonMap(ADDRESS_SUBJECT, ADDRESS_SCHEMA),
        null);
    int orderId = schemaRegistry.register(TOPIC + VALUE_SUFFIX, orderSchema);

    Schema connectSchema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .build();
    SchemaAndValue original = new SchemaAndValue(
        connectSchema, new Struct(connectSchema).put("id", "o1"));

    SchemaAndValue wrapped = helper.wrapWithBackupMetadata(
        original, TOPIC, orderId,
        SchemaBackupConfig.TYPE_AVRO, false,
        AVRO_FACTORY, SUBJECT_COMPUTER);

    Struct wrapper = (Struct) wrapped.value();
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_REFERENCE_TREE));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_DIRECT_REFS));
  }

  @Test
  public void testWrapNoReferences()
      throws Exception {
    AvroSchema avro = new AvroSchema(USER_SCHEMA);
    int schemaId = schemaRegistry.register(TOPIC + VALUE_SUFFIX, avro);

    SchemaAndValue original = new SchemaAndValue(
        Schema.STRING_SCHEMA, "test");

    SchemaAndValue wrapped = helper.wrapWithBackupMetadata(
        original, TOPIC, schemaId,
        SchemaBackupConfig.TYPE_AVRO, false,
        AVRO_FACTORY, SUBJECT_COMPUTER);

    Struct wrapper = (Struct) wrapped.value();
    assertNull(wrapper.getString(BackupWrapper.FIELD_REFERENCE_TREE));
    assertNull(wrapper.getString(BackupWrapper.FIELD_DIRECT_REFS));
  }

  @Test
  public void testWrapNullSchema()
      throws Exception {
    AvroSchema avro = new AvroSchema(USER_SCHEMA);
    int schemaId = schemaRegistry.register(TOPIC + VALUE_SUFFIX, avro);

    SchemaAndValue original = new SchemaAndValue(null, null);

    SchemaAndValue wrapped = helper.wrapWithBackupMetadata(
        original, TOPIC, schemaId,
        SchemaBackupConfig.TYPE_AVRO, false,
        AVRO_FACTORY, SUBJECT_COMPUTER);

    assertNotNull(wrapped.schema());
    assertEquals(BackupWrapper.NAME, wrapped.schema().name());
    // Data field schema defaults to OPTIONAL_BYTES_SCHEMA for null original schema
    assertNotNull(wrapped.schema().field(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testWrapCachedSchema()
      throws Exception {
    AvroSchema avro = new AvroSchema(USER_SCHEMA);
    int schemaId = schemaRegistry.register(TOPIC + VALUE_SUFFIX, avro);

    Schema connectSchema = Schema.STRING_SCHEMA;
    SchemaAndValue original = new SchemaAndValue(connectSchema, "test");

    // Wrap twice with the same original schema
    helper.wrapWithBackupMetadata(
        original, TOPIC, schemaId,
        SchemaBackupConfig.TYPE_AVRO, false,
        AVRO_FACTORY, SUBJECT_COMPUTER);
    helper.wrapWithBackupMetadata(
        original, TOPIC, schemaId,
        SchemaBackupConfig.TYPE_AVRO, false,
        AVRO_FACTORY, SUBJECT_COMPUTER);

    // Cache should have exactly one entry for STRING_SCHEMA
    assertEquals(1, wrapperSchemaCache.size());
    assertTrue(wrapperSchemaCache.containsKey(connectSchema));
  }
}
