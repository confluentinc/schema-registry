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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BackupWrapperTest {

  @Test
  public void testBuildSchemaWithDataSchema() {
    Schema dataSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    assertEquals(BackupWrapper.NAME, wrapperSchema.name());
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_DATA));
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_SCHEMA_ID));
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_SCHEMA_VERSION));
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_RAW_SCHEMA));
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_REFERENCE_TREE));
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_DIRECT_REFS));
  }

  @Test
  public void testBuildSchemaWithNullDataSchema() {
    Schema wrapperSchema = BackupWrapper.buildSchema(null);
    assertNotNull(wrapperSchema);
    assertEquals(BackupWrapper.NAME, wrapperSchema.name());
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testBuildWrapperAllFields() {
    Schema dataSchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA).build();
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    Struct data = new Struct(dataSchema).put("id", 42);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        100, 1, "AVRO", "test-value", "{}", "{\"tree\":{}}", "[{\"name\":\"ref\"}]");
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, data, fields);
    assertEquals(data, wrapper.get(BackupWrapper.FIELD_DATA));
    assertEquals(100, (int) wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertEquals(1, (int) wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
    assertEquals("AVRO", wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertEquals("test-value", wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertEquals("{}", wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertEquals("{\"tree\":{}}", wrapper.getString(BackupWrapper.FIELD_REFERENCE_TREE));
    assertEquals("[{\"name\":\"ref\"}]", wrapper.getString(BackupWrapper.FIELD_DIRECT_REFS));
  }

  @Test
  public void testBuildWrapperNullOptionalFields() {
    Schema dataSchema = Schema.STRING_SCHEMA;
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        1, null, "STRING", "test-key", null, null, null);
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, "hello", fields);
    assertNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
    assertNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertNull(wrapper.getString(BackupWrapper.FIELD_REFERENCE_TREE));
    assertNull(wrapper.getString(BackupWrapper.FIELD_DIRECT_REFS));
  }

  @Test
  public void testIsWrapperTrue() {
    Schema wrapperSchema = BackupWrapper.buildSchema(Schema.STRING_SCHEMA);
    assertTrue(BackupWrapper.isWrapper(wrapperSchema));
  }

  @Test
  public void testIsWrapperFalseNullSchema() {
    assertFalse(BackupWrapper.isWrapper(null));
  }

  @Test
  public void testIsWrapperFalseWrongName() {
    Schema notWrapper = SchemaBuilder.struct()
        .name("com.example.SomeStruct")
        .field("data", Schema.STRING_SCHEMA).build();
    assertFalse(BackupWrapper.isWrapper(notWrapper));
  }

  @Test
  public void testIsWrapperFalseNoName() {
    Schema noName = SchemaBuilder.struct()
        .field("data", Schema.STRING_SCHEMA).build();
    assertFalse(BackupWrapper.isWrapper(noName));
  }

  @Test
  public void testExtractSchemaIdValid() {
    byte[] wire = new byte[10];
    wire[0] = 0x00;
    ByteBuffer.wrap(wire, 1, 4).putInt(42);
    assertEquals(Integer.valueOf(42), BackupWrapper.extractSchemaId(wire));
  }

  @Test
  public void testExtractSchemaIdNull() {
    assertNull(BackupWrapper.extractSchemaId(null));
  }

  @Test
  public void testExtractSchemaIdTooShort() {
    assertNull(BackupWrapper.extractSchemaId(new byte[]{0x00, 0x01}));
  }

  @Test
  public void testExtractSchemaIdWrongMagicByte() {
    byte[] wire = new byte[]{0x01, 0x00, 0x00, 0x00, 0x2A};
    assertNull(BackupWrapper.extractSchemaId(wire));
  }

  @Test
  public void testExtractSchemaIdEmptyArray() {
    assertNull(BackupWrapper.extractSchemaId(new byte[0]));
  }

  @Test
  public void testExtractSchemaIdFromHeaderValid() {
    org.apache.kafka.common.header.internals.RecordHeaders headers =
        new org.apache.kafka.common.header.internals.RecordHeaders();
    byte[] headerValue = new byte[]{0x00, 0x00, 0x00, 0x00, 0x2A};
    headers.add("__value_schema_id", headerValue);
    assertEquals(Integer.valueOf(42),
        BackupWrapper.extractSchemaIdFromHeader(headers, false));
  }

  @Test
  public void testExtractSchemaIdFromHeaderKey() {
    org.apache.kafka.common.header.internals.RecordHeaders headers =
        new org.apache.kafka.common.header.internals.RecordHeaders();
    byte[] headerValue = new byte[]{0x00, 0x00, 0x00, 0x00, 0x07};
    headers.add("__key_schema_id", headerValue);
    assertEquals(Integer.valueOf(7),
        BackupWrapper.extractSchemaIdFromHeader(headers, true));
  }

  @Test
  public void testExtractSchemaIdFromHeaderNull() {
    assertNull(BackupWrapper.extractSchemaIdFromHeader(null, false));
  }

  @Test
  public void testExtractSchemaIdFromHeaderMissing() {
    org.apache.kafka.common.header.internals.RecordHeaders headers =
        new org.apache.kafka.common.header.internals.RecordHeaders();
    assertNull(BackupWrapper.extractSchemaIdFromHeader(headers, false));
  }

  @Test
  public void testExtractSchemaIdFromHeaderGuidReturnsNull() {
    org.apache.kafka.common.header.internals.RecordHeaders headers =
        new org.apache.kafka.common.header.internals.RecordHeaders();
    byte[] guidHeader = new byte[17];
    guidHeader[0] = 0x01;
    headers.add("__value_schema_id", guidHeader);
    assertNull(BackupWrapper.extractSchemaIdFromHeader(headers, false));
  }

  @Test
  public void testRemoveSchemaIdHeaders() {
    org.apache.kafka.common.header.internals.RecordHeaders headers =
        new org.apache.kafka.common.header.internals.RecordHeaders();
    headers.add("__key_schema_id", new byte[]{0x00, 0x00, 0x00, 0x00, 0x01});
    headers.add("__value_schema_id", new byte[]{0x00, 0x00, 0x00, 0x00, 0x02});
    headers.add("other-header", "keep".getBytes());
    BackupWrapper.removeSchemaIdHeaders(headers, true);
    assertNull(headers.lastHeader("__key_schema_id"));
    assertNotNull(headers.lastHeader("__value_schema_id"));
    BackupWrapper.removeSchemaIdHeaders(headers, false);
    assertNull(headers.lastHeader("__value_schema_id"));
    assertNotNull(headers.lastHeader("other-header"));
  }

  @Test
  public void testExtractSchemaGuidFromHeaderValid() {
    RecordHeaders headers = new RecordHeaders();
    UUID testGuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    byte[] guidHeader = new byte[17];
    guidHeader[0] = 0x01;
    ByteBuffer buf = ByteBuffer.wrap(guidHeader, 1, 16);
    buf.putLong(testGuid.getMostSignificantBits());
    buf.putLong(testGuid.getLeastSignificantBits());
    headers.add("__value_schema_id", guidHeader);
    assertEquals("550e8400-e29b-41d4-a716-446655440000",
        BackupWrapper.extractSchemaGuidFromHeader(headers, false));
  }

  @Test
  public void testExtractSchemaGuidFromHeaderKeyValid() {
    RecordHeaders headers = new RecordHeaders();
    UUID testGuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    byte[] guidHeader = new byte[17];
    guidHeader[0] = 0x01;
    ByteBuffer buf = ByteBuffer.wrap(guidHeader, 1, 16);
    buf.putLong(testGuid.getMostSignificantBits());
    buf.putLong(testGuid.getLeastSignificantBits());
    headers.add("__key_schema_id", guidHeader);
    assertEquals("550e8400-e29b-41d4-a716-446655440000",
        BackupWrapper.extractSchemaGuidFromHeader(headers, true));
  }

  @Test
  public void testExtractSchemaGuidFromHeaderIntIdReturnsNull() {
    RecordHeaders headers = new RecordHeaders();
    byte[] intHeader = new byte[5];
    intHeader[0] = 0x00;
    ByteBuffer.wrap(intHeader, 1, 4).putInt(42);
    headers.add("__value_schema_id", intHeader);
    assertNull(BackupWrapper.extractSchemaGuidFromHeader(headers, false));
  }

  @Test
  public void testExtractSchemaGuidFromHeaderNull() {
    assertNull(BackupWrapper.extractSchemaGuidFromHeader(null, false));
  }

  @Test
  public void testExtractSchemaIdHeaderBytes() {
    RecordHeaders headers = new RecordHeaders();
    byte[] headerValue = new byte[]{0x00, 0x00, 0x00, 0x00, 0x2A};
    headers.add("__value_schema_id", headerValue);
    assertArrayEquals(headerValue,
        BackupWrapper.extractSchemaIdHeaderBytes(headers, false));
  }

  @Test
  public void testBuildSchemaWithGuidField() {
    Schema wrapperSchema = BackupWrapper.buildSchema(Schema.STRING_SCHEMA);
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_SCHEMA_GUID));
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA,
        wrapperSchema.field(BackupWrapper.FIELD_SCHEMA_GUID).schema());
  }

  @Test
  public void testBuildWrapperWithGuid() {
    Schema dataSchema = Schema.STRING_SCHEMA;
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        null, null, "AVRO", "test-value", null, null, null, "test-guid");
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, "hello", fields);
    assertEquals("test-guid", wrapper.getString(BackupWrapper.FIELD_SCHEMA_GUID));
    assertNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
  }

  @Test
  public void testBuildWrapperWithIntIdAndNoGuid() {
    Schema dataSchema = Schema.STRING_SCHEMA;
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        7, null, "AVRO", "test-value", null, null, null);
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, "hello", fields);
    assertEquals(Integer.valueOf(7), wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertNull(wrapper.getString(BackupWrapper.FIELD_SCHEMA_GUID));
  }

  @Test
  public void testConstants() {
    assertEquals("io.confluent.connect.backup.Wrapper", BackupWrapper.NAME);
    assertEquals("data", BackupWrapper.FIELD_DATA);
    assertEquals("schemaId", BackupWrapper.FIELD_SCHEMA_ID);
    assertEquals("schemaVersion", BackupWrapper.FIELD_SCHEMA_VERSION);
    assertEquals("schemaType", BackupWrapper.FIELD_SCHEMA_TYPE);
    assertEquals("schemaSubject", BackupWrapper.FIELD_SCHEMA_SUBJECT);
    assertEquals("rawSchema", BackupWrapper.FIELD_RAW_SCHEMA);
    assertEquals("referenceTree", BackupWrapper.FIELD_REFERENCE_TREE);
    assertEquals("directRefs", BackupWrapper.FIELD_DIRECT_REFS);
  }
}
