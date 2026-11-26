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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BackupWrapperTest {

  private static final String SCHEMA_TYPE_AVRO = "AVRO";
  private static final String TEST_SUBJECT = "test-value";

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
    assertNotNull(wrapperSchema.field(BackupWrapper.FIELD_SCHEMA_GUID));
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
        100, 1, SCHEMA_TYPE_AVRO, TEST_SUBJECT, "{}", "{\"tree\":{}}", "[{\"name\":\"ref\"}]");
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, data, fields);
    assertEquals(data, wrapper.get(BackupWrapper.FIELD_DATA));
    assertEquals(100, (int) wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertEquals(1, (int) wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
    assertEquals(SCHEMA_TYPE_AVRO, wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertEquals(TEST_SUBJECT, wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
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
    assertNull(wrapper.getString(BackupWrapper.FIELD_SCHEMA_GUID));
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
        null, null, SCHEMA_TYPE_AVRO, TEST_SUBJECT, null, null, null, "test-guid");
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, "hello", fields);
    assertEquals("test-guid", wrapper.getString(BackupWrapper.FIELD_SCHEMA_GUID));
    assertNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
  }

  @Test
  public void testBuildWrapperWithIntIdAndNoGuid() {
    Schema dataSchema = Schema.STRING_SCHEMA;
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        7, null, SCHEMA_TYPE_AVRO, TEST_SUBJECT, null, null, null);
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, "hello", fields);
    assertEquals(Integer.valueOf(7), wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertNull(wrapper.getString(BackupWrapper.FIELD_SCHEMA_GUID));
  }

  @Test
  public void testRequireFieldsPassesWhenAllPresent() {
    Schema wrapperSchema = BackupWrapper.buildSchema(Schema.STRING_SCHEMA);
    BackupWrapper.requireFields(wrapperSchema,
        BackupWrapper.FIELD_DATA,
        BackupWrapper.FIELD_SCHEMA_TYPE,
        BackupWrapper.FIELD_RAW_SCHEMA);
  }

  @Test
  public void testRequireFieldsThrowsWithNameWhenFieldMissing() {
    Schema stripped = SchemaBuilder.struct()
        .name(BackupWrapper.NAME)
        .field(BackupWrapper.FIELD_DATA, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    try {
      BackupWrapper.requireFields(stripped,
          BackupWrapper.FIELD_DATA,
          BackupWrapper.FIELD_SCHEMA_TYPE);
      fail("Expected DataException naming the missing field");
    } catch (DataException e) {
      assertEquals("Malformed backup wrapper: missing '"
          + BackupWrapper.FIELD_SCHEMA_TYPE + "' field", e.getMessage());
    }
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
    assertEquals("schemaGuid", BackupWrapper.FIELD_SCHEMA_GUID);
  }

  // ---------------- dataEncoding ----------------

  private static final String DATA_VALUE = "hello";
  private static final String CONVERTER_NAME = "TestConverter";
  private static final String UNKNOWN_ENCODING = "SOMETHING_ELSE";
  private static final String SAME_CONVERTER_HINT =
      "Restore with the same converter class that was used for the backup";

  private static Struct wrapperWithEncoding(String dataEncoding) {
    Schema wrapperSchema = BackupWrapper.buildSchema(Schema.STRING_SCHEMA);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        null, null, SCHEMA_TYPE_AVRO, TEST_SUBJECT, null, null, null, null, dataEncoding);
    return BackupWrapper.buildWrapper(wrapperSchema, DATA_VALUE, fields);
  }

  @Test
  public void testBuildSchemaHasOptionalDataEncodingField() {
    Schema wrapperSchema = BackupWrapper.buildSchema(Schema.STRING_SCHEMA);
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA,
        wrapperSchema.field(BackupWrapper.FIELD_DATA_ENCODING).schema());
  }

  @Test
  public void testExistingConstructorsLeaveDataEncodingNull() {
    Schema wrapperSchema = BackupWrapper.buildSchema(Schema.STRING_SCHEMA);
    Struct sevenArg = BackupWrapper.buildWrapper(wrapperSchema, DATA_VALUE,
        new BackupWrapper.WrapperFields(null, null, SCHEMA_TYPE_AVRO, TEST_SUBJECT,
            null, null, null));
    Struct eightArg = BackupWrapper.buildWrapper(wrapperSchema, DATA_VALUE,
        new BackupWrapper.WrapperFields(null, null, SCHEMA_TYPE_AVRO, TEST_SUBJECT,
            null, null, null, null));
    assertNull(sevenArg.getString(BackupWrapper.FIELD_DATA_ENCODING));
    assertNull(eightArg.getString(BackupWrapper.FIELD_DATA_ENCODING));
  }

  @Test
  public void testBuildWrapperSetsDataEncoding() {
    Struct wrapper = wrapperWithEncoding(BackupWrapper.DATA_ENCODING_WIRE_BYTES);
    assertEquals(BackupWrapper.DATA_ENCODING_WIRE_BYTES,
        wrapper.getString(BackupWrapper.FIELD_DATA_ENCODING));
  }

  @Test
  public void testBuildWrapperSkipsDataEncodingWhenSchemaLacksField() {
    Schema olderShape = SchemaBuilder.struct()
        .name(BackupWrapper.NAME)
        .field(BackupWrapper.FIELD_DATA, Schema.OPTIONAL_STRING_SCHEMA)
        .field(BackupWrapper.FIELD_SCHEMA_ID, Schema.OPTIONAL_INT32_SCHEMA)
        .field(BackupWrapper.FIELD_SCHEMA_VERSION, Schema.OPTIONAL_INT32_SCHEMA)
        .field(BackupWrapper.FIELD_SCHEMA_GUID, Schema.OPTIONAL_STRING_SCHEMA)
        .field(BackupWrapper.FIELD_SCHEMA_TYPE, Schema.STRING_SCHEMA)
        .field(BackupWrapper.FIELD_SCHEMA_SUBJECT, Schema.STRING_SCHEMA)
        .field(BackupWrapper.FIELD_RAW_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .field(BackupWrapper.FIELD_REFERENCE_TREE, Schema.OPTIONAL_STRING_SCHEMA)
        .field(BackupWrapper.FIELD_DIRECT_REFS, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct wrapper = BackupWrapper.buildWrapper(olderShape, DATA_VALUE,
        new BackupWrapper.WrapperFields(null, null, SCHEMA_TYPE_AVRO, TEST_SUBJECT,
            null, null, null, null, BackupWrapper.DATA_ENCODING_CONNECT));
    assertNull(olderShape.field(BackupWrapper.FIELD_DATA_ENCODING));
    assertEquals(DATA_VALUE, wrapper.getString(BackupWrapper.FIELD_DATA));
    BackupWrapper.requireDataEncoding(olderShape, wrapper,
        BackupWrapper.DATA_ENCODING_WIRE_BYTES, CONVERTER_NAME);
  }

  @Test
  public void testRequireDataEncodingAcceptsMatch() {
    Struct wrapper = wrapperWithEncoding(BackupWrapper.DATA_ENCODING_CONNECT);
    BackupWrapper.requireDataEncoding(wrapper.schema(), wrapper,
        BackupWrapper.DATA_ENCODING_CONNECT, CONVERTER_NAME);
  }

  @Test
  public void testRequireDataEncodingAcceptsNull() {
    Struct wrapper = wrapperWithEncoding(null);
    BackupWrapper.requireDataEncoding(wrapper.schema(), wrapper,
        BackupWrapper.DATA_ENCODING_WIRE_BYTES, CONVERTER_NAME);
  }

  @Test
  public void testRequireDataEncodingAcceptsSchemaWithoutField() {
    // Wrapper shape written by releases before dataEncoding existed.
    Schema olderShape = SchemaBuilder.struct().name(BackupWrapper.NAME)
        .field(BackupWrapper.FIELD_DATA, Schema.OPTIONAL_BYTES_SCHEMA)
        .build();
    BackupWrapper.requireDataEncoding(olderShape, new Struct(olderShape),
        BackupWrapper.DATA_ENCODING_WIRE_BYTES, CONVERTER_NAME);
    BackupWrapper.requireDataEncoding(olderShape, new Struct(olderShape),
        BackupWrapper.DATA_ENCODING_CONNECT, CONVERTER_NAME);
  }

  @Test
  public void testRequireDataEncodingRejectsWireBytesForConnectConverter() {
    Struct wrapper = wrapperWithEncoding(BackupWrapper.DATA_ENCODING_WIRE_BYTES);
    try {
      BackupWrapper.requireDataEncoding(wrapper.schema(), wrapper,
          BackupWrapper.DATA_ENCODING_CONNECT, CONVERTER_NAME);
      fail("Expected DataException for mismatched data encoding");
    } catch (DataException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("SchemaBackupAwareByteArrayConverter"));
      assertTrue(e.getMessage(), e.getMessage().contains(CONVERTER_NAME));
      assertTrue(e.getMessage(), e.getMessage().contains(SAME_CONVERTER_HINT));
    }
  }

  @Test
  public void testRequireDataEncodingRejectsConnectForWireBytesConverter() {
    Struct wrapper = wrapperWithEncoding(BackupWrapper.DATA_ENCODING_CONNECT);
    try {
      BackupWrapper.requireDataEncoding(wrapper.schema(), wrapper,
          BackupWrapper.DATA_ENCODING_WIRE_BYTES, CONVERTER_NAME);
      fail("Expected DataException for mismatched data encoding");
    } catch (DataException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("AvroConverter"));
      assertTrue(e.getMessage(), e.getMessage().contains(SAME_CONVERTER_HINT));
    }
  }

  @Test
  public void testRequireDataEncodingRejectsUnknownValue() {
    Struct wrapper = wrapperWithEncoding(UNKNOWN_ENCODING);
    try {
      BackupWrapper.requireDataEncoding(wrapper.schema(), wrapper,
          BackupWrapper.DATA_ENCODING_CONNECT, CONVERTER_NAME);
      fail("Expected DataException for unknown data encoding");
    } catch (DataException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(UNKNOWN_ENCODING));
    }
  }

  @Test
  public void testDataEncodingConstants() {
    assertEquals("dataEncoding", BackupWrapper.FIELD_DATA_ENCODING);
    assertEquals("CONNECT", BackupWrapper.DATA_ENCODING_CONNECT);
    assertEquals("WIRE_BYTES", BackupWrapper.DATA_ENCODING_WIRE_BYTES);
  }
}
