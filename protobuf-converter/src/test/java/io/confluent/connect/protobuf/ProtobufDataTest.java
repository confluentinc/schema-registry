/*
 * Copyright 2020 Confluent Inc.
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
 *
 */

package io.confluent.connect.protobuf;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.NestedMessage;
import io.confluent.kafka.serializers.protobuf.test.SInt32ValueOuterClass;
import io.confluent.kafka.serializers.protobuf.test.SInt64ValueOuterClass;
import io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass;
import io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue;
import io.confluent.kafka.serializers.protobuf.test.UInt32ValueOuterClass;

import static io.confluent.connect.protobuf.ProtobufData.PROTOBUF_TYPE_TAG;
import static io.confluent.connect.protobuf.ProtobufData.PROTOBUF_TYPE_UNION_PREFIX;
import static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue.newBuilder;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT16_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT8_SCHEMA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ProtobufDataTest {

  private static final Logger log = LoggerFactory.getLogger(ProtobufDataTest.class);

  private static Schema OPTIONAL_INT32_SCHEMA = SchemaBuilder.int32()
      .optional()
      .parameter(PROTOBUF_TYPE_TAG, String.valueOf(1))
      .build();
  private static Schema OPTIONAL_INT64_SCHEMA = SchemaBuilder.int64()
      .optional()
      .parameter(PROTOBUF_TYPE_TAG, String.valueOf(1))
      .build();
  private static Schema OPTIONAL_FLOAT32_SCHEMA = SchemaBuilder.float32()
      .optional()
      .parameter(PROTOBUF_TYPE_TAG, String.valueOf(1))
      .build();
  private static Schema OPTIONAL_FLOAT64_SCHEMA = SchemaBuilder.float64()
      .optional()
      .parameter(PROTOBUF_TYPE_TAG, String.valueOf(1))
      .build();
  private static Schema OPTIONAL_BOOLEAN_SCHEMA = SchemaBuilder.bool()
      .optional()
      .parameter(PROTOBUF_TYPE_TAG, String.valueOf(1))
      .build();
  private static Schema OPTIONAL_STRING_SCHEMA = SchemaBuilder.string()
      .optional()
      .parameter(PROTOBUF_TYPE_TAG, String.valueOf(1))
      .build();
  private static Schema OPTIONAL_BYTES_SCHEMA = SchemaBuilder.bytes()
      .optional()
      .parameter(PROTOBUF_TYPE_TAG, String.valueOf(1))
      .build();

  private static final String VALUE_FIELD_NAME = "value";

  private SchemaAndValue getExpectedSchemaAndValue(
      Schema fieldSchema,
      Message message,
      Object value
  ) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name(message.getDescriptorForType().getName());
    schemaBuilder.field(VALUE_FIELD_NAME, fieldSchema);
    final Schema schema = schemaBuilder.build();
    Struct expectedResult = new Struct(schema);
    expectedResult.put(VALUE_FIELD_NAME, value);
    return new SchemaAndValue(schema, expectedResult);
  }

  private StringValue createStringValueMessage(String messageText) {
    StringValue.Builder builder = StringValue.newBuilder();
    builder.setValue(messageText);
    return builder.build();
  }

  private NestedMessage createNestedTestProtoStringUserId() throws ParseException {
    return createNestedTestProto(NestedTestProto.UserId.newBuilder()
        .setKafkaUserId("my_user")
        .build());
  }

  private NestedMessage createNestedTestProtoIntUserId() throws ParseException {
    return createNestedTestProto(NestedTestProto.UserId.newBuilder().setOtherUserId(5).build());
  }

  private NestedMessage createNestedTestProto(NestedTestProto.UserId id) throws ParseException {
    NestedMessage.Builder message = NestedMessage.newBuilder();
    message.setUserId(id);
    message.setIsActive(true);
    message.addExperimentsActive("first experiment");
    message.addExperimentsActive("second experiment");
    message.setStatus(NestedTestProto.Status.INACTIVE);
    NestedMessage.InnerMessage.Builder inner = NestedMessage.InnerMessage.newBuilder();
    inner.setId("");
    message.setInner(inner.build());
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    java.util.Date date = sdf.parse("2017/09/18");
    Timestamp timestamp = Timestamps.fromMillis(date.getTime());
    message.setUpdatedAt(timestamp);
    message.putMapType("Hello", "World");
    return message.build();
  }

  private Schema getExpectedNestedTestProtoSchemaStringUserId() {
    return getExpectedNestedTestProtoSchema();
  }

  private Schema getExpectedNestedTestProtoSchemaIntUserId() {
    return getExpectedNestedTestProtoSchema();
  }

  private SchemaBuilder getComplexTypeSchemaBuilder() {
    final SchemaBuilder complexTypeBuilder = SchemaBuilder.struct();
    complexTypeBuilder.name("ComplexType");
    final SchemaBuilder someValBuilder = SchemaBuilder.struct();
    someValBuilder.name(PROTOBUF_TYPE_UNION_PREFIX + "some_val");
    someValBuilder.field(
        "one_id",
        SchemaBuilder.string().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    someValBuilder.field(
        "other_id",
        SchemaBuilder.int32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(2)).build()
    );
    complexTypeBuilder.field("some_val_0", someValBuilder.optional().build());
    complexTypeBuilder.field(
        "is_active",
        SchemaBuilder.bool().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(3)).build()
    );
    return complexTypeBuilder;
  }

  private SchemaBuilder getInnerMessageSchemaBuilder() {
    final SchemaBuilder innerMessageBuilder = SchemaBuilder.struct();
    innerMessageBuilder.name("InnerMessage");
    innerMessageBuilder.field(
        "id",
      SchemaBuilder.string().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    innerMessageBuilder.field(
        "ids",
        SchemaBuilder.array(SchemaBuilder.int32().optional().build())
            .optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(2)).build()
    );
    return innerMessageBuilder;
  }

  private Schema getExpectedNestedTestProtoSchema() {
    final SchemaBuilder builder = SchemaBuilder.struct();
    builder.name("NestedMessage");
    final SchemaBuilder userIdBuilder = SchemaBuilder.struct();
    userIdBuilder.name("UserId");
    final SchemaBuilder idBuilder = SchemaBuilder.struct();
    idBuilder.name(PROTOBUF_TYPE_UNION_PREFIX + "user_id");
    idBuilder.field(
        "kafka_user_id",
        SchemaBuilder.string().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    idBuilder.field(
        "other_user_id",
        SchemaBuilder.int32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(2)).build()
    );
    final SchemaBuilder messageIdBuilder = SchemaBuilder.struct();
    messageIdBuilder.name("MessageId");
    messageIdBuilder.field(
        "id",
        SchemaBuilder.string().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    idBuilder.field(
        "another_id",
        messageIdBuilder.optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(3)).build()
    );
    userIdBuilder.field("user_id_0", idBuilder.optional().build());
    builder.field(
        "user_id",
        userIdBuilder.optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    builder.field(
        "is_active",
        SchemaBuilder.bool().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(2)).build()
    );
    builder.field(
        "experiments_active",
        SchemaBuilder.array(SchemaBuilder.string().optional().build())
            .optional()
            .parameter(PROTOBUF_TYPE_TAG, String.valueOf(3))
            .build()
    );
    builder.field(
        "updated_at",
        org.apache.kafka.connect.data.Timestamp.builder()
            .optional()
            .parameter(PROTOBUF_TYPE_TAG, String.valueOf(4))
            .build()
    );
    builder.field("status",
        SchemaBuilder.int32()
            .name("Status")
            .optional()
            .parameter(PROTOBUF_TYPE_TAG, String.valueOf(5))
            .parameter(ProtobufData.PROTOBUF_TYPE_ENUM, "Status")
            .parameter(ProtobufData.PROTOBUF_TYPE_ENUM_PREFIX + "ACTIVE", "0")
            .parameter(ProtobufData.PROTOBUF_TYPE_ENUM_PREFIX + "INACTIVE", "1")
            .build()
    );
    builder.field(
        "complex_type",
        getComplexTypeSchemaBuilder().optional()
            .parameter(PROTOBUF_TYPE_TAG, String.valueOf(6))
            .build()
    );
    builder.field("map_type",
        SchemaBuilder.map(
            OPTIONAL_STRING_SCHEMA,
            SchemaBuilder.string()
                .optional()
                .parameter(PROTOBUF_TYPE_TAG, String.valueOf(2))
                .build()
        ).name("map_type").optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(7)).build()
    );
    builder.field(
        "inner",
        getInnerMessageSchemaBuilder().optional()
            .parameter(PROTOBUF_TYPE_TAG, String.valueOf(8))
            .build()
    );
    return builder.build();
  }

  private Map<String, String> getTestKeyValueMap() {
    Map<String, String> result = new HashMap<>();
    result.put("Hello", "World");
    return result;
  }

  private Struct getExpectedNestedProtoResultStringUserId() throws ParseException {
    Schema schema = getExpectedNestedTestProtoSchemaStringUserId();
    Struct result = new Struct(schema.schema());
    Struct userId = new Struct(schema.field("user_id").schema());
    Struct union = new Struct(schema.field("user_id").schema().field("user_id_0").schema());
    union.put("kafka_user_id", "my_user");
    userId.put("user_id_0", union);
    result.put("user_id", userId);
    result.put("is_active", true);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    java.util.Date date = sdf.parse("2017/09/18");
    result.put("updated_at", date);

    List<String> experiments = new ArrayList<>();
    experiments.add("first experiment");
    experiments.add("second experiment");
    result.put("experiments_active", experiments);

    result.put("status", 1); // INACTIVE
    result.put("map_type", getTestKeyValueMap());

    Struct inner = new Struct(schema.field("inner").schema());
    inner.put("id", "");
    inner.put("ids", new ArrayList<>());
    result.put("inner", inner);
    return result;
  }

  private Struct getExpectedNestedTestProtoResultIntUserId() throws ParseException {
    Schema schema = getExpectedNestedTestProtoSchemaIntUserId();
    Struct result = new Struct(schema.schema());
    Struct userId = new Struct(schema.field("user_id").schema());
    Struct union = new Struct(schema.field("user_id").schema().field("user_id_0").schema());
    union.put("other_user_id", 5);
    userId.put("user_id_0", union);
    result.put("user_id", userId);
    result.put("is_active", true);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    java.util.Date date = sdf.parse("2017/09/18");
    result.put("updated_at", date);

    List<String> experiments = new ArrayList<>();
    experiments.add("first experiment");
    experiments.add("second experiment");
    result.put("experiments_active", experiments);

    result.put("status", 1); // INACTIVE
    result.put("map_type", getTestKeyValueMap());

    Struct inner = new Struct(schema.field("inner").schema());
    inner.put("id", "");
    inner.put("ids", new ArrayList<>());
    result.put("inner", inner);
    return result;
  }

  private NestedTestProto.ComplexType createProtoDefaultOneOf() throws ParseException {
    NestedTestProto.ComplexType.Builder complexTypeBuilder =
        NestedTestProto.ComplexType.newBuilder();
    complexTypeBuilder.setOtherId(0);
    return complexTypeBuilder.build();
  }

  private NestedTestProto.ComplexType createProtoMultipleSetOneOf() throws ParseException {
    NestedTestProto.ComplexType.Builder complexTypeBuilder =
        NestedTestProto.ComplexType.newBuilder();
    complexTypeBuilder.setOneId("asdf");
    complexTypeBuilder.setOtherId(0);
    return complexTypeBuilder.build();
  }

  private Struct getExpectedComplexTypeProtoWithDefaultOneOf() {
    Schema schema = getComplexTypeSchemaBuilder().build();
    Struct result = new Struct(schema.schema());
    Struct union = new Struct(schema.field("some_val_0").schema());
    union.put("other_id", 0);
    result.put("some_val_0", union);
    result.put("is_active", false);
    return result;
  }

  private void assertSchemasEqual(Schema expectedSchema, Schema actualSchema) {
    assertEquals(expectedSchema.isOptional(), actualSchema.isOptional());
    assertEquals(expectedSchema.version(), actualSchema.version());
    assertEquals(expectedSchema.name(), actualSchema.name());
    assertEquals(expectedSchema.doc(), actualSchema.doc());
    assertEquals(expectedSchema.type(), actualSchema.type());
    assertEquals(expectedSchema.defaultValue(), actualSchema.defaultValue());
    assertEquals(expectedSchema.parameters(), actualSchema.parameters());

    if (expectedSchema.type() == Schema.Type.STRUCT) {
      assertEquals(expectedSchema.fields().size(), actualSchema.fields().size());
      for (int i = 0; i < expectedSchema.fields().size(); ++i) {
        Field expectedField = expectedSchema.fields().get(i);
        Field actualField = actualSchema.field(expectedField.name());
        assertSchemasEqual(expectedField.schema(), actualField.schema());
      }
    } else if (expectedSchema.type() == Schema.Type.ARRAY) {
      assertSchemasEqual(expectedSchema.valueSchema(), actualSchema.valueSchema());
    } else if (expectedSchema.type() == Schema.Type.MAP) {
      assertSchemasEqual(expectedSchema.keySchema(), actualSchema.keySchema());
      assertSchemasEqual(expectedSchema.valueSchema(), actualSchema.valueSchema());
    }
  }

  private SchemaAndValue getSchemaAndValue(Message message) throws Exception {
    ProtobufSchema protobufSchema = new ProtobufSchema(message.getDescriptorForType());
    DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
        protobufSchema.toDescriptor(),
        message.toByteArray()
    );
    ProtobufData protobufData = new ProtobufData();
    return protobufData.toConnectData(protobufSchema, dynamicMessage);
  }

  @Test
  public void testToConnectDataWithNestedProtobufMessageAndStringUserId() throws Exception {
    NestedMessage message = createNestedTestProtoStringUserId();
    SchemaAndValue result = getSchemaAndValue(message);
    Schema expectedSchema = getExpectedNestedTestProtoSchemaStringUserId();
    assertSchemasEqual(expectedSchema, result.schema());
    Struct expected = getExpectedNestedProtoResultStringUserId();
    assertEquals(expected, result.value());
  }

  @Test
  public void testToConnectDataWithNestedProtobufMessageAndIntUserId() throws Exception {
    NestedMessage message = createNestedTestProtoIntUserId();
    SchemaAndValue result = getSchemaAndValue(message);
    Schema expectedSchema = getExpectedNestedTestProtoSchemaIntUserId();
    assertSchemasEqual(expectedSchema, result.schema());
    Struct expected = getExpectedNestedTestProtoResultIntUserId();
    assertSchemasEqual(expected.schema(), ((Struct) result.value()).schema());
    assertEquals(expected.schema(), ((Struct) result.value()).schema());
    assertEquals(expected, result.value());
  }

  @Test
  public void testToConnectDataDefaultOneOf() throws Exception {
    Schema schema = getComplexTypeSchemaBuilder().build();
    NestedTestProto.ComplexType message = createProtoDefaultOneOf();
    SchemaAndValue result = getSchemaAndValue(message);
    assertSchemasEqual(schema, result.schema());
    assertEquals(new SchemaAndValue(schema, getExpectedComplexTypeProtoWithDefaultOneOf()), result);
  }

  @Test
  public void testToConnectDataDefaultOneOfCannotHaveTwoOneOfsSet() throws Exception {
    Schema schema = getComplexTypeSchemaBuilder().build();
    NestedTestProto.ComplexType message = createProtoMultipleSetOneOf();
    SchemaAndValue result = getSchemaAndValue(message);
    assertSchemasEqual(schema, result.schema());
    assertEquals(new SchemaAndValue(schema, getExpectedComplexTypeProtoWithDefaultOneOf()), result);
  }

  // Data Conversion tests
  @Test
  public void testToConnectNull() {
    ProtobufData protobufData = new ProtobufData();
    Schema schema = OPTIONAL_BOOLEAN_SCHEMA.schema();
    assertNull(protobufData.toConnectData(schema, null));
  }

  @Test
  public void testToConnectBoolean() throws Exception {
    Boolean expectedValue = true;
    BoolValue.Builder builder = BoolValue.newBuilder();
    builder.setValue(expectedValue);
    BoolValue message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(
        getExpectedSchemaAndValue(OPTIONAL_BOOLEAN_SCHEMA, message, expectedValue),
        result
    );
  }

  @Test
  public void testToConnectInt32() throws Exception {
    Integer expectedValue = 12;
    Int32Value.Builder builder = Int32Value.newBuilder();
    builder.setValue(expectedValue);
    Int32Value message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(getExpectedSchemaAndValue(OPTIONAL_INT32_SCHEMA, message, expectedValue), result);
  }

  @Test
  public void testToConnectInt32With0() throws Exception {
    Integer expectedValue = 0;
    Int32Value.Builder builder = Int32Value.newBuilder();
    builder.setValue(expectedValue);
    Int32Value message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(getExpectedSchemaAndValue(OPTIONAL_INT32_SCHEMA, message, expectedValue), result);
  }

  @Test
  public void testToConnectInt32WithSint32() throws Exception {
    int expectedValue = 12;
    SInt32ValueOuterClass.SInt32Value.Builder builder =
        SInt32ValueOuterClass.SInt32Value.newBuilder();
    builder.setValue(expectedValue);
    SInt32ValueOuterClass.SInt32Value message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(getExpectedSchemaAndValue(OPTIONAL_INT32_SCHEMA, message, expectedValue), result);
  }

  @Test
  public void testToConnectInt32WithUInt32() throws Exception {
    final Long UNSIGNED_RESULT = 4294967295L;
    Integer expectedValue = -1;
    UInt32ValueOuterClass.UInt32Value.Builder builder =
        UInt32ValueOuterClass.UInt32Value.newBuilder();
    builder.setValue(expectedValue);
    UInt32ValueOuterClass.UInt32Value message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(
        getExpectedSchemaAndValue(OPTIONAL_INT64_SCHEMA, message, UNSIGNED_RESULT),
        result
    );
  }

  @Test
  public void testToConnectInt64() throws Exception {
    Long expectedValue = 12L;
    Int64Value.Builder builder = Int64Value.newBuilder();
    builder.setValue(expectedValue);
    Int64Value message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(getExpectedSchemaAndValue(OPTIONAL_INT64_SCHEMA, message, expectedValue), result);
  }

  @Test
  public void testToConnectSInt64() throws Exception {
    Long expectedValue = 12L;
    SInt64ValueOuterClass.SInt64Value.Builder builder =
        SInt64ValueOuterClass.SInt64Value.newBuilder();
    builder.setValue(expectedValue);
    SInt64ValueOuterClass.SInt64Value message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(getExpectedSchemaAndValue(OPTIONAL_INT64_SCHEMA, message, expectedValue), result);
  }

  @Test
  public void testToConnectFloat32() throws Exception {
    Float expectedValue = 12.f;
    FloatValue.Builder builder = FloatValue.newBuilder();
    builder.setValue(expectedValue);
    FloatValue message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(
        getExpectedSchemaAndValue(OPTIONAL_FLOAT32_SCHEMA, message, expectedValue),
        result
    );
  }

  @Test
  public void testToConnectFloat64() throws Exception {
    Double expectedValue = 12.0;
    DoubleValue.Builder builder = DoubleValue.newBuilder();
    builder.setValue(expectedValue);
    DoubleValue message = builder.build();
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(
        getExpectedSchemaAndValue(OPTIONAL_FLOAT64_SCHEMA, message, expectedValue),
        result
    );
  }

  @Test
  public void testToConnectString() throws Exception {
    String expectedValue = "Hello";
    StringValue message = createStringValueMessage(expectedValue);
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(getExpectedSchemaAndValue(OPTIONAL_STRING_SCHEMA, message, expectedValue), result);
  }

  @Test
  public void testToConnectEmptyString() throws Exception {
    String expectedValue = "";
    StringValue message = createStringValueMessage(expectedValue);
    SchemaAndValue result = getSchemaAndValue(message);
    assertEquals(getExpectedSchemaAndValue(OPTIONAL_STRING_SCHEMA, message, expectedValue), result);
  }

  @Test
  public void testToConnectTimestamp() throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    java.util.Date expectedValue = sdf.parse("2017/12/31");

    Timestamp timestamp = Timestamps.fromMillis(expectedValue.getTime());
    TimestampValueOuterClass.TimestampValue.Builder builder = newBuilder();
    builder.setValue(timestamp);
    TimestampValueOuterClass.TimestampValue message = builder.build();

    SchemaAndValue result = getSchemaAndValue(message);

    Schema timestampSchema = org.apache.kafka.connect.data.Timestamp.builder()
        .optional()
        .parameter(PROTOBUF_TYPE_TAG, String.valueOf(1))
        .build();
    assertEquals(getExpectedSchemaAndValue(timestampSchema, message, expectedValue), result);
  }

  @Test(expected = DataException.class)
  public void testToConnectSchemaMismatchArray() {
    ProtobufData protobufData = new ProtobufData();
    Schema schema = SchemaBuilder.array(OPTIONAL_STRING_SCHEMA).build();
    protobufData.toConnectData(schema, Arrays.asList(1, 2, 3));
  }

  private byte[] getMessageBytes(Schema schema, Object value) throws Exception {
    Schema structSchema = SchemaBuilder.struct().field(VALUE_FIELD_NAME, schema).build();
    Struct struct = new Struct(structSchema.schema());
    struct.put(VALUE_FIELD_NAME, value);
    SchemaAndValue schemaAndValue = new SchemaAndValue(structSchema, struct);
    return getMessageBytes(schemaAndValue);
  }

  private byte[] getMessageBytes(SchemaAndValue schemaAndValue) throws Exception {
    ProtobufData protobufData = new ProtobufData();
    ProtobufSchemaAndValue protobufSchemaAndValue = protobufData.fromConnectData(schemaAndValue);
    DynamicMessage dynamicMessage = (DynamicMessage) protobufSchemaAndValue.getValue();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    dynamicMessage.writeTo(baos);
    return baos.toByteArray();
  }

  @Test
  public void testFromConnectDataWithNestedProtobufMessageAndStringUserId() throws Exception {
    NestedMessage nestedMessage = createNestedTestProtoStringUserId();
    SchemaAndValue schemaAndValue = getSchemaAndValue(nestedMessage);
    byte[] messageBytes = getMessageBytes(schemaAndValue);
    Message message = NestedTestProto.NestedMessage.parseFrom(messageBytes);

    assertArrayEquals(messageBytes, message.toByteArray());
  }

  @Test
  public void testFromConnectDataWithNestedProtobufMessageAndIntUserId() throws Exception {
    NestedMessage nestedMessage = createNestedTestProtoIntUserId();
    SchemaAndValue schemaAndValue = getSchemaAndValue(nestedMessage);
    byte[] messageBytes = getMessageBytes(schemaAndValue);
    Message message = NestedTestProto.NestedMessage.parseFrom(messageBytes);

    assertArrayEquals(messageBytes, message.toByteArray());
  }

  @Test
  public void testFromConnectComplex() {
    Schema schema = SchemaBuilder.struct()
        .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .field("mapNonStringKeys",
            SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build()
        )
        .build();
    Struct struct = new Struct(schema).put("int8", (byte) 12)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", ByteBuffer.wrap("foo".getBytes()))
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1))
        .put("mapNonStringKeys", Collections.singletonMap(1, 1));

    ProtobufSchemaAndValue convertedRecord = new ProtobufData().fromConnectData(schema, struct);
    ProtobufSchema protobufSchema = convertedRecord.getSchema();
    DynamicMessage message = (DynamicMessage) convertedRecord.getValue();

    MessageElement messageElem = (MessageElement) protobufSchema.rawSchema().getTypes().get(0);
    FieldElement fieldElem = messageElem.getFields().get(0);
    assertEquals("int8", fieldElem.getName());
    assertEquals("int32", fieldElem.getType());
    fieldElem = messageElem.getFields().get(1);
    assertEquals("int16", fieldElem.getName());
    assertEquals("int32", fieldElem.getType());
    fieldElem = messageElem.getFields().get(2);
    assertEquals("int32", fieldElem.getName());
    assertEquals("int32", fieldElem.getType());
    fieldElem = messageElem.getFields().get(3);
    assertEquals("int64", fieldElem.getName());
    assertEquals("int64", fieldElem.getType());
    fieldElem = messageElem.getFields().get(4);
    assertEquals("float32", fieldElem.getName());
    assertEquals("float", fieldElem.getType());
    fieldElem = messageElem.getFields().get(5);
    assertEquals("float64", fieldElem.getName());
    assertEquals("double", fieldElem.getType());
    fieldElem = messageElem.getFields().get(6);
    assertEquals("boolean", fieldElem.getName());
    assertEquals("bool", fieldElem.getType());
    fieldElem = messageElem.getFields().get(7);
    assertEquals("string", fieldElem.getName());
    assertEquals("string", fieldElem.getType());
    fieldElem = messageElem.getFields().get(8);
    assertEquals("bytes", fieldElem.getName());
    assertEquals("bytes", fieldElem.getType());
    fieldElem = messageElem.getFields().get(9);
    assertEquals("array", fieldElem.getName());
    assertEquals("string", fieldElem.getType());
    fieldElem = messageElem.getFields().get(10);
    assertEquals("map", fieldElem.getName());
    assertEquals("ConnectDefault2Entry", fieldElem.getType());
    fieldElem = messageElem.getFields().get(11);
    assertEquals("mapNonStringKeys", fieldElem.getName());
    assertEquals("ConnectDefault3Entry", fieldElem.getType());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName("int8");
    assertEquals(12, message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("int16");
    assertEquals(12, message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("int32");
    assertEquals(12, message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("int64");
    assertEquals(12L, message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("float32");
    assertEquals(12.2f, message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("float64");
    assertEquals(12.2, message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("boolean");
    assertEquals(true, message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("string");
    assertEquals("foo", message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("bytes");
    assertEquals(ByteString.copyFrom("foo".getBytes()), message.getField(fieldDescriptor));
    fieldDescriptor = message.getDescriptorForType().findFieldByName("array");
    assertEquals(Arrays.asList("a", "b", "c"), message.getField(fieldDescriptor));

    fieldDescriptor = message.getDescriptorForType().findFieldByName("map");
    Object value = message.getField(fieldDescriptor);
    DynamicMessage dynamicMessage = ((List<DynamicMessage>) value).get(0);
    fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("key");
    assertEquals("field", dynamicMessage.getField(fieldDescriptor));
    fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("value");
    assertEquals(1, dynamicMessage.getField(fieldDescriptor));

    fieldDescriptor = message.getDescriptorForType().findFieldByName("mapNonStringKeys");
    value = message.getField(fieldDescriptor);
    dynamicMessage = ((List<DynamicMessage>) value).get(0);
    fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("key");
    assertEquals(1, dynamicMessage.getField(fieldDescriptor));
    fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("value");
    assertEquals(1, dynamicMessage.getField(fieldDescriptor));
  }


  @Test
  public void testFromConnectInt8() throws Exception {
    Byte value = 15;
    byte[] messageBytes = getMessageBytes(OPTIONAL_INT8_SCHEMA, value);
    Message message = Int32Value.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    // Value converted to int
    assertEquals(value.intValue(), message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectInt16() throws Exception {
    Short value = 15;
    byte[] messageBytes = getMessageBytes(OPTIONAL_INT16_SCHEMA, value);
    Message message = Int32Value.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    // Value converted to int
    assertEquals(value.intValue(), message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectInt32() throws Exception {
    Integer value = 15;
    byte[] messageBytes = getMessageBytes(OPTIONAL_INT32_SCHEMA, value);
    Message message = Int32Value.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(value, message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectInt64() throws Exception {
    Long value = 15L;
    byte[] messageBytes = getMessageBytes(OPTIONAL_INT64_SCHEMA, value);
    Message message = Int64Value.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(value, message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectTimestamp() throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    java.util.Date date = sdf.parse("2017/09/18");
    Timestamp timestamp = Timestamps.fromMillis(date.getTime());
    TimestampValue.Builder builder = newBuilder();
    builder.setValue(timestamp);
    TimestampValue timestampMessage = builder.build();

    SchemaAndValue schemaAndValue = getSchemaAndValue(timestampMessage);
    byte[] messageBytes = getMessageBytes(schemaAndValue);
    Message message = TimestampValue.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(Timestamps.fromMillis(date.getTime()), message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectFloat32() throws Exception {
    Float value = 12.3f;
    byte[] messageBytes = getMessageBytes(OPTIONAL_FLOAT32_SCHEMA, value);
    Message message = FloatValue.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(value, message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectFloat64() throws Exception {
    Double value = 12.3;
    byte[] messageBytes = getMessageBytes(OPTIONAL_FLOAT64_SCHEMA, value);
    Message message = DoubleValue.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(value, message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectBoolean() throws Exception {
    Boolean value = true;
    byte[] messageBytes = getMessageBytes(OPTIONAL_BOOLEAN_SCHEMA, value);
    Message message = BoolValue.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(value, message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectBooleanWithFalse() throws Exception {
    Boolean value = false;
    byte[] messageBytes = getMessageBytes(OPTIONAL_BOOLEAN_SCHEMA, value);
    Message message = BoolValue.parseFrom(messageBytes);

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(value, message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectString() throws Exception {
    String value = "Hello";
    byte[] messageBytes = getMessageBytes(OPTIONAL_STRING_SCHEMA, value);
    Message message = StringValue.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(value, message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectEmptyString() throws Exception {
    String value = "";
    byte[] messageBytes = getMessageBytes(OPTIONAL_STRING_SCHEMA, value);
    Message message = StringValue.parseFrom(messageBytes);

    // TODO verify that empty string is omitted in protobuf v3 spec
    assertEquals(0, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(value, message.getField(fieldDescriptor));
  }

  @Test
  public void testFromConnectBytes() throws Exception {
    byte[] value = ByteBuffer.wrap("foo".getBytes()).array();
    byte[] messageBytes = getMessageBytes(OPTIONAL_BYTES_SCHEMA, value);
    Message message = BytesValue.parseFrom(messageBytes);

    assertEquals(1, message.getAllFields().size());

    Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType()
        .findFieldByName(VALUE_FIELD_NAME);
    assertEquals(ByteString.copyFrom(value), message.getField(fieldDescriptor));
  }
}
