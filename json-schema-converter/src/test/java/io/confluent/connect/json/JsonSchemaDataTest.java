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
 */

package io.confluent.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.json.JsonSchemaData.SchemaWrapper;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.DecimalFormat;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;

import io.confluent.kafka.schemaregistry.json.JsonSchema;

import static io.confluent.connect.json.JsonSchemaData.CONNECT_TYPE_MAP;
import static io.confluent.connect.json.JsonSchemaData.CONNECT_TYPE_PROP;
import static io.confluent.connect.json.JsonSchemaData.JSON_TYPE_ENUM;
import static io.confluent.connect.json.JsonSchemaData.JSON_TYPE_ONE_OF;
import static io.confluent.connect.json.JsonSchemaData.KEY_FIELD;
import static io.confluent.connect.json.JsonSchemaData.VALUE_FIELD;
import static org.apache.kafka.connect.data.Decimal.LOGICAL_NAME;
import static org.apache.kafka.connect.data.Decimal.SCALE_FIELD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JsonSchemaDataTest {

  private static final Schema NAMED_MAP_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA,
      Schema.INT32_SCHEMA
  ).name("foo.bar").build();
  private static final org.everit.json.schema.Schema NAMED_JSON_MAP_SCHEMA = ObjectSchema.builder()
      .schemaOfAdditionalProperties(NumberSchema.builder()
          .requiresInteger(true)
          .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
          .build())
      .unprocessedProperties(Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP))
      .title("foo.bar")
      .build();

  private JsonSchemaData jsonSchemaData = new JsonSchemaData();

  public JsonSchemaDataTest() {
  }

  // Connect -> JSON Schema

  @Test
  public void testFromConnectNull() throws Exception {
    BooleanSchema booleanSchema = BooleanSchema.builder().build();
    CombinedSchema expectedSchema =
        CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE, booleanSchema)).build();
    Schema schema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
    checkNonObjectConversion(expectedSchema, NullNode.getInstance(), schema, null);

    checkNonObjectConversion(null, null, (Schema) null, null);
  }

  @Test
  public void testFromConnectBoolean() {
    BooleanSchema schema = BooleanSchema.builder().build();
    checkNonObjectConversion(schema, BooleanNode.getTrue(), Schema.BOOLEAN_SCHEMA, true);
  }

  @Test
  public void testFromConnectByte() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .build();
    checkNonObjectConversion(schema, ShortNode.valueOf((short) 12), Schema.INT8_SCHEMA, (byte) 12);
  }

  @Test
  public void testFromConnectShort() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
        .build();
    checkNonObjectConversion(schema,
        ShortNode.valueOf((short) 12),
        Schema.INT16_SCHEMA,
        (short) 12
    );
  }

  @Test
  public void testFromConnectInteger() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
        .build();
    checkNonObjectConversion(schema, IntNode.valueOf(12), Schema.INT32_SCHEMA, 12);
  }

  @Test
  public void testFromConnectLong() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int64"))
        .build();
    checkNonObjectConversion(schema, LongNode.valueOf(12L), Schema.INT64_SCHEMA, 12L);
  }

  @Test
  public void testFromConnectFloat() {
    NumberSchema schema = NumberSchema.builder()
        .unprocessedProperties(Collections.singletonMap("connect.type", "float32"))
        .build();
    checkNonObjectConversion(schema, FloatNode.valueOf(12.2f), Schema.FLOAT32_SCHEMA, 12.2f);
  }

  @Test
  public void testFromConnectDouble() {
    NumberSchema schema = NumberSchema.builder()
        .unprocessedProperties(Collections.singletonMap("connect.type", "float64"))
        .build();
    checkNonObjectConversion(schema, DoubleNode.valueOf(12.2), Schema.FLOAT64_SCHEMA, 12.2);
  }

  @Test
  public void testFromConnectBytes() throws Exception {
    StringSchema schema = StringSchema.builder()
        .unprocessedProperties(Collections.singletonMap("connect.type", "bytes"))
        .build();
    checkNonObjectConversion(schema,
        BinaryNode.valueOf("foo".getBytes()),
        Schema.BYTES_SCHEMA,
        "foo".getBytes()
    );
  }

  @Test
  public void testFromConnectString() {
    StringSchema schema = StringSchema.builder().build();
    checkNonObjectConversion(schema, TextNode.valueOf("string"), Schema.STRING_SCHEMA, "string");
  }

  @Test
  public void testFromConnectEnum() {
    EnumSchema schema = EnumSchema.builder()
        .possibleValue("one")
        .possibleValue("two")
        .possibleValue("three")
        .build();
    Schema connectSchema = new SchemaBuilder(Schema.Type.STRING).parameter(JSON_TYPE_ENUM, "")
        .parameter(JSON_TYPE_ENUM + ".one", "one")
        .parameter(JSON_TYPE_ENUM + ".two", "two")
        .parameter(JSON_TYPE_ENUM + ".three", "three")
        .build();

    checkNonObjectConversion(schema, TextNode.valueOf("one"), connectSchema, "one");
  }

  @Test
  public void testFromConnectUnion() {
    NumberSchema firstSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.type", "int8", "connect.index", 0))
        .build();
    NumberSchema secondSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.type", "int16", "connect.index", 1))
        .build();
    CombinedSchema schema = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", Schema.INT8_SCHEMA);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", Schema.INT16_SCHEMA);
    Schema connectSchema = builder.build();

    Struct actual = new Struct(connectSchema).put(JSON_TYPE_ONE_OF + ".field.0", (byte) 12);
    checkNonObjectConversion(schema, ShortNode.valueOf((short) 12), connectSchema, actual);
  }

  @Test
  public void testFromConnectUnionDifferentStruct() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder()
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema firstSchema = ObjectSchema.builder()
        .addPropertySchema("a", numberSchema)
        .addPropertySchema("b", stringSchema)
        .title("field0")
        .unprocessedProperties(ImmutableMap.of("connect.index", 0))
        .build();
    ObjectSchema secondSchema = ObjectSchema.builder()
        .addPropertySchema("c", numberSchema)
        .addPropertySchema("d", stringSchema)
        .title("field1")
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    CombinedSchema schema = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    Schema field0 = SchemaBuilder.struct()
        .name("field0")
        .field("a", Schema.INT8_SCHEMA)
        .field("b", Schema.STRING_SCHEMA)
        .optional()
        .build();
    Schema field1 = SchemaBuilder.struct()
        .name("field1")
        .field("c", Schema.INT8_SCHEMA)
        .field("d", Schema.STRING_SCHEMA)
        .optional()
        .build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", field0);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", field1);
    Schema connectSchema = builder.build();

    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("a", ShortNode.valueOf((short) 12));
    obj.set("b", TextNode.valueOf("sample string"));
    Struct struct = new Struct(field0).put("a", (byte) 12)
        .put("b", "sample string");
    Struct actual = new Struct(connectSchema).put(JSON_TYPE_ONE_OF + ".field.0", struct);
    checkNonObjectConversion(schema, obj, connectSchema, actual);

    obj = JsonNodeFactory.instance.objectNode();
    obj.set("c", ShortNode.valueOf((short) 12));
    obj.set("d", TextNode.valueOf("sample string"));
    struct = new Struct(field1).put("c", (byte) 12)
        .put("d", "sample string");
    actual = new Struct(connectSchema).put(JSON_TYPE_ONE_OF + ".field.1", struct);
    checkNonObjectConversion(schema, obj, connectSchema, actual);
  }

  @Test
  public void testFromConnectBase64Decimal() {
    jsonSchemaData =
        new JsonSchemaData(new JsonSchemaDataConfig(
            Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG,
                DecimalFormat.BASE64.name())));
    NumberSchema schema = NumberSchema.builder()
        .title("org.apache.kafka.connect.data.Decimal")
        .unprocessedProperties(ImmutableMap.of("connect.type",
            "bytes",
            "connect.version",
            1,
            "connect.parameters",
            ImmutableMap.of("scale", "2")
        ))
        .build();
    checkNonObjectConversion(schema,
        BinaryNode.valueOf(new byte[]{0, -100}),
        Decimal.schema(2),
        new BigDecimal(new BigInteger("156"), 2)
    );
    jsonSchemaData = new JsonSchemaData();
  }

  @Test
  public void testFromConnectNumericDecimal() {
    jsonSchemaData =
        new JsonSchemaData(new JsonSchemaDataConfig(
            Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG,
                DecimalFormat.NUMERIC.name())));
    NumberSchema schema = NumberSchema.builder()
        .title("org.apache.kafka.connect.data.Decimal")
        .unprocessedProperties(ImmutableMap.of("connect.type",
            "bytes",
            "connect.version",
            1,
            "connect.parameters",
            ImmutableMap.of("scale", "2")
        ))
        .build();
    checkNonObjectConversion(schema,
        DecimalNode.valueOf(new BigDecimal("1.56")),
        Decimal.schema(2),
        new BigDecimal(new BigInteger("156"), 2)
    );
    jsonSchemaData = new JsonSchemaData();
  }

  @Test
  public void testFromConnectNumericDecimalWithTrailingZeros() {
    jsonSchemaData =
        new JsonSchemaData(new JsonSchemaDataConfig(
            Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG,
                DecimalFormat.NUMERIC.name())));
    NumberSchema schema = NumberSchema.builder()
        .title("org.apache.kafka.connect.data.Decimal")
        .unprocessedProperties(ImmutableMap.of("connect.type",
            "bytes",
            "connect.version",
            1,
            "connect.parameters",
            ImmutableMap.of("scale", "4")
        ))
        .build();
    checkNonObjectConversion(schema,
        DecimalNode.valueOf(new BigDecimal("1.5600")),
        Decimal.schema(4),
        new BigDecimal(new BigInteger("15600"), 4)
    );
    jsonSchemaData = new JsonSchemaData();
  }

  @Test
  public void testFromConnectTimestampWithDefault() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .title("org.apache.kafka.connect.data.Timestamp")
        .defaultValue(0L)
        .unprocessedProperties(ImmutableMap.of("connect.type",
            "int64",
            "connect.version",
            1
        ))
        .build();
    checkNonObjectConversion(schema,
        LongNode.valueOf(1234567890),
        Timestamp.builder().defaultValue(new Date(0)).build(),
        new Date(1234567890)
    );
  }

  @Test
  public void testFromConnectMapWithStringKey() {
    Schema connectSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA);
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
        .build();
    ObjectSchema expected = ObjectSchema.builder()
        .schemaOfAdditionalProperties(numberSchema)
        .unprocessedProperties(Collections.singletonMap("connect.type", "map"))
        .build();

    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("hi", IntNode.valueOf(32));
    checkNonObjectConversion(expected, obj, connectSchema, Collections.singletonMap("hi", 32));
  }

  @Test
  public void testFromConnectMapWithOptionalKey() {
    Schema connectSchema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA);
    StringSchema stringSchema = StringSchema.builder().build();
    CombinedSchema keySchema = CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE,
        stringSchema
    ))
        .unprocessedProperties(ImmutableMap.of("connect.index", 0))
        .build();
    NumberSchema valueSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 1, "connect.type", "int32"))
        .build();
    ObjectSchema entrySchema = ObjectSchema.builder()
        .addPropertySchema(KEY_FIELD, keySchema)
        .addPropertySchema(VALUE_FIELD, valueSchema)
        .build();
    ArraySchema expected = ArraySchema.builder()
        .allItemSchema(entrySchema)
        .unprocessedProperties(Collections.singletonMap("connect.type", "map"))
        .build();

    ObjectNode entry = JsonNodeFactory.instance.objectNode();
    entry.set(KEY_FIELD, TextNode.valueOf("hi"));
    entry.set(VALUE_FIELD, IntNode.valueOf(32));
    ArrayNode array = JsonNodeFactory.instance.arrayNode().add(entry);
    checkNonObjectConversion(expected, array, connectSchema, Collections.singletonMap("hi", 32));
  }

  @Test
  public void testFromConnectMapWithNonStringKey() {
    Schema connectSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA);
    NumberSchema keySchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int32"))
        .build();
    NumberSchema valueSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 1, "connect.type", "int32"))
        .build();
    ObjectSchema entrySchema = ObjectSchema.builder()
        .addPropertySchema(KEY_FIELD, keySchema)
        .addPropertySchema(VALUE_FIELD, valueSchema)
        .build();
    ArraySchema expected = ArraySchema.builder()
        .allItemSchema(entrySchema)
        .unprocessedProperties(Collections.singletonMap("connect.type", "map"))
        .build();

    ObjectNode entry = JsonNodeFactory.instance.objectNode();
    entry.set(KEY_FIELD, IntNode.valueOf(54));
    entry.set(VALUE_FIELD, IntNode.valueOf(32));
    ArrayNode array = JsonNodeFactory.instance.arrayNode().add(entry);
    checkNonObjectConversion(expected, array, connectSchema, Collections.singletonMap(54, 32));
  }

  @Test
  public void testFromNamedConnectMap() {
    assertEquals(
        jsonSchemaData.fromConnectSchema(NAMED_MAP_SCHEMA).rawSchema(), NAMED_JSON_MAP_SCHEMA);
  }

  @Test
  public void testFromConnectRecordWithMissingNonoptional() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder()
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("int8", numberSchema)
        .addPropertySchema("string", stringSchema)
        .title("Record")
        .build();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("int8", ShortNode.valueOf((short) 12));
    // The string field is not set
    Schema actualSchema = SchemaBuilder.struct()
        .name("Record")
        .field("int8", Schema.INT8_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(actualSchema).put("int8", (byte) 12);
    checkNonObjectConversion(schema, obj, actualSchema, struct);
  }

  private void checkNonObjectConversion(
      org.everit.json.schema.Schema expectedSchema, Object expected, Schema schema, Object value
  ) {
    JsonSchema jsonSchema = jsonSchemaData.fromConnectSchema(schema);
    JsonNode jsonValue = jsonSchemaData.fromConnectData(schema, value);
    assertEquals(expectedSchema, jsonSchema != null ? jsonSchema.rawSchema() : null);
    assertEquals(expected, jsonValue);
  }

  // JSON Schema -> Connect: directly corresponding types

  @Test
  public void testToConnectNull() {
    BooleanSchema booleanSchema = BooleanSchema.builder().build();
    CombinedSchema schema =
        CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE, booleanSchema)).build();
    Schema expectedSchema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
    checkNonObjectConversion(expectedSchema, null, schema, null);

    checkNonObjectConversion((Schema) null, null, null, null);
  }

  @Test
  public void testToConnectBoolean() {
    BooleanSchema schema = BooleanSchema.builder().build();
    Schema expectedSchema = Schema.BOOLEAN_SCHEMA;
    checkNonObjectConversion(expectedSchema, true, schema, BooleanNode.getTrue());
  }

  @Test
  public void testToConnectInt8() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .build();
    Schema expectedSchema = Schema.INT8_SCHEMA;
    checkNonObjectConversion(expectedSchema, (byte) 12, schema, ShortNode.valueOf((short) 12));
  }

  @Test
  public void testToConnectInt8WithDefault() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .defaultValue((byte) 34)
        .build();
    Schema expectedSchema = SchemaBuilder.int8().defaultValue((byte) 34).build();
    checkNonObjectConversion(expectedSchema, (byte) 12, schema, ShortNode.valueOf((short) 12));
  }

  @Test
  public void testToConnectInt8WithDefaultConversion() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .defaultValue(34)
        .build();
    Schema expectedSchema = SchemaBuilder.int8().defaultValue((byte) 34).build();
    checkNonObjectConversion(expectedSchema, (byte) 12, schema, IntNode.valueOf(12));
  }

  @Test
  public void testToConnectInt16() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
        .build();
    Schema expectedSchema = Schema.INT16_SCHEMA;
    checkNonObjectConversion(expectedSchema, (short) 12, schema, ShortNode.valueOf((short) 12));
  }

  @Test
  public void testToConnectInt32() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
        .build();
    Schema expectedSchema = Schema.INT32_SCHEMA;
    checkNonObjectConversion(expectedSchema, 12, schema, IntNode.valueOf(12));
  }

  @Test
  public void testToConnectInt64() {
    NumberSchema schema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int64"))
        .build();
    Schema expectedSchema = Schema.INT64_SCHEMA;
    checkNonObjectConversion(expectedSchema, 12L, schema, LongNode.valueOf(12L));
  }

  @Test
  public void testToConnectFloat32() {
    NumberSchema schema = NumberSchema.builder()
        .unprocessedProperties(Collections.singletonMap("connect.type", "float32"))
        .build();
    Schema expectedSchema = Schema.FLOAT32_SCHEMA;
    checkNonObjectConversion(expectedSchema, 12.f, schema, FloatNode.valueOf(12.f));
  }

  @Test
  public void testToConnectFloat64() {
    NumberSchema schema = NumberSchema.builder()
        .unprocessedProperties(Collections.singletonMap("connect.type", "float64"))
        .build();
    Schema expectedSchema = Schema.FLOAT64_SCHEMA;
    checkNonObjectConversion(expectedSchema, 12.0, schema, DoubleNode.valueOf(12.0));
  }

  @Test
  public void testToConnectNullableStringNullvalue() {
    CombinedSchema schema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(StringSchema.builder().build())
        .build();
    Schema expectedSchema = Schema.OPTIONAL_STRING_SCHEMA;
    checkNonObjectConversion(expectedSchema, null, schema, NullNode.getInstance());
  }

  @Test
  public void testToConnectNullableString() {
    CombinedSchema schema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(StringSchema.builder().build())
        .build();
    Schema expectedSchema = Schema.OPTIONAL_STRING_SCHEMA;
    checkNonObjectConversion(expectedSchema, "teststring", schema, TextNode.valueOf("teststring"));
  }

  @Test
  public void testToConnectString() {
    StringSchema schema = StringSchema.builder().build();
    Schema expectedSchema = Schema.STRING_SCHEMA;
    checkNonObjectConversion(expectedSchema, "teststring", schema, TextNode.valueOf("teststring"));
  }

  @Test
  public void testToConnectBytes() {
    StringSchema schema = StringSchema.builder()
        .unprocessedProperties(Collections.singletonMap("connect.type", "bytes"))
        .build();
    Schema expectedSchema = Schema.BYTES_SCHEMA;
    checkNonObjectConversion(expectedSchema,
        "teststring".getBytes(),
        schema,
        BinaryNode.valueOf("teststring".getBytes())
    );
  }

  @Test
  public void testToConnectArray() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .build();
    ArraySchema schema = ArraySchema.builder().allItemSchema(numberSchema).build();
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema expectedSchema = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
    ArrayNode array = JsonNodeFactory.instance.arrayNode();
    array.add(12).add(13);
    checkNonObjectConversion(expectedSchema, Arrays.asList((byte) 12, (byte) 13), schema, array);
  }

  @Test
  public void testToConnectMapStringKeys() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .schemaOfAdditionalProperties(numberSchema)
        .unprocessedProperties(Collections.singletonMap("connect.type", "map"))
        .build();
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema expectedSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT8_SCHEMA).build();
    ObjectNode map = JsonNodeFactory.instance.objectNode();
    map.set("field", IntNode.valueOf(12));
    checkNonObjectConversion(expectedSchema,
        Collections.singletonMap("field", (byte) 12),
        schema,
        map
    );
  }

  @Test
  public void testToConnectMapNonStringKeys() {
    NumberSchema keySchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .build();
    NumberSchema valueSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
        .build();
    ObjectSchema mapSchema = ObjectSchema.builder()
        .addPropertySchema("key", keySchema)
        .addPropertySchema("value", valueSchema)
        .build();
    ArraySchema schema = ArraySchema.builder()
        .allItemSchema(mapSchema)
        .unprocessedProperties(Collections.singletonMap("connect.type", "map"))
        .build();
    Schema expectedSchema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT16_SCHEMA).build();
    ObjectNode map = JsonNodeFactory.instance.objectNode();
    map.set("key", IntNode.valueOf(12));
    map.set("value", ShortNode.valueOf((short) 16));
    ArrayNode array = JsonNodeFactory.instance.arrayNode();
    array.add(map);
    checkNonObjectConversion(expectedSchema,
        Collections.singletonMap((byte) 12, (short) 16),
        schema,
        array
    );
  }

  @Test
  public void testToConnectRecord() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder()
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("int8", numberSchema)
        .addPropertySchema("string", stringSchema)
        .title("Record")
        .build();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("int8", ShortNode.valueOf((short) 12));
    obj.set("string", TextNode.valueOf("sample string"));
    Schema expectedSchema = SchemaBuilder.struct()
        .name("Record")
        .field("int8", Schema.INT8_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(expectedSchema).put("int8", (byte) 12)
        .put("string", "sample string");
    checkNonObjectConversion(expectedSchema, struct, schema, obj);
  }

  @Test
  public void testToConnectRecordWithOptionalValue() {
    testToConnectRecordWithOptional("sample string");
  }

  @Test
  public void testToConnectRecordWithOptionalNullValue() {
    testToConnectRecordWithOptional(null);
  }

  private void testToConnectRecordWithOptional(String value) {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder().build();
    CombinedSchema combinedSchema = CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE,
        stringSchema
    ))
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("int8", numberSchema)
        .addPropertySchema("string", combinedSchema)
        .title("Record")
        .build();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("int8", ShortNode.valueOf((short) 12));
    if (value == null) {
      obj.set("string", NullNode.getInstance());
    } else {
      obj.set("string", TextNode.valueOf(value));
    }
    Schema expectedSchema = SchemaBuilder.struct()
        .name("Record")
        .field("int8", Schema.INT8_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct struct = new Struct(expectedSchema).put("int8", (byte) 12).put("string", value);
    checkNonObjectConversion(expectedSchema, struct, schema, obj);
  }

  @Test
  public void testToConnectRecordWithNoAdditionalProperties() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder()
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("int8", numberSchema)
        .addPropertySchema("string", stringSchema)
        .additionalProperties(false)
        .title("Record")
        .build();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("int8", ShortNode.valueOf((short) 12));
    Schema expectedSchema = SchemaBuilder.struct()
        .name("Record")
        .field("int8", Schema.INT8_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(expectedSchema).put("int8", (byte) 12);
    JsonSchemaDataConfig jsonSchemaDataConfig = new JsonSchemaDataConfig.Builder()
        .with(JsonSchemaDataConfig.OBJECT_ADDITIONAL_PROPERTIES_CONFIG, false)
        .build();
    JsonSchemaData jsonSchemaData = new JsonSchemaData(jsonSchemaDataConfig);
    checkNonObjectConversion(jsonSchemaData, expectedSchema, struct, schema, obj);
  }

  @Test
  public void testToConnectRecordWithOptionalAndRequired() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder()
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("int8", numberSchema)
        .addPropertySchema("string", stringSchema)
        .addRequiredProperty("int8")
        .title("Record")
        .build();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("int8", ShortNode.valueOf((short) 12));
    Schema expectedSchema = SchemaBuilder.struct()
        .name("Record")
        .field("int8", Schema.INT8_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct struct = new Struct(expectedSchema).put("int8", (byte) 12);
    JsonSchemaDataConfig jsonSchemaDataConfig = new JsonSchemaDataConfig.Builder()
        .with(JsonSchemaDataConfig.USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG, true)
        .build();
    JsonSchemaData jsonSchemaData = new JsonSchemaData(jsonSchemaDataConfig);
    checkNonObjectConversion(jsonSchemaData, expectedSchema, struct, schema, obj);
  }

  @Test
  public void testToConnectRecordWithOptionalReference() throws Exception {
    String schemaString = "{\n"
        + "  \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n"
        + "  \"title\": \"Event\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"vehicle\": {\n"
        + "      \"$ref\": \"#/definitions/VehicleDto\"\n"
        + "    }\n"
        + "  },\n"
        + "  \"definitions\": {\n"
        + "    \"PriceDto\": {\n"
        + "      \"type\": \"object\",\n"
        + "      \"required\": [\n"
        + "        \"amount\"\n"
        + "      ],\n"
        + "      \"properties\": {\n"
        + "        \"amount\": {\n"
        + "          \"type\": \"number\",\n"
        + "          \"format\": \"decimal\"\n"
        + "        }\n"
        + "      }\n"
        + "    },\n"
        + "    \"VehicleDto\": {\n"
        + "      \"type\": \"object\",\n"
        + "      \"required\": [\n"
        + "        \"offerPrice\",\n"
        + "        \"firstRegistrationDate\"\n"
        + "      ],\n"
        + "      \"properties\": {\n"
        + "        \"offerPrice\": {\n"
        + "          \"connect.index\": 1,\n"
        + "          \"$ref\": \"#/definitions/PriceDto\"\n"
        + "        },\n"
        + "        \"catalogPrice\": {\n"
        + "          \"connect.index\": 2,\n"
        + "          \"oneOf\": [\n"
        + "            {\n"
        + "              \"type\": \"null\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"$ref\": \"#/definitions/PriceDto\"\n"
        + "            }\n"
        + "          ]\n"
        + "        },\n"
        + "        \"firstRegistrationDate\": {\n"
        + "          \"connect.index\": 3,\n"
        + "          \"type\": \"string\",\n"
        + "          \"format\": \"date-time\"\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
    JsonSchema jsonSchema = new JsonSchema(schemaString);
    ObjectSchema schema = (ObjectSchema) jsonSchema.rawSchema();

    String json = "{\"vehicle\":{\"offerPrice\":{\"amount\":20000},\"firstRegistrationDate\":\"2020-08-19T12:05:15.953Z\"}}";
    ObjectNode obj = (ObjectNode) Jackson.newObjectMapper().readTree(json);

    JsonSchemaDataConfig jsonSchemaDataConfig = new JsonSchemaDataConfig.Builder()
        .with(JsonSchemaDataConfig.USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG, true)
        .build();
    JsonSchemaData jsonSchemaData = new JsonSchemaData(jsonSchemaDataConfig);

    Schema connectSchema = jsonSchemaData.toConnectSchema(schema);
    Struct connectStruct = (Struct) jsonSchemaData.toConnectData(connectSchema, obj);

    Schema vehicleSchema = connectSchema.field("vehicle").schema();
    Schema offerPriceSchema = vehicleSchema.field("offerPrice").schema();
    Schema catalogPriceSchema = vehicleSchema.field("catalogPrice").schema();
    assertFalse(offerPriceSchema.isOptional());
    assertTrue(catalogPriceSchema.isOptional());

    Struct vehicleStruct = (Struct) connectStruct.get("vehicle");
    Struct offerPriceStruct = (Struct) vehicleStruct.get("offerPrice");
    Struct catalogPriceStruct = (Struct) vehicleStruct.get("catalogPrice");
    assertNotNull(offerPriceStruct);
    assertNull(catalogPriceStruct);
  }

  @Test
  public void testToConnectRecordWithMissingNonoptional() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder()
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("int8", numberSchema)
        .addPropertySchema("string", stringSchema)
        .title("Record")
        .build();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("int8", ShortNode.valueOf((short) 12));
    // The string field is not set
    Schema expectedSchema = SchemaBuilder.struct()
        .name("Record")
        .field("int8", Schema.INT8_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(expectedSchema).put("int8", (byte) 12);
    checkNonObjectConversion(expectedSchema, struct, schema, obj);
  }

  @Test
  public void testToConnectRecordWithOptionalArrayValue() {
    testToConnectRecordWithOptionalArray(Arrays.asList("test"));
  }

  @Test
  public void testToConnectRecordWithOptionalArrayNullValue() {
    testToConnectRecordWithOptionalArray(null);
  }

  private void testToConnectRecordWithOptionalArray(java.util.List<String> value) {
    StringSchema stringSchema = StringSchema.builder().build();
    CombinedSchema combinedSchema1 = CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE,
        stringSchema
    ))
        .unprocessedProperties(ImmutableMap.of("connect.index", 0))
        .build();
    ArraySchema arraySchema = ArraySchema.builder().allItemSchema(stringSchema).build();
    CombinedSchema combinedSchema2 = CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE,
        arraySchema
    ))
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("string", combinedSchema1)
        .addPropertySchema("array", combinedSchema2)
        .title("Record")
        .build();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("string", TextNode.valueOf("xx"));
    if (value == null) {
      obj.set("array", NullNode.getInstance());
    } else {
      ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
      for (String s : value) {
        arrayNode.add(s);
      }
      obj.set("array", arrayNode);
    }
    Schema expectedSchema = SchemaBuilder.struct()
        .name("Record")
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .build();
    Struct struct = new Struct(expectedSchema).put("string", "xx").put("array", value);
    checkNonObjectConversion(expectedSchema, struct, schema, obj);
  }

  @Test
  public void testToConnectNestedRecordWithOptionalRecordValue() {
    ObjectSchema nested = ObjectSchema.builder().addPropertySchema("string",
        StringSchema.builder().unprocessedProperties(ImmutableMap.of("connect.index", 0)).build()
    ).title("nestedRecord").build();
    CombinedSchema combinedSchema = CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE,
        nested
    ))
        .unprocessedProperties(ImmutableMap.of("connect.index", 0))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("nestedRecord", combinedSchema)
        .title("Record")
        .build();
    Schema expectedSchema = nestedRecordSchema();
    ObjectNode nestedObj = JsonNodeFactory.instance.objectNode();
    nestedObj.set("string", TextNode.valueOf("xx"));
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("nestedRecord", nestedObj);
    Struct struct = new Struct(expectedSchema).put("nestedRecord",
        new Struct(recordWithStringSchema()).put("string", "xx")
    );
    checkNonObjectConversion(expectedSchema, struct, schema, obj);
  }

  @Test
  public void testToConnectNestedRecordWithOptionalRecordNullValue() {
    ObjectSchema nested = ObjectSchema.builder().addPropertySchema("string",
        StringSchema.builder().unprocessedProperties(ImmutableMap.of("connect.index", 0)).build()
    ).title("nestedRecord").build();
    CombinedSchema combinedSchema = CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE,
        nested
    ))
        .unprocessedProperties(ImmutableMap.of("connect.index", 0))
        .build();
    ObjectSchema schema = ObjectSchema.builder()
        .addPropertySchema("nestedRecord", combinedSchema)
        .title("Record")
        .build();
    Schema expectedSchema = nestedRecordSchema();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("nestedRecord", NullNode.getInstance());
    Struct struct = new Struct(expectedSchema).put("nestedRecord", null);
    checkNonObjectConversion(expectedSchema, struct, schema, obj);
  }

  private Schema recordWithStringSchema() {
    return SchemaBuilder.struct()
        .optional()
        .name("nestedRecord")
        .field("string", Schema.STRING_SCHEMA)
        .build();
  }

  private Schema nestedRecordSchema() {
    return SchemaBuilder.struct()
        .name("Record")
        .field("nestedRecord", recordWithStringSchema())
        .build();
  }

  @Test
  public void testToConnectBase64Decimal() {
    NumberSchema schema = NumberSchema.builder()
        .title("org.apache.kafka.connect.data.Decimal")
        .unprocessedProperties(ImmutableMap.of("connect.type",
            "bytes",
            "connect.parameters",
            ImmutableMap.of("scale", "2")
        ))
        .build();
    BigDecimal reference = new BigDecimal(new BigInteger("156"), 2);
    Schema expectedSchema = SchemaBuilder.bytes()
        .name(LOGICAL_NAME)
        .parameter(SCALE_FIELD, Integer.toString(2))
        .build();
    checkNonObjectConversion(expectedSchema, reference, schema,
        BinaryNode.valueOf(new byte[]{0, -100}));
  }

  @Test
  public void testToConnectNumericDecimal() {
    NumberSchema schema = NumberSchema.builder()
        .title("org.apache.kafka.connect.data.Decimal")
        .unprocessedProperties(ImmutableMap.of("connect.type",
            "bytes",
            "connect.parameters",
            ImmutableMap.of("scale", "2")
        ))
        .build();
    BigDecimal reference = new BigDecimal(new BigInteger("156"), 2);
    Schema expectedSchema = SchemaBuilder.bytes()
        .name(LOGICAL_NAME)
        .parameter(SCALE_FIELD, Integer.toString(2))
        .build();
    checkNonObjectConversion(expectedSchema, reference, schema, DecimalNode.valueOf(reference));
  }

  @Test
  public void testToConnectNumericDecimalWithTrailingZeros() {
    NumberSchema schema = NumberSchema.builder()
        .title("org.apache.kafka.connect.data.Decimal")
        .unprocessedProperties(ImmutableMap.of("connect.type",
            "bytes",
            "connect.parameters",
            ImmutableMap.of("scale", "4")
        ))
        .build();
    BigDecimal reference = new BigDecimal(new BigInteger("15600"), 4);
    Schema expectedSchema = SchemaBuilder.bytes()
        .name(LOGICAL_NAME)
        .parameter(SCALE_FIELD, Integer.toString(4))
        .build();
    checkNonObjectConversion(expectedSchema, reference, schema, DecimalNode.valueOf(reference));
  }

  @Test
  public void testToConnectHighPrecisionNumericDecimal() {
    NumberSchema schema = NumberSchema.builder()
        .title("org.apache.kafka.connect.data.Decimal")
        .unprocessedProperties(ImmutableMap.of("connect.type",
            "bytes",
            "connect.parameters",
            ImmutableMap.of("scale", "17")
        ))
        .build();
    // this number is too big to be kept in a float64!
    BigDecimal reference = new BigDecimal("1.23456789123456789");
    Schema expectedSchema = SchemaBuilder.bytes()
        .name(LOGICAL_NAME)
        .parameter(SCALE_FIELD, Integer.toString(17))
        .build();
    checkNonObjectConversion(expectedSchema, reference, schema, DecimalNode.valueOf(reference));
  }

  @Test
  public void testToConnectEnum() {
    EnumSchema schema = EnumSchema.builder()
        .possibleValue("one")
        .possibleValue("two")
        .possibleValue("three")
        .build();
    Schema expectedSchema = new SchemaBuilder(Schema.Type.STRING).parameter(JSON_TYPE_ENUM, "")
        .parameter(JSON_TYPE_ENUM + ".one", "one")
        .parameter(JSON_TYPE_ENUM + ".two", "two")
        .parameter(JSON_TYPE_ENUM + ".three", "three")
        .build();

    checkNonObjectConversion(expectedSchema, "one", schema, TextNode.valueOf("one"));
  }

  @Test
  public void testToConnectEnumInAllOf() {
    StringSchema stringSchema = StringSchema.builder().build();
    EnumSchema enumSchema = EnumSchema.builder()
        .possibleValue("one")
        .possibleValue("two")
        .possibleValue("three")
        .build();
    List<org.everit.json.schema.Schema> schemas = new ArrayList<>();
    schemas.add(stringSchema);
    schemas.add(enumSchema);
    CombinedSchema schema = CombinedSchema.allOf(schemas).build();
    Schema expectedSchema = new SchemaBuilder(Schema.Type.STRING).parameter(JSON_TYPE_ENUM, "")
        .parameter(JSON_TYPE_ENUM + ".one", "one")
        .parameter(JSON_TYPE_ENUM + ".two", "two")
        .parameter(JSON_TYPE_ENUM + ".three", "three")
        .build();

    checkNonObjectConversion(expectedSchema, "one", schema, TextNode.valueOf("one"));
  }

  @Test
  public void testToConnectIntEnumInAllOfIsValid() {
    NumberSchema numberSchema = NumberSchema.builder().build();
    EnumSchema enumSchema = EnumSchema.builder()
        .possibleValue(1)
        .possibleValue(2)
        .possibleValue(3)
        .build();
    List<org.everit.json.schema.Schema> schemas = new ArrayList<>();
    schemas.add(numberSchema);
    schemas.add(enumSchema);
    CombinedSchema schema = CombinedSchema.allOf(schemas).build();
    Schema expectedSchema = new SchemaBuilder(Schema.Type.FLOAT64).build();

    checkNonObjectConversion(expectedSchema, 123.45, schema, DoubleNode.valueOf(123.45));
  }

  @Test
  public void testToConnectUnionEnumInAllOfIsValid() {
    NumberSchema firstSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .build();
    NumberSchema secondSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
        .build();
    CombinedSchema oneof = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    EnumSchema enumSchema = EnumSchema.builder()
        .possibleValue(1)
        .possibleValue(2)
        .possibleValue(3)
        .build();
    List<org.everit.json.schema.Schema> schemas = new ArrayList<>();
    schemas.add(oneof);
    schemas.add(enumSchema);
    CombinedSchema schema = CombinedSchema.allOf(schemas).build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", Schema.OPTIONAL_INT8_SCHEMA);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", Schema.OPTIONAL_INT16_SCHEMA);
    Schema expectedSchema = builder.build();

    Struct expected = new Struct(expectedSchema).put(JSON_TYPE_ONE_OF + ".field.0", (byte) 12);
    checkNonObjectConversion(expectedSchema, expected, schema, ShortNode.valueOf((short) 12));
  }

  @Test
  public void testToConnectStringConstInAllOf() {
    JsonSchema jsonSchema = new JsonSchema("{\"const\":\"money\",\"type\":\"string\"}");
    CombinedSchema schema = (CombinedSchema) jsonSchema.rawSchema();
    Schema expectedSchema = new SchemaBuilder(Schema.Type.STRING).build();
    checkNonObjectConversion(expectedSchema, "money", schema, TextNode.valueOf("money"));
  }

  @Test
  public void testToConnectIntegerConstInAllOf() {
    JsonSchema jsonSchema = new JsonSchema("{\"const\":123,\"type\":\"integer\"}");
    CombinedSchema schema = (CombinedSchema) jsonSchema.rawSchema();
    Schema expectedSchema = new SchemaBuilder(Type.INT64).build();
    checkNonObjectConversion(expectedSchema, 123L, schema, LongNode.valueOf(123));
  }

  @Test
  public void testToConnectNumberConstInAllOf() {
    JsonSchema jsonSchema = new JsonSchema("{\"const\":123.45,\"type\":\"number\"}");
    CombinedSchema schema = (CombinedSchema) jsonSchema.rawSchema();
    Schema expectedSchema = new SchemaBuilder(Type.FLOAT64).build();
    checkNonObjectConversion(expectedSchema, 123.45, schema, DoubleNode.valueOf(123.45));
  }

  @Test
  public void testToConnectUnion() {
    NumberSchema firstSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .build();
    NumberSchema secondSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
        .build();
    CombinedSchema schema = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", Schema.OPTIONAL_INT8_SCHEMA);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", Schema.OPTIONAL_INT16_SCHEMA);
    Schema expectedSchema = builder.build();

    Struct expected = new Struct(expectedSchema).put(JSON_TYPE_ONE_OF + ".field.0", (byte) 12);
    checkNonObjectConversion(expectedSchema, expected, schema, ShortNode.valueOf((short) 12));
  }

  @Test
  public void testToConnectUnionSecondField() {
    StringSchema firstSchema = StringSchema.builder()
        .build();
    NumberSchema secondSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
        .build();
    CombinedSchema schema = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", Schema.OPTIONAL_INT32_SCHEMA);
    Schema expectedSchema = builder.build();

    Struct expected = new Struct(expectedSchema).put(JSON_TYPE_ONE_OF + ".field.1", 12);
    checkNonObjectConversion(expectedSchema, expected, schema, IntNode.valueOf(12));
  }

  @Test
  public void testToConnectUnionDifferentIntegralType() {
    StringSchema firstSchema = StringSchema.builder()
        .build();
    NumberSchema secondSchema = NumberSchema.builder()
        .requiresInteger(true)
        .build();
    CombinedSchema schema = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", Schema.OPTIONAL_INT64_SCHEMA);
    Schema expectedSchema = builder.build();

    Struct expected = new Struct(expectedSchema).put(JSON_TYPE_ONE_OF + ".field.1", 123L);
    // Pass an IntNode instead of a LongNode
    checkNonObjectConversion(expectedSchema, expected, schema, IntNode.valueOf(123));
  }

  @Test
  public void testToConnectUnionDifferentNumericType() {
    StringSchema firstSchema = StringSchema.builder()
        .build();
    NumberSchema secondSchema = NumberSchema.builder()
        .requiresNumber(true)
        .build();
    CombinedSchema schema = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", Schema.OPTIONAL_FLOAT64_SCHEMA);
    Schema expectedSchema = builder.build();

    Struct expected = new Struct(expectedSchema).put(JSON_TYPE_ONE_OF + ".field.1", (double) 123);
    // Pass an IntNode instead of a DoubleNode
    checkNonObjectConversion(expectedSchema, expected, schema, IntNode.valueOf(123));
  }

  @Test
  public void testToConnectUnionFromAnyOf() {
    NumberSchema firstSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
        .build();
    NumberSchema secondSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
        .build();
    CombinedSchema schema = CombinedSchema.anyOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", Schema.OPTIONAL_INT8_SCHEMA);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", Schema.OPTIONAL_INT16_SCHEMA);
    Schema expectedSchema = builder.build();

    Struct expected = new Struct(expectedSchema).put(JSON_TYPE_ONE_OF + ".field.0", (byte) 12);
    checkNonObjectConversion(expectedSchema, expected, schema, ShortNode.valueOf((short) 12));
  }

  @Test
  public void testToConnectUnionDifferentStruct() {
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.index", 0, "connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder()
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    ObjectSchema firstSchema = ObjectSchema.builder()
        .addPropertySchema("a", numberSchema)
        .addPropertySchema("b", stringSchema)
        .title("field0")
        .unprocessedProperties(ImmutableMap.of("connect.index", 0))
        .build();
    ObjectSchema secondSchema = ObjectSchema.builder()
        .addPropertySchema("c", numberSchema)
        .addPropertySchema("d", stringSchema)
        .title("field1")
        .unprocessedProperties(ImmutableMap.of("connect.index", 1))
        .build();
    CombinedSchema schema = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
        .build();
    Schema field0 = SchemaBuilder.struct()
        .name("field0")
        .field("a", Schema.INT8_SCHEMA)
        .field("b", Schema.STRING_SCHEMA)
        .optional()
        .build();
    Schema field1 = SchemaBuilder.struct()
        .name("field1")
        .field("c", Schema.INT8_SCHEMA)
        .field("d", Schema.STRING_SCHEMA)
        .optional()
        .build();
    SchemaBuilder builder = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF);
    builder.field(JSON_TYPE_ONE_OF + ".field.0", field0);
    builder.field(JSON_TYPE_ONE_OF + ".field.1", field1);
    Schema connectSchema = builder.build();

    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("a", ShortNode.valueOf((short) 12));
    obj.set("b", TextNode.valueOf("sample string"));
    Struct struct = new Struct(field0).put("a", (byte) 12)
        .put("b", "sample string");
    Struct expected = new Struct(connectSchema).put(JSON_TYPE_ONE_OF + ".field.0", struct);
    checkNonObjectConversion(connectSchema, expected, schema, obj);

    obj = JsonNodeFactory.instance.objectNode();
    obj.set("c", ShortNode.valueOf((short) 12));
    obj.set("d", TextNode.valueOf("sample string"));
    struct = new Struct(field1).put("c", (byte) 12)
        .put("d", "sample string");
    expected = new Struct(connectSchema).put(JSON_TYPE_ONE_OF + ".field.1", struct);
    checkNonObjectConversion(connectSchema, expected, schema, obj);
  }

  @Test
  public void testToConnectUnionSecondNestedSchemas() {
    StringSchema stringSchema = StringSchema.builder()
            .unprocessedProperties(ImmutableMap.of("connect.index", 0))
            .build();
    ObjectSchema firstSchemaNested = ObjectSchema.builder()
            .addPropertySchema("differentFieldNameA", stringSchema)
            .build();
    ObjectSchema firstSchema = ObjectSchema.builder()
            .addPropertySchema("commonFieldName", firstSchemaNested)
            .unprocessedProperties(ImmutableMap.of("connect.index", 0))
            .build();
    ObjectSchema secondSchemaNested = ObjectSchema.builder()
            .addPropertySchema("differentFieldNameB", stringSchema)
            .build();
    ObjectSchema secondSchema = ObjectSchema.builder()
            .addPropertySchema("commonFieldName", secondSchemaNested)
            .unprocessedProperties(ImmutableMap.of("connect.index", 1))
            .build();
    CombinedSchema schema = CombinedSchema.oneOf(ImmutableList.of(firstSchema, secondSchema))
            .build();

    Schema field0Nested = SchemaBuilder.struct().field("differentFieldNameA", Schema.STRING_SCHEMA).build();
    Schema field0 = SchemaBuilder.struct()
            .field("commonFieldName", field0Nested)
            .optional()
            .build();
    Schema field1Nested = SchemaBuilder.struct().field("differentFieldNameB", Schema.STRING_SCHEMA).build();
    Schema field1 = SchemaBuilder.struct()
            .field("commonFieldName", field1Nested)
            .optional()
            .build();
    Schema connectSchema = SchemaBuilder.struct().name(JSON_TYPE_ONE_OF)
            .field(JSON_TYPE_ONE_OF + ".field.0", field0)
            .field(JSON_TYPE_ONE_OF + ".field.1", field1)
            .build();

    ObjectNode firstObj = JsonNodeFactory.instance.objectNode()
            .set("differentFieldNameA", TextNode.valueOf("sample string A"));
    ObjectNode secondObj = JsonNodeFactory.instance.objectNode()
            .set("differentFieldNameB", TextNode.valueOf("sample string B"));
    Struct firstStruct = new Struct(field0Nested).put("differentFieldNameA", "sample string A");
    Struct secondStruct = new Struct(field1Nested).put("differentFieldNameB", "sample string B");

    ObjectNode obj = JsonNodeFactory.instance.objectNode().
            set("commonFieldName", firstObj);
    Struct struct = new Struct(field0).put("commonFieldName", firstStruct);
    Struct expected = new Struct(connectSchema).put(JSON_TYPE_ONE_OF + ".field.0", struct);
    checkNonObjectConversion(connectSchema, expected, schema, obj);

    obj = JsonNodeFactory.instance.objectNode().
            set("commonFieldName", secondObj);
    struct = new Struct(field1).put("commonFieldName", secondStruct);
    expected = new Struct(connectSchema).put(JSON_TYPE_ONE_OF + ".field.1", struct);
    checkNonObjectConversion(connectSchema, expected, schema, obj);
  }
  
  @Test
  public void testToConnectMapOptionalValue() {
    testToConnectMapOptional("some value");
  }

  @Test
  public void testToConnectMapOptionalNullValue() {
    testToConnectMapOptional(null);
  }

  private void testToConnectMapOptional(String value) {
    // Encoded as array of 2-tuple records. Use key and value types that require conversion to
    // make sure conversion of each element actually occurs.
    NumberSchema numberSchema = NumberSchema.builder()
        .requiresInteger(true)
        .unprocessedProperties(ImmutableMap.of("connect.type", "int8"))
        .build();
    StringSchema stringSchema = StringSchema.builder().build();
    CombinedSchema combinedSchema = CombinedSchema.oneOf(ImmutableList.of(NullSchema.INSTANCE,
        stringSchema
    )).build();
    ObjectSchema entrySchema = ObjectSchema.builder()
        .addPropertySchema("key", numberSchema)
        .addPropertySchema("value", combinedSchema)
        .build();
    ArraySchema schema = ArraySchema.builder()
        .allItemSchema(entrySchema)
        .unprocessedProperties(ImmutableMap.of("connect.type", "map"))
        .build();
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("key", ShortNode.valueOf((short) 12));
    if (value == null) {
      obj.set("value", NullNode.getInstance());
    } else {
      obj.set("value", TextNode.valueOf(value));
    }
    ArrayNode array = JsonNodeFactory.instance.arrayNode();
    array.add(obj);
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema expectedSchema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    checkNonObjectConversion(expectedSchema,
        Collections.singletonMap((byte) 12, value),
        schema,
        array
    );
  }

  @Test
  public void testToNamedConnectMap() {
    assertEquals(jsonSchemaData.toConnectSchema(NAMED_JSON_MAP_SCHEMA), NAMED_MAP_SCHEMA);
  }

  private void checkNonObjectConversion(
      Schema expectedSchema, Object expected, org.everit.json.schema.Schema schema, JsonNode value
  ) {
    checkNonObjectConversion(jsonSchemaData, expectedSchema, expected, schema, value);
  }

  private void checkNonObjectConversion(
      JsonSchemaData jsonSchemaData,
      Schema expectedSchema, Object expected, org.everit.json.schema.Schema schema, JsonNode value
  ) {
    Schema connectSchema = jsonSchemaData.toConnectSchema(schema);
    Object jsonValue = jsonSchemaData.toConnectData(connectSchema, value);
    if (connectSchema != null) {
      try {
        ConnectSchema.validateValue(connectSchema, jsonValue);
      } catch (DataException e) {
        assertTrue(e.getMessage().contains("Invalid value: null used for required field"));
      }
    }
    assertEquals(expectedSchema, connectSchema);
    if (expected instanceof byte[]) {
      assertArrayEquals((byte[]) expected, (byte[]) jsonValue);
    } else {
      assertEquals(expected, jsonValue);
    }
  }

  @Test
  public void testCombinedSchemaAllOfRef() {
    String schema = "{\n"
        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
        + "  \"allOf\": [\n"
        + "    { \"$ref\": \"#/definitions/MessageBase\" },\n"
        + "    { \"$ref\": \"#/definitions/MessageBase2\" }\n"
        + "  ],\n"
        + "\n"
        + "  \"title\": \"IdentifyUserMessage\",\n"
        + "  \"description\": \"An IdentifyUser message\",\n"
        + "  \"type\": \"object\",\n"
        + "\n"
        + "  \"properties\": {\n"
        + "    \"prevUserId\": {\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "\n"
        + "    \"newUserId\": {\n"
        + "      \"type\": \"string\"\n"
        + "    }\n"
        + "  },\n"
        + "\n"
        + "  \"definitions\": {\n"
        + "    \"MessageBase\": {\n"
        + "      \"properties\": {\n"
        + "        \"id\": {\n"
        + "          \"type\": \"string\"\n"
        + "        },\n"
        + "\n"
        + "        \"type\": {\n"
        + "          \"type\": \"string\"\n"
        + "        }\n"
        + "      }\n"
        + "    },\n"
        + "    \"MessageBase2\": {\n"
        + "      \"properties\": {\n"
        + "        \"id2\": {\n"
        + "          \"type\": \"string\"\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema connectSchema = jsonSchemaData.toConnectSchema(jsonSchema);
    assertNotNull(connectSchema.field("id"));
    assertNotNull(connectSchema.field("type"));
    assertNotNull(connectSchema.field("id2"));
    assertNotNull(connectSchema.field("newUserId"));
    assertNotNull(connectSchema.field("prevUserId"));
  }

  @Test
  public void testCombinedSchemaTitleAndObject() {
    String schema = "{\n"
        + "      \"title\": \"Product Identifier\",\n"
        + "      \"type\": \"object\",\n"
        + "      \"anyOf\": [\n"
        + "        {\n"
        + "          \"type\": \"object\",\n"
        + "          \"properties\": {\n"
        + "            \"keycode\": {\n"
        + "              \"type\": \"string\"\n"
        + "            }\n"
        + "          }\n"
        + "        },\n"
        + "        {\n"
        + "          \"type\": \"object\",\n"
        + "          \"properties\": {\n"
        + "            \"itemCode\": {\n"
        + "              \"type\": \"string\"\n"
        + "            }\n"
        + "          }\n"
        + "        }\n"
        + "      ]\n"
        + "    }";
    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema connectSchema = jsonSchemaData.toConnectSchema(jsonSchema);
    assertEquals("io.confluent.connect.json.OneOf", connectSchema.name());
  }

  @Test
  public void testNumericFormats() {
    String schema = "{\n"
        + "  \"title\": \"numerics with format\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"id\": {\n"
        + "      \"type\": \"integer\",\n"
        + "      \"format\": \"int32\"\n"
        + "    },\n"
        + "    \"num\": {\n"
        + "      \"type\": \"number\",\n"
        + "      \"format\": \"double\"\n"
        + "    }\n"
        + "  },\n"
        + "  \"required\": [\n"
        + "    \"id\"\n"
        + "  ],\n"
        + "  \"additionalProperties\": false\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema connectSchema = jsonSchemaData.toConnectSchema(jsonSchema);
    assertFalse(connectSchema.field("id").schema().isOptional());
    assertFalse(connectSchema.field("num").schema().isOptional());
  }

  @Test
  public void testOptionalNumericFormats() {
    String schema = "{\n"
        + "  \"title\": \"numerics with format\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"id\": {\n"
        + "      \"format\": \"int32\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"number\"\n"
        + "      ]"
        + "    },\n"
        + "    \"num\": {\n"
        + "      \"format\": \"double\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"integer\"\n"
        + "      ]"
        + "    }\n"
        + "  },\n"
        + "  \"required\": [\n"
        + "    \"id\"\n"
        + "  ],\n"
        + "  \"additionalProperties\": false\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema connectSchema = jsonSchemaData.toConnectSchema(jsonSchema);
    assertTrue(connectSchema.field("id").schema().isOptional());
    assertTrue(connectSchema.field("num").schema().isOptional());
  }

  @Test
  public void testStringFormats() {
    String schema = "{\n"
        + "  \"title\": \"strings with format\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"id\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"format\": \"password\"\n"
        + "    },\n"
        + "    \"str\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"format\": \"uuid\"\n"
        + "    }\n"
        + "  },\n"
        + "  \"required\": [\n"
        + "    \"id\"\n"
        + "  ],\n"
        + "  \"additionalProperties\": false\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema connectSchema = jsonSchemaData.toConnectSchema(jsonSchema);
    assertFalse(connectSchema.field("id").schema().isOptional());
    assertFalse(connectSchema.field("str").schema().isOptional());
  }

  @Test
  public void testOptionalStringFormats() {
    String schema = "{\n"
        + "  \"title\": \"strings with format\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"id\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"format\": \"password\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"string\"\n"
        + "      ]"
        + "    },\n"
        + "    \"str\": {\n"
        + "      \"format\": \"uuid\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"string\"\n"
        + "      ]"
        + "    }\n"
        + "  },\n"
        + "  \"required\": [\n"
        + "    \"id\"\n"
        + "  ],\n"
        + "  \"additionalProperties\": false\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema connectSchema = jsonSchemaData.toConnectSchema(jsonSchema);
    assertTrue(connectSchema.field("id").schema().isOptional());
    assertTrue(connectSchema.field("str").schema().isOptional());
  }

  @Test
  public void testOptionalReferencedSchema() {
    String schema = "{\n"
        + "    \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
        + "    \"title\": \"Test\",\n"
        + "    \"type\": \"object\",\n"
        + "    \"additionalProperties\": false,\n"
        + "    \"properties\": {\n"
        + "        \"id\": {\n"
        + "            \"type\": \"string\",\n"
        + "            \"description\": \"unique id\"\n"
        + "        },\n"
        + "        \"createById\": {\n"
        + "            \"oneOf\": [\n"
        + "                {\n"
        + "                    \"type\": \"null\",\n"
        + "                    \"title\": \"Not included\"\n"
        + "                },\n"
        + "                {\n"
        + "                    \"type\": \"string\"\n"
        + "                }\n"
        + "            ],\n"
        + "            \"description\": \"user id\"\n"
        + "        },\n"
        + "        \"complexNode\": {\n"
        + "            \"oneOf\": [\n"
        + "                {\n"
        + "                    \"type\": \"null\",\n"
        + "                    \"title\": \"Not included\"\n"
        + "                },\n"
        + "                {\n"
        + "                    \"$ref\": \"#/definitions/complexNode\"\n"
        + "                }\n"
        + "            ]\n"
        + "        }\n"
        + "    },\n"
        + "    \"required\": [\n"
        + "        \"id\"\n"
        + "    ],\n"
        + "    \"definitions\": {\n"
        + "        \"complexNode\": {\n"
        + "            \"type\": \"object\",\n"
        + "            \"additionalProperties\": false,\n"
        + "            \"properties\": {\n"
        + "                \"id\": {\n"
        + "                    \"type\": \"string\",\n"
        + "                    \"description\": \"unique id\"\n"
        + "                },\n"
        + "                \"createById\": {\n"
        + "                    \"oneOf\": [\n"
        + "                        {\n"
        + "                            \"type\": \"null\",\n"
        + "                            \"title\": \"Not included\"\n"
        + "                        },\n"
        + "                        {\n"
        + "                            \"type\": \"string\"\n"
        + "                        }\n"
        + "                    ],\n"
        + "                    \"description\": \"user id\"\n"
        + "                }\n"
        + "            },\n"
        + "            \"required\": [\n"
        + "                \"id\"\n"
        + "            ]\n"
        + "        }\n"
        + "    }\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema connectSchema = jsonSchemaData.toConnectSchema(jsonSchema);
    assertTrue(connectSchema.field("complexNode").schema().isOptional());
  }

  @Test
  public void testToConnectRecursiveSchema() {
    JsonSchema jsonSchema = getRecursiveJsonSchema();
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema expected = getRecursiveSchema();
    Schema actual = jsonSchemaData.toConnectSchema(jsonSchema);
    assertEquals(expected.field("title"), actual.field("title"));
    Schema expectedNested = expected.field("parent").schema();
    Schema actualNested = actual.field("parent").schema();
    assertEquals(expectedNested.name(), actualNested.name());
    assertEquals(expectedNested.type(), actualNested.type());
  }

  @Test
  public void testFromConnectRecursiveSchema() {
    JsonSchema expected = getRecursiveJsonSchema();
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    JsonSchema jsonSchema = jsonSchemaData.fromConnectSchema(getRecursiveSchema());
    assertEquals(expected, jsonSchema);
  }

  private JsonSchema getRecursiveJsonSchema() {
    String schema = "{\n"
        + "  \"type\": \"object\",\n"
        + "  \"title\": \"Task\",\n"
        + "  \"description\": \"A task\",\n"
        + "  \"id\": \"#id1\",\n"
        + "  \"properties\": {\n"
        + "    \"parent\": {\n"
        + "      \"$ref\": \"#id1\"\n"
        + "    },\n"
        + "    \"title\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"description\": \"Task title\",\n"
        + "      \"connect.index\": 1\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
    return new JsonSchema(schema);
  }

  private Schema getRecursiveSchema() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("Task");
    schemaBuilder.doc("A task");
    schemaBuilder.parameter(JsonSchemaData.JSON_ID_PROP, "#id1");
    schemaBuilder.field("parent",
        new SchemaWrapper(schemaBuilder, false)
            .build()
    );
    schemaBuilder.field("title",
        SchemaBuilder.string()
            .doc("Task title")
            .build()
    );
    return schemaBuilder.build();
  }

  @Test
  public void testToConnectRecursiveSchema2() {
    JsonSchema jsonSchema = getRecursiveJsonSchema2();
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    Schema expected = getRecursiveSchema2();
    Schema actual = jsonSchemaData.toConnectSchema(jsonSchema);
    assertEquals(expected.field("title"), actual.field("title"));
    Schema expectedNested = expected.field("foos").schema();
    Schema actualNested = actual.field("foos").schema();
    assertEquals(expectedNested.name(), actualNested.name());
    assertEquals(expectedNested.type(), actualNested.type());
  }

  @Test
  public void testFromConnectRecursiveSchema2() {
    JsonSchema expected = getRecursiveJsonSchema2();
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    JsonSchema jsonSchema = jsonSchemaData.fromConnectSchema(getRecursiveSchema2());
    assertEquals(expected.name(), jsonSchema.name());
  }

  private JsonSchema getRecursiveJsonSchema2() {
    String schema = "{\n"
        + "    \"title\": \"Foo\",\n"
        + "    \"$ref\": \"#/$defs/Foo\",\n"
        + "    \"$defs\": {\n"
        + "        \"Foo\": {\n"
        + "            \"properties\": {\n"
        + "                \"foos\": {\n"
        + "                    \"oneOf\": [\n"
        + "                        {\n"
        + "                            \"items\": {\n"
        + "                                \"$ref\": \"#/$defs/Foo\"\n"
        + "                            },\n"
        + "                            \"type\": \"array\"\n"
        + "                        },\n"
        + "                        {\n"
        + "                            \"type\": \"null\"\n"
        + "                        }\n"
        + "                    ],\n"
        + "                    \"items\": {\n"
        + "                        \"$ref\": \"#/$defs/Foo\"\n"
        + "                    },\n"
        + "                    \"type\": \"array\"\n"
        + "                }\n"
        + "            },\n"
        + "            \"type\": \"object\"\n"
        + "        }\n"
        + "    }\n"
        + "}";
    return new JsonSchema(schema);
  }

  private Schema getRecursiveSchema2() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("Foo");
    schemaBuilder.parameter(JsonSchemaData.JSON_ID_PROP, "#id1");
    Schema arraySchema = SchemaBuilder.array(schemaBuilder).build();
    schemaBuilder.field("foos", arraySchema);
    return schemaBuilder.build();
  }
}
