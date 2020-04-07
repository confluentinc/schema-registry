/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.serializers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AvroSchemaTest {

  private static ObjectMapper objectMapper = new ObjectMapper();

  private static final Schema.Parser parser = new Schema.Parser();

  private static final Schema recordSchema = new Schema.Parser().parse(
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"test\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"null\", \"type\": \"null\"},\n"
          + "     {\"name\": \"boolean\", \"type\": \"boolean\"},\n"
          + "     {\"name\": \"int\", \"type\": \"int\"},\n"
          + "     {\"name\": \"long\", \"type\": \"long\"},\n"
          + "     {\"name\": \"float\", \"type\": \"float\"},\n"
          + "     {\"name\": \"double\", \"type\": \"double\"},\n"
          + "     {\"name\": \"bytes\", \"type\": \"bytes\"},\n"
          + "     {\"name\": \"string\", \"type\": \"string\", \"aliases\": [\"string_alias\"]},\n"
          + "     {\"name\": \"null_default\", \"type\": \"null\", \"default\": null},\n"
          + "     {\"name\": \"boolean_default\", \"type\": \"boolean\", \"default\": false},\n"
          + "     {\"name\": \"int_default\", \"type\": \"int\", \"default\": 24},\n"
          + "     {\"name\": \"long_default\", \"type\": \"long\", \"default\": 4000000000},\n"
          + "     {\"name\": \"float_default\", \"type\": \"float\", \"default\": 12.3},\n"
          + "     {\"name\": \"double_default\", \"type\": \"double\", \"default\": 23.2},\n"
          + "     {\"name\": \"bytes_default\", \"type\": \"bytes\", \"default\": \"bytes\"},\n"
          + "     {\"name\": \"string_default\", \"type\": \"string\", \"default\": "
          + "\"default string\"}\n"
          + "]\n"
          + "}");

  private static final Schema arraySchema = new Schema.Parser().parse(
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"array\",\n"
          + " \"name\": \"test\",\n"
          + " \"items\": \"string\"\n"
          + "}");

  private static final Schema mapSchema = new Schema.Parser().parse(
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"map\",\n"
          + " \"name\": \"test\",\n"
          + " \"values\": \"string\"\n"
          + "}");

  private static final Schema unionSchema = new Schema.Parser().parse("{\"type\": \"record\",\n"
      + " \"name\": \"test\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"union\", \"type\": [\"string\", \"int\"]}\n"
      + "]}");


  private static final Schema enumSchema = new Schema.Parser().parse("{ \"type\": \"enum\",\n"
      + "  \"name\": \"Suit\",\n"
      + "  \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\n"
      + "}");

  @Test
  public void testPrimitiveTypesToAvro() throws Exception {
    Object result = AvroSchemaUtils.toObject((JsonNode) null, createPrimitiveSchema("null"));
    assertTrue(result == null);

    result = AvroSchemaUtils.toObject(jsonTree("true"), createPrimitiveSchema("boolean"));
    assertEquals(true, result);
    result = AvroSchemaUtils.toObject(jsonTree("false"), createPrimitiveSchema("boolean"));
    assertEquals(false, result);

    result = AvroSchemaUtils.toObject(jsonTree("12"), createPrimitiveSchema("int"));
    assertTrue(result instanceof Integer);
    assertEquals(12, result);

    result = AvroSchemaUtils.toObject(jsonTree("12"), createPrimitiveSchema("long"));
    assertTrue(result instanceof Long);
    assertEquals(12L, result);
    result = AvroSchemaUtils.toObject(jsonTree("5000000000"), createPrimitiveSchema("long"));
    assertTrue(result instanceof Long);
    assertEquals(5000000000L, result);

    result = AvroSchemaUtils.toObject(jsonTree("23.2"), createPrimitiveSchema("float"));
    assertTrue(result instanceof Float);
    assertEquals(23.2f, result);
    result = AvroSchemaUtils.toObject(jsonTree("23"), createPrimitiveSchema("float"));
    assertTrue(result instanceof Float);
    assertEquals(23.0f, result);

    result = AvroSchemaUtils.toObject(jsonTree("23.2"), createPrimitiveSchema("double"));
    assertTrue(result instanceof Double);
    assertEquals(23.2, result);
    result = AvroSchemaUtils.toObject(jsonTree("23"), createPrimitiveSchema("double"));
    assertTrue(result instanceof Double);
    assertEquals(23.0, result);

    // We can test bytes simply using simple ASCII string since the translation is direct in that
    // case
    result = AvroSchemaUtils.toObject(new TextNode("hello"), createPrimitiveSchema("bytes"));
    assertTrue(result instanceof ByteBuffer);
    assertArrayEquals(Base64.getEncoder().encode("hello".getBytes()),
        Base64.getEncoder().encode(((ByteBuffer) result).array())
    );

    result = AvroSchemaUtils.toObject(jsonTree("\"a string\""), createPrimitiveSchema("string"));
    assertTrue(result instanceof Utf8);
    assertEquals(new Utf8("a string"), result);
  }

  @Test
  public void testPrimitiveTypeToAvroSchemaMismatches() throws Exception {
    expectConversionException(jsonTree("12"), createPrimitiveSchema("null"));

    expectConversionException(jsonTree("12"), createPrimitiveSchema("boolean"));

    expectConversionException(jsonTree("false"), createPrimitiveSchema("int"));
    // Note that we don't test real numbers => int because JsonDecoder permits this and removes
    // the decimal part
    expectConversionException(jsonTree("5000000000"), createPrimitiveSchema("int"));

    expectConversionException(jsonTree("false"), createPrimitiveSchema("long"));
    // Note that we don't test real numbers => long because JsonDecoder permits this and removes
    // the decimal part

    expectConversionException(jsonTree("false"), createPrimitiveSchema("float"));

    expectConversionException(jsonTree("false"), createPrimitiveSchema("double"));

    expectConversionException(jsonTree("false"), createPrimitiveSchema("bytes"));

    expectConversionException(jsonTree("false"), createPrimitiveSchema("string"));
  }

  @Test
  public void testRecordToAvro() throws Exception {
    String json = "{\n"
        + "    \"null\": null,\n"
        + "    \"boolean\": true,\n"
        + "    \"int\": 12,\n"
        + "    \"long\": 5000000000,\n"
        + "    \"float\": 23.4,\n"
        + "    \"double\": 800.25,\n"
        + "    \"bytes\": \"hello\",\n"
        + "    \"string\": \"string\",\n"
        + "    \"null_default\": null,\n"
        + "    \"boolean_default\": false,\n"
        + "    \"int_default\": 24,\n"
        + "    \"long_default\": 4000000000,\n"
        + "    \"float_default\": 12.3,\n"
        + "    \"double_default\": 23.2,\n"
        + "    \"bytes_default\": \"bytes\",\n"
        + "    \"string_default\": \"default\"\n"
        + "}";

    Object result = AvroSchemaUtils.toObject(jsonTree(json), new AvroSchema(recordSchema));
    assertTrue(result instanceof GenericRecord);
    GenericRecord resultRecord = (GenericRecord) result;
    assertEquals(null, resultRecord.get("null"));
    assertEquals(true, resultRecord.get("boolean"));
    assertEquals(12, resultRecord.get("int"));
    assertEquals(5000000000L, resultRecord.get("long"));
    assertEquals(23.4f, resultRecord.get("float"));
    assertEquals(800.25, resultRecord.get("double"));
    assertArrayEquals(Base64.getEncoder().encode("hello".getBytes()),
        Base64.getEncoder().encode(((ByteBuffer) resultRecord.get("bytes")).array())
    );
    assertEquals("string", resultRecord.get("string").toString());
    // Nothing to check with default values, just want to make sure an exception wasn't thrown
    // when they values weren't specified for their fields.
  }

  @Test
  public void testArrayToAvro() throws Exception {
    String json = "[\"one\", \"two\", \"three\"]";

    Object result = AvroSchemaUtils.toObject(jsonTree(json), new AvroSchema(arraySchema));
    assertTrue(result instanceof GenericArray);
    assertArrayEquals(new Utf8[]{new Utf8("one"), new Utf8("two"), new Utf8("three")},
        ((GenericArray) result).toArray()
    );
  }

  @Test
  public void testMapToAvro() throws Exception {
    String json = "{\"first\": \"one\", \"second\": \"two\"}";

    Object result = AvroSchemaUtils.toObject(jsonTree(json), new AvroSchema(mapSchema));
    assertTrue(result instanceof Map);
    assertEquals(2, ((Map<String, Object>) result).size());
  }

  @Test
  public void testUnionToAvro() throws Exception {
    Object result = AvroSchemaUtils.toObject(jsonTree("{\"union\":{\"string\":\"test string\"}}"),
        new AvroSchema(unionSchema)
    );
    Object foo = ((GenericRecord) result).get("union");
    assertTrue(((GenericRecord) result).get("union") instanceof Utf8);

    result = AvroSchemaUtils.toObject(jsonTree("{\"union\":{\"int\":12}}"),
        new AvroSchema(unionSchema)
    );
    assertTrue(((GenericRecord) result).get("union") instanceof Integer);

    try {
      AvroSchemaUtils.toObject(jsonTree("12.4"), new AvroSchema(unionSchema));
      fail("Trying to convert floating point number to union(string,int) schema should fail");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testEnumToAvro() throws Exception {
    Object result = AvroSchemaUtils.toObject(jsonTree("\"SPADES\""), new AvroSchema(enumSchema));
    assertTrue(result instanceof GenericEnumSymbol);

    // There's no failure case here because the only failure mode is passing in non-string data.
    // Even if they put in an invalid symbol name, the exception won't be thrown until
    // serialization.
  }


  @Test
  public void testPrimitiveTypesToJson() throws Exception {
    JsonNode result = objectMapper.readTree(AvroSchemaUtils.toJson((int) 0));
    assertTrue(result.isNumber());

    result = objectMapper.readTree(AvroSchemaUtils.toJson((long) 0));
    assertTrue(result.isNumber());

    result = objectMapper.readTree(AvroSchemaUtils.toJson(0.1f));
    assertTrue(result.isNumber());

    result = objectMapper.readTree(AvroSchemaUtils.toJson(0.1));
    assertTrue(result.isNumber());

    result = objectMapper.readTree(AvroSchemaUtils.toJson(true));
    assertTrue(result.isBoolean());

    // "Primitive" here refers to Avro primitive types, which are returned as standalone objects,
    // which can't have attached schemas. This includes, for example, Strings and byte[] even
    // though they are not Java primitives

    result = objectMapper.readTree(AvroSchemaUtils.toJson("abcdefg"));
    assertTrue(result.isTextual());
    assertEquals("abcdefg", result.textValue());

    result = objectMapper.readTree(AvroSchemaUtils.toJson(ByteBuffer.wrap("hello".getBytes())));
    assertTrue(result.isTextual());
    // Was generated from a string, so the Avro encoding should be equivalent to the string
    assertEquals("hello", result.textValue());
  }

  @Test
  public void testUnsupportedJavaPrimitivesToJson() throws Exception {
    expectConversionException((byte) 0);
    expectConversionException((char) 0);
    expectConversionException((short) 0);
  }

  @Test
  public void testRecordToJson() throws Exception {
    GenericRecord data = new GenericRecordBuilder(recordSchema).set("null", null)
        .set("boolean", true)
        .set("int", 12)
        .set("long", 5000000000L)
        .set("float", 23.4f)
        .set("double", 800.25)
        .set("bytes", ByteBuffer.wrap("bytes".getBytes()))
        .set("string", "string")
        .build();

    JsonNode result = objectMapper.readTree(AvroSchemaUtils.toJson(data));
    assertTrue(result.isObject());
    assertTrue(result.get("null").isNull());
    assertTrue(result.get("boolean").isBoolean());
    assertEquals(true, result.get("boolean").booleanValue());
    assertTrue(result.get("int").isIntegralNumber());
    assertEquals(12, result.get("int").intValue());
    assertTrue(result.get("long").isIntegralNumber());
    assertEquals(5000000000L, result.get("long").longValue());
    assertTrue(result.get("float").isFloatingPointNumber());
    assertEquals(23.4f, result.get("float").floatValue(), 0.1);
    assertTrue(result.get("double").isFloatingPointNumber());
    assertEquals(800.25, result.get("double").doubleValue(), 0.01);
    assertTrue(result.get("bytes").isTextual());
    // The bytes value was created from an ASCII string, so Avro's encoding should just give that
    // string back to us in the JSON-serialized version
    assertEquals("bytes", result.get("bytes").textValue());
    assertTrue(result.get("string").isTextual());
    assertEquals("string", result.get("string").textValue());
  }

  @Test
  public void testArrayToJson() throws Exception {
    GenericData.Array<String> data = new GenericData.Array(arraySchema,
        Arrays.asList("one", "two", "three")
    );
    JsonNode result = objectMapper.readTree(AvroSchemaUtils.toJson(data));

    assertTrue(result.isArray());
    assertEquals(3, result.size());
    assertEquals(JsonNodeFactory.instance.textNode("one"), result.get(0));
    assertEquals(JsonNodeFactory.instance.textNode("two"), result.get(1));
    assertEquals(JsonNodeFactory.instance.textNode("three"), result.get(2));
  }

  @Test
  public void testMapToJson() throws Exception {
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("first", "one");
    data.put("second", "two");
    JsonNode result = objectMapper.readTree(AvroSchemaUtils.toJson(data));

    assertTrue(result.isObject());
    assertEquals(2, result.size());
    assertNotNull(result.get("first"));
    assertEquals("one", result.get("first").asText());
    assertNotNull(result.get("second"));
    assertEquals("two", result.get("second").asText());
  }

  @Test
  public void testEnumToJson() throws Exception {
    JsonNode result =
        objectMapper.readTree(AvroSchemaUtils.toJson(new GenericData.EnumSymbol(enumSchema,
        "SPADES"
    )));
    assertTrue(result.isTextual());
    assertEquals("SPADES", result.textValue());
  }


  private static void expectConversionException(JsonNode obj, AvroSchema schema) {
    try {
      AvroSchemaUtils.toObject(obj, schema);
      fail("Expected conversion of "
          + (obj == null ? "null" : obj.toString())
          + " to schema "
          + schema.toString()
          + " to fail");
    } catch (Exception e) {
      // Expected
    }
  }

  private static void expectConversionException(Object obj) {
    try {
      AvroSchemaUtils.toJson(obj);
      fail("Expected conversion of "
          + (
          obj == null ? "null" : (obj.toString() + " (" + obj.getClass().getName() + ")"))
          + " to fail");
    } catch (Exception e) {
      // Expected
    }
  }

  private static AvroSchema createPrimitiveSchema(String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return new AvroSchema(parser.parse(schemaString));
  }

  private static JsonNode jsonTree(String jsonData) {
    try {
      return objectMapper.readTree(jsonData);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }
}
