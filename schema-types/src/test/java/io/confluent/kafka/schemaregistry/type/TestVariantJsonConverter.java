/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class TestVariantJsonConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // -- toJsonNode: scalars --

  @Test
  public void testToJsonNull() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendNull();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isNull());
  }

  @Test
  public void testToJsonBooleanTrue() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendBoolean(true);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isBoolean());
    Assert.assertTrue(node.booleanValue());
  }

  @Test
  public void testToJsonBooleanFalse() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendBoolean(false);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertFalse(node.booleanValue());
  }

  @Test
  public void testToJsonByte() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendByte((byte) 42);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isNumber());
    Assert.assertEquals(42, node.intValue());
  }

  @Test
  public void testToJsonShort() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendShort((short) 1234);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isNumber());
    Assert.assertEquals(1234, node.intValue());
  }

  @Test
  public void testToJsonInt() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendInt(123456);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isInt());
    Assert.assertEquals(123456, node.intValue());
  }

  @Test
  public void testToJsonLong() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendLong(9876543210L);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isNumber());
    Assert.assertEquals(9876543210L, node.longValue());
  }

  @Test
  public void testToJsonFloat() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendFloat(3.14f);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isNumber());
    Assert.assertEquals(3.14f, node.floatValue(), 0.001f);
  }

  @Test
  public void testToJsonDouble() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDouble(2.718281828);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isDouble());
    Assert.assertEquals(2.718281828, node.doubleValue(), 0.0000001);
  }

  @Test
  public void testToJsonDecimal() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDecimal(new BigDecimal("123.456"));
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isNumber());
    Assert.assertEquals(new BigDecimal("123.456"), node.decimalValue());
  }

  @Test
  public void testToJsonString() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendString("hello world");
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isTextual());
    Assert.assertEquals("hello world", node.textValue());
  }

  @Test
  public void testToJsonEmptyString() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendString("");
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertEquals("", node.textValue());
  }

  // -- toJsonNode: temporal types --

  @Test
  public void testToJsonDate() {
    VariantBuilder builder = new VariantBuilder();
    int days = (int) LocalDate.of(2025, 4, 17).toEpochDay();
    builder.appendDate(days);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertEquals("2025-04-17", node.textValue());
  }

  @Test
  public void testToJsonTimestampTz() {
    VariantBuilder builder = new VariantBuilder();
    long micros = Instant.parse("2025-04-17T12:00:00Z").getEpochSecond() * 1_000_000;
    builder.appendTimestampTz(micros);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isTextual());
    Assert.assertTrue(node.textValue().contains("2025-04-17"));
  }

  @Test
  public void testToJsonTimestampNtz() {
    VariantBuilder builder = new VariantBuilder();
    LocalDateTime ldt = LocalDateTime.of(2025, 4, 17, 12, 0, 0);
    long micros = ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000;
    builder.appendTimestampNtz(micros);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isTextual());
    Assert.assertTrue(node.textValue().contains("2025-04-17"));
  }

  @Test
  public void testToJsonTime() {
    VariantBuilder builder = new VariantBuilder();
    long micros = LocalTime.of(14, 30, 0).toNanoOfDay() / 1000;
    builder.appendTime(micros);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertEquals("14:30", node.textValue());
  }

  // -- toJsonNode: binary and UUID --

  @Test
  public void testToJsonBinary() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendBinary(ByteBuffer.wrap(new byte[]{0, 1, 2, 3}));
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isTextual());
    Assert.assertEquals("AAECAw==", node.textValue());
  }

  @Test
  public void testToJsonUUID() {
    VariantBuilder builder = new VariantBuilder();
    UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    builder.appendUUID(uuid);
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", node.textValue());
  }

  // -- toJsonNode: objects --

  @Test
  public void testToJsonEmptyObject() {
    VariantBuilder builder = new VariantBuilder();
    builder.startObject();
    builder.endObject();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isObject());
    Assert.assertEquals(0, node.size());
  }

  @Test
  public void testToJsonSimpleObject() {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder obj = builder.startObject();
    obj.appendKey("name");
    obj.appendString("Alice");
    obj.appendKey("age");
    obj.appendInt(30);
    builder.endObject();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isObject());
    Assert.assertEquals("Alice", node.get("name").textValue());
    Assert.assertEquals(30, node.get("age").intValue());
  }

  @Test
  public void testToJsonNestedObject() {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder outer = builder.startObject();
    outer.appendKey("address");
    VariantObjectBuilder inner = outer.startObject();
    inner.appendKey("city");
    inner.appendString("NY");
    inner.appendKey("zip");
    inner.appendString("10001");
    outer.endObject();
    builder.endObject();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertEquals("NY", node.get("address").get("city").textValue());
    Assert.assertEquals("10001", node.get("address").get("zip").textValue());
  }

  // -- toJsonNode: arrays --

  @Test
  public void testToJsonEmptyArray() {
    VariantBuilder builder = new VariantBuilder();
    builder.startArray();
    builder.endArray();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isArray());
    Assert.assertEquals(0, node.size());
  }

  @Test
  public void testToJsonIntArray() {
    VariantBuilder builder = new VariantBuilder();
    VariantArrayBuilder arr = builder.startArray();
    arr.appendInt(1);
    arr.appendInt(2);
    arr.appendInt(3);
    builder.endArray();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isArray());
    Assert.assertEquals(3, node.size());
    Assert.assertEquals(1, node.get(0).intValue());
    Assert.assertEquals(2, node.get(1).intValue());
    Assert.assertEquals(3, node.get(2).intValue());
  }

  @Test
  public void testToJsonMixedArray() {
    VariantBuilder builder = new VariantBuilder();
    VariantArrayBuilder arr = builder.startArray();
    arr.appendString("hello");
    arr.appendInt(42);
    arr.appendBoolean(true);
    arr.appendNull();
    builder.endArray();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertEquals(4, node.size());
    Assert.assertEquals("hello", node.get(0).textValue());
    Assert.assertEquals(42, node.get(1).intValue());
    Assert.assertTrue(node.get(2).booleanValue());
    Assert.assertTrue(node.get(3).isNull());
  }

  // -- toJsonNode: complex nesting --

  @Test
  public void testToJsonObjectWithArray() {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder obj = builder.startObject();
    obj.appendKey("items");
    VariantArrayBuilder arr = obj.startArray();
    arr.appendInt(1);
    arr.appendInt(2);
    obj.endArray();
    builder.endObject();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.get("items").isArray());
    Assert.assertEquals(2, node.get("items").size());
    Assert.assertEquals(1, node.get("items").get(0).intValue());
  }

  @Test
  public void testToJsonArrayWithObjects() {
    VariantBuilder builder = new VariantBuilder();
    VariantArrayBuilder arr = builder.startArray();
    VariantObjectBuilder obj1 = arr.startObject();
    obj1.appendKey("id");
    obj1.appendInt(1);
    arr.endObject();
    VariantObjectBuilder obj2 = arr.startObject();
    obj2.appendKey("id");
    obj2.appendInt(2);
    arr.endObject();
    builder.endArray();
    JsonNode node = VariantUtils.toJsonNode(builder.build());
    Assert.assertTrue(node.isArray());
    Assert.assertEquals(2, node.size());
    Assert.assertEquals(1, node.get(0).get("id").intValue());
    Assert.assertEquals(2, node.get(1).get("id").intValue());
  }

  // -- toJsonString --

  @Test
  public void testToJsonStringMethod() {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder obj = builder.startObject();
    obj.appendKey("a");
    obj.appendInt(1);
    builder.endObject();
    String json = VariantUtils.toJsonString(builder.build());
    Assert.assertEquals("{\"a\":1}", json);
  }

  // -- round-trip: JsonNode → Variant → JsonNode --

  @Test
  public void testRoundTripSimpleObject() throws Exception {
    String json = "{\"name\":\"Alice\",\"age\":30,\"active\":true}";
    JsonNode original = MAPPER.readTree(json);
    Variant variant = VariantUtils.fromJsonNode(original);
    JsonNode result = VariantUtils.toJsonNode(variant);
    // Fields are sorted alphabetically in variant encoding
    Assert.assertEquals("Alice", result.get("name").textValue());
    Assert.assertEquals(30, result.get("age").intValue());
    Assert.assertTrue(result.get("active").booleanValue());
  }

  @Test
  public void testRoundTripNestedStructure() throws Exception {
    String json = "{\"items\":[{\"id\":1},{\"id\":2}],\"count\":2}";
    JsonNode original = MAPPER.readTree(json);
    Variant variant = VariantUtils.fromJsonNode(original);
    JsonNode result = VariantUtils.toJsonNode(variant);
    Assert.assertEquals(2, result.get("count").intValue());
    Assert.assertEquals(1, result.get("items").get(0).get("id").intValue());
    Assert.assertEquals(2, result.get("items").get(1).get("id").intValue());
  }

  @Test
  public void testRoundTripScalars() throws Exception {
    JsonNode original = MAPPER.readTree("\"hello\"");
    Assert.assertEquals("hello",
        VariantUtils.toJsonNode(VariantUtils.fromJsonNode(original)).textValue());

    original = MAPPER.readTree("42");
    Assert.assertEquals(42,
        VariantUtils.toJsonNode(VariantUtils.fromJsonNode(original)).intValue());

    original = MAPPER.readTree("true");
    Assert.assertTrue(
        VariantUtils.toJsonNode(VariantUtils.fromJsonNode(original)).booleanValue());

    original = MAPPER.readTree("null");
    Assert.assertTrue(
        VariantUtils.toJsonNode(VariantUtils.fromJsonNode(original)).isNull());
  }
}
