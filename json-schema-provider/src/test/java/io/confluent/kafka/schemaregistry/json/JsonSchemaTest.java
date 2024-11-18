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

package io.confluent.kafka.schemaregistry.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.diff.Difference;
import io.confluent.kafka.schemaregistry.json.diff.SchemaDiff;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.everit.json.schema.ValidationException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonSchemaTest {

  private static ObjectMapper objectMapper = new ObjectMapper();

  private static final String recordSchemaString = "{\"properties\": {\n"
      + "     \"null\": {\"type\": \"null\"},\n"
      + "     \"boolean\": {\"type\": \"boolean\"},\n"
      + "     \"number\": {\"type\": \"number\"},\n"
      + "     \"string\": {\"type\": \"string\"}\n"
      + "  },\n"
      + "  \"additionalProperties\": false\n"
      + "}";

  private static final JsonSchema recordSchema = new JsonSchema(recordSchemaString);

  private static final String arraySchemaString = "{\"type\": \"array\", \"items\": { \"type\": "
      + "\"string\" } }";

  private static final JsonSchema arraySchema = new JsonSchema(arraySchemaString);

  private static final String unionSchemaString = "{\n"
      + "  \"oneOf\": [\n"
      + "    { \"type\": \"string\", \"maxLength\": 5 },\n"
      + "    { \"type\": \"number\", \"minimum\": 0 }\n"
      + "  ]\n"
      + "}";

  private static final JsonSchema unionSchema = new JsonSchema(unionSchemaString);

  private static final String enumSchemaString = "{ \"type\": \"string\", \"enum\": [\"red\", "
      + "\"amber\", \"green\"] }";

  private static final JsonSchema enumSchema = new JsonSchema(enumSchemaString);

  private static final String invalidSchemaString = "{\"properties\": {\n"
      + "  \"string\": {\"type\": \"str\"}\n"
      + "  }"
      + "  \"additionalProperties\": false\n"
      + "}";

  @Test
  public void testPrimitiveTypesToJsonSchema() throws Exception {
    Object envelope = JsonSchemaUtils.toObject((String) null, createPrimitiveSchema("null"));
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(NullNode.getInstance(), result);

    envelope = JsonSchemaUtils.toObject("true", createPrimitiveSchema("boolean"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(true, ((BooleanNode) result).asBoolean());

    envelope = JsonSchemaUtils.toObject("false", createPrimitiveSchema("boolean"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(false, ((BooleanNode) result).asBoolean());

    envelope = JsonSchemaUtils.toObject("12", createPrimitiveSchema("number"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(12, ((NumericNode) result).asInt());

    envelope = JsonSchemaUtils.toObject("23.2", createPrimitiveSchema("number"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(23.2, ((NumericNode) result).asDouble(), 0.1);

    envelope = JsonSchemaUtils.toObject("\"a string\"", createPrimitiveSchema("string"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals("a string", ((TextNode) result).asText());
  }

  @Test
  public void testRecordToJsonSchema() throws Exception {
    String json = "{\n"
        + "    \"null\": null,\n"
        + "    \"boolean\": true,\n"
        + "    \"number\": 12,\n"
        + "    \"string\": \"string\"\n"
        + "}";

    JsonNode envelope = (JsonNode) JsonSchemaUtils.toObject(json, recordSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(true, result.get("null").isNull());
    assertEquals(true, result.get("boolean").booleanValue());
    assertEquals(12, result.get("number").intValue());
    assertEquals("string", result.get("string").textValue());
  }

  @Test(expected = ValidationException.class)
  public void testInvalidRecordToJsonSchema() throws Exception {
    String json = "{\n"
        + "    \"null\": null,\n"
        + "    \"boolean\": true,\n"
        + "    \"number\": 12,\n"
        + "    \"string\": \"string\",\n"
        + "    \"badString\": \"string\"\n"
        + "}";

    JsonNode envelope = (JsonNode) JsonSchemaUtils.toObject(json, recordSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(true, result.get("null").isNull());
    assertEquals(true, result.get("boolean").booleanValue());
    assertEquals(12, result.get("number").intValue());
    assertEquals("string", result.get("string").textValue());
  }

  @Test
  public void testArrayToJsonSchema() throws Exception {
    String json = "[\"one\", \"two\", \"three\"]";

    Object envelope = JsonSchemaUtils.toObject(json, arraySchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    ArrayNode arrayNode = (ArrayNode) result;
    Iterator<JsonNode> elements = arrayNode.elements();
    List<String> strings = new ArrayList<String>();
    while (elements.hasNext()) {
      strings.add(elements.next().textValue());
    }
    assertArrayEquals(new String[]{"one", "two", "three"}, strings.toArray());
  }

  @Test
  public void testUnionToJsonSchema() throws Exception {
    Object envelope = JsonSchemaUtils.toObject("\"test\"", unionSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals("test", ((TextNode) result).asText());

    envelope = JsonSchemaUtils.toObject("12", unionSchema);
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(12, ((NumericNode) result).asInt());

    try {
      JsonSchemaUtils.toObject("-1", unionSchema);
      fail("Trying to use negative number should fail");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testEnumToJsonSchema() throws Exception {
    Object envelope = JsonSchemaUtils.toObject("\"red\"", enumSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals("red", ((TextNode) result).asText());

    try {
      JsonSchemaUtils.toObject("\"yellow\"", enumSchema);
      fail("Trying to use non-enum should fail");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testPrimitiveTypesToJson() throws Exception {
    JsonNode result = objectMapper.readTree(JsonSchemaUtils.toJson((int) 0));
    assertTrue(result.isNumber());

    result = objectMapper.readTree(JsonSchemaUtils.toJson((long) 0));
    assertTrue(result.isNumber());

    result = objectMapper.readTree(JsonSchemaUtils.toJson(0.1f));
    assertTrue(result.isNumber());

    result = objectMapper.readTree(JsonSchemaUtils.toJson(0.1));
    assertTrue(result.isNumber());

    result = objectMapper.readTree(JsonSchemaUtils.toJson(true));
    assertTrue(result.isBoolean());

    // "Primitive" here refers to JsonSchema primitive types, which are returned as standalone
    // objects,
    // which can't have attached schemas. This includes, for example, Strings and byte[] even
    // though they are not Java primitives

    result = objectMapper.readTree(JsonSchemaUtils.toJson("abcdefg"));
    assertTrue(result.isTextual());
    assertEquals("abcdefg", result.textValue());
  }

  @Test
  public void testRecordToJson() throws Exception {
    String json = "{\n"
        + "    \"null\": null,\n"
        + "    \"boolean\": true,\n"
        + "    \"number\": 12,\n"
        + "    \"string\": \"string\"\n"
        + "}";
    JsonNode data = new ObjectMapper().readTree(json);
    JsonNode result = objectMapper.readTree(JsonSchemaUtils.toJson(data));
    assertTrue(result.isObject());
    assertTrue(result.get("null").isNull());
    assertTrue(result.get("boolean").isBoolean());
    assertEquals(true, result.get("boolean").booleanValue());
    assertTrue(result.get("number").isIntegralNumber());
    assertEquals(12, result.get("number").intValue());
    assertTrue(result.get("string").isTextual());
    assertEquals("string", result.get("string").textValue());
  }

  @Test
  public void testArrayToJson() throws Exception {
    String json = "[\"one\", \"two\", \"three\"]";
    JsonNode data = new ObjectMapper().readTree(json);
    JsonNode result = objectMapper.readTree(JsonSchemaUtils.toJson(data));

    assertTrue(result.isArray());
    assertEquals(3, result.size());
    assertEquals(JsonNodeFactory.instance.textNode("one"), result.get(0));
    assertEquals(JsonNodeFactory.instance.textNode("two"), result.get(1));
    assertEquals(JsonNodeFactory.instance.textNode("three"), result.get(2));
  }

  @Test
  public void testSchemaWithDraft4() throws Exception {
    TestObj testObj = new TestObj();
    String actual =
        JsonSchemaUtils.getSchema(testObj, SpecificationVersion.DRAFT_4, true, null).toString();
    String expected = "{\"$schema\":\"http://json-schema.org/draft-04/schema#\","
        + "\"title\":\"Test Obj\",\"type\":\"object\",\"additionalProperties\":false,"
        + "\"properties\":{\"prop\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},"
        + "{\"type\":\"string\"}]}}}";
    assertEquals(expected, actual);
  }

  @Test
  public void testSchemaWithOneofs() throws Exception {
    TestObj testObj = new TestObj();
    String actual = JsonSchemaUtils.getSchema(testObj).toString();
    String expected = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"title\":\"Test Obj\",\"type\":\"object\",\"additionalProperties\":false,"
        + "\"properties\":{\"prop\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},"
        + "{\"type\":\"string\"}]}}}";
    assertEquals(expected, actual);
  }

  @Test
  public void testSchemaWithoutOneofs() throws Exception {
    TestObj testObj = new TestObj();
    String actual =
        JsonSchemaUtils.getSchema(testObj, SpecificationVersion.DRAFT_7, false, null).toString();
    String expected = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"title\":\"Test Obj\",\"type\":\"object\",\"additionalProperties\":false,"
        + "\"properties\":{\"prop\":{\"type\":\"string\"}}}";
    assertEquals(expected, actual);
  }

  @Test
  public void testSchemaWithAdditionalProperties() throws Exception {
    TestObj testObj = new TestObj();
    String actual =
        JsonSchemaUtils.getSchema(testObj, SpecificationVersion.DRAFT_7, false, false, null).toString();
    String expected = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"title\":\"Test Obj\",\"type\":\"object\",\"additionalProperties\":true,"
        + "\"properties\":{\"prop\":{\"type\":\"string\"}}}";
    assertEquals(expected, actual);
  }

  @Test
  public void testEnvelopeWithReferences() throws Exception {
    Map<String, String> schemas = getJsonSchemaWithReferences();
    SchemaReference ref = new SchemaReference("ref.json", "reference", 1);
    JsonSchema schema = new JsonSchema(schemas.get("main.json"), Collections.singletonList(ref),
        Collections.singletonMap("ref.json", schemas.get("ref.json")), null);
    Object envelope = JsonSchemaUtils.envelope(schema, null);
    JsonSchema schema2 = JsonSchemaUtils.getSchema(envelope);
    assertEquals(schema, schema2);
  }

  @Test
  public void testRecursiveSchema() {
    String schema = "{\n"
        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
        + "  \"$id\": \"task.schema.json\",\n"
        + "  \"title\": \"Task\",\n"
        + "  \"description\": \"A task\",\n"
        + "  \"type\": [\"null\", \"object\"],\n"
        + "  \"properties\": {\n"
        + "    \"parent\": {\n"
        + "        \"$ref\": \"task.schema.json\"\n"
        + "    },    \n"
        + "    \"title\": {\n"
        + "        \"description\": \"Task title\",\n"
        + "        \"type\": \"string\"\n"
        + "    }\n"
        + "  }\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    List<Difference> diff = SchemaDiff.compare(jsonSchema.rawSchema(), jsonSchema.rawSchema());
    assertEquals(0, diff.size());
  }

  @Test
  public void testParseSchema() {
    SchemaProvider jsonSchemaProvider = new JsonSchemaProvider();
    ParsedSchema parsedSchema = jsonSchemaProvider.parseSchemaOrElseThrow(
        new Schema(null, null, null, JsonSchema.TYPE, new ArrayList<>(), recordSchemaString), false, false);
    Optional<ParsedSchema> parsedSchemaOptional = jsonSchemaProvider.parseSchema(recordSchemaString,
            new ArrayList<>(), false, false);

    assertNotNull(parsedSchema);
    assertTrue(parsedSchemaOptional.isPresent());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseSchemaThrowException() {
    SchemaProvider jsonSchemaProvider = new JsonSchemaProvider();
    jsonSchemaProvider.parseSchemaOrElseThrow(
        new Schema(null, null, null, JsonSchema.TYPE, new ArrayList<>(), invalidSchemaString), false, false);
  }

  @Test
  public void testParseSchemaSuppressException() {
    SchemaProvider jsonSchemaProvider = new JsonSchemaProvider();
    Optional<ParsedSchema> parsedSchema = jsonSchemaProvider.parseSchema(invalidSchemaString,
            new ArrayList<>(), false, false);
    assertFalse(parsedSchema.isPresent());
  }

  @Test
  public void testSchemasDifferentFieldOrder() {
    String schema1 = "{\n"
        + "  \"title\": \"Person\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"lastName\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"description\": \"The person's last name.\"\n"
        + "    },\n"
        + "    \"firstName\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"description\": \"The person's first name.\"\n"
        + "    }\n" + "  }\n"
        + "}";
    String schema2 = "{\n"
        + "  \"title\": \"Person\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"firstName\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"description\": \"The person's first name.\"\n"
        + "    },\n"
        + "    \"lastName\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"description\": \"The person's last name.\"\n"
        + "    }\n" + "  }\n"
        + "}";
    JsonSchema jsonSchema1 = new JsonSchema(schema1);
    JsonSchema jsonSchema2 = new JsonSchema(schema2);
    assertNotEquals(jsonSchema1, jsonSchema2);
  }

  private static Map<String, String> getJsonSchemaWithReferences() {
    Map<String, String> schemas = new HashMap<>();
    String reference = "{\"type\":\"object\",\"additionalProperties\":false,\"definitions\":"
        + "{\"ExternalType\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},"
        + "\"additionalProperties\":false}}}";
    schemas.put("ref.json", new JsonSchema(reference).canonicalString());
    String schemaString = "{"
        + "\"$id\": \"https://acme.com/referrer.json\","
        + "\"$schema\": \"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"Ref\":"
        + "{\"$ref\":\"ref.json#/definitions/ExternalType\"}},\"additionalProperties\":false}";
    schemas.put("main.json", schemaString);
    return schemas;
  }

  private static JsonSchema createPrimitiveSchema(String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return new JsonSchema(schemaString);
  }

  static class TestObj {
    private String prop;

    public String getProp() {
      return prop;
    }
  }
}
