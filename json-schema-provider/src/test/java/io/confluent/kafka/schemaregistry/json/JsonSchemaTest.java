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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.diff.Difference;
import io.confluent.kafka.schemaregistry.json.diff.SchemaDiff;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.everit.json.schema.ValidationException;
import org.junit.Test;

public class JsonSchemaTest {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final String recordSchemaString = "{\"properties\": {\n"
      + "     \"null\": {\"type\": \"null\"},\n"
      + "     \"boolean\": {\"type\": \"boolean\"},\n"
      + "     \"number\": {\"type\": \"number\"},\n"
      + "     \"string\": {\"type\": \"string\"}\n"
      + "  },\n"
      + "  \"additionalProperties\": false\n"
      + "}";

  private static final JsonSchema recordSchema = new JsonSchema(recordSchemaString);

  private static final String recordWithDefaultsSchemaString = "{\"properties\": {\n"
      + "     \"null\": {\"type\": \"null\", \"default\": null},\n"
      + "     \"boolean\": {\"type\": \"boolean\", \"default\": true},\n"
      + "     \"number\": {\"type\": \"number\", \"default\": 123},\n"
      + "     \"string\": {\"type\": \"string\", \"default\": \"abc\"}\n"
      + "  },\n"
      + "  \"additionalProperties\": false\n"
      + "}";

  private static final JsonSchema recordWithDefaultsSchema =
      new JsonSchema(recordWithDefaultsSchemaString);

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

  private static final String schema = "{\n"
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

  @Test
  public void testHasTopLevelField() {
    ParsedSchema parsedSchema = new JsonSchema(schema);
    assertTrue(parsedSchema.hasTopLevelField("parent"));
    assertFalse(parsedSchema.hasTopLevelField("doesNotExist"));
  }

  @Test
  public void testGetReservedFields() {
    Metadata reservedFieldMetadata = new Metadata(Collections.emptyMap(),
        Collections.singletonMap(ParsedSchema.RESERVED, "name, city"),
        Collections.emptySet());
    ParsedSchema parsedSchema = new JsonSchema(schema,
        Collections.emptyList(),
        Collections.emptyMap(),
        reservedFieldMetadata,
        null,
        null);
    assertEquals(ImmutableSet.of("name", "city"), parsedSchema.getReservedFields());
  }

  @Test
  public void testPrimitiveTypesToJsonSchema() throws Exception {
    Object envelope = JsonSchemaUtils.toObject((String) null, createPrimitiveSchema("null"));
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(NullNode.getInstance(), result);

    envelope = JsonSchemaUtils.toObject("true", createPrimitiveSchema("boolean"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertTrue(result.asBoolean());

    envelope = JsonSchemaUtils.toObject(BooleanNode.getTrue(), createPrimitiveSchema("boolean"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(BooleanNode.getTrue(), result);

    envelope = JsonSchemaUtils.toObject("false", createPrimitiveSchema("boolean"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertFalse(result.asBoolean());

    envelope = JsonSchemaUtils.toObject(BooleanNode.getFalse(), createPrimitiveSchema("boolean"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(BooleanNode.getFalse(), result);

    envelope = JsonSchemaUtils.toObject("12", createPrimitiveSchema("number"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(12, result.asInt());

    envelope = JsonSchemaUtils.toObject(IntNode.valueOf(12), createPrimitiveSchema("number"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(IntNode.valueOf(12), result);

    envelope = JsonSchemaUtils.toObject("23.2", createPrimitiveSchema("number"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(23.2, result.asDouble(), 0.1);

    envelope = JsonSchemaUtils.toObject(FloatNode.valueOf(23.2f), createPrimitiveSchema("number"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(FloatNode.valueOf(23.2f), result);

    envelope = JsonSchemaUtils.toObject("\"a string\"", createPrimitiveSchema("string"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals("a string", result.asText());

    envelope = JsonSchemaUtils.toObject(TextNode.valueOf("a string"), createPrimitiveSchema("string"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(TextNode.valueOf("a string"), result);
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
    assertTrue(result.get("null").isNull());
    assertTrue(result.get("boolean").booleanValue());
    assertEquals(12, result.get("number").intValue());
    assertEquals("string", result.get("string").textValue());
  }

  @Test
  public void testRecordWithDefaultsToJsonSchema() throws Exception {
    String json = "{}";

    JsonNode envelope = (JsonNode) JsonSchemaUtils.toObject(json, recordWithDefaultsSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(true, result.get("null").isNull());
    assertEquals(true, result.get("boolean").booleanValue());
    assertEquals(123, result.get("number").intValue());
    assertEquals("abc", result.get("string").textValue());
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
    assertTrue(result.get("null").isNull());
    assertTrue(result.get("boolean").booleanValue());
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
    List<String> strings = new ArrayList<>();
    while (elements.hasNext()) {
      strings.add(elements.next().textValue());
    }
    assertArrayEquals(new String[]{"one", "two", "three"}, strings.toArray());
  }

  @Test
  public void testUnionToJsonSchema() throws Exception {
    Object envelope = JsonSchemaUtils.toObject("\"test\"", unionSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals("test", result.asText());

    envelope = JsonSchemaUtils.toObject("12", unionSchema);
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(12, result.asInt());

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
    assertEquals("red", result.asText());

    try {
      JsonSchemaUtils.toObject("\"yellow\"", enumSchema);
      fail("Trying to use non-enum should fail");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testPrimitiveTypesToJson() throws Exception {
    JsonNode result = objectMapper.readTree(JsonSchemaUtils.toJson(0));
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
    assertTrue(result.get("boolean").booleanValue());
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
    String draft = "\"$schema\": \"http://json-schema.org/draft-07/schema#\"";
    testEnvelopeWithReferences(draft);
  }

  @Test
  public void testEnvelopeWithReferencesDraft_2020_12() throws Exception {
    String draft = "\"$schema\": \"https://json-schema.org/draft/2020-12/schema\"";
    testEnvelopeWithReferences(draft);
  }

  private void testEnvelopeWithReferences(String draft) throws Exception {
    Map<String, String> schemas = getJsonSchemaWithReferences(draft);
    SchemaReference ref = new SchemaReference("ref.json", "reference", 1);
    JsonSchema schema = new JsonSchema(schemas.get("main.json"), Collections.singletonList(ref),
        Collections.singletonMap("ref.json", schemas.get("ref.json")), null);
    schema.validate(true);
    Object envelope = JsonSchemaUtils.envelope(schema, null);
    JsonSchema schema2 = JsonSchemaUtils.getSchema(envelope);
    schema2.validate(true);
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

  @Test
  public void testBasicAddAndRemoveTags() {
    String schemaString = "{\n" +
      "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
      "  \"$id\": \"http://example.com/myURI.schema.json\",\n" +
      "  \"title\": \"SampleRecord\",\n" +
      "  \"description\": \"Sample schema to help you get started.\",\n" +
      "  \"type\": \"object\",\n" +
      "  \"additionalProperties\": false,\n" +
      "  \"properties\": {\n" +
      "    \"myfield1\": {\n" +
      "      \"type\": \"array\",\n" +
      "      \"items\": {\n" +
      "        \"type\": \"object\",\n" +
      "        \"title\": \"arrayRecord\",\n" +
      "        \"properties\": {\n" +
      "          \"field1\" : {\n" +
      "            \"type\": \"string\"\n" +
      "          },\n" +
      "          \"field2\": {\n" +
      "            \"type\": \"number\"\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    },\n" +
      "    \"myfield2\": {\n" +
      "      \"allOf\": [\n" +
      "        { \"type\": \"string\" },\n" +
      "        { \"type\": \"object\",\n" +
      "          \"title\": \"nestedUnion\",\n" +
      "          \"properties\": {\n" +
      "            \"nestedUnionField1\": { \"type\": \"boolean\"},\n" +
      "            \"nestedUnionField2\": { \"type\": \"number\"}\n" +
      "          }\n" +
      "        }\n" +
      "      ]\n" +
      "    },\n" +
      "    \"myfield3\": {\n" +
      "      \"not\": {\n" +
      "        \"type\": \"object\",\n" +
      "        \"title\": \"nestedNot\",\n" +
      "        \"properties\": {\n" +
      "          \"nestedNotField1\": { \"type\": \"boolean\"},\n" +
      "          \"nestedNotField2\": { \"type\": \"string\"}\n" +
      "        }\n" +
      "      }\n" +
      "    },\n" +
      "    \"myfield4\": { \"enum\": [\"red\", \"amber\", \"green\"]}\n" +
      "  }\n" +
      "}";

    String addedTagSchema = "{\n" +
      "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
      "  \"$id\": \"http://example.com/myURI.schema.json\",\n" +
      "  \"title\": \"SampleRecord\",\n" +
      "  \"description\": \"Sample schema to help you get started.\",\n" +
      "  \"type\": \"object\",\n" +
      "  \"additionalProperties\": false,\n" +
      "  \"properties\": {\n" +
      "    \"myfield1\": {\n" +
      "      \"type\": \"array\",\n" +
      "      \"items\": {\n" +
      "        \"type\": \"object\",\n" +
      "        \"title\": \"arrayRecord\",\n" +
      "        \"properties\": {\n" +
      "          \"field1\" : {\n" +
      "            \"type\": \"string\",\n" +
      "            \"confluent:tags\": [ \"PII\" ]\n" +
      "          },\n" +
      "          \"field2\": {\n" +
      "            \"type\": \"number\"\n" +
      "          }\n" +
      "        },\n" +
      "        \"confluent:tags\": [ \"record\" ]\n" +
      "      }\n" +
      "    },\n" +
      "    \"myfield2\": {\n" +
      "      \"allOf\": [\n" +
      "        { \"type\": \"string\" },\n" +
      "        { \"type\": \"object\",\n" +
      "          \"title\": \"nestedUnion\",\n" +
      "          \"properties\": {\n" +
      "            \"nestedUnionField1\": { \"type\": \"boolean\"},\n" +
      "            \"nestedUnionField2\": { \n" +
      "              \"type\": \"number\", \n" +
      "              \"confluent:tags\": [ \"PII\" ]\n" +
      "            }\n" +
      "          },\n" +
      "          \"confluent:tags\": [ \"record\" ]\n" +
      "        }\n" +
      "      ]\n" +
      "    },\n" +
      "    \"myfield3\": {\n" +
      "      \"not\": {\n" +
      "        \"type\": \"object\",\n" +
      "        \"title\": \"nestedNot\",\n" +
      "        \"properties\": {\n" +
      "          \"nestedNotField1\": { \"type\": \"boolean\" },\n" +
      "          \"nestedNotField2\": {\n" +
      "            \"type\": \"string\",\n" +
      "            \"confluent:tags\": [ \"PII\" ]\n" +
      "          }\n" +
      "        },\n" +
      "        \"confluent:tags\": [ \"record\" ]\n" +
      "      }\n" +
      "    },\n" +
      "    \"myfield4\": { \n" +
      "      \"enum\": [\"red\", \"amber\", \"green\"],\n" +
      "      \"confluent:tags\": [ \"PII\" ]\n" +
      "    }\n" +
      "  }\n" +
      "}\n";

    JsonSchema schema = new JsonSchema(schemaString);
    JsonSchema expectSchema = new JsonSchema(addedTagSchema);
    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    tags.put(new SchemaEntity("object.myfield1.array.object.field1",
      SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tags.put(new SchemaEntity("object.myfield2.allof.0.object.nestedUnionField2",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tags.put(new SchemaEntity("object.myfield3.not.object.nestedNotField2",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tags.put(new SchemaEntity("object.myfield4",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tags.put(new SchemaEntity("object.myfield1.array.object",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("record"));
    tags.put(new SchemaEntity("object.myfield2.allof.0.object",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("record"));
    tags.put(new SchemaEntity("object.myfield3.not.object",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("record"));

    ParsedSchema resultSchema = schema.copy(tags, Collections.emptyMap());
    assertEquals(expectSchema.canonicalString(), resultSchema.canonicalString());
    assertEquals(ImmutableSet.of("record", "PII"), resultSchema.inlineTags());

    resultSchema = resultSchema.copy(Collections.emptyMap(), tags);
    assertEquals(schema.canonicalString(), resultSchema.canonicalString());
    assertEquals(ImmutableSet.of(), resultSchema.inlineTags());

    Map<String, Set<String>> pathTags =
        Collections.singletonMap("some.path", Collections.singleton("EXTERNAL"));
    Metadata metadata = new Metadata(pathTags, null, null);
    resultSchema = resultSchema.copy(metadata, null);
    assertEquals(ImmutableSet.of("EXTERNAL"), resultSchema.tags());
  }

  @Test
  public void testAddTagsToConditional() {
    String schemaString = "{\n" +
      "  \"else\": {\n" +
      "    \"properties\": {\n" +
      "      \"postal_code\": {\n" +
      "        \"pattern\": \"[A-Z][0-9][A-Z] [0-9][A-Z][0-9]\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"if\": {\n" +
      "    \"properties\": {\n" +
      "      \"country\": {\n" +
      "        \"const\": \"United States of America\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"properties\": {\n" +
      "    \"country\": {\n" +
      "      \"default\": \"United States of America\",\n" +
      "      \"enum\": [\n" +
      "        \"United States of America\",\n" +
      "        \"Canada\"\n" +
      "      ]\n" +
      "    },\n" +
      "    \"street_address\": {\n" +
      "      \"type\": \"string\"\n" +
      "    }\n" +
      "  },\n" +
      "  \"then\": {\n" +
      "    \"properties\": {\n" +
      "      \"postal_code\": {\n" +
      "        \"pattern\": \"[0-9]{5}(-[0-9]{4})?\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"type\": \"object\"\n" +
      "}\n";

    String addedTagSchema = "{\n" +
      "  \"else\": {\n" +
      "    \"properties\": {\n" +
      "      \"postal_code\": {\n" +
      "        \"pattern\": \"[A-Z][0-9][A-Z] [0-9][A-Z][0-9]\",\n" +
      "        \"confluent:tags\": [ \"testConditional\" ]\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"if\": {\n" +
      "    \"properties\": {\n" +
      "      \"country\": {\n" +
      "        \"const\": \"United States of America\"\n" +
      "      }\n" +
      "    },\n" +
      "    \"confluent:tags\": [ \"record\" ]\n" +
      "  },\n" +
      "  \"properties\": {\n" +
      "    \"country\": {\n" +
      "      \"default\": \"United States of America\",\n" +
      "      \"enum\": [\n" +
      "        \"United States of America\",\n" +
      "        \"Canada\"\n" +
      "      ],\n" +
      "      \"confluent:tags\": [ \"testConditional\" ]\n" +
      "    },\n" +
      "    \"street_address\": {\n" +
      "      \"type\": \"string\"\n" +
      "    }\n" +
      "  },\n" +
      "  \"then\": {\n" +
      "    \"properties\": {\n" +
      "      \"postal_code\": {\n" +
      "        \"pattern\": \"[0-9]{5}(-[0-9]{4})?\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"type\": \"object\",\n" +
      "  \"confluent:tags\": [ \"record\" ]\n" +
      "}\n";

    JsonSchema schema = new JsonSchema(schemaString);
    JsonSchema expectSchema = new JsonSchema(addedTagSchema);
    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    tags.put(new SchemaEntity("allof.0.conditional.else.object.postal_code",
      SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("testConditional"));
    tags.put(new SchemaEntity("allof.1.object.country",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("testConditional"));
    tags.put(new SchemaEntity("allof.1.object",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("record"));
    tags.put(new SchemaEntity("allof.0.conditional.if.object",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("record"));

    ParsedSchema resultSchema = schema.copy(tags, Collections.emptyMap());
    assertEquals(expectSchema.canonicalString(), resultSchema.canonicalString());
    assertEquals(ImmutableSet.of("record", "testConditional"), resultSchema.inlineTags());
  }

  @Test
  public void testAddTagToRecursiveSchema() {
    String schema = "{\n" +
      "  \"$id\": \"task.schema.json\",\n" +
      "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
      "  \"description\": \"A task\",\n" +
      "  \"properties\": {\n" +
      "    \"parent\": {\n" +
      "      \"$ref\": \"task.schema.json\",\n" +
      "      \"confluent:tags\": [ \"testRecursive\" ]\n" +
      "    },\n" +
      "    \"title\": {\n" +
      "      \"description\": \"Task title\",\n" +
      "      \"type\": \"string\"\n" +
      "    }\n" +
      "  },\n" +
      "  \"title\": \"Task\",\n" +
      "  \"type\": [\n" +
      "    \"null\",\n" +
      "    \"object\"\n" +
      "  ]\n" +
      "}\n";

    String addedTagSchema = "{\n" +
      "  \"$id\": \"task.schema.json\",\n" +
      "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
      "  \"description\": \"A task\",\n" +
      "  \"properties\": {\n" +
      "    \"parent\": {\n" +
      "      \"$ref\": \"task.schema.json\",\n" +
      "      \"confluent:tags\": [ \"testRecursive\", \"PII\" ]\n" +
      "    },\n" +
      "    \"title\": {\n" +
      "      \"description\": \"Task title\",\n" +
      "      \"type\": \"string\"\n" +
      "    }\n" +
      "  },\n" +
      "  \"title\": \"Task\",\n" +
      "  \"type\": [\n" +
      "    \"null\",\n" +
      "    \"object\"\n" +
      "  ]\n" +
      "}\n";

    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchema expectSchema = new JsonSchema(addedTagSchema);
    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    tags.put(new SchemaEntity("anyof.1.object.parent",
      SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));

    ParsedSchema resultSchema = jsonSchema.copy(tags, Collections.emptyMap());
    assertEquals(expectSchema.canonicalString(), resultSchema.canonicalString());
    assertEquals(ImmutableSet.of("testRecursive", "PII"), resultSchema.inlineTags());
  }

  @Test
  public void testAddTagToCompositeField() {
    String schema = "{\n" +
        "    \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
        "    \"title\": \"Customer\",\n" +
        "    \"type\": \"object\",\n" +
        "    \"additionalProperties\": false,\n" +
        "    \"properties\": {\n" +
        "        \"cc_details\": {\n" +
        "            \"oneOf\": [\n" +
        "                {\n" +
        "                    \"type\": \"null\",\n" +
        "                    \"title\": \"Not included\"\n" +
        "                },\n" +
        "                {\n" +
        "                    \"$ref\": \"#/definitions/CardDetails\"\n" +
        "                }\n" +
        "            ]\n" +
        "        }\n" +
        "    },\n" +
        "    \"definitions\": {\n" +
        "        \"Another.Details\": {\n" +
        "            \"additionalProperties\": false,\n" +
        "            \"properties\": {\n" +
        "              \"additional.field1\": {\n" +
        "                \"type\": \"string\"\n" +
        "              },\n" +
        "              \"field2\": {\n" +
        "                \"type\": \"number\"\n" +
        "              }\n" +
        "            },\n" +
        "            \"type\": \"object\"\n" +
        "          },\n" +
        "        \"CardDetails\": {\n" +
        "            \"type\": \"object\",\n" +
        "            \"additionalProperties\": false,\n" +
        "            \"properties\": {\n" +
        "                \"credit_card\": {\n" +
        "                    \"oneOf\": [\n" +
        "                        {\n" +
        "                            \"type\": \"null\",\n" +
        "                            \"title\": \"Not included\"\n" +
        "                        },\n" +
        "                        {\n" +
        "                            \"type\": \"string\"\n" +
        "                        }\n" +
        "                    ]\n" +
        "                }\n" +
        "            }\n" +
        "        }\n" +
        "    }\n" +
        "}";

    String addedTagSchema = "{\n" +
        "    \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
        "    \"title\": \"Customer\",\n" +
        "    \"type\": \"object\",\n" +
        "    \"additionalProperties\": false,\n" +
        "    \"properties\": {\n" +
        "        \"cc_details\": {\n" +
        "            \"oneOf\": [\n" +
        "                {\n" +
        "                    \"type\": \"null\",\n" +
        "                    \"title\": \"Not included\"\n" +
        "                },\n" +
        "                {\n" +
        "                    \"$ref\": \"#/definitions/CardDetails\"\n" +
        "                }\n" +
        "            ]\n" +
        "        }\n" +
        "    },\n" +
        "    \"definitions\": {\n" +
        "        \"Another.Details\": {\n" +
        "            \"additionalProperties\": false,\n" +
        "            \"properties\": {\n" +
        "              \"additional.field1\": {\n" +
        "                \"type\": \"string\",\n" +
        "                \"confluent:tags\": [ \"TEST2\" ]\n" +
        "              },\n" +
        "              \"field2\": {\n" +
        "                \"type\": \"number\"\n" +
        "              }\n" +
        "            },\n" +
        "            \"type\": \"object\"\n" +
        "          },\n" +
        "        \"CardDetails\": {\n" +
        "            \"type\": \"object\",\n" +
        "            \"additionalProperties\": false,\n" +
        "            \"properties\": {\n" +
        "                \"credit_card\": {\n" +
        "                    \"oneOf\": [\n" +
        "                        {\n" +
        "                            \"type\": \"null\",\n" +
        "                            \"title\": \"Not included\"\n" +
        "                        },\n" +
        "                        {\n" +
        "                            \"type\": \"string\"\n" +
        "                        }\n" +
        "                    ],\n" +
        "                    \"confluent:tags\": [ \"PII\" ]\n" +
        "                }\n" +
        "            },\n" +
        "            \"confluent:tags\": [ \"TEST3\" ]\n" +
        "        }\n" +
        "    }\n" +
        "}";

    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchema expectSchema = new JsonSchema(addedTagSchema);
    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    tags.put(new SchemaEntity("object.definitions.CardDetails.object.credit_card",
            SchemaEntity.EntityType.SR_FIELD),
        Collections.singleton("PII"));
    tags.put(new SchemaEntity("object.definitions.Another.Details.object.additional.field1",
            SchemaEntity.EntityType.SR_FIELD),
        Collections.singleton("TEST2"));
    tags.put(new SchemaEntity("object.definitions.CardDetails.object",
            SchemaEntity.EntityType.SR_RECORD),
        Collections.singleton("TEST3"));

    ParsedSchema resultSchema = jsonSchema.copy(tags, Collections.emptyMap());
    assertEquals(expectSchema.canonicalString(), resultSchema.canonicalString());
    assertEquals(ImmutableSet.of("PII", "TEST2", "TEST3"), resultSchema.inlineTags());
  }

  @Test
  public void testRestrictedFields() {
    String schema = "{\n"
        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
        + "  \"$id\": \"task.schema.json\",\n"
        + "  \"title\": \"Task\",\n"
        + "  \"description\": \"A task\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"$id\": {\n"
        + "        \"type\": \"string\"\n"
        + "    },    \n"
        + "    \"$$title\": {\n"
        + "        \"description\": \"Task title\",\n"
        + "        \"type\": \"string\"\n"
        + "    },    \n"
        + "    \"status\": {\n"
        + "        \"type\": \"string\"\n"
        + "    }\n"
        + "  }\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    jsonSchema.validate(false);
    assertThrows(ValidationException.class, () -> jsonSchema.validate(true));
    String stringSchema = "{\n"
        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
        + "  \"$id\": \"task.schema.json\",\n"
        + "  \"title\": \"Task\",\n"
        + "  \"description\": \"A task\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"$id\": {\n"
        + "        \"type\": \"string\"\n"
        + "    },    \n"
        + "    \"title\": {\n"
        + "        \"description\": \"Task title\",\n"
        + "        \"type\": \"string\"\n"
        + "    },    \n"
        + "    \"status\": {\n"
        + "        \"type\": \"string\"\n"
        + "    }\n"
        + "  }\n"
        + "}";
    JsonSchema validSchema = new JsonSchema(stringSchema);
    validSchema.validate(true);
  }

  @Test
  public void testMultiTypeSchemaDraft_2020_12() {
    String schema = "{ \n"
        + "  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "   \"object_details\": {\n"
        + "      \"additionalProperties\": true,\n"
        + "      \"properties\": {\n"
        + "        \"object_parents\": {\n"
        + "          \"items\": {\n"
        + "            \"properties\": {\n"
        + "              \"object_parents_file_location\": {\n"
        + "                \"type\": [\n"
        + "                  \"string\",\n"
        + "                  \"null\"\n"
        + "                ]\n"
        + "              },\n"
        + "              \"object_parents_id\": {\n"
        + "                \"type\": [\n"
        + "                  \"string\",\n"
        + "                  \"null\"\n"
        + "                ]\n"
        + "              }\n"
        + "            },\n"
        + "            \"type\": [\n"
        + "              \"object\",\n"
        + "              \"null\"\n"
        + "            ]\n"
        + "          },\n"
        + "          \"type\": [\n"
        + "            \"array\",\n"
        + "            \"null\"\n"
        + "          ]\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(schema);
    JsonSchema jsonSchema2 = jsonSchema.copyIgnoringModernDialects();
    assertTrue(jsonSchema2.isBackwardCompatible(jsonSchema).isEmpty());
    assertTrue(jsonSchema.isBackwardCompatible(jsonSchema2).isEmpty());
  }

  @Test
  public void testLocalReferenceDraft_2020_12() {
    String parent = "{\n"
        + "    \"$id\": \"acme.webhooks.checkout-application_updated.jsonschema.json\",\n"
        + "    \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n"
        + "    \"$scope\": \"r:application\",\n"
        + "    \"title\": \"ApplicationUpdatedEvent\",\n"
        + "    \"description\": \"Application updated event representing a state change in application data.\",\n"
        + "    \"type\": \"object\",\n"
        + "    \"properties\": {\n"
        + "        \"identity\": {\n"
        + "            \"$ref\": \"https://getbread.github.io/docs/oas/v2/models.openapi3.json#/components/schemas/Identity\"\n"
        + "        },\n"
        + "        \"application\": {\n"
        + "            \"$ref\": \"./checkout.common.webhooks.jsonschema.json#/components/schemas/Application\"\n"
        + "        }\n"
        + "    },\n"
        + "    \"required\": [\n"
        + "        \"identity\",\n"
        + "        \"application\"\n"
        + "    ]\n"
        + "}";
    String child = "{\n"
        + "  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n"
        + "  \"components\": {\n"
        + "    \"schemas\": {\n"
        + "      \"Application\": {\n"
        + "        \"properties\": {\n"
        + "          \"id\": {\n"
        + "            \"description\": \"The unique identifier of the Application.\",\n"
        + "            \"format\": \"uuid\",\n"
        + "            \"readOnly\": true,\n"
        + "            \"type\": \"string\"\n"
        + "          },\n"
        + "          \"shippingContact\": {\n"
        + "            \"$ref\": \"https://getbread.github.io/docs/oas/v2/models.openapi3.json#/components/schemas/Contact\"\n"
        + "          }\n"
        + "        },\n"
        + "        \"title\": \"Application\",\n"
        + "        \"type\": \"object\"\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
    SchemaReference ref = new SchemaReference("checkout.common.webhooks.jsonschema.json", "reference", 1);
    JsonSchema jsonSchema = new JsonSchema(parent, Collections.singletonList(ref),
        Collections.singletonMap("checkout.common.webhooks.jsonschema.json", child), null);
    jsonSchema.validate(true);
  }

  @Test
  public void testNestedReferenceDraft_2020_12() {
    String parent = "{\n"
        + "  \"$schema\" : \"https://json-schema.org/draft/2020-12/schema\",\n"
        + "  \"type\" : \"object\",\n"
        + "  \"properties\" : {\n"
        + "    \"applicationSchema\" : {\n"
        + "      \"$ref\" : \"#/$defs/ApplicationSchema\"\n"
        + "    },\n"
        + "    \"additionalProperties\" : false\n"
        + "  },\n"
        + "  \"$defs\" : {\n"
        + "    \"ApplicationSchema\" : {\n"
        + "      \"type\" : \"object\",\n"
        + "      \"properties\" : {\n"
        + "        \"protocolVersion\" : {\n"
        + "          \"type\" : \"array\",\n"
        + "          \"minItems\" : 0,\n"
        + "          \"items\" : {\n"
        + "            \"$ref\" : \"child.schema.json#/$defs/ProtocolVersionName\"\n"
        + "          }\n"
        + "        }\n"
        + "      },\n"
        + "      \"additionalProperties\" : false\n"
        + "    }\n"
        + "  },\n"
        + "  \"additionalProperties\" : false\n"
        + "}\n";
    String child = "{\n"
        + "    \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n"
        + "    \"type\": \"object\",\n"
        + "    \"properties\": {\n"
        + "        \"message\": {\n"
        + "            \"$ref\": \"#/$defs/Message\"\n"
        + "        },\n"
        + "        \"additionalProperties\": false\n"
        + "    },\n"
        + "    \"$defs\": {\n"
        + "        \"Message\": {\n"
        + "            \"type\": \"object\",\n"
        + "            \"properties\": {\n"
        + "                \"messageId\": {\n"
        + "                    \"$ref\": \"grandchild.schema.json#/$defs/MessageId\"\n"
        + "                }\n"
        + "            },\n"
        + "            \"additionalProperties\": false\n"
        + "        },\n"
        + "        \"ProtocolVersionName\": {\n"
        + "            \"type\": \"object\",\n"
        + "            \"properties\": {\n"
        + "                \"version\": {\n"
        + "                    \"type\": \"string\"\n"
        + "                },\n"
        + "                \"name\": {\n"
        + "                    \"type\": \"string\"\n"
        + "                }\n"
        + "            },\n"
        + "            \"required\": [\n"
        + "                \"name\",\n"
        + "                \"version\"\n"
        + "            ],\n"
        + "            \"additionalProperties\": false\n"
        + "        }\n"
        + "    },\n"
        + "    \"additionalProperties\": false\n"
        + "}\n";
    String messageDef = "{\n"
        + "    \"type\": \"object\",\n"
        + "    \"properties\": {\n"
        + "        \"messageId\": {\n"
        + "            \"$ref\": \"grandchild.schema.json#/$defs/MessageId\"\n"
        + "        }\n"
        + "    },\n"
        + "    \"additionalProperties\": false\n"
        + "}\n";
    String protocolDef = "{\n"
        + "    \"type\": \"object\",\n"
        + "    \"properties\": {\n"
        + "        \"version\": {\n"
        + "            \"type\": \"string\"\n"
        + "        },\n"
        + "        \"name\": {\n"
        + "            \"type\": \"string\"\n"
        + "        }\n"
        + "    },\n"
        + "    \"required\": [\n"
        + "        \"name\",\n"
        + "        \"version\"\n"
        + "    ],\n"
        + "    \"additionalProperties\": false\n"
        + "}\n";
    String grandchild = "{\n"
        + "    \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n"
        + "    \"type\": \"object\",\n"
        + "    \"properties\": {\n"
        + "        \"additionalProperties\": false\n"
        + "    },\n"
        + "    \"$defs\": {\n"
        + "        \"MessageId\": {\n"
        + "            \"type\": \"object\",\n"
        + "            \"properties\": {\n"
        + "                \"id\": {\n"
        + "                    \"type\": \"string\"\n"
        + "                }\n"
        + "            },\n"
        + "            \"additionalProperties\": false\n"
        + "        }\n"
        + "    },\n"
        + "    \"additionalProperties\": false\n"
        + "}\n";
    String applicationDef = "{\n"
        + "    \"type\" : \"object\",\n"
        + "    \"properties\" : {\n"
        + "      \"protocolVersion\" : {\n"
        + "        \"type\" : \"array\",\n"
        + "        \"minItems\" : 0,\n"
        + "        \"items\" : {\n"
        + "          \"$ref\" : \"child.schema.json#/$defs/ProtocolVersionName\"\n"
        + "        }\n"
        + "      }\n"
        + "    },\n"
        + "    \"additionalProperties\" : false\n"
        + "}\n";
    SchemaReference ref0 = new SchemaReference("grandchild.schema.json", "reference", 1);
    JsonSchema jsonSchema0 = new JsonSchema(child, Collections.singletonList(ref0),
        Collections.singletonMap("grandchild.schema.json", grandchild), null);
    jsonSchema0.validate(true);

    JsonSchema protocolDefSchema = new JsonSchema(protocolDef);
    JsonSchema messageDefSchema = new JsonSchema(messageDef, Collections.singletonList(ref0),
        Collections.singletonMap("grandchild.schema.json", grandchild), null);
    Map<String, Object> defs = (Map<String, Object>)
        jsonSchema0.rawSchema().getUnprocessedProperties().get("$defs");
    assertEquals(protocolDefSchema.rawSchema(), defs.get("ProtocolVersionName"));
    assertEquals(messageDefSchema.rawSchema(), defs.get("Message"));

    SchemaReference ref = new SchemaReference("child.schema.json", "reference", 1);
    JsonSchema jsonSchema = new JsonSchema(parent, ImmutableList.of(ref, ref0),
        ImmutableMap.of("child.schema.json", child,
            "grandchild.schema.json", grandchild), null);
    jsonSchema.validate(true);

    JsonSchema applicationDefSchema = new JsonSchema(applicationDef, ImmutableList.of(ref, ref0),
        ImmutableMap.of("child.schema.json", child,
            "grandchild.schema.json", grandchild), null);
    defs = (Map<String, Object>)
        jsonSchema.rawSchema().getUnprocessedProperties().get("$defs");
    assertEquals(applicationDefSchema.rawSchema(), defs.get("ApplicationSchema"));
  }

  private static Map<String, String> getJsonSchemaWithReferences(String draft) {
    Map<String, String> schemas = new HashMap<>();
    String reference = "{"
        + draft
        + ",\"type\":\"object\",\"additionalProperties\":false,\"definitions\":"
        + "{\"ExternalType\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},"
        + "\"additionalProperties\":false}}}";
    schemas.put("ref.json", new JsonSchema(reference).canonicalString());
    String schemaString = "{"
        + draft
        + ",\"$id\": \"https://acme.com/referrer.json\","
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

    public void setProp(String prop) {
      this.prop = prop;
    }
  }
}
