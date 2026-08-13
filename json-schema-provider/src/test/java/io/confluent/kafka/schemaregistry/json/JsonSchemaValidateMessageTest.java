/*
 * Copyright 2026 Confluent Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Walker-level tests for {@link JsonSchema#validateMessage}. Stubs the executor to always
 * return {@code false} so every walker invocation surfaces as a {@link ValidationRuleError};
 * the test asserts on (rule name, path) pairs to verify the walker's dispatch (recursion
 * into nested objects, array iteration, CombinedSchema/oneOf semantics, ReferenceSchema
 * resolution, skip-on-null) and that each rule is evaluated once per location reached.
 */
public class JsonSchemaValidateMessageTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Stub: every rule fires a violation. */
  private static final ValidationRuleExecutor ALWAYS_FAIL =
      (rule, schema, value) -> Boolean.FALSE;

  private static List<String> firedRules(List<ValidationRuleError> errors) {
    List<String> out = new ArrayList<>();
    for (ValidationRuleError e : errors) {
      out.add(e.getRule().getName() + "@" + e.getFieldPath());
    }
    return out;
  }

  @Test
  public void nestedObject_recursesAndProducesDottedPath() throws Exception {
    String schemaStr = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "  \"inner\":{\"type\":\"object\",\"properties\":{"
        + "    \"x\":{\"type\":\"integer\","
        + "      \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}}}"
        + "}}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ObjectNode inner = MAPPER.createObjectNode().put("x", 5);
    ObjectNode outer = MAPPER.createObjectNode().set("inner", inner);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    assertEquals(Collections.singletonList("r@$.inner.x"), firedRules(errors));
  }

  @Test
  public void arrayOfObjects_firesRulePerElementWithIndexedPath() throws Exception {
    String schemaStr = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "  \"items\":{\"type\":\"array\",\"items\":{"
        + "    \"type\":\"object\",\"properties\":{"
        + "      \"x\":{\"type\":\"integer\","
        + "        \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}}}}"
        + "}}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ArrayNode items = MAPPER.createArrayNode();
    items.add(MAPPER.createObjectNode().put("x", 1));
    items.add(MAPPER.createObjectNode().put("x", 2));
    ObjectNode outer = MAPPER.createObjectNode().set("items", items);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    assertEquals(Arrays.asList("r@$.items[0].x", "r@$.items[1].x"), firedRules(errors));
  }

  @Test
  public void failFast_stopsAfterFirstViolation() throws Exception {
    // Same array-of-objects shape that without fail-fast produces two violations.
    // With failFast=true, the walker should stop after the first.
    String schemaStr = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "  \"items\":{\"type\":\"array\",\"items\":{"
        + "    \"type\":\"object\",\"properties\":{"
        + "      \"x\":{\"type\":\"integer\","
        + "        \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}}}}"
        + "}}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ArrayNode items = MAPPER.createArrayNode();
    items.add(MAPPER.createObjectNode().put("x", 1));
    items.add(MAPPER.createObjectNode().put("x", 2));
    ObjectNode outer = MAPPER.createObjectNode().set("items", items);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer, true);
    assertEquals(Collections.singletonList("r@$.items[0].x"), firedRules(errors));
  }

  @Test
  public void oneOf_firesRuleOnlyOnMatchingSubschema() throws Exception {
    // Two oneOf branches with different rules. The walker should only descend into the
    // branch whose schema validates the value.
    String schemaStr = "{"
        + "\"oneOf\":["
        + "  {\"type\":\"object\","
        + "   \"properties\":{\"a\":{\"type\":\"string\"}},"
        + "   \"required\":[\"a\"],"
        + "   \"confluent:rules\":[{\"name\":\"matchA\",\"expr\":\"true\"}]},"
        + "  {\"type\":\"object\","
        + "   \"properties\":{\"b\":{\"type\":\"integer\"}},"
        + "   \"required\":[\"b\"],"
        + "   \"confluent:rules\":[{\"name\":\"matchB\",\"expr\":\"true\"}]}]"
        + "}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ObjectNode aMessage = MAPPER.createObjectNode().put("a", "hi");

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, aMessage);
    // Only the matching branch's rule fires.
    assertEquals(Collections.singletonList("matchA@$"), firedRules(errors));
  }

  @Test
  public void referenceSchema_resolvesAndFiresRule() throws Exception {
    // $ref within definitions; the walker must follow the reference and invoke the
    // rule on the referenced object schema.
    String schemaStr = "{"
        + "\"definitions\":{"
        + "  \"Inner\":{\"type\":\"object\",\"properties\":{"
        + "    \"x\":{\"type\":\"integer\","
        + "      \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}}}},"
        + "\"type\":\"object\","
        + "\"properties\":{\"inner\":{\"$ref\":\"#/definitions/Inner\"}}"
        + "}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ObjectNode inner = MAPPER.createObjectNode().put("x", 5);
    ObjectNode outer = MAPPER.createObjectNode().set("inner", inner);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    assertEquals(Collections.singletonList("r@$.inner.x"), firedRules(errors));
  }

  @Test
  public void objectValuedProperty_firesItsRuleOnce() throws Exception {
    // A property's schema and the schema the walk recurses into for that property are the
    // same everit Schema. Reading confluent:rules in the property loop as well as at the
    // object being walked charged an object-valued property's rules twice, at one path.
    String schemaStr = "{"
        + "\"type\":\"object\","
        + "\"confluent:rules\":[{\"name\":\"root\",\"expr\":\"true\"}],"
        + "\"properties\":{"
        + "  \"inner\":{\"type\":\"object\","
        + "    \"confluent:rules\":[{\"name\":\"obj\",\"expr\":\"true\"}],"
        + "    \"properties\":{\"x\":{\"type\":\"integer\","
        + "      \"confluent:rules\":[{\"name\":\"leaf\",\"expr\":\"true\"}]}}}"
        + "}}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ObjectNode inner = MAPPER.createObjectNode().put("x", 5);
    ObjectNode outer = MAPPER.createObjectNode().set("inner", inner);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    assertEquals(Arrays.asList("root@$", "obj@$.inner", "leaf@$.inner.x"), firedRules(errors));
  }

  @Test
  public void arrayItems_firesTheItemSchemaRuleOnEachElement() throws Exception {
    // Items are not properties, so a rule on a scalar items subschema has no parent
    // property loop to evaluate it: it is only reached by the walk landing on the element.
    String schemaStr = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "  \"tags\":{\"type\":\"array\","
        + "    \"confluent:rules\":[{\"name\":\"arr\",\"expr\":\"true\"}],"
        + "    \"items\":{\"type\":\"string\","
        + "      \"confluent:rules\":[{\"name\":\"item\",\"expr\":\"true\"}]}}"
        + "}}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ArrayNode tags = MAPPER.createArrayNode();
    tags.add("a");
    tags.add("b");
    ObjectNode outer = MAPPER.createObjectNode().set("tags", tags);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    assertEquals(Arrays.asList("arr@$.tags", "item@$.tags[0]", "item@$.tags[1]"),
        firedRules(errors));
  }

  @Test
  public void nullableObjectProperty_firesItsRuleOnce() throws Exception {
    // A "type" array makes the loader synthesize an anyOf whose members do not carry the
    // keyword, so the rule is declared on the enclosing CombinedSchema alone. The walk
    // descends into the matching member, which must not re-read it.
    String schemaStr = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "  \"inner\":{\"type\":[\"object\",\"null\"],"
        + "    \"confluent:rules\":[{\"name\":\"obj\",\"expr\":\"true\"}],"
        + "    \"properties\":{\"x\":{\"type\":\"integer\"}}}"
        + "}}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ObjectNode inner = MAPPER.createObjectNode().put("x", 5);
    ObjectNode outer = MAPPER.createObjectNode().set("inner", inner);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    assertEquals(Collections.singletonList("obj@$.inner"), firedRules(errors));
  }

  @Test
  public void nullableProperty_skipsRuleWhenValueIsNull() throws Exception {
    // Property-level rule on a property that's missing/null; walker must skip.
    String schemaStr = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "  \"maybeName\":{\"type\":[\"string\",\"null\"],"
        + "    \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}"
        + "}}";
    JsonSchema schema = new JsonSchema(schemaStr);
    // Missing property → null → skip.
    assertTrue("Rule must skip when property value is null/missing",
        schema.validateMessage(ALWAYS_FAIL, MAPPER.createObjectNode()).isEmpty());
    // Set property → fire.
    assertEquals(Collections.singletonList("r@$.maybeName"),
        firedRules(schema.validateMessage(
            ALWAYS_FAIL, MAPPER.createObjectNode().put("maybeName", "alice"))));
  }
}
