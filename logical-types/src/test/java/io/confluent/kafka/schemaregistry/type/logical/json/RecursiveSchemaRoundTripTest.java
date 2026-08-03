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

package io.confluent.kafka.schemaregistry.type.logical.json;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip behavior for recursive (self-referential) JSON schemas. Uses the
 * canonical Tree {@code {value, left, right}} shape where {@code left} and
 * {@code right} reference Tree itself (made nullable so the recursion can
 * terminate). Covers both directions:
 * <ul>
 *   <li>LT with a recursive Tree &rarr; JSON &rarr; LT.</li>
 *   <li>Legacy JSON Tree &rarr; LT &rarr; JSON.</li>
 * </ul>
 */
class RecursiveSchemaRoundTripTest {

  @Test
  void ltToJsonAndBack() {
    // Build the LT directly: a Tree named type with self-references on
    // left/right via NAMED_TYPE_REF + a namedTypes entry.
    Schema treeBody = Schema.createStruct(Arrays.asList(
        new Field("value", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("left",
            Schema.createNamedTypeRef("Tree").setNullable(true), 1),
        new Field("right",
            Schema.createNamedTypeRef("Tree").setNullable(true), 2)));
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Tree", treeBody);
    LogicalType original = new LogicalType(
        Schema.createNamedTypeRef("Tree").setNullable(false), namedTypes);

    // Forward: LT -> JSON. Tree appears as a $defs entry; left/right emit as
    // local $refs that close the cycle.
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Tree");
    assertNotNull(json.rawSchema());
    String jsonText = json.canonicalString();
    assertTrue(jsonText.contains("\"$defs\""), "expected $defs section: " + jsonText);
    assertTrue(jsonText.contains("\"#/$defs/Tree\""),
        "expected local $ref to Tree: " + jsonText);

    // Reverse: JSON -> LT. Recursive structure preserved.
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rtBody = roundTripped.getNamedTypes().get("Tree");
    assertNotNull(rtBody, "Tree body should round-trip via $defs");
    assertEquals(Schema.Type.STRUCT, rtBody.getType());
    Schema rtLeft = rtBody.getField("left").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rtLeft.getType());
    assertEquals("Tree", rtLeft.getQualifiedName());
    assertTrue(rtLeft.isNullable(), "self-reference must remain nullable");
    Schema rtRight = rtBody.getField("right").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rtRight.getType());
    assertEquals("Tree", rtRight.getQualifiedName());
  }

  @Test
  void legacyJsonToLtAndBack() {
    // Hand-write a recursive JSON schema: Tree lives in $defs and refers to
    // itself via $ref. The active schema's root is a $ref to that Tree entry.
    String legacyJson = "{"
        + "\"$ref\":\"#/$defs/Tree\","
        + "\"$defs\":{"
        + "\"Tree\":{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "\"value\":{\"type\":\"integer\"},"
        + "\"left\":{\"oneOf\":[{\"type\":\"null\"},{\"$ref\":\"#/$defs/Tree\"}]},"
        + "\"right\":{\"oneOf\":[{\"type\":\"null\"},{\"$ref\":\"#/$defs/Tree\"}]}"
        + "}"
        + "}"
        + "}}";
    JsonSchema legacy = new JsonSchema(legacyJson);

    // Forward: legacy JSON -> LT.
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(legacy);
    Schema body = lt.getNamedTypes().get("Tree");
    assertNotNull(body, "Tree body should land in namedTypes");
    assertEquals(Schema.Type.STRUCT, body.getType());
    Schema left = body.getField("left").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, left.getType());
    assertEquals("Tree", left.getQualifiedName());
    assertTrue(left.isNullable());

    // Reverse: LT -> JSON. Result must parse and the recursive $ref must
    // resolve back through Tree.
    JsonSchema rebuilt = LogicalTypeToJsonConverter.fromLogicalType(lt, "Tree");
    assertNotNull(rebuilt.rawSchema());
    String rebuiltText = rebuilt.canonicalString();
    assertTrue(rebuiltText.contains("\"$defs\""),
        "expected $defs in rebuilt: " + rebuiltText);
    assertTrue(rebuiltText.contains("\"#/$defs/Tree\""),
        "expected local $ref to Tree in rebuilt: " + rebuiltText);

    // Re-read the rebuilt to confirm the recursive structure survives.
    LogicalType rt = JsonToLogicalTypeConverter.toLogicalType(rebuilt);
    Schema rtBody = rt.getNamedTypes().get("Tree");
    assertNotNull(rtBody);
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        rtBody.getField("left").getSchema().getType());
    assertEquals("Tree",
        rtBody.getField("left").getSchema().getQualifiedName());
  }

  /**
   * Mutual recursion within a single file: A {b: B?} and B {a: A?}, with A
   * registered as the root via a top-level {@code $ref} to {@code #/$defs/A}.
   * Both types live as {@code $defs} entries.
   */
  @Test
  void ltToJsonAndBack_mutuallyRecursive() {
    Schema aBody = Schema.createStruct(Arrays.asList(
        new Field("b",
            Schema.createNamedTypeRef("B").setNullable(true), 0)));
    Schema bBody = Schema.createStruct(Arrays.asList(
        new Field("a",
            Schema.createNamedTypeRef("A").setNullable(true), 0)));
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("A", aBody);
    namedTypes.put("B", bBody);
    LogicalType original = new LogicalType(
        Schema.createNamedTypeRef("A").setNullable(false), namedTypes);

    // Forward: LT -> JSON. Both A and B emitted as $defs entries; field-
    // level refs use #/$defs/<name> form.
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "A");
    assertNotNull(json.rawSchema());
    String text = json.canonicalString();
    assertTrue(text.contains("\"#/$defs/A\""), text);
    assertTrue(text.contains("\"#/$defs/B\""), text);

    // Reverse: JSON -> LT.
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rtA = roundTripped.getNamedTypes().get("A");
    Schema rtB = roundTripped.getNamedTypes().get("B");
    assertNotNull(rtA);
    assertNotNull(rtB);
    Schema rtAB = rtA.getField("b").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rtAB.getType());
    assertEquals("B", rtAB.getQualifiedName());
    assertTrue(rtAB.isNullable());
    Schema rtBA = rtB.getField("a").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rtBA.getType());
    assertEquals("A", rtBA.getQualifiedName());
    assertTrue(rtBA.isNullable());
  }

  @Test
  void legacyJsonToLtAndBack_mutuallyRecursive() {
    String legacyJson = "{"
        + "\"$ref\":\"#/$defs/A\","
        + "\"$defs\":{"
        + "\"A\":{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "\"b\":{\"oneOf\":[{\"type\":\"null\"},{\"$ref\":\"#/$defs/B\"}]}"
        + "}"
        + "},"
        + "\"B\":{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "\"a\":{\"oneOf\":[{\"type\":\"null\"},{\"$ref\":\"#/$defs/A\"}]}"
        + "}"
        + "}"
        + "}}";
    JsonSchema legacy = new JsonSchema(legacyJson);

    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(legacy);
    Schema aBody = lt.getNamedTypes().get("A");
    Schema bBody = lt.getNamedTypes().get("B");
    assertNotNull(aBody);
    assertNotNull(bBody);
    assertEquals("B", aBody.getField("b").getSchema().getQualifiedName());
    assertEquals("A", bBody.getField("a").getSchema().getQualifiedName());

    // LT -> JSON.
    JsonSchema rebuilt = LogicalTypeToJsonConverter.fromLogicalType(lt, "A");
    assertNotNull(rebuilt.rawSchema());
    String text = rebuilt.canonicalString();
    assertTrue(text.contains("\"#/$defs/A\""), text);
    assertTrue(text.contains("\"#/$defs/B\""), text);

    // Re-read to confirm the cycle survives.
    LogicalType rt = JsonToLogicalTypeConverter.toLogicalType(rebuilt);
    assertNotNull(rt.getNamedTypes().get("A"));
    assertNotNull(rt.getNamedTypes().get("B"));
    assertEquals("B",
        rt.getNamedTypes().get("A").getField("b").getSchema().getQualifiedName());
    assertEquals("A",
        rt.getNamedTypes().get("B").getField("a").getSchema().getQualifiedName());
  }
}
