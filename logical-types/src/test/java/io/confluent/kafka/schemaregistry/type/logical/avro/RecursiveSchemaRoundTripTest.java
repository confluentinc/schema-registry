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

package io.confluent.kafka.schemaregistry.type.logical.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
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
 * Round-trip behavior for recursive (self-referential) schemas. Uses the
 * canonical Tree {@code {value, left, right}} shape where {@code left} and
 * {@code right} reference Tree itself (made nullable so the recursion can
 * terminate). Covers both directions:
 * <ul>
 *   <li>LT with a recursive Tree &rarr; Avro &rarr; LT.</li>
 *   <li>Legacy Avro Tree &rarr; LT &rarr; Avro.</li>
 * </ul>
 */
class RecursiveSchemaRoundTripTest {

  @Test
  void ltToAvroAndBack() {
    // Build the LT directly: a Tree named type with self-references on
    // left/right. The named-type pattern (NAMED_TYPE_REF + namedTypes entry)
    // is what allows the cycle without infinite recursion.
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

    // Forward: LT -> Avro.
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Tree");
    assertNotNull(avro.rawSchema());
    assertEquals(org.apache.avro.Schema.Type.RECORD, avro.rawSchema().getType());
    assertEquals("Tree", avro.rawSchema().getName());
    // The left/right fields are nullable unions of [null, Tree].
    org.apache.avro.Schema leftField = avro.rawSchema().getField("left").schema();
    assertEquals(org.apache.avro.Schema.Type.UNION, leftField.getType());
    assertEquals(org.apache.avro.Schema.Type.RECORD,
        leftField.getTypes().get(1).getType(),
        "Avro union [null, Tree] places Tree at index 1");
    // Avro's parser uses the same Schema instance for the recursive reference.
    assertTrue(leftField.getTypes().get(1) == avro.rawSchema(),
        "left's record type should be the SAME Avro Schema instance as Tree");

    // Reverse: Avro -> LT. Recursive structure preserved.
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    assertEquals(Schema.Type.NAMED_TYPE_REF, roundTripped.getRootSchema().getType());
    assertEquals("Tree", roundTripped.getRootSchema().getQualifiedName());
    Schema rtBody = roundTripped.getNamedTypes().get("Tree");
    assertNotNull(rtBody, "Tree body should be in namedTypes");
    assertEquals(Schema.Type.STRUCT, rtBody.getType());
    assertEquals(3, rtBody.getFields().size());
    assertEquals(Schema.Type.INT,
        rtBody.getField("value").getSchema().getType());
    Schema rtLeft = rtBody.getField("left").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rtLeft.getType());
    assertEquals("Tree", rtLeft.getQualifiedName());
    assertTrue(rtLeft.isNullable(), "self-reference must remain nullable");
    Schema rtRight = rtBody.getField("right").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rtRight.getType());
    assertEquals("Tree", rtRight.getQualifiedName());
    assertTrue(rtRight.isNullable());
  }

  @Test
  void legacyAvroToLtAndBack() {
    // Hand-construct a recursive Avro schema (the org.apache.avro.Schema
    // graph allows self-reference because the "self" union is built after
    // the record is named). This is the shape Avro produces for any
    // recursive record.
    org.apache.avro.Schema treeAvro =
        org.apache.avro.Schema.createRecord("Tree", null, "com.example", false);
    org.apache.avro.Schema nullableTree = org.apache.avro.Schema.createUnion(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
        treeAvro);
    treeAvro.setFields(Arrays.asList(
        new org.apache.avro.Schema.Field("value",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field("left", nullableTree, null, null),
        new org.apache.avro.Schema.Field("right", nullableTree, null, null)));

    // Forward: legacy Avro -> LT.
    AvroSchema legacy = new AvroSchema(treeAvro);
    LogicalType lt = AvroToLogicalTypeConverter.toLogicalType(legacy);
    assertEquals(Schema.Type.NAMED_TYPE_REF, lt.getRootSchema().getType());
    assertEquals("com.example.Tree", lt.getRootSchema().getQualifiedName());
    Schema body = lt.getNamedTypes().get("com.example.Tree");
    assertNotNull(body, "Tree body should be in namedTypes");
    Schema left = body.getField("left").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, left.getType());
    assertEquals("com.example.Tree", left.getQualifiedName());
    assertTrue(left.isNullable());

    // Reverse: LT -> Avro. Result must parse and recover the recursive shape.
    AvroSchema rebuilt = LogicalTypeToAvroConverter.fromLogicalType(lt, "Tree");
    assertNotNull(rebuilt.rawSchema());
    assertEquals(org.apache.avro.Schema.Type.RECORD,
        rebuilt.rawSchema().getType());
    assertEquals("Tree", rebuilt.rawSchema().getName());
    assertEquals("com.example", rebuilt.rawSchema().getNamespace());
    org.apache.avro.Schema rebuiltLeft = rebuilt.rawSchema().getField("left").schema();
    assertEquals(org.apache.avro.Schema.Type.UNION, rebuiltLeft.getType());
    assertTrue(rebuiltLeft.getTypes().get(1) == rebuilt.rawSchema(),
        "left's record type should be the SAME Avro Schema instance as Tree");
  }

  /**
   * Mutual recursion within a single file: A {b: B?} and B {a: A?}, with A
   * registered as the root. Both types live in {@code namedTypes} and
   * reference each other. The root participates in the cycle (A → B → A) so
   * {@code rootSchema} surfaces as a {@code NAMED_TYPE_REF}.
   */
  @Test
  void ltToAvroAndBack_mutuallyRecursive() {
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

    // Forward: LT -> Avro.
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "A");
    assertNotNull(avro.rawSchema());
    assertEquals("A", avro.rawSchema().getName());
    // B is defined inline as the type of A.b's nullable union; it references
    // back to A by name (Avro's pointer-equality recovery for cycles).
    org.apache.avro.Schema bField = avro.rawSchema().getField("b").schema();
    assertEquals(org.apache.avro.Schema.Type.UNION, bField.getType());
    org.apache.avro.Schema bRecord = bField.getTypes().get(1);
    assertEquals(org.apache.avro.Schema.Type.RECORD, bRecord.getType());
    assertEquals("B", bRecord.getName());
    org.apache.avro.Schema aField = bRecord.getField("a").schema();
    assertTrue(aField.getTypes().get(1) == avro.rawSchema(),
        "B.a's record type should be the SAME Avro Schema instance as A");

    // Reverse: Avro -> LT.
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    assertEquals(Schema.Type.NAMED_TYPE_REF, roundTripped.getRootSchema().getType());
    assertEquals("A", roundTripped.getRootSchema().getQualifiedName());
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
  void legacyAvroToLtAndBack_mutuallyRecursive() {
    // Hand-construct the Avro graph: A is the outer record, B is defined
    // inline as the type of A.b, B.a references A by name.
    org.apache.avro.Schema aAvro =
        org.apache.avro.Schema.createRecord("A", null, "com.example", false);
    org.apache.avro.Schema bAvro =
        org.apache.avro.Schema.createRecord("B", null, "com.example", false);
    org.apache.avro.Schema nullableA = org.apache.avro.Schema.createUnion(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), aAvro);
    org.apache.avro.Schema nullableB = org.apache.avro.Schema.createUnion(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), bAvro);
    bAvro.setFields(java.util.Collections.singletonList(
        new org.apache.avro.Schema.Field("a", nullableA, null, null)));
    aAvro.setFields(java.util.Collections.singletonList(
        new org.apache.avro.Schema.Field("b", nullableB, null, null)));

    AvroSchema legacy = new AvroSchema(aAvro);
    LogicalType lt = AvroToLogicalTypeConverter.toLogicalType(legacy);
    assertEquals(Schema.Type.NAMED_TYPE_REF, lt.getRootSchema().getType());
    assertEquals("com.example.A", lt.getRootSchema().getQualifiedName());
    Schema aBody = lt.getNamedTypes().get("com.example.A");
    Schema bBody = lt.getNamedTypes().get("com.example.B");
    assertNotNull(aBody);
    assertNotNull(bBody);
    assertEquals("com.example.B",
        aBody.getField("b").getSchema().getQualifiedName());
    assertEquals("com.example.A",
        bBody.getField("a").getSchema().getQualifiedName());

    // LT -> Avro.
    AvroSchema rebuilt = LogicalTypeToAvroConverter.fromLogicalType(lt, "A");
    assertNotNull(rebuilt.rawSchema());
    assertEquals("A", rebuilt.rawSchema().getName());
    org.apache.avro.Schema rebuiltB =
        rebuilt.rawSchema().getField("b").schema().getTypes().get(1);
    assertEquals("B", rebuiltB.getName());
    assertTrue(rebuiltB.getField("a").schema().getTypes().get(1) == rebuilt.rawSchema(),
        "B.a should reference back to A via shared instance");
  }
}
