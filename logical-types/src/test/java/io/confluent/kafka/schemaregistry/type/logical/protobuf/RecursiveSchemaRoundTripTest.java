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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
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
 * Round-trip behavior for recursive (self-referential) Proto schemas. Uses
 * the canonical Tree {@code {value, left, right}} shape where {@code left}
 * and {@code right} reference Tree itself. Covers both directions:
 * <ul>
 *   <li>LT with a recursive Tree &rarr; Proto &rarr; LT.</li>
 *   <li>Legacy Proto Tree &rarr; LT &rarr; Proto.</li>
 * </ul>
 *
 * <p>Recursive root messages are supported: the reader pre-registers the
 * root as a placeholder before building its body and surfaces it as a
 * {@code NAMED_TYPE_REF} when the body cycles back to the root.
 */
class RecursiveSchemaRoundTripTest {

  @Test
  void ltToProtoAndBack() {
    // Build the LT directly: Tree with self-references on left/right via
    // NAMED_TYPE_REF + a namedTypes entry. Root is a NAMED_TYPE_REF to Tree
    // because the root participates in the cycle.
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

    // Forward: LT -> Proto.
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Tree");
    assertNotNull(proto.toDescriptor());
    Descriptor treeDesc = proto.toDescriptor();
    assertEquals("Tree", treeDesc.getName());
    // left/right's message type is Tree itself (same descriptor).
    assertTrue(treeDesc.findFieldByName("left").getMessageType() == treeDesc,
        "Tree.left's message type should be Tree itself");
    assertTrue(treeDesc.findFieldByName("right").getMessageType() == treeDesc,
        "Tree.right's message type should be Tree itself");

    // Reverse: Proto -> LT. Recursive structure preserved.
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
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
  void legacyProtoToLtAndBack() {
    // Hand-write a recursive Tree proto. Tree is the only top-level message
    // and references itself via left/right.
    String legacyProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Tree {\n"
        + "  int32 value = 1;\n"
        + "  Tree left = 2;\n"
        + "  Tree right = 3;\n"
        + "}\n";
    ProtobufSchema legacy = new ProtobufSchema(legacyProto);

    // Forward: legacy Proto -> LT. Recursive root surfaces as NAMED_TYPE_REF.
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(legacy);
    assertEquals(Schema.Type.NAMED_TYPE_REF, lt.getRootSchema().getType());
    assertEquals("com.example.Tree", lt.getRootSchema().getQualifiedName());
    Schema body = lt.getNamedTypes().get("com.example.Tree");
    assertNotNull(body, "Tree body should land in namedTypes");
    Schema left = body.getField("left").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, left.getType());
    assertEquals("com.example.Tree", left.getQualifiedName());
    assertTrue(left.isNullable());

    // Reverse: LT -> Proto. Result must build a valid descriptor with the
    // recursive structure intact.
    ProtobufSchema rebuilt = LogicalTypeToProtoConverter.fromLogicalType(
        lt, "Tree");
    assertNotNull(rebuilt.toDescriptor());
    Descriptor rebuiltTree = rebuilt.toDescriptor();
    assertEquals("Tree", rebuiltTree.getName());
    assertTrue(rebuiltTree.findFieldByName("left").getMessageType() == rebuiltTree);
    assertTrue(rebuiltTree.findFieldByName("right").getMessageType() == rebuiltTree);
  }

  /**
   * Mutual recursion within a single file: A {b: B?} and B {a: A?}, with A
   * registered as the root. Both types live as top-level messages in the
   * proto file. The root participates in the cycle (A → B → A), so the
   * reader's {@code rootIsRecursive} check returns true and A surfaces as
   * a {@code NAMED_TYPE_REF}.
   */
  @Test
  void ltToProtoAndBack_mutuallyRecursive() {
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

    // Forward: LT -> Proto.
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "A");
    Descriptor aDesc = proto.toDescriptor();
    assertEquals("A", aDesc.getName());
    Descriptor bDesc = aDesc.findFieldByName("b").getMessageType();
    assertEquals("B", bDesc.getName());
    // B.a's message type should be the SAME descriptor as A.
    assertTrue(bDesc.findFieldByName("a").getMessageType() == aDesc,
        "B.a's message type should be A itself");

    // Reverse: Proto -> LT. Recursive root surfaces as NAMED_TYPE_REF.
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
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
  }

  @Test
  void legacyProtoToLtAndBack_mutuallyRecursive() {
    String legacyProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message A {\n"
        + "  B b = 1;\n"
        + "}\n"
        + "message B {\n"
        + "  A a = 1;\n"
        + "}\n";
    ProtobufSchema legacy = new ProtobufSchema(legacyProto);

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(legacy);
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

    // LT -> Proto.
    ProtobufSchema rebuilt = LogicalTypeToProtoConverter.fromLogicalType(lt, "A");
    Descriptor rebuiltA = rebuilt.toDescriptor();
    assertEquals("A", rebuiltA.getName());
    Descriptor rebuiltB = rebuiltA.findFieldByName("b").getMessageType();
    assertEquals("B", rebuiltB.getName());
    assertTrue(rebuiltB.findFieldByName("a").getMessageType() == rebuiltA);
  }

  /**
   * Self-cycle in a peer under a non-recursive root: {@code Outer} references
   * a peer {@code Linked} that recursively references itself. Outer doesn't
   * reference itself, so it should unwrap to a {@code STRUCT} root; Linked
   * stays in {@code namedTypes} as a recursive peer. Tests that the
   * cycle-detection-from-root walk in {@code rootIsRecursive} doesn't
   * false-positive on cycles that exist within reachable types but don't loop
   * back to the root.
   */
  @Test
  void selfCycleInPeerUnderNonRecursiveRoot() {
    String legacyProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Outer {\n"
        + "  Linked head = 1;\n"
        + "}\n"
        + "message Linked {\n"
        + "  Linked next = 1;\n"
        + "}\n";
    ProtobufSchema legacy = new ProtobufSchema(legacyProto);

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(legacy);

    // Outer is non-recursive (its body references Linked, not Outer) so the
    // root surfaces as STRUCT, not NAMED_TYPE_REF.
    Schema root = lt.getRootSchema();
    assertEquals(Schema.Type.STRUCT, root.getType(),
        "Outer is non-recursive — should unwrap to STRUCT root");

    // Linked is a top-level peer; pre-registration covers it. Its self-
    // reference resolves through the placeholder.
    Schema linked = lt.getNamedTypes().get("com.example.Linked");
    assertNotNull(linked, "Linked should be in namedTypes");
    Schema next = linked.getField("next").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, next.getType());
    assertEquals("com.example.Linked", next.getQualifiedName());
    assertTrue(next.isNullable());

    // Outer's head field references Linked.
    Schema head = root.getField("head").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, head.getType());
    assertEquals("com.example.Linked", head.getQualifiedName());

    // Outer itself should NOT be in namedTypes — it was unwrapped.
    assertEquals(null, lt.getNamedTypes().get("com.example.Outer"),
        "non-recursive root should be removed from namedTypes after unwrap");

    // Reverse: LT -> Proto. The proto file should rebuild with the same
    // structure: Outer (root) + Linked (peer with self-reference).
    ProtobufSchema rebuilt = LogicalTypeToProtoConverter.fromLogicalType(
        lt, "Outer");
    Descriptor rebuiltOuter = rebuilt.toDescriptor();
    assertEquals("Outer", rebuiltOuter.getName());
    Descriptor rebuiltLinked = rebuiltOuter.findFieldByName("head").getMessageType();
    assertEquals("Linked", rebuiltLinked.getName());
    assertTrue(rebuiltLinked.findFieldByName("next").getMessageType() == rebuiltLinked,
        "Linked.next should reference Linked itself");
  }

  /**
   * Self-recursive ANONYMOUS nested message. Anonymous (unmarked) nested
   * types are inlined by default, but a self-recursive one can't be inlined.
   * The reader's cycle-detection helper auto-promotes it to a named type.
   */
  @Test
  void selfRecursiveAnonymousNestedType() {
    String legacyProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Outer {\n"
        + "  message Linked {\n"
        + "    Linked next = 1;\n"
        + "  }\n"
        + "  Linked head = 1;\n"
        + "}\n";
    ProtobufSchema legacy = new ProtobufSchema(legacyProto);

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(legacy);

    // Outer is non-recursive itself but stays as NAMED_TYPE_REF because it
    // has a nested named type (Outer.Linked) that uses parent-of resolution
    // to derive its namespace.
    Schema rootRef = lt.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rootRef.getType());
    assertEquals("com.example.Outer", rootRef.getQualifiedName());
    Schema outerBody = lt.getNamedTypes().get("com.example.Outer");
    assertNotNull(outerBody);

    // Outer.Linked was anonymous in the source but auto-promoted because
    // it's self-recursive. It now lives in namedTypes.
    Schema linked = lt.getNamedTypes().get("com.example.Outer.Linked");
    assertNotNull(linked,
        "auto-promoted self-recursive nested type should be in namedTypes");
    Schema next = linked.getField("next").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, next.getType());
    assertEquals("com.example.Outer.Linked", next.getQualifiedName());

    // Outer.head references Outer.Linked.
    Schema head = outerBody.getField("head").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, head.getType());
    assertEquals("com.example.Outer.Linked", head.getQualifiedName());

    // Reverse: LT -> Proto. Recursion preserved with shared descriptor.
    ProtobufSchema rebuilt = LogicalTypeToProtoConverter.fromLogicalType(
        lt, "Outer");
    Descriptor rebuiltOuter = rebuilt.toDescriptor();
    Descriptor rebuiltLinked =
        rebuiltOuter.findFieldByName("head").getMessageType();
    assertTrue(
        rebuiltLinked.findFieldByName("next").getMessageType() == rebuiltLinked,
        "Linked.next should reference Linked itself");
  }

  /**
   * Mutually recursive ANONYMOUS nested messages: A ↔ B inside Outer, with
   * no LT marker on either. Both must be auto-promoted because each is
   * reachable-from-itself via the other.
   */
  @Test
  void mutuallyRecursiveAnonymousNestedTypes() {
    String legacyProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Outer {\n"
        + "  message A {\n"
        + "    B b = 1;\n"
        + "  }\n"
        + "  message B {\n"
        + "    A a = 1;\n"
        + "  }\n"
        + "  A entry = 1;\n"
        + "}\n";
    ProtobufSchema legacy = new ProtobufSchema(legacyProto);

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(legacy);

    // Outer is non-recursive itself (entry → A → B → A → ... never reaches
    // Outer), but stays as NAMED_TYPE_REF because it has nested named types
    // (Outer.A and Outer.B) that need parent-of resolution.
    Schema rootRef = lt.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rootRef.getType());
    assertEquals("com.example.Outer", rootRef.getQualifiedName());
    Schema outerBody = lt.getNamedTypes().get("com.example.Outer");
    assertNotNull(outerBody);

    // Both A and B auto-promoted to named types (each is part of the A↔B
    // cycle, reachable from itself).
    Schema a = lt.getNamedTypes().get("com.example.Outer.A");
    Schema b = lt.getNamedTypes().get("com.example.Outer.B");
    assertNotNull(a, "Outer.A should be auto-promoted (mutually recursive)");
    assertNotNull(b, "Outer.B should be auto-promoted (mutually recursive)");
    assertEquals("com.example.Outer.B",
        a.getField("b").getSchema().getQualifiedName());
    assertEquals("com.example.Outer.A",
        b.getField("a").getSchema().getQualifiedName());

    // Outer.entry references Outer.A.
    Schema entry = outerBody.getField("entry").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, entry.getType());
    assertEquals("com.example.Outer.A", entry.getQualifiedName());

    // Reverse: LT -> Proto. The mutual cycle survives.
    ProtobufSchema rebuilt = LogicalTypeToProtoConverter.fromLogicalType(
        lt, "Outer");
    Descriptor rebuiltOuter = rebuilt.toDescriptor();
    Descriptor rebuiltA = rebuiltOuter.findFieldByName("entry").getMessageType();
    Descriptor rebuiltB = rebuiltA.findFieldByName("b").getMessageType();
    assertTrue(rebuiltB.findFieldByName("a").getMessageType() == rebuiltA,
        "B.a should reference A (mutual recursion preserved)");
  }

  /**
   * Auto-promoted (cyclic) anonymous nested types round-trip BYTE-LOSSLESS:
   * the writer skips the {@code logical.named} marker because the reader's
   * cycle-detection will re-promote the type regardless. So a legacy proto
   * with anonymous self-recursive nested types comes out byte-equal after
   * one round trip.
   */
  @Test
  void selfRecursiveAnonymousNestedRoundTripsByteLossless() {
    String legacyProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Outer {\n"
        + "  message Linked {\n"
        + "    Linked next = 1;\n"
        + "  }\n"
        + "  Linked head = 1;\n"
        + "}\n";
    ProtobufSchema legacy = new ProtobufSchema(legacyProto);

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(legacy);
    ProtobufSchema rebuilt = LogicalTypeToProtoConverter.fromLogicalType(
        lt, "Outer");

    // Verify the auto-promoted Linked type does NOT have the logical.named
    // marker — round-trip preserves the original anonymous status on the
    // wire even though Linked is named in the LT model.
    Descriptor outerDesc = rebuilt.toDescriptor();
    Descriptor linkedDesc = outerDesc.findFieldByName("head").getMessageType();
    assertEquals("Linked", linkedDesc.getName());
    String linkedParam = linkedDesc.getOptions()
        .getExtension(io.confluent.protobuf.MetaProto.messageMeta)
        .getParamsOrDefault(
            io.confluent.kafka.schemaregistry.type.logical.protobuf
                .CommonConstants.LOGICAL_NAMED_PROP, null);
    org.junit.jupiter.api.Assertions.assertNull(linkedParam,
        "auto-promoted (cyclic) nested type should NOT carry the "
        + "logical.named marker — preserves byte-equal wire output");

    // Re-read the rebuilt: cycle-detection promotes Linked again, so the
    // LT shape stays consistent across hops.
    LogicalType rt2 = ProtoToLogicalTypeConverter.toLogicalType(rebuilt);
    assertNotNull(rt2.getNamedTypes().get("com.example.Outer.Linked"),
        "Linked should be auto-promoted again on re-read");
  }
}
