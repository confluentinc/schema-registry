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

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip behavior of Proto schemas that carry external references.
 * Covers two directions:
 * <ul>
 *   <li>Legacy Proto with refs &rarr; LT &rarr; Proto: validity-preserving,
 *       reference list may differ.</li>
 *   <li>LT with refs &rarr; Proto &rarr; LT: observably lossless — same field
 *       types, same external markings, same {@code SchemaReference} list.</li>
 * </ul>
 */
class ExternalRefsRoundTripTest {

  @Test
  void legacyProtoWithTwoPeerRefsRoundTrip() {
    // External proto file holding two peer (top-level) messages, M1 and M2.
    String externalProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message M1 {\n  string a = 1;\n}\n"
        + "message M2 {\n  int32 b = 1;\n}\n";

    // Proto's SR-level reference is the import filename, so a single file
    // imported once carries both peer messages — even though the main schema
    // uses two distinct field-type references into it.
    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("peers.proto", "peers-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("peers.proto", externalProto);

    // Main proto imports the external file once and uses both peer messages
    // as field types — these are the two type references the test exercises.
    String mainProto = "syntax = \"proto3\";\n"
        + "package acme;\n"
        + "import \"peers.proto\";\n"
        + "message Order {\n"
        + "  com.example.M1 a = 1;\n"
        + "  com.example.M2 b = 2;\n"
        + "}\n";

    ProtobufSchema legacy = new ProtobufSchema(
        mainProto, refs, resolved, null, null, null, null);

    // Forward: legacy Proto -> LT. The proto reader keeps the root message
    // as the rootSchema STRUCT (no NAMED_TYPE_REF wrap for the root).
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(legacy);

    Schema a = lt.getRootSchema().getField("a").getSchema();
    Schema b = lt.getRootSchema().getField("b").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, a.getType());
    assertEquals("com.example.M1", a.getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF, b.getType());
    assertEquals("com.example.M2", b.getQualifiedName());
    assertTrue(lt.isExternal("com.example.M1"),
        "M1 should be flagged as external");
    assertTrue(lt.isExternal("com.example.M2"),
        "M2 should be flagged as external");

    // Pin the DDL projection of the resulting LT — both externals surface as
    // REFERENCE TYPE declarations and the file's package becomes a namespace.
    assertEquals(
        "NAMESPACE acme;\n"
            + "REFERENCE TYPE com.example.M1;\n"
            + "REFERENCE TYPE com.example.M2;\n"
            + "TYPE ROW(a com.example.M1, b com.example.M2) NOT NULL;\n",
        LogicalTypeToDdlConverter.toDdl(lt));

    // Reverse: LT -> Proto. The result must be a valid (parseable) proto
    // schema and must carry at least one reference forward. The exact shape
    // of references() is intentionally NOT asserted.
    ProtobufSchema rebuilt = LogicalTypeToProtoConverter.fromLogicalType(
        lt, "Order");
    assertNotNull(rebuilt.toDescriptor(), "rebuilt schema must build a descriptor");
    assertFalse(rebuilt.references().isEmpty(),
        "external imports must propagate to the rebuilt proto schema");

    // Re-read the rebuilt schema as a final validity check: the field types
    // must still resolve as NAMED_TYPE_REFs to the same FQNs.
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(rebuilt);
    Schema a2 = rt.getRootSchema().getField("a").getSchema();
    Schema b2 = rt.getRootSchema().getField("b").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, a2.getType());
    assertEquals("com.example.M1", a2.getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF, b2.getType());
    assertEquals("com.example.M2", b2.getQualifiedName());
  }

  /**
   * LT-side authoring: build a LT that references two peer messages from a
   * single external proto file, emit to Proto, read back, verify lossless
   * round trip — same field types, same external markings, same SR refs.
   */
  @Test
  void ltToProtoAndBackLossless() {
    // External proto with two peer messages.
    String externalProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message M1 {\n  string a = 1;\n}\n"
        + "message M2 {\n  int32 b = 1;\n}\n";

    // Proto's SR-level reference is the import filename — one file with two
    // peer messages collapses to one SchemaReference, two field-type uses.
    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("peers.proto", "peers-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("peers.proto", externalProto);

    // LT root struct with two fields referencing the externals.
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("a",
            Schema.createNamedTypeRef("com.example.M1").setNullable(false), 0),
        new Field("b",
            Schema.createNamedTypeRef("com.example.M2").setNullable(false), 1)))
        .setNullable(false);
    LogicalType original = new LogicalType(
        rootSchema, java.util.Collections.emptyMap(), refs, resolved);

    // Forward: LT -> Proto.
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Order");
    assertNotNull(proto.toDescriptor());

    // The emitted import statement MUST match the SchemaReference name —
    // otherwise SR consumers can't resolve the import via the references
    // list. With one external file holding two messages, both field-type
    // references collapse to one import using the SR ref name "peers.proto".
    java.util.List<String> imports =
        proto.toDescriptor().getFile().toProto().getDependencyList();
    assertTrue(imports.contains("peers.proto"),
        "import statement must use the SchemaReference name; got " + imports);

    // Reverse: Proto -> LT.
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    // Proto reader leaves the root as STRUCT (the active message is the root).
    Schema rtRoot = roundTripped.getRootSchema();
    assertEquals(Schema.Type.STRUCT, rtRoot.getType());

    Schema a = rtRoot.getField("a").getSchema();
    Schema b = rtRoot.getField("b").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, a.getType());
    assertEquals("com.example.M1", a.getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF, b.getType());
    assertEquals("com.example.M2", b.getQualifiedName());

    // Externals preserved.
    assertTrue(roundTripped.isExternal("com.example.M1"));
    assertTrue(roundTripped.isExternal("com.example.M2"));

    // SR references preserved exactly.
    assertEquals(refs, roundTripped.getReferences());

    // Proto doesn't use synthetic-wrapper externals.
    assertTrue(roundTripped.getExternalImports().isEmpty(),
        "Proto round-trip should not produce externalImports");
  }
}
