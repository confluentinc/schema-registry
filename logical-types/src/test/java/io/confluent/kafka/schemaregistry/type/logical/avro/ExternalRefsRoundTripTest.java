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
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
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
 * Round-trip behavior of Avro schemas that carry external references.
 * Covers two directions:
 * <ul>
 *   <li>Legacy Avro with refs &rarr; LT &rarr; Avro: validity-preserving,
 *       reference list may differ.</li>
 *   <li>LT with refs &rarr; Avro &rarr; LT: observably lossless — same field
 *       types, same external markings, same {@code SchemaReference} list.</li>
 * </ul>
 */
class ExternalRefsRoundTripTest {

  @Test
  void legacyAvroWithRootAndNestedRefsRoundTrip() {
    // External Avro file: a top-level record (Outer) that contains a nested
    // record (Stuff). Both inherit namespace com.example, so their FQNs are
    // com.example.Outer (root) and com.example.Stuff (nested).
    String externalAvro = "{\"type\":\"record\",\"name\":\"Outer\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"label\",\"type\":\"string\"},"
        + "{\"name\":\"stuff\",\"type\":{"
        + "\"type\":\"record\",\"name\":\"Stuff\","
        + "\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]}}]}";

    // Two SchemaReferences targeting the same external subject — one for the
    // root Outer and one for the nested Stuff. Avro's loader parses every
    // entry of resolvedReferences before the main schema, so registering the
    // same external text twice is harmless (the second parse is a no-op on
    // already-defined names).
    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("com.example.Outer", "external-value", 1),
        new SchemaReference("com.example.Stuff", "external-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("com.example.Outer", externalAvro);
    resolved.put("com.example.Stuff", externalAvro);

    // Main schema uses the root and the nested type as field types.
    String mainAvro = "{\"type\":\"record\",\"name\":\"Person\","
        + "\"namespace\":\"acme\","
        + "\"fields\":["
        + "{\"name\":\"home\",\"type\":\"com.example.Outer\"},"
        + "{\"name\":\"thing\",\"type\":\"com.example.Stuff\"}]}";

    AvroSchema legacy = new AvroSchema(mainAvro, refs, resolved, null);

    // Forward: legacy Avro -> LT. The Avro reader body-promotes every named
    // record into namedTypes, so the root is a NAMED_TYPE_REF to acme.Person.
    LogicalType lt = AvroToLogicalTypeConverter.toLogicalType(legacy);

    assertEquals(Schema.Type.NAMED_TYPE_REF, lt.getRootSchema().getType());
    assertEquals("acme.Person", lt.getRootSchema().getQualifiedName());
    Schema personBody = lt.getNamedTypes().get("acme.Person");
    assertNotNull(personBody, "Person body should live in namedTypes");

    Schema home = personBody.getField("home").getSchema();
    Schema thing = personBody.getField("thing").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, home.getType());
    assertEquals("com.example.Outer", home.getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF, thing.getType());
    assertEquals("com.example.Stuff", thing.getQualifiedName());
    assertTrue(lt.isExternal("com.example.Outer"),
        "Outer should be flagged as external");
    assertTrue(lt.isExternal("com.example.Stuff"),
        "Stuff should be flagged as external");

    // Pin the DDL projection of the resulting LT — bare externals have no
    // syntactic marker (re-inferred from usage on read-back) and Person
    // becomes the registered root.
    assertEquals(
        "STRUCT acme.Person (home com.example.Outer NOT NULL,"
            + " thing com.example.Stuff NOT NULL);\n",
        LogicalTypeToDdlConverter.toDdl(lt));

    // Reverse: LT -> Avro. The result must be a valid Avro schema and must
    // carry at least one reference forward. The exact shape of references()
    // is intentionally NOT asserted — it may collapse, expand, or re-order.
    AvroSchema rebuilt = LogicalTypeToAvroConverter.fromLogicalType(lt, "Person");
    assertNotNull(rebuilt.rawSchema(), "rebuilt schema must parse");
    assertFalse(rebuilt.references().isEmpty(),
        "external references must propagate to the rebuilt Avro schema");

    // Re-read the rebuilt schema as a final validity check: the field types
    // must still resolve as NAMED_TYPE_REFs to the same FQNs.
    LogicalType rt = AvroToLogicalTypeConverter.toLogicalType(rebuilt);
    Schema personBody2 = rt.getNamedTypes().get("acme.Person");
    assertNotNull(personBody2, "Person body should survive the round trip");
    Schema home2 = personBody2.getField("home").getSchema();
    Schema thing2 = personBody2.getField("thing").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, home2.getType());
    assertEquals("com.example.Outer", home2.getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF, thing2.getType());
    assertEquals("com.example.Stuff", thing2.getQualifiedName());
  }

  /**
   * LT-side authoring: build a LT that already references two externals (root
   * and nested type of one external Avro doc), emit to Avro, read back, and
   * verify the round trip is observably lossless — same field types, same
   * external markings, same SR references.
   */
  @Test
  void ltToAvroAndBackLossless() {
    // External Avro defines com.example.Outer (root) containing nested
    // com.example.Stuff. Both names are addressable from the active schema.
    String externalAvro = "{\"type\":\"record\",\"name\":\"Outer\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"label\",\"type\":\"string\"},"
        + "{\"name\":\"stuff\",\"type\":{"
        + "\"type\":\"record\",\"name\":\"Stuff\","
        + "\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]}}]}";

    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("com.example.Outer", "external-value", 1),
        new SchemaReference("com.example.Stuff", "external-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("com.example.Outer", externalAvro);
    resolved.put("com.example.Stuff", externalAvro);

    // Build the input LT directly: anonymous STRUCT root with two field
    // types referencing the externals. The Avro writer adds the anonymous
    // marker so the round-trip preserves the STRUCT root shape.
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home",
            Schema.createNamedTypeRef("com.example.Outer").setNullable(false), 0),
        new Field("thing",
            Schema.createNamedTypeRef("com.example.Stuff").setNullable(false), 1)))
        .setNullable(false);
    LogicalType original = new LogicalType(
        rootSchema, java.util.Collections.emptyMap(), refs, resolved);

    // Forward: LT -> Avro.
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Person");
    assertNotNull(avro.rawSchema());

    // Reverse: Avro -> LT.
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    // Anonymous STRUCT root survives round-trip (writer marks it as such).
    Schema rtRoot = roundTripped.getRootSchema();
    assertEquals(Schema.Type.STRUCT, rtRoot.getType(),
        "anonymous root struct should round-trip as STRUCT, not NAMED_TYPE_REF");

    // Field types resolve to the same external NAMED_TYPE_REFs.
    Schema home = rtRoot.getField("home").getSchema();
    Schema thing = rtRoot.getField("thing").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, home.getType());
    assertEquals("com.example.Outer", home.getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF, thing.getType());
    assertEquals("com.example.Stuff", thing.getQualifiedName());

    // Externals preserved.
    assertTrue(roundTripped.isExternal("com.example.Outer"));
    assertTrue(roundTripped.isExternal("com.example.Stuff"));

    // SR references preserved exactly.
    assertEquals(refs, roundTripped.getReferences());

    // No synthetic-wrapper externals (Avro doesn't have that mechanism).
    assertTrue(roundTripped.getExternalImports().isEmpty(),
        "Avro round-trip should not produce externalImports");
  }
}
