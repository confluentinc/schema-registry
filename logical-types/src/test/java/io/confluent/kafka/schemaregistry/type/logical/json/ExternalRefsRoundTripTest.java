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

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
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
 * Round-trip behavior of JSON schemas that carry external references.
 * Covers both directions and the two reference flavors:
 * <ul>
 *   <li>Legacy JSON with refs &rarr; LT &rarr; JSON: non-canonical $refs get
 *       synthesized {@code Ref<N>} typeNames bound to the original URIs in
 *       {@code externalImports}; canonical {@code /$defs/} refs extract the
 *       key directly.</li>
 *   <li>LT with refs &rarr; JSON &rarr; LT: observably lossless for canonical
 *       {@code /$defs/} refs — same field types, same external markings,
 *       same {@code SchemaReference} list.</li>
 * </ul>
 */
class ExternalRefsRoundTripTest {

  @Test
  void legacyJsonWithRootAndDefsRefsRoundTrip() {
    // External JSON document: the root IS the "Outer" type, and the doc also
    // hosts an "Inner" type inside $defs. The two main-schema $refs target
    // these two locations respectively.
    String externalJson = "{"
        + "\"type\":\"object\","
        + "\"properties\":{\"label\":{\"type\":\"string\"}},"
        + "\"$defs\":{"
        + "\"Inner\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"value\":{\"type\":\"integer\"}}"
        + "}"
        + "}}";

    // One SR-level reference (one external document). The main schema's two
    // $refs both resolve through this single entry — one points at the doc
    // root, the other at a $defs entry inside it via JSON-Pointer fragment.
    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("ext.Outer", "ext-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("ext.Outer", externalJson);

    String mainJson = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "\"outer\":{\"$ref\":\"ext.Outer\"},"
        + "\"inner\":{\"$ref\":\"ext.Outer#/$defs/Inner\"}"
        + "}}";

    JsonSchema legacy = new JsonSchema(mainJson, refs, resolved, null);

    // Forward: legacy JSON -> LT.
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(legacy);

    Schema outer = lt.getRootSchema().getField("outer").getSchema();
    Schema inner = lt.getRootSchema().getField("inner").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, outer.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, inner.getType());
    // The inner ref has /$defs/Inner so the typeName extracts cleanly to
    // "Inner" (canonical). The outer ref is whole-doc — non-canonical, so
    // the reader synthesizes Ref1 and stashes the URI in externalImports.
    assertEquals("Ref1", outer.getQualifiedName());
    assertEquals("Inner", inner.getQualifiedName());
    assertTrue(lt.isExternal("Ref1"));
    assertTrue(lt.isExternal("Inner"));
    assertEquals("ext.Outer", lt.getExternalImports().get("Ref1"));

    // Pin the DDL projection — bare externals have no syntactic marker (they
    // re-infer from usage on read-back). The synthesized Ref1 stands in for
    // the whole-doc ext.Outer URI and is emitted as an DECLARE statement;
    // Inner extracted from the canonical /$defs/Inner ref needs no DECLARE
    // (its source is discoverable via resolvedReferences).
    assertEquals(
        "DECLARE Ref1 FOR 'ext.Outer';\n"
            + "TYPE STRUCT(inner Inner, outer Ref1) NOT NULL;\n",
        LogicalTypeToDdlConverter.toDdl(lt));

    // Reverse: LT -> JSON. Result must be parseable and carry references.
    JsonSchema rebuilt = LogicalTypeToJsonConverter.fromLogicalType(lt, "Holder");
    assertNotNull(rebuilt.rawSchema(), "rebuilt schema must parse");
    assertFalse(rebuilt.references().isEmpty(),
        "external references must propagate to the rebuilt JSON schema");

    // Re-read the rebuilt schema as a stability check: typeNames must round
    // trip exactly (the synthetic-wrapper marker drives recognition, not the
    // typeName), externals stay external, externalImports survives.
    LogicalType rt = JsonToLogicalTypeConverter.toLogicalType(rebuilt);
    Schema outer2 = rt.getRootSchema().getField("outer").getSchema();
    Schema inner2 = rt.getRootSchema().getField("inner").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, outer2.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, inner2.getType());
    assertEquals("Ref1", outer2.getQualifiedName());
    assertEquals("Inner", inner2.getQualifiedName());
    assertTrue(rt.isExternal("Ref1"));
    assertTrue(rt.isExternal("Inner"));
    assertEquals("ext.Outer", rt.getExternalImports().get("Ref1"));
  }

  /**
   * Two distinct fields referencing the SAME non-canonical URI dedup to a
   * single {@code Ref<N>} typeName — the URI is the dedup key, not the
   * encounter site. Exercises {@code synthesizeRefName}'s lookup-before-bump
   * behavior.
   */
  @Test
  void deduplicatesByUri() {
    String externalJson = "{\"type\":\"object\","
        + "\"properties\":{\"x\":{\"type\":\"string\"}}}";
    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("ext.Doc", "ext-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("ext.Doc", externalJson);

    String mainJson = "{\"type\":\"object\",\"properties\":{"
        + "\"a\":{\"$ref\":\"ext.Doc\"},"
        + "\"b\":{\"$ref\":\"ext.Doc\"}"
        + "}}";

    JsonSchema legacy = new JsonSchema(mainJson, refs, resolved, null);
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(legacy);

    Schema a = lt.getRootSchema().getField("a").getSchema();
    Schema b = lt.getRootSchema().getField("b").getSchema();
    assertEquals("Ref1", a.getQualifiedName());
    assertEquals("Ref1", b.getQualifiedName(),
        "same URI must dedup to the same synthesized name");
    assertEquals(1, lt.getExternalImports().size(),
        "only one externalImports entry for the deduped URI");
    assertEquals("ext.Doc", lt.getExternalImports().get("Ref1"));
  }

  /**
   * Exercises three classes of non-canonical legacy ref:
   * <ul>
   *   <li>Whole-doc URI</li>
   *   <li>Non-{@code $defs} sub-pointer</li>
   *   <li>Self-document non-{@code $defs} pointer</li>
   * </ul>
   * Each gets its own {@code Ref<N>} synthesis, and the writer materializes
   * one synthetic {@code $defs} entry per binding with a {@code logical.ref}
   * marker.
   */
  @Test
  void multipleLegacyClassesAllSynthesize() {
    String externalJson = "{\"type\":\"object\","
        + "\"properties\":{\"foo\":{"
        + "\"type\":\"array\","
        + "\"items\":{\"type\":\"object\","
        + "\"properties\":{\"id\":{\"type\":\"string\"}}}"
        + "}}}";
    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("ext.Doc", "ext-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("ext.Doc", externalJson);

    String mainJson = "{\"type\":\"object\","
        + "\"$defs\":{"
        + "\"Local\":{\"type\":\"object\","
        + "\"properties\":{\"x\":{\"type\":\"integer\"}}}"
        + "},"
        + "\"properties\":{"
        + "\"whole\":{\"$ref\":\"ext.Doc\"},"
        + "\"sub\":{\"$ref\":\"ext.Doc#/properties/foo/items\"},"
        + "\"local\":{\"$ref\":\"#/$defs/Local\"}"
        + "}}";

    JsonSchema legacy = new JsonSchema(mainJson, refs, resolved, null);
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(legacy);

    // The local ref is canonical → typeName "Local", treated as user-defined
    // (no externalImports entry, present in namedTypes as a real local type).
    assertEquals("Local",
        lt.getRootSchema().getField("local").getSchema().getQualifiedName());
    assertFalse(lt.isExternal("Local"));

    // Both legacy refs synthesize to Ref<N> with their original URIs in
    // externalImports. Order of N is determined by everit's property
    // iteration, so look up by URI rather than by name.
    String wholeName = lt.getRootSchema().getField("whole").getSchema().getQualifiedName();
    String subName = lt.getRootSchema().getField("sub").getSchema().getQualifiedName();
    assertTrue(wholeName.matches("Ref\\d+"), "wholeName was: " + wholeName);
    assertTrue(subName.matches("Ref\\d+"), "subName was: " + subName);
    assertTrue(lt.isExternal(wholeName));
    assertTrue(lt.isExternal(subName));
    assertEquals("ext.Doc", lt.getExternalImports().get(wholeName));
    assertEquals("ext.Doc#/properties/foo/items",
        lt.getExternalImports().get(subName));

    // DDL projects clean: Local is local (STRUCT declaration — backticked because
    // "Local" is a reserved word); the synthesized names appear as DECLARE
    // statements carrying their URI bindings.
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(
        ddl.contains("DECLARE " + wholeName + " FOR 'ext.Doc';"),
        ddl);
    assertTrue(
        ddl.contains("DECLARE " + subName
            + " FOR 'ext.Doc#/properties/foo/items';"),
        ddl);
    assertTrue(ddl.contains("STRUCT `Local` ("), ddl);

    // Round trip writes back, re-reads, structure preserved.
    JsonSchema rebuilt = LogicalTypeToJsonConverter.fromLogicalType(lt, "Holder");
    LogicalType rt = JsonToLogicalTypeConverter.toLogicalType(rebuilt);
    assertEquals("ext.Doc", rt.getExternalImports().get(wholeName));
    assertEquals("ext.Doc#/properties/foo/items",
        rt.getExternalImports().get(subName));
    assertTrue(rt.isExternal(wholeName));
    assertTrue(rt.isExternal(subName));
    assertFalse(rt.isExternal("Local"));
  }

  /**
   * LT-side authoring: build a LT that references two distinct $defs entries
   * in a single external JSON doc, emit to JSON, read back, verify lossless
   * round trip — same field types, same external markings, same SR refs.
   * This is the canonical-cross-doc path (no synthetic wrappers needed).
   */
  @Test
  void ltToJsonAndBackLossless() {
    // External JSON with two LT-canonical $defs entries, each FQN-keyed.
    String externalJson = "{\"$defs\":{"
        + "\"com.example.Inner1\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"a\":{\"type\":\"string\"}}"
        + "},"
        + "\"com.example.Inner2\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"b\":{\"type\":\"integer\"}}"
        + "}"
        + "}}";

    // JSON's SR-level reference is the doc URI — one external doc with N
    // $defs entries collapses to one SchemaReference, N field-type uses.
    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("ext.Doc", "ext-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("ext.Doc", externalJson);

    // LT root struct with two fields referencing the externals.
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("first",
            Schema.createNamedTypeRef("com.example.Inner1").setNullable(false), 0),
        new Field("second",
            Schema.createNamedTypeRef("com.example.Inner2").setNullable(false), 1)))
        .setNullable(false);
    LogicalType original = new LogicalType(
        rootSchema, java.util.Collections.emptyMap(), refs, resolved);

    // Forward: LT -> JSON.
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    assertNotNull(json.rawSchema());

    // Reverse: JSON -> LT.
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    Schema rtRoot = roundTripped.getRootSchema();
    Schema first = rtRoot.getField("first").getSchema();
    Schema second = rtRoot.getField("second").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, first.getType());
    assertEquals("com.example.Inner1", first.getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF, second.getType());
    assertEquals("com.example.Inner2", second.getQualifiedName());

    // Externals preserved.
    assertTrue(roundTripped.isExternal("com.example.Inner1"));
    assertTrue(roundTripped.isExternal("com.example.Inner2"));

    // SR references preserved exactly.
    assertEquals(refs, roundTripped.getReferences());

    // No synthetic wrappers — both refs are LT-canonical (FQN-keyed in a
    // canonical /$defs/ slot of the external doc).
    assertTrue(roundTripped.getExternalImports().isEmpty(),
        "canonical /$defs/ refs should not produce externalImports");
  }

  /**
   * Cascading synthetic wrappers: the active schema has a non-canonical $ref
   * to {@code ext.Outer}, and {@code ext.Outer}'s body itself contains a
   * non-canonical $ref to {@code ext.Inner}. The reader should synthesize
   * a Ref&lt;N&gt; for each non-canonical URI it encounters during lazy
   * promotion, populating {@code externalImports} for both.
   */
  @Test
  void cascadingSyntheticWrappers() {
    // Outer's body contains a non-canonical $ref to Inner (whole-doc ref,
    // not a /$defs/ entry). Inner is a plain object schema.
    String outerJson = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "\"sub\":{\"$ref\":\"ext.Inner\"}"
        + "}}";
    String innerJson = "{"
        + "\"type\":\"object\","
        + "\"properties\":{\"value\":{\"type\":\"string\"}}"
        + "}";

    // Both externals must be in resolvedReferences for the JSON loader to
    // resolve Outer's nested $ref to Inner at parse time.
    List<SchemaReference> refs = Arrays.asList(
        new SchemaReference("ext.Outer", "outer-value", 1),
        new SchemaReference("ext.Inner", "inner-value", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("ext.Outer", outerJson);
    resolved.put("ext.Inner", innerJson);

    // Active schema: a single field that $ref's the whole-doc Outer URI.
    String mainJson = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "\"outer\":{\"$ref\":\"ext.Outer\"}"
        + "}}";
    JsonSchema legacy = new JsonSchema(mainJson, refs, resolved, null);

    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(legacy);

    // Two synthetic externals get registered: one for outer's $ref to
    // ext.Outer (whole-doc, non-canonical), one for outer-body's nested
    // $ref to ext.Inner (also non-canonical, discovered during lazy
    // body promotion).
    Map<String, String> imports = lt.getExternalImports();
    assertEquals(2, imports.size(),
        "expected two synthetic externals, got " + imports);
    // Both URIs should be present as values; the typeNames are Ref<N>.
    assertTrue(imports.containsValue("ext.Outer"),
        "expected ext.Outer in externalImports values: " + imports);
    assertTrue(imports.containsValue("ext.Inner"),
        "expected ext.Inner in externalImports values: " + imports);
    // Both names should be flagged external.
    for (String name : imports.keySet()) {
      assertTrue(lt.isExternal(name), name + " should be external");
    }

    // The outer field's NAMED_TYPE_REF resolves to the synthetic name for
    // ext.Outer.
    Schema outer = lt.getRootSchema().getField("outer").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, outer.getType());
    String outerName = outer.getQualifiedName();
    assertEquals("ext.Outer", imports.get(outerName));

    // Inside Outer's body (lazy-promoted into namedTypes), the sub field
    // should reference the synthesized name for ext.Inner.
    Schema outerBody = lt.getNamedTypes().get(outerName);
    assertNotNull(outerBody, "Outer's body should be lazy-promoted");
    Schema sub = outerBody.getField("sub").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, sub.getType());
    String innerName = sub.getQualifiedName();
    assertEquals("ext.Inner", imports.get(innerName));
  }
}
