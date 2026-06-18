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

package io.confluent.kafka.schemaregistry.type.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LogicalTypeToDdlConverterTest {

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /** Parse DDL via the visitor and return the resulting LogicalType. */
  private static LogicalType parse(String ddl) {
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    visitor.visit(LogicalTypesParserFactory.parse(ddl));
    return visitor.toLogicalType();
  }

  /** Round-trip: emit, re-parse, compare schemas/named-types. */
  private static void assertRoundTrip(LogicalType original) {
    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    LogicalType parsed = parse(ddl);
    assertEquals(original.getNamespace(), parsed.getNamespace(),
        "namespace should round-trip\nDDL:\n" + ddl);
    assertEquals(original.getRootSchema(), parsed.getRootSchema(),
        "rootSchema should round-trip\nDDL:\n" + ddl);
    assertEquals(original.getNamedTypes(), parsed.getNamedTypes(),
        "namedTypes should round-trip\nDDL:\n" + ddl);
  }

  // -----------------------------------------------------------------------
  // Phase A: primitives, structs, arrays, maps, unions, enums
  // -----------------------------------------------------------------------

  @Test
  void primitives() {
    // Named-type (STRUCT/ENUM) bodies are parsed as nullable (the body's own nullability
    // is "ambient"; the use site sets nullability via the NAMED_TYPE_REF
    // expression). So tests don't set NOT NULL on the body itself.
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 0),
        new Schema.Field("b", Schema.create(Schema.Type.INT).setNullable(false), 1),
        new Schema.Field("c", Schema.create(Schema.Type.BIGINT).setNullable(false), 2),
        new Schema.Field("d", Schema.create(Schema.Type.DOUBLE).setNullable(false), 3),
        new Schema.Field("e", Schema.createString().setNullable(false), 4),
        new Schema.Field("f", Schema.createBytes().setNullable(false), 5),
        new Schema.Field("g", Schema.create(Schema.Type.DATE).setNullable(false), 6)));
    Map<String, Schema> nt = new LinkedHashMap<>();
    nt.put("Row", struct);
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Row").setNullable(false), nt);
    assertRoundTrip(lt);
  }

  @Test
  void parametricTypes() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("d", Schema.createDecimal(10, 2).setNullable(false), 0),
        new Schema.Field("vc", Schema.createVarchar(64).setNullable(false), 1),
        new Schema.Field("c", Schema.createChar(8).setNullable(false), 2),
        new Schema.Field("vb", Schema.createVarbinary(32).setNullable(false), 3),
        new Schema.Field("b", Schema.createBinary(16).setNullable(false), 4),
        new Schema.Field("t", Schema.createTime(6).setNullable(false), 5),
        new Schema.Field("ts", Schema.createTimestamp(9).setNullable(false), 6),
        new Schema.Field("tsltz", Schema.createTimestampLtz(3).setNullable(false), 7)));
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Row").setNullable(false),
        Map.of("Row", struct));
    assertRoundTrip(lt);
  }

  @Test
  void nullableAndNotNull() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("nullable", Schema.create(Schema.Type.INT), 0),
        new Schema.Field("notnull", Schema.create(Schema.Type.INT).setNullable(false), 1)));
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Row").setNullable(false),
        Map.of("Row", struct));
    assertRoundTrip(lt);
  }

  @Test
  void arrayMapMultisetUnion() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("ints",
            Schema.createArray(Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(false), 0),
        new Schema.Field("dict",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.create(Schema.Type.BIGINT).setNullable(false))
                .setNullable(false), 1),
        new Schema.Field("bag",
            Schema.createMultiset(Schema.createString().setNullable(false))
                .setNullable(false), 2),
        new Schema.Field("u",
            Schema.createUnion(Arrays.asList(
                new Schema.UnionBranch("s", Schema.createString().setNullable(false)),
                new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false))))
                .setNullable(false), 3)));
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Row").setNullable(false),
        Map.of("Row", struct));
    assertRoundTrip(lt);
  }

  @Test
  void enumType() {
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("RED"),
        new Schema.EnumValue("GREEN"),
        new Schema.EnumValue("BLUE")));
    Map<String, Schema> nt = new LinkedHashMap<>();
    nt.put("Color", enumSchema);
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Color").setNullable(false), nt);
    assertRoundTrip(lt);
  }

  @Test
  void nestedStructs() {
    Schema inner = Schema.createStruct(Arrays.asList(
        new Schema.Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0))
    ).setNullable(false);
    Schema outer = Schema.createStruct(Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Schema.Field("b", inner, 1)));
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Outer").setNullable(false),
        Map.of("Outer", outer));
    assertRoundTrip(lt);
  }

  @Test
  void declareNamespace() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)));
    LogicalType lt = new LogicalType(
        "com.example",
        Schema.createNamedTypeRef("com.example.Row").setNullable(false),
        Map.of("com.example.Row", struct),
        List.of(),
        Map.of());
    assertRoundTrip(lt);
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.startsWith("NAMESPACE com.example;"),
        "expected namespace declaration first, got:\n" + ddl);
  }

  // -----------------------------------------------------------------------
  // Phase B: DECLARE (URI bindings for external NAMED_TYPE_REFs)
  // -----------------------------------------------------------------------

  @Test
  void bareExternalRefHasNoSyntacticMarker() {
    // External-ness is inferred from usage on read-back, so a bare external
    // (no URI binding) needs no syntactic marker in the emitted DDL.
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("addr",
            Schema.createNamedTypeRef("com.example.Address").setNullable(false), 0)));
    LogicalType lt = new LogicalType(
        null,
        Schema.createNamedTypeRef("Row").setNullable(false),
        Map.of("Row", struct),
        java.util.Set.of("com.example.Address"),
        java.util.Collections.emptyList(),
        java.util.Collections.emptyMap(),
        java.util.Collections.emptyMap());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertFalse(ddl.contains("DECLARE"),
        "expected no DECLARE for bare external, got:\n" + ddl);
    assertTrue(ddl.contains("com.example.Address"),
        "field type should still reference the external by FQN, got:\n" + ddl);
    assertRoundTrip(lt);
  }

  @Test
  void aliasCarriesUriBinding() {
    // External Ref1 carries a synthetic-wrapper URI binding via externalImports.
    // Emitter writes `DECLARE Ref1 FOR '<uri>';` so the binding round-trips
    // through DDL → LT → DDL.
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("a",
            Schema.createNamedTypeRef("Ref1").setNullable(false), 0)));
    java.util.Map<String, String> externalImports = new java.util.LinkedHashMap<>();
    externalImports.put("Ref1", "ext.Outer");
    LogicalType lt = new LogicalType(
        null,
        Schema.createNamedTypeRef("Row").setNullable(false),
        Map.of("Row", struct),
        java.util.Set.of("Ref1"),
        externalImports,
        java.util.Collections.emptyList(),
        java.util.Collections.emptyMap(),
        java.util.Collections.emptyMap());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.contains("DECLARE Ref1 FOR 'ext.Outer';"),
        "expected DECLARE with URI binding, got:\n" + ddl);
    assertRoundTrip(lt);
  }

  @Test
  void aliasEscapesSingleQuotes() {
    // Single quotes in the URI must be doubled per the SQL string-literal
    // convention so the emitted DDL re-parses cleanly.
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("a",
            Schema.createNamedTypeRef("Ref1").setNullable(false), 0)));
    java.util.Map<String, String> externalImports = new java.util.LinkedHashMap<>();
    externalImports.put("Ref1", "ext.O'Hare");  // contains single quote
    LogicalType lt = new LogicalType(
        null,
        Schema.createNamedTypeRef("Row").setNullable(false),
        Map.of("Row", struct),
        java.util.Set.of("Ref1"),
        externalImports,
        java.util.Collections.emptyList(),
        java.util.Collections.emptyMap(),
        java.util.Collections.emptyMap());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.contains("DECLARE Ref1 FOR 'ext.O''Hare';"),
        "expected escaped single-quote in DECLARE URI, got:\n" + ddl);
    assertRoundTrip(lt);
  }

  // -----------------------------------------------------------------------
  // Phase C: metadata (defaults, comments, tags, withParams)
  // -----------------------------------------------------------------------

  @Test
  void fieldDefaults() {
    Map<String, Object> withParams = new LinkedHashMap<>();
    withParams.put("k1", "v1");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("name", Schema.createString().setNullable(false),
            0, "Anonymous", true, "person name", List.of("PII"), withParams),
        new Schema.Field("age", Schema.create(Schema.Type.INT).setNullable(false),
            1, 18, true, null, null, null),
        new Schema.Field("active", Schema.create(Schema.Type.BOOLEAN).setNullable(false),
            2, Boolean.TRUE, true, null, null, null)));
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Row").setNullable(false),
        Map.of("Row", struct));
    assertRoundTrip(lt);
  }

  // -----------------------------------------------------------------------
  // Phase D: sugar (TYPE elision)
  // -----------------------------------------------------------------------

  @Test
  void singleRootSugarElidesRegister() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)));
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Row").setNullable(false),
        Map.of("Row", struct));
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(!ddl.contains("TYPE"),
        "single-root sugar should elide TYPE, got:\n" + ddl);
    assertRoundTrip(lt);
  }

  @Test
  void explicitRegisterEmittedWhenSugarWouldDiffer() {
    // Root is the local type but NULLABLE — sugar would produce NOT NULL,
    // so we MUST emit explicit TYPE to preserve nullability.
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)));
    LogicalType lt = new LogicalType(
        Schema.createNamedTypeRef("Row"),  // nullable
        Map.of("Row", struct));
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.contains("TYPE"),
        "nullable root should emit explicit TYPE, got:\n" + ddl);
    assertRoundTrip(lt);
  }
}
