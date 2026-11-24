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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
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

  /**
   * Round-trip: assert the DDL projection is stable across {@code emit -> parse -> emit}.
   *
   * <p>This is idempotence rather than {@code LogicalType} equality on purpose: a leaf named root
   * canonicalizes on parse (a {@code NAMED_TYPE_REF} root + {@code namedTypes} entry unwraps to a
   * bare {@code STRUCT}/{@code ENUM} root carrying {@link LogicalType#getName()}), so the parsed
   * {@code LogicalType} may differ in shape from an original built the older way while representing
   * the same schema. DDL-string stability still catches real losses — a dropped root name, for
   * instance, would re-emit as an anonymous {@code TYPE STRUCT(...)} and fail here.
   */
  private static void assertRoundTrip(LogicalType original) {
    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    String reemitted = LogicalTypeToDdlConverter.toDdl(parse(ddl));
    assertEquals(ddl, reemitted,
        "DDL should be stable across emit -> parse -> emit\nDDL:\n" + ddl);
    // On one line it is the same script.
    String oneLine = LogicalTypeToDdlConverter.toDdl(original, true);
    assertFalse(oneLine.contains("\n"), oneLine);
    assertEquals(ddl, LogicalTypeToDdlConverter.toDdl(parse(oneLine)), oneLine);
  }

  // -----------------------------------------------------------------------
  // Phase A: primitives, structs, arrays, maps, unions, enums
  // -----------------------------------------------------------------------

  @Test
  void namespacedNamesSimplifyInDdlIncludingNestedTypes() {
    // Names under the document NAMESPACE render simplified (the com.example.demo prefix is
    // stripped), including nested types whose nesting portion (Outer.Inner) is preserved.
    // A foreign-namespace reference stays fully qualified.
    LogicalType lt = parse(
        "NAMESPACE com.example.demo;\n"
        + "STRUCT Outer (inner Outer.Inner NOT NULL, addr Address, ext other.ns.Thing);\n"
        + "STRUCT Outer.Inner (x INT NOT NULL);\n"
        + "STRUCT Address (city STRING NOT NULL);\n");
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.contains("NAMESPACE com.example.demo;"), ddl);
    assertTrue(ddl.contains("STRUCT Outer ("), ddl);
    assertTrue(ddl.contains("STRUCT Outer.Inner ("), ddl);
    assertTrue(ddl.contains("STRUCT Address ("), ddl);
    assertTrue(ddl.contains("inner Outer.Inner NOT NULL"), ddl);
    assertTrue(ddl.contains("addr Address"), ddl);
    // Foreign-namespace reference is not stripped.
    assertTrue(ddl.contains("ext other.ns.Thing"), ddl);
    // The document namespace prefix never appears on a local name.
    assertTrue(!ddl.contains("com.example.demo.Outer")
        && !ddl.contains("com.example.demo.Address"), ddl);
    assertRoundTrip(lt);
  }

  @Test
  void subNamespaceNamesAreNotOverSimplified() {
    // A type in a sub-namespace of the document namespace with NO local parent must stay fully
    // qualified: stripping com.example.sub.Address -> sub.Address would re-read as an unrelated
    // FQN, since the visitor only re-qualifies a dotted name when a prefix names a local parent.
    LogicalType lt = parse(
        "NAMESPACE com.example;\n"
        + "STRUCT Order (addr com.example.sub.Address NOT NULL);\n"
        + "STRUCT com.example.sub.Address (city STRING NOT NULL);\n");
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.contains("STRUCT com.example.sub.Address ("), ddl);
    assertTrue(ddl.contains("addr com.example.sub.Address NOT NULL"), ddl);
    // Type identity must survive the projection. String idempotence alone would NOT catch
    // over-stripping (sub.Address re-emits as sub.Address), so assert the FQN key round-trips.
    assertTrue(parse(ddl).getNamedTypes().containsKey("com.example.sub.Address"), ddl);
  }

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
        new Schema.Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    // Canonical shape for a leaf named root: bare STRUCT root carrying name + namespace,
    // no namedTypes entry (this is what parsing the DDL back yields).
    LogicalType lt = new LogicalType(
        "Row",
        "com.example",
        struct,
        Map.of(),
        Set.of(),
        Map.of(),
        List.of(),
        Map.of(),
        Map.of());
    assertRoundTrip(lt);
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.startsWith("NAMESPACE com.example;"),
        "expected namespace declaration first, got:\n" + ddl);
  }

  // -----------------------------------------------------------------------
  // Phase B: USING TYPE (URI bindings for external NAMED_TYPE_REFs)
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
    assertFalse(ddl.contains("USING TYPE"),
        "expected no USING TYPE for bare external, got:\n" + ddl);
    assertTrue(ddl.contains("com.example.Address"),
        "field type should still reference the external by FQN, got:\n" + ddl);
    assertRoundTrip(lt);
  }

  @Test
  void aliasCarriesUriBinding() {
    // External Ref1 carries a synthetic-wrapper URI binding via externalImports.
    // Emitter writes `USING TYPE Ref1 FOR REF '<uri>';` so the binding round-trips
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
    assertTrue(ddl.contains("USING TYPE Ref1 FOR REF 'ext.Outer';"),
        "expected USING TYPE with URI binding, got:\n" + ddl);
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
    assertTrue(ddl.contains("USING TYPE Ref1 FOR REF 'ext.O''Hare';"),
        "expected escaped single-quote in USING TYPE URI, got:\n" + ddl);
    assertRoundTrip(lt);
  }

  @Test
  void localAliasDoesNotSurviveRoundTrip() {
    // A local alias is substituted at parse time and leaves nothing in the LT, so there is no
    // binding to re-emit: the writer prints the resolved FQN inline. Contrast the REF form above,
    // which does round-trip.
    LogicalType lt = parse(
        "NAMESPACE com.example;"
            + "USING TYPE Addr FOR com.example.Address;"
            + "STRUCT Address (street VARCHAR NOT NULL);"
            + "STRUCT Employee (id BIGINT NOT NULL, home Addr NOT NULL);"
            + "TYPE Employee");

    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertFalse(ddl.contains("USING TYPE"), ddl);
    assertEquals(lt.getRootSchema().getFields().get(1).getSchema().getQualifiedName(),
        parse(ddl).getRootSchema().getFields().get(1).getSchema().getQualifiedName());
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
  void namedRootNullabilityIsPreserved() {
    // A leaf named root canonicalizes to a bare STRUCT/ENUM carrying a name. A bare named
    // declaration re-parses under sugar as NOT NULL (ambient), so to keep a *nullable* root
    // lossless the converter forces an explicit trailing `TYPE <name>` (no NOT NULL). A NOT NULL
    // named root still elides TYPE (sugar re-infers it NOT NULL). Both round-trip stably.
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)));
    LogicalType lt = new LogicalType(
        "Row",
        null,
        struct,  // nullable (Schema default)
        Map.of(),
        Set.of(),
        Map.of(),
        List.of(),
        Map.of(),
        Map.of());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.contains("TYPE `Row`;"),
        "nullable named root should force an explicit nullable TYPE, got:\n" + ddl);
    assertTrue(!ddl.contains("TYPE `Row` NOT NULL"),
        "nullable named root's TYPE must not carry NOT NULL, got:\n" + ddl);
    // Nullability survives the round-trip.
    assertTrue(parse(ddl).getRootSchema().isNullable(),
        "re-parsed nullable named root should stay nullable, got:\n" + ddl);
    assertRoundTrip(lt);

    // A NOT NULL named root elides TYPE (sugar re-infers NOT NULL — nothing to preserve).
    Schema notNullStruct = Schema.createStruct(Arrays.asList(
        new Schema.Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType notNull = new LogicalType(
        "Row", null, notNullStruct, Map.of(), Set.of(), Map.of(), List.of(), Map.of(), Map.of());
    String notNullDdl = LogicalTypeToDdlConverter.toDdl(notNull);
    assertTrue(!notNullDdl.contains("TYPE"),
        "NOT NULL named root should elide TYPE, got:\n" + notNullDdl);
    assertTrue(!parse(notNullDdl).getRootSchema().isNullable(),
        "re-parsed NOT NULL named root should stay NOT NULL, got:\n" + notNullDdl);
    assertRoundTrip(notNull);
  }

  @Test
  void testFieldNumberAndAliasesRoundTripThroughDdlWithClause() {
    // DDL has no native slot for either signal, so both ride opaquely through WITH(...).
    Map<String, Object> params = new LinkedHashMap<>();
    params.put(Schema.PROTOBUF_FIELD_NUMBER, "42");
    params.put(Schema.AVRO_ALIASES, "a_old,a_older");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.INT).setNullable(false), 0,
            null, false, null, null, params)))
        .setNullable(false);
    LogicalType lt = new LogicalType(struct);

    assertRoundTrip(lt);

    LogicalType parsed = parse(LogicalTypeToDdlConverter.toDdl(lt));
    Schema.Field a = parsed.getRootSchema().getField("a");
    assertEquals(Integer.valueOf(42), a.getFieldNumber());
    assertEquals(Arrays.asList("a_old", "a_older"), a.getAliases());
  }

  @Test
  void namedInlineRootRoundTripsThroughDdl() {
    // A named inline root (bare STRUCT + LogicalType.name) round-trips: it emits as a named
    // declaration and re-parses to the SAME bare-STRUCT root carrying the name (the visitor unwraps
    // a leaf named root back to bare + name).
    Schema root = Schema.createStruct(Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType original = new LogicalType("Order", null, root,
        Map.of(), Set.of(), Map.of(), List.of(), Map.of(), Map.of());

    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    assertTrue(ddl.contains("STRUCT Order ("), ddl);

    LogicalType parsed = parse(ddl);
    assertEquals(Schema.Type.STRUCT, parsed.getRootSchema().getType());
    assertEquals("Order", parsed.getName());
    assertEquals(original.getRootSchema(), parsed.getRootSchema());
    assertTrue(parsed.getNamedTypes().isEmpty(), parsed.getNamedTypes().keySet().toString());
  }

  // An anonymous enum prints inline, as ENUM('a', ...), wherever a type goes, and parses back.
  @Test
  void anonymousEnumsRoundTripThroughDdl() {
    LogicalType original = parse("STRUCT p (e ENUM('A', 'B') NOT NULL, s STRUCT(e ENUM('C')), "
        + "xs ARRAY<ENUM('D') NOT NULL>, m MAP<STRING, ENUM('E', 'F')>, "
        + "u UNION(b ENUM('X', 'Y'), t STRING));");
    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    assertTrue(ddl.contains("e ENUM('A', 'B') NOT NULL"), ddl);
    assertEquals(original.getRootSchema(), parse(ddl).getRootSchema());
    assertRoundTrip(original);
  }

  // A Protobuf nested enum keeps its values' numbers, gaps and aliases included, through DDL.
  @Test
  void aProtobufNestedEnumKeepsItsNumbersThroughDdl() {
    LogicalType original = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(
        "syntax = \"proto3\";\nmessage M {\n  Color c = 1;\n"
            + "  enum Color { option allow_alias = true; RED = 0; GREEN = 2; VERDE = 2; }\n}\n"));
    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    assertTrue(ddl.contains("'GREEN' WITH('protobuf.enum.number' = '2')"), ddl);
    assertEquals(original.getRootSchema(), parse(ddl).getRootSchema());
  }

  // JSON enums and consts, which have no names, are anonymous enums that parse back.
  @Test
  void jsonEnumsRoundTripThroughDdl() {
    LogicalType original = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"type\":\"object\",\"properties\":{\"e\":{\"enum\":[\"A\",\"B\"]},"
            + "\"c\":{\"const\":\"C\"}},\"required\":[\"e\"],\"additionalProperties\":false}"));
    assertEquals(original.getRootSchema(),
        parse(LogicalTypeToDdlConverter.toDdl(original)).getRootSchema());
  }

  @Test
  void aProto2BytesDefaultIsRenderedAsItsBytes() {
    // The converter records a Protobuf ByteString: its bytes, not its debug string, which carries
    // an identity hash.
    String ddl = LogicalTypeToDdlConverter.toDdl(ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema("syntax = \"proto2\";\nmessage M {\n"
            + "  optional bytes b = 1 [default = \"ab\"];\n}\n")));
    assertTrue(ddl.contains("DEFAULT x'6162'"), ddl);
  }

  // Written bare under the record's namespace, the referred type would read back as one of it.
  @Test
  void aReferredTypeOfNoNamespaceReadsBackAsItselfUnderANamespace() {
    String address = "{\"type\": \"record\", \"name\": \"PostalAddress\", \"fields\": ["
        + "{\"name\": \"street\", \"type\": \"string\"}]}";
    LogicalType original = AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(
        "{\"type\": \"record\", \"name\": \"Customer\", \"namespace\": \"shop\", "
            + "\"fields\": [{\"name\": \"id\", \"type\": \"string\"}, "
            + "{\"name\": \"addr\", \"type\": [\"null\", \"PostalAddress\"], "
            + "\"default\": null}]}",
        List.of(new SchemaReference("PostalAddress", "PostalAddress", 1)),
        Map.of("PostalAddress", address), null));
    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    assertTrue(ddl.startsWith("NAMESPACE shop;\nUSING TYPE PostalAddress FOR PostalAddress;\n"),
        ddl);
    assertEquals(Set.of("PostalAddress"), parse(ddl).getExternalTypes(), ddl);
    assertRoundTrip(original);

    // One of the record's namespace is written bare, and needs no such statement.
    LogicalType same = AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(
        "{\"type\": \"record\", \"name\": \"Customer\", \"namespace\": \"shop\", "
            + "\"fields\": [{\"name\": \"addr\", \"type\": \"PostalAddress\"}]}",
        List.of(new SchemaReference("shop.PostalAddress", "PostalAddress", 1)),
        Map.of("shop.PostalAddress", address.replace("\"fields\"",
            "\"namespace\": \"shop\", \"fields\"")), null));
    assertFalse(LogicalTypeToDdlConverter.toDdl(same).contains("USING TYPE"));
    assertRoundTrip(same);
  }

  // Both would be written Person: the namespace's own is then written with its namespace.
  @Test
  void aTypeOfNoNamespaceAndTheNamespacesOwnOfItsNameAreToldApart() {
    Schema root = Schema.createStruct(List.of(
        new Schema.Field("a", Schema.createNamedTypeRef("Person"), 0),
        new Schema.Field("b", Schema.createNamedTypeRef("shop.Person"), 1)));
    LogicalType referred = new LogicalType("Customer", "shop", root, Map.of(),
        Set.of("Person", "shop.Person"), Map.of(), List.of(), Map.of(), Map.of());
    String ddl = LogicalTypeToDdlConverter.toDdl(referred, true);
    assertTrue(ddl.startsWith("NAMESPACE shop; USING TYPE Person FOR Person; "), ddl);
    assertTrue(ddl.contains("(a Person, b shop.Person)"), ddl);
    assertEquals(Set.of("Person", "shop.Person"), parse(ddl).getExternalTypes(), ddl);
    assertRoundTrip(referred);

    // And when the namespace's own is one the script declares.
    Schema person = Schema.createStruct(List.of(
        new Schema.Field("name", Schema.createString(), 0)));
    LogicalType declared = new LogicalType("Customer", "shop", root,
        Map.of("shop.Person", person), Set.of("Person"), Map.of(), List.of(), Map.of(),
        Map.of());
    ddl = LogicalTypeToDdlConverter.toDdl(declared, true);
    assertTrue(ddl.contains("STRUCT shop.Person (name STRING)"), ddl);
    LogicalType read = parse(ddl);
    assertEquals(Set.of("Person"), read.getExternalTypes(), ddl);
    assertTrue(read.getNamedTypes().containsKey("shop.Person"), ddl);
    assertRoundTrip(declared);
  }

  // A dotted name that begins with a declared type's reads as a type nested in it.
  @Test
  void aReferredTypeWhoseNameBeginsWithADeclaredTypesReadsBackAsItself() {
    Schema root = Schema.createStruct(List.of(
        new Schema.Field("home", Schema.createNamedTypeRef("shop.PostalAddress"), 0),
        new Schema.Field("geo", Schema.createNamedTypeRef("PostalAddress.Geo"), 1),
        new Schema.Field("point", Schema.createNamedTypeRef("other.Point"), 2)));
    Schema address = Schema.createStruct(List.of(
        new Schema.Field("street", Schema.createString(), 0)));
    LogicalType lt = new LogicalType("Customer", "shop", root,
        Map.of("shop.PostalAddress", address), Set.of("PostalAddress.Geo", "other.Point"),
        Map.of(), List.of(), Map.of(), Map.of());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt, true);
    // Only the one that would be misread is aliased.
    assertTrue(ddl.startsWith("NAMESPACE shop; "
        + "USING TYPE PostalAddress.Geo FOR PostalAddress.Geo; STRUCT "), ddl);
    assertFalse(ddl.contains("USING TYPE other.Point"), ddl);
    LogicalType read = parse(ddl);
    assertEquals(Set.of("PostalAddress.Geo", "other.Point"), read.getExternalTypes(), ddl);
    assertTrue(read.getNamedTypes().containsKey("shop.PostalAddress"), ddl);
    assertRoundTrip(lt);
  }

  // The root is a type of the namespace too: named bare by a trailing TYPE, it would be the
  // type of no namespace its own name is aliased to.
  @Test
  void aRootNamedAsTheTypeOfNoNamespaceItRefersToIsStillTheRoot() {
    Schema root = Schema.createStruct(List.of(
        new Schema.Field("boss", Schema.createNamedTypeRef("Person"), 0))).setNullable(true);
    // As a type read from a schema has it: the referred type's body is among the named types.
    Schema referred = Schema.createStruct(List.of(
        new Schema.Field("name", Schema.createString(), 0)));
    LogicalType lt = new LogicalType("Person", "shop", root, Map.of("Person", referred),
        Set.of("Person"), Map.of(), List.of(), Map.of(), Map.of());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt, true);
    assertEquals("NAMESPACE shop; USING TYPE Person FOR Person; "
        + "STRUCT shop.Person (boss Person); TYPE shop.Person;", ddl);
    LogicalType read = parse(ddl);
    assertEquals(Schema.Type.STRUCT, read.getRootSchema().getType(), ddl);
    assertEquals("Person", read.getName(), ddl);
    assertEquals(Set.of("Person"), read.getExternalTypes(), ddl);
    assertEquals("Person",
        read.getRootSchema().getField("boss").getSchema().getQualifiedName(), ddl);
    assertRoundTrip(lt);
  }

  // A referred type whose name is the very name the root's declaration would have is that
  // declaration to the reader: the root is then written without a name, as it was.
  @Test
  void aRootIsNotDeclaredByTheNameOfATypeItRefersTo() {
    Schema referred = Schema.createStruct(List.of(
        new Schema.Field("x", Schema.createString(), 0)));
    // With no namespace, with one, and with one that the root's name gives too.
    for (String[] named : new String[][] {{"Order", null}, {"Order", "shop"},
        {"shop.Order", "shop"}}) {
      String namespace = named[1];
      String key = namespace == null ? "Order" : namespace + ".Order";
      Schema root = Schema.createStruct(List.of(
          new Schema.Field("id", Schema.createString(), 0),
          new Schema.Field("prev", Schema.createNamedTypeRef(key), 1)));
      LogicalType lt = new LogicalType(named[0], namespace, root, Map.of(key, referred),
          Set.of(key), Map.of(), List.of(), Map.of(), Map.of());
      String ddl = LogicalTypeToDdlConverter.toDdl(lt, true);
      assertFalse(ddl.contains("STRUCT Order ") || ddl.contains("STRUCT shop.Order "), ddl);
      LogicalType read = parse(ddl);
      assertEquals(Set.of(key), read.getExternalTypes(), ddl);
      assertEquals(ddl, LogicalTypeToDdlConverter.toDdl(read, true));
    }
  }

  // A referred type bound to a reference is not aliased to itself: its name is read with the
  // namespace, as the root's would be, so the root is written with no name.
  @Test
  void aRootIsNotDeclaredByTheNameOfATypeBoundToAReference() {
    Schema referred = Schema.createStruct(List.of(
        new Schema.Field("x", Schema.createString(), 0)));
    Schema root = Schema.createStruct(List.of(
        new Schema.Field("id", Schema.createString(), 0),
        new Schema.Field("prev", Schema.createNamedTypeRef("Order"), 1)));
    LogicalType lt = new LogicalType("Order", "shop", root, Map.of("Order", referred),
        Set.of("Order"), Map.of("Order", "common.json"), List.of(), Map.of(), Map.of());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt, true);
    assertTrue(ddl.contains("USING TYPE Order FOR REF 'common.json'"), ddl);
    assertFalse(ddl.contains("STRUCT Order "), ddl);
    LogicalType read = parse(ddl);
    assertEquals(Set.of("shop.Order"), read.getExternalTypes(), ddl);
    assertEquals(ddl, LogicalTypeToDdlConverter.toDdl(read, true));
  }

  // The root is declared in the script as a named type is: a name that begins with its name
  // reads as a type nested in it.
  @Test
  void aReferredTypeWhoseNameBeginsWithTheRootsReadsBackAsItself() {
    Schema root = Schema.createStruct(List.of(
        new Schema.Field("geo", Schema.createNamedTypeRef("Customer.Geo"), 0)));
    LogicalType lt = new LogicalType("Customer", "shop", root, Map.of(),
        Set.of("Customer.Geo"), Map.of(), List.of(), Map.of(), Map.of());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt, true);
    assertTrue(ddl.startsWith("NAMESPACE shop; USING TYPE Customer.Geo FOR Customer.Geo; "),
        ddl);
    assertEquals(Set.of("Customer.Geo"), parse(ddl).getExternalTypes(), ddl);
    assertRoundTrip(lt);
  }

  @Test
  void anAvroUnionDefaultIsItsFirstBranchsLiteralAndRoundTrips() {
    LogicalType original = AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(
        "{\"type\": \"record\", \"name\": \"ud\", \"fields\": ["
            + "{\"name\": \"u\", \"type\": [\"int\", \"string\", \"null\"], \"default\": 3}, "
            + "{\"name\": \"n\", \"type\": [\"null\", \"string\"], \"default\": null}]}"));
    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    assertTrue(ddl.contains("DEFAULT 3"), ddl);
    assertTrue(ddl.contains("DEFAULT NULL"), ddl);
    LogicalType parsed = parse(ddl);
    for (int i = 0; i < 2; i++) {
      assertEquals(original.getRootSchema().getFields().get(i).getDefaultValue(),
          parsed.getRootSchema().getFields().get(i).getDefaultValue(), ddl);
    }
    assertRoundTrip(original);
  }

  @Test
  void temporalDefaultsAreQuotedAndRoundTrip() {
    // {column as written, literal expected in the emitted DDL}
    String[][] cases = {
        {"d DATE DEFAULT '2026-01-02'", "'2026-01-02'"},
        {"t TIME DEFAULT '03:04:05'", "'03:04:05'"},
        {"t TIME DEFAULT '03:04:00'", "'03:04:00'"},
        {"ts TIMESTAMP DEFAULT '2026-01-02T03:04:05'", "'2026-01-02T03:04:05'"},
        {"ts TIMESTAMP DEFAULT '2026-01-02T03:04:00'", "'2026-01-02T03:04:00'"},
        {"ltz TIMESTAMP_LTZ DEFAULT '2026-01-02T03:04:05Z'", "'2026-01-02T03:04:05Z'"},
        // Instant's range ends, beyond LocalDateTime's
        {"ltz TIMESTAMP_LTZ DEFAULT '+1000000000-01-01T00:00:00Z'",
            "'+1000000000-01-01T00:00:00Z'"},
        {"ltz TIMESTAMP_LTZ DEFAULT '-1000000000-01-01T00:00:00Z'",
            "'-1000000000-01-01T00:00:00Z'"},
    };
    for (String[] c : cases) {
      assertTemporalDefaultRoundTrips(c[0], c[1]);
    }
  }

  @Test
  void temporalDefaultFractionsAreTrimmedToTheirSetDigits() {
    // toString() pads a fraction to 3, 6 or 9 digits, which the parser refuses beyond the precision
    String[][] cases = {
        {"t TIME(1) DEFAULT '03:04:05.1'", "'03:04:05.1'"},
        {"t TIME(3) DEFAULT '03:04:05.123'", "'03:04:05.123'"},
        {"t TIME(3) DEFAULT '03:04:05.120'", "'03:04:05.12'"},
        {"t TIME(3) DEFAULT '03:04:05'", "'03:04:05'"},
        {"t TIME(4) DEFAULT '03:04:05.1234'", "'03:04:05.1234'"},
        {"ts TIMESTAMP(1) DEFAULT '2026-01-02T03:04:05.1'", "'2026-01-02T03:04:05.1'"},
        {"ts TIMESTAMP(3) DEFAULT '2026-01-02T03:04:05.123'", "'2026-01-02T03:04:05.123'"},
        {"ts TIMESTAMP(4) DEFAULT '2026-01-02T03:04:05.1234'", "'2026-01-02T03:04:05.1234'"},
        {"ltz TIMESTAMP_LTZ(1) DEFAULT '2026-01-02T03:04:05.1Z'", "'2026-01-02T03:04:05.1Z'"},
        {"ltz TIMESTAMP_LTZ(3) DEFAULT '2026-01-02T03:04:05.123Z'",
            "'2026-01-02T03:04:05.123Z'"},
        {"ltz TIMESTAMP_LTZ(4) DEFAULT '2026-01-02T03:04:05.1234Z'",
            "'2026-01-02T03:04:05.1234Z'"},
    };
    for (String[] c : cases) {
      assertTemporalDefaultRoundTrips(c[0], c[1]);
    }
  }

  // Emits the column's DDL, checks the default literal, and parses the emitted DDL back.
  private static void assertTemporalDefaultRoundTrips(String column, String expectedLiteral) {
    LogicalType original = parse("STRUCT T (" + column + ");");
    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    assertTrue(ddl.contains("DEFAULT " + expectedLiteral), column + " emitted:\n" + ddl);
    assertEquals(original.getRootSchema().getFields().get(0).getDefaultValue(),
        parse(ddl).getRootSchema().getFields().get(0).getDefaultValue(), ddl);
    assertRoundTrip(original);
  }

  @Test
  void aNonReservedKeywordIsWrittenUnquotedAndAReservedOneQuoted() {
    // MESSAGE, RETURNING and STRUCT are names as they stand; VARIANT_GET and TRY_VARIANT_GET
    // are reserved, so a field of that name is written in backticks.
    LogicalType original = parse("TYPE STRUCT(message STRING, returning INT, struct INT, "
        + "`variant_get` INT, `try_variant_get` INT) NOT NULL;");
    String ddl = LogicalTypeToDdlConverter.toDdl(original);
    for (String name : List.of("message", "returning", "struct")) {
      assertTrue(ddl.contains(name + " ") && !ddl.contains("`" + name + "`"), ddl);
    }
    assertTrue(ddl.contains("`variant_get` ") && ddl.contains("`try_variant_get` "), ddl);
    assertRoundTrip(original);
  }

  @Test
  void aKeywordIsQuotedExactlyWhenItIsNoFieldName() {
    // Guards the writer's keyword list against the grammar's: a keyword the grammar refuses
    // as a field name is written in backticks, and one it takes is written as it stands.
    Vocabulary vocabulary = LogicalTypesLexer.VOCABULARY;
    int keywords = 0;
    for (int type = 1; type <= vocabulary.getMaxTokenType(); type++) {
      String name = vocabulary.getSymbolicName(type);
      if (name == null || type == LogicalTypesLexer.ID) {
        continue;
      }
      String word = name.toLowerCase(Locale.ROOT);
      List<? extends Token> tokens =
          new LogicalTypesLexer(CharStreams.fromString(word)).getAllTokens();
      if (tokens.size() != 1 || tokens.get(0).getType() != type) {
        continue;
      }
      keywords++;
      boolean fieldName;
      try {
        fieldName = parse("TYPE STRUCT(" + word + " INT) NOT NULL;")
            .getRootSchema().getField(word) != null;
      } catch (ValidationException e) {
        fieldName = false;
      }
      assertEquals(!fieldName, !Schema.quoteIdentifierIfNeeded(word).equals(word), word);
    }
    assertTrue(keywords > 50, "keywords found: " + keywords);
  }

  // -----------------------------------------------------------------------
  // On one line
  // -----------------------------------------------------------------------

  @Test
  void oneLineHasNoneOfTheWritersLineBreaks() {
    LogicalType lt = parse(
        "NAMESPACE com.acme;\n"
            + "ENUM Color ('RED', 'GREEN');\n"
            + "STRUCT Line (sku STRING NOT NULL, quantity INT NOT NULL, price DECIMAL(8, 2));\n"
            + "STRUCT Purchase (\n"
            + "  id BIGINT NOT NULL,\n"
            + "  color Color,\n"
            + "  lines ARRAY<Line>,\n"
            + "  total DECIMAL(10, 2),\n"
            + "  CONSTRAINT positive CHECK (total > 0) MESSAGE 'must be positive'\n"
            + ") COMMENT 'A purchase';\n");
    assertTrue(LogicalTypeToDdlConverter.toDdl(lt).contains("(\n  id BIGINT NOT NULL,\n"));
    // The root's declaration comes first, as the writer orders them.
    assertEquals("NAMESPACE com.acme; "
        + "STRUCT Purchase (id BIGINT NOT NULL, color Color, lines ARRAY<Line>, "
        + "total DECIMAL(10, 2), CONSTRAINT positive CHECK (total > 0) "
        + "MESSAGE 'must be positive') COMMENT 'A purchase'; "
        + "ENUM Color ('RED', 'GREEN'); "
        + "STRUCT Line (sku STRING NOT NULL, quantity INT NOT NULL, price DECIMAL(8, 2)); "
        + "TYPE Purchase NOT NULL;",
        LogicalTypeToDdlConverter.toDdl(lt, true));
    assertRoundTrip(lt);
  }

  @Test
  void oneLineOfAStructWithOnlyATableCheck() {
    LogicalType lt = parse("STRUCT T (a INT, CHECK (a > 0), CHECK (a < 9));");
    assertEquals("STRUCT T (a INT, CHECK (a > 0), CHECK (a < 9));",
        LogicalTypeToDdlConverter.toDdl(lt, true));
  }

  @Test
  void oneLineCheckLosesItsLineBreaksAndLineComments() {
    LogicalType lt = parse(
        "STRUCT T (\n"
            + "  a INT,\n"
            + "  note STRING CHECK (note <> '-- kept' -- gone\n"
            + "      AND note <> '// kept'),\n"
            + "  CHECK (a > 0   // gone too\r\n"
            + "    AND /* kept,\n"
            + "           joined */ a < 9\n"
            + "    AND note <> 'two\nlines')\n"
            + ");\n");
    String oneLine = LogicalTypeToDdlConverter.toDdl(lt, true);
    assertEquals("STRUCT T (a INT, "
        + "note STRING CHECK (note <> '-- kept' AND note <> '// kept'), "
        + "CHECK (a > 0 AND /* kept, joined */ a < 9 AND note <> 'two\nlines'));", oneLine);
    // The writer's own form keeps the expressions as they were written.
    assertTrue(LogicalTypeToDdlConverter.toDdl(lt).contains("-- gone\n      AND"));
    // Read back, the checks say the same without their comments and line breaks.
    Schema read = parse(oneLine).getRootSchema();
    assertEquals("note <> '-- kept' AND note <> '// kept'",
        read.getFields().get(1).getRules().get(0).getSql());
    assertEquals("a > 0 AND /* kept, joined */ a < 9 AND note <> 'two\nlines'",
        read.getRules().get(0).getSql());
    assertEquals(oneLine, LogicalTypeToDdlConverter.toDdl(parse(oneLine), true));
    // And they mean the same.
    assertEquals(lt.getRootSchema().getFields().get(1).getRules().get(0).getExpr(),
        read.getFields().get(1).getRules().get(0).getExpr());
    assertEquals(lt.getRootSchema().getRules().get(0).getExpr(),
        read.getRules().get(0).getExpr());
  }

  @Test
  void oneLineCheckKeepsItsPlaceAfterACharacterOfTwoChars() {
    // One code point and two chars: token indexes count the first, a string's the second.
    String smile = "\uD83D\uDE00";
    String three = smile + smile + smile;
    LogicalType lt = parse(
        "STRUCT T (\n"
            + "  a INT,\n"
            + "  note STRING,\n"
            + "  CHECK (note <> '" + three + "' AND note <> 'https://'  AND a > 0),\n"
            + "  CHECK (note <> '" + smile + smile + "' -- gone OR TRUE\n"
            + "    AND a < 9)\n"
            + ");\n");
    String oneLine = LogicalTypeToDdlConverter.toDdl(lt, true);
    assertEquals("STRUCT T (a INT, note STRING, "
        + "CHECK (note <> '" + three + "' AND note <> 'https://'  AND a > 0), "
        + "CHECK (note <> '" + smile + smile + "' AND a < 9));", oneLine);
    List<Rule> written = lt.getRootSchema().getRules();
    List<Rule> read = parse(oneLine).getRootSchema().getRules();
    assertEquals(written.get(0).getExpr(), read.get(0).getExpr());
    assertEquals(written.get(1).getExpr(), read.get(1).getExpr());
  }

  @Test
  void oneLineKeepsALineBreakInsideALiteral() {
    LogicalType lt = parse(
        "STRUCT T (\n"
            + "  a INT COMMENT 'first\nsecond',\n"
            + "  b STRING DEFAULT 'x\ny',\n"
            + "  c INT\n"
            + ");\n");
    String oneLine = LogicalTypeToDdlConverter.toDdl(lt, true);
    assertEquals("STRUCT T (a INT COMMENT 'first\nsecond', b STRING DEFAULT 'x\ny', c INT);",
        oneLine);
    assertEquals("first\nsecond", parse(oneLine).getRootSchema().getFields().get(0).getDoc());
  }
}
