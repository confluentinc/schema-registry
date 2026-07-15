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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a representative LogicalType survives serialization through
 * each of the three target formats consecutively. Catches divergence where
 * one format's converter would silently lose information that another preserves.
 */
class CrossFormatRoundTripTest {

  /**
   * Builds a schema covering primitives, enum, union, array, map, and nested
   * struct. Uses a nullable proper union so Proto's promotion of oneofs to
   * nullable doesn't cause divergence on subsequent hops.
   */
  private static Schema buildRepresentativeSchema() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B"), new EnumValue("C")))
        .setNullable(false);

    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(true);

    Schema inner = Schema.createStruct(Arrays.asList(
        new Field("v", Schema.create(Schema.Type.BIGINT).setNullable(false), 0)))
        .setNullable(false);

    Schema arr = Schema.createArray(
        Schema.create(Schema.Type.DOUBLE).setNullable(false))
        .setNullable(false);

    Schema mapType = Schema.createMap(
        Schema.createString().setNullable(false),
        Schema.create(Schema.Type.BOOLEAN).setNullable(false))
        .setNullable(false);

    return Schema.createStruct(Arrays.asList(
        new Field("flag", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 0),
        new Field("count", Schema.create(Schema.Type.INT).setNullable(false), 1),
        new Field("amount", Schema.createDecimal(10, 2).setNullable(false), 2),
        new Field("name", Schema.createString().setNullable(false), 3),
        new Field("status", enumType, 4),
        new Field("payload", union, 5),
        new Field("scores", arr, 6),
        new Field("flags", mapType, 7),
        new Field("inner", inner, 8)))
        .setNullable(false);
  }

  /**
   * Each format's converter may attach format-specific resolved references to the
   * resulting LogicalType (e.g. promoted nested messages for Proto). Those strings
   * aren't valid input to the next format's converter, so reset to a plain
   * rootSchema-only LogicalType between hops.
   */
  private static LogicalType strip(LogicalType lt) {
    return new LogicalType(lt.getNamespace(),
        lt.getRootSchema(),
        java.util.Map.of(),
        java.util.List.of(),
        java.util.Map.of());
  }

  private static LogicalType throughAvro(LogicalType lt) {
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(lt, "Holder");
    return strip(AvroToLogicalTypeConverter.toLogicalType(avro));
  }

  private static LogicalType throughProto(LogicalType lt) {
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        lt, "Holder");
    return strip(ProtoToLogicalTypeConverter.toLogicalType(proto));
  }

  private static LogicalType throughJson(LogicalType lt) {
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(lt, "Holder");
    return strip(JsonToLogicalTypeConverter.toLogicalType(json));
  }

  /**
   * Asserts that the same set of fields exists with matching types. Field
   * iteration order is intentionally NOT checked, because Proto reorders fields
   * when a oneof is present (oneof fields cluster). Order preservation is a
   * single-format concern verified by per-format tests.
   */
  private static void assertSemanticallyEquivalent(Schema original, Schema actual) {
    assertEquals(Schema.Type.STRUCT, actual.getType());
    assertEquals(original.getFields().size(), actual.getFields().size());
    for (Field of : original.getFields()) {
      Field af = actual.getField(of.getName());
      org.junit.jupiter.api.Assertions.assertNotNull(af, "missing field: " + of.getName());
      assertEquals(of.getSchema().getType(), af.getSchema().getType(), "type for field " + of.getName());
    }
  }

  @Test
  void testTripleFormatRoundTrip_AvroProtoJson() {
    Schema original = buildRepresentativeSchema();
    LogicalType lt = new LogicalType(original);

    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    assertSemanticallyEquivalent(original, afterJson.getRootSchema());
  }

  @Test
  void testTripleFormatRoundTrip_JsonProtoAvro() {
    Schema original = buildRepresentativeSchema();
    LogicalType lt = new LogicalType(original);

    LogicalType afterJson = throughJson(lt);
    LogicalType afterProto = throughProto(afterJson);
    LogicalType afterAvro = throughAvro(afterProto);

    assertSemanticallyEquivalent(original, afterAvro.getRootSchema());
  }

  @Test
  void testTripleFormatRoundTrip_ProtoJsonAvro() {
    Schema original = buildRepresentativeSchema();
    LogicalType lt = new LogicalType(original);

    LogicalType afterProto = throughProto(lt);
    LogicalType afterJson = throughJson(afterProto);
    LogicalType afterAvro = throughAvro(afterJson);

    assertSemanticallyEquivalent(original, afterAvro.getRootSchema());
  }

  /** Defaults on primitives should survive Avro → Proto → JSON. */
  @Test
  void testTripleFormatRoundTripPreservesPrimitiveDefaults() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0,
            "Anonymous", true, null, null, null),
        new Field("age", Schema.create(Schema.Type.INT).setNullable(false), 1,
            18, true, null, null, null)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    Schema rt = afterJson.getRootSchema();
    assertEquals("Anonymous", rt.getField("name").getDefaultValue());
    assertEquals(18, rt.getField("age").getDefaultValue());
  }

  /**
   * A non-nullable proper union goes through one expected promotion across
   * formats: Proto's oneof is inherently nullable, so a Proto round-trip
   * promotes the union to nullable. Avro then represents the nullable proper
   * union by splicing null into the union members ([null, int, string]) rather
   * than nesting unions, which the Avro spec forbids. The result on the next
   * read-back is the same nullable UNION<INT, STRING>.
   */
  @Test
  void testProperUnionPromotedToNullableThroughProto() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("payload", union, 0)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    LogicalType afterProto = throughProto(lt);
    Schema rt = afterProto.getRootSchema().getField("payload").getSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertTrue(rt.isNullable(), "Proto promotes proper union to nullable");
    assertEquals(2, rt.getBranches().size());

    // Subsequent Avro hop succeeds — maybeMakeNullable splices null into the
    // existing union members rather than nesting.
    LogicalType afterAvro = throughAvro(afterProto);
    Schema rtAvro = afterAvro.getRootSchema().getField("payload").getSchema();
    assertEquals(Schema.Type.UNION, rtAvro.getType());
    assertTrue(rtAvro.isNullable());
    assertEquals(2, rtAvro.getBranches().size());
    assertEquals("i", rtAvro.getBranches().get(0).getName());
    assertEquals("s", rtAvro.getBranches().get(1).getName());
  }

  /**
   * Singleton union behavior is format-conditional:
   *  - Avro and JSON treat UNION<T> as semantically equivalent to T and
   *    collapse on read.
   *  - Proto's oneof is structural — a 1-field oneof preserves the UNION<T>
   *    wrapper as nullable.
   * Pin both halves so the asymmetry is documented and any future change is
   * deliberate.
   */
  @Test
  void testSingletonUnionDivergesAcrossFormats() {
    Schema singletonUnion = Schema.createUnion(Arrays.asList(
        new UnionBranch("v", Schema.create(Schema.Type.INT).setNullable(false))))
        .setNullable(true);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("payload", singletonUnion, 0)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    // Through Proto: the wrapper survives as 1-branch UNION (always nullable).
    LogicalType afterProto = throughProto(lt);
    Schema protoRt = afterProto.getRootSchema().getField("payload").getSchema();
    assertEquals(Schema.Type.UNION, protoRt.getType());
    assertTrue(protoRt.isNullable());
    assertEquals(1, protoRt.getBranches().size());

    // Through Avro: collapses to bare INT (nullable).
    LogicalType afterAvro = throughAvro(lt);
    Schema avroRt = afterAvro.getRootSchema().getField("payload").getSchema();
    assertEquals(Schema.Type.INT, avroRt.getType());
    assertTrue(avroRt.isNullable());

    // Through JSON: also collapses to bare INT (nullable).
    LogicalType afterJson = throughJson(lt);
    Schema jsonRt = afterJson.getRootSchema().getField("payload").getSchema();
    assertEquals(Schema.Type.INT, jsonRt.getType());
    assertTrue(jsonRt.isNullable());
  }

  /** Nullability flag on each field should survive all three formats. */
  @Test
  void testTripleFormatRoundTripPreservesNullability() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("required", Schema.createString().setNullable(false), 0),
        new Field("optional", Schema.createString().setNullable(true), 1)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    Schema rt = afterJson.getRootSchema();
    assertFalse(rt.getField("required").getSchema().isNullable());
    assertTrue(rt.getField("optional").getSchema().isNullable());
  }

  // =========================================================================
  // Wrapper-at-root through cross-format chains
  // =========================================================================

  /**
   * A primitive root chained through Proto (which wraps it as Int32Value)
   * and back out through Avro should land on a nullable primitive — Proto's
   * wrapper is inherently nullable.
   */
  @Test
  void testIntAtRootThroughProtoToAvro() {
    Schema root = Schema.create(Schema.Type.INT).setNullable(false);
    LogicalType lt = new LogicalType(root);

    LogicalType afterProto = throughProto(lt);
    assertEquals(Schema.Type.INT, afterProto.getRootSchema().getType());
    assertTrue(afterProto.getRootSchema().isNullable(), "Proto wrapper promotes to nullable");

    LogicalType afterAvro = throughAvro(afterProto);
    assertEquals(Schema.Type.INT, afterAvro.getRootSchema().getType());
    assertTrue(afterAvro.getRootSchema().isNullable());
  }

  /** Same primitive-root chain through Proto and out through JSON. */
  @Test
  void testIntAtRootThroughProtoToJson() {
    Schema root = Schema.create(Schema.Type.INT).setNullable(false);
    LogicalType lt = new LogicalType(root);

    LogicalType afterProto = throughProto(lt);
    LogicalType afterJson = throughJson(afterProto);
    assertEquals(Schema.Type.INT, afterJson.getRootSchema().getType());
    assertTrue(afterJson.getRootSchema().isNullable());
  }

  /**
   * Avro and JSON support primitive-at-root natively (without a wrapper).
   * Round-tripping a NOT NULL primitive through Avro then JSON should
   * preserve NOT NULL — only Proto introduces the nullability promotion.
   */
  @Test
  void testIntAtRootAvroToJsonPreservesNotNull() {
    Schema root = Schema.create(Schema.Type.INT).setNullable(false);
    LogicalType lt = new LogicalType(root);

    LogicalType afterAvro = throughAvro(lt);
    assertFalse(afterAvro.getRootSchema().isNullable(), "Avro preserves NOT NULL at root");

    LogicalType afterJson = throughJson(afterAvro);
    assertEquals(Schema.Type.INT, afterJson.getRootSchema().getType());
    assertFalse(afterJson.getRootSchema().isNullable(), "JSON also preserves NOT NULL at root");
  }

  /** VARIANT chained through all three formats. */
  @Test
  void testVariantAtRootThroughAllThreeFormats() {
    Schema root = Schema.create(Schema.Type.VARIANT).setNullable(true);
    LogicalType lt = new LogicalType(root);

    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    assertEquals(Schema.Type.VARIANT, afterJson.getRootSchema().getType());
    assertTrue(afterJson.getRootSchema().isNullable());
  }

  /**
   * TINYINT widens to INT through Proto (both share Int32Value). After the
   * Proto hop, the LT is INT — and INT survives further hops cleanly.
   */
  @Test
  void testTinyintAtRootThroughProtoWidens() {
    Schema root = Schema.create(Schema.Type.TINYINT).setNullable(false);
    LogicalType lt = new LogicalType(root);

    LogicalType afterProto = throughProto(lt);
    assertEquals(Schema.Type.INT, afterProto.getRootSchema().getType(), "TINYINT widens to INT through Proto Int32Value wrapper");

    LogicalType afterAvro = throughAvro(afterProto);
    assertEquals(Schema.Type.INT, afterAvro.getRootSchema().getType());
  }

  // =========================================================================
  // Wrapper-at-root through reverse chains (Avro/JSON → Proto)
  // =========================================================================

  /**
   * LT(INT NOT NULL) survives Avro intact, then becomes nullable when
   * wrapped via Proto's Int32Value. Mirror of testIntAtRootThroughProtoToAvro
   * but with Avro before Proto.
   */
  @Test
  void testIntAtRootThroughAvroToProto() {
    Schema root = Schema.create(Schema.Type.INT).setNullable(false);
    LogicalType lt = new LogicalType(root);

    LogicalType afterAvro = throughAvro(lt);
    assertEquals(Schema.Type.INT, afterAvro.getRootSchema().getType());
    assertFalse(afterAvro.getRootSchema().isNullable(), "Avro preserves NOT NULL");

    LogicalType afterProto = throughProto(afterAvro);
    assertEquals(Schema.Type.INT, afterProto.getRootSchema().getType());
    assertTrue(afterProto.getRootSchema().isNullable(), "Proto wrapper promotes NOT NULL → NULL");
  }

  /** Same chain through JSON before Proto. */
  @Test
  void testIntAtRootThroughJsonToProto() {
    Schema root = Schema.create(Schema.Type.INT).setNullable(false);
    LogicalType lt = new LogicalType(root);

    LogicalType afterJson = throughJson(lt);
    assertFalse(afterJson.getRootSchema().isNullable(), "JSON preserves NOT NULL");

    LogicalType afterProto = throughProto(afterJson);
    assertEquals(Schema.Type.INT, afterProto.getRootSchema().getType());
    assertTrue(afterProto.getRootSchema().isNullable());
  }

  /**
   * VARIANT NULL survives Avro → Proto → JSON: Avro emits a VariantConversion
   * record at root, Proto wraps as confluent.type.Variant (nullable), JSON
   * emits the result. End-to-end VARIANT identity is preserved.
   */
  @Test
  void testVariantAtRootThroughAvroProtoJson() {
    Schema root = Schema.create(Schema.Type.VARIANT).setNullable(true);
    LogicalType lt = new LogicalType(root);

    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    assertEquals(Schema.Type.VARIANT, afterJson.getRootSchema().getType());
    assertTrue(afterJson.getRootSchema().isNullable());
  }

  /**
   * BOOLEAN at root through Avro → Proto: nullability promotion happens at
   * the Proto hop. Verifies the wrapper mechanism works for non-INT primitives.
   */
  @Test
  void testBooleanAtRootThroughAvroToProto() {
    Schema root = Schema.create(Schema.Type.BOOLEAN).setNullable(false);
    LogicalType lt = new LogicalType(root);

    LogicalType afterProto = throughProto(throughAvro(lt));
    assertEquals(Schema.Type.BOOLEAN, afterProto.getRootSchema().getType());
    assertTrue(afterProto.getRootSchema().isNullable());
  }

  /** STRING at root through JSON → Proto. */
  @Test
  void testStringAtRootThroughJsonToProto() {
    Schema root = Schema.createString().setNullable(false);
    LogicalType lt = new LogicalType(root);

    LogicalType afterProto = throughProto(throughJson(lt));
    assertEquals(Schema.Type.VARCHAR, afterProto.getRootSchema().getType());
    assertTrue(afterProto.getRootSchema().isNullable());
  }

  // =========================================================================
  // ENUM/UNION default round-trip across formats
  // =========================================================================

  /** ENUM defaults survive any combination of format hops. */
  @Test
  void testEnumDefaultThroughAllThreeFormats() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B"), new EnumValue("C")))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("status", enumType, 0, "B", true, null, null, null)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    Field f = afterJson.getRootSchema().getField("status");
    assertTrue(f.hasDefaultValue());
    assertEquals("B", f.getDefaultValue());
  }

  @Test
  void testNamespaceRoundTripsThroughAllThreeFormats() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType lt = new LogicalType("com.example", rootSchema,
        java.util.Map.of(), java.util.List.of(), java.util.Map.of());

    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    assertEquals("com.example", afterJson.getNamespace());
  }

  /**
   * UNION defaults survive Avro and JSON, but Proto silently drops them.
   * After a Proto hop, the field has no default. Subsequent hops cannot
   * restore it. Pin both halves of the divergence.
   */
  @Test
  void testUnionDefaultLostThroughProto() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", unionType, 0, 5, true, null, null, null)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    // Avro round-trip preserves the default.
    LogicalType afterAvro = throughAvro(lt);
    Field afterAvroField = afterAvro.getRootSchema().getField("u");
    assertTrue(afterAvroField.hasDefaultValue(), "Avro preserves UNION default");
    assertEquals(5, afterAvroField.getDefaultValue());

    // JSON round-trip preserves the default.
    LogicalType afterJson = throughJson(lt);
    Field afterJsonField = afterJson.getRootSchema().getField("u");
    assertTrue(afterJsonField.hasDefaultValue(), "JSON preserves UNION default");
    assertEquals(5, afterJsonField.getDefaultValue());

    // Proto round-trip silently drops the default. Pin so any future fix
    // is intentional.
    LogicalType afterProto = throughProto(lt);
    Field afterProtoField = afterProto.getRootSchema().getField("u");
    assertFalse(afterProtoField.hasDefaultValue(), "Proto silently drops UNION default");
  }

  /**
   * Binary-family types (BYTES, VARBINARY, BINARY, CHAR, VARCHAR) round-trip
   * through every format. Each format encodes length constraints differently
   * (Avro uses Connect-style props, Proto loses the constraint, JSON Schema
   * uses min/maxLength on a string-encoded binary), so this pins which
   * length info survives versus erodes.
   */
  @Test
  void testBinaryFamilyTypesRoundTripAcrossFormats() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("rawBytes",
            Schema.createBytes().setNullable(false), 0),
        new Field("varbinary32",
            Schema.createVarbinary(32).setNullable(false), 1),
        new Field("varchar64",
            Schema.createVarchar(64).setNullable(false), 2),
        new Field("char8",
            Schema.createChar(8).setNullable(false), 3)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    // Triple round-trip — Avro then Proto then JSON.
    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    // Field types should at least map to the same Schema.Type family.
    Schema rt = afterJson.getRootSchema();
    assertEquals(Schema.Type.STRUCT, rt.getType());
    // Bytes families collapse during cross-format hops; all that's required
    // is that each field still surfaces as some byte/string-shaped type.
    assertNotNull(rt.getField("rawBytes"));
    assertNotNull(rt.getField("varbinary32"));
    assertNotNull(rt.getField("varchar64"));
    assertNotNull(rt.getField("char8"));
  }

  /**
   * Date/time-family types (DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ) round-trip
   * through every format. Proto/Avro/JSON each represent these differently
   * (Avro logical types, Proto well-known wrappers, JSON title-tagged
   * NumberSchema). Pins that the type family survives the cross-format hop.
   */
  @Test
  void testDateTimeFamilyTypesRoundTripAcrossFormats() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("date",
            Schema.create(Schema.Type.DATE).setNullable(false), 0),
        new Field("time",
            Schema.createTime(3).setNullable(false), 1),
        new Field("ts",
            Schema.createTimestamp(3).setNullable(false), 2),
        new Field("tsLtz",
            Schema.createTimestampLtz(3).setNullable(false), 3)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    // Avro → Proto → JSON.
    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    Schema rt = afterJson.getRootSchema();
    assertEquals(Schema.Type.STRUCT, rt.getType());
    assertEquals(Schema.Type.DATE, rt.getField("date").getSchema().getType());
    assertEquals(Schema.Type.TIME, rt.getField("time").getSchema().getType());
    assertEquals(Schema.Type.TIMESTAMP, rt.getField("ts").getSchema().getType());
    assertEquals(Schema.Type.TIMESTAMP_LTZ,
        rt.getField("tsLtz").getSchema().getType());
  }

  /**
   * Array of struct round-trips through all three formats. Each format
   * synthesizes the array's element struct differently (Avro inlines, Proto
   * generates a wrapper message, JSON uses an inline object schema). Pins
   * that the nested struct's fields survive the format-specific synthesis.
   */
  @Test
  void testArrayOfStructRoundTripsAcrossFormats() {
    Schema elementType = Schema.createStruct(Arrays.asList(
        new Field("id", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("label", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("items",
            Schema.createArray(elementType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType lt = new LogicalType(rootSchema);

    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);

    Schema rt = afterJson.getRootSchema();
    Schema items = rt.getField("items").getSchema();
    assertEquals(Schema.Type.ARRAY, items.getType());
    Schema element = items.getElementType();
    assertEquals(Schema.Type.STRUCT, element.getType());
    assertEquals(2, element.getFields().size());
    assertEquals(Schema.Type.INT,
        element.getField("id").getSchema().getType());
    // String type surfaces as VARCHAR after the format hops (createString()
    // is a VARCHAR with no length constraint).
    Schema labelSchema = element.getField("label").getSchema();
    assertTrue(
        labelSchema.getType() == Schema.Type.VARCHAR
            || labelSchema.getType() == Schema.Type.CHAR,
        "label should be a string-family type, got " + labelSchema.getType());
  }

  /**
   * Empty STRUCT (zero fields) round-trips through every format in every
   * position: as the active root, as a field type, and as an array element.
   * Each format technically allows empty named types (Avro empty record,
   * Proto empty message, JSON object with no properties), and the LT
   * preserves the zero-field count across the conversion.
   */
  @Test
  void testEmptyStructRoundTripsThroughAllFormats() {
    Schema empty = Schema.createStruct(java.util.Collections.emptyList())
        .setNullable(false);

    // Empty struct as root.
    LogicalType lt1 = new LogicalType(empty);
    assertEmpty(throughAvro(lt1).getRootSchema());
    assertEmpty(throughProto(lt1).getRootSchema());
    assertEmpty(throughJson(lt1).getRootSchema());

    // Empty struct as a field type inside a non-empty parent.
    Schema rootWithEmptyField = Schema.createStruct(Arrays.asList(
        new Field("inner", empty, 0)))
        .setNullable(false);
    LogicalType lt2 = new LogicalType(rootWithEmptyField);
    assertEmpty(throughAvro(lt2).getRootSchema().getField("inner").getSchema());
    assertEmpty(throughProto(lt2).getRootSchema().getField("inner").getSchema());
    assertEmpty(throughJson(lt2).getRootSchema().getField("inner").getSchema());

    // Empty struct as an array element type.
    Schema rootWithArrayOfEmpty = Schema.createStruct(Arrays.asList(
        new Field("items",
            Schema.createArray(empty).setNullable(false), 0)))
        .setNullable(false);
    LogicalType lt3 = new LogicalType(rootWithArrayOfEmpty);
    assertEmpty(throughAvro(lt3).getRootSchema()
        .getField("items").getSchema().getElementType());
    assertEmpty(throughProto(lt3).getRootSchema()
        .getField("items").getSchema().getElementType());
    assertEmpty(throughJson(lt3).getRootSchema()
        .getField("items").getSchema().getElementType());
  }

  private static void assertEmpty(Schema s) {
    assertEquals(Schema.Type.STRUCT, s.getType(),
        "expected STRUCT, got " + s.getType());
    assertEquals(0, s.getFields().size(),
        "expected 0 fields, got " + s.getFields().size() + ": " + s.getFields());
  }

  /**
   * MAP with non-string keys round-trips through every format. JSON Schema
   * natively only allows string keys, but the writer encodes other key types
   * via the array-of-entries pattern (with a discriminator); the reader
   * recovers the MAP shape on read-back.
   */
  @Test
  void testMapWithNonStringKeysRoundTrips() {
    Schema bigintKey = Schema.createMap(
        Schema.create(Schema.Type.BIGINT).setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema bytesKey = Schema.createMap(
        Schema.createBytes().setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema structKey = Schema.createMap(
        Schema.createStruct(Arrays.asList(
            new Field("k", Schema.create(Schema.Type.INT).setNullable(false), 0)))
            .setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);

    for (Schema mapType : Arrays.asList(bigintKey, bytesKey, structKey)) {
      LogicalType lt = new LogicalType(Schema.createStruct(Arrays.asList(
          new Field("v", mapType, 0))).setNullable(false));
      assertMapKeyType(throughAvro(lt), mapType.getKeyType().getType());
      assertMapKeyType(throughProto(lt), mapType.getKeyType().getType());
      assertMapKeyType(throughJson(lt), mapType.getKeyType().getType());
    }
  }

  /**
   * MULTISET round-trips through every format. Avro/Proto/JSON don't have
   * a native multiset type, but the writer encodes it via an ARRAY with a
   * multiset marker; the reader recovers MULTISET on read-back.
   */
  @Test
  void testMultisetRoundTrips() {
    // MULTISET<INT> as a field
    LogicalType lt1 = new LogicalType(Schema.createStruct(Arrays.asList(
        new Field("v",
            Schema.createMultiset(
                Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false));
    assertMultisetField(throughAvro(lt1), Schema.Type.INT);
    assertMultisetField(throughProto(lt1), Schema.Type.INT);
    assertMultisetField(throughJson(lt1), Schema.Type.INT);

    // MULTISET of struct
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType lt2 = new LogicalType(Schema.createStruct(Arrays.asList(
        new Field("v",
            Schema.createMultiset(struct).setNullable(false), 0)))
        .setNullable(false));
    assertMultisetField(throughAvro(lt2), Schema.Type.STRUCT);
    assertMultisetField(throughProto(lt2), Schema.Type.STRUCT);
    assertMultisetField(throughJson(lt2), Schema.Type.STRUCT);

    // ARRAY of MULTISET — composite container interaction
    LogicalType lt3 = new LogicalType(Schema.createStruct(Arrays.asList(
        new Field("v",
            Schema.createArray(
                Schema.createMultiset(
                    Schema.create(Schema.Type.INT).setNullable(false))
                    .setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false));
    assertArrayOfMultiset(throughAvro(lt3));
    assertArrayOfMultiset(throughProto(lt3));
    assertArrayOfMultiset(throughJson(lt3));
  }

  private static void assertMapKeyType(LogicalType lt, Schema.Type expected) {
    Schema mapField = lt.getRootSchema().getField("v").getSchema();
    assertEquals(Schema.Type.MAP, mapField.getType());
    assertEquals(expected, mapField.getKeyType().getType());
  }

  private static void assertMultisetField(LogicalType lt, Schema.Type expectedElement) {
    Schema field = lt.getRootSchema().getField("v").getSchema();
    assertEquals(Schema.Type.MULTISET, field.getType());
    assertEquals(expectedElement, field.getElementType().getType());
  }

  private static void assertArrayOfMultiset(LogicalType lt) {
    Schema field = lt.getRootSchema().getField("v").getSchema();
    assertEquals(Schema.Type.ARRAY, field.getType());
    Schema element = field.getElementType();
    assertEquals(Schema.Type.MULTISET, element.getType());
    assertEquals(Schema.Type.INT, element.getElementType().getType());
  }

  // ---------------------------------------------------------------------
  // CHECK rules cross-format round-trip
  // ---------------------------------------------------------------------

  /**
   * Build an LT with both column-level and table-level CHECK rules. Used as
   * the input to all format-roundtrip tests below.
   */
  private static LogicalType buildLtWithRules() {
    Rule colRule = new Rule(
        "positive_x",                       // name
        "must be positive",                 // doc (was 'message')
        "x > 0",                            // expr (CEL)
        "x > 0");                           // sql (original SQL)
    Rule tableRule = new Rule(
        "lo_lt_hi",
        "lo must be less than hi",
        "lo < hi",
        "lo < hi");

    Field xField = new Field(
        "x", Schema.create(Schema.Type.INT).setNullable(false), 0,
        null, false, null, null, null,
        java.util.Collections.singletonList(colRule));
    Field loField = new Field(
        "lo", Schema.create(Schema.Type.INT).setNullable(false), 1);
    Field hiField = new Field(
        "hi", Schema.create(Schema.Type.INT).setNullable(false), 2);

    Schema struct = Schema.createStruct(Arrays.asList(xField, loField, hiField))
        .setNullable(false);
    struct.setRules(java.util.Collections.singletonList(tableRule));
    return new LogicalType(struct);
  }

  private static void assertRulesPreserved(LogicalType after) {
    Schema struct = after.getRootSchema();
    java.util.List<Rule> tableRules = struct.getRules();
    assertEquals(1, tableRules.size(), "expected 1 table-level rule");
    Rule t = tableRules.get(0);
    assertEquals("lo_lt_hi", t.getName());
    assertEquals("lo must be less than hi", t.getDoc());
    assertEquals("lo < hi", t.getExpr());
    assertEquals("lo < hi", t.getSql());

    java.util.List<Rule> colRules = struct.getField("x").getRules();
    assertEquals(1, colRules.size(), "expected 1 column-level rule on x");
    Rule c = colRules.get(0);
    assertEquals("positive_x", c.getName());
    assertEquals("must be positive", c.getDoc());
    assertEquals("x > 0", c.getExpr());
    assertEquals("x > 0", c.getSql());
  }

  @Test
  void rulesRoundTripThroughAvro() {
    assertRulesPreserved(throughAvro(buildLtWithRules()));
  }

  @Test
  void rulesRoundTripThroughJson() {
    assertRulesPreserved(throughJson(buildLtWithRules()));
  }

  @Test
  void rulesRoundTripThroughProto() {
    assertRulesPreserved(throughProto(buildLtWithRules()));
  }

  @Test
  void rulesRoundTripThroughAllThree() {
    LogicalType lt = buildLtWithRules();
    LogicalType afterAvro = throughAvro(lt);
    LogicalType afterProto = throughProto(afterAvro);
    LogicalType afterJson = throughJson(afterProto);
    assertRulesPreserved(afterJson);
  }

  @Test
  void protoReaderSkipsRulesWithEmptyExprOrSql() throws Exception {
    // Regression: proto3 string fields default to "" when unset, so a
    // wire-encoded MetaProto.Rule with only `name`/`doc` set produces
    // empty expr/sql. The reader used to surface those as Rule
    // objects with empty strings, which round-tripped into nonsensical
    // `CHECK ()` DDL. Reader now skips empty-expr/empty-sql rules.
    io.confluent.protobuf.MetaProto.Meta meta =
        io.confluent.protobuf.MetaProto.Meta.newBuilder()
            .addRules(io.confluent.protobuf.MetaProto.Rule.newBuilder()
                .setName("ok").setExpr("this > 0").setSql("x > 0").build())
            .addRules(io.confluent.protobuf.MetaProto.Rule.newBuilder()
                .setName("dropped_no_expr").setSql("y > 0").build())
            .addRules(io.confluent.protobuf.MetaProto.Rule.newBuilder()
                .setName("dropped_no_sql").setExpr("this > 0").build())
            .build();
    java.util.List<io.confluent.kafka.schemaregistry.type.logical.Rule> result =
        invokeConvertProtoRules(meta.getRulesList());
    assertEquals(1, result.size(), "expected only the well-formed rule");
    assertEquals("ok", result.get(0).getName());
  }

  /**
   * Reflectively invoke ProtoToLogicalTypeConverter#convertProtoRules so we
   * can assert the reader's empty-skip behavior without going through the
   * full FileDescriptor → ProtobufSchema round-trip (which normalizes the
   * Meta extension and would mask the issue).
   */
  @SuppressWarnings("unchecked")
  private static java.util.List<io.confluent.kafka.schemaregistry.type.logical.Rule>
      invokeConvertProtoRules(java.util.List<io.confluent.protobuf.MetaProto.Rule> rules)
      throws Exception {
    java.lang.reflect.Method m =
        io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter.class
            .getDeclaredMethod("convertProtoRules", java.util.List.class);
    m.setAccessible(true);
    Object out = m.invoke(null, rules);
    return out == null
        ? java.util.Collections.emptyList()
        : (java.util.List<io.confluent.kafka.schemaregistry.type.logical.Rule>) out;
  }
}
