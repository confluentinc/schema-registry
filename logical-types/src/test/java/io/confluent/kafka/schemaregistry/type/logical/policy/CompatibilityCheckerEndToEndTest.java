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

package io.confluent.kafka.schemaregistry.type.logical.policy;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.policy.Incompatibility.Rule;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compatibility checks driven from real Avro, Protobuf, and JSON Schema documents rather than from
 * hand-built logical types.
 *
 * <p><b>Why this file exists.</b> Every other compatibility test constructs an SRLT directly, which
 * makes them precise about the rules but blind to the derivation. That blindness hid a real defect:
 * the container relaxation read a default from {@link Schema.Field}, whereas the converters record a
 * derived default (an absent {@code repeated} field is an empty list, an absent proto map an empty
 * map) only in the schema's path-keyed map. Hand-built fixtures set the field flag, so the tests
 * passed while the relaxation never fired on anything a converter actually produced. Cases here start
 * from schema text, so an assumption about the derivation cannot go unnoticed again.
 *
 * <p>These are deliberately few and load-bearing. They cover the derivation facts the rules depend
 * on, not the rule matrix — that belongs in the per-mode suites.
 */
class CompatibilityCheckerEndToEndTest {

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static LogicalType fromProto(String protoText) {
    return ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(protoText));
  }

  private static LogicalType fromAvro(String avroText) {
    return AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(avroText));
  }

  private static LogicalType fromJson(String jsonText) {
    return JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(jsonText));
  }

  private static Set<Rule> rulesOf(Mode mode, LogicalType original, LogicalType update) {
    CompatibilityResult result = CompatibilityChecker.compare(mode, original, update);
    return result.getIncompatibilities().isEmpty()
        ? EnumSet.noneOf(Rule.class)
        : result.getIncompatibilities().stream()
            .map(Incompatibility::getRule)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(Rule.class)));
  }

  private static void assertCompatible(Mode mode, LogicalType original, LogicalType update) {
    CompatibilityResult result = CompatibilityChecker.compare(mode, original, update);
    assertTrue(result.isCompatible(),
        mode + " expected compatible but got: " + result.describe());
  }

  private static void assertSingle(
      Mode mode, LogicalType original, LogicalType update, Rule expected) {
    CompatibilityResult result = CompatibilityChecker.compare(mode, original, update);
    List<Rule> rules = result.getIncompatibilities().stream()
        .map(Incompatibility::getRule)
        .collect(Collectors.toList());
    assertEquals(1, rules.size(), mode + ": " + result.describe());
    assertEquals(expected, rules.get(0), mode + ": " + result.describe());
  }

  /** As {@link #assertSingle}, additionally pinning the path the finding is reported at. */
  private static void assertSingleAt(
      Mode mode, LogicalType original, LogicalType update, Rule expected, String expectedPath) {
    CompatibilityResult result = CompatibilityChecker.compare(mode, original, update);
    List<Incompatibility> found = result.getIncompatibilities();
    assertEquals(1, found.size(), mode + ": " + result.describe());
    assertEquals(expected, found.get(0).getRule(), mode + ": " + result.describe());
    assertEquals(expectedPath, found.get(0).getPath(), mode + ": " + result.describe());
  }

  // ---------------------------------------------------------------------------------------------
  // Protobuf -- derived container defaults, the case that was broken
  // ---------------------------------------------------------------------------------------------

  @Test
  void addingARepeatedProtoFieldIsAcceptedBecauseItsDefaultIsAnEmptyList() {
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; repeated string tags = 2; }\n");

    // The derived ARRAY is NOT NULL, so this only passes if the relaxation finds the empty-list
    // default the converter recorded in the path-keyed map.
    assertCompatible(Mode.ICEBERG_V2, before, after);
    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingAProtoMapFieldIsAcceptedBecauseItsDefaultIsAnEmptyMap() {
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; map<string, string> labels = 2; }\n");

    assertCompatible(Mode.ICEBERG_V2, before, after);
    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingARepeatedFieldInsideANestedProtoMessageIsAccepted() {
    // Inner is declared inside M, so the file has a single top-level message and the recorded
    // default paths line up with field positions in the returned root struct.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { Inner inner = 1;\n"
            + "  message Inner { string a = 1; } }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { Inner inner = 1;\n"
            + "  message Inner { string a = 1; repeated string tags = 2; } }\n");

    // Exercises the index path one level down.
    assertCompatible(Mode.ICEBERG_V2, before, after);
    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingAContainerInsideAnArrayElementIsAccepted() {
    // Previously rejected, and deliberately so: the walk passed a null index path below an ARRAY
    // because Avro and Protobuf disagree on whether to append an element index, which made every
    // derived default below an array unreachable. Reading the default off Schema.Field removes the
    // question -- there is no path to get wrong.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { repeated Inner items = 1;\n"
            + "  message Inner { string a = 1; } }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { repeated Inner items = 1;\n"
            + "  message Inner { string a = 1; repeated string tags = 2; } }\n");

    for (Mode mode : new Mode[] {Mode.FLINK, Mode.ICEBERG_V2, Mode.ICEBERG_V3}) {
      assertCompatible(mode, before, after);
    }
  }

  @Test
  void addingARepeatedFieldIsAcceptedRegardlessOfTopLevelMessageLayout() {
    // This was a known limitation, and it is the regression test for its fix.
    //
    // The derived empty-list default used to be readable only through the path-keyed defaults map,
    // whose Protobuf keys are prefixed by the referenced message's DECLARATION INDEX in the file
    // rather than by the referencing field's position. So the identical evolution was accepted or
    // rejected depending on nothing but where the messages were declared -- here 'inner' sits at
    // position 0 while Inner's body is keyed under [1], the lookup missed, and the relaxation did
    // not fire. Declaring Inner nested inside M made the two numbers coincide and it worked.
    //
    // The reader now records that default on the Schema.Field as well, where it has no addressing
    // problem, so the layout of the .proto file cannot change the verdict.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { Inner inner = 1; }\n"
            + "message Inner { string a = 1; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { Inner inner = 1; }\n"
            + "message Inner { string a = 1; repeated string tags = 2; }\n");

    assertCompatible(Mode.ICEBERG_V2, before, after);
    assertCompatible(Mode.ICEBERG_V3, before, after);
    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingAProtoScalarIsRejectedEvenThoughProto3GivesItAnImplicitZero() {
    // A deliberate policy choice, not an oversight.
    //
    // A proto3 singular scalar has implicit presence, so it derives as NOT NULL, and the converter
    // does record the wire-level zero in the path-keyed defaults map. The checker does not read
    // that map -- it reads Schema.Field, and the reader deliberately does not put implicit scalar
    // zeros there.
    //
    // The reason is what the value means at the TARGET rather than at the source. An absent
    // container and an empty one are query-equivalent, so relaxing a container costs nothing. An
    // absent scalar is not equivalent to zero: Iceberg v2 cannot store a column default, so an old
    // data file lacking the column yields null, and a NOT NULL INT column reading null is broken
    // in a way an ARRAY reading empty is not.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; int32 count = 2; }\n");

    for (Mode mode : new Mode[] {Mode.FLINK, Mode.ICEBERG_V2, Mode.ICEBERG_V3}) {
      assertSingleAt(mode, before, after, Rule.REQUIRED_FIELD_ADDED, "count");
    }
  }

  @Test
  void widenedProtoIntegerIsAcceptedAndNarrowedIsNot() {
    LogicalType asInt32 = fromProto(
        "syntax = \"proto3\";\npackage t;\nmessage M { int32 n = 1; }\n");
    LogicalType asInt64 = fromProto(
        "syntax = \"proto3\";\npackage t;\nmessage M { int64 n = 1; }\n");

    assertCompatible(Mode.ICEBERG_V2, asInt32, asInt64);
    assertCompatible(Mode.FLINK, asInt32, asInt64);
    assertSingle(Mode.ICEBERG_V2, asInt64, asInt32, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertSingle(Mode.FLINK, asInt64, asInt32, Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void reorderingProtoFieldDeclarationsIsRejectedByIcebergOnly() {
    // Tag numbers are untouched, so the format layer is indifferent, and Flink identifies columns by
    // name -- so only Iceberg, which needs a field ID to reposition a column, objects.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\npackage t;\nmessage M { string a = 1; string b = 2; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\npackage t;\nmessage M { string b = 2; string a = 1; }\n");

    assertCompatible(Mode.FLINK, before, after);
    assertSingle(Mode.ICEBERG_V2, before, after, Rule.FIELD_REORDERED);
  }

  // ---------------------------------------------------------------------------------------------
  // Avro -- a declared default inside a nested record, and the Iceberg format-version split
  // ---------------------------------------------------------------------------------------------

  @Test
  void addingADefaultedNonNullFieldToANestedRecordSplitsOnIcebergFormatVersion() {
    // What this pins is the version rule, not the derivation: Avro populates BOTH default channels
    // for a declared default -- Schema.Field and the path-keyed map -- and hasNonNullDefault
    // short-circuits on the field, so the index path is never consulted here. The path-keyed
    // channel reaching through a nested type is covered instead by
    // addingARepeatedFieldInsideANestedProtoMessageIsAccepted, where the default is derived and the
    // field carries nothing.
    //
    // The rule, from isEffectivelyOptional: a newly added non-null field counts as optional only
    // when the format version can store an initial-default, which makes the column readable for
    // rows written before it existed. v3 can, v2 cannot.
    LogicalType before = nestedRecord("");
    LogicalType after = nestedRecord(",{\"name\":\"added\",\"type\":\"int\",\"default\":7}");

    assertCompatible(Mode.ICEBERG_V3, before, after);
    assertSingleAt(Mode.ICEBERG_V2, before, after, Rule.REQUIRED_FIELD_ADDED, "p.added");
    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingAnUndefaultedNonNullFieldToANestedRecordIsRejectedByEveryMode() {
    // The control for the case above. Without the default v3 has nothing to store, so its
    // acceptance there is attributable to the default rather than to v3 simply being laxer about
    // added fields.
    LogicalType before = nestedRecord("");
    LogicalType after = nestedRecord(",{\"name\":\"added\",\"type\":\"int\"}");

    for (Mode mode : new Mode[] {Mode.FLINK, Mode.ICEBERG_V2, Mode.ICEBERG_V3}) {
      assertSingleAt(mode, before, after, Rule.REQUIRED_FIELD_ADDED, "p.added");
    }
  }

  /** {@code Root { p: Inner { x: int<extraInnerFields> } }}, as Avro schema text. */
  private static LogicalType nestedRecord(String extraInnerFields) {
    return fromAvro("{\"type\":\"record\",\"name\":\"Root\",\"fields\":["
        + "{\"name\":\"p\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "{\"name\":\"x\",\"type\":\"int\"}" + extraInnerFields + "]}}]}");
  }

  // ---------------------------------------------------------------------------------------------
  // Avro -- annotations that change the derived type
  // ---------------------------------------------------------------------------------------------

  @Test
  void annotatingAnAvroLongAsATimestampChangesTheDerivedType() {
    // The wire bytes are identical and Avro resolution is indifferent, but the derived type is not.
    LogicalType asLong = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"ts\",\"type\":\"long\"}]}");
    LogicalType asTimestamp = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}]}");

    assertSingle(Mode.FLINK, asLong, asTimestamp, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertSingle(Mode.ICEBERG_V2, asLong, asTimestamp, Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void addingAnAvroFieldWithADeclaredDefaultIsAccepted() {
    LogicalType before = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"}]}");
    LogicalType after = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"},"
            + "{\"name\":\"b\",\"type\":\"string\",\"default\":\"x\"}]}");

    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingARequiredAvroFieldWithNoDefaultIsRejected() {
    // Contrast with the proto cases: Avro has no implicit default for a missing field, so nothing
    // rescues this one.
    LogicalType before = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"}]}");
    LogicalType after = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"},"
            + "{\"name\":\"b\",\"type\":\"string\"}]}");

    assertSingle(Mode.FLINK, before, after, Rule.REQUIRED_FIELD_ADDED);
    assertSingle(Mode.ICEBERG_V2, before, after, Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void addingAnAvroArrayWithoutADefaultIsRejectedUnlikeProto() {
    // An absent Avro array is not defined to be empty, so the container relaxation has nothing to
    // find. This is the derivation difference between the two formats, made explicit.
    LogicalType before = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"}]}");
    LogicalType after = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");

    assertSingle(Mode.ICEBERG_V2, before, after, Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void makingAnAvroFieldNullableIsAcceptedAndTheReverseIsNot() {
    LogicalType required = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"}]}");
    LogicalType nullable = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    assertCompatible(Mode.FLINK, required, nullable);
    assertCompatible(Mode.ICEBERG_V2, required, nullable);
    assertSingle(Mode.FLINK, nullable, required, Rule.NULLABLE_TO_NON_NULLABLE);
    assertSingle(Mode.ICEBERG_V2, nullable, required, Rule.NULLABLE_TO_NON_NULLABLE);
  }

  @Test
  void addingAnAvroEnumSymbolDoesNotChangeTheDerivedType() {
    // Enums derive to an unbounded VARCHAR, so symbol changes are invisible to both modes -- even
    // though Avro's own resolution would reject reading an unknown symbol.
    LogicalType before = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"e\",\"type\":"
            + "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\"]}}]}");
    LogicalType after = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"e\",\"type\":"
            + "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}}]}");

    assertCompatible(Mode.FLINK, before, after);
    assertCompatible(Mode.ICEBERG_V2, before, after);
  }

  // ---------------------------------------------------------------------------------------------
  // JSON Schema
  // ---------------------------------------------------------------------------------------------

  @Test
  void anUnrelatedDefsDefaultDoesNotExcuseARequiredPropertyAddedAtTheRoot() {
    // Regression test for a false ACCEPT -- the dangerous direction.
    //
    // The JSON reader walks a $defs body with an empty index path, so that body's defaults land in
    // the ROOT's key space: Inner.b's default was recorded as [1], indistinguishable from root
    // property #1. A required, undefaulted property added at that position was then rescued by a
    // default belonging to an entirely different type, and a breaking change was accepted.
    //
    // The checker no longer reads the path-keyed map, so no such collision is possible. The
    // converter defect itself is still there and is worth fixing on its own account -- it also
    // reaches Flink, which does read that map.
    LogicalType before = jsonWithDefs(false);
    LogicalType after = jsonWithDefs(true);

    assertSingleAt(Mode.FLINK, before, after, Rule.REQUIRED_FIELD_ADDED, "mustHave");
    assertSingleAt(Mode.ICEBERG_V2, before, after, Rule.REQUIRED_FIELD_ADDED, "mustHave");
  }

  /** Root with a $defs body carrying a default, optionally gaining a required scalar property. */
  private static LogicalType jsonWithDefs(boolean withRequiredAddition) {
    return fromJson("{\"type\":\"object\",\"properties\":{"
        + "\"pad\":{\"type\":\"string\"}"
        + (withRequiredAddition ? ",\"mustHave\":{\"type\":\"integer\"}" : "")
        + ",\"inner\":{\"$ref\":\"#/$defs/Inner\"}},"
        + (withRequiredAddition
            ? "\"required\":[\"pad\",\"mustHave\"],"
            : "\"required\":[\"pad\"],")
        + "\"$defs\":{\"Inner\":{\"type\":\"object\",\"properties\":{"
        + "\"a\":{\"type\":\"string\"},"
        + "\"b\":{\"type\":\"integer\",\"default\":7}}}}}");
  }

  @Test
  void addingAnOptionalJsonPropertyIsAccepted() {
    LogicalType before = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"connect.index\":0}},\"required\":[\"a\"]}");
    LogicalType after = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"connect.index\":0},"
            + "\"b\":{\"type\":\"string\",\"connect.index\":1}},\"required\":[\"a\"]}");

    assertCompatible(Mode.FLINK, before, after);
    assertCompatible(Mode.ICEBERG_V2, before, after);
  }

  @Test
  void addingARequiredJsonPropertyIsRejected() {
    LogicalType before = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"connect.index\":0}},\"required\":[\"a\"]}");
    LogicalType after = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"connect.index\":0},"
            + "\"b\":{\"type\":\"string\",\"connect.index\":1}},\"required\":[\"a\",\"b\"]}");

    assertSingle(Mode.FLINK, before, after, Rule.REQUIRED_FIELD_ADDED);
    assertSingle(Mode.ICEBERG_V2, before, after, Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void relaxingAJsonIntegerToANumberChangesTheDerivedType() {
    // "integer" derives to BIGINT and "number" to DOUBLE. A JSON-Schema widening, but not a type
    // widening either mode accepts.
    LogicalType asInteger = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"n\":{\"type\":\"integer\",\"connect.index\":0}},\"required\":[\"n\"]}");
    LogicalType asNumber = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"n\":{\"type\":\"number\",\"connect.index\":0}},\"required\":[\"n\"]}");

    assertSingle(Mode.FLINK, asInteger, asNumber, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertSingle(Mode.ICEBERG_V2, asInteger, asNumber, Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  // ---------------------------------------------------------------------------------------------
  // Editions -- both schemas must be derived by the same one
  // ---------------------------------------------------------------------------------------------

  /**
   * {@code LogicalTypeVersion} selects the derivation edition on the way <em>in</em> as well as out.
   * V1 mirrors what the Flink converters would produce; V2 is the canonical logical-types reading.
   * For unions the two differ in ways that are invisible to a reader of the JSON but not to this
   * checker, so a comparison must be given two schemas derived under the same edition.
   */
  private static LogicalType fromJson(String jsonText, LogicalTypeVersion edition) {
    return JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(jsonText), edition);
  }

  private static final String TITLED_UNION =
      "{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":["
          + "{\"type\":\"string\",\"title\":\"AsText\"},"
          + "{\"type\":\"integer\",\"title\":\"AsNum\"}]}}}";

  @Test
  void aUnionIsUnchangedWithinEitherEdition() {
    // The point of reference: comparing like with like finds nothing, in both editions.
    assertCompatible(Mode.FLINK,
        fromJson(TITLED_UNION, LogicalTypeVersion.V1),
        fromJson(TITLED_UNION, LogicalTypeVersion.V1));
    assertCompatible(Mode.FLINK,
        fromJson(TITLED_UNION, LogicalTypeVersion.V2),
        fromJson(TITLED_UNION, LogicalTypeVersion.V2));
  }

  @Test
  void crossingEditionsReportsSpuriousUnionBranchRenames() {
    // KNOWN HAZARD, pinned so it is not discovered in production. V1 always synthesizes
    // connect_union_field_<index> as a branch name -- the Flink converters ignored titles, and
    // honouring them under V1 would rename union columns. V2 prefers a subschema's title. Branches
    // are matched by name, so the identical schema read under the two editions looks like every
    // branch was renamed, which is a drop plus an add.
    //
    // The lesson is a precondition rather than a bug: both sides of a comparison must be derived
    // under the same edition. Nothing in the signature enforces that today.
    // Under ICEBERG_V2, where drops and reorders are still findings. Mode.FLINK no longer reports
    // either, so the edition mismatch is invisible there -- which does not make it safe, only
    // silent: a renamed union column still means the data lands somewhere else.
    Set<Rule> rules = rulesOf(Mode.ICEBERG_V2,
        fromJson(TITLED_UNION, LogicalTypeVersion.V1),
        fromJson(TITLED_UNION, LogicalTypeVersion.V2));
    assertTrue(rules.contains(Rule.FIELD_DELETED), "expected spurious drops, got " + rules);

    assertCompatible(Mode.FLINK,
        fromJson(TITLED_UNION, LogicalTypeVersion.V1),
        fromJson(TITLED_UNION, LogicalTypeVersion.V2));
  }

  @Test
  void crossingEditionsCanAlsoChangeTheStructuralKind() {
    // A single-member oneOf with no null collapses to the member type under V2 but stays a union
    // under V1, so crossing editions changes the column's kind outright.
    String singleton =
        "{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":[{\"type\":\"string\"}]}}}";
    Set<Rule> rules = rulesOf(Mode.FLINK,
        fromJson(singleton, LogicalTypeVersion.V1),
        fromJson(singleton, LogicalTypeVersion.V2));
    assertTrue(rules.contains(Rule.TYPE_MISMATCH), "expected a kind change, got " + rules);
  }

  @Test
  void addingAUnionBranchIsAcceptedUnderEitherEdition() {
    // The verdict that prompted this section: adding a branch is an added nullable column, and that
    // is edition-independent because both editions keep a first-class UNION. Only the SRLT-to-Flink
    // lowering turns a union into a struct, and it marks the branches nullable.
    String twoBranch = "{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":["
        + "{\"type\":\"string\"},{\"type\":\"integer\"}]}}}";
    String threeBranch = "{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":["
        + "{\"type\":\"string\"},{\"type\":\"integer\"},{\"type\":\"boolean\"}]}}}";
    for (LogicalTypeVersion edition : LogicalTypeVersion.values()) {
      assertCompatible(Mode.FLINK, fromJson(twoBranch, edition), fromJson(threeBranch, edition));
      assertCompatible(Mode.ICEBERG_V2, fromJson(twoBranch, edition), fromJson(threeBranch, edition));
    }
  }

  @Test
  void insertingAJsonPropertyWithoutAConnectIndexShiftsColumnOrder() {
    // Unindexed properties sort alphabetically, so inserting "b" between "a" and "c" repositions
    // "c". The property set is otherwise unchanged, which is what makes this trap worth pinning.
    LogicalType before = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\"},\"c\":{\"type\":\"string\"}}}");
    LogicalType after = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"},"
            + "\"c\":{\"type\":\"string\"}}}");

    // "b" lands in the middle, so it is an addition rather than a reorder -- and being optional it
    // is accepted. Pinned to document that alphabetical ordering makes middle insertion the norm.
    assertCompatible(Mode.FLINK, before, after);
  }

  // -----------------------------------------------------------------------------------------------
  // Enum symbol removal, from real schema text -- no longer a finding in either mode
  // -----------------------------------------------------------------------------------------------

  @Test
  void anAvroEnumSymbolDropIsNotAFindingInEitherMode() {
    // An ENUM derives to VARCHAR for Flink and to string for Iceberg, so the symbol set is not part
    // of either type. The hazard is real -- a reader resolves a dropped symbol to the enum default --
    // but it belongs to the encoding, so the format-level checker owns it.
    LogicalType before = fromAvro(enumRecord("[\"A\",\"B\",\"C\"]", ""));
    LogicalType after = fromAvro(enumRecord("[\"A\",\"B\"]", ""));

    assertCompatible(Mode.FLINK, before, after);
    assertCompatible(Mode.ICEBERG_V2, before, after);
  }

  private static String enumRecord(String symbols, String extra) {
    return "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"e\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":" + symbols + extra + "}}]}";
  }
}
