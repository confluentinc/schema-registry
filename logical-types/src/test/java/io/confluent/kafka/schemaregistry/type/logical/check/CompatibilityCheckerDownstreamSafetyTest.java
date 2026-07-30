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

package io.confluent.kafka.schemaregistry.type.logical.check;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.check.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.check.Incompatibility.Rule;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One case per schema change known to break a downstream consumer, driven from real schema text.
 *
 * <p>This is a ledger rather than a specification: it records what the checker <em>does</em> for each
 * hazard, so a change in verdict is visible in review. Some verdicts here are deliberately more or
 * less permissive than what a given downstream engine does today; those say so, and why. What an
 * engine currently tolerates is not always the same question as whether a schema change is safe.
 *
 * <p>Hazards this checker cannot see at all are recorded too, with the reason. Those are not gaps in
 * the composed gate: a schema change must pass the format-level check as well, and that is where an
 * erased distinction is caught.
 *
 * <p>Single-schema hazards — cyclic schemas, empty structs, open content, schemaless payloads,
 * illegal field names — are properties of one schema rather than of a pair, so they belong to a
 * validity check and are out of scope here.
 */
class CompatibilityCheckerDownstreamSafetyTest {

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static LogicalType avro(String text) {
    return AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(text));
  }

  private static LogicalType proto(String text) {
    return ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(text));
  }

  private static LogicalType json(String text) {
    return JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(text));
  }

  /** An Avro record with the given field declarations. */
  private static LogicalType rec(String fields) {
    return avro("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + fields + "]}");
  }

  private static String fld(String name, String type) {
    return "{\"name\":\"" + name + "\",\"type\":" + type + "}";
  }

  private static Set<Rule> rulesOf(Mode mode, LogicalType original, LogicalType update) {
    CompatibilityResult result = CompatibilityChecker.compare(mode, original, update);
    return result.getIncompatibilities().isEmpty()
        ? EnumSet.noneOf(Rule.class)
        : result.getIncompatibilities().stream()
            .map(Incompatibility::getRule)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(Rule.class)));
  }

  private static void assertBlocked(Mode mode, LogicalType original, LogicalType update, Rule rule) {
    Set<Rule> rules = rulesOf(mode, original, update);
    assertTrue(rules.contains(rule), mode + " expected " + rule + " but got " + rules);
  }

  private static void assertBlockedByBoth(LogicalType original, LogicalType update, Rule rule) {
    assertBlocked(Mode.FLINK, original, update, rule);
    assertBlocked(Mode.ICEBERG_V2, original, update, rule);
  }

  private static void assertAllowed(Mode mode, LogicalType original, LogicalType update) {
    assertEquals(EnumSet.noneOf(Rule.class), rulesOf(mode, original, update),
        mode + " expected no findings");
  }

  private static void assertAllowedByBoth(LogicalType original, LogicalType update) {
    assertAllowed(Mode.FLINK, original, update);
    assertAllowed(Mode.ICEBERG_V2, original, update);
  }

  // ---------------------------------------------------------------------------------------------
  // Structural evolution hazards
  // ---------------------------------------------------------------------------------------------

  @Test
  void addingRequiredFields() {
    // Blocked in both modes when the field has no default.
    assertBlockedByBoth(rec(fld("a", "\"string\"")),
        rec(fld("a", "\"string\"") + "," + fld("b", "\"string\"")),
        Rule.REQUIRED_FIELD_ADDED);

    // An added scalar WITH a declared default splits the two modes. Flink can store a NOT NULL
    // column carrying a
    // default, so it accepts. Iceberg v2 cannot persist a column default at all -- initial-default
    // arrived in v3 -- so the only v2-safe shape is an optional column, and a defaulted scalar is
    // still refused.
    LogicalType withDefault = rec(fld("a", "\"string\"")
        + ",{\"name\":\"b\",\"type\":\"string\",\"default\":\"x\"}");
    assertAllowed(Mode.FLINK, rec(fld("a", "\"string\"")), withDefault);
    assertBlocked(Mode.ICEBERG_V2, rec(fld("a", "\"string\"")), withDefault,
        Rule.REQUIRED_FIELD_ADDED);

    // An added OPTIONAL field is accepted by both: that is the shape the Iceberg spec permits.
    assertAllowedByBoth(rec(fld("a", "\"string\"")),
        rec(fld("a", "\"string\"")
            + ",{\"name\":\"b\",\"type\":[\"null\",\"string\"],\"default\":null}"));
  }

  @Test
  void optionalToRequiredConversions() {
    LogicalType optional = rec(
        "{\"name\":\"a\",\"type\":[\"null\",\"string\"],\"default\":null}");
    LogicalType required = rec(fld("a", "\"string\""));
    assertBlockedByBoth(optional, required, Rule.NULLABLE_TO_NON_NULLABLE);
  }

  @Test
  void fieldRenamesWithoutAliases() {
    assertBlockedByBoth(rec(fld("name", "\"string\"")), rec(fld("banana", "\"string\"")),
        Rule.FIELD_DELETED);
  }

  @Test
  void fieldRenamesWithAnAliasAreAlsoBlocked() {
    // KNOWN GAP. A rename that carries an alias arguably ought to be permitted, since the alias is
    // exactly the assertion needed to preserve identity. Aliases are not implemented: an alias asserts
    // that two fields are the same field, and nothing here reads one, so the rename still reads as a
    // drop plus an add. A known gap rather than a considered verdict.
    LogicalType before = rec(fld("name", "\"string\""));
    LogicalType after = rec("{\"name\":\"banana\",\"type\":\"string\",\"aliases\":[\"name\"]}");
    assertBlockedByBoth(before, after, Rule.FIELD_DELETED);
  }

  @Test
  void fieldRemoval() {
    assertBlockedByBoth(rec(fld("a", "\"string\"") + "," + fld("b", "\"string\"")),
        rec(fld("a", "\"string\"")), Rule.FIELD_DELETED);
  }

  @Test
  void fieldReordering() {
    assertBlockedByBoth(rec(fld("a", "\"string\"") + "," + fld("b", "\"string\"")),
        rec(fld("b", "\"string\"") + "," + fld("a", "\"string\"")), Rule.FIELD_REORDERED);
  }

  @Test
  void jsonConstraintAdditions() {
    // No longer reaches either checker: the converter now rejects if/then/else outright, because a
    // property required only by a conditional branch gets no column and its values would be
    // silently dropped. Caught one layer earlier than a comparison, and so caught on a first
    // registration too.
    assertThatThrownBy(() -> json("{\"type\":\"object\",\"properties\":{"
        + "\"a\":{\"type\":\"string\"}},"
        + "\"if\":{\"required\":[\"a\"]},\"then\":{\"required\":[\"a\"]}}"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("if/then/else");

    // Value-level constraints carry no such cost and are still ignored, as intended.
    assertAllowedByBoth(
        json("{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}"),
        json("{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"minLength\":3,\"pattern\":\"^x\"}}}"));
  }

  @Test
  void protobufSameTagRenames() {
    assertBlockedByBoth(
        proto("syntax=\"proto3\";package t;message M{string name=1;}"),
        proto("syntax=\"proto3\";package t;message M{string banana=1;}"),
        Rule.FIELD_DELETED);
  }

  @Test
  void numericNarrowing() {
    assertBlockedByBoth(rec(fld("n", "\"long\"")), rec(fld("n", "\"int\"")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertBlockedByBoth(rec(fld("n", "\"double\"")), rec(fld("n", "\"float\"")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertBlockedByBoth(
        json("{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"number\"}}}"),
        json("{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"integer\"}}}"),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void numericCrossFamilyChanges() {
    // Iceberg blocks all of these; it has no integer-to-floating-point promotion.
    // Flink allows only the exact ones. INT to DOUBLE is exact (a 32-bit integer fits a 53-bit
    // significand) and is allowed. INT to FLOAT and BIGINT to DOUBLE are refused: Flink's own table
    // admits them, and they are often assumed safe, but both round silently above 2^24 and 2^53.
    assertAllowed(Mode.FLINK, rec(fld("n", "\"int\"")), rec(fld("n", "\"double\"")));
    assertBlocked(Mode.ICEBERG_V2, rec(fld("n", "\"int\"")), rec(fld("n", "\"double\"")),
        Rule.UNSUPPORTED_TYPE_CHANGE);

    assertBlockedByBoth(rec(fld("n", "\"int\"")), rec(fld("n", "\"float\"")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertBlockedByBoth(rec(fld("n", "\"long\"")), rec(fld("n", "\"double\"")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void typeCrossFamilySwaps() {
    assertBlockedByBoth(rec(fld("s", "\"string\"")), rec(fld("s", "\"bytes\"")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertBlockedByBoth(rec(fld("s", "\"int\"")), rec(fld("s", "\"string\"")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertBlockedByBoth(rec(fld("s", "\"int\"")), rec(fld("s", "\"boolean\"")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void avroFixedSizeChanges() {
    // Both directions, in both modes. A fixed size is the stored width rather than a bound, so
    // widening rewrites every historical value; Avro resolution also requires an identical size.
    String f16 = "{\"type\":\"fixed\",\"name\":\"F\",\"size\":16}";
    String f32 = "{\"type\":\"fixed\",\"name\":\"F\",\"size\":32}";
    assertBlockedByBoth(rec(fld("b", f16)), rec(fld("b", f32)), Rule.UNSUPPORTED_TYPE_CHANGE);
    assertBlockedByBoth(rec(fld("b", f32)), rec(fld("b", f16)), Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void cardinalityNarrowing() {
    assertBlockedByBoth(rec(fld("c", "{\"type\":\"array\",\"items\":\"string\"}")),
        rec(fld("c", "\"string\"")), Rule.TYPE_MISMATCH);
  }

  @Test
  void unionShapeChanges() {
    // Crossing the one-versus-many boundary changes the column from a nullable scalar to a struct of
    // branches, and is blocked.
    assertBlockedByBoth(rec(fld("u", "[\"null\",\"string\"]")),
        rec(fld("u", "[\"null\",\"string\",\"int\"]")), Rule.TYPE_MISMATCH);
    // Removing a branch from an already-multi-branch union drops a column.
    assertBlockedByBoth(rec(fld("u", "[\"null\",\"string\",\"int\"]")),
        rec(fld("u", "[\"null\",\"string\"]")), Rule.TYPE_MISMATCH);
  }

  @Test
  void addingABranchToAnAlreadyMultiBranchUnionIsAllowed() {
    // Sometimes assumed to be breaking, but under BACKWARD it is not: the new union has an extra
    // branch, historical records only ever used the original ones, and the reader resolves them all,
    // so the derived type simply gains a nullable column. The breaking case is FORWARD, or a
    // materializer that cannot apply the change in place.
    assertAllowedByBoth(rec(fld("u", "[\"null\",\"string\",\"int\"]")),
        rec(fld("u", "[\"null\",\"string\",\"int\",\"boolean\"]")));
  }

  @Test
  void avroUnionBranchReordering() {
    // Reordering two or more non-null branches reads as a struct-field reorder.
    assertBlockedByBoth(rec(fld("u", "[\"null\",\"string\",\"int\"]")),
        rec(fld("u", "[\"null\",\"int\",\"string\"]")), Rule.FIELD_REORDERED);
    // Flipping only the null branch is a no-op: a two-member union containing a null collapses to a
    // nullable type regardless of branch order.
    assertAllowedByBoth(rec(fld("u", "[\"null\",\"string\"]")),
        rec(fld("u", "[\"string\",\"null\"]")));
  }

  @Test
  void protobufReservedTagAdditions() {
    assertBlockedByBoth(
        proto("syntax=\"proto3\";package t;message M{string a=1;string b=2;}"),
        proto("syntax=\"proto3\";package t;message M{string a=1;reserved 2;}"),
        Rule.FIELD_DELETED);
  }

  // ---------------------------------------------------------------------------------------------
  // Value evolution hazards
  // ---------------------------------------------------------------------------------------------

  @Test
  void avroLogicalTypeChanges() {
    assertBlockedByBoth(rec(fld("ts", "\"long\"")),
        rec(fld("ts", "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void avroTimestampPrecisionShifts() {
    // Blocked in Flink mode: the annotation selects the unit of the stored integer, so the same long
    // is read a thousandfold out. Allowed in Iceberg mode, correctly -- Iceberg erases temporal
    // precision and the materializer converts each record under its own writer schema.
    LogicalType millis = rec(fld("ts", "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"));
    LogicalType micros = rec(fld("ts", "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"));
    assertBlocked(Mode.FLINK, millis, micros, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertAllowed(Mode.ICEBERG_V2, millis, micros);
  }

  @Test
  void avroDecimalScaleChanges() {
    String dec = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":";
    assertBlockedByBoth(rec(fld("d", dec + "2}")), rec(fld("d", dec + "4}")),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void avroTimezoneFlips() {
    // Blocked in both modes. The two annotations share a representation but not a reference frame.
    LogicalType instant = rec(fld("ts", "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"));
    LogicalType local = rec(
        fld("ts", "{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}"));
    assertBlockedByBoth(instant, local, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertBlockedByBoth(local, instant, Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void avroEnumValueDropsAreBlockedForFlinkOnly() {
    // Invisible to the *type* comparison -- an enum derives to an unbounded VARCHAR on both sides --
    // so it is caught by the one value-level rule instead. Flink re-resolves historical symbols
    // against the new set and renders a dropped one as the enum's default; Iceberg stores the string
    // already committed, so its column is unaffected.
    LogicalType before =
        rec(fld("e", "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}"));
    LogicalType after =
        rec(fld("e", "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\"]}"));
    assertBlocked(Mode.FLINK, before, after, Rule.ENUM_SYMBOL_REMOVED);
    assertAllowed(Mode.ICEBERG_V2, before, after);
  }

  @Test
  void protobufSameTagScalarDrift() {
    // A cross-family drift on the same tag is blocked.
    assertBlockedByBoth(
        proto("syntax=\"proto3\";package t;message M{int32 v=1;}"),
        proto("syntax=\"proto3\";package t;message M{string v=1;}"),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    // A widening on the same tag is allowed: int32 and int64 share a varint encoding, so the value
    // survives and the derived column merely widens.
    assertAllowedByBoth(
        proto("syntax=\"proto3\";package t;message M{int32 v=1;}"),
        proto("syntax=\"proto3\";package t;message M{int64 v=1;}"));
  }

  @Test
  void protobufEnumValueRemovalIsBlockedForFlinkOnly() {
    // Same reasoning as the Avro case; one rule covers both formats because both derive an ENUM.
    LogicalType before = proto("syntax=\"proto3\";package t;message M{E e=1;}"
        + "enum E{UNSET=0;A=1;B=2;}");
    LogicalType after = proto("syntax=\"proto3\";package t;message M{E e=1;}"
        + "enum E{UNSET=0;A=1;}");
    assertBlocked(Mode.FLINK, before, after, Rule.ENUM_SYMBOL_REMOVED);
    assertAllowed(Mode.ICEBERG_V2, before, after);
  }
}
