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

import io.confluent.kafka.schemaregistry.type.logical.CompatibilityChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.Incompatibility.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Where {@link Mode#ICEBERG_V3} differs from {@link Mode#ICEBERG_V2}.
 *
 * <p>Every case here asserts <em>both</em> verdicts, because the value of the split is the delta. Only
 * three things move, and they move in two directions:
 *
 * <ul>
 *   <li><b>Column defaults.</b> v3 adds {@code initial-default} and {@code write-default}, so a newly
 *       added required field becomes legal when it carries a non-null default. This is the change that
 *       matters — it is the single most requested evolution.
 *   <li><b>The promotion table.</b> v3 adds {@code date} to the without-timezone timestamps. It also
 *       adds {@code unknown} to any type, which is unreachable here: no {@link Schema.Type} maps onto
 *       {@code unknown}.
 *   <li><b>Representable types.</b> The nanosecond timestamps and {@code variant} arrive in v3, which
 *       makes v2 <em>stricter</em> than it used to be here — those types were previously waved through
 *       on the grounds that the target table's version was unknown.
 * </ul>
 *
 * <p>Everything else is version-independent: drops, renames, reordering, nullability tightening, and
 * the rest of the promotion table are all governed by field identity or by value representation, and
 * neither changed in v3.
 */
class CompatibilityCheckerIcebergV3Test {

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static Schema nonNull(Schema schema) {
    return schema.setNullable(false);
  }

  private static Field required(String name, Schema type) {
    return new Field(name, nonNull(type), 0);
  }

  private static Field optional(String name, Schema type) {
    return new Field(name, type.setNullable(true), 0);
  }

  private static Field requiredWithDefault(String name, Schema type, Object dflt) {
    return new Field(name, nonNull(type), 0, dflt, true, null, null, null);
  }

  private static LogicalType schema(Field... fields) {
    return new LogicalType(nonNull(Schema.createStruct(Arrays.asList(fields))));
  }

  private static LogicalType col(Schema type) {
    return schema(required("c", type));
  }

  private static Set<Rule> rulesOf(Mode mode, LogicalType original, LogicalType update) {
    CompatibilityResult result = CompatibilityChecker.compare(mode, original, update);
    return result.getIncompatibilities().isEmpty()
        ? EnumSet.noneOf(Rule.class)
        : result.getIncompatibilities().stream()
            .map(Incompatibility::getRule)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(Rule.class)));
  }

  /** Asserts the verdict differs between the two versions, in the stated direction. */
  private static void assertV2RejectsAndV3Accepts(
      LogicalType original, LogicalType update, Rule v2Rule) {
    assertEquals(EnumSet.of(v2Rule), rulesOf(Mode.ICEBERG_V2, original, update),
        "v2 should reject");
    assertEquals(EnumSet.noneOf(Rule.class), rulesOf(Mode.ICEBERG_V3, original, update),
        "v3 should accept");
  }

  private static void assertBothReject(LogicalType original, LogicalType update, Rule rule) {
    assertTrue(rulesOf(Mode.ICEBERG_V2, original, update).contains(rule), "v2");
    assertTrue(rulesOf(Mode.ICEBERG_V3, original, update).contains(rule), "v3");
  }

  private static void assertBothAccept(LogicalType original, LogicalType update) {
    assertEquals(EnumSet.noneOf(Rule.class), rulesOf(Mode.ICEBERG_V2, original, update), "v2");
    assertEquals(EnumSet.noneOf(Rule.class), rulesOf(Mode.ICEBERG_V3, original, update), "v3");
  }

  private static Schema type(Schema.Type t) {
    return Schema.create(t);
  }

  private static final LogicalType ONE_COLUMN = schema(required("id", type(Schema.Type.INT)));

  // ---------------------------------------------------------------------------------------------
  // Column defaults -- the relaxation
  // ---------------------------------------------------------------------------------------------

  @Test
  void addingARequiredScalarWithANonNullDefaultBecomesLegal() {
    // The headline change. v2 cannot persist a column default at all, so the only readable shape is an
    // optional column; v3 stores initial-default and the field can stay required.
    assertV2RejectsAndV3Accepts(ONE_COLUMN,
        schema(required("id", type(Schema.Type.INT)),
            requiredWithDefault("count", type(Schema.Type.INT), 0)),
        Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void addingARequiredStringWithANonNullDefaultBecomesLegal() {
    assertV2RejectsAndV3Accepts(ONE_COLUMN,
        schema(required("id", type(Schema.Type.INT)),
            requiredWithDefault("name", Schema.createString(), "x")),
        Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void addingARequiredFieldWithNoDefaultIsStillRejected() {
    // v3 requires both defaults to be set to a non-null value when a required field is added. Nothing
    // to store here, so nothing changes.
    assertBothReject(ONE_COLUMN,
        schema(required("id", type(Schema.Type.INT)), required("name", Schema.createString())),
        Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void addingARequiredFieldWhoseDefaultIsNullIsStillRejected() {
    // Presence of a default is not enough: a null default leaves pre-existing rows with nothing to
    // read, which is why the spec demands a non-null value.
    Field nullDefaulted =
        new Field("name", nonNull(Schema.createString()), 0, null, true, null, null, null);
    assertBothReject(ONE_COLUMN,
        schema(required("id", type(Schema.Type.INT)), nullDefaulted),
        Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void addingARequiredVariantIsRejectedEvenWithADefault() {
    // The spec forbids a non-null default on unknown, variant, geometry and geography, so this one
    // cannot be rescued at any version. Of those four only VARIANT is expressible here.
    assertBothReject(ONE_COLUMN,
        schema(required("id", type(Schema.Type.INT)),
            requiredWithDefault("v", type(Schema.Type.VARIANT), "{}")),
        Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void tighteningAnExistingOptionalScalarIsRejectedAtBothVersions() {
    // initial-default "is set only when a field is added to an existing schema", so it cannot be
    // attached to an existing column retroactively. The relaxation applies to additions only.
    assertBothReject(
        schema(optional("name", Schema.createString())),
        schema(requiredWithDefault("name", Schema.createString(), "x")),
        Rule.NULLABLE_TO_NON_NULLABLE);
  }

  @Test
  void addingAnOptionalFieldIsUnchanged() {
    assertBothAccept(ONE_COLUMN,
        schema(required("id", type(Schema.Type.INT)),
            optional("name", Schema.createString())));
  }

  @Test
  void theContainerRelaxationSurvivesIntoV3() {
    // It addresses a derivation quirk rather than an Iceberg capability: proto and Avro mark
    // containers NOT NULL because those formats cannot encode a null container. v3 does not change
    // that, so the relaxation is not retired by the upgrade.
    assertBothAccept(ONE_COLUMN,
        schema(required("id", type(Schema.Type.INT)),
            requiredWithDefault("tags", Schema.createArray(nonNull(Schema.createString())),
                Collections.emptyList())));
  }

  // ---------------------------------------------------------------------------------------------
  // The promotion table
  // ---------------------------------------------------------------------------------------------

  @Test
  void dateToTimestampBecomesAValidPromotion() {
    assertV2RejectsAndV3Accepts(col(type(Schema.Type.DATE)), col(Schema.createTimestamp(6)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void dateToTheNanosecondTimestampBecomesAValidPromotion() {
    // v2 has no date-to-timestamp_ns row in its promotion table; v3 adds one. That v2 additionally
    // cannot store timestamp_ns is ValidityChecker's finding, not this one's.
    assertEquals(EnumSet.of(Rule.UNSUPPORTED_TYPE_CHANGE),
        rulesOf(Mode.ICEBERG_V2, col(type(Schema.Type.DATE)), col(Schema.createTimestamp(9))));
    assertEquals(EnumSet.noneOf(Rule.class),
        rulesOf(Mode.ICEBERG_V3, col(type(Schema.Type.DATE)), col(Schema.createTimestamp(9))));
  }

  @Test
  void dateToTheWithTimezoneTimestampsStaysForbidden() {
    // Explicitly excluded by the spec: a date carries no zone, and assigning one invents information.
    assertBothReject(col(type(Schema.Type.DATE)), col(Schema.createTimestampLtz(6)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void theReverseDirectionStaysForbidden() {
    assertBothReject(col(Schema.createTimestamp(6)), col(type(Schema.Type.DATE)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void timestampToTheNanosecondTimestampIsNotAPromotionAtEitherVersion() {
    // There is no row for it in the promotion table, in either column.
    assertBothReject(col(Schema.createTimestamp(6)), col(Schema.createTimestamp(9)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void theUnchangedPromotionsBehaveIdentically() {
    assertBothAccept(col(type(Schema.Type.INT)), col(type(Schema.Type.BIGINT)));
    assertBothAccept(col(type(Schema.Type.FLOAT)), col(type(Schema.Type.DOUBLE)));
    assertBothAccept(col(Schema.createDecimal(10, 2)), col(Schema.createDecimal(12, 2)));
    assertBothReject(col(type(Schema.Type.BIGINT)), col(type(Schema.Type.INT)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertBothReject(col(Schema.createDecimal(10, 2)), col(Schema.createDecimal(12, 4)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  // ---------------------------------------------------------------------------------------------
  // Representable types -- where v2 becomes stricter
  // ---------------------------------------------------------------------------------------------

  @Test
  void theNanosecondTimestampsAreInvisibleToTheComparison() {
    // Unchanged on both sides, so there is no change to object to at either version. Whether v2 can
    // store the type is asserted in ValidityCheckerTest, which sees it on a first registration too.
    assertBothAccept(col(Schema.createTimestamp(9)), col(Schema.createTimestamp(9)));
    assertBothAccept(col(Schema.createTimestampLtz(9)), col(Schema.createTimestampLtz(9)));
    assertBothAccept(col(Schema.createTimestamp(6)), col(Schema.createTimestamp(6)));
  }

  @Test
  void variantIsInvisibleToTheComparison() {
    // Same reasoning as the nanosecond timestamps: representability is a single-schema question.
    assertBothAccept(col(type(Schema.Type.VARIANT)), col(type(Schema.Type.VARIANT)));
  }

  @Test
  void timeNeedsNoWiderTypeAtAnyPrecision() {
    // Iceberg has no nanosecond time type in either version, so the precision is simply erased.
    assertBothAccept(col(Schema.createTime(3)), col(Schema.createTime(9)));
  }

  @Test
  void decimalPrecisionWideningIsAPromotionAtBothVersions() {
    // The cap of 38 is unchanged in v3, but it bounds the type rather than the change, so it is
    // ValidityChecker that enforces it.
    assertBothAccept(col(Schema.createDecimal(38, 2)), col(Schema.createDecimal(40, 2)));
  }

  // ---------------------------------------------------------------------------------------------
  // Identity rules are version-independent
  // ---------------------------------------------------------------------------------------------

  @Test
  void dropsRenamesAndReorderingAreRejectedAtBothVersions() {
    LogicalType two = schema(
        required("a", type(Schema.Type.INT)), required("b", type(Schema.Type.INT)));

    assertBothReject(two, schema(required("a", type(Schema.Type.INT))), Rule.FIELD_DELETED);
    assertBothReject(two,
        schema(required("a", type(Schema.Type.INT)), required("renamed", type(Schema.Type.INT))),
        Rule.FIELD_DELETED);
    assertBothReject(two,
        schema(required("b", type(Schema.Type.INT)), required("a", type(Schema.Type.INT))),
        Rule.FIELD_REORDERED);
  }

  @Test
  void mapKeysStayFrozenAtBothVersions() {
    assertBothReject(
        col(Schema.createMap(nonNull(Schema.createString()), nonNull(type(Schema.Type.INT)))),
        col(Schema.createMap(nonNull(type(Schema.Type.INT)), nonNull(type(Schema.Type.INT)))),
        Rule.MAP_KEY_TYPE_MISMATCH);
  }
}
