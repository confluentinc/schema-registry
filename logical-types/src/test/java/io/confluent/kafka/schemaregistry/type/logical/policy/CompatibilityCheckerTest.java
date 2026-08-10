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

import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.policy.Incompatibility.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cases with <b>no counterpart in the Iceberg-schema checker</b>, and therefore deliberately not
 * synced with it.
 *
 * <p>Everything here exercises behaviour that suite structurally cannot reach. Its input is an
 * Iceberg schema that the Iceberg type mapping has *already* erased, so it can test neither the
 * erasure itself nor any SRLT type that does not survive the conversion:
 *
 * <ul>
 *   <li>the type erasure ({@code icebergClassOf}) — TINYINT/SMALLINT collapse, string and binary
 *       length, time and timestamp precision, the micros/nanos boundary;
 *   <li>SRLT types Iceberg has no equivalent for — UNION, ENUM, MULTISET, {@code NAMED_TYPE_REF};
 *   <li>this checker's own additions — report-all and {@link Mode#FLINK}.
 * </ul>
 *
 * <p>Cases carried over from that suite live in {@link CompatibilityCheckerIcebergSchemaTest} and are kept
 * diffable against it. Do not duplicate a case across the two files.
 */
class CompatibilityCheckerTest {

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static Schema nonNull(Schema schema) {
    return schema.setNullable(false);
  }

  private static Field required(String name, Schema type, int position) {
    return new Field(name, nonNull(type), position);
  }

  private static Field optional(String name, Schema type, int position) {
    return new Field(name, type.setNullable(true), position);
  }

  private static LogicalType struct(Field... fields) {
    return new LogicalType(nonNull(Schema.createStruct(Arrays.asList(fields))));
  }

  private static CompatibilityResult compare(LogicalType original, LogicalType update) {
    return CompatibilityChecker.compare(Mode.ICEBERG_V2, original, update);
  }

  private static List<Rule> rulesOf(CompatibilityResult result) {
    return result.getIncompatibilities().stream()
        .map(Incompatibility::getRule)
        .collect(Collectors.toList());
  }

  private static void assertCompatible(LogicalType original, LogicalType update) {
    CompatibilityResult result = compare(original, update);
    assertTrue(result.isCompatible(), "expected compatible but got: " + result.describe());
  }

  private static Incompatibility assertSingle(
      LogicalType original, LogicalType update, Rule expected) {
    CompatibilityResult result = compare(original, update);
    assertEquals(Collections.singletonList(expected), rulesOf(result), result.describe());
    return result.getIncompatibilities().get(0);
  }

  // ---------------------------------------------------------------------------------------------
  // Mode dispatch
  // ---------------------------------------------------------------------------------------------

  @Test
  void identicalSchemasAreCompatible() {
    assertCompatible(
        struct(required("id", Schema.create(Schema.Type.INT), 0)),
        struct(required("id", Schema.create(Schema.Type.INT), 0)));
  }

  @Test
  void bothModesDisagreeWhereTheirTypeSystemsDo() {
    // A MULTISET is a MAP to Iceberg but a distinct type to Flink, so the two modes reach opposite
    // verdicts on the same pair. Pinned here because it is the clearest evidence the modes are not
    // interchangeable.
    LogicalType before =
        struct(required("c", Schema.createMultiset(nonNull(Schema.createString())), 0));
    LogicalType after = struct(required("c", Schema.createMap(
        nonNull(Schema.createString()), nonNull(Schema.create(Schema.Type.INT))), 0));

    assertTrue(CompatibilityChecker.compare(Mode.ICEBERG_V2, before, after).isCompatible());
    assertEquals(
        Collections.singletonList(Rule.TYPE_MISMATCH),
        CompatibilityChecker.compare(Mode.FLINK, before, after).getIncompatibilities().stream()
            .map(Incompatibility::getRule)
            .collect(Collectors.toList()));
  }

  // ---------------------------------------------------------------------------------------------
  // Type erasure -- invisible there, because the Iceberg type mapping has already applied it
  // ---------------------------------------------------------------------------------------------

  @Test
  void smallintToBigintIsCompatibleViaIntClass() {
    // TINYINT/SMALLINT/INT all erase to Iceberg int, so this is int -> long.
    assertCompatible(
        struct(required("n", Schema.create(Schema.Type.SMALLINT), 0)),
        struct(required("n", Schema.create(Schema.Type.BIGINT), 0)));
  }

  @Test
  void tinyintToIntIsInvisibleToIceberg() {
    assertCompatible(
        struct(required("n", Schema.create(Schema.Type.TINYINT), 0)),
        struct(required("n", Schema.create(Schema.Type.INT), 0)));
  }

  @Test
  void floatToDoubleIsCompatibleButNotReverse() {
    assertCompatible(
        struct(required("x", Schema.create(Schema.Type.FLOAT), 0)),
        struct(required("x", Schema.create(Schema.Type.DOUBLE), 0)));
    assertSingle(
        struct(required("x", Schema.create(Schema.Type.DOUBLE), 0)),
        struct(required("x", Schema.create(Schema.Type.FLOAT), 0)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void varcharLengthChangeIsInvisibleToIceberg() {
    // Iceberg has no string length. The Flink-level checker is responsible for catching this.
    assertCompatible(
        struct(required("s", Schema.createVarchar(50), 0)),
        struct(required("s", Schema.createVarchar(10), 0)));
  }

  @Test
  void charToVarcharAndEnumToStringAreInvisibleToIceberg() {
    assertCompatible(
        struct(required("s", Schema.createChar(10), 0)),
        struct(required("s", Schema.createVarchar(10), 0)));
    assertCompatible(
        struct(required("s", Schema.createEnum(
            Arrays.asList(new Schema.EnumValue("A"), new Schema.EnumValue("B"))), 0)),
        struct(required("s", Schema.createString(), 0)));
  }

  @Test
  void enumSymbolChangesAreInvisibleToIceberg() {
    assertCompatible(
        struct(required("s", Schema.createEnum(
            Collections.singletonList(new Schema.EnumValue("A"))), 0)),
        struct(required("s", Schema.createEnum(
            Arrays.asList(new Schema.EnumValue("A"), new Schema.EnumValue("B"))), 0)));
  }

  @Test
  void varbinaryLengthIsErasedButBinaryLengthIsPreservedAsFixed() {
    assertCompatible(
        struct(required("b", Schema.createVarbinary(16), 0)),
        struct(required("b", Schema.createVarbinary(32), 0)));
    assertSingle(
        struct(required("b", Schema.createBinary(16), 0)),
        struct(required("b", Schema.createBinary(32), 0)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void timestampPrecisionChangeIsInvisibleToIcebergWithinMicros() {
    assertCompatible(
        struct(required("ts", Schema.createTimestamp(3), 0)),
        struct(required("ts", Schema.createTimestamp(6), 0)));
  }

  @Test
  void crossingTheMicrosBoundaryIsATypeChangeAtBothVersions() {
    // timestamp and timestamp_ns are distinct types, and the spec adds no promotion between them in
    // either version -- so the comparison reaches the same verdict at both. Whether v2 can store
    // timestamp_ns at all is a property of the one schema rather than of the change; see
    // ValidityCheckerTest.
    LogicalType micros = struct(required("ts", Schema.createTimestamp(6), 0));
    LogicalType nanos = struct(required("ts", Schema.createTimestamp(9), 0));
    assertSingle(micros, nanos, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertEquals(Collections.singletonList(Rule.UNSUPPORTED_TYPE_CHANGE),
        rulesOf(CompatibilityChecker.compare(Mode.ICEBERG_V3, micros, nanos)));
  }

  @Test
  void precisionChangeWithinTheNanosecondTypeIsInvisibleAtBothVersions() {
    // Both erase to timestamp_ns, so the comparison has nothing to see at either version. That v2
    // cannot store that type is ValidityCheckerTest's business, and it holds of both schemas here
    // rather than being caused by the change.
    LogicalType sevenDigits = struct(required("ts", Schema.createTimestamp(7), 0));
    LogicalType nineDigits = struct(required("ts", Schema.createTimestamp(9), 0));
    assertCompatible(sevenDigits, nineDigits);
    assertTrue(CompatibilityChecker.compare(Mode.ICEBERG_V3, sevenDigits, nineDigits)
        .isCompatible());
  }

  @Test
  void timestampAndTimestampLtzAreDistinctInIceberg() {
    assertSingle(
        struct(required("ts", Schema.createTimestamp(3), 0)),
        struct(required("ts", Schema.createTimestampLtz(3), 0)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void timePrecisionIsAlwaysErasedBecauseIcebergHasNoNanosecondTime() {
    assertCompatible(
        struct(required("t", Schema.createTime(3), 0)),
        struct(required("t", Schema.createTime(9), 0)));
  }

  @Test
  void decimalPrecisionMayWidenButScaleMayNot() {
    assertCompatible(
        struct(required("d", Schema.createDecimal(10, 2), 0)),
        struct(required("d", Schema.createDecimal(12, 2), 0)));
    assertSingle(
        struct(required("d", Schema.createDecimal(10, 2), 0)),
        struct(required("d", Schema.createDecimal(12, 4), 0)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertSingle(
        struct(required("d", Schema.createDecimal(12, 2), 0)),
        struct(required("d", Schema.createDecimal(10, 2), 0)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void decimalBeyondIcebergsMaximumPrecisionIsNotTheComparisonsConcern() {
    // Widening the precision while holding the scale is a listed promotion, so the change itself is
    // fine. That 40 exceeds Iceberg's cap is a property of the updated schema alone, and is reported
    // by ValidityChecker -- which, unlike this check, also catches it on a first registration.
    assertCompatible(
        struct(required("d", Schema.createDecimal(38, 2), 0)),
        struct(required("d", Schema.createDecimal(40, 2), 0)));
  }

  // ---------------------------------------------------------------------------------------------
  // Structural shapes
  // ---------------------------------------------------------------------------------------------

  @Test
  void structToPrimitiveIsATypeMismatchForIceberg() {
    assertSingle(
        struct(required("inner",
            Schema.createStruct(Collections.singletonList(
                required("x", Schema.create(Schema.Type.INT), 0))), 0)),
        struct(required("inner", Schema.create(Schema.Type.INT), 0)),
        Rule.TYPE_MISMATCH);
  }

  @Test
  void arrayToMapIsATypeMismatch() {
    assertSingle(
        struct(required("c", Schema.createArray(nonNull(Schema.createString())), 0)),
        struct(required("c", Schema.createMap(
            nonNull(Schema.createString()), nonNull(Schema.createString())), 0)),
        Rule.TYPE_MISMATCH);
  }

  @Test
  void arrayElementNarrowingIsReportedWithElementPath() {
    Incompatibility finding = assertSingle(
        struct(required("xs", Schema.createArray(Schema.create(Schema.Type.BIGINT)), 0)),
        struct(required("xs", Schema.createArray(Schema.create(Schema.Type.INT)), 0)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertEquals("xs[]", finding.getPath());
  }

  @Test
  void multisetIsComparedAsAMapFromElementToCount() {
    assertCompatible(
        struct(required("ms", Schema.createMultiset(nonNull(Schema.createString())), 0)),
        struct(required("ms", Schema.createMultiset(nonNull(Schema.createString())), 0)));
    assertSingle(
        struct(required("ms", Schema.createMultiset(nonNull(Schema.createString())), 0)),
        struct(required("ms", Schema.createMultiset(nonNull(Schema.create(Schema.Type.INT))), 0)),
        Rule.MAP_KEY_TYPE_MISMATCH);
  }

  @Test
  void multisetAndMapAreTheSameShapeToIceberg() {
    // Both lower to map<T, int>, so the kinds match and comparison proceeds into key and value.
    assertCompatible(
        struct(required("c", Schema.createMultiset(nonNull(Schema.createString())), 0)),
        struct(required("c", Schema.createMap(
            nonNull(Schema.createString()), nonNull(Schema.create(Schema.Type.INT))), 0)));
  }

  // ---------------------------------------------------------------------------------------------
  // Unions -- lowered to structs of optional branches; an Iceberg schema has no UNION
  // ---------------------------------------------------------------------------------------------

  @Test
  void unionBranchesAreComparedAsOptionalStructFieldsForIceberg() {
    Schema before = Schema.createUnion(Arrays.asList(
        new UnionBranch("s", Schema.createString()),
        new UnionBranch("i", Schema.create(Schema.Type.INT))));
    Schema after = Schema.createUnion(Arrays.asList(
        new UnionBranch("s", Schema.createString()),
        new UnionBranch("i", Schema.create(Schema.Type.BIGINT))));
    assertCompatible(
        struct(new Field("u", before, 0)),
        struct(new Field("u", after, 0)));
  }

  @Test
  void addingAUnionBranchIsCompatibleBecauseBranchesAreOptional() {
    Schema before = Schema.createUnion(Collections.singletonList(
        new UnionBranch("s", Schema.createString())));
    Schema after = Schema.createUnion(Arrays.asList(
        new UnionBranch("s", Schema.createString()),
        new UnionBranch("i", Schema.create(Schema.Type.INT))));
    assertCompatible(
        struct(new Field("u", before, 0)),
        struct(new Field("u", after, 0)));
  }

  @Test
  void removingAUnionBranchIsIncompatible() {
    Schema before = Schema.createUnion(Arrays.asList(
        new UnionBranch("s", Schema.createString()),
        new UnionBranch("i", Schema.create(Schema.Type.INT))));
    Schema after = Schema.createUnion(Collections.singletonList(
        new UnionBranch("s", Schema.createString())));
    assertSingle(
        struct(new Field("u", before, 0)),
        struct(new Field("u", after, 0)),
        Rule.FIELD_DELETED);
  }

  @Test
  void reorderingUnionBranchesIsIncompatible() {
    Schema before = Schema.createUnion(Arrays.asList(
        new UnionBranch("s", Schema.createString()),
        new UnionBranch("i", Schema.create(Schema.Type.INT))));
    Schema after = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT)),
        new UnionBranch("s", Schema.createString())));
    assertSingle(
        struct(new Field("u", before, 0)),
        struct(new Field("u", after, 0)),
        Rule.FIELD_REORDERED);
  }

  // ---------------------------------------------------------------------------------------------
  // Named type references -- Iceberg schemas cannot recurse, so there is no counterpart
  // ---------------------------------------------------------------------------------------------

  @Test
  void recursiveSchemaTerminatesForIceberg() {
    assertCompatible(recursiveTree(false), recursiveTree(false));
  }

  @Test
  void aChangeInsideARecursiveTypeIsReportedOnceAtTheShallowestPathForIceberg() {
    Incompatibility finding =
        assertSingle(recursiveTree(false), recursiveTree(true), Rule.REQUIRED_FIELD_ADDED);
    assertEquals("extra", finding.getPath());
  }

  @Test
  void unresolvableReferencesAreComparedByQualifiedName() {
    LogicalType before = new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            optional("ext", Schema.createNamedTypeRef("some.External"), 0)))));
    LogicalType same = new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            optional("ext", Schema.createNamedTypeRef("some.External"), 0)))));
    LogicalType different = new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            optional("ext", Schema.createNamedTypeRef("some.Other"), 0)))));

    assertCompatible(before, same);
    assertSingle(before, different, Rule.TYPE_MISMATCH);
  }

  /**
   * Builds {@code Node { value INT, children ARRAY<ref(Node)> }}, optionally with an extra required
   * field so the recursive walk has something to report.
   */
  private static LogicalType recursiveTree(boolean withExtraRequiredField) {
    List<Field> fields = new ArrayList<>();
    fields.add(required("value", Schema.create(Schema.Type.INT), 0));
    fields.add(optional("children",
        Schema.createArray(Schema.createNamedTypeRef("Node")), 1));
    if (withExtraRequiredField) {
      fields.add(required("extra", Schema.createString(), 2));
    }
    Schema node = nonNull(Schema.createStruct(fields));
    return new LogicalType(node, Collections.singletonMap("Node", node));
  }

  // ---------------------------------------------------------------------------------------------
  // Report-all -- the other implementation throws on the first violation
  // ---------------------------------------------------------------------------------------------

  @Test
  void allViolationsAreReportedNotJustTheFirst() {
    LogicalType original = struct(
        required("keep", Schema.create(Schema.Type.INT), 0),
        optional("nullable", Schema.createString(), 1),
        required("dropped", Schema.create(Schema.Type.INT), 2));
    LogicalType update = struct(
        required("keep", Schema.create(Schema.Type.BIGINT), 0),
        required("nullable", Schema.createString(), 1),
        required("added", Schema.create(Schema.Type.INT), 2));

    CompatibilityResult result = compare(original, update);
    List<Rule> rules = rulesOf(result);
    assertEquals(3, rules.size(), result.describe());
    assertTrue(rules.contains(Rule.NULLABLE_TO_NON_NULLABLE), result.describe());
    assertTrue(rules.contains(Rule.REQUIRED_FIELD_ADDED), result.describe());
    assertTrue(rules.contains(Rule.FIELD_DELETED), result.describe());
  }

  @Test
  void findingsCarryRuleAndPathAndDescribeRendersThemAll() {
    CompatibilityResult result = compare(
        struct(
            required("a", Schema.create(Schema.Type.BIGINT), 0),
            required("b", Schema.create(Schema.Type.BIGINT), 1)),
        struct(
            required("a", Schema.create(Schema.Type.INT), 0),
            required("b", Schema.create(Schema.Type.INT), 1)));

    assertEquals(2, result.getIncompatibilities().size(), result.describe());
    assertTrue(result.describe().contains("a"), result.describe());
    assertTrue(result.describe().contains("b"), result.describe());
  }

  // -----------------------------------------------------------------------------------------------
  // Regressions from the review of the policy package
  // -----------------------------------------------------------------------------------------------

  @Test
  void aRecursiveNamedTypeUnderAMapKeyDoesNotOverflowTheStack() {
    // erasedEquals has no share in the main walk's cycle guards, so it carries its own. Without it
    // a recursive named type reached through a map key resolves forever and throws StackOverflowError
    // -- an Error, which cannot be reported to a user as an incompatibility at all.
    LogicalType t = recursiveUnder(false);
    assertTrue(CompatibilityChecker.compare(Mode.ICEBERG_V2, t, t).isCompatible());
    assertTrue(CompatibilityChecker.compare(Mode.ICEBERG_V3, t, t).isCompatible());
  }

  @Test
  void aRecursiveNamedTypeUnderAMultisetElementDoesNotOverflowTheStack() {
    // Same path: Iceberg lowers MULTISET<T> to map<T, int>, so keyOf returns the element.
    LogicalType t = recursiveUnder(true);
    assertTrue(CompatibilityChecker.compare(Mode.ICEBERG_V2, t, t).isCompatible());
  }

  @Test
  void anOmittedDecimalScaleCompareEqualToAnExplicitZero() {
    // NO_PARAM is SRLT's "scale omitted", and SQL reads DECIMAL(p) as DECIMAL(p, 0), so this is a
    // no-op change rather than a scale change.
    LogicalType omitted = struct(required("d", Schema.createDecimal(10, Schema.NO_PARAM), 0));
    LogicalType explicit = struct(required("d", Schema.createDecimal(10, 0), 0));
    for (Mode mode : Mode.values()) {
      assertTrue(CompatibilityChecker.compare(mode, omitted, explicit).isCompatible(),
          mode + " omitted -> explicit");
      assertTrue(CompatibilityChecker.compare(mode, explicit, omitted).isCompatible(),
          mode + " explicit -> omitted");
    }
  }

  @Test
  void anIntegerWideningIntoADecimalWithOmittedScaleStillChecksTheRange() {
    // The dangerous half: precision - (-1) overstated the available integer digits by one, so
    // BIGINT -> DECIMAL(18) passed while the DECIMAL(18, 0) control was correctly rejected.
    // Mode.FLINK explicitly: this class's helpers default to ICEBERG_V2, which has no
    // integer-to-decimal promotion at all and would reject every case here for the wrong reason.
    LogicalType from = struct(required("d", Schema.create(Schema.Type.BIGINT), 0));
    assertFalse(CompatibilityChecker.compare(Mode.FLINK, from,
        struct(required("d", Schema.createDecimal(18, Schema.NO_PARAM), 0))).isCompatible());
    assertFalse(CompatibilityChecker.compare(Mode.FLINK, from,
        struct(required("d", Schema.createDecimal(18, 0), 0))).isCompatible());
    // 19 digits is enough for BIGINT, with or without the sentinel.
    assertTrue(CompatibilityChecker.compare(Mode.FLINK, from,
        struct(required("d", Schema.createDecimal(19, Schema.NO_PARAM), 0))).isCompatible());
    assertTrue(CompatibilityChecker.compare(Mode.FLINK, from,
        struct(required("d", Schema.createDecimal(19, 0), 0))).isCompatible());
  }

  @Test
  void aOneSidedRefDoesNotSwallowLaterFindings() {
    // refKey returns "" for a non-ref, so keying the dedup on a one-sided ref collapsed every
    // inline counterpart onto one key: the first (inline, ref X) pair claimed it and later ones
    // returned without comparing anything. Both fields must be reported.
    // Two distinct objects, as a format conversion produces. Reusing one instance would let the
    // identity-keyed struct-pair guard suppress the second finding for an unrelated reason.
    Schema inlineA = nonNull(Schema.createStruct(Collections.singletonList(
        required("v", Schema.create(Schema.Type.INT), 0))));
    Schema inlineB = nonNull(Schema.createStruct(Collections.singletonList(
        required("v", Schema.create(Schema.Type.INT), 0))));
    Map<String, Schema> named = new LinkedHashMap<>();
    named.put("ns.X", nonNull(Schema.createStruct(Collections.singletonList(
        required("v", Schema.createString(), 0)))));
    LogicalType before = new LogicalType(nonNull(Schema.createStruct(Arrays.asList(
        new Field("a", inlineA, 0), new Field("b", inlineB, 1)))));
    LogicalType after = new LogicalType(nonNull(Schema.createStruct(Arrays.asList(
        new Field("a", nonNull(Schema.createNamedTypeRef("ns.X")), 0),
        new Field("b", nonNull(Schema.createNamedTypeRef("ns.X")), 1)))), named);

    assertEquals(Arrays.asList("a.v", "b.v"),
        CompatibilityChecker.compare(Mode.ICEBERG_V2, before, after).getIncompatibilities().stream()
            .map(Incompatibility::getPath)
            .collect(Collectors.toList()));
  }

  /** {@code struct { m: <container><ns.R> }} where {@code ns.R} refers to itself. */
  private static LogicalType recursiveUnder(boolean multiset) {
    Map<String, Schema> named = new LinkedHashMap<>();
    named.put("ns.R", nonNull(Schema.createStruct(Arrays.asList(
        required("v", Schema.create(Schema.Type.INT), 0),
        new Field("next", Schema.createNamedTypeRef("ns.R").setNullable(true), 1)))));
    Schema ref = nonNull(Schema.createNamedTypeRef("ns.R"));
    Schema container = multiset
        ? Schema.createMultiset(ref)
        : Schema.createMap(ref, nonNull(Schema.create(Schema.Type.INT)));
    return new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            new Field("m", nonNull(container), 0)))), named);
  }

}
