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
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cases with <b>no counterpart in the Iceberg-schema checker</b>, and therefore deliberately not
 * synced with it.
 *
 * <p>Everything here exercises behaviour that suite structurally cannot reach. Its input is an
 * Iceberg schema that {@code FlinkTypeToType} has *already* erased, so it can test neither the
 * erasure itself nor any SRLT type that does not survive the conversion:
 *
 * <ul>
 *   <li>the type erasure ({@code icebergClassOf}) — TINYINT/SMALLINT collapse, string and binary
 *       length, time and timestamp precision, the micros/nanos boundary;
 *   <li>SRLT types Iceberg has no equivalent for — UNION, ENUM, MULTISET, {@code NAMED_TYPE_REF};
 *   <li>this checker's own additions — report-all, {@link Rule#UNREPRESENTABLE_TYPE},
 *       {@link Mode#FLINK}.
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
    return CompatibilityChecker.compare(Mode.ICEBERG, original, update);
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

    assertTrue(CompatibilityChecker.compare(Mode.ICEBERG, before, after).isCompatible());
    assertEquals(
        Collections.singletonList(Rule.TYPE_MISMATCH),
        CompatibilityChecker.compare(Mode.FLINK, before, after).getIncompatibilities().stream()
            .map(Incompatibility::getRule)
            .collect(Collectors.toList()));
  }

  // ---------------------------------------------------------------------------------------------
  // Type erasure -- invisible there, because FlinkTypeToType has already applied it
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
  void crossingTheMicrosBoundaryChangesTheIcebergTimestampType() {
    // Iceberg's FlinkTypeToType maps precision > 6 to timestamp_ns, a distinct type that is not a
    // promotion target of timestamp -- so this is a type change, not an unrepresentable value.
    assertSingle(
        struct(required("ts", Schema.createTimestamp(6), 0)),
        struct(required("ts", Schema.createTimestamp(9), 0)),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void precisionChangeWithinTheNanosecondTypeIsInvisibleToIceberg() {
    assertCompatible(
        struct(required("ts", Schema.createTimestamp(7), 0)),
        struct(required("ts", Schema.createTimestamp(9), 0)));
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
  void decimalBeyondIcebergsMaximumPrecisionIsUnrepresentable() {
    assertSingle(
        struct(required("d", Schema.createDecimal(38, 2), 0)),
        struct(required("d", Schema.createDecimal(40, 2), 0)),
        Rule.UNREPRESENTABLE_TYPE);
  }

  // ---------------------------------------------------------------------------------------------
  // Structural shapes
  // ---------------------------------------------------------------------------------------------

  @Test
  void structToPrimitiveIsATypeMismatch() {
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
  void unionBranchesAreComparedAsOptionalStructFields() {
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
  void recursiveSchemaTerminates() {
    assertCompatible(recursiveTree(false), recursiveTree(false));
  }

  @Test
  void incompatibleChangeInsideARecursiveTypeIsReportedOnceAtTheShallowestPath() {
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
}
