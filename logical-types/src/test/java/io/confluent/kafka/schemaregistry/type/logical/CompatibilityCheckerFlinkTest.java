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
 * Tests for {@link Mode#FLINK}.
 *
 * <p>Organised by the rule each case pins, so a rule change has an obvious blast radius. Leaf type
 * changes are split into the two halves they are built from: the root relation ported into
 * {@link FlinkLogicalTypeCasts}, and the parameter guards layered on top because that table is
 * root-keyed and cannot see a length, precision, or scale.
 *
 * <p>Several cases exist specifically to pin behaviour that <em>differs</em> from {@link
 * Mode#ICEBERG_V2}. Those are marked, and the two modes are deliberately not expected to agree.
 *
 * <p>A handful of leaf-type cases pin edges deliberately <em>narrower</em> than Flink's own table,
 * where
 * Flink admits a conversion that loses data. Each says so; the reasoning lives at the corresponding
 * declaration in {@link FlinkLogicalTypeCasts}.
 */
class CompatibilityCheckerFlinkTest {

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

  /** A single-column schema, for the many leaf-type cases below. */
  private static LogicalType col(Schema type) {
    return schema(required("c", type));
  }

  private static CompatibilityResult compare(LogicalType original, LogicalType update) {
    return CompatibilityChecker.compare(Mode.FLINK, original, update);
  }

  private static void assertCompatible(LogicalType original, LogicalType update) {
    CompatibilityResult result = compare(original, update);
    assertTrue(result.isCompatible(), "expected compatible but got: " + result.describe());
  }

  private static Incompatibility assertSingle(
      LogicalType original, LogicalType update, Rule expected) {
    CompatibilityResult result = compare(original, update);
    assertEquals(Collections.singletonList(expected),
        result.getIncompatibilities().stream()
            .map(Incompatibility::getRule)
            .collect(Collectors.toList()),
        result.describe());
    return result.getIncompatibilities().get(0);
  }

  /** Asserts a leaf type change is accepted, using a single-column schema. */
  private static void assertWidens(Schema from, Schema to) {
    assertCompatible(col(from), col(to));
  }

  /** Asserts a leaf type change is rejected, using a single-column schema. */
  private static void assertRejected(Schema from, Schema to) {
    assertSingle(col(from), col(to), Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  private static Schema type(Schema.Type t) {
    return Schema.create(t);
  }

  // ---------------------------------------------------------------------------------------------
  // Added columns
  // ---------------------------------------------------------------------------------------------

  @Test
  void addingANullableColumnIsCompatible() {
    assertCompatible(
        schema(required("id", type(Schema.Type.INT))),
        schema(required("id", type(Schema.Type.INT)), optional("name", Schema.createString())));
  }

  @Test
  void addingARequiredColumnWithoutADefaultIsIncompatible() {
    Incompatibility finding = assertSingle(
        schema(required("id", type(Schema.Type.INT))),
        schema(required("id", type(Schema.Type.INT)), required("name", Schema.createString())),
        Rule.REQUIRED_FIELD_ADDED);
    assertEquals("name", finding.getPath());
  }

  @Test
  void addingARequiredScalarWithADefaultIsCompatible() {
    // Differs from Iceberg mode, which rejects this: Iceberg v2 cannot persist a column default, so
    // it can only relax containers. Flink has no such limitation.
    assertCompatible(
        schema(required("id", type(Schema.Type.INT))),
        schema(
            required("id", type(Schema.Type.INT)),
            requiredWithDefault("count", type(Schema.Type.INT), 0)));
  }

  @Test
  void addingARequiredContainerWithADefaultIsCompatible() {
    assertCompatible(
        schema(required("id", type(Schema.Type.INT))),
        schema(
            required("id", type(Schema.Type.INT)),
            requiredWithDefault("tags", Schema.createArray(nonNull(Schema.createString())),
                Collections.emptyList())));
  }

  // ---------------------------------------------------------------------------------------------
  // Dropped and renamed columns
  // ---------------------------------------------------------------------------------------------

  @Test
  void droppingAColumnIsIncompatible() {
    Incompatibility finding = assertSingle(
        schema(required("id", type(Schema.Type.INT)), optional("name", Schema.createString())),
        schema(required("id", type(Schema.Type.INT))),
        Rule.FIELD_DELETED);
    assertEquals("name", finding.getPath());
  }

  @Test
  void renamingAColumnReportsBothADropAndAnAddition() {
    // A rename is not separately detectable without field IDs: it reads as a drop plus an add.
    CompatibilityResult result = compare(
        schema(required("id", type(Schema.Type.INT)), required("name", Schema.createString())),
        schema(required("id", type(Schema.Type.INT)), required("banana", Schema.createString())));
    List<Rule> rules = result.getIncompatibilities().stream()
        .map(Incompatibility::getRule)
        .collect(Collectors.toList());
    assertEquals(2, rules.size(), result.describe());
    assertTrue(rules.contains(Rule.FIELD_DELETED), result.describe());
    assertTrue(rules.contains(Rule.REQUIRED_FIELD_ADDED), result.describe());
  }

  // ---------------------------------------------------------------------------------------------
  // Column order
  // ---------------------------------------------------------------------------------------------

  @Test
  void reorderingColumnsIsIncompatible() {
    Incompatibility finding = assertSingle(
        schema(optional("a", type(Schema.Type.INT)), optional("b", type(Schema.Type.INT))),
        schema(optional("b", type(Schema.Type.INT)), optional("a", type(Schema.Type.INT))),
        Rule.FIELD_REORDERED);
    assertEquals("a", finding.getPath());
  }

  @Test
  void reorderingColumnsOfTheSameTypeIsStillDetected() {
    assertSingle(
        schema(optional("email", Schema.createString()), optional("name", Schema.createString())),
        schema(optional("name", Schema.createString()), optional("email", Schema.createString())),
        Rule.FIELD_REORDERED);
  }

  @Test
  void insertingAColumnInTheMiddleDoesNotCountAsReordering() {
    assertCompatible(
        schema(optional("a", type(Schema.Type.INT)), optional("b", type(Schema.Type.INT))),
        schema(
            optional("a", type(Schema.Type.INT)),
            optional("inserted", Schema.createString()),
            optional("b", type(Schema.Type.INT))));
  }

  // ---------------------------------------------------------------------------------------------
  // Nullability
  // ---------------------------------------------------------------------------------------------

  @Test
  void tighteningNullabilityIsIncompatible() {
    assertSingle(
        schema(optional("c", Schema.createString())),
        schema(required("c", Schema.createString())),
        Rule.NULLABLE_TO_NON_NULLABLE);
  }

  @Test
  void relaxingNullabilityIsCompatible() {
    assertCompatible(
        schema(required("c", Schema.createString())),
        schema(optional("c", Schema.createString())));
  }

  // ---------------------------------------------------------------------------------------------
  // Leaf types, part one -- the root relation ported from Flink
  // ---------------------------------------------------------------------------------------------

  @Test
  void integersWidenUpwardsOnly() {
    assertWidens(type(Schema.Type.TINYINT), type(Schema.Type.SMALLINT));
    assertWidens(type(Schema.Type.SMALLINT), type(Schema.Type.INT));
    assertWidens(type(Schema.Type.INT), type(Schema.Type.BIGINT));
    assertWidens(type(Schema.Type.TINYINT), type(Schema.Type.BIGINT));

    assertRejected(type(Schema.Type.BIGINT), type(Schema.Type.INT));
    assertRejected(type(Schema.Type.INT), type(Schema.Type.SMALLINT));
    assertRejected(type(Schema.Type.SMALLINT), type(Schema.Type.TINYINT));
  }

  @Test
  void integersWidenIntoFloatingPointOnlyWhenTheSignificandCanHoldThem() {
    // Narrower than Flink, which admits all four integer types into both. A FLOAT significand is 24
    // bits and a DOUBLE's is 53, so anything wider would be silently rounded.
    assertWidens(type(Schema.Type.TINYINT), type(Schema.Type.FLOAT));
    assertWidens(type(Schema.Type.SMALLINT), type(Schema.Type.FLOAT));
    assertRejected(type(Schema.Type.INT), type(Schema.Type.FLOAT));
    assertRejected(type(Schema.Type.BIGINT), type(Schema.Type.FLOAT));

    assertWidens(type(Schema.Type.TINYINT), type(Schema.Type.DOUBLE));
    assertWidens(type(Schema.Type.SMALLINT), type(Schema.Type.DOUBLE));
    assertWidens(type(Schema.Type.INT), type(Schema.Type.DOUBLE));
    assertRejected(type(Schema.Type.BIGINT), type(Schema.Type.DOUBLE));
  }

  @Test
  void floatWidensToDoubleButNotBack() {
    assertWidens(type(Schema.Type.FLOAT), type(Schema.Type.DOUBLE));
    assertRejected(type(Schema.Type.DOUBLE), type(Schema.Type.FLOAT));
  }

  @Test
  void decimalNeverConvertsToOrFromBinaryFloatingPoint() {
    // Narrower than Flink, which groups them into one NUMERIC family and admits both directions.
    // Approximate to exact overflows: no DECIMAL spans DOUBLE's range, and NaN and the infinities
    // have no representation. Exact to approximate cannot hold every decimal fraction.
    assertRejected(type(Schema.Type.DOUBLE), Schema.createDecimal(10, 2));
    assertRejected(type(Schema.Type.DOUBLE), Schema.createDecimal(38, 18));
    assertRejected(type(Schema.Type.FLOAT), Schema.createDecimal(10, 2));
    assertRejected(Schema.createDecimal(10, 2), type(Schema.Type.DOUBLE));
    assertRejected(Schema.createDecimal(10, 2), type(Schema.Type.FLOAT));
  }

  @Test
  void characterStringsWidenFromCharToVarcharOnly() {
    assertWidens(Schema.createChar(10), Schema.createVarchar(10));
    assertRejected(Schema.createVarchar(10), Schema.createChar(10));
  }

  @Test
  void binaryStringsWidenFromBinaryToVarbinaryOnly() {
    assertWidens(Schema.createBinary(10), Schema.createVarbinary(10));
    assertRejected(Schema.createVarbinary(10), Schema.createBinary(10));
  }

  @Test
  void timestampNeverConvertsToDateOrTime() {
    // Narrower than Flink, which admits both: one discards the time-of-day, the other the date.
    assertRejected(Schema.createTimestamp(3), type(Schema.Type.DATE));
    assertRejected(Schema.createTimestamp(3), Schema.createTime(3));
    assertRejected(type(Schema.Type.DATE), Schema.createTimestamp(3));
  }

  @Test
  void timestampAndTimestampLtzNeverConvertInEitherDirection() {
    // Narrower than Flink, which permits both directions. The two share a representation but not a
    // reference frame -- one is a wall-clock reading, the other an instant -- so re-annotating a
    // field shifts every historical value by the local UTC offset while the bytes stay put.
    assertRejected(Schema.createTimestamp(3), Schema.createTimestampLtz(3));
    assertRejected(Schema.createTimestampLtz(3), Schema.createTimestamp(3));
  }

  @Test
  void crossFamilyChangesAreRejected() {
    assertRejected(type(Schema.Type.INT), Schema.createString());
    assertRejected(Schema.createString(), type(Schema.Type.INT));
    assertRejected(type(Schema.Type.BOOLEAN), type(Schema.Type.INT));
    assertRejected(type(Schema.Type.INT), type(Schema.Type.BOOLEAN));
    assertRejected(type(Schema.Type.DATE), Schema.createTime(3));
    assertRejected(Schema.createString(), Schema.createVarbinary(10));
  }

  @Test
  void variantOnlyMatchesItself() {
    assertWidens(type(Schema.Type.VARIANT), type(Schema.Type.VARIANT));
    assertRejected(type(Schema.Type.VARIANT), Schema.createString());
    assertRejected(Schema.createString(), type(Schema.Type.VARIANT));
  }

  @Test
  void enumsDeriveToVarcharSoSymbolChangesAreInvisible() {
    Schema oneSymbol = Schema.createEnum(
        Collections.singletonList(new Schema.EnumValue("A")));
    Schema twoSymbols = Schema.createEnum(
        Arrays.asList(new Schema.EnumValue("A"), new Schema.EnumValue("B")));
    assertWidens(oneSymbol, twoSymbols);
    assertWidens(oneSymbol, Schema.createString());
    assertWidens(Schema.createString(), oneSymbol);
  }

  // ---------------------------------------------------------------------------------------------
  // Leaf types, part two -- the parameter guards Flink's table cannot see
  // ---------------------------------------------------------------------------------------------

  @Test
  void stringLengthMayGrowButNotShrink() {
    // The case that motivates Part B: the ported table passes this on roots alone, and neither the
    // format checker nor Iceberg mode sees a string length, so nothing else would catch it.
    assertWidens(Schema.createVarchar(10), Schema.createVarchar(50));
    assertRejected(Schema.createVarchar(50), Schema.createVarchar(10));
  }

  @Test
  void aNonPositiveVariableLengthIsTreatedAsUnbounded() {
    // The SR-LT-to-Flink shim maps a non-positive length on VARCHAR or VARBINARY to the unbounded
    // type rather than passing the number through, so this checker has to agree or the verdict
    // inverts in both directions: VARCHAR(0) -> VARCHAR(10) looks like growth here while the derived
    // Flink type actually narrows from unbounded to 10, and the reverse looks like a shrink while it
    // is really a widening. CHAR and BINARY are excluded -- their length is a stored width, and the
    // shim passes it through unchanged.
    assertRejected(Schema.createVarchar(0), Schema.createVarchar(10));
    assertWidens(Schema.createVarchar(10), Schema.createVarchar(0));
    assertRejected(Schema.createVarbinary(0), Schema.createVarbinary(10));
    assertWidens(Schema.createVarbinary(10), Schema.createVarbinary(0));

    // An unbounded VARCHAR is the same thing by either spelling.
    assertWidens(Schema.createVarchar(0), Schema.createString());
    assertWidens(Schema.createString(), Schema.createVarchar(0));
  }

  @Test
  void lengthGuardAppliesAcrossTheCharToVarcharWidening() {
    assertWidens(Schema.createChar(5), Schema.createVarchar(10));
    assertRejected(Schema.createChar(10), Schema.createVarchar(5));
  }

  @Test
  void fixedLengthTypesCannotChangeLengthAtAll() {
    // The declared length of CHAR and BINARY is the stored width, not a bound: BINARY pads to an
    // exact byte count and CHAR right-pads with spaces, so widening rewrites every historical value.
    // Avro resolution likewise requires an identical `fixed` size. Narrower than Flink, whose table
    // does not read the length at all.
    assertRejected(Schema.createBinary(16), Schema.createBinary(32));
    assertRejected(Schema.createBinary(32), Schema.createBinary(16));
    assertRejected(Schema.createChar(5), Schema.createChar(10));
    assertRejected(Schema.createChar(10), Schema.createChar(5));
  }

  @Test
  void temporalPrecisionIsFrozenInBothDirections() {
    // Precision is not a bound here: for an Avro logical type it selects the unit of the stored
    // integer, so a timestamp-millis field re-annotated as timestamp-micros keeps its bytes and its
    // column type while every value is read a thousandfold out. Growing it is no safer than
    // shrinking it -- the same reasoning that freezes decimal scale.
    assertRejected(Schema.createTimestamp(3), Schema.createTimestamp(6));
    assertRejected(Schema.createTimestamp(6), Schema.createTimestamp(3));
    assertRejected(Schema.createTime(3), Schema.createTime(9));
    assertRejected(Schema.createTime(9), Schema.createTime(3));
    assertRejected(Schema.createTimestampLtz(3), Schema.createTimestampLtz(9));
    assertRejected(Schema.createTimestampLtz(9), Schema.createTimestampLtz(3));
  }

  @Test
  void decimalPrecisionMayGrowButScaleIsFrozen() {
    assertWidens(Schema.createDecimal(10, 2), Schema.createDecimal(12, 2));
    assertRejected(Schema.createDecimal(12, 2), Schema.createDecimal(10, 2));
    assertRejected(Schema.createDecimal(10, 2), Schema.createDecimal(12, 4));
    assertRejected(Schema.createDecimal(10, 2), Schema.createDecimal(12, 0));
  }

  @Test
  void anIntegerWideningIntoDecimalMustLeaveRoomForItsRange() {
    assertWidens(type(Schema.Type.INT), Schema.createDecimal(10, 0));
    assertWidens(type(Schema.Type.INT), Schema.createDecimal(12, 2));
    assertRejected(type(Schema.Type.INT), Schema.createDecimal(9, 0));
    assertRejected(type(Schema.Type.INT), Schema.createDecimal(10, 2));

    assertWidens(type(Schema.Type.BIGINT), Schema.createDecimal(19, 0));
    assertRejected(type(Schema.Type.BIGINT), Schema.createDecimal(18, 0));

    assertWidens(type(Schema.Type.TINYINT), Schema.createDecimal(3, 0));
    assertRejected(type(Schema.Type.TINYINT), Schema.createDecimal(2, 0));
  }

  // ---------------------------------------------------------------------------------------------
  // Containers
  // ---------------------------------------------------------------------------------------------

  @Test
  void multisetAndMapAreDistinctTypesToFlink() {
    // Iceberg mode lowers a MULTISET to map<T,int> and accepts this; Flink keeps them apart.
    assertSingle(
        col(Schema.createMultiset(nonNull(Schema.createString()))),
        col(Schema.createMap(nonNull(Schema.createString()), nonNull(type(Schema.Type.INT)))),
        Rule.TYPE_MISMATCH);
  }

  @Test
  void arrayAndMapAreDistinctTypes() {
    assertSingle(
        col(Schema.createArray(nonNull(Schema.createString()))),
        col(Schema.createMap(nonNull(Schema.createString()), nonNull(Schema.createString()))),
        Rule.TYPE_MISMATCH);
  }

  @Test
  void structToPrimitiveIsATypeMismatch() {
    assertSingle(
        col(Schema.createStruct(Collections.singletonList(required("x", type(Schema.Type.INT))))),
        col(type(Schema.Type.INT)),
        Rule.TYPE_MISMATCH);
  }

  @Test
  void arrayElementsAreComparedAndReportedWithAnElementPath() {
    assertCompatible(
        col(Schema.createArray(nonNull(type(Schema.Type.INT)))),
        col(Schema.createArray(nonNull(type(Schema.Type.BIGINT)))));
    Incompatibility finding = assertSingle(
        col(Schema.createArray(nonNull(type(Schema.Type.BIGINT)))),
        col(Schema.createArray(nonNull(type(Schema.Type.INT)))),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertEquals("c[]", finding.getPath());
  }

  @Test
  void multisetElementsAreCompared() {
    assertCompatible(
        col(Schema.createMultiset(nonNull(type(Schema.Type.INT)))),
        col(Schema.createMultiset(nonNull(type(Schema.Type.BIGINT)))));
    assertSingle(
        col(Schema.createMultiset(nonNull(type(Schema.Type.BIGINT)))),
        col(Schema.createMultiset(nonNull(type(Schema.Type.INT)))),
        Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void mapValuesWidenButKeysAreFrozen() {
    assertCompatible(
        col(Schema.createMap(nonNull(Schema.createString()), nonNull(type(Schema.Type.INT)))),
        col(Schema.createMap(nonNull(Schema.createString()), nonNull(type(Schema.Type.BIGINT)))));

    Incompatibility finding = assertSingle(
        col(Schema.createMap(nonNull(type(Schema.Type.INT)), nonNull(Schema.createString()))),
        col(Schema.createMap(nonNull(type(Schema.Type.BIGINT)), nonNull(Schema.createString()))),
        Rule.MAP_KEY_TYPE_MISMATCH);
    assertEquals("c", finding.getPath());
  }

  @Test
  void mapKeysAreFrozenDownToTheirParameters() {
    assertSingle(
        col(Schema.createMap(nonNull(Schema.createVarchar(10)), nonNull(Schema.createString()))),
        col(Schema.createMap(nonNull(Schema.createVarchar(50)), nonNull(Schema.createString()))),
        Rule.MAP_KEY_TYPE_MISMATCH);
  }

  // ---------------------------------------------------------------------------------------------
  // Nested structs
  // ---------------------------------------------------------------------------------------------

  @Test
  void addingANullableNestedColumnIsCompatible() {
    assertCompatible(
        schema(required("inner", Schema.createStruct(Collections.singletonList(
            required("a", type(Schema.Type.INT)))))),
        schema(required("inner", Schema.createStruct(Arrays.asList(
            required("a", type(Schema.Type.INT)),
            optional("b", Schema.createString()))))));
  }

  @Test
  void addingARequiredNestedColumnIsReportedWithADottedPath() {
    Incompatibility finding = assertSingle(
        schema(required("inner", Schema.createStruct(Collections.singletonList(
            required("a", type(Schema.Type.INT)))))),
        schema(required("inner", Schema.createStruct(Arrays.asList(
            required("a", type(Schema.Type.INT)),
            required("b", Schema.createString()))))),
        Rule.REQUIRED_FIELD_ADDED);
    assertEquals("inner.b", finding.getPath());
  }

  @Test
  void nestedTypeChangesAreReportedWithADottedPath() {
    Incompatibility finding = assertSingle(
        schema(required("inner", Schema.createStruct(Collections.singletonList(
            required("a", type(Schema.Type.BIGINT)))))),
        schema(required("inner", Schema.createStruct(Collections.singletonList(
            required("a", type(Schema.Type.INT)))))),
        Rule.UNSUPPORTED_TYPE_CHANGE);
    assertEquals("inner.a", finding.getPath());
  }

  // ---------------------------------------------------------------------------------------------
  // Unions and named type references
  // ---------------------------------------------------------------------------------------------

  @Test
  void unionBranchesAreComparedAsOptionalStructFields() {
    Schema before = Schema.createUnion(Arrays.asList(
        new UnionBranch("s", Schema.createString()),
        new UnionBranch("i", type(Schema.Type.INT))));
    Schema after = Schema.createUnion(Arrays.asList(
        new UnionBranch("s", Schema.createString()),
        new UnionBranch("i", type(Schema.Type.BIGINT))));
    assertCompatible(schema(new Field("u", before, 0)), schema(new Field("u", after, 0)));
  }

  @Test
  void removingAUnionBranchIsIncompatible() {
    Schema before = Schema.createUnion(Arrays.asList(
        new UnionBranch("s", Schema.createString()),
        new UnionBranch("i", type(Schema.Type.INT))));
    Schema after = Schema.createUnion(Collections.singletonList(
        new UnionBranch("s", Schema.createString())));
    assertSingle(schema(new Field("u", before, 0)), schema(new Field("u", after, 0)),
        Rule.FIELD_DELETED);
  }

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

  private static LogicalType recursiveTree(boolean withExtraRequiredField) {
    List<Field> fields = new ArrayList<>();
    fields.add(required("value", type(Schema.Type.INT)));
    fields.add(optional("children", Schema.createArray(Schema.createNamedTypeRef("Node"))));
    if (withExtraRequiredField) {
      fields.add(required("extra", Schema.createString()));
    }
    Schema node = nonNull(Schema.createStruct(fields));
    return new LogicalType(node, Collections.singletonMap("Node", node));
  }

  // ---------------------------------------------------------------------------------------------
  // Report-all
  // ---------------------------------------------------------------------------------------------

  @Test
  void allViolationsAreReportedNotJustTheFirst() {
    LogicalType original = schema(
        required("keep", type(Schema.Type.BIGINT)),
        optional("nullable", Schema.createString()),
        required("dropped", type(Schema.Type.INT)));
    LogicalType update = schema(
        required("keep", type(Schema.Type.INT)),
        required("nullable", Schema.createString()),
        required("added", type(Schema.Type.INT)));

    CompatibilityResult result = compare(original, update);
    List<Rule> rules = result.getIncompatibilities().stream()
        .map(Incompatibility::getRule)
        .collect(Collectors.toList());
    assertEquals(4, rules.size(), result.describe());
    assertTrue(rules.contains(Rule.UNSUPPORTED_TYPE_CHANGE), result.describe());
    assertTrue(rules.contains(Rule.NULLABLE_TO_NON_NULLABLE), result.describe());
    assertTrue(rules.contains(Rule.REQUIRED_FIELD_ADDED), result.describe());
    assertTrue(rules.contains(Rule.FIELD_DELETED), result.describe());
  }
}
