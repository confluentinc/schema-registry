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
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Cases carried over from Apache Flink's own {@code LogicalTypeCastsTest}, to check that
 * {@link FlinkLogicalTypeCasts} really is a faithful transcription of the table it mirrors.
 *
 * <p>Flink's suite is a single parameterised provider of
 * {@code (sourceType, targetType, supportsImplicit, supportsExplicit)} tuples. Only the implicit flag
 * is carried over — nothing here consults an explicit cast table — and only the cases whose types have
 * an SRLT equivalent. Of Flink's 37 tuples, 24 qualify: the rest use intervals, raw types, the null
 * type, structured types, or a zoned (as opposed to local-zoned) timestamp.
 *
 * <p>Bare types are compared by wrapping each in a single-column schema, since
 * {@link CompatibilityChecker#compare} takes a whole {@link LogicalType}. Flink's own default
 * nullability is preserved: a type is nullable unless its Flink constructor was passed {@code false}.
 *
 * <p><b>The split between the two providers is the point of this file.</b>
 * {@link #flinkAgreedTestData()} copies Flink's expectations verbatim, so a disagreement there means
 * the transcription drifted. {@link #flinkDivergentTestData()} holds the cases where this module
 * deliberately answers differently, each with its reason — and all three turn out to be parameter
 * narrowings that Flink's root-keyed table structurally cannot see, which is exactly what the
 * parameter guards exist for.
 */
class FlinkLogicalTypeCastsTest {

  // ---------------------------------------------------------------------------------------------
  // Translations of Flink's type constructors
  // ---------------------------------------------------------------------------------------------

  /** Flink's {@code new SmallIntType()} and friends: nullable by default. */
  private static Schema nullable(Schema schema) {
    return schema.setNullable(true);
  }

  /** Flink's {@code new SmallIntType(false)}: the boolean is {@code isNullable}. */
  private static Schema notNull(Schema schema) {
    return schema.setNullable(false);
  }

  private static Schema smallIntType() {
    return nullable(Schema.create(Schema.Type.SMALLINT));
  }

  private static Schema intType() {
    return nullable(Schema.create(Schema.Type.INT));
  }

  private static Schema bigIntType() {
    return nullable(Schema.create(Schema.Type.BIGINT));
  }

  private static Schema floatType() {
    return nullable(Schema.create(Schema.Type.FLOAT));
  }

  private static Schema booleanType() {
    return nullable(Schema.create(Schema.Type.BOOLEAN));
  }

  /** Flink's {@code new VarCharType(Integer.MAX_VALUE)}. */
  private static Schema varCharMaxType() {
    return nullable(Schema.createString());
  }

  private static Schema decimalType(int precision, int scale) {
    return nullable(Schema.createDecimal(precision, scale));
  }

  private static Schema timestampType(int precision) {
    return nullable(Schema.createTimestamp(precision));
  }

  private static Schema localZonedTimestampType(int precision) {
    return nullable(Schema.createTimestampLtz(precision));
  }

  private static Schema arrayType(Schema element) {
    return nullable(Schema.createArray(element));
  }

  /** Flink's {@code new RowType(asList(new RowField(...), ...))}. */
  private static Schema rowType(Field... fields) {
    return nullable(Schema.createStruct(Arrays.asList(fields)));
  }

  private static Field rowField(String name, Schema type) {
    return new Field(name, type, 0);
  }

  private static Field rowField(String name, Schema type, String description) {
    return new Field(name, type, 0, null, false, description, null, null);
  }

  // ---------------------------------------------------------------------------------------------
  // Cases where Flink's expectation is copied verbatim
  // ---------------------------------------------------------------------------------------------

  static Stream<Arguments> flinkAgreedTestData() {
    return Stream.of(
        Arguments.of(smallIntType(), bigIntType(), true),

        // nullability does not match
        Arguments.of(notNull(Schema.create(Schema.Type.SMALLINT)), smallIntType(), true),
        Arguments.of(smallIntType(), notNull(Schema.create(Schema.Type.SMALLINT)), false),

        // loss of precision
        Arguments.of(floatType(), intType(), false),
        Arguments.of(varCharMaxType(), floatType(), false),
        Arguments.of(floatType(), varCharMaxType(), false),
        Arguments.of(decimalType(3, 2), varCharMaxType(), false),

        Arguments.of(arrayType(intType()), arrayType(bigIntType()), true),
        Arguments.of(arrayType(intType()), arrayType(varCharMaxType()), false),

        Arguments.of(
            rowType(rowField("f1", intType()), rowField("f2", intType())),
            rowType(rowField("f1", intType()), rowField("f2", bigIntType())),
            true),
        Arguments.of(
            rowType(rowField("f1", intType(), "description"), rowField("f2", intType())),
            rowType(rowField("f1", intType()), rowField("f2", bigIntType())),
            true),
        Arguments.of(
            rowType(rowField("f1", intType()), rowField("f2", intType())),
            rowType(rowField("f1", intType()), rowField("f2", booleanType())),
            false),
        Arguments.of(
            rowType(rowField("f1", intType()), rowField("f2", intType())),
            varCharMaxType(),
            false),

        Arguments.of(timestampType(9), timestampType(9), true),
        Arguments.of(localZonedTimestampType(9), localZonedTimestampType(9), true),
        Arguments.of(timestampType(3), localZonedTimestampType(3), true),
        Arguments.of(localZonedTimestampType(3), timestampType(3), true),
        Arguments.of(timestampType(3), localZonedTimestampType(6), true),
        Arguments.of(localZonedTimestampType(3), timestampType(6), true),
        Arguments.of(notNull(Schema.createTimestamp(3)), localZonedTimestampType(6), true),
        Arguments.of(notNull(Schema.createTimestampLtz(3)), timestampType(6), true));
  }

  @ParameterizedTest(name = "{0} -> {1}")
  @MethodSource("flinkAgreedTestData")
  void agreesWithFlink(Schema sourceType, Schema targetType, boolean expectedCompatible) {
    assertVerdict(sourceType, targetType, expectedCompatible);
  }

  // ---------------------------------------------------------------------------------------------
  // Cases where this module deliberately answers differently
  // ---------------------------------------------------------------------------------------------

  /**
   * Every entry here is a parameter narrowing that Flink calls an implicit cast. Its table is keyed by
   * type root and never reads a precision or scale, so it cannot distinguish these from the widening
   * of the same roots. The parameter guards can, and refuse them.
   *
   * <p>These three are, in effect, Flink's own test suite documenting that
   * {@code supportsImplicitCast} is not a lossless relation.
   */
  static Stream<Arguments> flinkDivergentTestData() {
    return Stream.of(
        // Flink: true. INT needs 10 digits; DECIMAL(5,5) leaves none for the integer part.
        Arguments.of(intType(), decimalType(5, 5), false),

        // Flink: true. Both narrow the fractional-second precision from 6 to 3.
        Arguments.of(timestampType(6), localZonedTimestampType(3), false),
        Arguments.of(localZonedTimestampType(6), timestampType(3), false));
  }

  @ParameterizedTest(name = "{0} -> {1}")
  @MethodSource("flinkDivergentTestData")
  void divergesFromFlink(Schema sourceType, Schema targetType, boolean expectedCompatible) {
    assertVerdict(sourceType, targetType, expectedCompatible);
  }

  // ---------------------------------------------------------------------------------------------

  private static void assertVerdict(Schema sourceType, Schema targetType, boolean expected) {
    CompatibilityResult result = CompatibilityChecker.compare(
        Mode.FLINK, singleColumn(sourceType), singleColumn(targetType));
    assertEquals(expected, result.isCompatible(), result.describe());
  }

  private static LogicalType singleColumn(Schema type) {
    return new LogicalType(notNull(Schema.createStruct(
        Collections.singletonList(new Field("c", type, 0)))));
  }
}
