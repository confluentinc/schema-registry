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
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.type.logical.check.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.check.Invalidity.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tests for {@link ValidityChecker}.
 *
 * <p>Every case here is a schema that is wrong on its own terms, so none of it is reachable from
 * {@link CompatibilityChecker} — which needs a pair, and which therefore lets all of it through on a
 * first registration. Where a rule moved out of the comparison, the corresponding compatibility test
 * now asserts that the comparison stays silent, and the rejection is asserted here instead.
 */
class ValidityCheckerTest {

  // -----------------------------------------------------------------------------------------------
  // Decimal precision and scale
  // -----------------------------------------------------------------------------------------------

  @Test
  void decimalPrecisionAboveThirtyEightIsRejectedByEveryConsumer() {
    // The bound Flink's DecimalType and Iceberg's decimal agree on. Avro caps precision only at
    // what the underlying fixed can hold, so 40 really does arrive from registered schemas.
    for (Mode mode : Mode.values()) {
      assertThat(rulesOf(mode, col(Schema.createDecimal(40, 2))))
          .containsExactly(Rule.PRECISION_OUT_OF_RANGE);
    }
  }

  @Test
  void decimalPrecisionOfThirtyEightIsAccepted() {
    for (Mode mode : Mode.values()) {
      assertThat(ValidityChecker.validate(mode, col(Schema.createDecimal(38, 2))).isValid())
          .isTrue();
    }
  }

  @Test
  void decimalPrecisionOfZeroIsRejected() {
    assertThat(rulesOf(Mode.FLINK, col(Schema.createDecimal(0, 0))))
        .containsExactly(Rule.PRECISION_OUT_OF_RANGE);
  }

  @Test
  void decimalScaleAboveThePrecisionIsRejected() {
    assertThat(rulesOf(Mode.FLINK, col(Schema.createDecimal(5, 7))))
        .containsExactly(Rule.SCALE_OUT_OF_RANGE);
  }

  @Test
  void anOmittedDecimalScaleIsAccepted() {
    // NO_PARAM is SRLT's encoding of "scale omitted", and it round-trips through DDL as
    // DECIMAL(p), which SQL reads as DECIMAL(p, 0). Rejecting it would reject valid DDL.
    assertThat(Schema.createDecimal(10, Schema.NO_PARAM).toDdl()).isEqualTo("DECIMAL(10)");
    assertThat(ValidityChecker
        .validate(Mode.FLINK, col(Schema.createDecimal(10, Schema.NO_PARAM))).isValid()).isTrue();
  }

  @Test
  void aDecimalScaleBelowTheSentinelIsRejected() {
    assertThat(rulesOf(Mode.FLINK, col(Schema.createDecimal(5, -2))))
        .containsExactly(Rule.SCALE_OUT_OF_RANGE);
  }

  @Test
  void aDecimalCanBreakBothRulesAtOnce() {
    // Report-all: the scale is checked against the precision as written, not against the cap, so a
    // schema wrong in both ways says so once for each.
    assertThat(rulesOf(Mode.FLINK, col(Schema.createDecimal(40, 41))))
        .containsExactly(Rule.PRECISION_OUT_OF_RANGE, Rule.SCALE_OUT_OF_RANGE);
  }

  // -----------------------------------------------------------------------------------------------
  // Fractional-second precision
  // -----------------------------------------------------------------------------------------------

  @Test
  void outOfRangeTemporalPrecisionCannotBeConstructedSoIsNotARuleHere() {
    // Schema pins fractional-second precision to 0..9 at construction, for all three temporal
    // types, so the checker has nothing left to say about it.
    assertThatThrownBy(() -> Schema.createTimestamp(10))
        .isInstanceOf(ValidationException.class).hasMessageContaining("[0, 9]");
    assertThatThrownBy(() -> Schema.createTimestampLtz(10))
        .isInstanceOf(ValidationException.class).hasMessageContaining("[0, 9]");
    assertThatThrownBy(() -> Schema.createTime(10))
        .isInstanceOf(ValidationException.class).hasMessageContaining("[0, 9]");
  }

  @Test
  void timePrecisionAboveThreeIsAcceptedBecauseTheConversionRetypesItRatherThanFailing() {
    // The conversion to a Flink type maps TIME(p>3) to BIGINT, because Flink carries TIME as an int
    // millis-of-day. Lossy, but deliberate, and not this checker's to override.
    for (Mode mode : Mode.values()) {
      assertThat(ValidityChecker.validate(mode, col(Schema.createTime(9))).isValid()).isTrue();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Iceberg v2 representability -- the only mode-specific rules
  // -----------------------------------------------------------------------------------------------

  @Test
  void subMicrosecondTimestampsAreUnrepresentableAtIcebergV2Only() {
    LogicalType nanos = col(Schema.createTimestamp(9));
    assertThat(rulesOf(Mode.ICEBERG_V2, nanos)).containsExactly(Rule.UNREPRESENTABLE_TYPE);
    assertThat(ValidityChecker.validate(Mode.ICEBERG_V3, nanos).isValid()).isTrue();
    assertThat(ValidityChecker.validate(Mode.FLINK, nanos).isValid()).isTrue();
  }

  @Test
  void subMicrosecondTimestampLtzIsUnrepresentableAtIcebergV2Only() {
    LogicalType nanos = col(Schema.createTimestampLtz(9));
    assertThat(rulesOf(Mode.ICEBERG_V2, nanos)).containsExactly(Rule.UNREPRESENTABLE_TYPE);
    assertThat(ValidityChecker.validate(Mode.ICEBERG_V3, nanos).isValid()).isTrue();
  }

  @Test
  void microsecondTimestampsAreRepresentableEverywhere() {
    for (Mode mode : Mode.values()) {
      assertThat(ValidityChecker.validate(mode, col(Schema.createTimestamp(6))).isValid()).isTrue();
    }
  }

  @Test
  void variantIsUnrepresentableAtIcebergV2Only() {
    LogicalType variant = col(type(Schema.Type.VARIANT));
    assertThat(rulesOf(Mode.ICEBERG_V2, variant)).containsExactly(Rule.UNREPRESENTABLE_TYPE);
    assertThat(ValidityChecker.validate(Mode.ICEBERG_V3, variant).isValid()).isTrue();
    assertThat(ValidityChecker.validate(Mode.FLINK, variant).isValid()).isTrue();
  }

  @Test
  void multisetIsAcceptedByEveryConsumer() {
    // Iceberg's own FlinkTypeToType maps a multiset to map<T, int>, so it is representable. A
    // rejection elsewhere in the stack is policy, not a limit of the type system.
    LogicalType multiset = col(Schema.createMultiset(nonNull(Schema.createString())));
    for (Mode mode : Mode.values()) {
      assertThat(ValidityChecker.validate(mode, multiset).isValid()).isTrue();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // String and binary length
  // -----------------------------------------------------------------------------------------------

  @Test
  void zeroLengthCharIsRejected() {
    assertThat(rulesOf(Mode.FLINK, col(Schema.createChar(0))))
        .containsExactly(Rule.LENGTH_OUT_OF_RANGE);
  }

  @Test
  void zeroLengthBinaryIsRejected() {
    assertThat(rulesOf(Mode.FLINK, col(Schema.createBinary(0))))
        .containsExactly(Rule.LENGTH_OUT_OF_RANGE);
  }

  @Test
  void zeroLengthVarcharIsAcceptedBecauseZeroMeansUnbounded() {
    // The conversion reads a length of 0 on the unbounded types as "no bound declared" and widens it
    // to the maximum, so zero is a legal encoding there and only a negative length is wrong.
    assertThat(ValidityChecker.validate(Mode.FLINK, col(Schema.createVarchar(0))).isValid())
        .isTrue();
    assertThat(ValidityChecker.validate(Mode.FLINK, col(Schema.createVarbinary(0))).isValid())
        .isTrue();
  }

  @Test
  void aLengthOfMinusOneIsTheUnspecifiedSentinelAndIsCoercedAway() {
    // NO_PARAM, which the factory turns into the default length, so it never reaches the checker.
    assertThat(Schema.createChar(-1).getLength()).isEqualTo(1);
    assertThat(ValidityChecker.validate(Mode.FLINK, col(Schema.createChar(-1))).isValid()).isTrue();
  }

  @Test
  void aLengthBelowTheSentinelIsRejected() {
    assertThat(rulesOf(Mode.FLINK, col(Schema.createVarchar(-2))))
        .containsExactly(Rule.LENGTH_OUT_OF_RANGE);
  }

  @Test
  void lengthOfOneIsAccepted() {
    assertThat(ValidityChecker.validate(Mode.FLINK, col(Schema.createChar(1))).isValid()).isTrue();
    assertThat(ValidityChecker.validate(Mode.FLINK, col(Schema.createBinary(1))).isValid()).isTrue();
  }

  // -----------------------------------------------------------------------------------------------
  // Struct and union shape
  // -----------------------------------------------------------------------------------------------

  @Test
  void anEmptyStructIsRejected() {
    LogicalType empty = new LogicalType(nonNull(Schema.createStruct(Collections.emptyList())));
    for (Mode mode : Mode.values()) {
      assertThat(rulesOf(mode, empty)).containsExactly(Rule.EMPTY_STRUCT);
    }
  }

  @Test
  void anEmptyNestedStructIsRejectedAtItsPath() {
    LogicalType type = schema(
        required("outer", nonNull(Schema.createStruct(Collections.emptyList()))));
    assertThat(ValidityChecker.validate(Mode.FLINK, type).getInvalidities())
        .singleElement()
        .satisfies(i -> {
          assertThat(i.getRule()).isEqualTo(Rule.EMPTY_STRUCT);
          assertThat(i.getPath()).isEqualTo("outer");
        });
  }

  @Test
  void anEmptyUnionIsRejected() {
    LogicalType empty = col(nonNull(Schema.createUnion(Collections.emptyList())));
    assertThat(rulesOf(Mode.FLINK, empty)).containsExactly(Rule.EMPTY_STRUCT);
  }

  @Test
  void duplicateFieldNamesCannotBeConstructedSoAreNotARuleHere() {
    // Schema rejects these itself, which is why the checker has no rule for them: a finding could
    // never be produced, and a test for it could never be written.
    assertThatThrownBy(() -> Schema.createStruct(Arrays.asList(
        required("a", type(Schema.Type.INT)),
        new Field("a", nonNull(type(Schema.Type.BIGINT)), 1))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Duplicate field");
  }

  @Test
  void duplicateUnionBranchNamesCannotBeConstructedEither() {
    assertThatThrownBy(() -> Schema.createUnion(Arrays.asList(
        new UnionBranch("b", nonNull(type(Schema.Type.INT))),
        new UnionBranch("b", nonNull(type(Schema.Type.BIGINT))))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Duplicate union branch");
  }

  @Test
  void blankFieldNamesCannotBeConstructedSoAreNotARuleHere() {
    assertThatThrownBy(() -> new Field("", nonNull(type(Schema.Type.INT)), 0))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> new Field("   ", nonNull(type(Schema.Type.INT)), 0))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void fieldNamesDifferingOnlyInCaseAreAccepted() {
    // The duplicate check is case-sensitive, so these really are two distinct columns.
    LogicalType type = schema(
        required("a", type(Schema.Type.INT)),
        new Field("A", nonNull(type(Schema.Type.INT)), 1));
    assertThat(ValidityChecker.validate(Mode.FLINK, type).isValid()).isTrue();
  }

  // -----------------------------------------------------------------------------------------------
  // Named types
  // -----------------------------------------------------------------------------------------------

  @Test
  void aSelfReferencingNamedTypeIsRejected() {
    // Neither Flink nor Iceberg has a recursive type, so this cannot be inlined at all.
    Map<String, Schema> named = new LinkedHashMap<>();
    named.put("Node", nonNull(Schema.createStruct(Arrays.asList(
        required("value", type(Schema.Type.INT)),
        new Field("next", Schema.createNamedTypeRef("Node").setNullable(true), 1)))));
    LogicalType type = new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            required("root", Schema.createNamedTypeRef("Node"))))),
        named);

    for (Mode mode : Mode.values()) {
      assertThat(rulesOf(mode, type)).containsExactly(Rule.CYCLIC_TYPE);
    }
  }

  @Test
  void aMutuallyRecursivePairIsRejected() {
    Map<String, Schema> named = new LinkedHashMap<>();
    named.put("A", nonNull(Schema.createStruct(Collections.singletonList(
        new Field("b", Schema.createNamedTypeRef("B").setNullable(true), 0)))));
    named.put("B", nonNull(Schema.createStruct(Collections.singletonList(
        new Field("a", Schema.createNamedTypeRef("A").setNullable(true), 0)))));
    LogicalType type = new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            required("root", Schema.createNamedTypeRef("A"))))),
        named);

    assertThat(rulesOf(Mode.FLINK, type)).containsExactly(Rule.CYCLIC_TYPE);
  }

  @Test
  void anUnresolvedNamedTypeReferenceIsRejected() {
    LogicalType type = schema(required("root", Schema.createNamedTypeRef("Missing")));
    assertThat(rulesOf(Mode.FLINK, type)).containsExactly(Rule.UNRESOLVED_TYPE_REF);
  }

  @Test
  void aValidNamedTypeIsAccepted() {
    Map<String, Schema> named = new LinkedHashMap<>();
    named.put("Point", nonNull(Schema.createStruct(Arrays.asList(
        required("x", type(Schema.Type.INT)),
        new Field("y", nonNull(type(Schema.Type.INT)), 1)))));
    LogicalType type = new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            required("p", Schema.createNamedTypeRef("Point"))))),
        named);

    assertThat(ValidityChecker.validate(Mode.FLINK, type).isValid()).isTrue();
  }

  @Test
  void aProblemInsideANamedTypeIsFoundThroughTheReference() {
    Map<String, Schema> named = new LinkedHashMap<>();
    named.put("Money", nonNull(Schema.createStruct(Collections.singletonList(
        required("amount", Schema.createDecimal(40, 2))))));
    LogicalType type = new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            required("m", Schema.createNamedTypeRef("Money"))))),
        named);

    assertThat(ValidityChecker.validate(Mode.FLINK, type).getInvalidities())
        .singleElement()
        .satisfies(i -> {
          assertThat(i.getRule()).isEqualTo(Rule.PRECISION_OUT_OF_RANGE);
          assertThat(i.getPath()).isEqualTo("m.amount");
        });
  }

  @Test
  void aNamedTypeUsedTwiceIsReportedOnce() {
    // Walking a shared named type once keeps one bad definition from producing a finding per use.
    Map<String, Schema> named = new LinkedHashMap<>();
    named.put("Money", nonNull(Schema.createStruct(Collections.singletonList(
        required("amount", Schema.createDecimal(40, 2))))));
    LogicalType type = new LogicalType(
        nonNull(Schema.createStruct(Arrays.asList(
            required("paid", Schema.createNamedTypeRef("Money")),
            new Field("owed", nonNull(Schema.createNamedTypeRef("Money")), 1)))),
        named);

    assertThat(rulesOf(Mode.FLINK, type)).containsExactly(Rule.PRECISION_OUT_OF_RANGE);
  }

  @Test
  void anUnreferencedNamedTypeIsNotWalked() {
    // Nothing inlines it, so its contents cannot reach a consumer.
    Map<String, Schema> named = new LinkedHashMap<>();
    named.put("Unused", nonNull(Schema.createStruct(Collections.singletonList(
        required("amount", Schema.createDecimal(40, 2))))));
    LogicalType type = new LogicalType(
        nonNull(Schema.createStruct(Collections.singletonList(
            required("kept", type(Schema.Type.INT))))),
        named);

    assertThat(ValidityChecker.validate(Mode.FLINK, type).isValid()).isTrue();
  }

  // -----------------------------------------------------------------------------------------------
  // Traversal and paths
  // -----------------------------------------------------------------------------------------------

  @Test
  void aProblemInsideAnArrayElementIsFoundAtTheElementPath() {
    LogicalType type = schema(
        required("items", Schema.createArray(nonNull(Schema.createDecimal(40, 2)))));
    assertThat(pathsOf(Mode.FLINK, type)).containsExactly("items[]");
  }

  @Test
  void aProblemInsideAMapValueIsFoundAtTheValuePath() {
    LogicalType type = schema(required("m", Schema.createMap(
        nonNull(Schema.createString()), nonNull(Schema.createDecimal(40, 2)))));
    assertThat(pathsOf(Mode.FLINK, type)).containsExactly("m{}");
  }

  @Test
  void aProblemInsideAMapKeyIsFoundAtTheKeyPath() {
    LogicalType type = schema(required("m", Schema.createMap(
        nonNull(Schema.createChar(0)), nonNull(type(Schema.Type.INT)))));
    assertThat(pathsOf(Mode.FLINK, type)).containsExactly("m{key}");
  }

  @Test
  void aProblemInsideAMultisetElementIsFound() {
    LogicalType type = schema(
        required("s", Schema.createMultiset(nonNull(Schema.createDecimal(40, 2)))));
    assertThat(pathsOf(Mode.FLINK, type)).containsExactly("s[]");
  }

  @Test
  void nestedPathsAreDotJoined() {
    LogicalType type = schema(required("order", nonNull(Schema.createStruct(
        Collections.singletonList(required("items", Schema.createArray(
            nonNull(Schema.createStruct(Collections.singletonList(
                required("price", Schema.createDecimal(40, 2))))))))))));
    assertThat(pathsOf(Mode.FLINK, type)).containsExactly("order.items[].price");
  }

  @Test
  void everyProblemIsReportedRatherThanJustTheFirst() {
    LogicalType type = schema(
        required("a", Schema.createDecimal(40, 2)),
        new Field("b", nonNull(Schema.createChar(0)), 1),
        new Field("c", nonNull(Schema.createDecimal(4, 9)), 2));
    assertThat(rulesOf(Mode.FLINK, type)).containsExactly(
        Rule.PRECISION_OUT_OF_RANGE, Rule.LENGTH_OUT_OF_RANGE, Rule.SCALE_OUT_OF_RANGE);
  }

  // -----------------------------------------------------------------------------------------------
  // Contract
  // -----------------------------------------------------------------------------------------------

  @Test
  void aWellFormedSchemaIsValidUnderEveryMode() {
    LogicalType type = schema(
        required("id", type(Schema.Type.BIGINT)),
        new Field("name", Schema.createVarchar(0).setNullable(true), 1),
        new Field("price", nonNull(Schema.createDecimal(10, 2)), 2),
        new Field("at", nonNull(Schema.createTimestamp(3)), 3));
    for (Mode mode : Mode.values()) {
      assertThat(ValidityChecker.validate(mode, type).isValid()).isTrue();
    }
  }

  @Test
  void aScalarRootIsAccepted() {
    // Whether a table may have a scalar at its root is a question about tables, not about types.
    assertThat(ValidityChecker.validate(Mode.FLINK, new LogicalType(nonNull(type(Schema.Type.INT))))
        .isValid()).isTrue();
  }

  @Test
  void anEnumWithNoSymbolsIsAccepted() {
    // It derives to an unbounded VARCHAR, which no consumer objects to.
    LogicalType type = col(nonNull(Schema.createEnum(Collections.emptyList())));
    assertThat(ValidityChecker.validate(Mode.FLINK, type).isValid()).isTrue();
  }

  @Test
  void nullArgumentsAreRejected() {
    assertThatThrownBy(() -> ValidityChecker.validate(null, col(type(Schema.Type.INT))))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> ValidityChecker.validate(Mode.FLINK, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void aValidResultDescribesItselfAsEmpty() {
    ValidityResult result = ValidityChecker.validate(Mode.FLINK, col(type(Schema.Type.INT)));
    assertThat(result.isValid()).isTrue();
    assertThat(result.getInvalidities()).isEmpty();
    assertThat(result.describe()).isEmpty();
  }

  @Test
  void anInvalidResultNamesTheRuleAndThePath() {
    ValidityResult result = ValidityChecker.validate(
        Mode.FLINK, schema(required("d", Schema.createDecimal(40, 2))));
    assertThat(result.isValid()).isFalse();
    assertThat(result.describe()).contains("PRECISION_OUT_OF_RANGE", "'d'", "40");
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  private static List<Rule> rulesOf(Mode mode, LogicalType type) {
    return ValidityChecker.validate(mode, type).getInvalidities().stream()
        .map(Invalidity::getRule)
        .collect(Collectors.toList());
  }

  private static List<String> pathsOf(Mode mode, LogicalType type) {
    return ValidityChecker.validate(mode, type).getInvalidities().stream()
        .map(Invalidity::getPath)
        .collect(Collectors.toList());
  }

  private static Schema nonNull(Schema schema) {
    return schema.setNullable(false);
  }

  private static Field required(String name, Schema type) {
    return new Field(name, nonNull(type), 0);
  }

  private static LogicalType schema(Field... fields) {
    return new LogicalType(nonNull(Schema.createStruct(Arrays.asList(fields))));
  }

  private static LogicalType col(Schema type) {
    return schema(required("c", type));
  }

  private static Schema type(Schema.Type t) {
    return Schema.create(t);
  }
}
