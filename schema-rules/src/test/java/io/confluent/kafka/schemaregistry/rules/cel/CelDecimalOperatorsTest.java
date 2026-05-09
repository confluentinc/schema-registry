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

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.protobuf.type.Decimal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Operator + arithmetic coverage for the {@code decimal_*} function family.
 * Builds a single proto schema once per test method and inlines the expression
 * as the rule body on a Decimal field. Rule passing → expression evaluates
 * true; rule violations → expression evaluates false.
 */
public class CelDecimalOperatorsTest {

  /**
   * Build a schema with {@code expr} as the rule body on a Decimal field, and
   * evaluate it against {@code amount}. Returns true iff the rule passes.
   */
  private static boolean eval(BigDecimal amount, String expr) {
    String escaped = expr.replace("\\", "\\\\").replace("\"", "\\\"");
    String schema = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message Order {\n"
        + "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"check\", expr: \"" + escaped + "\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema ps = new ProtobufSchema(schema);
    DynamicMessage msg = orderOf(ps, amount);
    List<ValidationRuleError> errors = ps.validateMessage(new CelValidator(), msg);
    if (!errors.isEmpty() && errors.get(0).getCause() != null) {
      // surface unexpected runtime failures (vs. clean rule violations)
      throw new AssertionError(
          "Rule errored for expr `" + expr + "`: " + errors.get(0).getCause(),
          errors.get(0).getCause());
    }
    return errors.isEmpty();
  }

  private static DynamicMessage orderOf(ProtobufSchema ps, BigDecimal amount) {
    try {
      Descriptor desc = ps.toDescriptor("test.Order");
      FieldDescriptor amountField = desc.findFieldByName("amount");
      BigInteger unscaled = amount.unscaledValue();
      Decimal d = Decimal.newBuilder()
          .setValue(ByteString.copyFrom(unscaled.toByteArray()))
          .setScale(amount.scale())
          .build();
      Descriptor decimalDesc = amountField.getMessageType();
      DynamicMessage payload = DynamicMessage.parseFrom(decimalDesc, d.toByteArray());
      return DynamicMessage.newBuilder(desc).setField(amountField, payload).build();
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  // -- comparison operators --

  @Test
  void decimalEq() {
    assertTrue(eval(new BigDecimal("5.00"),
        "decimal_eq(to_decimal(this), to_decimal(\"5.00\"))"));
    // numerically-equal-but-different-scale should compare equal (compareTo, not equals)
    assertTrue(eval(new BigDecimal("5"),
        "decimal_eq(to_decimal(this), to_decimal(\"5.000\"))"));
    assertFalse(eval(new BigDecimal("5"),
        "decimal_eq(to_decimal(this), to_decimal(\"6\"))"));
  }

  @Test
  void decimalNe() {
    assertTrue(eval(new BigDecimal("5"),
        "decimal_ne(to_decimal(this), to_decimal(\"6\"))"));
    assertFalse(eval(new BigDecimal("5"),
        "decimal_ne(to_decimal(this), to_decimal(\"5\"))"));
  }

  @Test
  void decimalLt() {
    assertTrue(eval(new BigDecimal("5"),
        "decimal_lt(to_decimal(this), to_decimal(\"10\"))"));
    assertFalse(eval(new BigDecimal("10"),
        "decimal_lt(to_decimal(this), to_decimal(\"10\"))"));
    assertFalse(eval(new BigDecimal("11"),
        "decimal_lt(to_decimal(this), to_decimal(\"10\"))"));
  }

  @Test
  void decimalLe() {
    assertTrue(eval(new BigDecimal("5"),
        "decimal_le(to_decimal(this), to_decimal(\"10\"))"));
    assertTrue(eval(new BigDecimal("10"),
        "decimal_le(to_decimal(this), to_decimal(\"10\"))"));
    assertFalse(eval(new BigDecimal("11"),
        "decimal_le(to_decimal(this), to_decimal(\"10\"))"));
  }

  @Test
  void decimalGt() {
    assertTrue(eval(new BigDecimal("11"),
        "decimal_gt(to_decimal(this), to_decimal(\"10\"))"));
    assertFalse(eval(new BigDecimal("10"),
        "decimal_gt(to_decimal(this), to_decimal(\"10\"))"));
  }

  @Test
  void decimalGe() {
    assertTrue(eval(new BigDecimal("10"),
        "decimal_ge(to_decimal(this), to_decimal(\"10\"))"));
    assertTrue(eval(new BigDecimal("11"),
        "decimal_ge(to_decimal(this), to_decimal(\"10\"))"));
    assertFalse(eval(new BigDecimal("9"),
        "decimal_ge(to_decimal(this), to_decimal(\"10\"))"));
  }

  // -- arithmetic --

  @Test
  void decimalAdd() {
    assertTrue(eval(new BigDecimal("1"),
        "decimal_eq(decimal_add(to_decimal(this), to_decimal(\"2\")), to_decimal(\"3\"))"));
  }

  @Test
  void decimalSub() {
    assertTrue(eval(new BigDecimal("10"),
        "decimal_eq(decimal_sub(to_decimal(this), to_decimal(\"3\")), to_decimal(\"7\"))"));
  }

  @Test
  void decimalMul() {
    assertTrue(eval(new BigDecimal("4"),
        "decimal_eq(decimal_mul(to_decimal(this), to_decimal(\"2.5\")), to_decimal(\"10.0\"))"));
  }

  @Test
  void decimalDiv() {
    assertTrue(eval(new BigDecimal("10"),
        "decimal_eq(decimal_div(to_decimal(this), to_decimal(\"2\")), to_decimal(\"5\"))"));
    // Non-terminating decimal — must not throw; uses MathContext(38, HALF_UP)
    // per implementation, matching Flink's MC_DIVIDE exactly. 1 / 3 = 0.333...
    // truncated/rounded at 38 significant digits.
    assertTrue(eval(new BigDecimal("1"),
        "decimal_lt(decimal_div(to_decimal(this), to_decimal(\"3\")), to_decimal(\"1\")) "
            + "&& decimal_gt(decimal_div(to_decimal(this), to_decimal(\"3\")), to_decimal(\"0\"))"));
    // Verify the 38-digit precision (Flink alignment). Pre-Flink alignment
    // this would have been 16 (DECIMAL64).
    assertTrue(eval(new BigDecimal("1"),
        "decimal_precision(decimal_div(to_decimal(this), to_decimal(\"3\"))) == 38"));
  }

  // -- unary --

  @Test
  void decimalNeg() {
    assertTrue(eval(new BigDecimal("5"),
        "decimal_eq(decimal_neg(to_decimal(this)), to_decimal(\"-5\"))"));
  }

  @Test
  void decimalAbs() {
    assertTrue(eval(new BigDecimal("-5"),
        "decimal_eq(decimal_abs(to_decimal(this)), to_decimal(\"5\"))"));
    assertTrue(eval(new BigDecimal("5"),
        "decimal_eq(decimal_abs(to_decimal(this)), to_decimal(\"5\"))"));
  }

  // -- introspection --

  @Test
  void decimalScale() {
    assertTrue(eval(new BigDecimal("100.50"),
        "decimal_scale(to_decimal(this)) == 2"));
    assertTrue(eval(new BigDecimal("100"),
        "decimal_scale(to_decimal(this)) == 0"));
  }

  @Test
  void decimalPrecision() {
    // BigDecimal.precision() = number of significant digits in unscaled value
    assertTrue(eval(new BigDecimal("100.50"),
        "decimal_precision(to_decimal(this)) == 5"));
    assertTrue(eval(new BigDecimal("0"),
        "decimal_precision(to_decimal(this)) == 1"));
  }

  @Test
  void decimalToString() {
    assertTrue(eval(new BigDecimal("100.50"),
        "decimal_to_string(to_decimal(this)) == \"100.50\""));
    assertTrue(eval(new BigDecimal("-1.5"),
        "decimal_to_string(to_decimal(this)) == \"-1.5\""));
  }

  // -- Flink-aligned rounding family --

  @Test
  void decimalRound_oneArg_halfUp() {
    // 2.5 → 3 (HALF_UP, away from zero); -2.5 → -3
    assertTrue(eval(new BigDecimal("2.5"),
        "decimal_eq(decimal_round(to_decimal(this)), to_decimal(\"3\"))"));
    assertTrue(eval(new BigDecimal("-2.5"),
        "decimal_eq(decimal_round(to_decimal(this)), to_decimal(\"-3\"))"));
    assertTrue(eval(new BigDecimal("2.4"),
        "decimal_eq(decimal_round(to_decimal(this)), to_decimal(\"2\"))"));
  }

  @Test
  void decimalRound_twoArg_specificScale() {
    // PostgreSQL example: round(42.4382, 2) → 42.44
    assertTrue(eval(new BigDecimal("42.4382"),
        "decimal_eq(decimal_round(to_decimal(this), 2), to_decimal(\"42.44\"))"));
    // negative scale rounds left of decimal point: round(1234.56, -1) → 1230
    assertTrue(eval(new BigDecimal("1234.56"),
        "decimal_eq(decimal_round(to_decimal(this), -1), to_decimal(\"1230\"))"));
  }

  @Test
  void decimalTruncate_oneArg_towardZero() {
    // Truncate (RoundingMode.DOWN) is toward zero, distinct from floor (toward -∞)
    assertTrue(eval(new BigDecimal("42.8"),
        "decimal_eq(decimal_truncate(to_decimal(this)), to_decimal(\"42\"))"));
    assertTrue(eval(new BigDecimal("-42.8"),
        "decimal_eq(decimal_truncate(to_decimal(this)), to_decimal(\"-42\"))"));
  }

  @Test
  void decimalTruncate_twoArg_specificScale() {
    // Flink example: 42.4382.truncate(2) → 42.43
    assertTrue(eval(new BigDecimal("42.4382"),
        "decimal_eq(decimal_truncate(to_decimal(this), 2), to_decimal(\"42.43\"))"));
  }

  @Test
  void decimalFloor() {
    // FLOOR: toward -∞. -2.5 → -3, 2.5 → 2
    assertTrue(eval(new BigDecimal("2.5"),
        "decimal_eq(decimal_floor(to_decimal(this)), to_decimal(\"2\"))"));
    assertTrue(eval(new BigDecimal("-2.5"),
        "decimal_eq(decimal_floor(to_decimal(this)), to_decimal(\"-3\"))"));
  }

  @Test
  void decimalCeil() {
    // CEILING: toward +∞. 2.5 → 3, -2.5 → -2
    assertTrue(eval(new BigDecimal("2.5"),
        "decimal_eq(decimal_ceil(to_decimal(this)), to_decimal(\"3\"))"));
    assertTrue(eval(new BigDecimal("-2.5"),
        "decimal_eq(decimal_ceil(to_decimal(this)), to_decimal(\"-2\"))"));
  }

  @Test
  void decimalSign() {
    assertTrue(eval(new BigDecimal("5"),
        "decimal_sign(to_decimal(this)) == 1"));
    assertTrue(eval(new BigDecimal("0"),
        "decimal_sign(to_decimal(this)) == 0"));
    assertTrue(eval(new BigDecimal("-5"),
        "decimal_sign(to_decimal(this)) == -1"));
  }

  // -- chained / composite --

  @Test
  void chainedExpressions() {
    // (this + 10) * 2 == 30 when this == 5
    assertTrue(eval(new BigDecimal("5"),
        "decimal_eq("
            + "  decimal_mul(decimal_add(to_decimal(this), to_decimal(\"10\")), to_decimal(\"2\")),"
            + "  to_decimal(\"30\"))"));
  }
}
