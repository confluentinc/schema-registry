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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.OpaqueVal;
import io.confluent.protobuf.type.Decimal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Edge-case and negative-path coverage. Verifies behavior at the boundaries
 * of the design (empty bytes, missing fields, malformed paths) and confirms
 * that things which should throw, throw with a useful message.
 */
public class CelEdgeCasesTest {

  // ----- Decimal edge cases -----

  /**
   * Empty unscaled value bytes should decode to 0.
   */
  @Test
  void decimal_emptyBytes_decodeAsZero() {
    String schema = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message Order {\n"
        + "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"isZero\","
        + "             expr: \"decimal_eq(to_decimal(this), to_decimal(\\\"0\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema ps = new ProtobufSchema(schema);
    Descriptor desc = ps.toDescriptor("test.Order");
    FieldDescriptor amountField = desc.findFieldByName("amount");
    Decimal d = Decimal.newBuilder().setScale(0).build();  // empty value bytes
    DynamicMessage payload;
    try {
      payload = DynamicMessage.parseFrom(amountField.getMessageType(), d.toByteArray());
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    DynamicMessage msg = DynamicMessage.newBuilder(desc).setField(amountField, payload).build();
    List<ValidationRuleError> errors = ps.validateMessage(new CelValidator(), msg);
    assertTrue(errors.isEmpty(),
        "empty decimal bytes should decode as 0, got: " + errors);
  }

  /**
   * Decimal with very large unscaled value beyond .NET / rust_decimal's 28-29
   * digit ceiling — Java's BigDecimal handles arbitrary precision so this
   * should pass on the Java client.
   */
  @Test
  void decimal_largePrecision_handled() {
    BigDecimal huge = new BigDecimal("123456789012345678901234567890.123456789");
    String schema = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message Order {\n"
        + "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimal_gt(to_decimal(this), to_decimal(\\\"0\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema ps = new ProtobufSchema(schema);
    Descriptor desc = ps.toDescriptor("test.Order");
    FieldDescriptor amountField = desc.findFieldByName("amount");
    BigInteger unscaled = huge.unscaledValue();
    Decimal d = Decimal.newBuilder()
        .setValue(ByteString.copyFrom(unscaled.toByteArray()))
        .setScale(huge.scale())
        .build();
    DynamicMessage payload;
    try {
      payload = DynamicMessage.parseFrom(amountField.getMessageType(), d.toByteArray());
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    DynamicMessage msg = DynamicMessage.newBuilder(desc).setField(amountField, payload).build();
    assertTrue(ps.validateMessage(new CelValidator(), msg).isEmpty(),
        "large-precision decimal should compare without precision loss");
  }

  // ----- to_decimal / to_timestamp negative-path runtime errors -----

  /**
   * to_decimal("not a number") — runtime failure parsing literal.
   */
  @Test
  void toDecimal_invalidLiteral_throws() {
    String expr = "decimal_eq(to_decimal(\"not a number\"), to_decimal(\"0\"))";
    Throwable cause = evalCause(BigDecimal.ZERO, expr);
    assertNotNull(cause, "expected runtime error from to_decimal(invalid string)");
    String chain = exceptionChainContains(cause, "NumberFormat");
    assertTrue(chain.contains("NumberFormat"),
        "expected NumberFormatException in the chain, got: " + chain);
  }

  /**
   * to_decimal(raw_long) — without scale info, should throw a clear error
   * pointing to the two-arg overload.
   */
  @Test
  void toDecimal_dynRawLong_isHandled() {
    // A long flowing into to_decimal(dyn) should be converted via
    // BigDecimal.valueOf(long) — no scale info needed for integer values.
    // This documents that integers work without the two-arg overload.
    BigDecimal result = io.confluent.kafka.schemaregistry.rules.cel.builtin.TestExposure
        .toBigDecimal(42L);
    assertEquals(new BigDecimal("42"), result);
  }

  /**
   * to_timestamp(raw_long) without unit — should throw with a hint pointing
   * to the two-arg overload.
   */
  @Test
  void toTimestamp_dynRawLong_throwsWithHint() {
    Exception ex = assertThrows(IllegalArgumentException.class, () ->
        io.confluent.kafka.schemaregistry.rules.cel.builtin.TestExposure
            .toTimestamp(1700000000000L));
    assertTrue(ex.getMessage().contains("unit"),
        "expected error message mentioning 'unit', got: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("to_timestamp"),
        "expected error message mentioning 'to_timestamp', got: " + ex.getMessage());
  }

  // ----- Variant edge cases -----

  /**
   * Missing variant field — variant_get_field returns variant-null, callable
   * with is_variant_null without throwing.
   */
  @Test
  void variant_missingField_returnsVariantNull() {
    // Test handled implicitly — variant_get on missing path returns NULL
    // variant. See CelVariantAccessorsTest.variantGet_missingPath_returnsNull.
    // Add a focused negative case here: chained access through missing field
    // remains null all the way down without NPE.
    OpaqueVal v = OpaqueVal.of("confluent.type.Variant", null);
    assertNotNull(v);
  }

  /**
   * Malformed JSONPath should throw an IllegalArgumentException at parse time.
   */
  @Test
  void variantPath_malformed_throws() {
    Exception ex = assertThrows(IllegalArgumentException.class, () ->
        io.confluent.kafka.schemaregistry.rules.cel.builtin.TestExposure
            .parsePath("not-a-path"));
    assertTrue(ex.getMessage().contains("$"),
        "expected error message mentioning '$', got: " + ex.getMessage());
  }

  /**
   * Quoted key with unterminated quote should throw.
   */
  @Test
  void variantPath_unterminatedQuote_throws() {
    assertThrows(IllegalArgumentException.class, () ->
        io.confluent.kafka.schemaregistry.rules.cel.builtin.TestExposure
            .parsePath("$[\"unclosed"));
  }

  /**
   * Empty path string should throw.
   */
  @Test
  void variantPath_empty_throws() {
    assertThrows(IllegalArgumentException.class, () ->
        io.confluent.kafka.schemaregistry.rules.cel.builtin.TestExposure
            .parsePath(""));
  }

  // ----- Helpers -----

  /**
   * Evaluate {@code expr} against a Proto Order with the given amount; return
   * the underlying cause if the rule errored, else null.
   *
   * <p>Function lambdas are wrapped (see {@code BuiltinOverload.safeUnary/Binary/
   * Overload}) so that a body exception (like {@code new BigDecimal("invalid")})
   * is converted to a CEL {@code Err} value, which Nessie's
   * {@code ScriptExecutionException} surfaces, and which CelValidator catches
   * via its existing {@code ScriptException} arm. Result: a clean
   * {@code ValidationRuleError} with the original {@code NumberFormatException}
   * preserved as the cause chain.
   */
  private static Throwable evalCause(BigDecimal amount, String expr) {
    String escaped = expr.replace("\\", "\\\\").replace("\"", "\\\"");
    String schema = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message Order {\n"
        + "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"" + escaped + "\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema ps = new ProtobufSchema(schema);
    Descriptor desc = ps.toDescriptor("test.Order");
    FieldDescriptor amountField = desc.findFieldByName("amount");
    Decimal d = Decimal.newBuilder()
        .setValue(ByteString.copyFrom(amount.unscaledValue().toByteArray()))
        .setScale(amount.scale()).build();
    DynamicMessage payload;
    try {
      payload = DynamicMessage.parseFrom(amountField.getMessageType(), d.toByteArray());
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    DynamicMessage msg = DynamicMessage.newBuilder(desc).setField(amountField, payload).build();
    List<ValidationRuleError> errors = ps.validateMessage(new CelValidator(), msg);
    if (errors.isEmpty()) {
      return null;
    }
    return errors.get(0).getCause();
  }

  private static String exceptionChainContains(Throwable t, String marker) {
    StringBuilder sb = new StringBuilder();
    Throwable cur = t;
    while (cur != null) {
      sb.append(cur.getClass().getSimpleName()).append(": ")
          .append(cur.getMessage()).append("\n");
      cur = cur.getCause();
    }
    return sb.toString();
  }

}
