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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantBuilder;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for CEL {@code ==}/{@code !=} on {@code confluent.type.Decimal} values.
 *
 * <p>The {@code ==} operator must agree with {@code decimals.eq} / {@link BigDecimal#compareTo}:
 * numeric equality that is scale-insensitive ({@code 2.0 == 2.00}). Decimal values used to travel
 * through CEL as a plain {@link BigDecimal}, and cel-java's stdlib {@code equals} binding routes
 * two {@link Number}s through {@code ComparisonFunctions.numericEquals}, which knows only
 * Double / Long / UnsignedLong and answers {@code false} for any pair of BigDecimals — so even
 * {@code decimal("2.0") == decimal("2.0")} was false. The runtime value is now a
 * {@link io.confluent.kafka.schemaregistry.rules.cel.builtin.CelDecimal} wrapper, deliberately
 * not a {@link Number}, whose {@code equals} is {@code compareTo(...) == 0} — so the stock stdlib
 * bindings answer numerically. See {@link CelDecimalEqualityTest} for the {@code in} /
 * container / nested cases that a binding-level override could not have reached.
 *
 * <p>Most rules here are field-level CONDITIONS on a {@code Money.amount} Decimal field. A
 * satisfied rule yields no {@link ValidationRuleError}; an unsatisfied rule yields exactly one.
 * The Variant section instead puts the decimal inside a {@code confluent.type.Variant} field, to
 * cover the {@code variants.* } + {@code decimals.*} + {@code ==} composition.
 */
public class CelValidatorDecimalEqualityTest {

  /** Build a Money schema whose {@code amount} field carries the given rule expression. */
  private static ProtobufSchema schemaWithRule(String expr) {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message Money {\n"
        + "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"eq\", expr: \"" + expr + "\"}]\n"
        + "  }];\n"
        + "}\n";
    return new ProtobufSchema(s);
  }

  /** Build a Money message with the given decimal amount, encoded as a proto Decimal. */
  private static DynamicMessage money(ProtobufSchema schema, String amount) {
    Descriptor moneyDesc = schema.toDescriptor("test.Money");
    FieldDescriptor amountField = moneyDesc.findFieldByName("amount");
    Descriptor decimalDesc = amountField.getMessageType();

    BigDecimal bd = new BigDecimal(amount);
    BigInteger unscaled = bd.unscaledValue();

    DynamicMessage decimalMsg = DynamicMessage.newBuilder(decimalDesc)
        .setField(decimalDesc.findFieldByName("value"),
            ByteString.copyFrom(unscaled.toByteArray()))
        .setField(decimalDesc.findFieldByName("scale"), bd.scale())
        .build();

    return DynamicMessage.newBuilder(moneyDesc)
        .setField(amountField, decimalMsg)
        .build();
  }

  /** True iff the rule expr holds for a Money whose amount is {@code amount}. */
  private static boolean holds(String expr, String amount) {
    ProtobufSchema schema = schemaWithRule(expr);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), money(schema, amount));
    return errors.isEmpty();
  }

  // ---- TARGET CONTRACT: == is numeric, scale-insensitive ----

  @Test
  void eq_sameValueDifferentScale_isTrue() {
    // decimal("2.0") == decimal("2.00") -> TRUE
    assertTrue(holds("decimal(this) == decimal(\\\"2.00\\\")", "2.0"),
        "2.0 == 2.00 should be true (scale-insensitive numeric equality)");
  }

  @Test
  void eq_sameValueSameScale_isTrue() {
    // decimal("2.0") == decimal("2.0") -> TRUE (was false under identity equality)
    assertTrue(holds("decimal(this) == decimal(\\\"2.0\\\")", "2.0"),
        "2.0 == 2.0 should be true");
  }

  @Test
  void eq_differentValue_isFalse() {
    // decimal("2.0") == decimal("2.1") -> FALSE
    assertEquals(false, holds("decimal(this) == decimal(\\\"2.1\\\")", "2.0"),
        "2.0 == 2.1 should be false");
  }

  // ---- TARGET CONTRACT: != is the negation of == ----

  @Test
  void ne_differentValue_isTrue() {
    // decimal("2.0") != decimal("2.1") -> TRUE
    assertTrue(holds("decimal(this) != decimal(\\\"2.1\\\")", "2.0"),
        "2.0 != 2.1 should be true");
  }

  @Test
  void ne_sameValueDifferentScale_isFalse() {
    // decimal("2.0") != decimal("2.00") -> FALSE (negation of a true ==)
    assertEquals(false, holds("decimal(this) != decimal(\\\"2.00\\\")", "2.0"),
        "2.0 != 2.00 should be false (== is true, so != negates to false)");
  }

  @Test
  void ne_sameValueSameScale_isFalse() {
    // decimal("2.0") != decimal("2.0") -> FALSE
    assertEquals(false, holds("decimal(this) != decimal(\\\"2.0\\\")", "2.0"),
        "2.0 != 2.0 should be false");
  }

  // ---- == agrees with decimals.eq on the same operands ----

  @Test
  void eq_agreesWithDecimalsEq() {
    assertEquals(
        holds("decimals.eq(decimal(this), decimal(\\\"2.00\\\"))", "2.0"),
        holds("decimal(this) == decimal(\\\"2.00\\\")", "2.0"),
        "== must agree with decimals.eq for 2.0 vs 2.00");
    assertEquals(
        holds("decimals.eq(decimal(this), decimal(\\\"2.1\\\"))", "2.0"),
        holds("decimal(this) == decimal(\\\"2.1\\\")", "2.0"),
        "== must agree with decimals.eq for 2.0 vs 2.1");
  }

  // ---- COMPOSITION: a decimal pulled out of a Variant ----
  //
  // A Decimal inside a Variant reaches CEL through variants.as(..., "decimal"), which has to
  // yield the very same runtime class that decimal(...) produces — dispatch is by Java class at
  // runtime, and at the CEL level both are just the DECIMAL OpaqueType, so the compiler cannot
  // catch a mismatch. All four consumers below therefore have to keep working off one value:
  // ==, decimals.eq, decimals.add, and string(). These are the regression gate for the
  // producer sweep.

  /** Build a Doc schema whose {@code payload} Variant field carries the given rule expression. */
  private static ProtobufSchema variantSchemaWithRule(String expr) {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"eq\", expr: \"" + expr + "\"}]\n"
        + "  }];\n"
        + "}\n";
    return new ProtobufSchema(s);
  }

  /** Build a Doc message whose Variant payload is a single decimal value. */
  private static DynamicMessage docWithVariantDecimal(ProtobufSchema schema, String amount) {
    VariantBuilder vb = new VariantBuilder();
    vb.appendDecimal(new BigDecimal(amount));
    Variant v = vb.build();

    Descriptor docDesc = schema.toDescriptor("test.Doc");
    Descriptor variantDesc = docDesc.findFieldByName("payload").getMessageType();
    DynamicMessage variantMsg = DynamicMessage.newBuilder(variantDesc)
        .setField(variantDesc.findFieldByName("value"), toByteString(v.getValueBuffer()))
        .setField(variantDesc.findFieldByName("metadata"), toByteString(v.getMetadataBuffer()))
        .build();
    return DynamicMessage.newBuilder(docDesc)
        .setField(docDesc.findFieldByName("payload"), variantMsg)
        .build();
  }

  private static ByteString toByteString(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return ByteString.copyFrom(out);
  }

  /** True iff the rule expr holds for a Doc whose Variant payload is {@code amount}. */
  private static boolean variantHolds(String expr, String amount) {
    ProtobufSchema schema = variantSchemaWithRule(expr);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), docWithVariantDecimal(schema, amount));
    assertTrue(errors.isEmpty() || errors.get(0).getCause() == null,
        "rule failed to evaluate: " + dumpCauses(errors));
    return errors.isEmpty();
  }

  private static String dumpCauses(List<ValidationRuleError> errs) {
    StringBuilder sb = new StringBuilder();
    for (ValidationRuleError e : errs) {
      Throwable t = e.getCause();
      while (t != null) {
        sb.append("\n  ").append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
        t = t.getCause();
      }
    }
    return sb.toString();
  }

  @Test
  void variantDecimal_equalsDecimalLiteral() {
    assertTrue(
        variantHolds("variants.as(variant(this), \\\"decimal\\\") == decimal(\\\"2.50\\\")", "2.50"),
        "a Variant decimal must compare equal to the same decimal literal via ==");
  }

  @Test
  void variantDecimal_decimalsEq() {
    assertTrue(
        variantHolds(
            "decimals.eq(variants.as(variant(this), \\\"decimal\\\"), decimal(\\\"2.50\\\"))",
            "2.50"),
        "decimals.eq must still accept a Variant-sourced decimal");
  }

  @Test
  void variantDecimal_decimalsAdd() {
    assertTrue(
        variantHolds(
            "decimals.eq("
                + "decimals.add(variants.as(variant(this), \\\"decimal\\\"), decimal(\\\"1\\\")),"
                + " decimal(\\\"3.50\\\"))",
            "2.50"),
        "decimals.add must still accept a Variant-sourced decimal");
  }

  @Test
  void variantDecimal_stringConversion() {
    assertTrue(
        variantHolds(
            "string(variants.as(variant(this), \\\"decimal\\\")) == \\\"2.50\\\"", "2.50"),
        "string(Decimal) must still dispatch for a Variant-sourced decimal");
  }

  // ---- Decimals nested in containers ----

  /**
   * A Decimal inside a list or map compares numerically too. Making the operands' own {@code ==}
   * numeric is not enough on its own: the standard implementation recurses into containers with
   * its own equality, so a bare protobuf Decimal nested one level deep was compared by its
   * encoding and {@code [a] == [b]} disagreed with {@code a == b} on the very same values.
   */
  @Test
  void containerEqualityIsNumericForNestedDecimals() {
    // `this` is 1.50; decimal("1.500") is the same number with a different scale.
    assertTrue(holds("[this] == [decimal(\\\"1.500\\\")]", "1.50"));
    assertTrue(holds("{\\\"k\\\": this} == {\\\"k\\\": decimal(\\\"1.500\\\")}", "1.50"));
    assertTrue(holds("[[this]] == [[decimal(\\\"1.500\\\")]]", "1.50"));
    // Negative controls: a different number, and a size mismatch.
    assertFalse(holds("[this] == [decimal(\\\"9\\\")]", "1.50"));
    assertFalse(holds("[this] == [this, this]", "1.50"));
  }

  /** {@code in} over a list follows the same equality, or it contradicts {@code ==}. */
  @Test
  void listMembershipIsNumericForNestedDecimals() {
    assertTrue(holds("this in [decimal(\\\"1.500\\\")]", "1.50"));
    assertTrue(holds("this in [decimal(\\\"9\\\"), decimal(\\\"1.500\\\")]", "1.50"));
    assertFalse(holds("this in [decimal(\\\"9\\\")]", "1.50"));
  }

  /** Decimal-free comparisons keep standard semantics — the recursion is gated on a Decimal. */
  @Test
  void containerEqualityUnchangedWithoutDecimals() {
    assertTrue(holds("[1, 2] == [1, 2]", "1.50"));
    assertFalse(holds("[1, 2] == [2, 1]", "1.50"));
    assertTrue(holds("{\\\"a\\\": 1} == {\\\"a\\\": 1}", "1.50"));
    assertTrue(holds("2 in [1, 2]", "1.50"));
    assertFalse(holds("3 in [1, 2]", "1.50"));
    assertTrue(holds("[\\\"x\\\"] == [\\\"x\\\"]", "1.50"));
  }
}
