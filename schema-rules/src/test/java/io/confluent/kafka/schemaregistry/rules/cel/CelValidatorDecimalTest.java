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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.protobuf.type.Decimal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the {@code decimal(...)} constructor and {@code decimals.*}
 * operator surface against a Proto schema with a {@code confluent.type.Decimal} field.
 * Exercises type-checker registration of the {@code OpaqueType("confluent.type.Decimal")},
 * runtime dispatch on the proto Decimal Message shape, and CEL eval of each operator
 * family.
 */
public class CelValidatorDecimalTest {

  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"confluent/type/decimal.proto\";\n"
      + "message Money {\n"
      + "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"nonNegative\","
      + "             expr: \"decimals.ge(decimal(this), decimal(\\\"0\\\"))\"}]\n"
      + "  }];\n"
      + "}\n";

  /** Build a Money message with the given decimal amount, encoded as a proto Decimal. */
  private static DynamicMessage money(String amount) {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    Descriptor moneyDesc = schema.toDescriptor("test.Money");
    Descriptor decimalDesc = moneyDesc.findFieldByName("amount").getMessageType();

    java.math.BigDecimal bd = new java.math.BigDecimal(amount);
    BigInteger unscaled = bd.unscaledValue();
    int scale = bd.scale();

    DynamicMessage decimalMsg = DynamicMessage.newBuilder(decimalDesc)
        .setField(decimalDesc.findFieldByName("value"),
            ByteString.copyFrom(unscaled.toByteArray()))
        .setField(decimalDesc.findFieldByName("scale"), scale)
        .build();

    FieldDescriptor amountField = moneyDesc.findFieldByName("amount");
    return DynamicMessage.newBuilder(moneyDesc)
        .setField(amountField, decimalMsg)
        .build();
  }

  @Test
  void positiveAmount_passes() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), money("100.50"));
    assertTrue(errors.isEmpty(),
        "Positive amount should satisfy rule, got: " + errors);
  }

  @Test
  void zeroAmount_passes() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), money("0"));
    assertTrue(errors.isEmpty(),
        "Zero should satisfy `>= 0`, got: " + errors);
  }

  @Test
  void negativeAmount_fails() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), money("-0.01"));
    assertEquals(1, errors.size());
    assertEquals("nonNegative", errors.get(0).getRule().getName());
    assertEquals("amount", errors.get(0).getFieldPath());
  }

  // ---- decimals.* operator surface ----

  @Test
  void decimalArithmetic_eqAddSubMulDiv() {
    // Message-level rule (message_meta) so `this` is the Order message and we can
    // reference all three fields.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message Order {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"totalsMatch\","
        + "             expr: \"decimals.eq(decimals.add(decimal(this.subtotal),"
        + "                                              decimal(this.tax)),"
        + "                                decimal(this.total))\"}]\n"
        + "  };\n"
        + "  confluent.type.Decimal subtotal = 1;\n"
        + "  confluent.type.Decimal tax = 2;\n"
        + "  confluent.type.Decimal total = 3;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor orderDesc = schema.toDescriptor("test.Order");

    DynamicMessage order = DynamicMessage.newBuilder(orderDesc)
        .setField(orderDesc.findFieldByName("subtotal"), decimal(orderDesc, "subtotal", "100.00"))
        .setField(orderDesc.findFieldByName("tax"), decimal(orderDesc, "tax", "8.50"))
        .setField(orderDesc.findFieldByName("total"), decimal(orderDesc, "total", "108.50"))
        .build();

    List<ValidationRuleError> errors = schema.validateMessage(new CelValidator(), order);
    assertTrue(errors.isEmpty(), "Sums match — should pass. Got: " + errors);

    // Total wrong → rule fires
    DynamicMessage wrong = DynamicMessage.newBuilder(orderDesc)
        .setField(orderDesc.findFieldByName("subtotal"), decimal(orderDesc, "subtotal", "100.00"))
        .setField(orderDesc.findFieldByName("tax"), decimal(orderDesc, "tax", "8.50"))
        .setField(orderDesc.findFieldByName("total"), decimal(orderDesc, "total", "999.00"))
        .build();
    errors = schema.validateMessage(new CelValidator(), wrong);
    assertEquals(1, errors.size());
    assertEquals("totalsMatch", errors.get(0).getRule().getName());
  }

  @Test
  void decimalRound_halfUpDefault() {
    // 33.337 round to 2 dp = 33.34 (HALF_UP). Verifies decimals.round(d, scale).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message X {\n"
        + "  confluent.type.Decimal d = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimals.eq(decimals.round(decimal(this), 2),"
        + "                                 decimal(\\\"33.34\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("d"), decimal(xDesc, "d", "33.337"))
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), msg).isEmpty());
  }

  @Test
  void decimalSign_signum() {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message X {\n"
        + "  confluent.type.Decimal d = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"decimals.sign(decimal(this)) == -1\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage neg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("d"), decimal(xDesc, "d", "-1.5"))
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), neg).isEmpty());
  }

  @Test
  void decimalDiv_byZero_throwsCanonicalMessage() {
    // BigDecimal.divide throws a bare ArithmeticException on b=0; our binding
    // re-emits the canonical "decimals.div: division by zero" message so the
    // user-visible cause matches cel-python / cel-es. Locks in the message
    // so the next refactor doesn't quietly revert to the JDK default.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message X {\n"
        + "  confluent.type.Decimal d = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimals.eq(decimals.div(decimal(this),"
        + "                                              decimal(\\\"0\\\")),"
        + "                                decimal(\\\"0\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("d"), decimal(xDesc, "d", "1.0"))
        .build();
    List<ValidationRuleError> errors = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errors.size());
    // Walk the cause chain looking for the canonical message — cel-java wraps
    // our IllegalArgumentException in a CelEvaluationException, which the
    // CelExecutor wraps again in a RuleException.
    String chain = causeChainMessages(errors.get(0).getCause());
    assertTrue(chain.contains("decimals.div: division by zero"),
        "expected canonical 'decimals.div: division by zero' in cause chain; got: " + chain);
  }

  @Test
  void decimalSqrt_exactRoot() {
    // sqrt(144) == 12. Verifies decimals.sqrt returns a Decimal that compares
    // equal (compareTo, so trailing-zero scale differences don't matter).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message X {\n"
        + "  confluent.type.Decimal d = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimals.eq(decimals.sqrt(decimal(this)),"
        + "                                 decimal(\\\"12\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("d"), decimal(xDesc, "d", "144"))
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), msg).isEmpty());
  }

  @Test
  void decimalSqrt_irrationalRoot_38DigitPrecision() {
    // sqrt(2) is non-terminating; MathContext(38, HALF_UP) gives 38 significant
    // digits. Round to 10 dp and compare against the known prefix.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message X {\n"
        + "  confluent.type.Decimal d = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimals.eq(decimals.round(decimals.sqrt(decimal(this)), 10),"
        + "                                 decimal(\\\"1.4142135624\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("d"), decimal(xDesc, "d", "2"))
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), msg).isEmpty());
  }

  @Test
  void decimalSqrt_negative_throwsCanonicalMessage() {
    // BigDecimal.sqrt throws a bare ArithmeticException on a negative input;
    // our binding re-emits the canonical "decimals.sqrt: square root of
    // negative number" message. Locks in the user-visible cause.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message X {\n"
        + "  confluent.type.Decimal d = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimals.ge(decimals.sqrt(decimal(this)),"
        + "                                 decimal(\\\"0\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("d"), decimal(xDesc, "d", "-4"))
        .build();
    List<ValidationRuleError> errors = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errors.size());
    String chain = causeChainMessages(errors.get(0).getCause());
    assertTrue(chain.contains("decimals.sqrt: square root of negative number"),
        "expected canonical 'decimals.sqrt: square root of negative number' in cause chain; "
            + "got: " + chain);
  }

  private static String causeChainMessages(Throwable t) {
    StringBuilder sb = new StringBuilder();
    while (t != null) {
      sb.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" | ");
      t = t.getCause();
    }
    return sb.toString();
  }

  @Test
  void stringDecimal_extendsStdlib() {
    // CEL stdlib string(...) is extended with a (Decimal) -> string overload.
    // Verifies the extension registers cleanly and uses BigDecimal.toPlainString.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "message X {\n"
        + "  confluent.type.Decimal d = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"string(decimal(this)) == \\\"100.50\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("d"), decimal(xDesc, "d", "100.50"))
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), msg).isEmpty());
  }

  @Test
  void decimalFromBytesAndScale_byteOverload() {
    // Use the (bytes, int) overload from rule text. Message-level rule so `this`
    // is the X message and we can reference both fields.
    Decimal d = Decimal.newBuilder()
        .setValue(ByteString.copyFrom(new BigInteger("12345").toByteArray()))
        .setScale(2)
        .build();
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimals.eq(decimal(this.value, this.scale),"
        + "                                 decimal(\\\"123.45\\\"))\"}]\n"
        + "  };\n"
        + "  bytes value = 1;\n"
        + "  int32 scale = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("value"), d.getValue())
        .setField(xDesc.findFieldByName("scale"), 2)
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), msg).isEmpty());
  }

  @Test
  void decimalFromBytesAndScale_overflowScaleThrowsCanonicalMessage() {
    // CEL int is i64; BigDecimal scale is int32. A scale value outside int
    // range must throw with a clear message rather than silently truncating
    // via Long.intValue() (which would take the lower 32 bits and produce a
    // completely wrong scale). Mirrors variants.elem's int-overflow check.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"r\","
        // 4_294_967_296 = 2^32, far outside int range. Without the check,
        // intValue() would silently wrap to 0 and the rule would pass with
        // an unscaled BigDecimal.
        + "             expr: \"decimals.eq(decimal(this.value, 4294967296),"
        + "                                 decimal(\\\"123.45\\\"))\"}]\n"
        + "  };\n"
        + "  bytes value = 1;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor xDesc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(xDesc)
        .setField(xDesc.findFieldByName("value"),
            ByteString.copyFrom(new BigInteger("12345").toByteArray()))
        .build();
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errs.size(),
        "expected out-of-range scale to throw; got: " + errs);
    String causes = causeChainMessages(errs.get(0).getCause());
    assertTrue(causes.contains("scale out of int range"),
        "expected canonical 'scale out of int range' message; got causes: " + causes);
  }

  @Test
  void decimalsRound_overflowScaleThrowsCanonicalMessage() {
    // Same int-truncation hazard as decimal(bytes, scale): a CEL Long scale
    // outside int range must throw with a clear "decimals.round: scale out of
    // int range" message, not silently wrap via Long.intValue().
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimals.round(decimal(\\\"1.23\\\"), 4294967296)"
        + "                    == decimal(\\\"1.23\\\")\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("anchor"), 1)
        .build();
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errs.size(),
        "expected out-of-range scale to throw; got: " + errs);
    String causes = causeChainMessages(errs.get(0).getCause());
    assertTrue(causes.contains("decimals.round: scale out of int range"),
        "expected canonical 'decimals.round: scale out of int range'; got causes: " + causes);
  }

  @Test
  void decimalsTrunc_overflowScaleThrowsCanonicalMessage() {
    // Same int-truncation hazard for decimals.trunc(d, scale).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"decimals.trunc(decimal(\\\"1.23\\\"), 4294967296)"
        + "                    == decimal(\\\"1.23\\\")\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("anchor"), 1)
        .build();
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errs.size(),
        "expected out-of-range scale to throw; got: " + errs);
    String causes = causeChainMessages(errs.get(0).getCause());
    assertTrue(causes.contains("decimals.trunc: scale out of int range"),
        "expected canonical 'decimals.trunc: scale out of int range'; got causes: " + causes);
  }

  /** Build a confluent.type.Decimal DynamicMessage as a value for the given field. */
  private static DynamicMessage decimal(Descriptor parentDesc, String fieldName, String amount) {
    Descriptor decimalDesc = parentDesc.findFieldByName(fieldName).getMessageType();
    java.math.BigDecimal bd = new java.math.BigDecimal(amount);
    return DynamicMessage.newBuilder(decimalDesc)
        .setField(decimalDesc.findFieldByName("value"),
            ByteString.copyFrom(bd.unscaledValue().toByteArray()))
        .setField(decimalDesc.findFieldByName("scale"), bd.scale())
        .build();
  }

  // ---- Avro decimal logical type ----

  /**
   * Walker passes the field value through to CelValidator as-is. With Avro
   * converters on, decimal-logical-typed fields arrive as {@link BigDecimal};
   * with converters off they arrive as {@link ByteBuffer}. For in-memory
   * validation we put the value type we want to test directly into the
   * {@link GenericRecord}.
   */
  private static final String AVRO_DECIMAL_SCHEMA = ""
      + "{"
      + "  \"type\":\"record\","
      + "  \"name\":\"Money\","
      + "  \"namespace\":\"test\","
      + "  \"fields\":["
      + "    {"
      + "      \"name\":\"amount\","
      + "      \"type\":{"
      + "        \"type\":\"bytes\","
      + "        \"logicalType\":\"decimal\","
      + "        \"precision\":10,"
      + "        \"scale\":2"
      + "      },"
      + "      \"confluent:rules\":["
      + "        {\"name\":\"nonNeg\","
      + "         \"expr\":\"decimals.ge(decimal(this), decimal(\\\"0\\\"))\"}]"
      + "    }"
      + "  ]"
      + "}";

  @Test
  void avroDecimal_convertersOn_bigDecimalArrives_passes() {
    AvroSchema schema = new AvroSchema(AVRO_DECIMAL_SCHEMA);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("amount", new BigDecimal("100.50"));
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  @Test
  void avroDecimal_convertersOn_negativeFails() {
    AvroSchema schema = new AvroSchema(AVRO_DECIMAL_SCHEMA);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("amount", new BigDecimal("-0.01"));
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), r);
    assertEquals(1, errs.size());
    assertEquals("nonNeg", errs.get(0).getRule().getName());
  }

  @Test
  void avroDecimal_convertersOff_rawBytesRequireTwoArgOverload() {
    // With useLogicalTypeConverters=false, the field value arrives as a
    // ByteBuffer of the two's-complement unscaled bytes. The one-arg
    // decimal(dyn) overload throws (raw bytes have no scale) — the record-
    // level rule uses decimal(bytes, scale) referencing both fields.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Money\","
        + "  \"namespace\":\"test\","
        + "  \"confluent:rules\":["
        + "    {\"name\":\"r\","
        + "     \"expr\":\"decimals.eq(decimal(this.value, this.scale), decimal(\\\"123.45\\\"))\"}],"
        + "  \"fields\":["
        + "    {\"name\":\"value\",\"type\":\"bytes\"},"
        + "    {\"name\":\"scale\",\"type\":\"int\"}"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    BigDecimal bd = new BigDecimal("123.45");
    r.put("value", ByteBuffer.wrap(bd.unscaledValue().toByteArray()));
    r.put("scale", bd.scale());
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  @Test
  void avroDecimal_logicalTypeSchemaResolves_atFieldLevel() {
    // Sanity: the decimal logical type from the schema doesn't confuse the
    // walker. The field-level rule sees a BigDecimal as `this`.
    Schema schema = new AvroSchema(AVRO_DECIMAL_SCHEMA).rawSchema();
    Schema amountSchema = schema.getField("amount").schema();
    assertEquals("decimal", LogicalTypes.fromSchemaIgnoreInvalid(amountSchema).getName());
  }
}
