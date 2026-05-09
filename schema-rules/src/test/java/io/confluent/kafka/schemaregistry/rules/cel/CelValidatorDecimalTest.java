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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.protobuf.type.Decimal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * End-to-end smoke test for the CEL extended-types skeleton: proto
 * {@code confluent.type.Decimal} field, accessor function {@code to_decimal}, and
 * comparison operator {@code decimal_lt}. Verifies the full pipeline (declarations,
 * overloads, OpaqueVal carrier, type-checker overload resolution) for the simplest
 * case before expanding the test surface.
 */
public class CelValidatorDecimalTest {

  /**
   * Schema with a {@code confluent.type.Decimal} field. The rule rejects negative
   * amounts using the new accessor + comparison functions.
   */
  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"confluent/type/decimal.proto\";\n"
      + "message Order {\n"
      + "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"nonNegative\","
      + "             expr: \"decimal_ge(to_decimal(this), to_decimal(\\\"0\\\"))\"}]\n"
      + "  }];\n"
      + "}\n";

  private static DynamicMessage order(BigDecimal amount) {
    try {
      ProtobufSchema schema = new ProtobufSchema(SCHEMA);
      Descriptor desc = schema.toDescriptor("test.Order");
      FieldDescriptor amountField = desc.findFieldByName("amount");

      BigInteger unscaled = amount.unscaledValue();
      Decimal decimal = Decimal.newBuilder()
          .setValue(ByteString.copyFrom(unscaled.toByteArray()))
          .setScale(amount.scale())
          .build();

      // The Decimal proto descriptor used by the rule's schema and the one bundled
      // with the generated Decimal class are distinct instances; convert through
      // DynamicMessage so the field setter accepts it.
      Descriptor decimalDesc = amountField.getMessageType();
      DynamicMessage amountMsg = DynamicMessage.parseFrom(decimalDesc, decimal.toByteArray());

      return DynamicMessage.newBuilder(desc)
          .setField(amountField, amountMsg)
          .build();
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void positiveAmount_satisfiesRule() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    DynamicMessage msg = order(new BigDecimal("100.50"));

    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), msg);

    assertTrue(errors.isEmpty(),
        "Positive amount should satisfy decimal_ge(this, 0), got errors: " + errors);
  }

  @Test
  void zeroAmount_satisfiesRule() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    DynamicMessage msg = order(new BigDecimal("0.00"));

    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), msg);

    assertTrue(errors.isEmpty(),
        "Zero amount should satisfy decimal_ge(this, 0), got errors: " + errors);
  }

  @Test
  void negativeAmount_violatesRule() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    DynamicMessage msg = order(new BigDecimal("-1.50"));

    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), msg);

    assertEquals(1, errors.size(),
        "Negative amount should fail the rule, got: " + errors);
    assertEquals("nonNegative", errors.get(0).getRule().getName());
    assertEquals("amount", errors.get(0).getFieldPath());
  }
}
