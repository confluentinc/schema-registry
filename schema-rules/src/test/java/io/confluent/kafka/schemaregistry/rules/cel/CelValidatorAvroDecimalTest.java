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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import java.math.BigDecimal;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * Avro counterpart to {@link CelValidatorDecimalTest}: verifies the {@code to_decimal}
 * accessor + {@code decimal_*} operators work end-to-end against an Avro {@code decimal}
 * logical-type field. Tests the converters-on path, where the Avro reader produces
 * {@link BigDecimal} directly — the recommended client setup per the design doc.
 */
public class CelValidatorAvroDecimalTest {

  /**
   * Avro schema with a {@code decimal} logical-type field on bytes. Rule rejects
   * negative amounts using the new accessor + comparison.
   */
  private static final String SCHEMA_STR =
      "{"
      + "\"type\":\"record\","
      + "\"name\":\"Order\","
      + "\"namespace\":\"test\","
      + "\"fields\":["
      + "  {\"name\":\"amount\","
      + "   \"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
      + "            \"precision\":10,\"scale\":2},"
      + "   \"confluent:rules\":["
      + "     {\"name\":\"nonNegative\","
      + "      \"expr\":\"decimal_ge(to_decimal(this), to_decimal(\\\"0\\\"))\"}"
      + "   ]}"
      + "]"
      + "}";

  private static final AvroSchema AVRO_SCHEMA = new AvroSchema(SCHEMA_STR);
  private static final Schema RAW_SCHEMA = AVRO_SCHEMA.rawSchema();

  private static GenericRecord order(BigDecimal amount) {
    GenericRecord r = new GenericData.Record(RAW_SCHEMA);
    // Avro 1.12 GenericRecord accepts BigDecimal directly for a decimal-logical-type
    // field when the Conversion is registered globally (which it is for the standard
    // decimal conversion). This mirrors the useLogicalTypeConverters=true path.
    r.put("amount", amount);
    return r;
  }

  @Test
  void positiveAmount_satisfiesRule() {
    GenericRecord rec = order(new BigDecimal("100.50"));
    List<ValidationRuleError> errors =
        AVRO_SCHEMA.validateMessage(new CelValidator(), rec);
    assertTrue(errors.isEmpty(),
        "Positive amount should satisfy decimal_ge(this, 0), got: " + errors);
  }

  @Test
  void zeroAmount_satisfiesRule() {
    GenericRecord rec = order(new BigDecimal("0.00"));
    List<ValidationRuleError> errors =
        AVRO_SCHEMA.validateMessage(new CelValidator(), rec);
    assertTrue(errors.isEmpty(),
        "Zero amount should satisfy decimal_ge(this, 0), got: " + errors);
  }

  @Test
  void negativeAmount_violatesRule() {
    GenericRecord rec = order(new BigDecimal("-1.50"));
    List<ValidationRuleError> errors =
        AVRO_SCHEMA.validateMessage(new CelValidator(), rec);
    assertEquals(1, errors.size(),
        "Negative amount should fail the rule, got: " + errors);
    assertEquals("nonNegative", errors.get(0).getRule().getName());
    assertEquals("amount", errors.get(0).getFieldPath());
  }
}
