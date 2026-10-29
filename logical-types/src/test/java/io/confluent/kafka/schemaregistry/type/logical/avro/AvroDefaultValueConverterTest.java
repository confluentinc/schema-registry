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

package io.confluent.kafka.schemaregistry.type.logical.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroDefaultValueConverterTest {

  private static final Schema DECIMAL_5_2 = Schema.createDecimal(5, 2).setNullable(false);

  private static BigDecimal roundTrip(Schema type, String value) {
    String encoded = (String) AvroDefaultValueConverter.toAvroData(type, new BigDecimal(value));
    return (BigDecimal) AvroDefaultValueConverter.toJavaData(
        type, encoded.getBytes(StandardCharsets.ISO_8859_1));
  }

  @Test
  void decimalDefaultIsEncodedAtTheSchemaScale() {
    assertEquals(new BigDecimal("1.50"), roundTrip(DECIMAL_5_2, "1.5"));
    assertEquals(new BigDecimal("2.00"), roundTrip(DECIMAL_5_2, "2"));
    assertEquals(new BigDecimal("1.50"), roundTrip(DECIMAL_5_2, "1.50"));
    assertEquals(new BigDecimal("999.99"), roundTrip(DECIMAL_5_2, "999.99"));
    assertEquals(new BigDecimal("-0.01"), roundTrip(DECIMAL_5_2, "-0.01"));
    assertEquals(new BigDecimal("0.00"), roundTrip(DECIMAL_5_2, "0"));
  }

  @Test
  void decimalDefaultNeedingRoundingIsRefused() {
    ValidationException e = assertThrows(ValidationException.class,
        () -> AvroDefaultValueConverter.toAvroData(DECIMAL_5_2, new BigDecimal("1.555")));
    assertTrue(e.getMessage().contains("more digits after the decimal point"),
        "got: " + e.getMessage());
  }

  @Test
  void decimalDefaultExceedingThePrecisionIsRefused() {
    ValidationException e = assertThrows(ValidationException.class,
        () -> AvroDefaultValueConverter.toAvroData(DECIMAL_5_2, new BigDecimal("99999")));
    assertTrue(e.getMessage().contains("exceeds its precision"), "got: " + e.getMessage());
  }

  @Test
  void convertedSchemaCarriesTheRescaledDecimalDefault() {
    Schema struct = Schema.createStruct(Collections.singletonList(
        new Field("amount", DECIMAL_5_2, 0, new BigDecimal("1.5"), true, null, null, null)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "Holder");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals(new BigDecimal("1.50"), roundTripped.getField("amount").getDefaultValue());
  }
}
