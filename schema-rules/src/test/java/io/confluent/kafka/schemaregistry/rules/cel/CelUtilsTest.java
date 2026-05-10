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

import static org.junit.Assert.assertEquals;

import dev.cel.common.types.SimpleType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class CelUtilsTest {

  // ---- findCelTypeForClass: STRING covers all CharSequence subtypes ------

  @Test
  public void stringClass_mapsToString() {
    assertEquals(SimpleType.STRING, CelUtils.findCelTypeForClass(String.class));
  }

  @Test
  public void utf8Class_mapsToString() {
    // Regression: Avro returns string fields as Utf8 in some configurations.
    // Declaring as MapType<STRING, DYN> caused compile-time `value + "..."`
    // to mis-dispatch on the AVRO field-rule path.
    assertEquals(SimpleType.STRING, CelUtils.findCelTypeForClass(Utf8.class));
  }

  @Test
  public void charSequenceClass_mapsToString() {
    // Any CharSequence implementation should declare as STRING.
    assertEquals(SimpleType.STRING, CelUtils.findCelTypeForClass(CharSequence.class));
    assertEquals(SimpleType.STRING, CelUtils.findCelTypeForClass(StringBuilder.class));
  }

  // ---- findCelTypeForAvroSchema: logical types map to DYN -----------------

  @Test
  public void timestampMillisSchema_mapsToDyn() {
    // Long underlying, but runtime value is Instant when converters are on.
    // Declaring INT would mismatch — use DYN so the runtime dispatches
    // against whatever the value actually is.
    Schema schema = LogicalTypes.timestampMillis().addToSchema(
        SchemaBuilder.builder().longType());
    assertEquals(SimpleType.DYN, CelUtils.findCelTypeForAvroSchema(schema));
  }

  @Test
  public void dateSchema_mapsToDyn() {
    // Int underlying, runtime LocalDate.
    Schema schema = LogicalTypes.date().addToSchema(
        SchemaBuilder.builder().intType());
    assertEquals(SimpleType.DYN, CelUtils.findCelTypeForAvroSchema(schema));
  }

  @Test
  public void decimalOnBytesSchema_mapsToDyn() {
    // Bytes underlying, runtime BigDecimal.
    Schema schema = LogicalTypes.decimal(10, 2).addToSchema(
        SchemaBuilder.builder().bytesType());
    assertEquals(SimpleType.DYN, CelUtils.findCelTypeForAvroSchema(schema));
  }

  @Test
  public void uuidOnStringSchema_mapsToDyn() {
    // String underlying, runtime UUID.
    Schema schema = LogicalTypes.uuid().addToSchema(
        SchemaBuilder.builder().stringType());
    assertEquals(SimpleType.DYN, CelUtils.findCelTypeForAvroSchema(schema));
  }

  @Test
  public void plainLongSchema_stillMapsToInt() {
    // Sanity check: schemas without logical types keep their concrete mapping.
    Schema schema = SchemaBuilder.builder().longType();
    assertEquals(SimpleType.INT, CelUtils.findCelTypeForAvroSchema(schema));
  }

  @Test
  public void plainStringSchema_stillMapsToString() {
    Schema schema = SchemaBuilder.builder().stringType();
    assertEquals(SimpleType.STRING, CelUtils.findCelTypeForAvroSchema(schema));
  }
}
