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

  // ---- findCelTypeForClass: non-Map values map to DYN, not MapType -------

  @Test
  public void bigDecimalClass_mapsToDyn() {
    // BigDecimal is the runtime Java rep for decimal logical types. It isn't
    // a Map — declaring MapType would block comparison/arithmetic ops at
    // compile time.
    assertEquals(SimpleType.DYN,
        CelUtils.findCelTypeForClass(java.math.BigDecimal.class));
  }

  @Test
  public void uuidClass_mapsToDyn() {
    // UUID is the runtime Java rep for uuid-on-string logical type.
    assertEquals(SimpleType.DYN, CelUtils.findCelTypeForClass(java.util.UUID.class));
  }

  @Test
  public void localDateClass_mapsToDyn() {
    // LocalDate is the runtime Java rep for date logical type.
    assertEquals(SimpleType.DYN, CelUtils.findCelTypeForClass(java.time.LocalDate.class));
  }

  @Test
  public void localTimeClass_mapsToDyn() {
    // LocalTime is the runtime Java rep for time-millis/time-micros.
    assertEquals(SimpleType.DYN, CelUtils.findCelTypeForClass(java.time.LocalTime.class));
  }

  @Test
  public void unknownPojoClass_mapsToDyn() {
    // Arbitrary POJO bean — DYN keeps .field access working at runtime via
    // the Jackson-converted Map and avoids over-constraining ops at compile.
    class MyPojo {
      @SuppressWarnings("unused") private String name;
    }
    assertEquals(SimpleType.DYN, CelUtils.findCelTypeForClass(MyPojo.class));
  }

  @Test
  public void mapClass_stillMapsToMapType() {
    // Sanity: Map subclasses keep their MapType<STRING, DYN> declaration.
    assertEquals(dev.cel.common.types.MapType.create(SimpleType.STRING, SimpleType.DYN),
        CelUtils.findCelTypeForClass(java.util.HashMap.class));
  }
}
