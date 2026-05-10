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

package io.confluent.kafka.schemaregistry.rules.cel.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import dev.cel.common.types.CelType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructType;
import java.util.Optional;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

public class AvroCelTypeProviderTest {

  // ---- fieldCelType: logical types map to DYN ----------------------------

  @Test
  public void fieldCelType_timestampMillis_returnsDyn() {
    // Long underlying with timestamp-millis logical type — runtime value is
    // Instant, declaring INT would mismatch.
    Schema schema = LogicalTypes.timestampMillis().addToSchema(
        SchemaBuilder.builder().longType());
    assertEquals(SimpleType.DYN, AvroCelTypeProvider.fieldCelType(schema));
  }

  @Test
  public void fieldCelType_dateOnInt_returnsDyn() {
    // Int underlying, runtime LocalDate.
    Schema schema = LogicalTypes.date().addToSchema(
        SchemaBuilder.builder().intType());
    assertEquals(SimpleType.DYN, AvroCelTypeProvider.fieldCelType(schema));
  }

  @Test
  public void fieldCelType_decimalOnBytes_returnsDyn() {
    // Bytes underlying, runtime BigDecimal.
    Schema schema = LogicalTypes.decimal(10, 2).addToSchema(
        SchemaBuilder.builder().bytesType());
    assertEquals(SimpleType.DYN, AvroCelTypeProvider.fieldCelType(schema));
  }

  @Test
  public void fieldCelType_uuidOnString_returnsDyn() {
    // String underlying, runtime UUID.
    Schema schema = LogicalTypes.uuid().addToSchema(
        SchemaBuilder.builder().stringType());
    assertEquals(SimpleType.DYN, AvroCelTypeProvider.fieldCelType(schema));
  }

  @Test
  public void fieldCelType_logicalTypeInsideNullableUnion_returnsDyn() {
    // [null, {long, timestamp-millis}] → after stripNullableUnion, the
    // remaining branch is the logical-typed long → DYN.
    Schema longType = LogicalTypes.timestampMillis().addToSchema(
        SchemaBuilder.builder().longType());
    Schema union = SchemaBuilder.unionOf().nullType().and().type(longType).endUnion();
    assertEquals(SimpleType.DYN, AvroCelTypeProvider.fieldCelType(union));
  }

  // ---- record-level: logical-typed field reflected in StructType ---------

  @Test
  public void recordWithLogicalTypedField_fieldResolvesToDyn() {
    // A registered record's StructType field resolver should report the
    // logical-typed field as DYN.
    Schema timestampField = LogicalTypes.timestampMillis().addToSchema(
        SchemaBuilder.builder().longType());
    Schema record = SchemaBuilder.record("Event").fields()
        .name("ts").type(timestampField).noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    AvroCelTypeProvider provider = AvroCelTypeProvider.forSchema(record);
    Optional<CelType> structOpt = provider.findType("Event");
    assertTrue("Event should be registered", structOpt.isPresent());
    StructType struct = (StructType) structOpt.get();
    assertEquals(SimpleType.DYN, struct.findField("ts").get().type());
    assertEquals(SimpleType.STRING, struct.findField("name").get().type());
  }

  // ---- sanity: non-logical schemas still map to concrete types -----------

  @Test
  public void fieldCelType_plainLong_returnsInt() {
    assertEquals(SimpleType.INT,
        AvroCelTypeProvider.fieldCelType(SchemaBuilder.builder().longType()));
  }

  @Test
  public void fieldCelType_plainBytes_returnsBytes() {
    assertEquals(SimpleType.BYTES,
        AvroCelTypeProvider.fieldCelType(SchemaBuilder.builder().bytesType()));
  }
}
