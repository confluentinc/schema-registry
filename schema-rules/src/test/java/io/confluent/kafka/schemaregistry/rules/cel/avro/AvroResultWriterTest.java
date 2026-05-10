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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import dev.cel.common.values.CelByteString;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class AvroResultWriterTest {

  // ---- bytes -------------------------------------------------------------

  @Test
  public void bytesField_acceptsCelByteString() {
    Schema schema = SchemaBuilder.builder().bytesType();
    Object out = AvroResultWriter.convert(CelByteString.of(new byte[]{0, 1, 2}), schema);
    ByteBuffer buf = (ByteBuffer) out;
    assertArrayEquals(new byte[]{0, 1, 2}, bytes(buf));
  }

  @Test
  public void bytesField_acceptsRawByteArray() {
    Schema schema = SchemaBuilder.builder().bytesType();
    Object out = AvroResultWriter.convert(new byte[]{9, 8, 7}, schema);
    assertArrayEquals(new byte[]{9, 8, 7}, bytes((ByteBuffer) out));
  }

  // ---- nullable union ----------------------------------------------------

  @Test
  public void nullableUnion_javaNullPicksNullBranch() {
    Schema schema = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
    assertNull(AvroResultWriter.convert(null, schema));
  }

  @Test
  public void nullableUnion_celNullValuePicksNullBranch() {
    Schema schema = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
    // dev.cel.common.values.NullValue is what CEL's `null` literal evaluates to;
    // walker must treat it as Java null, not as a value to dispatch on type.
    assertNull(AvroResultWriter.convert(dev.cel.common.values.NullValue.NULL_VALUE, schema));
  }

  @Test
  public void nullableUnion_stringValuePicksStringBranch() {
    Schema schema = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
    assertEquals("hi", AvroResultWriter.convert("hi", schema));
  }

  // ---- non-null union dispatch by Java type ------------------------------

  @Test
  public void union_dispatchByJavaType_pickStringBranch() {
    Schema schema = SchemaBuilder.unionOf().intType().and().stringType().endUnion();
    assertEquals("x", AvroResultWriter.convert("x", schema));
  }

  @Test
  public void union_dispatchByJavaType_pickIntBranchWhenLongInRange() {
    Schema schema = SchemaBuilder.unionOf().intType().and().stringType().endUnion();
    // CEL widens int → long; walker should still let a small Long match an int branch.
    Object out = AvroResultWriter.convert(42L, schema);
    assertEquals(Integer.valueOf(42), out);
  }

  @Test
  public void union_firstMatchingBranchWinsInDeclarationOrder() {
    // Both branches accept Long; first wins.
    Schema schema = SchemaBuilder.unionOf().longType().and().doubleType().endUnion();
    Object out = AvroResultWriter.convert(7L, schema);
    assertEquals(Long.valueOf(7L), out);
  }

  @Test
  public void union_unresolvedThrows() {
    Schema schema = SchemaBuilder.unionOf().intType().and().stringType().endUnion();
    assertThrows(UnresolvedUnionException.class,
        () -> AvroResultWriter.convert(true, schema));
  }

  // ---- numeric narrowing -------------------------------------------------

  @Test
  public void intField_narrowsLongInRange() {
    Schema schema = SchemaBuilder.builder().intType();
    assertEquals(Integer.valueOf(123), AvroResultWriter.convert(123L, schema));
  }

  @Test
  public void intField_overflowThrows() {
    Schema schema = SchemaBuilder.builder().intType();
    assertThrows(AvroTypeException.class,
        () -> AvroResultWriter.convert((long) Integer.MAX_VALUE + 1L, schema));
  }

  @Test
  public void floatField_narrowsDouble() {
    Schema schema = SchemaBuilder.builder().floatType();
    assertEquals(Float.valueOf(1.5f), AvroResultWriter.convert(1.5d, schema));
  }

  // ---- record + missing fields -------------------------------------------

  private static Schema personSchema(boolean lastNameHasDefault) {
    SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.record("Person").fields()
        .name("name").type().stringType().noDefault();
    if (lastNameHasDefault) {
      fa = fa.name("lastName").type().stringType().stringDefault("doe");
    } else {
      fa = fa.name("lastName").type().stringType().noDefault();
    }
    return fa.endRecord();
  }

  @Test
  public void record_missingFieldUsesDeclaredDefault() {
    Schema schema = personSchema(true);
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("name", "alice");
    // lastName intentionally omitted; field has default "doe".
    GenericRecord rec = (GenericRecord) AvroResultWriter.convert(map, schema);
    assertEquals("alice", rec.get("name").toString());
    assertEquals("doe", rec.get("lastName").toString());
  }

  @Test
  public void record_missingFieldWithoutDefaultThrows() {
    Schema schema = personSchema(false);
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("name", "alice");
    assertThrows(AvroRuntimeException.class,
        () -> AvroResultWriter.convert(map, schema));
  }

  @Test
  public void record_widenedNumericNarrowsToDeclaredWidth() {
    Schema schema = SchemaBuilder.record("R").fields()
        .name("count").type().intType().noDefault()
        .endRecord();
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("count", 5L);
    GenericRecord rec = (GenericRecord) AvroResultWriter.convert(map, schema);
    // Declared int — must come back as Integer, not Long.
    assertEquals(Integer.valueOf(5), rec.get("count"));
  }

  // ---- logical type pass-through -----------------------------------------

  @Test
  public void logicalType_timestampMillis_passesValueThrough() {
    Schema schema = SchemaBuilder.builder().longBuilder().endLong();
    org.apache.avro.LogicalTypes.timestampMillis().addToSchema(schema);
    Instant now = Instant.ofEpochMilli(1_700_000_000_000L);
    Object out = AvroResultWriter.convert(now, schema);
    // Walker bypasses primitive narrowing for logical-typed schemas; Avro's
    // Conversion will handle Instant ↔ Long downstream.
    assertEquals(now, out);
  }

  @Test
  public void logicalType_nullForNonNullableLogicalField_throws() {
    // Non-nullable timestamp-millis field — CEL null shouldn't silently slip
    // through the logical-type bypass and explode later in build().
    Schema schema = SchemaBuilder.builder().longBuilder().endLong();
    org.apache.avro.LogicalTypes.timestampMillis().addToSchema(schema);
    assertThrows(AvroTypeException.class,
        () -> AvroResultWriter.convert(dev.cel.common.values.NullValue.NULL_VALUE, schema));
    assertThrows(AvroTypeException.class,
        () -> AvroResultWriter.convert(null, schema));
  }

  @Test
  public void logicalType_inUnion_stillResolvesBranch() {
    Schema longType = SchemaBuilder.builder().longBuilder().endLong();
    org.apache.avro.LogicalTypes.timestampMillis().addToSchema(longType);
    Schema union = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), longType));
    Instant now = Instant.ofEpochMilli(1_700_000_000_000L);
    // Unions still get branch-resolved; logical-type bypass kicks in only on
    // the chosen branch.
    Object out = AvroResultWriter.convert(now, union);
    assertEquals(now, out);
    // null still picks the null branch.
    assertNull(AvroResultWriter.convert(null, union));
  }

  // ---- enum, array, map, fixed ------------------------------------------

  @Test
  public void enumField_acceptsString() {
    Schema schema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO");
    Object out = AvroResultWriter.convert("TWO", schema);
    assertTrue(out instanceof GenericData.EnumSymbol);
    assertEquals("TWO", out.toString());
  }

  @Test
  public void enumField_unknownSymbolThrows() {
    Schema schema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO");
    assertThrows(AvroTypeException.class,
        () -> AvroResultWriter.convert("THREE", schema));
  }

  @Test
  public void arrayField_recursesIntoElementSchema() {
    Schema schema = SchemaBuilder.array().items().intType();
    @SuppressWarnings("unchecked")
    List<Object> out = (List<Object>) AvroResultWriter.convert(Arrays.asList(1L, 2L, 3L), schema);
    assertEquals(Arrays.asList(1, 2, 3), out);
  }

  @Test
  public void mapField_recursesIntoValueSchema() {
    Schema schema = SchemaBuilder.map().values().intType();
    Map<String, Object> in = new LinkedHashMap<>();
    in.put("a", 1L);
    in.put("b", 2L);
    @SuppressWarnings("unchecked")
    Map<String, Object> out = (Map<String, Object>) AvroResultWriter.convert(in, schema);
    assertEquals(Integer.valueOf(1), out.get("a"));
    assertEquals(Integer.valueOf(2), out.get("b"));
  }

  @Test
  public void fixedField_validatesLength() {
    Schema schema = SchemaBuilder.fixed("Hash").size(3);
    Object out = AvroResultWriter.convert(new byte[]{1, 2, 3}, schema);
    assertTrue(out instanceof GenericData.Fixed);
    assertArrayEquals(new byte[]{1, 2, 3}, ((GenericData.Fixed) out).bytes());
    assertThrows(AvroTypeException.class,
        () -> AvroResultWriter.convert(new byte[]{1, 2}, schema));
  }

  @Test
  public void fixedInUnion_acceptsByteBuffer() {
    // Regression: branchAccepts didn't recognize ByteBuffer for FIXED branches,
    // so a ByteBuffer for [null, fixed] threw UnresolvedUnionException even
    // though the same value worked for a non-union FIXED field.
    Schema fixed = SchemaBuilder.fixed("Hash").size(3);
    Schema union = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), fixed));
    ByteBuffer in = ByteBuffer.wrap(new byte[]{4, 5, 6});
    Object out = AvroResultWriter.convert(in, union);
    assertTrue(out instanceof GenericData.Fixed);
    assertArrayEquals(new byte[]{4, 5, 6}, ((GenericData.Fixed) out).bytes());
  }

  @Test
  public void bytesField_byteBufferIsDuplicatedNotAliased() {
    // Regression: toByteBuffer used to return the input ByteBuffer reference
    // directly, so position-mutating reads downstream would leak back to the
    // caller's value.
    Schema schema = SchemaBuilder.builder().bytesType();
    ByteBuffer in = ByteBuffer.wrap(new byte[]{7, 8, 9});
    ByteBuffer out = (ByteBuffer) AvroResultWriter.convert(in, schema);
    // Drain the returned buffer; the input's position must remain at 0.
    out.get();
    out.get();
    out.get();
    assertEquals(0, in.position());
  }

  // ---- helpers -----------------------------------------------------------

  private static byte[] bytes(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return out;
  }
}
