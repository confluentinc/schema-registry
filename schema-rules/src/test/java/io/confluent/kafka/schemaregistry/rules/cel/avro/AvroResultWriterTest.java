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
import static org.junit.Assert.assertSame;
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
  public void stringField_acceptsEnumSymbol() {
    // Chained-transform scenario: a previous transform's Map carries an
    // EnumSymbol value for a STRING-schema field. The walker must accept it
    // (toString gives the symbol name, which is the expected Avro string-
    // field shape) instead of throwing typeMismatch.
    Schema enumSchema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO");
    Schema stringSchema = SchemaBuilder.builder().stringType();
    GenericData.EnumSymbol enumValue = new GenericData.EnumSymbol(enumSchema, "ONE");
    assertEquals("ONE", AvroResultWriter.convert(enumValue, stringSchema));
  }

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

  @Test
  public void fixedField_acceptsGenericFixedWithMatchingSchema() {
    // Pass-through case: caller already constructed a Fixed with the matching
    // schema (e.g., a CEL rule reads message.fixedField and writes it back).
    Schema schema = SchemaBuilder.fixed("Hash").size(3);
    GenericData.Fixed in = new GenericData.Fixed(schema, new byte[]{1, 2, 3});
    Object out = AvroResultWriter.convert(in, schema);
    assertSame("same-schema GenericFixed should pass through unchanged", in, out);
  }

  @Test
  public void fixedField_sameNameDifferentSize_rebuildsAndValidates() {
    // Two Fixed schemas share the fullName "Hash" but declare different sizes
    // (schema evolution / accident). Pass-through must NOT happen — the source
    // Fixed has 4 bytes, the target wants 3, so the size validation in toFixed
    // throws.
    Schema sourceSchema = SchemaBuilder.fixed("Hash").size(4);
    Schema targetSchema = SchemaBuilder.fixed("Hash").size(3);
    GenericData.Fixed in = new GenericData.Fixed(sourceSchema, new byte[]{1, 2, 3, 4});
    assertThrows(AvroTypeException.class,
        () -> AvroResultWriter.convert(in, targetSchema));
  }

  @Test
  public void fixedField_acceptsGenericFixedFromAnotherSchema() {
    // GenericFixed with a different schema name: not pass-through, but
    // unwrapBytes still extracts the bytes and a new Fixed is built.
    Schema otherSchema = SchemaBuilder.fixed("OtherHash").size(3);
    Schema schema = SchemaBuilder.fixed("Hash").size(3);
    GenericData.Fixed in = new GenericData.Fixed(otherSchema, new byte[]{4, 5, 6});
    Object out = AvroResultWriter.convert(in, schema);
    assertTrue(out instanceof GenericData.Fixed);
    assertEquals("Hash", ((GenericData.Fixed) out).getSchema().getName());
    assertArrayEquals(new byte[]{4, 5, 6}, ((GenericData.Fixed) out).bytes());
  }

  @Test
  public void bytesField_acceptsGenericFixed() {
    // A Fixed value bound for a BYTES field — unwrapBytes covers it.
    Schema fixedSchema = SchemaBuilder.fixed("Hash").size(2);
    Schema bytesSchema = SchemaBuilder.builder().bytesType();
    GenericData.Fixed in = new GenericData.Fixed(fixedSchema, new byte[]{9, 10});
    ByteBuffer out = (ByteBuffer) AvroResultWriter.convert(in, bytesSchema);
    assertArrayEquals(new byte[]{9, 10}, bytes(out));
  }

  // ---- union resolution for richer Avro shapes ---------------------------

  @Test
  public void bytesInUnion_acceptsGenericFixed() {
    // A Fixed value bound for a [null, bytes] union should resolve to the
    // BYTES branch — branchAccepts now recognizes GenericFixed as bytes-shaped.
    Schema fixedSchema = SchemaBuilder.fixed("Hash").size(2);
    Schema bytesSchema = SchemaBuilder.builder().bytesType();
    Schema union = Schema.createUnion(
        Arrays.asList(Schema.create(Schema.Type.NULL), bytesSchema));
    GenericData.Fixed in = new GenericData.Fixed(fixedSchema, new byte[]{1, 2});
    ByteBuffer out = (ByteBuffer) AvroResultWriter.convert(in, union);
    assertArrayEquals(new byte[]{1, 2}, bytes(out));
  }

  @Test
  public void enumInUnion_acceptsEnumSymbolWithDifferentSchema() {
    // EnumSymbol carries a different (more permissive) source schema; symbol
    // is valid in the target. Should resolve to the ENUM branch and the output
    // EnumSymbol should be rebuilt against the target schema.
    Schema sourceSchema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO", "THREE");
    Schema targetEnum = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO");
    Schema union = Schema.createUnion(
        Arrays.asList(Schema.create(Schema.Type.NULL), targetEnum));
    GenericData.EnumSymbol two = new GenericData.EnumSymbol(sourceSchema, "TWO");
    GenericData.EnumSymbol out = (GenericData.EnumSymbol) AvroResultWriter.convert(two, union);
    assertEquals("TWO", out.toString());
    assertEquals(targetEnum, out.getSchema());
  }

  @Test
  public void fixedInUnion_sameNameDifferentSize_routesToBytesBranch() {
    // [null, fixed3, bytes] union with a 4-byte Fixed value whose fullName
    // collides with fixed3's. branchAccepts must NOT match the FIXED branch
    // (size mismatch); resolution should fall through to the BYTES branch
    // where the value can be encoded successfully.
    Schema fixed3 = SchemaBuilder.fixed("Hash").size(3);
    Schema fixed4 = SchemaBuilder.fixed("Hash").size(4);
    Schema bytesSchema = SchemaBuilder.builder().bytesType();
    Schema union = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL), fixed3, bytesSchema));
    GenericData.Fixed in = new GenericData.Fixed(fixed4, new byte[]{1, 2, 3, 4});
    Object out = AvroResultWriter.convert(in, union);
    // BYTES branch picked → ByteBuffer with all 4 bytes.
    assertTrue("should resolve to BYTES branch, got: " + out.getClass(),
        out instanceof ByteBuffer);
    assertArrayEquals(new byte[]{1, 2, 3, 4}, bytes((ByteBuffer) out));
  }

  @Test
  public void decimalOnFixedInUnion_acceptsBigDecimal() {
    // Decimal logical type can be encoded as either bytes or fixed. branchAccepts
    // already handled BigDecimal for BYTES; this verifies it also handles
    // BigDecimal for FIXED so a [null, decimal-on-fixed] union resolves.
    Schema fixedSchema = org.apache.avro.LogicalTypes.decimal(10, 2).addToSchema(
        SchemaBuilder.fixed("Money").size(8));
    Schema union = Schema.createUnion(
        Arrays.asList(Schema.create(Schema.Type.NULL), fixedSchema));
    java.math.BigDecimal in = new java.math.BigDecimal("123.45");
    Object out = AvroResultWriter.convert(in, union);
    // Logical-type bypass returns the BigDecimal unchanged on the chosen branch.
    assertEquals(in, out);
  }

  @Test
  public void enumInUnion_unresolvedWhenSymbolMissingInTarget() {
    // EnumSymbol's symbol is absent from the target enum AND no other branch
    // accepts the value → UnresolvedUnionException.
    Schema sourceSchema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO", "THREE");
    Schema targetEnum = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO");
    Schema union = Schema.createUnion(
        Arrays.asList(Schema.create(Schema.Type.NULL), targetEnum));
    GenericData.EnumSymbol three = new GenericData.EnumSymbol(sourceSchema, "THREE");
    assertThrows(UnresolvedUnionException.class,
        () -> AvroResultWriter.convert(three, union));
  }

  // ---- record / enum schema evolution ------------------------------------

  @Test
  public void recordEvolution_targetAddsField_filledFromDeclaredDefault() {
    // Source schema has {name, age}; target adds {email} with a default. The
    // source IndexedRecord doesn't pass through (different schema); walker
    // rebuilds against the target, picking up the email default.
    Schema sourceSchema = SchemaBuilder.record("Person").fields()
        .name("name").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .endRecord();
    Schema targetSchema = SchemaBuilder.record("Person").fields()
        .name("name").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .name("email").type().stringType().stringDefault("none@example.com")
        .endRecord();
    GenericRecord source = new GenericData.Record(sourceSchema);
    source.put("name", "alice");
    source.put("age", 30);
    GenericRecord out = (GenericRecord) AvroResultWriter.convert(source, targetSchema);
    assertEquals("alice", out.get("name").toString());
    assertEquals(30, out.get("age"));
    assertEquals("none@example.com", out.get("email").toString());
  }

  @Test
  public void recordEvolution_targetDropsField_extraSourceFieldIgnored() {
    // Source has {name, age, email}; target dropped email. Walker rebuilds
    // against the target's two fields and silently ignores the source's
    // extra email.
    Schema sourceSchema = SchemaBuilder.record("Person").fields()
        .name("name").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .name("email").type().stringType().noDefault()
        .endRecord();
    Schema targetSchema = SchemaBuilder.record("Person").fields()
        .name("name").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .endRecord();
    GenericRecord source = new GenericData.Record(sourceSchema);
    source.put("name", "alice");
    source.put("age", 30);
    source.put("email", "alice@example.com");
    GenericRecord out = (GenericRecord) AvroResultWriter.convert(source, targetSchema);
    assertEquals("alice", out.get("name").toString());
    assertEquals(30, out.get("age"));
    assertEquals(targetSchema, out.getSchema());
  }

  @Test
  public void enumEvolution_symbolMissingInTargetThrows() {
    // Source enum has {ONE, TWO, THREE}; target dropped THREE. Passing an
    // EnumSymbol("THREE") with the source schema should throw, not pass
    // through silently.
    Schema sourceSchema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO", "THREE");
    Schema targetSchema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO");
    GenericData.EnumSymbol three = new GenericData.EnumSymbol(sourceSchema, "THREE");
    assertThrows(AvroTypeException.class,
        () -> AvroResultWriter.convert(three, targetSchema));
  }

  @Test
  public void enumEvolution_symbolValidInTargetIsRebuiltWithTargetSchema() {
    // EnumSymbol carries the source schema; symbol "TWO" exists in the target.
    // Walker should rebuild the EnumSymbol bound to the target schema rather
    // than pass through the source-bound one.
    Schema sourceSchema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO", "THREE");
    Schema targetSchema = SchemaBuilder.enumeration("Kind").symbols("ONE", "TWO");
    GenericData.EnumSymbol two = new GenericData.EnumSymbol(sourceSchema, "TWO");
    GenericData.EnumSymbol out = (GenericData.EnumSymbol) AvroResultWriter.convert(
        two, targetSchema);
    assertEquals("TWO", out.toString());
    assertEquals(targetSchema, out.getSchema());
  }

  // ---- helpers -----------------------------------------------------------

  private static byte[] bytes(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return out;
  }
}
