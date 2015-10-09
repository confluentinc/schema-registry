/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.copycat.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.kafka.copycat.data.Date;
import org.apache.kafka.copycat.data.Decimal;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaAndValue;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.data.Struct;
import org.apache.kafka.copycat.data.Time;
import org.apache.kafka.copycat.data.Timestamp;
import org.apache.kafka.copycat.errors.DataException;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import io.confluent.kafka.serializers.NonRecordContainer;

import static org.junit.Assert.*;

public class AvroDataTest {
  private static final int TEST_SCALE = 2;
  private static final BigDecimal TEST_DECIMAL = new BigDecimal(new BigInteger("156"), TEST_SCALE);
  private static final byte[] TEST_DECIMAL_BYTES = new byte[]{0, -100};

  private static final GregorianCalendar EPOCH;
  private static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_DAYS;
  private static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_MILLIS;
  static {
    EPOCH = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
    EPOCH.setTimeZone(TimeZone.getTimeZone("UTC"));

    EPOCH_PLUS_TEN_THOUSAND_DAYS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
    EPOCH_PLUS_TEN_THOUSAND_DAYS.setTimeZone(TimeZone.getTimeZone("UTC"));
    EPOCH_PLUS_TEN_THOUSAND_DAYS.add(Calendar.DATE, 10000);

    EPOCH_PLUS_TEN_THOUSAND_MILLIS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
    EPOCH_PLUS_TEN_THOUSAND_MILLIS.setTimeZone(TimeZone.getTimeZone("UTC"));
    EPOCH_PLUS_TEN_THOUSAND_MILLIS.add(Calendar.MILLISECOND, 10000);
  }

  // Copycat -> Avro

  @Test
  public void testFromCopycatBoolean() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
    checkNonRecordConversion(avroSchema, true, Schema.BOOLEAN_SCHEMA, true);
  }

  @Test
  public void testFromCopycatByte() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("copycat.type", "int8");
    checkNonRecordConversion(avroSchema, 12, Schema.INT8_SCHEMA, (byte) 12);
  }

  @Test
  public void testFromCopycatShort() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("copycat.type", "int16");
    checkNonRecordConversion(avroSchema, 12, Schema.INT16_SCHEMA, (short) 12);
  }

  @Test
  public void testFromCopycatInteger() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    checkNonRecordConversion(avroSchema, 12, Schema.INT32_SCHEMA, 12);
  }

  @Test
  public void testFromCopycatLong() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    checkNonRecordConversion(avroSchema, 12L, Schema.INT64_SCHEMA, 12L);
  }

  @Test
  public void testFromCopycatFloat() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().floatType();
    checkNonRecordConversion(avroSchema, 12.2f, Schema.FLOAT32_SCHEMA, 12.2f);
  }

  @Test
  public void testFromCopycatDouble() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
    checkNonRecordConversion(avroSchema, 12.2, Schema.FLOAT64_SCHEMA, 12.2);
  }

  @Test
  public void testFromCopycatBytes() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    checkNonRecordConversion(avroSchema, ByteBuffer.wrap("foo".getBytes()),
                             Schema.BYTES_SCHEMA, "foo".getBytes());
  }

  @Test
  public void testFromCopycatString() {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().stringType();
    checkNonRecordConversion(avroSchema, "string", Schema.STRING_SCHEMA, "string");
  }

  @Test
  public void testFromCopycatComplex() {
    Schema schema = SchemaBuilder.struct()
        .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .field("mapNonStringKeys",
               SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
        .build();
    Struct struct = new Struct(schema)
        .put("int8", (byte) 12)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", ByteBuffer.wrap("foo".getBytes()))
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1))
        .put("mapNonStringKeys", Collections.singletonMap(1, 1));

    Object convertedRecord = AvroData.fromCopycatData(schema, struct);

    org.apache.avro.Schema complexMapElementSchema =
        org.apache.avro.SchemaBuilder
            .record("MapEntry").namespace("io.confluent.copycat.avro").fields()
            .requiredInt("key")
            .requiredInt("value")
            .endRecord();

    // One field has some extra data set on it to ensure it gets passed through via the fields
    // config
    org.apache.avro.Schema int8Schema = org.apache.avro.SchemaBuilder.builder().intType();
    int8Schema.addProp("copycat.doc", "int8 field");
    int8Schema.addProp("copycat.default", JsonNodeFactory.instance.numberNode(2));
    int8Schema.addProp("copycat.type", "int8");
    org.apache.avro.Schema int16Schema = org.apache.avro.SchemaBuilder.builder().intType();
    int16Schema.addProp("copycat.type", "int16");
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record(AvroData.DEFAULT_SCHEMA_NAME).namespace(AvroData.NAMESPACE) // default values
        .fields()
        .name("int8").type(int8Schema).withDefault(2)
        .name("int16").type(int16Schema).noDefault()
        .requiredInt("int32")
        .requiredLong("int64")
        .requiredFloat("float32")
        .requiredDouble("float64")
        .requiredBoolean("boolean")
        .requiredString("string")
        .requiredBytes("bytes")
        .name("array").type().array().items().stringType().noDefault()
        .name("map").type().map().values().intType().noDefault()
        .name("mapNonStringKeys").type().array().items(complexMapElementSchema).noDefault()
        .endRecord();
    org.apache.avro.generic.GenericRecord avroRecord
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("int8", 12)
        .set("int16", 12)
        .set("int32", 12)
        .set("int64", 12L)
        .set("float32", 12.2f)
        .set("float64", 12.2)
        .set("boolean", true)
        .set("string", "foo")
        .set("bytes", ByteBuffer.wrap("foo".getBytes()))
        .set("array", Arrays.asList("a", "b", "c"))
        .set("map", Collections.singletonMap("field", 1))
        .set("mapNonStringKeys", Arrays.asList(
            new GenericRecordBuilder(complexMapElementSchema)
                .set(AvroData.KEY_FIELD, 1)
                .set(AvroData.VALUE_FIELD, 1)
                .build()))
        .build();

    assertEquals(avroSchema, ((org.apache.avro.generic.GenericRecord) convertedRecord).getSchema());
    assertEquals(avroRecord, convertedRecord);
  }

  @Test
  public void testFromCopycatOptionalPrimitiveWithMetadata() {
    Schema schema = SchemaBuilder.string().
        doc("doc").defaultValue("foo").name("io.confluent.stringtype").version(2).optional()
        .parameter("foo", "bar").parameter("baz", "baz")
        .build();

    // Missing some metadata, used to validate missing properties on the Avro schema will cause
    // schemas to be considered not equal
    org.apache.avro.Schema wrongAvroSchema =
        org.apache.avro.SchemaBuilder.builder().unionOf()
            .stringType().and()
            .nullType().endUnion();

    // The complete schema
    org.apache.avro.Schema avroStringSchema = org.apache.avro.SchemaBuilder.builder().stringType();
    avroStringSchema.addProp("copycat.name", "io.confluent.stringtype");
    avroStringSchema.addProp("copycat.version",
                             JsonNodeFactory.instance.numberNode(2));
    avroStringSchema.addProp("copycat.doc", "doc");
    avroStringSchema.addProp("copycat.default", "foo");
    ObjectNode params = JsonNodeFactory.instance.objectNode();
    params.put("foo", "bar");
    params.put("baz", "baz");
    avroStringSchema.addProp("copycat.parameters", params);
    org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().unionOf()
            .type(avroStringSchema).and()
            .nullType().endUnion();

    NonRecordContainer converted = checkNonRecordConversion(avroSchema, "string",
                                                            schema, "string");
    assertNotEquals(wrongAvroSchema, converted.getSchema());
  }

  @Test
  public void testFromCopycatRecordWithMetadata() {
    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.TestSchema").version(12).doc("doc")
        .field("int32", Schema.INT32_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("int32", 12);

    Object convertedRecord = AvroData.fromCopycatData(schema, struct);

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("TestSchema").namespace("io.confluent.test")
        .fields()
        .requiredInt("int32")
        .endRecord();
    avroSchema.addProp("copycat.name", "io.confluent.test.TestSchema");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(12));
    avroSchema.addProp("copycat.doc", "doc");
    org.apache.avro.generic.GenericRecord avroRecord
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("int32", 12)
        .build();

    assertEquals(avroSchema, ((org.apache.avro.generic.GenericRecord) convertedRecord).getSchema());
    assertEquals(avroRecord, convertedRecord);
  }

  @Test
  public void testFromCopycatLogicalDecimal() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    avroSchema.addProp("copycat.name", "org.apache.kafka.copycat.data.Decimal");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(1));
    ObjectNode avroParams = JsonNodeFactory.instance.objectNode();
    avroParams.put("scale", "2");
    avroSchema.addProp("copycat.parameters", avroParams);
    checkNonRecordConversion(avroSchema, ByteBuffer.wrap(TEST_DECIMAL_BYTES),
                             Decimal.schema(2), TEST_DECIMAL);
  }

  @Test
  public void testFromCopycatLogicalDate() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("copycat.name", "org.apache.kafka.copycat.data.Date");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(1));
    checkNonRecordConversion(avroSchema, 10000, Date.SCHEMA,
                             EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime());
  }

  @Test
  public void testFromCopycatLogicalTime() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("copycat.name", "org.apache.kafka.copycat.data.Time");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(1));
    checkNonRecordConversion(avroSchema, 10000, Time.SCHEMA,
                             EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime());
  }

  @Test
  public void testFromCopycatLogicalTimestamp() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    avroSchema.addProp("copycat.name", "org.apache.kafka.copycat.data.Timestamp");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(1));
    java.util.Date date = new java.util.Date();
    checkNonRecordConversion(avroSchema, date.getTime(), Timestamp.SCHEMA, date);
  }

  @Test(expected = DataException.class)
  public void testFromCopycatMismatchSchemaPrimitive() {
    AvroData.fromCopycatData(Schema.OPTIONAL_BOOLEAN_SCHEMA, 12);
  }

  @Test(expected = DataException.class)
  public void testFromCopycatMismatchSchemaPrimitiveRequired() {
    AvroData.fromCopycatData(Schema.BOOLEAN_SCHEMA, null);
  }

  @Test(expected = DataException.class)
  public void testFromCopycatMismatchSchemaArray() {
    AvroData.fromCopycatData(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build(), Arrays.asList(12));
  }

  @Test(expected = DataException.class)
  public void testFromCopycatMismatchSchemaMapWithStringKeyMismatchKey() {
    AvroData.fromCopycatData(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
                             Collections.singletonMap(true, 12));
  }

  @Test(expected = DataException.class)
  public void testFromCopycatMismatchSchemaMapWithStringKeyMismatchValue() {
    AvroData.fromCopycatData(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
                             Collections.singletonMap("foobar", 12L));
  }

  @Test(expected = DataException.class)
  public void testFromCopycatMismatchSchemaMapWithNonStringKeyMismatchKey() {
    AvroData.fromCopycatData(SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(),
                             Collections.singletonMap(true, 12));
  }

  @Test(expected = DataException.class)
  public void testFromCopycatMismatchSchemaMapWithNonStringKeyMismatchValue() {
    AvroData.fromCopycatData(SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(),
                             Collections.singletonMap(12, 12L));
  }

  @Test(expected = DataException.class)
  public void testFromCopycatMismatchSchemaRecord() {
    Schema firstSchema = SchemaBuilder.struct()
        .field("foo", Schema.BOOLEAN_SCHEMA)
        .build();
    Schema secondSchema = SchemaBuilder.struct()
        .field("foo", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    AvroData.fromCopycatData(firstSchema, new Struct(secondSchema).put("foo", null));
  }

  @Test
  public void testFromCopycatSchemaless() {
    GenericRecord avroNullRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroNullRecord, null, null);

    GenericRecord avroIntRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("int", 12)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, (byte) 12);
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, (short) 12);
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, 12);

    GenericRecord avroLongRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("long", 12L)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroLongRecord, null, 12L);

    GenericRecord avroFloatRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("float", 12.2f)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroFloatRecord, null, 12.2f);

    GenericRecord avroDoubleRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("double", 12.2)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroDoubleRecord, null, 12.2);

    GenericRecord avroBooleanRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("boolean", true)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroBooleanRecord, null, true);

    GenericRecord avroStringRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("string", "teststring")
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroStringRecord, null, "teststring");

    GenericRecord avroArrayRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("array", Arrays.asList(avroIntRecord, avroStringRecord))
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroArrayRecord,
                             null, Arrays.asList(12, "teststring"));

    GenericRecord avroMapEntry = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
        .set("key", avroIntRecord)
        .set("value", avroStringRecord)
        .build();
    GenericRecord avroMapRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("map", Arrays.asList(avroMapEntry))
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroMapRecord,
                             null, Collections.singletonMap(12, "teststring"));
  }

  // Avro -> Copycat. Validate a) all Avro types that convert directly to Avro, b) specialized
  // Avro types where we can convert to a Copycat type that doesn't have a corresponding Avro
  // type, and c) Avro types which need specialized transformation because there is no
  // corresponding Copycat type.

  // Avro -> Copycat: directly corresponding types

  @Test
  public void testToCopycatBoolean() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
    assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true),
                 AvroData.toCopycatData(avroSchema, true));
  }

  @Test
  public void testToCopycatInt32() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    assertEquals(new SchemaAndValue(Schema.INT32_SCHEMA, 12),
                 AvroData.toCopycatData(avroSchema, 12));
  }

  @Test
  public void testToCopycatInt64() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 12L),
                 AvroData.toCopycatData(avroSchema, 12L));
  }

  @Test
  public void testToCopycatFloat32() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().floatType();
    assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 12.f),
                 AvroData.toCopycatData(avroSchema, 12.f));
  }

  @Test
  public void testToCopycatFloat64() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
    assertEquals(new SchemaAndValue(Schema.FLOAT64_SCHEMA, 12.0),
                 AvroData.toCopycatData(avroSchema, 12.0));
  }

  @Test
  public void testToCopycatString() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().stringType();
    assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "teststring"),
                 AvroData.toCopycatData(avroSchema, "teststring"));

    // Avro deserializer allows CharSequence, not just String, and returns Utf8 objects
    assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "teststring"),
                 AvroData.toCopycatData(avroSchema, new Utf8("teststring")));
  }

  @Test
  public void testToCopycatBytes() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foo".getBytes())),
                 AvroData.toCopycatData(avroSchema, "foo".getBytes()));

    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foo".getBytes())),
                 AvroData.toCopycatData(avroSchema, ByteBuffer.wrap("foo".getBytes())));
  }

  @Test
  public void testToCopycatArray() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().array()
        .items().intType();
    avroSchema.getElementType().addProp("copycat.type", "int8");
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
    assertEquals(new SchemaAndValue(schema, Arrays.asList((byte) 12, (byte) 13)),
                 AvroData.toCopycatData(avroSchema, Arrays.asList(12, 13)));
  }

  @Test
  public void testToCopycatMapStringKeys() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().map()
        .values().intType();
    avroSchema.getValueType().addProp("copycat.type", "int8");
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT8_SCHEMA).build();
    assertEquals(new SchemaAndValue(schema, Collections.singletonMap("field", (byte) 12)),
                 AvroData.toCopycatData(avroSchema, Collections.singletonMap("field", 12)));
  }

  @Test
  public void testToCopycatRecord() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("Record").fields()
        .requiredInt("int8")
        .requiredString("string")
        .endRecord();
    avroSchema.getField("int8").schema().addProp("copycat.type", "int8");
    GenericRecord avroRecord = new GenericRecordBuilder(avroSchema)
        .set("int8", 12)
        .set("string", "sample string")
        .build();
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.struct()
        .name("Record")
        .field("int8", Schema.INT8_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema).put("int8", (byte) 12).put("string", "sample string");
    assertEquals(new SchemaAndValue(schema, struct),
                 AvroData.toCopycatData(avroSchema, avroRecord));
  }

  // Avro -> Copycat: Copycat logical types

  @Test
  public void testToCopycatDecimal() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    avroSchema.addProp("copycat.name", "org.apache.kafka.copycat.data.Decimal");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(1));
    ObjectNode avroParams = JsonNodeFactory.instance.objectNode();
    avroParams.put("scale", "2");
    avroSchema.addProp("copycat.parameters", avroParams);
    assertEquals(new SchemaAndValue(Decimal.schema(2), TEST_DECIMAL),
                 AvroData.toCopycatData(avroSchema, TEST_DECIMAL_BYTES));
  }

  @Test
  public void testToCopycatDate() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("copycat.name", "org.apache.kafka.copycat.data.Date");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(1));
    assertEquals(new SchemaAndValue(Date.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime()),
                 AvroData.toCopycatData(avroSchema, 10000));
  }

  @Test
  public void testToCopycatTime() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("copycat.name", "org.apache.kafka.copycat.data.Time");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(1));
    assertEquals(new SchemaAndValue(Time.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime()),
                 AvroData.toCopycatData(avroSchema, 10000));
  }

  @Test
  public void testToCopycatTimestamp() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    avroSchema.addProp("copycat.name", "org.apache.kafka.copycat.data.Timestamp");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(1));
    java.util.Date date = new java.util.Date();
    assertEquals(new SchemaAndValue(Timestamp.SCHEMA, date),
                 AvroData.toCopycatData(avroSchema, date.getTime()));
  }

  // Avro -> Copycat: Copycat types with no corresponding Avro type

  @Test
  public void testToCopycatInt8() {
    // int8 should have a special annotation and Avro will have decoded an Integer
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("copycat.type", "int8");
    assertEquals(new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 12),
                 AvroData.toCopycatData(avroSchema, 12));
  }

  @Test
  public void testToCopycatInt16() {
    // int16 should have a special annotation and Avro will have decoded an Integer
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("copycat.type", "int16");
    assertEquals(new SchemaAndValue(Schema.INT16_SCHEMA, (short) 12),
                 AvroData.toCopycatData(avroSchema, 12));
  }

  @Test
  public void testToCopycatMapNonStringKeys() {
    // Encoded as array of 2-tuple records. Use key and value types that require conversion to
    // make sure conversion of each element actually occurs. The more verbose construction of the
    // Avro schema avoids reuse of schemas, which is needed since after constructing the schemas
    // we set additional properties on them.
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().array()
        .items().record("MapEntry").namespace("io.confluent.copycat.avro").fields()
        .name("key").type(org.apache.avro.SchemaBuilder.builder().intType()).noDefault()
        .name("value").type(org.apache.avro.SchemaBuilder.builder().intType()).noDefault()
        .endRecord();
    avroSchema.getElementType().getField("key").schema().addProp("copycat.type", "int8");
    avroSchema.getElementType().getField("value").schema().addProp("copycat.type", "int16");
    GenericRecord record = new GenericRecordBuilder(avroSchema.getElementType())
        .set("key", 12)
        .set("value", 16)
        .build();
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT16_SCHEMA).build();
    assertEquals(new SchemaAndValue(schema, Collections.singletonMap((byte) 12, (short) 16)),
                 AvroData.toCopycatData(avroSchema, Arrays.asList(record)));
  }

  // Avro -> Copycat: Avro types with no corresponding Copycat type

  @Test(expected = DataException.class)
  public void testToCopycatNull() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().nullType();
    // If we somehow did end up with a null schema and an actual value that let it get past the
    AvroData.toCopycatData(avroSchema, true);
  }

  @Test
  public void testToCopycatFixed() {
    // Our conversion simply loses the fixed size information.
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .fixed("sample").size(4);
    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foob".getBytes())),
                 AvroData.toCopycatData(avroSchema, "foob".getBytes()));

    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foob".getBytes())),
                 AvroData.toCopycatData(avroSchema, ByteBuffer.wrap("foob".getBytes())));
  }

  @Test
  public void testToCopycatUnion() {
    // Make sure we handle primitive types and named types properly by using a variety of types
    org.apache.avro.Schema avroRecordSchema1 = org.apache.avro.SchemaBuilder.builder()
        .record("Test1").fields().requiredInt("test").endRecord();
    org.apache.avro.Schema avroRecordSchema2 = org.apache.avro.SchemaBuilder.builder()
        .record("Test2").namespace("io.confluent").fields().requiredInt("test").endRecord();
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
        .intType().and()
        .stringType().and()
        .type(avroRecordSchema1).and()
        .type(avroRecordSchema2)
        .endUnion();


    Schema recordSchema1 = SchemaBuilder.struct().name("Test1")
        .field("test", Schema.INT32_SCHEMA).optional().build();
    Schema recordSchema2 = SchemaBuilder.struct().name("io.confluent.Test2")
        .field("test", Schema.INT32_SCHEMA).optional().build();
    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.copycat.avro.Union")
        .field("int", Schema.OPTIONAL_INT32_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .field("Test1", recordSchema1)
        .field("Test2", recordSchema2)
        .build();
    assertEquals(new SchemaAndValue(schema, new Struct(schema).put("int", 12)),
                 AvroData.toCopycatData(avroSchema, 12));
    assertEquals(new SchemaAndValue(schema, new Struct(schema).put("string", "teststring")),
                 AvroData.toCopycatData(avroSchema, "teststring"));

    Struct schema1Test = new Struct(schema).put("Test1", new Struct(recordSchema1).put("test", 12));
    GenericRecord record1Test = new GenericRecordBuilder(avroRecordSchema1).set("test", 12).build();
    Struct schema2Test = new Struct(schema).put("Test2", new Struct(recordSchema2).put("test", 12));
    GenericRecord record2Test = new GenericRecordBuilder(avroRecordSchema2).set("test", 12).build();
    assertEquals(new SchemaAndValue(schema, schema1Test),
                 AvroData.toCopycatData(avroSchema, record1Test));
    assertEquals(new SchemaAndValue(schema, schema2Test),
                 AvroData.toCopycatData(avroSchema, record2Test));
  }

  @Test(expected = DataException.class)
  public void testToCopycatUnionRecordConflict() {
    // If the records have the same name but are in different namespaces, we don't support this
    // because Avro field naming is fairly restrictive
    org.apache.avro.Schema avroRecordSchema1 = org.apache.avro.SchemaBuilder.builder()
        .record("Test1").fields().requiredInt("test").endRecord();
    org.apache.avro.Schema avroRecordSchema2 = org.apache.avro.SchemaBuilder.builder()
        .record("Test1").namespace("io.confluent").fields().requiredInt("test").endRecord();
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
        .type(avroRecordSchema1).and()
        .type(avroRecordSchema2)
        .endUnion();

    GenericRecord recordTest = new GenericRecordBuilder(avroRecordSchema1).set("test", 12).build();
    AvroData.toCopycatData(avroSchema, recordTest);
  }

  @Test
  public void testToCopycatEnum() {
    // Enums are just converted to strings, no trace of the original enum is preserved
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .enumeration("TestEnum").symbols("foo", "bar", "baz");
    assertEquals(new SchemaAndValue(SchemaBuilder.string().name("TestEnum").build(), "bar"),
                 AvroData.toCopycatData(avroSchema, "bar"));
    assertEquals(new SchemaAndValue(SchemaBuilder.string().name("TestEnum").build(), "bar"),
                 AvroData.toCopycatData(avroSchema, new GenericData.EnumSymbol(avroSchema, "bar")));
  }


  @Test
  public void testToCopycatOptionalPrimitiveWithCopycatMetadata() {
    Schema schema = SchemaBuilder.string().
        doc("doc").defaultValue("foo").name("io.confluent.stringtype").version(2).optional()
        .parameter("foo", "bar").parameter("baz", "baz")
        .build();

    org.apache.avro.Schema avroStringSchema = org.apache.avro.SchemaBuilder.builder().stringType();
    avroStringSchema.addProp("copycat.name", "io.confluent.stringtype");
    avroStringSchema.addProp("copycat.version",
                             JsonNodeFactory.instance.numberNode(2));
    avroStringSchema.addProp("copycat.doc", "doc");
    avroStringSchema.addProp("copycat.default", "foo");
    ObjectNode params = JsonNodeFactory.instance.objectNode();
    params.put("foo", "bar");
    params.put("baz", "baz");
    avroStringSchema.addProp("copycat.parameters", params);
    org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().unionOf()
            .type(avroStringSchema).and()
            .nullType().endUnion();


    assertEquals(new SchemaAndValue(schema, "string"),
                 AvroData.toCopycatData(avroSchema, "string"));
  }

  @Test
  public void testToCopycatRecordWithMetadata() {
    // One important difference between record schemas in Avro and Copycat is that Avro has some
    // per-field metadata (doc, default value) that Copycat holds in the schema itself. We set
    // these properties on one of these fields to ensure they are properly converted
    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.TestSchema").version(12).doc("doc")
        .field("int32", SchemaBuilder.int32().defaultValue(7).doc("field doc").build())
        .build();
    Struct struct = new Struct(schema)
        .put("int32", 12);

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("TestSchema").namespace("io.confluent.test")
        .fields()
        .name("int32").doc("field doc").type().intType().intDefault(7)
        .endRecord();
    avroSchema.addProp("copycat.name", "io.confluent.test.TestSchema");
    avroSchema.addProp("copycat.version", JsonNodeFactory.instance.numberNode(12));
    avroSchema.addProp("copycat.doc", "doc");
    org.apache.avro.generic.GenericRecord avroRecord
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("int32", 12)
        .build();

    assertEquals(new SchemaAndValue(schema, struct),
                 AvroData.toCopycatData(avroSchema, avroRecord));
  }

  @Test
  public void testToCopycatSchemaless() {
    GenericRecord avroNullRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).build();
    assertEquals(new SchemaAndValue(null, null),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroNullRecord));

    GenericRecord avroIntRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("int", 12)
        .build();
    assertEquals(new SchemaAndValue(null, 12),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroIntRecord));

    GenericRecord avroLongRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("long", 12L)
        .build();
    assertEquals(new SchemaAndValue(null, 12L),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroLongRecord));

    GenericRecord avroFloatRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("float", 12.2f)
        .build();
    assertEquals(new SchemaAndValue(null, 12.2f),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroFloatRecord));

    GenericRecord avroDoubleRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("double", 12.2)
        .build();
    assertEquals(new SchemaAndValue(null, 12.2),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroDoubleRecord));

    GenericRecord avroBooleanRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("boolean", true)
        .build();
    assertEquals(new SchemaAndValue(null, true),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroBooleanRecord));

    GenericRecord avroStringRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("string", "teststring")
        .build();
    assertEquals(new SchemaAndValue(null, "teststring"),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroStringRecord));


    GenericRecord avroArrayRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("array", Arrays.asList(avroIntRecord, avroStringRecord))
        .build();
    assertEquals(new SchemaAndValue(null, Arrays.asList(12, "teststring")),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroArrayRecord));

    GenericRecord avroMapEntry = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
        .set("key", avroIntRecord)
        .set("value", avroStringRecord)
        .build();
    GenericRecord avroMapRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("map", Arrays.asList(avroMapEntry))
        .build();
    assertEquals(new SchemaAndValue(null, Collections.singletonMap(12, "teststring")),
                 AvroData.toCopycatData(AvroData.ANYTHING_SCHEMA, avroMapRecord));

  }

  @Test(expected = DataException.class)
  public void testToCopycatSchemaMismatchPrimitive() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    AvroData.toCopycatData(avroSchema, 12L);
  }

  @Test(expected = DataException.class)
  public void testToCopycatSchemaMismatchArray() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .array().items().stringType();
    AvroData.toCopycatData(avroSchema, Arrays.asList(1, 2, 3));
  }

  @Test(expected = DataException.class)
  public void testToCopycatSchemaMismatchMapMismatchKey() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .map().values().intType();
    AvroData.toCopycatData(avroSchema, Collections.singletonMap(12, 12));
  }

  @Test(expected = DataException.class)
  public void testToCopycatSchemaMismatchMapMismatchValue() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .map().values().intType();
    AvroData.toCopycatData(avroSchema, Collections.singletonMap("foo", 12L));
  }

  @Test(expected = DataException.class)
  public void testToCopycatSchemaMismatchRecord() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("Record").fields()
        .requiredString("string")
        .endRecord();
    org.apache.avro.Schema avroSchemaWrong = org.apache.avro.SchemaBuilder.builder()
        .record("Record").fields()
        .requiredInt("string")
        .endRecord();
    GenericRecord avroRecordWrong = new GenericRecordBuilder(avroSchemaWrong)
        .set("string", 12)
        .build();

    AvroData.toCopycatData(avroSchema, avroRecordWrong);
  }


  private static NonRecordContainer checkNonRecordConversion(
      org.apache.avro.Schema expectedSchema, Object expected,
      Schema schema, Object value)
  {
    Object converted = AvroData.fromCopycatData(schema, value);
    assertTrue(converted instanceof NonRecordContainer);
    NonRecordContainer container = (NonRecordContainer) converted;
    assertEquals(expectedSchema, container.getSchema());
    assertEquals(expected, container.getValue());
    return container;
  }
}
