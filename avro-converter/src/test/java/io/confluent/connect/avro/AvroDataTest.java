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

package io.confluent.connect.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.hamcrest.core.IsEqual;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

import io.confluent.kafka.serializers.NonRecordContainer;

import static io.confluent.connect.avro.AvroData.AVRO_TYPE_ENUM;
import static io.confluent.connect.avro.AvroData.CONNECT_ENUM_DOC_PROP;
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

  private AvroData avroData = new AvroData(2);

  // Connect -> Avro

  @Test
  public void testFromConnectBoolean() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
    checkNonRecordConversion(avroSchema, true, Schema.BOOLEAN_SCHEMA, true);

    checkNonRecordConversionNull(Schema.OPTIONAL_BOOLEAN_SCHEMA);
  }

  @Test
  public void testFromConnectByte() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.type", "int8");
    checkNonRecordConversion(avroSchema, 12, Schema.INT8_SCHEMA, (byte) 12);

    checkNonRecordConversionNull(Schema.OPTIONAL_INT8_SCHEMA);
  }

  @Test
  public void testFromConnectShort() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.type", "int16");
    checkNonRecordConversion(avroSchema, 12, Schema.INT16_SCHEMA, (short) 12);

    checkNonRecordConversionNull(Schema.OPTIONAL_INT16_SCHEMA);
  }

  @Test
  public void testFromConnectInteger() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    checkNonRecordConversion(avroSchema, 12, Schema.INT32_SCHEMA, 12);

    checkNonRecordConversionNull(Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void testFromConnectLong() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    checkNonRecordConversion(avroSchema, 12L, Schema.INT64_SCHEMA, 12L);

    checkNonRecordConversionNull(Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void testFromConnectFloat() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().floatType();
    checkNonRecordConversion(avroSchema, 12.2f, Schema.FLOAT32_SCHEMA, 12.2f);

    checkNonRecordConversionNull(Schema.OPTIONAL_FLOAT32_SCHEMA);
  }

  @Test
  public void testFromConnectDouble() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
    checkNonRecordConversion(avroSchema, 12.2, Schema.FLOAT64_SCHEMA, 12.2);

    checkNonRecordConversionNull(Schema.OPTIONAL_FLOAT64_SCHEMA);
  }

  @Test
  public void testFromConnectBytes() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    checkNonRecordConversion(avroSchema, ByteBuffer.wrap("foo".getBytes()),
                             Schema.BYTES_SCHEMA, "foo".getBytes());

    checkNonRecordConversionNull(Schema.OPTIONAL_BYTES_SCHEMA);
  }

  @Test
  public void testFromConnectString() {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().stringType();
    checkNonRecordConversion(avroSchema, "string", Schema.STRING_SCHEMA, "string");

    checkNonRecordConversionNull(Schema.OPTIONAL_STRING_SCHEMA);
  }

  @Test
  public void testFromConnectComplex() {
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

    Object convertedRecord = avroData.fromConnectData(schema, struct);

    org.apache.avro.Schema complexMapElementSchema =
        org.apache.avro.SchemaBuilder
            .record("MapEntry").namespace("io.confluent.connect.avro").fields()
            .requiredInt("key")
            .requiredInt("value")
            .endRecord();

    // One field has some extra data set on it to ensure it gets passed through via the fields
    // config
    org.apache.avro.Schema int8Schema = org.apache.avro.SchemaBuilder.builder().intType();
    int8Schema.addProp("connect.doc", "int8 field");
    int8Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(2));
    int8Schema.addProp("connect.type", "int8");
    org.apache.avro.Schema int16Schema = org.apache.avro.SchemaBuilder.builder().intType();
    int16Schema.addProp("connect.type", "int16");
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
  public void testFromConnectOptionalComplex() {
    Schema optionalStructSchema = SchemaBuilder.struct().optional()
        .name("optionalStruct")
        .field("int32", Schema.INT32_SCHEMA)  
        .build();
   
    Schema schema = SchemaBuilder.struct()
        .field("int32", Schema.INT32_SCHEMA)
        .field("optionalStruct", optionalStructSchema)
        .field("optionalArray",  SchemaBuilder.array(Schema.INT32_SCHEMA).optional().build())
        .field("optionalMap", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.INT32_SCHEMA).optional().build())
        .field("optionalMapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA,Schema.INT32_SCHEMA).optional().build())
        
        .build();
    
    Struct struct = new Struct(schema)
        .put("int32", 12)
        .put("optionalStruct", new Struct(optionalStructSchema).put("int32", 12))
        .put("optionalArray", Arrays.asList( 12,  13))
        .put("optionalMap", Collections.singletonMap("field", 12))
        .put("optionalMapNonStringKeys", Collections.singletonMap(123, 12)); 
    
    Object convertedRecord = avroData.fromConnectData(schema, struct);
    
    org.apache.avro.Schema structAvroSchema = org.apache.avro.SchemaBuilder.builder().record("optionalStruct").fields().requiredInt("int32").endRecord();
    
    // Maps with non-string keys get converted into an array of records with key & values fields 
    org.apache.avro.Schema mapNonStringKeysAvroSchema = org.apache.avro.SchemaBuilder.builder()
        .record(AvroData.MAP_ENTRY_TYPE_NAME).namespace(AvroData.NAMESPACE).fields()
        .requiredInt((AvroData.KEY_FIELD))
        .requiredInt((AvroData.VALUE_FIELD))
        .endRecord();
    
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);

    
    org.apache.avro.generic.GenericRecord  avroStruct = new GenericRecordBuilder(structAvroSchema)
                                                          .set("int32", 12)
                                                          .build();
    org.apache.avro.generic.GenericRecord  mapNonStringKeysAvroStruct = new GenericRecordBuilder(mapNonStringKeysAvroSchema)
        .set(AvroData.KEY_FIELD, 123)
        .set(AvroData.VALUE_FIELD, 12)
        .build();
    
    List<GenericRecord> mapNonStringKeys  = new ArrayList<GenericRecord>();
    mapNonStringKeys.add(mapNonStringKeysAvroStruct);
    
    org.apache.avro.generic.GenericRecord avroRecord = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("int32", 12)
        .set("optionalStruct", avroStruct)
        .set("optionalArray",  Arrays.asList( 12,  13))
        .set("optionalMap", Collections.singletonMap("field", 12))
        .set("optionalMapNonStringKeys", mapNonStringKeys)
        .build();

    assertEquals(avroRecord, convertedRecord);
    
    // Now check for null values
    struct = new Struct(schema)
        .put("int32", 12)
        .put("optionalStruct", null)
        .put("optionalArray", null)
        .put("optionalMap",null)
        .put("optionalMapNonStringKeys", null);
    
    convertedRecord = avroData.fromConnectData(schema, struct);
    
    avroRecord = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("int32", 12)
        .set("optionalStruct", null)
        .set("optionalArray",  null)
        .set("optionalMap", null)
        .set("optionalMapNonStringKeys", null)
        .build();
    
    assertEquals(avroRecord, convertedRecord);
  }

  @Test
  public void testFromConnectDataOptionalArrayMultiTypes() {
    org.apache.avro.Schema schemaRecA = org.apache.avro.SchemaBuilder.builder()
            .record("nestedRecordA").fields().requiredString("a").endRecord();
    org.apache.avro.Schema schemaRecB = org.apache.avro.SchemaBuilder.builder()
            .record("nestedRecordB").fields().requiredString("b").endRecord();

    org.apache.avro.Schema multiTypeSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
            .type(schemaRecA).and()
            .type(schemaRecB)
            .endUnion();

    org.apache.avro.Schema schema = org.apache.avro.SchemaBuilder.builder()
              .record("RecordsHolder").fields()
              .name("nestedRecords")
              .type(org.apache.avro.SchemaBuilder
                    .builder().nullable().array().items()
                    .type(multiTypeSchema))
              .noDefault()
              .endRecord();


    GenericRecord recA = new GenericRecordBuilder(schemaRecA)
            .set("a","aValue").build();
    GenericRecord recB = new GenericRecordBuilder(schemaRecB)
            .set("b","bValue").build();

    GenericRecord avroRecord = new GenericRecordBuilder(schema)
            .set("nestedRecords", Arrays.asList(recA, recB))
            .build();
    SchemaAndValue res = avroData.toConnectData(schema, avroRecord);

    Object convertedRecord = avroData.fromConnectData(res.schema(),res.value()); // should not throw an exception
    assertNotNull(convertedRecord);
  }

  @Test
  public void testFromConnectOptionalPrimitiveWithMetadata() {
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
    avroStringSchema.addProp("connect.name", "io.confluent.stringtype");
    avroStringSchema.addProp("connect.version",
                             JsonNodeFactory.instance.numberNode(2));
    avroStringSchema.addProp("connect.doc", "doc");
    avroStringSchema.addProp("connect.default", "foo");
    ObjectNode params = JsonNodeFactory.instance.objectNode();
    params.put("foo", "bar");
    params.put("baz", "baz");
    avroStringSchema.addProp("connect.parameters", params);
    org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().unionOf()
            .type(avroStringSchema).and()
            .nullType().endUnion();

    NonRecordContainer converted = checkNonRecordConversion(avroSchema, "string",
                                                            schema, "string");
    assertNotEquals(wrongAvroSchema, converted.getSchema());

    // Validate null is correctly translated to null again
    checkNonRecordConversionNull(schema);
  }

  @Test
  public void testFromConnectRecordWithMetadata() {
    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.TestSchema").version(12).doc("doc")
        .field("int32", Schema.INT32_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("int32", 12);

    Object convertedRecord = avroData.fromConnectData(schema, struct);

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("TestSchema").namespace("io.confluent.test")
        .fields()
        .requiredInt("int32")
        .endRecord();
    avroSchema.addProp("connect.name", "io.confluent.test.TestSchema");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(12));
    avroSchema.addProp("connect.doc", "doc");
    org.apache.avro.generic.GenericRecord avroRecord
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("int32", 12)
        .build();

    assertEquals(avroSchema, ((org.apache.avro.generic.GenericRecord) convertedRecord).getSchema());
    assertEquals(avroRecord, convertedRecord);
  }

  private static org.apache.avro.Schema createDecimalSchema(boolean required, int precision) {
    org.apache.avro.Schema avroSchema
        = required ? org.apache.avro.SchemaBuilder.builder().bytesType() :
          org.apache.avro.SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion();
    org.apache.avro.Schema decimalSchema = required ? avroSchema : avroSchema.getTypes().get(1);
    decimalSchema.addProp("scale", new IntNode(2));
    decimalSchema.addProp("precision", new IntNode(precision));
    decimalSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    ObjectNode avroParams = JsonNodeFactory.instance.objectNode();
    avroParams.put("scale", "2");
    avroParams.put(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, new Integer(precision).toString());
    decimalSchema.addProp("connect.parameters", avroParams);
    decimalSchema.addProp("connect.name", "org.apache.kafka.connect.data.Decimal");
    decimalSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL);

    return avroSchema;
  }

  @Test
  public void testFromConnectLogicalDecimal() {
    org.apache.avro.Schema avroSchema = createDecimalSchema(true, 64);
    NonRecordContainer container = checkNonRecordConversion(avroSchema, ByteBuffer.wrap(TEST_DECIMAL_BYTES),
                             Decimal.builder(2).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64").build(), TEST_DECIMAL);
    checkNonRecordConversionNull(Decimal.builder(2).optional().build());
  }

  @Test
  public void testFromConnectLogicalDate() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Date");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE);
    checkNonRecordConversion(avroSchema, 10000, Date.SCHEMA,
                             EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime());
  }

  @Test
  public void testFromConnectLogicalTime() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Time");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS);
    checkNonRecordConversion(avroSchema, 10000, Time.SCHEMA,
                             EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime());
  }

  @Test
  public void testFromConnectLogicalTimestamp() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Timestamp");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS);
    java.util.Date date = new java.util.Date();
    checkNonRecordConversion(avroSchema, date.getTime(), Timestamp.SCHEMA, date);
  }

  @Test(expected = DataException.class)
  public void testFromConnectMismatchSchemaPrimitive() {
    avroData.fromConnectData(Schema.OPTIONAL_BOOLEAN_SCHEMA, 12);
  }

  @Test(expected = DataException.class)
  public void testFromConnectMismatchSchemaPrimitiveRequired() {
    avroData.fromConnectData(Schema.BOOLEAN_SCHEMA, null);
  }

  @Test(expected = DataException.class)
  public void testFromConnectMismatchSchemaArray() {
    avroData.fromConnectData(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build(), Arrays.asList(12));
  }

  @Test(expected = DataException.class)
  public void testFromConnectMismatchSchemaMapWithStringKeyMismatchKey() {
    avroData.fromConnectData(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
                             Collections.singletonMap(true, 12));
  }

  @Test(expected = DataException.class)
  public void testFromConnectMismatchSchemaMapWithStringKeyMismatchValue() {
    avroData.fromConnectData(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
                             Collections.singletonMap("foobar", 12L));
  }

  @Test(expected = DataException.class)
  public void testFromConnectMismatchSchemaMapWithNonStringKeyMismatchKey() {
    avroData.fromConnectData(SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(),
                             Collections.singletonMap(true, 12));
  }

  @Test(expected = DataException.class)
  public void testFromConnectMismatchSchemaMapWithNonStringKeyMismatchValue() {
    avroData.fromConnectData(SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(),
                             Collections.singletonMap(12, 12L));
  }

  @Test(expected = DataException.class)
  public void testFromConnectMismatchSchemaRecord() {
    Schema firstSchema = SchemaBuilder.struct()
        .field("foo", Schema.BOOLEAN_SCHEMA)
        .build();
    Schema secondSchema = SchemaBuilder.struct()
        .field("foo", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    avroData.fromConnectData(firstSchema, new Struct(secondSchema).put("foo", null));
  }
  
  @Test(expected = DataException.class)
  public void testToConnectRecordWithIllegalNullValue() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("Record").fields()
        .requiredString("string")
        .endRecord();
    GenericRecord avroRecord = new GenericRecordBuilder(avroSchema).set("string", "some value").build();
    avroRecord.put("string", null);    
    avroData.toConnectData(avroSchema, avroRecord);
  }

  @Test
  public void testFromConnectSchemaless() {
    // Null has special handling. We do *not* want to get back ANYTHING_SCHEMA because we're going
    // to discard it anyway. We should just be passing through the null value.
    Object nullConverted = avroData.fromConnectData(null, null);
    assertNull(nullConverted);

    checkNonRecordConversionNull(null);

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

    GenericRecord avroNullRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).build();
    GenericRecord avroArrayRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("array", Arrays.asList(avroIntRecord, avroStringRecord, avroNullRecord))
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroArrayRecord,
                             null, Arrays.asList(12, "teststring", null));

    GenericRecord avroMapEntry = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
        .set("key", avroIntRecord)
        .set("value", avroStringRecord)
        .build();
    GenericRecord avroMapEntryNull = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
        .set("key", new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).set("int", 13).build())
        .set("value", avroNullRecord)
        .build();
    GenericRecord avroMapRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("map", Arrays.asList(avroMapEntry, avroMapEntryNull))
        .build();
    HashMap<Object, Object> convertedMap = new HashMap<>();
    convertedMap.put(12, "teststring");
    convertedMap.put(13, null);
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroMapRecord,
                             null, convertedMap);
  }

  @Test
  public void testCacheSchemaFromConnectConversion() {
    Cache<org.apache.avro.Schema, Schema> cache =
        Whitebox.getInternalState(avroData, "fromConnectSchemaCache");
    assertEquals(0, cache.size());

    avroData.fromConnectData(Schema.BOOLEAN_SCHEMA, true);
    assertEquals(1, cache.size());

    avroData.fromConnectData(Schema.BOOLEAN_SCHEMA, true);
    assertEquals(1, cache.size());

    avroData.fromConnectData(Schema.OPTIONAL_BOOLEAN_SCHEMA, true);
    assertEquals(2, cache.size());

    // Should hit limit of cache
    avroData.fromConnectData(Schema.STRING_SCHEMA, "foo");
    assertEquals(2, cache.size());
  }

  // Avro -> Connect. Validate a) all Avro types that convert directly to Avro, b) specialized
  // Avro types where we can convert to a Connect type that doesn't have a corresponding Avro
  // type, and c) Avro types which need specialized transformation because there is no
  // corresponding Connect type.

  // Avro -> Connect: directly corresponding types

  @Test
  public void testToConnectBoolean() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
    assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true),
                 avroData.toConnectData(avroSchema, true));
  }

  @Test
  public void testToConnectInt32() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    assertEquals(new SchemaAndValue(Schema.INT32_SCHEMA, 12),
                 avroData.toConnectData(avroSchema, 12));
  }

  @Test
  public void testToConnectInt64() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 12L),
                 avroData.toConnectData(avroSchema, 12L));
  }

  @Test
  public void testToConnectFloat32() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().floatType();
    assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 12.f),
                 avroData.toConnectData(avroSchema, 12.f));
  }

  @Test
  public void testToConnectFloat64() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
    assertEquals(new SchemaAndValue(Schema.FLOAT64_SCHEMA, 12.0),
                 avroData.toConnectData(avroSchema, 12.0));
  }

  @Test
  public void testToConnectNullableStringNullvalue() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.nullable().stringType();
    assertEquals(null, avroData.toConnectData(avroSchema, null));
  }  
  
  @Test
  public void testToConnectNullableString() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.nullable().stringType();
    assertEquals(new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, "teststring"), 
            avroData.toConnectData(avroSchema, "teststring"));

    // Avro deserializer allows CharSequence, not just String, and returns Utf8 objects
    assertEquals(new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, "teststring"),
                 avroData.toConnectData(avroSchema, new Utf8("teststring")));
  }  
  
  @Test
  public void testToConnectString() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().stringType();
    assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "teststring"),
                 avroData.toConnectData(avroSchema, "teststring"));

    // Avro deserializer allows CharSequence, not just String, and returns Utf8 objects
    assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "teststring"),
                 avroData.toConnectData(avroSchema, new Utf8("teststring")));
  }

  @Test
  public void testToConnectBytes() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foo".getBytes())),
                 avroData.toConnectData(avroSchema, "foo".getBytes()));

    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foo".getBytes())),
                 avroData.toConnectData(avroSchema, ByteBuffer.wrap("foo".getBytes())));
  }

  @Test
  public void testToConnectArray() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().array()
        .items().intType();
    avroSchema.getElementType().addProp("connect.type", "int8");
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
    assertEquals(new SchemaAndValue(schema, Arrays.asList((byte) 12, (byte) 13)),
                 avroData.toConnectData(avroSchema, Arrays.asList(12, 13)));
  }
  
  @Test
  public void testToConnectMapStringKeys() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().map()
        .values().intType();
    avroSchema.getValueType().addProp("connect.type", "int8");
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT8_SCHEMA).build();
    assertEquals(new SchemaAndValue(schema, Collections.singletonMap("field", (byte) 12)),
                 avroData.toConnectData(avroSchema, Collections.singletonMap("field", 12)));
  }

  @Test
  public void testToConnectRecord() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("Record").fields()
        .requiredInt("int8")
        .requiredString("string")
        .endRecord();
    avroSchema.getField("int8").schema().addProp("connect.type", "int8");
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
                 avroData.toConnectData(avroSchema, avroRecord));
  }
  
  @Test
  public void testToConnectRecordWithOptionalValue() {
      testToConnectRecordWithOptional("sample string");
  }

  @Test
  public void testToConnectRecordWithOptionalNullValue() {
      testToConnectRecordWithOptional(null);
  }
  
  private void testToConnectRecordWithOptional(String value) {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("Record").fields()
        .requiredInt("int8")
        .optionalString("string")
        .endRecord();
    avroSchema.getField("int8").schema().addProp("connect.type", "int8");
    GenericRecord avroRecord = new GenericRecordBuilder(avroSchema)
        .set("int8", 12)
        .set("string", value)
        .build();
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.struct()
        .name("Record")
        .field("int8", Schema.INT8_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema).put("int8", (byte) 12).put("string", value);
    assertEquals(new SchemaAndValue(schema, struct),
                 avroData.toConnectData(avroSchema, avroRecord));
  }  

  @Test
  public void testToConnectRecordWithOptionalArrayValue() {    
      testToConnectRecordWithOptionalArray(Arrays.asList("test"));
  }

  @Test
  public void testToConnectRecordWithOptionalArrayNullValue() {    
      testToConnectRecordWithOptionalArray(null);
  }
  
  private void testToConnectRecordWithOptionalArray(java.util.List<String> value) {    
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("Record").fields()
        .optionalString("string")
        .name("array").type(org.apache.avro.SchemaBuilder.builder().nullable().array().items().stringType()).noDefault()
        .endRecord();
    GenericRecord avroRecord = new GenericRecordBuilder(avroSchema)
        .set("string", "xx")
        .set("array", value)
        .build();
    Schema schema = SchemaBuilder.struct()
        .name("Record")
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .build();
    Struct struct = new Struct(schema).put("string", "xx").put("array", value);
    assertEquals(new SchemaAndValue(schema, struct),
                 avroData.toConnectData(avroSchema, avroRecord));
  }
  
  @Test
  public void testToConnectNestedRecordWithOptionalRecordValue() {  
    org.apache.avro.Schema avroSchema  = nestedRecordAvroSchema();
    Schema schema = nestedRecordSchema();
    GenericRecord avroRecord = new GenericRecordBuilder(avroSchema)
        .set("nestedRecord", new GenericRecordBuilder(recordWithStringAvroSchema()).set("string", "xx").build())
        .build();
    Struct struct = new Struct(schema).put("nestedRecord", new Struct(recordWithStringSchema()).put("string", "xx"));
    assertEquals(new SchemaAndValue(schema, struct),
                  avroData.toConnectData(avroSchema, avroRecord));
  }

  @Test
  public void testToConnectNestedRecordWithOptionalRecordNullValue() {     
    org.apache.avro.Schema avroSchema  = nestedRecordAvroSchema();
    Schema schema = nestedRecordSchema();;
    GenericRecord avroRecord = new GenericRecordBuilder(avroSchema)
        .set("nestedRecord", null)
        .build();
    Struct struct = new Struct(schema).put("nestedRecord", null);
    assertEquals(new SchemaAndValue(schema, struct),
                  avroData.toConnectData(avroSchema, avroRecord));
  }

  private  org.apache.avro.Schema  recordWithStringAvroSchema(){
    return org.apache.avro.SchemaBuilder.builder().record("nestedRecord").fields().requiredString("string").endRecord();
  }
  private  org.apache.avro.Schema  nestedRecordAvroSchema(){
    org.apache.avro.Schema optionalRecordAvroSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
        .type(recordWithStringAvroSchema()).and()
        .nullType()
        .endUnion();
    return  org.apache.avro.SchemaBuilder.builder()
        .record("Record").fields()
        .name("nestedRecord").type(optionalRecordAvroSchema).noDefault()
        .endRecord();
  }
  private Schema recordWithStringSchema(){
    return  SchemaBuilder.struct().optional().name("nestedRecord").field("string", Schema.STRING_SCHEMA).build();
  }
  private Schema nestedRecordSchema(){
    return SchemaBuilder.struct()
        .name("Record")
        .field("nestedRecord", recordWithStringSchema())
        .build();
  }
  
  // Avro -> Connect: Connect logical types

  @Test
  public void testToConnectDecimal() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Decimal");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    ObjectNode avroParams = JsonNodeFactory.instance.objectNode();
    avroParams.put("scale", "2");
    avroSchema.addProp("connect.parameters", avroParams);
    assertEquals(new SchemaAndValue(Decimal.schema(2), TEST_DECIMAL),
                 avroData.toConnectData(avroSchema, TEST_DECIMAL_BYTES));
  }

  @Test
  public void testToConnectDecimalAvro() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL);
    avroSchema.addProp("precision", new IntNode(50));
    avroSchema.addProp("scale", new IntNode(2));

    final SchemaAndValue expected = new SchemaAndValue(
        Decimal.builder(2).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "50").build(),
        TEST_DECIMAL
    );

    final SchemaAndValue actual = avroData.toConnectData(avroSchema, TEST_DECIMAL_BYTES);
    assertThat("schema.parameters() does not match.",
        actual.schema().parameters(),
        IsEqual.equalTo(expected.schema().parameters())
    );
    assertEquals("schema does not match.", expected.schema(), actual.schema());
    assertEquals("value does not match.", expected.value(), actual.value());
  }

  @Test
  public void testToConnectDate() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Date");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    assertEquals(new SchemaAndValue(Date.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime()),
                 avroData.toConnectData(avroSchema, 10000));
  }

  @Test
  public void testToConnectDateAvro() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE);
    assertEquals(new SchemaAndValue(Date.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime()),
        avroData.toConnectData(avroSchema, 10000));
  }

  @Test
  public void testToConnectTime() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Time");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    assertEquals(new SchemaAndValue(Time.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime()),
                 avroData.toConnectData(avroSchema, 10000));
  }

  @Test
  public void testToConnectTimeAvro() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS);
    assertEquals(new SchemaAndValue(Time.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime()),
        avroData.toConnectData(avroSchema, 10000));
  }

  @Test
  public void testToConnectTimestamp() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Timestamp");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    java.util.Date date = new java.util.Date();
    assertEquals(new SchemaAndValue(Timestamp.SCHEMA, date),
                 avroData.toConnectData(avroSchema, date.getTime()));
  }

  @Test
  public void testToConnectTimestampAvro() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS);
    java.util.Date date = new java.util.Date();
    assertEquals(new SchemaAndValue(Timestamp.SCHEMA, date),
        avroData.toConnectData(avroSchema, date.getTime()));
  }

  // Avro -> Connect: Connect types with no corresponding Avro type

  @Test
  public void testToConnectInt8() {
    // int8 should have a special annotation and Avro will have decoded an Integer
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.type", "int8");
    assertEquals(new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 12),
                 avroData.toConnectData(avroSchema, 12));
  }

  @Test
  public void testToConnectInt16() {
    // int16 should have a special annotation and Avro will have decoded an Integer
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.type", "int16");
    assertEquals(new SchemaAndValue(Schema.INT16_SCHEMA, (short) 12),
                 avroData.toConnectData(avroSchema, 12));
  }

  @Test
  public void testToConnectMapNonStringKeys() {
    // Encoded as array of 2-tuple records. Use key and value types that require conversion to
    // make sure conversion of each element actually occurs. The more verbose construction of the
    // Avro schema avoids reuse of schemas, which is needed since after constructing the schemas
    // we set additional properties on them.
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().array()
        .items().record("MapEntry").namespace("io.confluent.connect.avro").fields()
        .name("key").type(org.apache.avro.SchemaBuilder.builder().intType()).noDefault()
        .name("value").type(org.apache.avro.SchemaBuilder.builder().intType()).noDefault()
        .endRecord();
    avroSchema.getElementType().getField("key").schema().addProp("connect.type", "int8");
    avroSchema.getElementType().getField("value").schema().addProp("connect.type", "int16");
    GenericRecord record = new GenericRecordBuilder(avroSchema.getElementType())
        .set("key", 12)
        .set("value", 16)
        .build();
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT16_SCHEMA).build();
    assertEquals(new SchemaAndValue(schema, Collections.singletonMap((byte) 12, (short) 16)),
                 avroData.toConnectData(avroSchema, Arrays.asList(record)));
  }

  @Test
  public void testToConnectMapOptionalValue() {
      testToConnectMapOptional("some value");      
  }

  @Test
  public void testToConnectMapOptionalNullValue() {
      testToConnectMapOptional(null);      
  }
      
  private void testToConnectMapOptional(String value) {
     // Encoded as array of 2-tuple records. Use key and value types that require conversion to
    // make sure conversion of each element actually occurs. The more verbose construction of the
    // Avro schema avoids reuse of schemas, which is needed since after constructing the schemas
    // we set additional properties on them.
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().array()
        .items().record("MapEntry").namespace("io.confluent.connect.avro").fields()
        .name("key").type(org.apache.avro.SchemaBuilder.builder().intType()).noDefault()
        .name("value").type(org.apache.avro.SchemaBuilder.builder().nullable().stringType()).noDefault()
        .endRecord();
    avroSchema.getElementType().getField("key").schema().addProp("connect.type", "int8");
    GenericRecord record = new GenericRecordBuilder(avroSchema.getElementType())
        .set("key", 12)
        .set("value", value)
        .build();
    // Use a value type which ensures we test conversion of elements. int8 requires extra
    // conversion steps but keeps the test simple.
    Schema schema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build();
    assertEquals(new SchemaAndValue(schema, Collections.singletonMap((byte) 12, value)),
                 avroData.toConnectData(avroSchema, Arrays.asList(record)));
  }  
  
  
  // Avro -> Connect: Avro types with no corresponding Connect type

  @Test(expected = DataException.class)
  public void testToConnectNull() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().nullType();
    // If we somehow did end up with a null schema and an actual value that let it get past the
    avroData.toConnectData(avroSchema, true);
  }

  @Test
  public void testToConnectFixed() {
    // Our conversion simply loses the fixed size information.
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .fixed("sample").size(4);
    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foob".getBytes())),
                 avroData.toConnectData(avroSchema, "foob".getBytes()));

    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foob".getBytes())),
                 avroData.toConnectData(avroSchema, ByteBuffer.wrap("foob".getBytes())));

    // test with actual fixed type
    assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foob".getBytes())),
            avroData.toConnectData(avroSchema, new GenericData.Fixed(avroSchema, "foob".getBytes())));
  }

  @Test
  public void testToConnectUnion() {
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
        .name("io.confluent.connect.avro.Union")
        .field("int", Schema.OPTIONAL_INT32_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .field("Test1", recordSchema1)
        .field("Test2", recordSchema2)
        .build();
    assertEquals(new SchemaAndValue(schema, new Struct(schema).put("int", 12)),
                 avroData.toConnectData(avroSchema, 12));
    assertEquals(new SchemaAndValue(schema, new Struct(schema).put("string", "teststring")),
                 avroData.toConnectData(avroSchema, "teststring"));

    Struct schema1Test = new Struct(schema).put("Test1", new Struct(recordSchema1).put("test", 12));
    GenericRecord record1Test = new GenericRecordBuilder(avroRecordSchema1).set("test", 12).build();
    Struct schema2Test = new Struct(schema).put("Test2", new Struct(recordSchema2).put("test", 12));
    GenericRecord record2Test = new GenericRecordBuilder(avroRecordSchema2).set("test", 12).build();
    assertEquals(new SchemaAndValue(schema, schema1Test),
                 avroData.toConnectData(avroSchema, record1Test));
    assertEquals(new SchemaAndValue(schema, schema2Test),
                 avroData.toConnectData(avroSchema, record2Test));
  }

  @Test
  public void testToConnectUnionWithEnhanced() {
    avroData = new AvroData(new AvroDataConfig.Builder()
                               .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
                               .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                               .build());
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
                                 .name("io.confluent.connect.avro.Union")
                                 .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                                 .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                                 .field("Test1", recordSchema1)
                                 .field("io.confluent.Test2", recordSchema2)
                                 .build();
    assertEquals(new SchemaAndValue(schema, new Struct(schema).put("int", 12)),
                 avroData.toConnectData(avroSchema, 12));
    assertEquals(new SchemaAndValue(schema, new Struct(schema).put("string", "teststring")),
                 avroData.toConnectData(avroSchema, "teststring"));

    Struct schema1Test = new Struct(schema).put("Test1", new Struct(recordSchema1).put("test", 12));
    GenericRecord record1Test = new GenericRecordBuilder(avroRecordSchema1).set("test", 12).build();
    Struct schema2Test = new Struct(schema).put("io.confluent.Test2", new Struct(recordSchema2).put("test", 12));
    GenericRecord record2Test = new GenericRecordBuilder(avroRecordSchema2).set("test", 12).build();
    assertEquals(new SchemaAndValue(schema, schema1Test),
                 avroData.toConnectData(avroSchema, record1Test));
    assertEquals(new SchemaAndValue(schema, schema2Test),
                 avroData.toConnectData(avroSchema, record2Test));
  }

  @Test(expected = DataException.class)
  public void testToConnectUnionRecordConflict() {
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
    avroData.toConnectData(avroSchema, recordTest);
  }
  
  @Test
  public void testToConnectUnionRecordConflictWithEnhanced() {
    // If the records have the same name but are in different namespaces, 
    // ensure these are handled without throwing exception
    avroData = new AvroData(new AvroDataConfig.Builder()
                                        .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
                                        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                                        .build());
    org.apache.avro.Schema avroRecordSchema1 = org.apache.avro.SchemaBuilder.builder()
        .record("Test1").fields().requiredInt("test").endRecord();
    org.apache.avro.Schema avroRecordSchema2 = org.apache.avro.SchemaBuilder.builder()
        .record("Test1").namespace("io.confluent").fields().requiredInt("test").endRecord();
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
        .type(avroRecordSchema1).and()
        .type(avroRecordSchema2)
        .endUnion();

    GenericRecord recordTest = new GenericRecordBuilder(avroRecordSchema1).set("test", 12).build();
    avroData.toConnectData(avroSchema, recordTest);
  }

  @Test
  public void testToConnectEnum() {
    // Enums are just converted to strings, original enum is preserved in parameters
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .enumeration("TestEnum").symbols("foo", "bar", "baz");
    SchemaBuilder builder = SchemaBuilder.string().name("TestEnum");
    builder.parameter(CONNECT_ENUM_DOC_PROP, null);
    builder.parameter(AVRO_TYPE_ENUM, "TestEnum");
    for(String enumSymbol : new String[]{"foo", "bar", "baz"}) {
      builder.parameter(AVRO_TYPE_ENUM+"."+enumSymbol, enumSymbol);
    }

    assertEquals(new SchemaAndValue(builder.build(), "bar"),
                 avroData.toConnectData(avroSchema, "bar"));
    assertEquals(new SchemaAndValue(builder.build(), "bar"),
                 avroData.toConnectData(avroSchema, new GenericData.EnumSymbol(avroSchema, "bar")));
  }

  @Test
  public void testToConnectOptionalPrimitiveWithConnectMetadata() {
    Schema schema = SchemaBuilder.string().
        doc("doc").defaultValue("foo").name("io.confluent.stringtype").version(2).optional()
        .parameter("foo", "bar").parameter("baz", "baz")
        .build();

    org.apache.avro.Schema avroStringSchema = org.apache.avro.SchemaBuilder.builder().stringType();
    avroStringSchema.addProp("connect.name", "io.confluent.stringtype");
    avroStringSchema.addProp("connect.version",
                             JsonNodeFactory.instance.numberNode(2));
    avroStringSchema.addProp("connect.doc", "doc");
    avroStringSchema.addProp("connect.default", "foo");
    ObjectNode params = JsonNodeFactory.instance.objectNode();
    params.put("foo", "bar");
    params.put("baz", "baz");
    avroStringSchema.addProp("connect.parameters", params);
    org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().unionOf()
            .type(avroStringSchema).and()
            .nullType().endUnion();


    assertEquals(new SchemaAndValue(schema, "string"),
                 avroData.toConnectData(avroSchema, "string"));
  }

  @Test
  public void testToConnectRecordWithMetadata() {
    // One important difference between record schemas in Avro and Connect is that Avro has some
    // per-field metadata (doc, default value) that Connect holds in the schema itself. We set
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
    avroSchema.addProp("connect.name", "io.confluent.test.TestSchema");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(12));
    avroSchema.addProp("connect.doc", "doc");
    org.apache.avro.generic.GenericRecord avroRecord
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("int32", 12)
        .build();

    assertEquals(new SchemaAndValue(schema, struct),
                 avroData.toConnectData(avroSchema, avroRecord));
  }

  @Test
  public void testToConnectSchemaless() {
    GenericRecord avroNullRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).build();
    assertEquals(new SchemaAndValue(null, null),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroNullRecord));

    GenericRecord avroIntRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("int", 12)
        .build();
    assertEquals(new SchemaAndValue(null, 12),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroIntRecord));

    GenericRecord avroLongRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("long", 12L)
        .build();
    assertEquals(new SchemaAndValue(null, 12L),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroLongRecord));

    GenericRecord avroFloatRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("float", 12.2f)
        .build();
    assertEquals(new SchemaAndValue(null, 12.2f),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroFloatRecord));

    GenericRecord avroDoubleRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("double", 12.2)
        .build();
    assertEquals(new SchemaAndValue(null, 12.2),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroDoubleRecord));

    GenericRecord avroBooleanRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("boolean", true)
        .build();
    assertEquals(new SchemaAndValue(null, true),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroBooleanRecord));

    GenericRecord avroStringRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("string", "teststring")
        .build();
    assertEquals(new SchemaAndValue(null, "teststring"),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroStringRecord));


    GenericRecord avroArrayRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("array", Arrays.asList(avroIntRecord, avroStringRecord, avroNullRecord))
        .build();
    assertEquals(new SchemaAndValue(null, Arrays.asList(12, "teststring", null)),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroArrayRecord));

    GenericRecord avroMapEntry = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
        .set("key", avroIntRecord)
        .set("value", avroStringRecord)
        .build();
    GenericRecord avroMapEntryNull = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
        .set("key", new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).set("int", 13).build())
        .set("value", avroNullRecord)
        .build();
    GenericRecord avroMapRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("map", Arrays.asList(avroMapEntry, avroMapEntryNull))
        .build();
    HashMap<Object, Object> convertedMap = new HashMap<>();
    convertedMap.put(12, "teststring");
    convertedMap.put(13, null);
    assertEquals(new SchemaAndValue(null, convertedMap),
                 avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroMapRecord));

  }

  @Test(expected = DataException.class)
  public void testToConnectSchemaMismatchPrimitive() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroData.toConnectData(avroSchema, 12L);
  }

  @Test(expected = DataException.class)
  public void testToConnectSchemaMismatchArray() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .array().items().stringType();
    avroData.toConnectData(avroSchema, Arrays.asList(1, 2, 3));
  }

  @Test(expected = DataException.class)
  public void testToConnectSchemaMismatchMapMismatchKey() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .map().values().intType();
    avroData.toConnectData(avroSchema, Collections.singletonMap(12, 12));
  }

  @Test(expected = DataException.class)
  public void testToConnectSchemaMismatchMapMismatchValue() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .map().values().intType();
    avroData.toConnectData(avroSchema, Collections.singletonMap("foo", 12L));
  }

  @Test(expected = DataException.class)
  public void testToConnectSchemaMismatchRecord() {
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

    avroData.toConnectData(avroSchema, avroRecordWrong);
  }

  @Test
  public void testCacheSchemaToConnectConversion() {
    Cache<Schema, org.apache.avro.Schema> cache =
        Whitebox.getInternalState(avroData, "toConnectSchemaCache");
    assertEquals(0, cache.size());

    avroData.toConnectData(org.apache.avro.SchemaBuilder.builder().booleanType(), true);
    assertEquals(1, cache.size());

    avroData.toConnectData(org.apache.avro.SchemaBuilder.builder().booleanType(), true);
    assertEquals(1, cache.size());

    avroData.toConnectData(org.apache.avro.SchemaBuilder.builder().intType(), 32);
    assertEquals(2, cache.size());

    // Should hit limit of cache
    avroData.toConnectData(org.apache.avro.SchemaBuilder.builder().stringType(), "foo");
    assertEquals(2, cache.size());
  }

  @Test
  public void testArrayOfRecordWithNullNamespace() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.array().items()
            .record("item").fields()
            .name("value").type().intType().noDefault()
            .endRecord();

    avroData.toConnectSchema(avroSchema);
  }

  @Test
  public void testLogicalTypeWithMatchingNameAndVersion() {
    // When we use a logical type, the builder we get sets a version. If a version is also included in the schema we're
    // converting, the conversion should still work as long as the versions match.
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Message\",\"namespace\":\"org.cmatta.kafka.connect.irc\",\"fields\":[{\"name\":\"createdat\",\"type\":{\"type\":\"long\",\"connect.doc\":\"When this message was received.\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\",\"logicalType\":\"timestamp-millis\"}}]}");
    avroData.toConnectSchema(schema);
  }

  @Test(expected = DataException.class)
  public void testLogicalTypeWithMismatchingName() {
    // When we use a logical type, the builder we get sets a version. If a version is also included in the schema we're
    // converting, a mismatch between the versions should cause an exception.
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Message\",\"namespace\":\"org.cmatta.kafka.connect.irc\",\"fields\":[{\"name\":\"createdat\",\"type\":{\"type\":\"long\",\"connect.doc\":\"When this message was received.\",\"connect.version\":1,\"connect.name\":\"com.custom.Timestamp\",\"logicalType\":\"timestamp-millis\"}}]}");
    avroData.toConnectSchema(schema);
  }

  @Test(expected = DataException.class)
  public void testLogicalTypeWithMismatchingVersion() {
    // When we use a logical type, the builder we get sets a version. If a version is also included in the schema we're
    // converting, a mismatch between the versions should cause an exception.
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Message\",\"namespace\":\"org.cmatta.kafka.connect.irc\",\"fields\":[{\"name\":\"createdat\",\"type\":{\"type\":\"long\",\"connect.doc\":\"When this message was received.\",\"connect.version\":2,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\",\"logicalType\":\"timestamp-millis\"}}]}");
    avroData.toConnectSchema(schema);
  }

  private NonRecordContainer checkNonRecordConversion(
      org.apache.avro.Schema expectedSchema, Object expected,
      Schema schema, Object value)
  {
    Object converted = avroData.fromConnectData(schema, value);
    assertTrue(converted instanceof NonRecordContainer);
    NonRecordContainer container = (NonRecordContainer) converted;
    assertEquals(expectedSchema, container.getSchema());
    assertEquals(expected, container.getValue());
    return container;
  }

  private void checkNonRecordConversionNull(Schema schema)
  {
    Object converted = avroData.fromConnectData(schema, null);
    assertNull(converted);
  }
}
