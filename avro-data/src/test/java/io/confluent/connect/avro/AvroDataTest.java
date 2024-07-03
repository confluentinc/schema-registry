/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.connect.avro;

import com.connect.avro.EnumStringUnion;
import com.connect.avro.EnumUnion;
import com.connect.avro.UserType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.util.LinkedHashMap;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import foo.bar.EnumTest;
import foo.bar.Kind;
import io.confluent.kafka.serializers.NonRecordContainer;

import static io.confluent.connect.avro.AvroData.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
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

  private static final Schema NAMED_MAP_SCHEMA =
      SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
          .name("foo.bar")
          .build();
  private static final org.apache.avro.Schema NAMED_AVRO_MAP_SCHEMA =
      org.apache.avro.SchemaBuilder.array()
          .prop(CONNECT_NAME_PROP, "foo.bar")
          .items(
              org.apache.avro.SchemaBuilder.record("foo.bar")
                  .prop(CONNECT_INTERNAL_TYPE_NAME, MAP_ENTRY_TYPE_NAME)
                  .fields()
                  .optionalString(KEY_FIELD)
                  .requiredInt(VALUE_FIELD)
                  .endRecord()
          );

  private AvroData avroData = new AvroData(2);

  // Connect -> Avro

  @Test
  public void testFromConnectBoolean() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
    checkNonRecordConversion(avroSchema, true, Schema.BOOLEAN_SCHEMA, true, avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_BOOLEAN_SCHEMA);
  }

  @Test
  public void testFromConnectByte() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.type", "int8");
    checkNonRecordConversion(avroSchema, 12, Schema.INT8_SCHEMA, (byte) 12, avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_INT8_SCHEMA);
  }

  @Test
  public void testFromConnectShort() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.type", "int16");
    checkNonRecordConversion(avroSchema, 12, Schema.INT16_SCHEMA, (short) 12, avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_INT16_SCHEMA);
  }

  @Test
  public void testFromConnectInteger() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    checkNonRecordConversion(avroSchema, 12, Schema.INT32_SCHEMA, 12, avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void testFromConnectLong() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    checkNonRecordConversion(avroSchema, 12L, Schema.INT64_SCHEMA, 12L, avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void testFromConnectFloat() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().floatType();
    checkNonRecordConversion(avroSchema, 12.2f, Schema.FLOAT32_SCHEMA, 12.2f, avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_FLOAT32_SCHEMA);
  }

  @Test
  public void testFromConnectDouble() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
    checkNonRecordConversion(avroSchema, 12.2, Schema.FLOAT64_SCHEMA, 12.2, avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_FLOAT64_SCHEMA);
  }

  @Test
  public void testFromConnectBytes() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    checkNonRecordConversion(avroSchema, ByteBuffer.wrap("foo".getBytes()),
                             Schema.BYTES_SCHEMA, "foo".getBytes(), avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_BYTES_SCHEMA);
  }

  @Test
  public void testFromConnectString() {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().stringType();
    checkNonRecordConversion(avroSchema, "string", Schema.STRING_SCHEMA, "string", avroData);

    checkNonRecordConversionNull(Schema.OPTIONAL_STRING_SCHEMA);
  }

  @Test
  public void testFromConnectEnum() {
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);

    org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().enumeration("enum").symbols("one","two","three");
    GenericData.EnumSymbol avroObj = new GenericData.EnumSymbol(avroSchema, "one");

    Map connectPropsMap = ImmutableMap.of("connect.enum.doc","null",
        "io.confluent.connect.avro.Enum","enum",
        "io.confluent.connect.avro.Enum.one", "one",
        "io.confluent.connect.avro.Enum.two","two",
        "io.confluent.connect.avro.Enum.three","three");
    avroSchema.addProp("connect.parameters", connectPropsMap);
    avroSchema.addProp("connect.name", "enum");
    SchemaAndValue schemaAndValue = avroData.toConnectData(avroSchema, avroObj);
    checkNonRecordConversion(avroSchema, avroObj, schemaAndValue.schema(), schemaAndValue.value(),
        avroData);
  }

  @Test
  public void testFromConnectEnumWithGeneralizedSumTypeSupport() {
    avroData = new AvroData(new AvroDataConfig.Builder()
        .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
        .with(AvroDataConfig.GENERALIZED_SUM_TYPE_SUPPORT_CONFIG, true)
        .build());
    // Enums are just converted to strings, original enum is preserved in parameters
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .enumeration("TestEnum")
        .doc("some documentation")
        .symbols("foo", "bar", "baz");
    Map<String, String> params = new LinkedHashMap<>();
    params.put("io.confluent.connect.avro.enum.doc.TestEnum", "some documentation");
    params.put("org.apache.kafka.connect.data.Enum", "TestEnum");
    params.put("org.apache.kafka.connect.data.Enum.foo", "0");
    params.put("org.apache.kafka.connect.data.Enum.bar", "1");
    params.put("org.apache.kafka.connect.data.Enum.baz", "2");
    avroSchema.addProp("connect.parameters", params);
    avroSchema.addProp("connect.name", "TestEnum");
    SchemaBuilder builder = SchemaBuilder.string().name("TestEnum");
    builder.parameter(AVRO_ENUM_DOC_PREFIX_PROP + "TestEnum", "some documentation");
    builder.parameter(GENERALIZED_TYPE_ENUM, "TestEnum");
    int i = 0;
    for(String enumSymbol : new String[]{"foo", "bar", "baz"}) {
      builder.parameter(GENERALIZED_TYPE_ENUM+"."+enumSymbol, String.valueOf(i++));
    }

    checkNonRecordConversion(avroSchema, new GenericData.EnumSymbol(avroSchema, "bar"),
        builder.build(), "bar", avroData);
  }

  @Test
  public void testFromConnectMapWithStringKey() {
    final Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA);
    final org.apache.avro.Schema expected = org.apache.avro.SchemaBuilder.map()
        .values(org.apache.avro.SchemaBuilder.builder().intType());
    assertThat(avroData.fromConnectSchema(schema), equalTo(expected));
  }

  @Test
  public void testFromConnectMapWithOptionalKey() {
    final Schema schema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA);
    final org.apache.avro.Schema expected = org.apache.avro.SchemaBuilder.array()
        .items(
            org.apache.avro.SchemaBuilder.record(NAMESPACE + "." + MAP_ENTRY_TYPE_NAME)
                .fields()
                .optionalString(KEY_FIELD)
                .requiredInt(VALUE_FIELD)
                .endRecord()
        );

    assertThat(avroData.fromConnectSchema(schema), equalTo(expected));
  }

  @Test
  public void testFromConnectMapWithAllowOptionalKey() {
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.ALLOW_OPTIONAL_MAP_KEYS_CONFIG, true)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);

    final Schema schema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    final org.apache.avro.Schema expected = org.apache.avro.SchemaBuilder.map()
        .values(org.apache.avro.SchemaBuilder.unionOf().nullType().and().stringType().endUnion());

    assertThat(avroData.fromConnectSchema(schema), equalTo(expected));
  }

  @Test
  public void testFromConnectMapWithNonStringKey() {
    final Schema schema = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA);
    final org.apache.avro.Schema expected = org.apache.avro.SchemaBuilder.array()
        .items(
            org.apache.avro.SchemaBuilder.record(NAMESPACE + "." + MAP_ENTRY_TYPE_NAME)
                .fields()
                .requiredInt(KEY_FIELD)
                .requiredInt(VALUE_FIELD)
                .endRecord()
        );

    assertThat(avroData.fromConnectSchema(schema), equalTo(expected));
  }

  @Test
  public void testFromNamedConnectMapWithNonStringKey() {
    assertThat(avroData.fromConnectSchema(NAMED_MAP_SCHEMA), equalTo(NAMED_AVRO_MAP_SCHEMA));
  }

  @Test
  public void testFromConnectBytesFixed() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().fixed("sample").size(4);
    GenericData.Fixed avroObj = new GenericData.Fixed(avroSchema, "foob".getBytes());
    avroSchema.addProp("connect.parameters", ImmutableMap.of("connect.fixed.size", "4"));
    avroSchema.addProp("connect.name", "sample");
    SchemaAndValue schemaAndValue = avroData.toConnectData(avroSchema, avroObj);
    checkNonRecordConversion(avroSchema, avroObj, schemaAndValue.schema(), schemaAndValue.value(), avroData);
  }

  @Test
  public void testFromConnectFixedUnion() {
    org.apache.avro.Schema sampleSchema = org.apache.avro.SchemaBuilder.builder().fixed("sample")
        .size(4);
    org.apache.avro.Schema otherSchema = org.apache.avro.SchemaBuilder.builder().fixed("other")
        .size(6);
    org.apache.avro.Schema sameOtherSchema = org.apache.avro.SchemaBuilder.builder()
        .fixed("sameOther").size(6);
    org.apache.avro.Schema unionSchema = org.apache.avro.SchemaBuilder.builder()
        .unionOf().type(sampleSchema).and().type(otherSchema).and().type(sameOtherSchema)
        .endUnion();
    Schema union = SchemaBuilder.struct()
        .name(AVRO_TYPE_UNION)
        .field("sample",
            SchemaBuilder.bytes().name("sample").parameter(CONNECT_AVRO_FIXED_SIZE_PROP, "4").optional()
                .build())
        .field("other",
            SchemaBuilder.bytes().name("other").parameter(CONNECT_AVRO_FIXED_SIZE_PROP, "6").optional()
                .build())
        .field("sameOther",
            SchemaBuilder.bytes().name("sameOther").parameter(CONNECT_AVRO_FIXED_SIZE_PROP, "6")
                .optional().build())
        .build();
    Struct unionSample = new Struct(union).put("sample", ByteBuffer.wrap("foob".getBytes()));
    Struct unionOther = new Struct(union).put("other", ByteBuffer.wrap("foobar".getBytes()));
    Struct unionSameOther = new Struct(union)
        .put("sameOther", ByteBuffer.wrap("foobar".getBytes()));

    GenericData genericData = GenericData.get();
    assertEquals(0,
        genericData.resolveUnion(unionSchema, avroData.fromConnectData(union, unionSample)));
    assertEquals(1,
        genericData.resolveUnion(unionSchema, avroData.fromConnectData(union, unionOther)));
    assertEquals(2,
        genericData.resolveUnion(unionSchema, avroData.fromConnectData(union, unionSameOther)));
  }

  @Test
  public void testFromConnectUnionWithGeneralizedSumTypeSupport() {
    avroData = new AvroData(new AvroDataConfig.Builder()
        .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
        .with(AvroDataConfig.GENERALIZED_SUM_TYPE_SUPPORT_CONFIG, true)
        .build());
    // Make sure we handle primitive types and named types properly by using a variety of types
    org.apache.avro.Schema avroRecordSchema1 = org.apache.avro.SchemaBuilder.builder()
        .record("Test1").fields().requiredInt("test").endRecord();
    // Add connect name
    avroRecordSchema1.addProp("connect.name", "Test1");
    org.apache.avro.Schema avroRecordSchema2 = org.apache.avro.SchemaBuilder.builder()
        .record("Test2").namespace("io.confluent").fields().requiredInt("test").endRecord();
    // Add connect name
    avroRecordSchema2.addProp("connect.name", "io.confluent.Test2");
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
        .name("connect_union_0")
        .parameter("org.apache.kafka.connect.data.Union", "connect_union_0")
        .field("connect_union_field_0", Schema.OPTIONAL_INT32_SCHEMA)
        .field("connect_union_field_1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("connect_union_field_2", recordSchema1)
        .field("connect_union_field_3", recordSchema2)
        .build();
    assertEquals(12,
        avroData.fromConnectData(schema, new Struct(schema).put("connect_union_field_0", 12)));
    assertEquals("teststring",
        avroData.fromConnectData(schema, new Struct(schema).put("connect_union_field_1", "teststring")));

    Struct schema1Test = new Struct(schema).put("connect_union_field_2", new Struct(recordSchema1).put("test", 12));
    GenericRecord record1Test = new GenericRecordBuilder(avroRecordSchema1).set("test", 12).build();
    Struct schema2Test = new Struct(schema).put("connect_union_field_3", new Struct(recordSchema2).put("test", 12));
    GenericRecord record2Test = new GenericRecordBuilder(avroRecordSchema2).set("test", 12).build();
    assertEquals(record1Test, avroData.fromConnectData(schema, schema1Test));
    assertEquals(record2Test, avroData.fromConnectData(schema, schema2Test));
  }

  @Test
  public void testFromConnectWithInvalidName() {
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.SCRUB_INVALID_NAMES_CONFIG, true)
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);
    Schema schema = SchemaBuilder.struct()
        .name("org.acme-corp.invalid record-name")
        .field("invalid field-name", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema);
    struct.put("invalid field-name", "foo");
    org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder
        .record("invalid_record_name").namespace("org.acme_corp") // default values
        .fields()
        .requiredString("invalid_field_name")
        .endRecord();
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
    assertEquals(expectedAvroSchema, avroSchema);
    GenericRecord convertedRecord = (GenericRecord) avroData.fromConnectData(schema, struct);
    assertEquals("invalid_record_name", avroSchema.getName());
    assertEquals("invalid_field_name", avroSchema.getFields().get(0).name());
    assertEquals("invalid_record_name", convertedRecord.getSchema().getName());
    assertEquals("invalid_field_name", convertedRecord.getSchema().getFields().get(0).name());
    assertEquals("foo", convertedRecord.get("invalid_field_name"));
  }

  @Test
  public void testFromConnectOptionalRecordWithInvalidName() {
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.SCRUB_INVALID_NAMES_CONFIG, true)
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);
    Schema schema = SchemaBuilder.struct()
        .name("invalid record-name")
        .optional()
        .field("invalid field-name", SchemaBuilder.bool().optional().defaultValue(null).build())
        .build();
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
    org.apache.avro.Schema recordSchema = org.apache.avro.SchemaBuilder.builder()
        .record("invalid_record_name").fields()
        .name("invalid_field_name").type(org.apache.avro.SchemaBuilder.builder()
            .unionOf().nullType().and().booleanType().endUnion()).withDefault(null)
        .endRecord();
    org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder.builder()
        .unionOf().nullType().and()
        .record("invalid_record_name").fields()
        .name("invalid_field_name").type(org.apache.avro.SchemaBuilder.builder()
            .unionOf().nullType().and().booleanType().endUnion()).withDefault(null)
        .endRecord()
        .endUnion();

    assertEquals(expectedAvroSchema, avroSchema);

    Struct struct = new Struct(schema)
        .put("invalid field-name", true);
    Object convertedRecord = avroData.fromConnectData(schema, struct);
    org.apache.avro.generic.GenericRecord avroRecord = new org.apache.avro.generic.GenericRecordBuilder(recordSchema)
        .set("invalid_field_name", true)
        .build();

    assertEquals(avroRecord, convertedRecord);
  }

  @Test
  public void testFromConnectOptionalRecordWithNullName() {
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.SCRUB_INVALID_NAMES_CONFIG, true)
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);
    Schema schema = SchemaBuilder.struct()
        .optional()
        .field("invalid field-name", SchemaBuilder.bool().optional().defaultValue(null).build())
        .build();
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
    org.apache.avro.Schema recordSchema = org.apache.avro.SchemaBuilder.builder()
        .record("ConnectDefault").namespace("io.confluent.connect.avro").fields()
        .name("invalid_field_name").type(org.apache.avro.SchemaBuilder.builder()
            .unionOf().nullType().and().booleanType().endUnion()).withDefault(null)
        .endRecord();
    org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder.builder()
        .unionOf().nullType().and()
        .record("ConnectDefault").namespace("io.confluent.connect.avro").fields()
        .name("invalid_field_name").type(org.apache.avro.SchemaBuilder.builder()
            .unionOf().nullType().and().booleanType().endUnion()).withDefault(null)
        .endRecord()
        .endUnion();

    assertEquals(expectedAvroSchema, avroSchema);

    Struct struct = new Struct(schema)
        .put("invalid field-name", true);
    Object convertedRecord = avroData.fromConnectData(schema, struct);
    org.apache.avro.generic.GenericRecord avroRecord = new org.apache.avro.generic.GenericRecordBuilder(recordSchema)
        .set("invalid_field_name", true)
        .build();

    assertEquals(avroRecord, convertedRecord);
  }

  @Test
  public void testNameScrubbing() {
    assertEquals(null, AvroData.doScrubName(null));
    assertEquals("", AvroData.doScrubName(""));
    assertEquals("abc_DEF_123", AvroData.doScrubName("abc_DEF_123")); // nothing to scrub

    assertEquals("abc_2B____", AvroData.doScrubName("abc+-.*_"));
    assertEquals("abc_def", AvroData.doScrubName("abc-def"));
    assertEquals("abc_2Bdef", AvroData.doScrubName("abc+def"));
    assertEquals("abc__def", AvroData.doScrubName("abc  def"));
    assertEquals("abc_def", AvroData.doScrubName("abc.def"));
    assertEquals("x0abc_def", AvroData.doScrubName("0abc.def")); // invalid start char
    assertEquals("x0abc", AvroData.doScrubName("0abc")); // (only) invalid start char
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
  public void testFromConnectComplexWithDefaultStructContainingNulls() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("int8", SchemaBuilder.int8().optional().doc("int8 field").build())
        .field("int16", SchemaBuilder.int16().optional().doc("int16 field").build())
        .field("int32", SchemaBuilder.int32().optional().doc("int32 field").build())
        .field("int64", SchemaBuilder.int64().optional().doc("int64 field").build())
        .field("float32", SchemaBuilder.float32().optional().doc("float32 field").build())
        .field("float64", SchemaBuilder.float64().optional().doc("float64 field").build())
        .field("boolean", SchemaBuilder.bool().optional().doc("bool field").build())
        .field("string", SchemaBuilder.string().optional().doc("string field").build())
        .field("bytes", SchemaBuilder.bytes().optional().doc("bytes field").build())
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
        .field("date", Date.builder().doc("date field").optional().build())
        .field("time", Time.builder().doc("time field").optional().build())
        .field("ts", Timestamp.builder().doc("ts field").optional().build())
        .field("decimal", Decimal.builder(5).doc("decimal field").optional().build());
    Struct defaultValue = new Struct(schemaBuilder);
    defaultValue.put("int8", (byte) 2);
    Schema schema2 = schemaBuilder
        .defaultValue(defaultValue)
        .build();
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema2);
    assertNotNull(avroSchema);
  }

  @Test
  public void testFromConnectComplexWithDefaults() {
    int dateDefVal = 100;
    int timeDefVal = 1000 * 60 * 60 * 2;
    long tsDefVal = 1000 * 60 * 60 * 24 * 365 + 100;
    java.util.Date dateDef = Date.toLogical(Date.SCHEMA, dateDefVal);
    java.util.Date timeDef = Time.toLogical(Time.SCHEMA, timeDefVal);
    java.util.Date tsDef = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal);
    BigDecimal decimalDef = new BigDecimal(BigInteger.valueOf(314159L), 5);
    byte[] decimalDefVal = decimalDef.unscaledValue().toByteArray();

    Schema schema = SchemaBuilder.struct()
                                 .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
                                 .field("int16", SchemaBuilder.int16().defaultValue((short)12).doc("int16 field").build())
                                 .field("int32", SchemaBuilder.int32().defaultValue(12).doc("int32 field").build())
                                 .field("int64", SchemaBuilder.int64().defaultValue(12L).doc("int64 field").build())
                                 .field("float32", SchemaBuilder.float32().defaultValue(12.2f).doc("float32 field").build())
                                 .field("float64", SchemaBuilder.float64().defaultValue(12.2).doc("float64 field").build())
                                 .field("boolean", SchemaBuilder.bool().defaultValue(true).doc("bool field").build())
                                 .field("string", SchemaBuilder.string().defaultValue("foo").doc("string field").build())
                                 .field("bytes", SchemaBuilder.bytes().defaultValue(ByteBuffer.wrap("foo".getBytes())).doc("bytes field").build())
                                 .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(Arrays.asList("a", "b", "c")).build())
                                 .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).defaultValue(Collections.singletonMap("field", 1)).build())
                                 .field("date", Date.builder().defaultValue(dateDef).doc("date field").build())
                                 .field("time", Time.builder().defaultValue(timeDef).doc("time field").build())
                                 .field("ts", Timestamp.builder().defaultValue(tsDef).doc("ts field").build())
                                 .field("decimal", Decimal.builder(5).defaultValue(decimalDef).doc("decimal field").build())
                                 .build();
    // leave the struct empty so that only defaults are used
    Struct struct = new Struct(schema)
        .put("int8", (byte) 2)
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
        .put("date", dateDef)
        .put("time", timeDef)
        .put("ts", tsDef)
        .put("decimal", decimalDef);

    // Define the expected Avro schema
    org.apache.avro.Schema complexMapElementSchema =
        org.apache.avro.SchemaBuilder
            .record("MapEntry").namespace("io.confluent.connect.avro").fields()
            .requiredInt("key")
            .requiredInt("value")
            .endRecord();

    org.apache.avro.Schema int8Schema = org.apache.avro.SchemaBuilder.builder().intType();
    int8Schema.addProp("connect.doc", "int8 field");
    int8Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(2));
    int8Schema.addProp("connect.type", "int8");
    org.apache.avro.Schema int16Schema = org.apache.avro.SchemaBuilder.builder().intType();
    int16Schema.addProp("connect.doc", "int16 field");
    int16Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(((short)12)).intValue());
    int16Schema.addProp("connect.type", "int16");
    org.apache.avro.Schema int32Schema = org.apache.avro.SchemaBuilder.builder().intType();
    int32Schema.addProp("connect.doc", "int32 field");
    int32Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12));
    org.apache.avro.Schema int64Schema = org.apache.avro.SchemaBuilder.builder().longType();
    int64Schema.addProp("connect.doc", "int64 field");
    int64Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12L));
    org.apache.avro.Schema float32Schema = org.apache.avro.SchemaBuilder.builder().floatType();
    float32Schema.addProp("connect.doc", "float32 field");
    float32Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12.2f));
    org.apache.avro.Schema float64Schema = org.apache.avro.SchemaBuilder.builder().doubleType();
    float64Schema.addProp("connect.doc", "float64 field");
    float64Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12.2));
    org.apache.avro.Schema boolSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
    boolSchema.addProp("connect.doc", "bool field");
    boolSchema.addProp("connect.default", JsonNodeFactory.instance.booleanNode(true));
    org.apache.avro.Schema stringSchema = org.apache.avro.SchemaBuilder.builder().stringType();
    stringSchema.addProp("connect.doc", "string field");
    stringSchema.addProp("connect.default", JsonNodeFactory.instance.textNode("foo"));
    org.apache.avro.Schema bytesSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    bytesSchema.addProp("connect.doc", "bytes field");
    bytesSchema.addProp("connect.default", JsonNodeFactory.instance.textNode(
        new String("foo".getBytes(), StandardCharsets.ISO_8859_1)
    ));

    org.apache.avro.Schema dateSchema = org.apache.avro.SchemaBuilder.builder().intType();
    dateSchema.addProp("connect.doc", "date field");
    dateSchema.addProp("connect.default", JsonNodeFactory.instance.numberNode(dateDefVal));
    dateSchema.addProp(AvroData.CONNECT_NAME_PROP, Date.LOGICAL_NAME);
    dateSchema.addProp(AvroData.CONNECT_VERSION_PROP, 1);
    // this is the new and correct way to set logical type
    LogicalTypes.date().addToSchema(dateSchema);
    // this is the old and wrong way to set logical type
    // leave the line here for back compatibility
    dateSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE);

    org.apache.avro.Schema timeSchema = org.apache.avro.SchemaBuilder.builder().intType();
    timeSchema.addProp("connect.doc", "time field");
    timeSchema.addProp("connect.default", JsonNodeFactory.instance.numberNode(timeDefVal));
    timeSchema.addProp(AvroData.CONNECT_NAME_PROP, Time.LOGICAL_NAME);
    timeSchema.addProp(AvroData.CONNECT_VERSION_PROP, 1);
    // this is the new and correct way to set logical type
    LogicalTypes.timeMillis().addToSchema(timeSchema);
    // this is the old and wrong way to set logical type
    // leave the line here for back compatibility
    timeSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS);

    org.apache.avro.Schema tsSchema = org.apache.avro.SchemaBuilder.builder().longType();
    tsSchema.addProp("connect.doc", "ts field");
    tsSchema.addProp("connect.default", JsonNodeFactory.instance.numberNode(tsDefVal));
    tsSchema.addProp(AvroData.CONNECT_NAME_PROP, Timestamp.LOGICAL_NAME);
    tsSchema.addProp(AvroData.CONNECT_VERSION_PROP, 1);
    // this is the new and correct way to set logical type
    LogicalTypes.timestampMillis().addToSchema(tsSchema);
    // this is the old and wrong way to set logical type
    // leave the line here for back compatibility
    tsSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS);

    org.apache.avro.Schema decimalSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    decimalSchema.addProp("scale", 5);
    decimalSchema.addProp("precision", 64);
    decimalSchema.addProp("connect.doc", "decimal field");
    decimalSchema.addProp(AvroData.CONNECT_VERSION_PROP, 1);
    decimalSchema.addProp("connect.default", JsonNodeFactory.instance.textNode(
        new String(decimalDefVal, StandardCharsets.ISO_8859_1)));
    decimalSchema.addProp("connect.parameters", parameters("scale", "5"));
    decimalSchema.addProp(AvroData.CONNECT_NAME_PROP, Decimal.LOGICAL_NAME);
    // this is the new and correct way to set logical type
    LogicalTypes.decimal(64, 5).addToSchema(decimalSchema);
    // this is the old and wrong way to set logical type
    // leave the line here for back compatibility
    decimalSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL);

    org.apache.avro.Schema arraySchema = org.apache.avro.SchemaBuilder.builder().array().items().stringType();
    ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
    arrayNode.add("a");
    arrayNode.add("b");
    arrayNode.add("c");
    arraySchema.addProp("connect.default", arrayNode);

    org.apache.avro.Schema mapSchema = org.apache.avro.SchemaBuilder.builder().map().values().intType();
    ObjectNode mapNode = JsonNodeFactory.instance.objectNode();
    mapNode.put("field", 1);
    mapSchema.addProp("connect.default", mapNode);

    org.apache.avro.Schema nonStringMapSchema = org.apache.avro.SchemaBuilder.builder().array()
                                                                             .items(complexMapElementSchema);
    ArrayNode nonStringMapNode = JsonNodeFactory.instance.arrayNode();
    nonStringMapNode.add(JsonNodeFactory.instance.numberNode(1));
    nonStringMapNode.add(JsonNodeFactory.instance.numberNode(1));
    ArrayNode nonStringMapArrayNode = JsonNodeFactory.instance.arrayNode();
    nonStringMapArrayNode.add(nonStringMapNode);
    nonStringMapSchema.addProp("connect.default", nonStringMapArrayNode);

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record(AvroData.DEFAULT_SCHEMA_NAME).namespace(AvroData.NAMESPACE) // default values
        .fields()
        .name("int8").type(int8Schema).withDefault(2)
        .name("int16").type(int16Schema).withDefault(12)
        .name("int32").type(int32Schema).withDefault(12)
        .name("int64").type(int64Schema).withDefault(12L)
        .name("float32").type(float32Schema).withDefault(12.2f)
        .name("float64").type(float64Schema).withDefault(12.2)
        .name("boolean").type(boolSchema).withDefault(true)
        .name("string").type(stringSchema).withDefault("foo")
        .name("bytes").type(bytesSchema).withDefault(ByteBuffer.wrap("foo".getBytes()))
        .name("array").type(arraySchema).withDefault(Arrays.asList("a", "b", "c"))
        .name("map").type(mapSchema).withDefault(Collections.singletonMap("field", 1))
        .name("date").type(dateSchema).withDefault(dateDefVal)
        .name("time").type(timeSchema).withDefault(timeDefVal)
        .name("ts").type(tsSchema).withDefault(tsDefVal)
        .name("decimal").type(decimalSchema).withDefault(ByteBuffer.wrap(decimalDefVal))
        .endRecord();
    org.apache.avro.generic.GenericRecord avroRecord
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("int8", 2)
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
        .set("date", dateDefVal)
        .set("time", timeDefVal)
        .set("ts", tsDefVal)
        .set("decimal", decimalDefVal)
        .build();

    SchemaAndValue schemaAndValue = new SchemaAndValue(schema, struct);
    schemaAndValue = convertToConnect(avroSchema, avroRecord, schemaAndValue);
    schemaAndValue = convertToConnect(avroSchema, avroRecord, schemaAndValue);
    schemaAndValue = convertToConnect(avroSchema, avroRecord, schemaAndValue);
    assertNotNull(schemaAndValue);
  }

  @Test
  public void testFromConnectOptionalAndNonOptionalStruct() {
    Schema innerSchema = SchemaBuilder.struct().name("inner")
        .field("value", Schema.INT64_SCHEMA).optional().build();
    Schema outerSchema = SchemaBuilder.struct().name("outer")
        .field("struct", innerSchema)
        .field("array", SchemaBuilder.array(innerSchema).build())
        .build();
    Struct innerStruct = new Struct(innerSchema)
        .put("value", 46421L);
    Struct outerStruct = new Struct(outerSchema)
        .put("struct", innerStruct)
        .put("array", Collections.singletonList(innerStruct));

    org.apache.avro.Schema int64Schema = org.apache.avro.SchemaBuilder.builder().longType();
    org.apache.avro.Schema recordSchema = org.apache.avro.SchemaBuilder.builder()
        .record("inner").fields().name("value").type(int64Schema).noDefault()
        .endRecord();
    recordSchema.addProp("connect.name", "inner");
    org.apache.avro.Schema unionSchema = org.apache.avro.SchemaBuilder.builder()
        .unionOf().nullType().and()
        .type(recordSchema)
        .endUnion();
    org.apache.avro.Schema arraySchema = org.apache.avro.SchemaBuilder.builder()
        .array().items(unionSchema);
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("outer")
        .fields()
        .name("struct").type(unionSchema).withDefault(null)
        .name("array").type(arraySchema).noDefault()
        .endRecord();
    avroSchema.addProp("connect.name", "outer");

    org.apache.avro.generic.GenericRecord innerRecord
        = new org.apache.avro.generic.GenericRecordBuilder(recordSchema)
        .set("value", 46421L)
        .build();
    org.apache.avro.generic.GenericRecord avroRecord
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("struct", innerRecord)
        .set("array", Collections.singletonList(innerRecord))
        .build();
    SchemaAndValue schemaAndValue = new SchemaAndValue(outerSchema, outerStruct);
    schemaAndValue = convertToConnect(avroSchema, avroRecord, schemaAndValue);
    assertNotNull(schemaAndValue);
  }

  @Test
  public void testFromConnectStructsWithNullName() {
    Schema innerSchema = SchemaBuilder.struct()
        .field("value", Schema.INT64_SCHEMA).optional().build();
    Schema outerSchema = SchemaBuilder.struct()
        .field("struct", innerSchema)
        .field("array", SchemaBuilder.array(innerSchema).build())
        .build();
    Struct innerStruct = new Struct(innerSchema)
        .put("value", 46421L);
    Struct outerStruct = new Struct(outerSchema)
        .put("struct", innerStruct)
        .put("array", Collections.singletonList(innerStruct));

    org.apache.avro.Schema int64Schema = org.apache.avro.SchemaBuilder.builder().longType();
    org.apache.avro.Schema recordSchema = org.apache.avro.SchemaBuilder.builder()
        .record("ConnectDefault2")
        .namespace("io.confluent.connect.avro")
        .fields().name("value").type(int64Schema).noDefault()
        .endRecord();
    org.apache.avro.Schema unionSchema = org.apache.avro.SchemaBuilder.builder()
        .unionOf().nullType().and()
        .type(recordSchema)
        .endUnion();
    org.apache.avro.Schema arraySchema = org.apache.avro.SchemaBuilder.builder()
        .array().items(unionSchema);
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("ConnectDefault")
        .namespace("io.confluent.connect.avro")
        .fields()
        .name("struct").type(unionSchema).withDefault(null)
        .name("array").type(arraySchema).noDefault()
        .endRecord();

    org.apache.avro.generic.GenericRecord innerRecord
        = new org.apache.avro.generic.GenericRecordBuilder(recordSchema)
        .set("value", 46421L)
        .build();
    org.apache.avro.generic.GenericRecord avroRecord
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("struct", innerRecord)
        .set("array", Collections.singletonList(innerRecord))
        .build();
    SchemaAndValue schemaAndValue = new SchemaAndValue(outerSchema, outerStruct);
    schemaAndValue = convertToConnect(avroSchema, avroRecord, schemaAndValue);
    assertNotNull(schemaAndValue);
  }

  protected SchemaAndValue convertToConnect(
      org.apache.avro.Schema expectedAvroSchema,
      org.apache.avro.generic.GenericRecord expectedAvroRecord,
      SchemaAndValue connectSchemaAndValue
  ) {
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(connectSchemaAndValue.schema());
    // Validate the schema
    assertEquals(avroSchema.toString(), new AvroSchema(avroSchema.toString()).toString());

    Object convertedRecord = avroData.fromConnectData(
        connectSchemaAndValue.schema(),
        connectSchemaAndValue.value()
    );

    org.apache.avro.generic.GenericRecord convertedAvroRecord =
        (org.apache.avro.generic.GenericRecord) convertedRecord;
    assertSchemaEquals(expectedAvroSchema, convertedAvroRecord.getSchema());
    assertSchemaEquals(expectedAvroRecord.getSchema(), convertedAvroRecord.getSchema());

    // This doesn't work because the long field's default value is an integer
    //assertEquals(avroRecord, convertedRecord);
    // We've already checked the schemas, so we just need to check the record field values
    for (org.apache.avro.Schema.Field field : expectedAvroSchema.getFields()) {
      Object actual = convertedAvroRecord.get(field.name());
      Object expected = expectedAvroRecord.get(field.name());
      assertValueEquals(expected, actual);
    }

    SchemaAndValue schemaAndValue = avroData.toConnectData(
        convertedAvroRecord.getSchema(),
        convertedRecord
    );
    assertEquals(connectSchemaAndValue.schema(), schemaAndValue.schema());
    assertEquals(connectSchemaAndValue.value(), schemaAndValue.value());
    return schemaAndValue;
  }

  @Test
  public void testFromConnectOptionalWithDefaultNull() {
    Schema schema = SchemaBuilder.struct()
                                 .field("optionalBool", SchemaBuilder.bool().optional().defaultValue(null).build())
                                 .build();
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
    org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("ConnectDefault").namespace("io.confluent.connect.avro").fields()
        .name("optionalBool").type(org.apache.avro.SchemaBuilder.builder()
            .unionOf().nullType().and().booleanType().endUnion()).withDefault(null)
        .endRecord();

    assertEquals(expectedAvroSchema, avroSchema);

    Struct struct = new Struct(schema)
        .put("optionalBool", true);
    Object convertedRecord = avroData.fromConnectData(schema, struct);
    org.apache.avro.generic.GenericRecord avroRecord = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("optionalBool", true)
        .build();

    assertEquals(avroRecord, convertedRecord);
  }

  @Test
  public void testFromConnectOptionalWithInvalidDefault() {
    Schema schema = SchemaBuilder.struct()
        .field("array", SchemaBuilder.array(SchemaBuilder.string().optional().build())
            .defaultValue(Arrays.asList("a", "b", "c")).build())
        .build();
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
    org.apache.avro.Schema arraySchema = org.apache.avro.SchemaBuilder.builder().array().items()
        .unionOf().nullType().and().stringType().endUnion();
    ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
    arrayNode.add("a");
    arrayNode.add("b");
    arrayNode.add("c");
    arraySchema.addProp("connect.default", arrayNode);
    org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("ConnectDefault").namespace("io.confluent.connect.avro").fields()
        .name("array").type(arraySchema).noDefault()  // no default
        .endRecord();

    assertEquals(expectedAvroSchema, avroSchema);

    Struct struct = new Struct(schema)
        .put("array", Arrays.asList("a", "b", "c"));
    Object convertedRecord = avroData.fromConnectData(schema, struct);
    org.apache.avro.generic.GenericRecord avroRecord = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
        .set("array", Arrays.asList("a", "b", "c"))
        .build();

    assertEquals(avroRecord, convertedRecord);
  }

  @Test
  public void testFromConnectOptionalAnonymousStruct() {
    Schema schema = SchemaBuilder.struct().optional()
        .field("int32", Schema.INT32_SCHEMA)
        .build();

    Struct struct = new Struct(schema).put("int32", 12);

    Object convertedRecord = avroData.fromConnectData(schema, struct);

    assertThat(convertedRecord, instanceOf(GenericRecord.class));
    assertThat(((GenericRecord)convertedRecord).get("int32"), equalTo(12));
  }

  @Test
  public void testFromConnectStructIgnoreDefaultForNullables() {
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.IGNORE_DEFAULT_FOR_NULLABLES_CONFIG, true)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);

    Schema connectStringSchema = SchemaBuilder.string().optional().defaultValue("default-string").build();
    // The string field is not set
    Schema schema = SchemaBuilder.struct()
        .name("Record")
        .field("int32", Schema.INT32_SCHEMA)
        .field("string", connectStringSchema)
        .build();

    Struct struct = new Struct(schema).put("int32", 12);

    Object convertedRecord = avroData.fromConnectData(schema, struct);

    assertThat(convertedRecord, instanceOf(GenericRecord.class));
    assertThat(((GenericRecord)convertedRecord).get("int32"), equalTo(12));
    assertNull(((GenericRecord)convertedRecord).get("string"));
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
                                                            schema, "string", avroData);
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
    return createDecimalSchema(required, precision, TEST_SCALE);
  }

  private static org.apache.avro.Schema createDecimalSchema(boolean required, int precision, int scale) {
    org.apache.avro.Schema avroSchema
        = required ? org.apache.avro.SchemaBuilder.builder().bytesType() :
          org.apache.avro.SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion();
    org.apache.avro.Schema decimalSchema = required ? avroSchema : avroSchema.getTypes().get(1);
    decimalSchema.addProp("scale", scale);
    decimalSchema.addProp("precision", precision);
    decimalSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    ObjectNode avroParams = JsonNodeFactory.instance.objectNode();
    avroParams.put("scale", Integer.toString(scale));
    avroParams.put(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(precision));
    decimalSchema.addProp("connect.parameters", avroParams);
    decimalSchema.addProp("connect.name", "org.apache.kafka.connect.data.Decimal");
    decimalSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL);
    if (scale >= 0 && scale <= precision) {
      org.apache.avro.LogicalTypes.decimal(precision, scale).addToSchema(decimalSchema);
    }

    return avroSchema;
  }

  // test for new way of logical type handling
  @Test
  public void testFromConnectLogicalDecimalNew() {
    org.apache.avro.Schema avroSchema = createDecimalSchema(true, 64);
    checkNonRecordConversionNew(avroSchema, ByteBuffer.wrap(TEST_DECIMAL_BYTES), Decimal.builder(2).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64").build(), TEST_DECIMAL, avroData);
    checkNonRecordConversionNull(Decimal.builder(2).optional().build());
  }

  // test for new way of logical type handling
  @Test
  public void testFromConnectLogicalDateNew() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Date");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE);
    org.apache.avro.LogicalTypes.date().addToSchema(avroSchema);
    checkNonRecordConversionNew(avroSchema, 10000, Date.SCHEMA,
        EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime(), avroData);
  }

  // test for new way of logical type handling
  @Test
  public void testFromConnectLogicalTimeNew() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Time");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS);
    org.apache.avro.LogicalTypes.timeMillis().addToSchema(avroSchema);
    checkNonRecordConversionNew(avroSchema, 10000, Time.SCHEMA,
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime(), avroData);
  }

  // test for new way of logical type handling
  @Test
  public void testFromConnectLogicalTimestampNew() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Timestamp");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS);
    org.apache.avro.LogicalTypes.timestampMillis().addToSchema(avroSchema);
    java.util.Date date = new java.util.Date();
    checkNonRecordConversionNew(avroSchema, date.getTime(), Timestamp.SCHEMA, date, avroData);
  }

  // Test to ensure that a decimal with a scale greater than its precision can be safely handled
  @Test
  public void testFromConnectLogicalDecimalScaleGreaterThanPrecision() {
    int precision = 5;
    int scale = 7;
    BigDecimal testDecimal = new BigDecimal(new BigInteger("12358"), scale);
    org.apache.avro.Schema avroSchema = createDecimalSchema(true, precision, scale);
    checkNonRecordConversion(avroSchema, ByteBuffer.wrap(testDecimal.unscaledValue().toByteArray()), Decimal.builder(scale).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(precision)).build(), testDecimal, avroData);
    checkNonRecordConversionNull(Decimal.builder(scale).optional().build());
  }

  // test for old way of logical type handling
  @Test
  public void testFromConnectLogicalDecimal() {
    org.apache.avro.Schema avroSchema = createDecimalSchema(true, 64);
    checkNonRecordConversion(avroSchema, ByteBuffer.wrap(TEST_DECIMAL_BYTES), Decimal.builder(2).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64").build(), TEST_DECIMAL, avroData);
    checkNonRecordConversionNull(Decimal.builder(2).optional().build());
  }

  // test for old way of logical type handling
  @Test
  public void testFromConnectLogicalDate() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Date");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE);
    checkNonRecordConversion(avroSchema, 10000, Date.SCHEMA,
                             EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime(), avroData);
  }

  // test for old way of logical type handling
  @Test
  public void testFromConnectLogicalTime() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Time");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS);
    checkNonRecordConversion(avroSchema, 10000, Time.SCHEMA,
                             EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime(), avroData);
  }

  // test for old way of logical type handling
  @Test
  public void testFromConnectLogicalTimestamp() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
    avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Timestamp");
    avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1));
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS);
    java.util.Date date = new java.util.Date();
    checkNonRecordConversion(avroSchema, date.getTime(), Timestamp.SCHEMA, date, avroData);
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
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, (byte) 12, avroData);
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, (short) 12, avroData);
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, 12, avroData);

    GenericRecord avroLongRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("long", 12L)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroLongRecord, null, 12L, avroData);

    GenericRecord avroFloatRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("float", 12.2f)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroFloatRecord, null, 12.2f, avroData);

    GenericRecord avroDoubleRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("double", 12.2)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroDoubleRecord, null, 12.2, avroData);

    GenericRecord avroBooleanRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("boolean", true)
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroBooleanRecord, null, true, avroData);

    GenericRecord avroStringRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("string", "teststring")
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroStringRecord, null, "teststring",
        avroData);

    GenericRecord avroNullRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).build();
    GenericRecord avroArrayRecord = new GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
        .set("array", Arrays.asList(avroIntRecord, avroStringRecord, avroNullRecord))
        .build();
    checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroArrayRecord,
                             null, Arrays.asList(12, "teststring", null), avroData);

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
                             null, convertedMap, avroData);
  }

  @Test
  public void testCacheSchemaFromConnectConversion() {
    Map<org.apache.avro.Schema, Schema> cache =
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

  @Test
  public void testEnum() throws Exception {
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
        .build();

    AvroData avroData = new AvroData(avroDataConfig);

    EnumTest testModel = EnumTest.newBuilder()
        .setTestkey("name")
        .setKind(Kind.ONE)
        .build();

    SchemaAndValue schemaAndValue = avroData.toConnectData(EnumTest.SCHEMA$, testModel);
    org.apache.kafka.connect.data.Schema schema = schemaAndValue.schema();
    Object schemaValue = schemaAndValue.value();

    GenericData.Record value = (GenericData.Record) avroData.fromConnectData(schema, schemaValue);
    GenericContainer userTypeValue = (GenericContainer) value.get("kind");
    Assert.assertEquals(userTypeValue.getSchema().getType(), org.apache.avro.Schema.Type.ENUM);
  }

  @Test
  public void testEnumUnion() throws Exception {
    GenericData genericData = GenericData.get();
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
        .build();

    AvroData avroData = new AvroData(avroDataConfig);

    EnumUnion testModel = EnumUnion.newBuilder()
        .setUserType(UserType.ANONYMOUS)
        .build();

    SchemaAndValue schemaAndValue = avroData.toConnectData(EnumUnion.SCHEMA$, testModel);
    org.apache.kafka.connect.data.Schema schema = schemaAndValue.schema();
    Object schemaValue = schemaAndValue.value();

    GenericData.Record value = (GenericData.Record) avroData.fromConnectData(schema, schemaValue);

    org.apache.avro.Schema userTypeSchema = EnumUnion.SCHEMA$.getField("userType").schema();

    Object userTypeValue = value.get("userType");

    int unionIndex = genericData.resolveUnion(userTypeSchema, userTypeValue);
    Assert.assertEquals(1, unionIndex);
  }

  @Test
  public void testEnumUnionNullValue() throws Exception {
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
        .build();

    AvroData avroData = new AvroData(avroDataConfig);

    EnumUnion testModel = EnumUnion.newBuilder()
        .setUserType(null)
        .build();

    SchemaAndValue schemaAndValue = avroData.toConnectData(EnumUnion.SCHEMA$, testModel);
    org.apache.kafka.connect.data.Schema schema = schemaAndValue.schema();
    Object schemaValue = schemaAndValue.value();

    GenericData.Record value = (GenericData.Record) avroData.fromConnectData(schema, schemaValue);
    Object userTypeValue = value.get("userType");
    Assert.assertNull(userTypeValue);
  }

  @Test
  public void testEnumStringUnion() throws Exception {
    GenericData genericData = GenericData.get();
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
        .build();

    AvroData avroData = new AvroData(avroDataConfig);

    EnumStringUnion testModel = EnumStringUnion.newBuilder()
        .setUserType(UserType.ANONYMOUS)
        .build();

    SchemaAndValue schemaAndValue = avroData.toConnectData(EnumStringUnion.SCHEMA$, testModel);
    org.apache.kafka.connect.data.Schema schema = schemaAndValue.schema();
    Object schemaValue = schemaAndValue.value();

    GenericData.Record value = (GenericData.Record) avroData.fromConnectData(schema, schemaValue);

    org.apache.avro.Schema userTypeSchema = EnumStringUnion.SCHEMA$.getField("userType").schema();

    Object userTypeValue = value.get("userType");

    int unionIndex = genericData.resolveUnion(userTypeSchema, userTypeValue);
    Assert.assertEquals(2, unionIndex);
  }

  @Test
  public void testEnumStringUnionGeneric() throws Exception {
    org.apache.avro.Schema enumField = org.apache.avro.SchemaBuilder.builder()
        .enumeration("testEnum").namespace("com").symbols("A", "B", "C", "D");

    org.apache.avro.Schema unionField = org.apache.avro.SchemaBuilder.builder().unionOf()
        .nullType().and()
        .stringType().and()
        .type(enumField)
        .endUnion();

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .record("EnumUnionTest").namespace("com").fields()
        .name("id").type(unionField).noDefault()
        .endRecord();

    GenericRecord avroRecord1 = new GenericRecordBuilder(avroSchema)
        .set("id", null)
        .build();
    GenericRecord avroRecord2 = new GenericRecordBuilder(avroSchema)
        .set("id", "teststring")
        .build();
    GenericRecord avroRecord3 = new GenericRecordBuilder(avroSchema)
        .set("id", new GenericData.EnumSymbol(enumField, "A"))
        .build();

    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);

    SchemaAndValue schemaAndValue1 = avroData.toConnectData(avroSchema, avroRecord1);
    SchemaAndValue schemaAndValue2 = avroData.toConnectData(avroSchema, avroRecord2);
    SchemaAndValue schemaAndValue3 = avroData.toConnectData(avroSchema, avroRecord3);

    GenericRecord convertedRecord1 = (GenericRecord) avroData.fromConnectData(schemaAndValue1.schema(), schemaAndValue1.value());
    assertEquals(avroRecord1.get("id"), convertedRecord1.get("id"));
    GenericRecord convertedRecord2 = (GenericRecord) avroData.fromConnectData(schemaAndValue2.schema(), schemaAndValue2.value());
    assertEquals(avroRecord2.get("id"), convertedRecord2.get("id"));
    GenericRecord convertedRecord3 = (GenericRecord) avroData.fromConnectData(schemaAndValue3.schema(), schemaAndValue3.value());
    assertEquals(avroRecord3.get("id"), convertedRecord3.get("id"));
  }

  // Avro -> Connect. Validate a) all Avro types that convert directly to Avro, b) specialized
  // Avro types where we can convert to a Connect type that doesn't have a corresponding Avro
  // type, and c) Avro types which need specialized transformation because there is no
  // corresponding Connect type.

  // Avro -> Connect: directly corresponding types

  @Test
  public void testToConnectNull() {
    assertNull(avroData.toConnectData(null, null));
  }

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
    avroSchema.addProp("precision", 50);
    avroSchema.addProp("scale", 2);

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
  public void testToConnectDecimalAvroDefaultScale() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
    avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL);
    avroSchema.addProp("precision", 50);

    final SchemaAndValue expected = new SchemaAndValue(
        Decimal.builder(0).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "50").build(),
        new BigDecimal(new BigInteger("156"), 0)
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

  @Test
  public void testToConnectMapWithNamedSchema() {
    assertThat(avroData.toConnectSchema(NAMED_AVRO_MAP_SCHEMA), equalTo(NAMED_MAP_SCHEMA));
  }

  // Avro -> Connect: Avro types with no corresponding Connect type

  @Test(expected = DataException.class)
  public void testToConnectNullType() {
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().nullType();
    // If we somehow did end up with a null schema and an actual value that let it get past the
    avroData.toConnectData(avroSchema, true);
  }

  @Test
  public void testToConnectFixed() {
    Schema connectSchema = SchemaBuilder.bytes().name("sample").parameter(
        CONNECT_AVRO_FIXED_SIZE_PROP, "4").build();
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .fixed("sample").size(4);
    assertEquals(new SchemaAndValue(connectSchema, ByteBuffer.wrap("foob".getBytes())),
                 avroData.toConnectData(avroSchema, "foob".getBytes()));

    assertEquals(new SchemaAndValue(connectSchema, ByteBuffer.wrap("foob".getBytes())),
                 avroData.toConnectData(avroSchema, ByteBuffer.wrap("foob".getBytes())));

    // test with actual fixed type
    assertEquals(new SchemaAndValue(connectSchema, ByteBuffer.wrap("foob".getBytes())),
            avroData.toConnectData(avroSchema, new GenericData.Fixed(avroSchema, "foob".getBytes())));
  }

  @Test
  public void testToConnectFixedUnion() {
    org.apache.avro.Schema sampleSchema = org.apache.avro.SchemaBuilder.builder().fixed("sample").size(4);
    org.apache.avro.Schema otherSchema = org.apache.avro.SchemaBuilder.builder().fixed("other").size(6);
    org.apache.avro.Schema sameOtherSchema = org.apache.avro.SchemaBuilder.builder().fixed("sameOther").size(6);
    org.apache.avro.Schema unionAvroSchema = org.apache.avro.SchemaBuilder.builder()
            .unionOf().type(sampleSchema).and().type(otherSchema).and().type(sameOtherSchema)
            .endUnion();
    Schema unionConnectSchema = SchemaBuilder.struct()
            .name(AVRO_TYPE_UNION)
            .field("sample", SchemaBuilder.bytes().name("sample").parameter(
                CONNECT_AVRO_FIXED_SIZE_PROP, "4").optional().build())
            .field("other", SchemaBuilder.bytes().name("other").parameter(
                CONNECT_AVRO_FIXED_SIZE_PROP, "6").optional().build())
            .field("sameOther", SchemaBuilder.bytes().name("sameOther").parameter(
                CONNECT_AVRO_FIXED_SIZE_PROP, "6").optional().build())
            .build();
    GenericData.Fixed valueSample = new GenericData.Fixed(sampleSchema, "foob".getBytes());
    GenericData.Fixed valueOther = new GenericData.Fixed(otherSchema, "foobar".getBytes());
    GenericData.Fixed valueSameOther = new GenericData.Fixed(sameOtherSchema, "foobar".getBytes());
    Schema generatedSchema = avroData.toConnectSchema(unionAvroSchema);

    assertEquals(unionConnectSchema, generatedSchema);
    Struct unionSame = new Struct(unionConnectSchema).put("sample", ByteBuffer.wrap("foob".getBytes()));
    assertEquals(new SchemaAndValue(unionConnectSchema, unionSame),
            avroData.toConnectData(unionAvroSchema, valueSample));
    Struct unionOther = new Struct(unionConnectSchema).put("other", ByteBuffer.wrap("foobar".getBytes()));
    assertEquals(new SchemaAndValue(unionConnectSchema, unionOther),
            avroData.toConnectData(unionAvroSchema, valueOther));
    Struct unionSameOther = new Struct(unionConnectSchema).put("sameOther",
            ByteBuffer.wrap("foobar".getBytes()));
    assertEquals(new SchemaAndValue(unionConnectSchema, unionSameOther),
            avroData.toConnectData(unionAvroSchema, valueSameOther));
  }

  @Test
  public void testToConnectFixedUnionWithEnhanced() {
    avroData = new AvroData(new AvroDataConfig.Builder()
            .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
            .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
            .build());

    org.apache.avro.Schema sampleSchema = org.apache.avro.SchemaBuilder.builder().fixed("sample").size(4);
    org.apache.avro.Schema otherSchema = org.apache.avro.SchemaBuilder.builder().fixed("other").size(6);
    org.apache.avro.Schema sameOtherSchema = org.apache.avro.SchemaBuilder.builder().fixed("sameOther").size(6);
    org.apache.avro.Schema unionAvroSchema = org.apache.avro.SchemaBuilder.builder()
            .unionOf().type(sampleSchema).and().type(otherSchema).and().type(sameOtherSchema)
            .endUnion();
    Schema unionConnectSchema = SchemaBuilder.struct()
            .name("io.confluent.connect.avro.Union")
            .field("sample", SchemaBuilder.bytes().name("sample").parameter(
                CONNECT_AVRO_FIXED_SIZE_PROP, "4").optional().build())
            .field("other", SchemaBuilder.bytes().name("other").parameter(
                CONNECT_AVRO_FIXED_SIZE_PROP, "6").optional().build())
            .field("sameOther", SchemaBuilder.bytes().name("sameOther").parameter(
                CONNECT_AVRO_FIXED_SIZE_PROP, "6").optional().build())
            .build();
    GenericData.Fixed valueSample = new GenericData.Fixed(sampleSchema, "foob".getBytes());
    GenericData.Fixed valueOther = new GenericData.Fixed(otherSchema, "foobar".getBytes());
    GenericData.Fixed valueSameOther = new GenericData.Fixed(sameOtherSchema, "foobar".getBytes());
    Schema generatedSchema = avroData.toConnectSchema(unionAvroSchema);

    assertEquals(unionConnectSchema, generatedSchema);
    Struct unionSame = new Struct(unionConnectSchema).put("sample", ByteBuffer.wrap("foob".getBytes()));
    assertEquals(new SchemaAndValue(unionConnectSchema, unionSame),
            avroData.toConnectData(unionAvroSchema, valueSample));
    Struct unionOther = new Struct(unionConnectSchema).put("other", ByteBuffer.wrap("foobar".getBytes()));
    assertEquals(new SchemaAndValue(unionConnectSchema, unionOther),
            avroData.toConnectData(unionAvroSchema, valueOther));
    Struct unionSameOther = new Struct(unionConnectSchema).put("sameOther",
            ByteBuffer.wrap("foobar".getBytes()));
    assertEquals(new SchemaAndValue(unionConnectSchema, unionSameOther),
            avroData.toConnectData(unionAvroSchema, valueSameOther));
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

  @Test
  public void testToConnectUnionWithGeneralizedSumTypeSupport() {
    avroData = new AvroData(new AvroDataConfig.Builder()
        .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
        .with(AvroDataConfig.GENERALIZED_SUM_TYPE_SUPPORT_CONFIG, true)
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
        .name("connect_union_0")
        .parameter("org.apache.kafka.connect.data.Union", "connect_union_0")
        .field("connect_union_field_0", Schema.OPTIONAL_INT32_SCHEMA)
        .field("connect_union_field_1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("connect_union_field_2", recordSchema1)
        .field("connect_union_field_3", recordSchema2)
        .build();
    assertEquals(new SchemaAndValue(schema, new Struct(schema).put("connect_union_field_0", 12)),
        avroData.toConnectData(avroSchema, 12));
    assertEquals(new SchemaAndValue(schema, new Struct(schema).put("connect_union_field_1", "teststring")),
        avroData.toConnectData(avroSchema, "teststring"));

    Struct schema1Test = new Struct(schema).put("connect_union_field_2", new Struct(recordSchema1).put("test", 12));
    GenericRecord record1Test = new GenericRecordBuilder(avroRecordSchema1).set("test", 12).build();
    Struct schema2Test = new Struct(schema).put("connect_union_field_3", new Struct(recordSchema2).put("test", 12));
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
  public void testToConnectUnionDocChange() {
    org.apache.avro.Schema oldSchema = org.apache.avro.SchemaBuilder.builder()
        .record("FirstRecord").doc("old doc").fields().requiredInt("int1").endRecord();
    org.apache.avro.Schema newSchema = org.apache.avro.SchemaBuilder.builder()
        .record("FirstRecord").doc("new doc").fields().requiredInt("int1").endRecord();
    org.apache.avro.Schema irrelevant = org.apache.avro.SchemaBuilder.builder()
        .record("placeholder").doc("old doc").fields().requiredInt("holder").endRecord();

    org.apache.avro.Schema unionAvroSchema = org.apache.avro.SchemaBuilder.builder()
        .unionOf().type(oldSchema).and().type(irrelevant)
        .endUnion();
    org.apache.avro.Schema unionAvroSchemaNew = org.apache.avro.SchemaBuilder.builder()
        .unionOf().type(newSchema).and().type(irrelevant)
        .endUnion();
    GenericRecord record1Test = new GenericRecordBuilder(oldSchema).set("int1", 12).build();
    GenericRecord record2Test = new GenericRecordBuilder(newSchema).set("int1", 12).build();
    // Cache the old schema
    assertNotNull(avroData.toConnectData(unionAvroSchema, record1Test));
    assertNotNull(avroData.toConnectData(unionAvroSchemaNew, record2Test));
  }

  @Test
  public void testToConnectSingletonUnion() {
    org.apache.avro.Schema avroStringSchema = org.apache.avro.SchemaBuilder.builder().stringType();
    org.apache.avro.Schema unionAvroSchema = org.apache.avro.SchemaBuilder.builder()
        .unionOf().type(avroStringSchema).endUnion();
    SchemaBuilder builder = SchemaBuilder.string();
    assertEquals(new SchemaAndValue(builder.build(), "bar"),
        avroData.toConnectData(unionAvroSchema, "bar"));
  }

  @Test
  public void testToConnectEnum() {
    // Enums are just converted to strings, original enum is preserved in parameters
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .enumeration("TestEnum")
        .doc("some documentation")
        .symbols("foo", "bar", "baz");
    SchemaBuilder builder = SchemaBuilder.string().name("TestEnum");
    builder.parameter(AVRO_ENUM_DOC_PREFIX_PROP + "TestEnum", "some documentation");
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
  public void testToConnectEnumWithNoDoc() {
    // Enums are just converted to strings, original enum is preserved in parameters
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
            .enumeration("TestEnum")
            .symbols("foo", "bar", "baz");
    SchemaBuilder builder = SchemaBuilder.string().name("TestEnum");
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
  public void testToConnectEnumWithGeneralizedSumTypeSupport() {
    avroData = new AvroData(new AvroDataConfig.Builder()
        .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
        .with(AvroDataConfig.GENERALIZED_SUM_TYPE_SUPPORT_CONFIG, true)
        .build());
    // Enums are just converted to strings, original enum is preserved in parameters
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder()
        .enumeration("TestEnum")
        .doc("some documentation")
        .symbols("foo", "bar", "baz");
    SchemaBuilder builder = SchemaBuilder.string().name("TestEnum");
    builder.parameter(AVRO_ENUM_DOC_PREFIX_PROP + "TestEnum", "some documentation");
    builder.parameter(GENERALIZED_TYPE_ENUM, "TestEnum");
    int i = 0;
    for(String enumSymbol : new String[]{"foo", "bar", "baz"}) {
      builder.parameter(GENERALIZED_TYPE_ENUM+"."+enumSymbol, String.valueOf(i++));
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
    // per-field metadata (doc, default value) that Connect holds in parameters(). We set
    // these properties on one of these fields to ensure they are properly converted
    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.TestSchema").version(12).doc("doc")
        .field("int32", SchemaBuilder
            .int32()
            .defaultValue(7)
            .parameter(AVRO_FIELD_DEFAULT_FLAG_PROP, "true")
            .build())
        .parameter(AVRO_FIELD_DOC_PREFIX_PROP + "int32", "field doc")
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
    Map<Schema, org.apache.avro.Schema> cache =
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
  public void testAvroWithAndWithoutMetaData() {
    String s1 = "{"
        + "  \"type\": \"record\","
        + "  \"name\": \"ListingStateChangedEventKeyRecord\","
        + "  \"namespace\": \"com.acme.property\","
        + "  \"doc\": \"Listing State Changed Event Key\","
        + "  \"fields\": ["
        + "    {"
        + "      \"name\": \"listingUuid\","
        + "      \"type\": {"
        + "        \"type\": \"string\","
        + "        \"avro.java.string\": \"String\""
        + "      }"
        + "    }"
        + "  ]"
        + "}";
    String s2 = "{"
        + "  \"type\": \"record\","
        + "  \"name\": \"ListingStateChangedEventKeyRecord\","
        + "  \"namespace\": \"com.acme.property\","
        + "  \"doc\": \"Another listing State Changed Event Key\","
        + "  \"fields\": ["
        + "    {"
        + "      \"name\": \"listingUuid\","
        + "      \"type\": \"string\""
        + "    }"
        + "  ]"
        + "}";

    org.apache.avro.Schema avroSchema1 = new org.apache.avro.Schema.Parser().parse(s1);
    org.apache.avro.Schema avroSchema2 = new org.apache.avro.Schema.Parser().parse(s2);

    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);
    Schema schema1 = avroData.toConnectSchema(avroSchema1);
    Schema schema2 = avroData.toConnectSchema(avroSchema2);
    assertEquals(schema1.parameters(), schema2.parameters());
  }

  @Test
  public void testIntWithConnectDefault() {
    final String s = "{"
        + "  \"type\": \"record\","
        + "  \"name\": \"SomeThing\","
        + "  \"namespace\": \"com.acme.property\","
        + "  \"fields\": ["
        + "    {"
        + "      \"name\": \"f\","
        + "      \"type\": {"
        + "        \"type\": \"int\","
        + "        \"connect.default\": 42,"
        + "        \"connect.version\": 1"
        + "      }"
        + "    }"
        + "  ]"
        + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(s);

    AvroData avroData = new AvroData(0);
    Schema schema = avroData.toConnectSchema(avroSchema);

    assertEquals(42, schema.field("f").schema().defaultValue());
  }

  @Test
  public void testLongWithConnectDefault() {
    final String s = "{"
        + "  \"type\": \"record\","
        + "  \"name\": \"SomeThing\","
        + "  \"namespace\": \"com.acme.property\","
        + "  \"fields\": ["
        + "    {"
        + "      \"name\": \"f\","
        + "      \"type\": {"
        + "        \"type\": \"long\","
        + "        \"connect.default\": 42,"
        + "        \"connect.version\": 1"
        + "      }"
        + "    }"
        + "  ]"
        + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(s);

    AvroData avroData = new AvroData(0);
    Schema schema = avroData.toConnectSchema(avroSchema);

    assertEquals(42L, schema.field("f").schema().defaultValue());
  }

  @Test
  public void testFloatWithInvalidDefault() {
    final String s = "{"
        + "  \"type\": \"record\","
        + "  \"name\": \"SomeThing\","
        + "  \"namespace\": \"com.acme.property\","
        + "  \"fields\": ["
        + "    {"
        + "      \"name\": \"f\","
        + "      \"type\": \"float\","
        + "      \"default\": [1.23]"
        + "    }"
        + "  ]"
        + "}";

    // Don't validate defaults
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().setValidateDefaults(false).parse(s);

    AvroData avroData = new AvroData(0);
    Schema schema = avroData.toConnectSchema(avroSchema);

    assertNull(schema.field("f").schema().defaultValue());
  }

  @Test
  public void testNestedRecordWithNullDefault() {
    final String fullSchema = "{"
        + "  \"name\": \"RecordWithObjectDefault\","
        + "  \"type\": \"record\","
        + "  \"fields\": [{"
        + "    \"name\": \"obj\","
        + "      \"default\": {\"nullableString\": null},"
        + "      \"type\": {"
        + "        \"name\": \"Object\","
        + "        \"type\": \"record\","
        + "        \"fields\": [{"
        + "            \"name\": \"nullableString\","
        + "            \"type\": [\"null\",\"string\"]}"
        + "        ]}"
        + "    }]"
        + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(fullSchema);

    org.apache.avro.Schema innerSchema = new org.apache.avro.Schema.Parser().parse("{"
        + "        \"name\": \"Object\","
        + "        \"type\": \"record\","
        + "        \"fields\": [{"
        + "            \"name\": \"nullableString\","
        + "            \"type\": [\"null\",\"string\"]}"
        + "        ]}");

    AvroData avroData = new AvroData(0);

    // test record:
    // {"obj": {"nullableString": null}}
    GenericRecord nestedRecord = new GenericRecordBuilder(avroSchema)
        .set("obj", new GenericRecordBuilder(innerSchema).set("nullableString", null).build())
        .build();

    SchemaAndValue schemaAndValue = avroData.toConnectData(avroSchema, nestedRecord);
    Struct value = (Struct)schemaAndValue.value();
    assertNotNull(value.get("obj"));
    Struct objFieldValue = (Struct)value.get("obj");
    assertNull(objFieldValue.get("nullableString"));
  }

  @Test
  public void testNestedRecordWithInvalidDefault() {
    final String fullSchema = "{"
        + "  \"name\": \"RecordWithObjectDefault\","
        + "  \"type\": \"record\","
        + "  \"fields\": [{"
        + "    \"name\": \"obj\","
        + "      \"type\": {"
        + "        \"name\": \"Object\","
        + "        \"type\": \"record\","
        + "        \"connect.default\": [1.23],"
        + "        \"fields\": [{"
        + "            \"name\": \"nullableString\","
        + "            \"type\": [\"null\",\"string\"]}"
        + "        ]}"
        + "    }]"
        + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser()
        .setValidateDefaults(false).parse(fullSchema);

    org.apache.avro.Schema innerSchema = new org.apache.avro.Schema.Parser().parse("{"
        + "        \"name\": \"Object\","
        + "        \"type\": \"record\","
        + "        \"fields\": [{"
        + "            \"name\": \"nullableString\","
        + "            \"type\": [\"null\",\"string\"]}"
        + "        ]}");

    AvroData avroData = new AvroData(0);

    // test record:
    // {"obj": {"nullableString": null}}
    GenericRecord nestedRecord = new GenericRecordBuilder(avroSchema)
        .set("obj", new GenericRecordBuilder(innerSchema).set("nullableString", null).build())
        .build();

    SchemaAndValue schemaAndValue = avroData.toConnectData(avroSchema, nestedRecord);
    Struct value = (Struct)schemaAndValue.value();
    assertNotNull(value.get("obj"));
    Struct objFieldValue = (Struct)value.get("obj");
    assertNull(objFieldValue.get("nullableString"));
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

  @Test
  public void testCyclicalAvroSchema() {
    //This test would test the round trip and asserting the intermediate connect data as well

    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, false)
        .build();
    AvroData listAvroData = new AvroData(avroDataConfig);
    String linkedListAvroSchema =
        "{\"type\": \"record\",\"name\": \"linked_list\",\"fields\" : "
            + "[{\"name\": \"value\", \"type\": \"long\"},"
            + "{\"name\": \"next\", \"type\": [\"null\", \"linked_list\"],\"default\" : null}]}";
    org.apache.avro.Schema.Parser avroParser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = avroParser.parse(linkedListAvroSchema);

    GenericRecord next = new GenericRecordBuilder(avroSchema)
        .set("value", 2l)
        .set("next", null)
        .build();

    GenericRecord headNode = new GenericRecordBuilder(avroSchema)
        .set("value", 3l)
        .set("next", next)
        .build();

    SchemaAndValue schemaAndValue = listAvroData.toConnectData(avroSchema, headNode);

    assertNonNullSchemaValue(schemaAndValue);
    Struct linkedListNode = (Struct) schemaAndValue.value();
    assertEquals(3l, linkedListNode.get("value"));
    assertNotNull(linkedListNode.get("next"));
    linkedListNode = (Struct) linkedListNode.get("next");
    assertEquals(2l, linkedListNode.get("value"));
    assertNull(linkedListNode.get("next"));

    GenericRecord genericRecord = (GenericRecord) listAvroData.fromConnectData(
        schemaAndValue.schema(), schemaAndValue.value());

    assertEquals(headNode, genericRecord);

  }

  @Test
  public void testArrayCycle() {
    //This test would test the round trip and asserting the intermediate connect data as well
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, false)
        .build();
    AvroData graphAvroData = new AvroData(avroDataConfig);
    org.apache.avro.Schema.Parser avroParser = new org.apache.avro.Schema.Parser();

    String graphAvroSchema = "{\"type\": \"record\",\"name\": \"Users\",\"fields\" : [{\"name\": " +
        "\"name\", \"type\": \"string\"},{\"name\": \"friends\", \"type\" : [ \"null\", " +
        "{\"type\": \"array\", \"items\":\"Users\"}], \"default\" : null}]}";

    org.apache.avro.Schema graphSchema = avroParser.parse(graphAvroSchema);
    org.apache.avro.Schema friendsListSchema = graphSchema.getField("friends").schema().getTypes().get(1);

    GenericRecord friend1 = new GenericRecordBuilder(graphSchema)
        .set("name", "Person A")
        .build();
    GenericRecord friend2 = new GenericRecordBuilder(graphSchema)
        .set("name", "Person B")
        .set("friends", new GenericData.Array(friendsListSchema, Arrays.asList(friend1)))
        .build();

    GenericRecord person = new GenericRecordBuilder(graphSchema)
        .set("name", "Person C")
        .set("friends", new GenericData.Array(friendsListSchema, Arrays.asList(friend1, friend2)))
        .build();

    SchemaAndValue schemaAndValue = graphAvroData.toConnectData(graphSchema, person);

    Map<String,List<String>> expectedMap = ImmutableMap.of(
        "Person C", Arrays.asList("Person A", "Person B"),
        "Person B", Arrays.asList("Person A"),
        "Person A", Arrays.asList()
    );
    assertNonNullSchemaValue(schemaAndValue);
    assertPersons("Person C", schemaAndValue.value(), expectedMap);

    GenericRecord genericRecord = (GenericRecord) graphAvroData.fromConnectData(
        schemaAndValue.schema(), schemaAndValue.value());

    assertEquals(person, genericRecord);
  }

  private void assertPersons(
      String currentPerson, Object value, Map<String, List<String>> expectedMap) {

    assertNotNull(value);
    assertTrue(value instanceof Struct);
    Struct personStruct = (Struct) value;
    assertEquals(currentPerson, personStruct.get("name"));
    if (expectedMap.containsKey(currentPerson) && expectedMap.get(currentPerson).size() > 0) {
      assertNotNull(personStruct.get("friends"));
      assertTrue(personStruct.get("friends") instanceof List);
      List friends = personStruct.getArray("friends");
      assertEquals(expectedMap.get(currentPerson).size(), friends.size());
      for (int i = 0; i < friends.size(); i++) {
        assertPersons(expectedMap.get(currentPerson).get(i), friends.get(i), expectedMap);
      }
    } else {
      assertNull(personStruct.get("friends"));
    }

  }

  @Test
  public void testMapCycle() {
    //This test would test the round trip and asserting the intermediate connect data as well
    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .build();
    AvroData avroData = new AvroData(avroDataConfig);
    org.apache.avro.Schema.Parser avroParser = new org.apache.avro.Schema.Parser();

    String mapCycleSchema = "{\"type\": \"record\",\"name\": \"Node\",\"fields\" : [{\"name\": " +
        "\"value\", \"type\": \"long\"},{\"name\": \"siblings\", \"type\" : [ \"null\", " +
        "{\"type\": \"map\", \"values\":\"Node\"}], \"default\" : null}]}";

    org.apache.avro.Schema graphSchema = avroParser.parse(mapCycleSchema);
    GenericRecord node1 = new GenericRecordBuilder(graphSchema)
        .set("value", 1l)
        .build();
    GenericRecord node2 = new GenericRecordBuilder(graphSchema)
        .set("value", 2l)
        .set("siblings", ImmutableMap.of("node1", node1))
        .build();

    Map siblings = ImmutableMap.of("node1", node1, "node2", node2);
    GenericRecord person = new GenericRecordBuilder(graphSchema)
        .set("value", 3l)
        .set("siblings", siblings)
        .build();

    SchemaAndValue schemaAndValue = avroData.toConnectData(graphSchema, person);
    Map<Long,Map<String, Long>> expectedMap = ImmutableMap.of(
        3l, ImmutableMap.of("node1",1l, "node2", 2l),
        2l, ImmutableMap.of("node1",1l),
        1l, ImmutableMap.of()
    );
    assertNonNullSchemaValue(schemaAndValue);
    assertMapCycle(3l, schemaAndValue.value(), expectedMap);
    GenericRecord genericRecord = (GenericRecord) avroData.fromConnectData(
        schemaAndValue.schema(), schemaAndValue.value());

    assertEquals(person, genericRecord);
  }

  private void assertMapCycle(
      Long current, Object value, Map<Long, Map<String, Long>> expectedMap) {

    assertNotNull(value);
    assertTrue(value instanceof Struct);
    Struct struct = (Struct) value;
    assertEquals(current, struct.get("value"));
    if (expectedMap.containsKey(current) && expectedMap.get(current).size() > 0) {
      assertNotNull(struct.get("siblings"));
      assertTrue(struct.get("siblings") instanceof Map);
      Map siblings = struct.getMap("siblings");
      assertEquals(expectedMap.get(current).size(), siblings.size());
      assertTrue(expectedMap.get(current).keySet().equals(siblings.keySet()));
      for (Map.Entry<String, Long> entry : expectedMap.get(current).entrySet()) {
        assertMapCycle(entry.getValue(), siblings.get(entry.getKey()), expectedMap);
      }
    } else {
      assertNull(struct.get("siblings"));
    }

  }

  private NonRecordContainer checkNonRecordConversion(
      org.apache.avro.Schema expectedSchema, Object expected,
      Schema schema, Object value, AvroData avroData)
  {
    Object converted = avroData.fromConnectData(schema, value);
    assertTrue(converted instanceof NonRecordContainer);
    NonRecordContainer container = (NonRecordContainer) converted;
    assertEquals(expectedSchema, container.getSchema());
    assertEquals(expected, container.getValue());
    return container;
  }

  private NonRecordContainer checkNonRecordConversionNew(
      org.apache.avro.Schema expectedSchema, Object expected,
      Schema schema, Object value, AvroData avroData)
  {
    Object converted = avroData.fromConnectData(schema, value);
    assertTrue(converted instanceof NonRecordContainer);
    NonRecordContainer container = (NonRecordContainer) converted;
    assertSchemaEquals(expectedSchema, container.getSchema());
    assertValueEquals(expected, container.getValue());
    return container;
  }

  private void checkNonRecordConversionNull(Schema schema)
  {
    Object converted = avroData.fromConnectData(schema, null);
    assertNull(converted);
  }

  private void assertNonNullSchemaValue(SchemaAndValue schemaAndValue) {
    assertNotNull(schemaAndValue);
    assertNotNull(schemaAndValue.schema());
    assertNotNull(schemaAndValue.value());
  }


  protected void assertSchemaEquals(
      org.apache.avro.Schema expected,
      org.apache.avro.Schema actual) {
    assertEquals(expected.getObjectProps(), actual.getObjectProps());
    assertEquals(expected.getLogicalType(), actual.getLogicalType());
    assertEquals(expected.getType(), actual.getType());
    assertEquals(expected.getDoc(), actual.getDoc());
    // added to test new way of handling logical type
    assertEquals(expected.getLogicalType(), actual.getLogicalType());
    switch(actual.getType()) {
      case UNION:
        assertEquals(expected.getTypes(), actual.getTypes());
        break;
      case ENUM:
        assertEquals(expected.getEnumSymbols(), actual.getEnumSymbols());
        for (String symbol : actual.getEnumSymbols()) {
          assertEquals(expected.getEnumOrdinal(symbol), actual.getEnumOrdinal(symbol));
        }
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getNamespace(), actual.getNamespace());
        assertEquals(expected.getFullName(), actual.getFullName());
        assertEquals(expected.getAliases(), actual.getAliases());
        break;
      case RECORD:
        assertFieldEquals(expected.getFields(), actual.getFields());
        assertEquals(expected.isError(), actual.isError());
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getNamespace(), actual.getNamespace());
        assertEquals(expected.getFullName(), actual.getFullName());
        assertEquals(expected.getAliases(), actual.getAliases());
        break;
      case FIXED:
        assertEquals(expected.getFixedSize(), actual.getFixedSize());
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getNamespace(), actual.getNamespace());
        assertEquals(expected.getFullName(), actual.getFullName());
        assertEquals(expected.getAliases(), actual.getAliases());
        break;
      case ARRAY:
        assertEquals(expected.getElementType(), actual.getElementType());
        break;
      default:
    }
  }

  protected void assertFieldEquals(
      List<org.apache.avro.Schema.Field> expected,
      List<org.apache.avro.Schema.Field> actual
  ) {
    Set<String> expectedNames = expected.stream().map(f -> f.name()).collect(Collectors.toSet());
    Set<String> actualNames = actual.stream().map(f -> f.name()).collect(Collectors.toSet());
    assertEquals(expectedNames, actualNames);
    for (int i=0; i!=actualNames.size(); ++i) {
      assertFieldEquals(expected.get(i), actual.get(i));
    }
  }

  protected void assertFieldEquals(
      org.apache.avro.Schema.Field expected,
      org.apache.avro.Schema.Field actual
  ) {
    assertEquals(expected.name(), actual.name());
    assertEquals(expected.aliases(), actual.aliases());
    assertEquals(expected.doc(), actual.doc());
    assertSchemaEquals(expected.schema(), actual.schema());
    Object expectedDef = expected.defaultVal();
    Object actualDef = actual.defaultVal();
    String msg = "Mismatched default value for field '" + expected.name() + "'";
    if (expectedDef == null) {
      assertNull(msg, actualDef);
      return;
    }
    switch(actual.schema().getType()) {
      case INT:
      case LONG:
        long expectedLong = ((Number) expectedDef).longValue();
        long actualLong = ((Number) actualDef).longValue();
        assertEquals(msg, expectedLong, actualLong);
        break;
      case FLOAT:
      case DOUBLE:
        double expectedDouble = ((Number) expectedDef).doubleValue();
        double actualDouble = ((Number) actualDef).doubleValue();
        assertEquals(msg, expectedDouble, actualDouble, expectedDouble / 100.0d);
        break;
      case BYTES:
        assertArrayEquals(msg, (byte[])expectedDef, (byte[])actualDef);
        break;
      default:
        assertEquals(msg, expectedDef, actualDef);
        break;
    }
  }

  protected void assertValueEquals(Object expected, Object actual) {
    if (actual instanceof byte[]) {
      actual = ByteBuffer.wrap((byte[]) actual);
    }
    if (expected instanceof byte[]) {
      expected = ByteBuffer.wrap((byte[]) expected);
    }
    assertEquals(expected, actual);
  }

  private JsonNode parameters(String key1, String v1) {
    return parametersFromConnect(Collections.singletonMap(key1, v1));
  }

  private JsonNode parametersFromConnect(Map<String, String> params) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  @Test
  public void testUnionCycle() {
    String schemaStr =
            "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"Person\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"name\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"follows\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\",\n" +
            "        \"Person\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}\n";

    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
            .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
            .build();
    AvroData graphAvroData = new AvroData(avroDataConfig);

    // Use the generated Connect schema
    org.apache.avro.Schema schema = graphAvroData.fromConnectSchema(graphAvroData.toConnectSchema(
            new org.apache.avro.Schema.Parser().parse(schemaStr)));

    Integer version = 1;

    GenericRecord person = getUnionCycleRecord(schema);
    SchemaAndValue sv = graphAvroData.toConnectData(schema, person, version);

    assertEquals(sv, graphAvroData.toConnectData(schema, getUnionCycleRecord(schema), version));
    assertEquals(person, graphAvroData.fromConnectData(sv.schema(), sv.value()));
  }

  private GenericRecord getUnionCycleRecord(org.apache.avro.Schema connectSchema) {
    GenericRecord leader = new GenericRecordBuilder(connectSchema)
            .set("name", "Leader")
            .set("follows", null)
            .build();
    GenericRecord follower = new GenericRecordBuilder(connectSchema)
            .set("name", "Follower")
            .set("follows", leader)
            .build();
    return follower;
  }

  @Test
  public void testRecordUnionSingleTypeCycle() {
    String schemaStr = "{\n" +
        "  \"fields\": [\n" +
        "    {\n" +
        "      \"default\": \"\",\n" +
        "      \"name\": \"field1\",\n" +
        "      \"type\": [\n" +
        "        \"string\"\n" +
        "      ]\n" +
        "    }\n" +
        "  ],\n" +
        "  \"name\": \"TestRecord\",\n" +
        "  \"type\": \"record\"\n" +
        "}";

    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .build();
    AvroData testAvroData = new AvroData(avroDataConfig);

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaStr);
    avroData.fromConnectSchema(testAvroData.toConnectSchema(avroSchema));

    Integer version = 1;
    GenericRecord record = new GenericRecordBuilder(avroSchema).set("field1", "value1").build();
    SchemaAndValue sv = testAvroData.toConnectData(avroSchema, record, version);
    assertEquals(sv, testAvroData.toConnectData(avroSchema, record, version));
    assertEquals(record, testAvroData.fromConnectData(sv.schema(), sv.value()));
  }

  @Test
  public void testRecordUnionMultipleTypeCycle() {
    String schemaStr = "{\n" +
        "  \"fields\": [\n" +
        "    {\n" +
        "      \"default\": \"\",\n" +
        "      \"name\": \"field1\",\n" +
        "      \"type\": [\n" +
        "        \"string\",\n" +
        "        \"int\",\n" +
        "        \"float\"\n" +
        "      ]\n" +
        "    }\n" +
        "  ],\n" +
        "  \"name\": \"TestRecord\",\n" +
        "  \"type\": \"record\"\n" +
        "}";

    AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
        .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
        .build();
    AvroData testAvroData = new AvroData(avroDataConfig);

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaStr);
    avroData.fromConnectSchema(testAvroData.toConnectSchema(avroSchema));

    Integer version = 1;
    GenericRecord record = new GenericRecordBuilder(avroSchema).set("field1", "value1").build();
    SchemaAndValue sv = testAvroData.toConnectData(avroSchema, record, version);
    assertEquals(sv, testAvroData.toConnectData(avroSchema, record, version));
    assertEquals(record, testAvroData.fromConnectData(sv.schema(), sv.value()));
  }

  @Test
  public void testRecordDefaultAtFieldLevel() {
    String schemaStr = "{\n"
        + "   \"name\": \"top\",\n"
        + "   \"type\": \"record\",\n"
        + "   \"fields\": [\n"
        + "        {\n"
        + "            \"default\": {},\n"
        + "            \"name\": \"settlement\",\n"
        + "            \"type\": {\n"
        + "                \"fields\": [\n"
        + "                    {\n"
        + "                        \"default\": \"\",\n"
        + "                        \"name\": \"time\",\n"
        + "                        \"type\": \"string\"\n"
        + "                    },\n"
        + "                    {\n"
        + "                        \"default\": 0,\n"
        + "                        \"name\": \"cycle\",\n"
        + "                        \"type\": \"int\"\n"
        + "                    },\n"
        + "                    {\n"
        + "                        \"default\": \"\",\n"
        + "                        \"name\": \"date\",\n"
        + "                        \"type\": \"string\"\n"
        + "                    },\n"
        + "                    {\n"
        + "                        \"default\": \"\",\n"
        + "                        \"name\": \"priority\",\n"
        + "                        \"type\": \"string\"\n"
        + "                    }\n"
        + "                ],\n"
        + "                \"name\": \"settlement\",\n"
        + "                \"type\": \"record\"\n"
        + "            }\n"
        + "        }\n"
        + "    ] \n"
        + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().setValidateDefaults(true).parse(schemaStr);
    Schema schema = avroData.toConnectSchema(avroSchema);
    assertNotNull(avroData.fromConnectSchema(schema));
  }

}
