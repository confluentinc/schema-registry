/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import java.net.SocketException;
import java.net.UnknownHostException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// AvroConverter is a trivial combination of the serializers and the AvroData conversions, so
// most testing is performed on AvroData since it is much easier to compare the results in Avro
// runtime format than in serialized form. This just adds a few sanity checks to make sure things
// work end-to-end.
public class AvroConverterTest {
  private static final String TOPIC = "topic";

  private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");

  private final SchemaRegistryClient schemaRegistry;
  private final AvroConverter converter;

  public AvroConverterTest() {
    schemaRegistry = new MockSchemaRegistryClient();
    converter = new AvroConverter(schemaRegistry);
  }

  @Before
  public void setUp() {
    converter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
  }

  @Test
  public void testConfigure() {
    converter.configure(SR_CONFIG, true);
    assertTrue(Whitebox.<Boolean>getInternalState(converter, "isKey"));
    assertNotNull(Whitebox.getInternalState(
        Whitebox.<AbstractKafkaSchemaSerDe>getInternalState(converter, "serializer"),
        "schemaRegistry"));
  }

  @Test
  public void testConfigureAlt() {
    converter.configure(SR_CONFIG, false);
    assertFalse(Whitebox.<Boolean>getInternalState(converter, "isKey"));
    assertNotNull(Whitebox.getInternalState(
        Whitebox.<AbstractKafkaSchemaSerDe>getInternalState(converter, "serializer"),
        "schemaRegistry"));
  }

  @Test
  public void testPrimitive() {
    SchemaAndValue original = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original.value());
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    // Because of registration in schema registry and lookup, we'll have added a version number
    SchemaAndValue expected = new SchemaAndValue(SchemaBuilder.bool().version(1).build(), true);
    assertEquals(expected, schemaAndValue);
  }

  @Test
  public void testComplex() {
    SchemaBuilder builder = SchemaBuilder.struct()
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
        .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
            .build());
    Schema schema = builder.build();
    Struct original = new Struct(schema)
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
    // Because of registration in schema registry and lookup, we'll have added a version number
    Schema expectedSchema = builder.version(1).build();
    Struct expected = new Struct(expectedSchema)
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

    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(expected, schemaAndValue.value());
  }

  @Test
  public void testTypeBytes() {
    Schema schema = SchemaBuilder.bytes().build();
    byte[] b = converter.fromConnectData("topic", schema, "jellomellow".getBytes());
    SchemaAndValue sv = converter.toConnectData("topic", b);
    assertEquals(Type.BYTES, sv.schema().type());
    assertArrayEquals("jellomellow".getBytes(), ((ByteBuffer) sv.value()).array());
  }

  @Test
  public void testNull() {
    // Because of the way our serialization works, it's expected that we'll lose schema information
    // when the entire schema is optional. The null value should be written as a null and this
    // should mean we also do *not* register a schema.
    byte[] converted = converter.fromConnectData(TOPIC, Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
    assertNull(converted);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(SchemaAndValue.NULL, schemaAndValue);
  }

  @Test
  public void testVersionExtractedForDefaultSubjectNameStrategy() throws Exception {
    // Version info should be extracted even if the data was not created with Copycat. Manually
    // register a few compatible schemas and validate that data serialized with our normal
    // serializer can be read and gets version info inserted
    String subject = TOPIC + "-value";
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
    testVersionExtracted(subject, serializer, avroConverter);

  }

  @Test
  public void testVersionExtractedForRecordSubjectNameStrategy() throws Exception {
    // Version info should be extracted even if the data was not created with Copycat. Manually
    // register a few compatible schemas and validate that data serialized with our normal
    // serializer can be read and gets version info inserted
    String subject =  "Foo";
    Map<String, Object> configs = ImmutableMap.<String, Object>of("schema.registry.url", "http://fake-url", "value.subject.name.strategy", RecordNameStrategy.class.getName());
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
    serializer.configure(configs, false);
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);

    avroConverter.configure(configs, false);
    testVersionExtracted(subject, serializer, avroConverter);
  }

  @Test
  public void testVersionExtractedFromMetadata() throws Exception {
    String subject = TOPIC + "-value";
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
    testVersionExtractedFromMetadata(subject, serializer, avroConverter);
  }

  private void testVersionExtracted(String subject, KafkaAvroSerializer serializer, AvroConverter avroConverter) throws IOException, RestClientException {
    // Pre-register to ensure ordering
    org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
        .record("Foo").fields()
        .requiredInt("key")
        .endRecord();
    schemaRegistry.register(subject, new AvroSchema(avroSchema1));

    org.apache.avro.Schema avroSchema2 = org.apache.avro.SchemaBuilder
        .record("Foo").fields()
        .requiredInt("key")
        .requiredString("value")
        .endRecord();
    schemaRegistry.register(subject, new AvroSchema(avroSchema2));


    // Get serialized data
    org.apache.avro.generic.GenericRecord avroRecord1
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema1).set("key", 15).build();
    byte[] serializedRecord1 = serializer.serialize(TOPIC, avroRecord1);
    org.apache.avro.generic.GenericRecord avroRecord2
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2).set("key", 15).set
        ("value", "bar").build();
    byte[] serializedRecord2 = serializer.serialize(TOPIC, avroRecord2);


    SchemaAndValue converted1 = avroConverter.toConnectData(TOPIC, serializedRecord1);
    assertEquals(1L, (long) converted1.schema().version());

    SchemaAndValue converted2 = avroConverter.toConnectData(TOPIC, serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());
  }

  private void testVersionExtractedFromMetadata(String subject, KafkaAvroSerializer serializer,
      AvroConverter avroConverter) throws IOException, RestClientException {
    // Pre-register to ensure ordering
    org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
        .record("Foo").fields()
        .requiredInt("key")
        .endRecord();
    schemaRegistry.register(subject, new AvroSchema(avroSchema1).copy(
        new Metadata(null, ImmutableMap.of("confluent:version", "2"), null), null));

    org.apache.avro.Schema avroSchema2 = org.apache.avro.SchemaBuilder
        .record("Foo").fields()
        .requiredInt("key")
        .requiredString("value")
        .endRecord();
    schemaRegistry.register(subject, new AvroSchema(avroSchema2).copy(
        new Metadata(null, ImmutableMap.of("confluent:version", "200"), null), null));


    // Get serialized data
    org.apache.avro.generic.GenericRecord avroRecord1
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema1).set("key", 15).build();
    byte[] serializedRecord1 = serializer.serialize(TOPIC, avroRecord1);
    org.apache.avro.generic.GenericRecord avroRecord2
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2).set("key", 15).set
        ("value", "bar").build();
    byte[] serializedRecord2 = serializer.serialize(TOPIC, avroRecord2);


    SchemaAndValue converted1 = avroConverter.toConnectData(TOPIC, serializedRecord1);
    assertEquals(2L, (long) converted1.schema().version());

    SchemaAndValue converted2 = avroConverter.toConnectData(TOPIC, serializedRecord2);
    assertEquals(200L, (long) converted2.schema().version());
  }


  @Test
  public void testVersionMaintained() {
    // Version info provided from the Copycat schema should be maintained. This should be true
    // regardless of any underlying schema registry versioning since the versions are explicitly
    // specified by the connector.

    // Use newer schema first
    Schema newerSchema = SchemaBuilder.struct().version(2)
        .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
        .field("new", Schema.OPTIONAL_INT16_SCHEMA)
        .build();
    SchemaAndValue newer = new SchemaAndValue(newerSchema, new Struct(newerSchema));
    byte[] newerSerialized = converter.fromConnectData(TOPIC, newer.schema(), newer.value());

    Schema olderSchema = SchemaBuilder.struct().version(1)
        .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
        .build();
    SchemaAndValue older = new SchemaAndValue(olderSchema, new Struct(olderSchema));
    byte[] olderSerialized = converter.fromConnectData(TOPIC, older.schema(), older.value());

    assertEquals(2L, (long) converter.toConnectData(TOPIC, newerSerialized).schema().version());
    assertEquals(1L, (long) converter.toConnectData(TOPIC, olderSerialized).schema().version());
  }


  @Test
  public void testSameSchemaMultipleTopicForValue() throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(SR_CONFIG, false);
    assertSameSchemaMultipleTopic(avroConverter, schemaRegistry, false);
  }

  @Test
  public void testSameSchemaMultipleTopicForKey() throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(SR_CONFIG, true);
    assertSameSchemaMultipleTopic(avroConverter, schemaRegistry, true);
  }

  @Test
  public void testSameSchemaMultipleTopicWithDeprecatedSubjectNameStrategyForValue() throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    Map<String, ?> converterConfig = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost",
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, DeprecatedTestTopicNameStrategy.class.getName());
    avroConverter.configure(converterConfig, false);
    assertSameSchemaMultipleTopic(avroConverter, schemaRegistry, false);
  }

  @Test
  public void testSameSchemaMultipleTopicWithDeprecatedSubjectNameStrategyForKey() throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    Map<String, ?> converterConfig = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost",
        AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, DeprecatedTestTopicNameStrategy.class.getName());
    avroConverter.configure(converterConfig, true);
    assertSameSchemaMultipleTopic(avroConverter, schemaRegistry, true);
  }

  @Test
  public void testExplicitlyNamedNestedMapsWithNonStringKeys() {
    final Schema schema = SchemaBuilder.map(
        Schema.OPTIONAL_STRING_SCHEMA,
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA,
            Schema.INT32_SCHEMA
        ).name("foo.bar").build()
    ).name("biz.baz").version(1).build();
    final AvroConverter avroConverter = new AvroConverter(new MockSchemaRegistryClient());
    avroConverter.configure(
        Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost"
        ),
        false
    );
    final Object value = Collections.singletonMap("foo", Collections.singletonMap("bar", 1));

    final byte[] bytes = avroConverter.fromConnectData("topic", schema, value);
    final SchemaAndValue schemaAndValue = avroConverter.toConnectData("topic", bytes);

    assertThat(schemaAndValue.schema(), equalTo(schema));
    assertThat(schemaAndValue.value(), equalTo(value));
  }

  @Test
  public void testSingleFieldSerialization() throws RestClientException, IOException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    Map<String, ?> converterConfig = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost",
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
        AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, false,
        AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, true,
        AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true,
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());
    avroConverter.configure(converterConfig, false);

    org.apache.avro.Schema registredSchema = org.apache.avro.SchemaBuilder
        .record("MySchema")
        .fields()
        .requiredString("id")
        .endRecord();

    schemaRegistry.register("topic-value", new AvroSchema(registredSchema));

    Schema inputSchema1 = SchemaBuilder.struct()
        .field("foo", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .build();

    Struct inputValue1 = new Struct(inputSchema1)
        .put("foo", "123")
        .put("id", "456");

    Schema inputSchema2 = SchemaBuilder.struct()
        .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("foo", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .build();

    Struct inputValue2 = new Struct(inputSchema2)
        .put("id", "456")
        .put("foo", "123");

    final byte[] bytes1 = avroConverter.fromConnectData("topic", inputSchema1, inputValue1);
    final SchemaAndValue schemaAndValue1 = avroConverter.toConnectData("topic", bytes1);

    final byte[] bytes2 = avroConverter.fromConnectData("topic", inputSchema2, inputValue2);
    final SchemaAndValue schemaAndValue2 = avroConverter.toConnectData("topic", bytes2);


    assertEquals(schemaAndValue1.value(), schemaAndValue2.value());
  }

  @Test
  public void testSingleFieldSerializationDifferentCase() throws RestClientException, IOException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    Map<String, ?> converterConfig = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost",
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
        AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, false,
        AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, true,
        AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true,
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());
    avroConverter.configure(converterConfig, false);

    org.apache.avro.Schema registredSchema = org.apache.avro.SchemaBuilder
        .record("MySchema")
        .fields()
        .requiredString("id")
        .endRecord();

    schemaRegistry.register("topic-value", new AvroSchema(registredSchema));

    Schema inputSchema1 = SchemaBuilder.struct()
        .field("ID", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .build();

    Struct inputValue1 = new Struct(inputSchema1)
        .put("ID", "456");

    Schema inputSchema2 = SchemaBuilder.struct()
        .field("ID", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .build();

    Struct inputValue2 = new Struct(inputSchema2)
        .put("ID", "456");

    final byte[] bytes1 = avroConverter.fromConnectData("topic", inputSchema1, inputValue1);
    final SchemaAndValue schemaAndValue1 = avroConverter.toConnectData("topic", bytes1);

    final byte[] bytes2 = avroConverter.fromConnectData("topic", inputSchema2, inputValue2);
    final SchemaAndValue schemaAndValue2 = avroConverter.toConnectData("topic", bytes2);


    assertEquals(schemaAndValue1.value(), schemaAndValue2.value());
  }

  private void assertSameSchemaMultipleTopic(AvroConverter converter, SchemaRegistryClient schemaRegistry, boolean isKey) throws IOException, RestClientException {
    org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
        .record("Foo").fields()
        .requiredInt("key")
        .endRecord();

    org.apache.avro.Schema avroSchema2_1 = org.apache.avro.SchemaBuilder
        .record("Foo").fields()
        .requiredInt("key")
        .requiredString("value")
        .endRecord();
    org.apache.avro.Schema avroSchema2_2 = org.apache.avro.SchemaBuilder
        .record("Foo").fields()
        .requiredInt("key")
        .requiredString("value")
        .endRecord();
    String subjectSuffix = isKey ? "key" : "value";
    schemaRegistry.register("topic1-" + subjectSuffix, new AvroSchema(avroSchema2_1));
    schemaRegistry.register("topic2-" + subjectSuffix, new AvroSchema(avroSchema1));
    schemaRegistry.register("topic2-" + subjectSuffix, new AvroSchema(avroSchema2_2));

    org.apache.avro.generic.GenericRecord avroRecord1
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2_1).set("key", 15).set
        ("value", "bar").build();
    org.apache.avro.generic.GenericRecord avroRecord2
        = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2_2).set("key", 15).set
        ("value", "bar").build();


    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
    serializer.configure(SR_CONFIG, isKey);
    byte[] serializedRecord1 = serializer.serialize("topic1", avroRecord1);
    byte[] serializedRecord2 = serializer.serialize("topic2", avroRecord2);

    SchemaAndValue converted1 = converter.toConnectData("topic1", serializedRecord1);
    assertEquals(1L, (long) converted1.schema().version());

    SchemaAndValue converted2 = converter.toConnectData("topic2", serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());

    converted2 = converter.toConnectData("topic2", serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());
  }

  @Test(expected = NetworkException.class)
  public void testFromConnectDataThrowsNetworkExceptionOnSerializationExceptionCausedByNetworkConnectionException() {
    AvroConverter.Serializer serializer = mock(AvroConverter.Serializer.class);
    SerializationException serializationException = new SerializationException("fail", new SocketException("io fail"));
    AvroData avroData = new AvroData(
        new AvroDataConfig(Collections.singletonMap("schema.registry.url", "http://fake-url")));
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(Schema.STRING_SCHEMA);

    when(serializer.serialize(TOPIC, false, null,
        avroData.fromConnectData(Schema.STRING_SCHEMA, avroSchema, "value"),
        new AvroSchema(avroSchema))).thenThrow(serializationException);

    try {
      java.lang.reflect.Field serializerField = AvroConverter.class.getDeclaredField("serializer");
      serializerField.setAccessible(true);
      serializerField.set(converter, serializer);
    } catch (Exception e) {
      fail("Reflection failed: " + e);
    }

    converter.fromConnectData(TOPIC, Schema.STRING_SCHEMA, "value");
  }

  @Test(expected = NetworkException.class)
  public void testToConnectDataThrowsNetworkExceptionOnSerializationExceptionCausedByNetworkConnectionException() {
    AvroConverter.Deserializer deserializer = mock(AvroConverter.Deserializer.class);
    SerializationException serializationException = new SerializationException("fail", new UnknownHostException("io fail"));
    SchemaAndValue schemaAndValue = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
    byte[] valueBytes =
        converter.fromConnectData(TOPIC, schemaAndValue.schema(), schemaAndValue.value());
    when(deserializer.deserialize(TOPIC, false, null, valueBytes)).thenThrow(
        serializationException);

    try {
      java.lang.reflect.Field deserializerField = AvroConverter.class.getDeclaredField("deserializer");
      deserializerField.setAccessible(true);
      deserializerField.set(converter, deserializer);
    } catch (Exception e) {
      fail("Reflection failed: " + e);
    }

    converter.toConnectData(TOPIC, valueBytes);
  }
}
