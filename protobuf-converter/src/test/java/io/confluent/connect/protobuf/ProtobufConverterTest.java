/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package io.confluent.connect.protobuf;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage2;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.protobuf.test.Key;
import io.confluent.connect.protobuf.test.KeyValue;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;

import static io.confluent.connect.protobuf.ProtobufData.PROTOBUF_TYPE_TAG;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProtobufConverterTest {

  private static final String TOPIC = "topic";

  private static final int PROTOBUF_BYTES_START = 6; // extra byte for message index
  private static final String TEST_MSG_STRING = "Hello World";
  private static final TestMessage HELLO_WORLD_MESSAGE = TestMessage.newBuilder()
      .setTestString(TEST_MSG_STRING)
      .setTestInt32(123)
      .build();
  private static final TestMessage2 HELLO_WORLD_MESSAGE2 = TestMessage2.newBuilder()
      .setTestString(TEST_MSG_STRING)
      .setTestInt32(123)
      .build();
  private static final TestMessage2 HELLO_WORLD_MESSAGE_NESTED = TestMessage2.newBuilder()
      .setTestString(TEST_MSG_STRING)
      .setTestInt32(123)
      .setTestMessage(HELLO_WORLD_MESSAGE)
      .build();

  private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      "localhost"
  );

  private final SchemaRegistryClient schemaRegistry;
  private final ProtobufConverter converter;

  public ProtobufConverterTest() {
    schemaRegistry = new MockSchemaRegistryClient();
    converter = new ProtobufConverter(schemaRegistry);
  }

  @Before
  public void setUp() {
    converter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
  }

  private Schema getTestMessageSchema() {
    return getTestMessageSchema("TestMessage");
  }

  private Schema getTestMessageSchema(String name) {
    return getTestMessageSchemaBuilder(name).version(1).build();
  }

  private SchemaBuilder getTestMessageSchemaBuilder(String name) {
    final SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(name);
    builder.field(
        "test_string",
        SchemaBuilder.string().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    builder.field(
        "test_bool",
        SchemaBuilder.bool().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(2)).build()
    );
    builder.field(
        "test_bytes",
        SchemaBuilder.bytes().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(3)).build()
    );
    builder.field(
        "test_double",
        SchemaBuilder.float64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(4)).build()
    );
    builder.field(
        "test_float",
        SchemaBuilder.float32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(5)).build()
    );
    builder.field(
        "test_fixed32",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(6)).build()
    );
    builder.field(
        "test_fixed64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(7)).build()
    );
    builder.field(
        "test_int32",
        SchemaBuilder.int32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(8)).build()
    );
    builder.field(
        "test_int64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(9)).build()
    );
    builder.field(
        "test_sfixed32",
        SchemaBuilder.int32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(10)).build()
    );
    builder.field(
        "test_sfixed64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(11)).build()
    );
    builder.field(
        "test_sint32",
        SchemaBuilder.int32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(12)).build()
    );
    builder.field(
        "test_sint64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(13)).build()
    );
    builder.field(
        "test_uint32",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(14)).build()
    );
    builder.field(
        "test_uint64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(15)).build()
    );
    return builder;
  }

  private Struct getTestMessageStruct(String messageText, int messageInt) {
    return getTestMessageStruct("TestMessage", messageText, messageInt);
  }

  private Struct getTestMessageStruct(String schemaName, String messageText, int messageInt) {
    Schema schema = getTestMessageSchema(schemaName);
    return getTestMessageStruct(schema, messageText, messageInt);
  }

  private Struct getTestMessageStruct(Schema schema, String messageText, int messageInt) {
    Struct result = new Struct(schema.schema());
    result.put("test_string", messageText);
    result.put("test_bool", false);
    result.put("test_bytes", ByteBuffer.allocate(0));
    result.put("test_double", 0.0d);
    result.put("test_float", 0.0f);
    result.put("test_fixed32", 0L);
    result.put("test_fixed64", 0L);
    result.put("test_int32", messageInt);
    result.put("test_int64", 0L);
    result.put("test_sfixed32", 0);
    result.put("test_sfixed64", 0L);
    result.put("test_sint32", 0);
    result.put("test_sint64", 0L);
    result.put("test_uint32", 0L);
    result.put("test_uint64", 0L);
    return result;
  }

  private ProtobufConverter getConfiguredProtobufConverter(boolean isKey) {
    ProtobufConverter protobufConverter = new ProtobufConverter();

    Map<String, Object> configs = new HashMap<>();
    configs.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");

    protobufConverter.configure(configs, isKey);

    return protobufConverter;
  }

  @Test
  public void testFromConnectDataForKey() {
    final byte[] expected = HELLO_WORLD_MESSAGE.toByteArray();

    converter.configure(SR_CONFIG, true);
    byte[] result = converter.fromConnectData("my-topic",
        getTestMessageSchema(),
        getTestMessageStruct(TEST_MSG_STRING, 123)
    );

    assertArrayEquals(expected, Arrays.copyOfRange(result, PROTOBUF_BYTES_START, result.length));
  }

  @Test
  public void testFromConnectDataForValue() {
    final byte[] expected = HELLO_WORLD_MESSAGE.toByteArray();

    converter.configure(SR_CONFIG, false);
    byte[] result = converter.fromConnectData("my-topic",
        getTestMessageSchema(),
        getTestMessageStruct(TEST_MSG_STRING, 123)
    );

    assertArrayEquals(expected, Arrays.copyOfRange(result, PROTOBUF_BYTES_START, result.length));
  }

  @Test
  public void testFromConnectDataForValueWithNamespace() {
    final byte[] expected = HELLO_WORLD_MESSAGE.toByteArray();

    converter.configure(SR_CONFIG, false);
    String fullName = "io.confluent.kafka.serializers.protobuf.test.TestMessage";
    byte[] result = converter.fromConnectData("my-topic",
        getTestMessageSchema(fullName),
        getTestMessageStruct(fullName, TEST_MSG_STRING, 123)
    );

    assertArrayEquals(expected, Arrays.copyOfRange(result, PROTOBUF_BYTES_START, result.length));
  }

  @Test
  public void testFromConnectDataForValueWithNamespaceNested() {
    final byte[] expected = HELLO_WORLD_MESSAGE_NESTED.toByteArray();

    converter.configure(SR_CONFIG, false);
    String fullName = "io.confluent.kafka.serializers.protobuf.test.TestMessage2";
    Schema nested = getTestMessageSchemaBuilder(
        "io.confluent.kafka.serializers.protobuf.test.TestMessage")
        .optional()
        .parameter(PROTOBUF_TYPE_TAG, String.valueOf(16))
        .build();
    SchemaBuilder builder = getTestMessageSchemaBuilder(fullName);
    builder.field(
        "test_message",
        nested
    );
    Schema schema = builder.version(1).build();
    Struct struct = getTestMessageStruct(schema, TEST_MSG_STRING, 123);
    struct.put("test_message", getTestMessageStruct(nested, TEST_MSG_STRING, 123));
    byte[] result = converter.fromConnectData("my-topic",
        schema,
        struct
    );

    assertArrayEquals(expected, Arrays.copyOfRange(result, PROTOBUF_BYTES_START, result.length));
  }

  @Test
  public void testToConnectDataForKey() throws Exception {
    converter.configure(SR_CONFIG, true);
    // extra byte for message index
    final byte[] input = concat(new byte[]{0, 0, 0, 0, 1, 0}, HELLO_WORLD_MESSAGE.toByteArray());
    schemaRegistry.register("my-topic-key", getSchema(TestMessage.getDescriptor()));
    SchemaAndValue result = converter.toConnectData("my-topic", input);

    SchemaAndValue expected = new SchemaAndValue(getTestMessageSchema(),
        getTestMessageStruct(TEST_MSG_STRING, 123)
    );

    assertEquals(expected, result);
  }

  @Test
  public void testToConnectDataForKeyWithSecondMessage() throws Exception {
    converter.configure(SR_CONFIG, true);
    // extra bytes for message index
    final byte[] input = concat(new byte[]{0, 0, 0, 0, 1, 2, 2}, HELLO_WORLD_MESSAGE2.toByteArray());
    schemaRegistry.register("my-topic-key", getSchema(TestMessage2.getDescriptor()));
    SchemaAndValue result = converter.toConnectData("my-topic", input);

    SchemaBuilder builder = getTestMessageSchemaBuilder("TestMessage2");
    builder.field(
        "test_message",
        getTestMessageSchemaBuilder("TestMessage")
            .optional()
            .parameter(PROTOBUF_TYPE_TAG, String.valueOf(16))
            .build()
    );
    Schema schema = builder.version(1).build();
    Struct struct = getTestMessageStruct(schema, TEST_MSG_STRING, 123);
    struct.put("test_message", null);
    SchemaAndValue expected = new SchemaAndValue(schema, struct);

    assertEquals(expected, result);
  }

  @Test
  public void testToConnectDataForValue() throws Exception {
    converter.configure(SR_CONFIG, false);
    // extra byte for message index
    final byte[] input = concat(new byte[]{0, 0, 0, 0, 1, 0}, HELLO_WORLD_MESSAGE.toByteArray());
    schemaRegistry.register("my-topic-value", getSchema(TestMessage.getDescriptor()));
    SchemaAndValue result = converter.toConnectData("my-topic", input);

    SchemaAndValue expected = new SchemaAndValue(getTestMessageSchema(),
        getTestMessageStruct(TEST_MSG_STRING, 123)
    );

    assertEquals(expected, result);
  }

  @Test
  public void testToConnectDataForValueWithSecondMessage() throws Exception {
    converter.configure(SR_CONFIG, false);
    // extra bytes for message index
    final byte[] input = concat(new byte[]{0, 0, 0, 0, 1, 2, 2}, HELLO_WORLD_MESSAGE2.toByteArray());
    schemaRegistry.register("my-topic-value", getSchema(TestMessage2.getDescriptor()));
    SchemaAndValue result = converter.toConnectData("my-topic", input);

    SchemaBuilder builder = getTestMessageSchemaBuilder("TestMessage2");
    builder.field(
        "test_message",
        getTestMessageSchemaBuilder("TestMessage")
            .optional()
            .parameter(PROTOBUF_TYPE_TAG, String.valueOf(16))
            .build()
    );
    Schema schema = builder.version(1).build();
    Struct struct = getTestMessageStruct(schema, TEST_MSG_STRING, 123);
    struct.put("test_message", null);
    SchemaAndValue expected = new SchemaAndValue(schema, struct);

    assertEquals(expected, result);
  }

  @Test
  public void testToConnectDataForValueWithBothMessages() throws Exception {
    converter.configure(SR_CONFIG, false);
    // extra byte for message index
    byte[] input = concat(new byte[]{0, 0, 0, 0, 1, 0}, HELLO_WORLD_MESSAGE.toByteArray());
    schemaRegistry.register("my-topic-value", getSchema(TestMessage.getDescriptor()));
    SchemaAndValue result = converter.toConnectData("my-topic", input);

    SchemaAndValue expected = new SchemaAndValue(getTestMessageSchema(),
            getTestMessageStruct(TEST_MSG_STRING, 123)
    );

    assertEquals(expected, result);

    // extra bytes for message index
    input = concat(new byte[]{0, 0, 0, 0, 1, 2, 2}, HELLO_WORLD_MESSAGE2.toByteArray());
    schemaRegistry.register("my-topic-value", getSchema(TestMessage2.getDescriptor()));
    result = converter.toConnectData("my-topic", input);

    SchemaBuilder builder = getTestMessageSchemaBuilder("TestMessage2");
    builder.field(
            "test_message",
            getTestMessageSchemaBuilder("TestMessage")
                    .optional()
                    .parameter(PROTOBUF_TYPE_TAG, String.valueOf(16))
                    .build()
    );
    Schema schema = builder.version(1).build();
    Struct struct = getTestMessageStruct(schema, TEST_MSG_STRING, 123);
    struct.put("test_message", null);
    expected = new SchemaAndValue(schema, struct);

    assertEquals(expected, result);
  }

  @Test
  public void testToConnectDataForValueWithNamespace() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    configs.put(ProtobufDataConfig.ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG, true);
    converter.configure(configs, false);
    // extra byte for message index
    final byte[] input = concat(new byte[]{0, 0, 0, 0, 1, 0}, HELLO_WORLD_MESSAGE.toByteArray());
    schemaRegistry.register("my-topic-value", getSchema(TestMessage.getDescriptor()));
    SchemaAndValue result = converter.toConnectData("my-topic", input);

    String fullName = "io.confluent.kafka.serializers.protobuf.test.TestMessage";
    SchemaAndValue expected = new SchemaAndValue(getTestMessageSchema(fullName),
        getTestMessageStruct(fullName, TEST_MSG_STRING, 123)
    );

    assertEquals(expected, result);
  }

  @Test
  public void testToConnectDataForValueWithNamespaceNested() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    configs.put(ProtobufDataConfig.ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG, true);
    converter.configure(configs, false);
    // extra bytes for message index
    final byte[] input = concat(new byte[]{0, 0, 0, 0, 1, 2, 2},
        HELLO_WORLD_MESSAGE_NESTED.toByteArray());
    schemaRegistry.register("my-topic-value", getSchema(TestMessage2.getDescriptor()));
    SchemaAndValue result = converter.toConnectData("my-topic", input);

    String fullName = "io.confluent.kafka.serializers.protobuf.test.TestMessage2";
    Schema nested = getTestMessageSchemaBuilder(
        "io.confluent.kafka.serializers.protobuf.test.TestMessage")
        .optional()
        .parameter(PROTOBUF_TYPE_TAG, String.valueOf(16))
        .build();
    SchemaBuilder builder = getTestMessageSchemaBuilder(fullName);
    builder.field(
        "test_message",
        nested
    );
    Schema schema = builder.version(1).build();
    Struct struct = getTestMessageStruct(schema, TEST_MSG_STRING, 123);
    struct.put("test_message", getTestMessageStruct(nested, TEST_MSG_STRING, 123));
    SchemaAndValue expected = new SchemaAndValue(schema, struct);

    assertEquals(expected, result);
  }

  @Test
  public void testComplex() {
    SchemaBuilder builder = SchemaBuilder.struct()
        .field(
            "int32",
            SchemaBuilder.int32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "1")
                .build()
        )
        .field(
            "int64",
            SchemaBuilder.int64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "2")
                .build()
        )
        .field(
            "float32",
            SchemaBuilder.float32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "3")
                .build()
        )
        .field(
            "float64",
            SchemaBuilder.float64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "4")
                .build()
        )
        .field(
            "boolean",
            SchemaBuilder.bool()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "5")
                .build()
        )
        .field(
            "string",
            SchemaBuilder.string()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "6")
                .build()
        )
        .field(
            "bytes",
            SchemaBuilder.bytes()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "7")
                .build()
        )
        .field(
            "array",
            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "8")
                .build()
        )
        .field("map", SchemaBuilder.map(SchemaBuilder.string()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "1")
                .build(),
            SchemaBuilder.int32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "2")
                .build()
        )
            .name("connect_default2")
            .optional()
            .parameter("io.confluent.connect.protobuf.Tag", "9")
            .build());
    Schema schema = builder.build();
    Struct original = new Struct(schema).put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", ByteBuffer.wrap("foo".getBytes()))
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1));
    // Because of registration in schema registry and lookup, we'll have added a version number
    Schema expectedSchema = builder.name("ConnectDefault1").version(1).build();
    Struct expected = new Struct(expectedSchema).put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", ByteBuffer.wrap("foo".getBytes()).asReadOnlyBuffer())
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1));

    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(expected.schema(), ((Struct) schemaAndValue.value()).schema());
    assertEquals(expected, schemaAndValue.value());
  }

  @Test
  public void testNull() {
    Schema schema = SchemaBuilder.struct().build();
    byte[] converted = converter.fromConnectData(TOPIC, schema, null);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(SchemaAndValue.NULL, schemaAndValue);
  }

  @Test
  public void testVersionExtractedForDefaultSubjectNameStrategy() throws Exception {
    // Version info should be extracted even if the data was not created with Copycat. Manually
    // register a few compatible schemas and validate that data serialized with our normal
    // serializer can be read and gets version info inserted
    String subject = TOPIC + "-value";
    KafkaProtobufSerializer serializer = new KafkaProtobufSerializer(schemaRegistry,
        ImmutableMap.of("schema.registry.url", "http://fake-url")
    );
    ProtobufConverter protobufConverter = new ProtobufConverter(schemaRegistry);
    protobufConverter.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"),
        false
    );
    testVersionExtracted(subject, serializer, protobufConverter);
  }

  private void testVersionExtracted(
      String subject,
      KafkaProtobufSerializer serializer,
      ProtobufConverter jsonConverter
  ) throws IOException, RestClientException {
    Key.KeyMessage.Builder keyBuilder = Key.KeyMessage.newBuilder();
    Key.KeyMessage keyMessage = keyBuilder.setKey(15).build();
    KeyValue.KeyValueMessage.Builder kvBuilder = KeyValue.KeyValueMessage.newBuilder();
    KeyValue.KeyValueMessage keyValueMessage = kvBuilder.setKey(15).setValue("bar").build();

    // Get serialized data
    byte[] serializedRecord1 = serializer.serialize(TOPIC, keyMessage);
    byte[] serializedRecord2 = serializer.serialize(TOPIC, keyValueMessage);

    SchemaAndValue converted1 = jsonConverter.toConnectData(TOPIC, serializedRecord1);
    assertEquals(1L, (long) converted1.schema().version());

    SchemaAndValue converted2 = jsonConverter.toConnectData(TOPIC, serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());
  }

  @Test
  @Ignore // TODO currently don't have a way to store extra metadata such as version
  public void testVersionMaintained() {
    // Version info provided from the Copycat schema should be maintained. This should be true
    // regardless of any underlying schema registry versioning since the versions are explicitly
    // specified by the connector.

    // Use newer schema first
    Schema newerSchema = SchemaBuilder.struct()
        .version(2)
        .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
        .field("new", Schema.OPTIONAL_INT16_SCHEMA)
        .build();
    SchemaAndValue newer = new SchemaAndValue(newerSchema, new Struct(newerSchema));
    byte[] newerSerialized = converter.fromConnectData(TOPIC, newer.schema(), newer.value());

    Schema olderSchema = SchemaBuilder.struct()
        .version(1)
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
    ProtobufConverter protobufConverter = new ProtobufConverter(schemaRegistry);
    protobufConverter.configure(SR_CONFIG, false);
    assertSameSchemaMultipleTopic(protobufConverter, schemaRegistry, false);
  }

  @Test
  public void testSameSchemaMultipleTopicForKey() throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    ProtobufConverter protobufConverter = new ProtobufConverter(schemaRegistry);
    protobufConverter.configure(SR_CONFIG, true);
    assertSameSchemaMultipleTopic(protobufConverter, schemaRegistry, true);
  }

  @Test
  public void testExplicitlyNamedNestedMapsWithNonStringKeys() {
    final Schema fieldschema = SchemaBuilder.map(SchemaBuilder.string()
        .optional()
        .parameter("io.confluent.connect.protobuf.Tag", "1")
        .build(), SchemaBuilder.map(SchemaBuilder.string()
            .optional()
            .parameter("io.confluent.connect.protobuf.Tag", "1")
            .build(),
        SchemaBuilder.int32()
            .optional()
            .parameter("io.confluent.connect.protobuf.Tag", "2")
            .build()
    ).name("foo_bar").optional().parameter("io.confluent.connect.protobuf.Tag", "2").build())
        .name("biz_baz")
        .optional()
        .parameter("io.confluent.connect.protobuf.Tag", "1")
        .build();
    final Schema schema = SchemaBuilder.struct()
        .name("ConnectDefault1")
        .field("map", fieldschema)
        .version(1)
        .build();
    final ProtobufConverter protobufConverter =
        new ProtobufConverter(new MockSchemaRegistryClient());
    protobufConverter.configure(Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "localhost"
    ), false);
    final Object value = new Struct(schema).put(
        "map",
        Collections.singletonMap("foo", Collections.singletonMap("bar", 1))
    );

    final byte[] bytes = protobufConverter.fromConnectData("topic", schema, value);
    final SchemaAndValue schemaAndValue = protobufConverter.toConnectData("topic", bytes);

    assertEquals(schemaAndValue.schema(), schema);
    assertEquals(schemaAndValue.value(), value);
  }

  private void assertSameSchemaMultipleTopic(
      ProtobufConverter converter,
      SchemaRegistryClient schemaRegistry,
      boolean isKey
  ) throws IOException, RestClientException {
    Key.KeyMessage.Builder keyBuilder = Key.KeyMessage.newBuilder();
    Key.KeyMessage keyMessage = keyBuilder.setKey(15).build();
    KeyValue.KeyValueMessage.Builder kvBuilder = KeyValue.KeyValueMessage.newBuilder();
    KeyValue.KeyValueMessage keyValueMessage = kvBuilder.setKey(15).setValue("bar").build();
    KeyValue.KeyValueMessage.Builder kvBuilder2 = KeyValue.KeyValueMessage.newBuilder();
    KeyValue.KeyValueMessage keyValueMessage2 = kvBuilder2.setKey(15).setValue("bar").build();

    String subjectSuffix = isKey ? "key" : "value";
    ProtobufSchema schema = getSchema(keyValueMessage.getDescriptorForType());
    ReferenceSubjectNameStrategy strategy = new DefaultReferenceSubjectNameStrategy();
    schema = KafkaProtobufSerializer.resolveDependencies(
        schemaRegistry, true, false, null, strategy, "topic1", isKey, schema);
    schemaRegistry.register("topic1-" + subjectSuffix, schema);
    schema = getSchema(keyMessage.getDescriptorForType());
    schema = KafkaProtobufSerializer.resolveDependencies(
        schemaRegistry, true, false, null, strategy, "topic2", isKey, schema);
    schemaRegistry.register("topic2-" + subjectSuffix, schema);
    schema = getSchema(keyValueMessage2.getDescriptorForType());
    schema = KafkaProtobufSerializer.resolveDependencies(
        schemaRegistry, true, false, null, strategy, "topic2", isKey, schema);
    schemaRegistry.register("topic2-" + subjectSuffix, schema);

    KafkaProtobufSerializer serializer = new KafkaProtobufSerializer(schemaRegistry);
    serializer.configure(SR_CONFIG, isKey);
    byte[] serializedRecord1 = serializer.serialize("topic1", keyValueMessage);
    byte[] serializedRecord2 = serializer.serialize("topic2", keyValueMessage2);

    SchemaAndValue converted1 = converter.toConnectData("topic1", serializedRecord1);
    assertEquals(1L, (long) converted1.schema().version());

    SchemaAndValue converted2 = converter.toConnectData("topic2", serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());

    converted2 = converter.toConnectData("topic2", serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());
  }

  private static ProtobufSchema getSchema(Descriptor descriptor) {
    // Return a schema as Schema Registry would see it
    return new ProtobufSchema(new ProtobufSchema(descriptor).toString());
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }
}
