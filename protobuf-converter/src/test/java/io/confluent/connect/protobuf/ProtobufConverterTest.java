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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.ListValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.confluent.connect.protobuf.test.KeyValueOptional.KeyValueOptionalMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.test.KeyTimestampValueOuterClass.KeyTimestampValue;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage2;
import io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue;
import java.util.List;
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

import static io.confluent.connect.protobuf.ProtobufData.PROTOBUF_TYPE_PROP;
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
  private static final TimestampValue TIMESTAMP_VALUE = TimestampValue.newBuilder()
      .setValue(Timestamp.newBuilder().setSeconds(1000).build())
      .build();
  private static final KeyTimestampValue KEY_TIMESTAMP_VALUE = KeyTimestampValue.newBuilder()
      .setKey(123)
      .setValue(TIMESTAMP_VALUE)
      .setValue2(Timestamp.newBuilder().setSeconds(2000).build())
      .build();
  private static final KeyValueOptionalMessage KEY_VALUE_OPT = KeyValueOptionalMessage.newBuilder()
      .setKey(123)
      .setValue("")
      .build();

  private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      "localhost"
  );

  private final SchemaRegistryClient schemaRegistry;
  private final ProtobufConverter converter;

  public ProtobufConverterTest() {
    schemaRegistry = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
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
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(6))
            .parameter(PROTOBUF_TYPE_PROP, "fixed32").build()
    );
    builder.field(
        "test_fixed64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(7))
            .parameter(PROTOBUF_TYPE_PROP, "fixed64").build()
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
        SchemaBuilder.int32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(10))
            .parameter(PROTOBUF_TYPE_PROP, "sfixed32").build()
    );
    builder.field(
        "test_sfixed64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(11))
            .parameter(PROTOBUF_TYPE_PROP, "sfixed64").build()
    );
    builder.field(
        "test_sint32",
        SchemaBuilder.int32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(12))
            .parameter(PROTOBUF_TYPE_PROP, "sint32").build()
    );
    builder.field(
        "test_sint64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(13))
            .parameter(PROTOBUF_TYPE_PROP, "sint64").build()
    );
    builder.field(
        "test_uint32",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(14))
            .parameter(PROTOBUF_TYPE_PROP, "uint32").build()
    );
    builder.field(
        "test_uint64",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(15))
            .parameter(PROTOBUF_TYPE_PROP, "uint64").build()
    );
    return builder;
  }

  private SchemaBuilder getTimestampBuilder() {
    final SchemaBuilder builder = SchemaBuilder.struct();
    builder.name("google.protobuf.Timestamp");
    builder.field(
        "seconds",
        SchemaBuilder.int64().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    builder.field(
        "nanos",
        SchemaBuilder.int32().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(2)).build()
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

  private Struct getTimestampStruct(Schema schema, long seconds, int nanos) {
    Struct result = new Struct(schema.schema());
    result.put("seconds", seconds);
    result.put("nanos", nanos);
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
  public void testFromConnectDataWithReference() {
    final byte[] expected = TIMESTAMP_VALUE.toByteArray();

    converter.configure(SR_CONFIG, false);
    String fullName = "io.confluent.kafka.serializers.protobuf.test.TimestampValue";
    Schema timestampSchema =
        getTimestampBuilder().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build();
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(fullName);
    builder.field(
        "value",
        timestampSchema
    );
    Schema schema = builder.version(1).build();
    Struct struct = new Struct(schema);
    struct.put("value", getTimestampStruct(timestampSchema, 1000L, 0));
    byte[] result = converter.fromConnectData("my-topic",
        schema,
        struct
    );

    assertArrayEquals(expected, Arrays.copyOfRange(result, PROTOBUF_BYTES_START, result.length));
  }

  @Test
  public void testFromConnectDataWithReferenceUsingLatest() throws Exception {
    final byte[] expected = TIMESTAMP_VALUE.toByteArray();

    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");
    config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    config.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, "false");
    converter.configure(config, false);
    schemaRegistry.register("google/protobuf/timestamp.proto", new ProtobufSchema(Timestamp.getDescriptor()));
    SchemaReference ref = new SchemaReference("google/protobuf/timestamp.proto", "google/protobuf/timestamp.proto", 1);
    schemaRegistry.register("my-topic-value", new ProtobufSchema(TimestampValue.getDescriptor(), ImmutableList.of(ref)));

    String fullName = "io.confluent.kafka.serializers.protobuf.test.TimestampValue";
    Schema timestampSchema =
        getTimestampBuilder().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build();
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(fullName);
    builder.field(
        "value",
        timestampSchema
    );
    Schema schema = builder.version(1).build();
    Struct struct = new Struct(schema);
    struct.put("value", getTimestampStruct(timestampSchema, 1000L, 0));
    byte[] result = converter.fromConnectData("my-topic",
        schema,
        struct
    );

    assertArrayEquals(expected, Arrays.copyOfRange(result, PROTOBUF_BYTES_START, result.length));
  }

  @Test
  public void testFromConnectDataWithNestedReferenceUsingLatest() throws Exception {
    final byte[] expected = KEY_TIMESTAMP_VALUE.toByteArray();

    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");
    config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    config.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, "false");
    converter.configure(config, false);
    schemaRegistry.register("google/protobuf/timestamp.proto", new ProtobufSchema(Timestamp.getDescriptor()));
    SchemaReference ref1 = new SchemaReference("google/protobuf/timestamp.proto", "google/protobuf/timestamp.proto", 1);
    schemaRegistry.register("TimestampValue.proto", new ProtobufSchema(TimestampValue.getDescriptor(), ImmutableList.of(ref1)));
    SchemaReference ref2 = new SchemaReference("TimestampValue.proto", "TimestampValue.proto", 1);
    schemaRegistry.register("my-topic-value", new ProtobufSchema(KeyTimestampValue.getDescriptor(), ImmutableList.of(ref1, ref2)));

    String tsFullName = "io.confluent.kafka.serializers.protobuf.test.TimestampValue";
    Schema timestampSchema =
        getTimestampBuilder().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build();
    SchemaBuilder tsBuilder = SchemaBuilder.struct();
    tsBuilder.name(tsFullName);
    tsBuilder.field(
        "value",
        timestampSchema
    );
    tsBuilder.parameter(PROTOBUF_TYPE_TAG, String.valueOf(2));
    Schema tsSchema = tsBuilder.version(1).build();
    Struct tsStruct = new Struct(tsSchema);
    tsStruct.put("value", getTimestampStruct(timestampSchema, 1000L, 0));

    Schema timestampSchema2 =
        getTimestampBuilder().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(3)).build();
    String keyTsFullName = "io.confluent.kafka.serializers.protobuf.test.KeyTimestampValue";
    SchemaBuilder keyTsBuilder = SchemaBuilder.struct();
    keyTsBuilder.name(keyTsFullName);
    keyTsBuilder.field(
        "key",
        SchemaBuilder.int32().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    keyTsBuilder.field(
        "value",
        tsSchema
    );
    keyTsBuilder.field(
        "value2",
        timestampSchema2
    );
    Schema keyTsSchema = keyTsBuilder.version(1).build();
    Struct keyTsStruct = new Struct(keyTsSchema);
    keyTsStruct.put("key", 123);
    keyTsStruct.put("value", tsStruct);
    keyTsStruct.put("value2", getTimestampStruct(timestampSchema2, 2000L, 0));

    byte[] result = converter.fromConnectData("my-topic",
        keyTsSchema,
        keyTsStruct
    );

    assertArrayEquals(expected, Arrays.copyOfRange(result, PROTOBUF_BYTES_START, result.length));
  }

  @Test
  public void testFromConnectDataWithOptionalForNullablesUsingLatest() throws Exception {
    final byte[] expected = KEY_VALUE_OPT.toByteArray();

    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");
    config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    config.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, "false");
    config.put(ProtobufDataConfig.OPTIONAL_FOR_NULLABLES_CONFIG, "true");
    converter.configure(config, false);
    schemaRegistry.register("my-topic-value",
        new ProtobufSchema(KeyValueOptionalMessage.getDescriptor(), Collections.emptyList()));

    String fullName = "io.confluent.connect.protobuf.test.KeyValueOptional.KeyValueOptionalMessage";
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(fullName);
    builder.field("key",
        SchemaBuilder.int32().parameter(PROTOBUF_TYPE_TAG, String.valueOf(1)).build()
    );
    builder.field("value",
        SchemaBuilder.string().optional().parameter(PROTOBUF_TYPE_TAG, String.valueOf(2)).build()
    );
    Schema schema = builder.version(1).build();
    Struct struct = new Struct(schema);
    struct.put("key", 123);
    struct.put("value", "");
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
  public void testToConnectDataForProtobufStruct() throws Exception {
    Value v = Value.newBuilder().setNumberValue(123).build();
    ListValue listValue = ListValue.newBuilder().addValues(v).build();
    Value value = Value.newBuilder().setListValue(listValue).build();
    com.google.protobuf.Struct.Builder builder = com.google.protobuf.Struct.newBuilder();
    builder.putFields("key", value);
    com.google.protobuf.Struct struct = builder.build();

    Map<String, Object> configs = new HashMap<>();
    configs.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    configs.put(ProtobufDataConfig.ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG, true);
    converter.configure(configs, false);
    // extra bytes for message index
    final byte[] input = concat(new byte[]{0, 0, 0, 0, 1, 0},
        struct.toByteArray());
    schemaRegistry.register("my-topic-value", getSchema(com.google.protobuf.Struct.getDescriptor()));
    SchemaAndValue result = converter.toConnectData("my-topic", input);

    Struct data1 = (Struct) result.value();
    Map<String, Object> map = (Map<String, Object>) data1.get("fields");
    Struct data2 = (Struct) map.get("key");
    Struct data3 = (Struct) data2.get("kind_0");
    Struct data4 = (Struct) data3.get("list_value");
    List<Struct> list = (List<Struct>) data4.get("values");
    Struct data5 = list.get(0);
    Struct data6 = (Struct) data5.get("kind_0");
    Number value1 = (Number) data6.get("number_value");

    assertEquals(123, value1.intValue());
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
        .put("bytes", ByteBuffer.wrap("foo".getBytes()))
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1));

    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(expected.schema(), ((Struct) schemaAndValue.value()).schema());
    assertEquals(expected, schemaAndValue.value());
  }

  @Test
  public void testComplexOptional() {
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");
    config.put(ProtobufDataConfig.OPTIONAL_FOR_NULLABLES_CONFIG, true);
    ProtobufConverter converter = new ProtobufConverter(schemaRegistry);
    converter.configure(config, false);

    SchemaBuilder builder = SchemaBuilder.struct()
        .field(
            "int32_null",
            SchemaBuilder.int32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "1")
                .build()
        )
        .field(
            "int64_null",
            SchemaBuilder.int64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "2")
                .build()
        )
        .field(
            "float32_null",
            SchemaBuilder.float32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "3")
                .build()
        )
        .field(
            "float64_null",
            SchemaBuilder.float64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "4")
                .build()
        )
        .field(
            "boolean_null",
            SchemaBuilder.bool()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "5")
                .build()
        )
        .field(
            "string_null",
            SchemaBuilder.string()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "6")
                .build()
        )
        .field(
            "bytes_null",
            SchemaBuilder.bytes()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "7")
                .build()
        )
        .field(
            "int32_opt",
            SchemaBuilder.int32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "8")
                .build()
        )
        .field(
            "int64_opt",
            SchemaBuilder.int64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "9")
                .build()
        )
        .field(
            "float32_opt",
            SchemaBuilder.float32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "10")
                .build()
        )
        .field(
            "float64_opt",
            SchemaBuilder.float64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "11")
                .build()
        )
        .field(
            "boolean_opt",
            SchemaBuilder.bool()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "12")
                .build()
        )
        .field(
            "string_opt",
            SchemaBuilder.string()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "13")
                .build()
        )
        .field(
            "bytes_opt",
            SchemaBuilder.bytes()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "14")
                .build()
        )
        .field(
            "int32",
            SchemaBuilder.int32()
                .parameter("io.confluent.connect.protobuf.Tag", "15")
                .build()
        )
        .field(
            "int64",
            SchemaBuilder.int64()
                .parameter("io.confluent.connect.protobuf.Tag", "16")
                .build()
        )
        .field(
            "float32",
            SchemaBuilder.float32()
                .parameter("io.confluent.connect.protobuf.Tag", "17")
                .build()
        )
        .field(
            "float64",
            SchemaBuilder.float64()
                .parameter("io.confluent.connect.protobuf.Tag", "18")
                .build()
        )
        .field(
            "boolean",
            SchemaBuilder.bool()
                .parameter("io.confluent.connect.protobuf.Tag", "19")
                .build()
        )
        .field(
            "string",
            SchemaBuilder.string()
                .parameter("io.confluent.connect.protobuf.Tag", "20")
                .build()
        )
        .field(
            "bytes",
            SchemaBuilder.bytes()
                .parameter("io.confluent.connect.protobuf.Tag", "21")
                .build()
        )
        .field(
            "array",
            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "22")
                .build()
        )
        .field("map", SchemaBuilder.map(SchemaBuilder.string()
                    .parameter("io.confluent.connect.protobuf.Tag", "1")
                    .build(),
                SchemaBuilder.int32()
                    .parameter("io.confluent.connect.protobuf.Tag", "2")
                    .build()
            )
            .name("connect_default2")
            .optional()
            .parameter("io.confluent.connect.protobuf.Tag", "23")
            .build());
    Schema schema = builder.build();
    Struct original = new Struct(schema).put("int32_null", null)
        .put("int64_null", null)
        .put("float32_null", null)
        .put("float64_null", null)
        .put("boolean_null", null)
        .put("string_null", null)
        .put("bytes_null", null)
        .put("int32_opt", 0)
        .put("int64_opt", 0L)
        .put("float32_opt", 0.0f)
        .put("float64_opt", 0.0)
        .put("boolean_opt", false)
        .put("string_opt", "")
        .put("bytes_opt", ByteBuffer.wrap("".getBytes()))
        .put("int32", 12)
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
    Struct expected = new Struct(expectedSchema).put("int32_null", null)
        .put("int64_null", null)
        .put("float32_null", null)
        .put("float64_null", null)
        .put("boolean_null", null)
        .put("string_null", null)
        .put("bytes_null", null)
        .put("int32_opt", 0)
        .put("int64_opt", 0L)
        .put("float32_opt", 0.0f)
        .put("float64_opt", 0.0)
        .put("boolean_opt", false)
        .put("string_opt", "")
        .put("bytes_opt", ByteBuffer.wrap("".getBytes()).asReadOnlyBuffer())
        .put("int32", 12)
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
  public void testComplexWrappers() {
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");
    config.put(ProtobufDataConfig.WRAPPER_FOR_NULLABLES_CONFIG, true);
    ProtobufConverter converter = new ProtobufConverter(schemaRegistry);
    converter.configure(config, false);

    SchemaBuilder builder = SchemaBuilder.struct()
        .field(
            "int32_null",
            SchemaBuilder.int32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "1")
                .build()
        )
        .field(
            "int64_null",
            SchemaBuilder.int64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "2")
                .build()
        )
        .field(
            "float32_null",
            SchemaBuilder.float32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "3")
                .build()
        )
        .field(
            "float64_null",
            SchemaBuilder.float64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "4")
                .build()
        )
        .field(
            "boolean_null",
            SchemaBuilder.bool()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "5")
                .build()
        )
        .field(
            "string_null",
            SchemaBuilder.string()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "6")
                .build()
        )
        .field(
            "bytes_null",
            SchemaBuilder.bytes()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "7")
                .build()
        )
        .field(
            "int32_opt",
            SchemaBuilder.int32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "8")
                .build()
        )
        .field(
            "int64_opt",
            SchemaBuilder.int64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "9")
                .build()
        )
        .field(
            "float32_opt",
            SchemaBuilder.float32()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "10")
                .build()
        )
        .field(
            "float64_opt",
            SchemaBuilder.float64()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "11")
                .build()
        )
        .field(
            "boolean_opt",
            SchemaBuilder.bool()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "12")
                .build()
        )
        .field(
            "string_opt",
            SchemaBuilder.string()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "13")
                .build()
        )
        .field(
            "bytes_opt",
            SchemaBuilder.bytes()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "14")
                .build()
        )
        .field(
            "int32",
            SchemaBuilder.int32()
                .parameter("io.confluent.connect.protobuf.Tag", "15")
                .build()
        )
        .field(
            "int64",
            SchemaBuilder.int64()
                .parameter("io.confluent.connect.protobuf.Tag", "16")
                .build()
        )
        .field(
            "float32",
            SchemaBuilder.float32()
                .parameter("io.confluent.connect.protobuf.Tag", "17")
                .build()
        )
        .field(
            "float64",
            SchemaBuilder.float64()
                .parameter("io.confluent.connect.protobuf.Tag", "18")
                .build()
        )
        .field(
            "boolean",
            SchemaBuilder.bool()
                .parameter("io.confluent.connect.protobuf.Tag", "19")
                .build()
        )
        .field(
            "string",
            SchemaBuilder.string()
                .parameter("io.confluent.connect.protobuf.Tag", "20")
                .build()
        )
        .field(
            "bytes",
            SchemaBuilder.bytes()
                .parameter("io.confluent.connect.protobuf.Tag", "21")
                .build()
        )
        .field(
            "array",
            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", "22")
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
            .parameter("io.confluent.connect.protobuf.Tag", "23")
            .build());
    Schema schema = builder.build();
    Struct original = new Struct(schema).put("int32_null", null)
        .put("int64_null", null)
        .put("float32_null", null)
        .put("float64_null", null)
        .put("boolean_null", null)
        .put("string_null", null)
        .put("bytes_null", null)
        .put("int32_opt", 0)
        .put("int64_opt", 0L)
        .put("float32_opt", 0.0f)
        .put("float64_opt", 0.0)
        .put("boolean_opt", false)
        .put("string_opt", "")
        .put("bytes_opt", ByteBuffer.wrap("".getBytes()))
        .put("int32", 12)
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
    Struct expected = new Struct(expectedSchema).put("int32_null", null)
        .put("int64_null", null)
        .put("float32_null", null)
        .put("float64_null", null)
        .put("boolean_null", null)
        .put("string_null", null)
        .put("bytes_null", null)
        .put("int32_opt", 0)
        .put("int64_opt", 0L)
        .put("float32_opt", 0.0f)
        .put("float64_opt", 0.0)
        .put("boolean_opt", false)
        .put("string_opt", "")
        .put("bytes_opt", ByteBuffer.wrap("".getBytes()).asReadOnlyBuffer())
        .put("int32", 12)
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
  @Ignore // Protobuf does not support nested maps
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
        schemaRegistry, true, false, true, null, strategy, "topic1", isKey, schema);
    schemaRegistry.register("topic1-" + subjectSuffix, schema);
    schema = getSchema(keyMessage.getDescriptorForType());
    schema = KafkaProtobufSerializer.resolveDependencies(
        schemaRegistry, true, false, true, null, strategy, "topic2", isKey, schema);
    schemaRegistry.register("topic2-" + subjectSuffix, schema);
    schema = getSchema(keyValueMessage2.getDescriptorForType());
    schema = KafkaProtobufSerializer.resolveDependencies(
        schemaRegistry, true, false, true, null, strategy, "topic2", isKey, schema);
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
