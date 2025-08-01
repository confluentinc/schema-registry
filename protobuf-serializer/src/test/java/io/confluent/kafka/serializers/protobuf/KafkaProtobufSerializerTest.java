/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.serializers.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.Format;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.test.CustomOptions.CustomMessageOptions;
import io.confluent.kafka.serializers.protobuf.test.CustomOptions2;
import io.confluent.kafka.serializers.protobuf.test.DecimalValueOuterClass.DecimalValue;
import io.confluent.kafka.serializers.protobuf.test.DecimalValuePb2OuterClass.DecimalValuePb2;
import io.confluent.kafka.serializers.protobuf.test.Ranges;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import java.io.IOException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage2;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.test.TestMessageOptionalProtos;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.test.DependencyTestProto.DependencyMessage;
import io.confluent.kafka.serializers.protobuf.test.EnumReferenceOuter.EnumReference;
import io.confluent.kafka.serializers.protobuf.test.EnumRootOuter.EnumRoot;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.ComplexType;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.NestedMessage;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.Status;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.UserId;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage;

import static org.junit.Assert.assertEquals;

public class KafkaProtobufSerializerTest {

  private final Properties serializerConfig;
  private final Properties deserializerConfig;
  private final SchemaRegistryClient schemaRegistry;
  private final KafkaProtobufSerializer protobufSerializer;
  private final KafkaProtobufDeserializer protobufDeserializer;
  private final KafkaProtobufDeserializer deriveTypeDeserializer;
  private final KafkaProtobufDeserializer testMessageDeserializer;
  private final KafkaProtobufDeserializer nestedMessageDeserializer;
  private final KafkaProtobufDeserializer dependencyMessageDeserializer;
  private final KafkaProtobufDeserializer enumRefDeserializer;
  private final KafkaProtobufDeserializer innerMessageDeserializer;
  private final KafkaProtobufDeserializer optionalMessageDeserializer;
  private final String topic;

  private static final String TEST_MSG_STRING = "Hello World";
  private static final TestMessage HELLO_WORLD_MESSAGE = TestMessage.newBuilder()
      .setTestString(TEST_MSG_STRING)
      .setTestInt32(123)
      .build();
  private static final TestMessage2 HELLO_WORLD_MESSAGE2 = TestMessage2.newBuilder()
      .setTestString(TEST_MSG_STRING)
      .setTestInt32(123)
      .build();
  private static final UserId USER_ID = UserId.newBuilder().setKafkaUserId("user1").build();
  private static final ComplexType COMPLEX_TYPE = ComplexType.newBuilder()
      .setOneId("complex")
      .setIsActive(true)
      .build();
  private static final Timestamp TS = Timestamp.newBuilder()
      .setSeconds(1000)
      .setNanos(2000)
      .build();
  private static final NestedMessage NESTED_MESSAGE = NestedMessage.newBuilder()
      .setUserId(USER_ID)
      .setIsActive(true)
      .addExperimentsActive("first")
      .addExperimentsActive("second")
      .setUpdatedAt(TS)
      .setStatus(Status.ACTIVE)
      .setComplexType(COMPLEX_TYPE)
      .putMapType("key1", "value1")
      .putMapType("key2", "value2")
      .build();
  private static final DependencyMessage DEPENDENCY_MESSAGE = DependencyMessage.newBuilder()
      .setNestedMessage(NESTED_MESSAGE)
      .setIsActive(true)
      .setTestMesssage(HELLO_WORLD_MESSAGE)
      .build();
  private static final EnumReference ENUM_REF =
      EnumReference.newBuilder().setEnumRoot(EnumRoot.GOODBYE).build();
  private static final NestedMessage.InnerMessage INNER_MESSAGE =
      NestedMessage.InnerMessage.newBuilder().setId("inner").build();
  private static final TestMessageOptionalProtos.TestMessageOptional OPTIONAL_MESSAGE =
      TestMessageOptionalProtos.TestMessageOptional.newBuilder().setTestString("hi").build();
  private static final TestMessageOptionalProtos.TestMessageOptional OPTIONAL_MESSAGE_DEFAULT =
      TestMessageOptionalProtos.TestMessageOptional.newBuilder()
          .setTestString("hi")
          .setTestOptionalString("")
          .build();
  private static final CustomMessageOptions CUSTOM_MESSAGE_OPTIONS =
      CustomMessageOptions.newBuilder()
          .setTestString("hi")
          .setTestInt(123)
          .build();
  private static final CustomOptions2.FooBar FOO_BAR =
      CustomOptions2.FooBar.newBuilder()
          .setFoo(123)
          .build();

  public KafkaProtobufSerializerTest() {
    serializerConfig = createSerializerConfig();
    schemaRegistry = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
    protobufSerializer = new KafkaProtobufSerializer(schemaRegistry, new HashMap(serializerConfig));

    protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    Properties deriveTypeDeserializerConfig = new Properties();
    deriveTypeDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    deriveTypeDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG,
        true
    );
    deriveTypeDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deriveTypeDeserializerConfig),
        null
    );

    deserializerConfig = new Properties();
    deserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    testMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        TestMessage.class
    );

    nestedMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        NestedMessage.class
    );

    dependencyMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        DependencyMessage.class
    );

    enumRefDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        EnumReference.class
    );

    innerMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        NestedMessage.InnerMessage.class
    );

    optionalMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        TestMessageOptionalProtos.TestMessageOptional.class
    );

    topic = "test";
  }

  protected Properties createSerializerConfig() {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializerConfig.put(KafkaProtobufSerializerConfig.NORMALIZE_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_FORMAT, "ignore_extensions");
    return serializerConfig;
  }

  public static Object getField(DynamicMessage message, String fieldName) {
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      if (entry.getKey().getName().equals(fieldName)) {
        return entry.getValue();
      }
    }
    return null;
  }

  @Test
  public void testKafkaProtobufSerializer() {
    byte[] bytes;

    // specific -> specific
    RecordHeaders headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, HELLO_WORLD_MESSAGE);
    assertEquals(HELLO_WORLD_MESSAGE, testMessageDeserializer.deserialize(topic, headers, bytes));

    // specific -> derived
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, HELLO_WORLD_MESSAGE);
    assertEquals(HELLO_WORLD_MESSAGE, deriveTypeDeserializer.deserialize(topic, headers, bytes));

    // specific -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, HELLO_WORLD_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(HELLO_WORLD_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE.getTestInt32(), getField(message, "test_int32"));

    // dynamic -> specific
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    assertEquals(HELLO_WORLD_MESSAGE, testMessageDeserializer.deserialize(topic, headers, bytes));

    // dynamic -> derived
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    assertEquals(HELLO_WORLD_MESSAGE, deriveTypeDeserializer.deserialize(topic, headers, bytes));

    // dynamic -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(HELLO_WORLD_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE.getTestInt32(), getField(message, "test_int32"));


    // specific -> derived
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, HELLO_WORLD_MESSAGE2);
    assertEquals(HELLO_WORLD_MESSAGE2, deriveTypeDeserializer.deserialize(topic, headers, bytes));

    // specific -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, HELLO_WORLD_MESSAGE2);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(HELLO_WORLD_MESSAGE2.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE2.getTestInt32(), getField(message, "test_int32"));

    // dynamic -> derived
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    assertEquals(HELLO_WORLD_MESSAGE2, deriveTypeDeserializer.deserialize(topic, headers, bytes));

    // dynamic -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(HELLO_WORLD_MESSAGE2.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE2.getTestInt32(), getField(message, "test_int32"));


    // specific -> specific
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, NESTED_MESSAGE);
    assertEquals(NESTED_MESSAGE, nestedMessageDeserializer.deserialize(topic, headers, bytes));

    // dynamic -> derived
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, NESTED_MESSAGE);
    assertEquals(NESTED_MESSAGE, deriveTypeDeserializer.deserialize(topic, headers, bytes));

    // specific -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, NESTED_MESSAGE);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(NESTED_MESSAGE.getUserId().getKafkaUserId(),
        getField((DynamicMessage) getField(message, "user_id"), "kafka_user_id")
    );

    // dynamic -> specific
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    assertEquals(NESTED_MESSAGE, nestedMessageDeserializer.deserialize(topic, headers, bytes));

    // dynamic -> derived
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    assertEquals(NESTED_MESSAGE, deriveTypeDeserializer.deserialize(topic, headers, bytes));

    // specific -> specific
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(NESTED_MESSAGE.getUserId().getKafkaUserId(),
        getField((DynamicMessage) getField(message, "user_id"), "kafka_user_id")
    );

    // null -> null
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, null);
    assertEquals(null, protobufDeserializer.deserialize(topic, headers, bytes));
  }


  @Test(expected = InvalidConfigurationException.class)
  public void testKafkaJsonSchemaSerializerWithoutConfigure() {
    KafkaProtobufSerializer unconfiguredSerializer = new KafkaProtobufSerializer();
    unconfiguredSerializer.serialize(topic, HELLO_WORLD_MESSAGE);
  }

  @Test(expected = InvalidConfigurationException.class)
  public void testKafkaJsonSchemaDeserializerWithoutConfigure() {
    KafkaProtobufDeserializer unconfiguredSerializer = new KafkaProtobufDeserializer();
    byte[] randomBytes = "foo".getBytes();
    unconfiguredSerializer.deserialize("foo", randomBytes);
  }

  @Test
  public void testDependency() {
    byte[] bytes;

    // specific -> specific
    RecordHeaders headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, DEPENDENCY_MESSAGE);
    assertEquals(DEPENDENCY_MESSAGE, dependencyMessageDeserializer.deserialize(topic, headers, bytes));

    // specific -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, DEPENDENCY_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(DEPENDENCY_MESSAGE.getNestedMessage().getUserId().getKafkaUserId(),
        getField((DynamicMessage) getField((DynamicMessage) getField(message, "nested_message"),
            "user_id"
        ), "kafka_user_id")
    );
  }

  @Test
  public void testEnumRoot() {
    byte[] bytes;

    // specific -> specific
    RecordHeaders headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, ENUM_REF);
    assertEquals(ENUM_REF, enumRefDeserializer.deserialize(topic, headers, bytes));

    // specific -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, ENUM_REF);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(ENUM_REF.getEnumRoot().name(), ((EnumValueDescriptor) getField(message, "enum_root")).getName());
  }

  @Test
  public void testInner() {
    byte[] bytes;

    // specific -> specific
    RecordHeaders headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, INNER_MESSAGE);
    assertEquals(INNER_MESSAGE, innerMessageDeserializer.deserialize(topic, headers, bytes));

    // specific -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, INNER_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(INNER_MESSAGE.getId(), getField(message, "id"));
  }

  @Test
  public void testOptional() {
    String schemaString = "syntax = \"proto3\";\n"
        + "\n"
        + "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "option java_outer_classname = \"TestMessageOptionalProtos\";\n"
        + "\n"
        + "message TestMessageOptional {\n"
        + "    string test_string = 1;\n"
        + "    optional string test_optional_string = 2;\n"
        + "}";
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    // Ensure optional is preserved
    assertEquals(schema, new ProtobufSchema(schema.toDescriptor()));

    byte[] bytes;

    // specific -> specific
    RecordHeaders headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, OPTIONAL_MESSAGE);
    assertEquals(OPTIONAL_MESSAGE, optionalMessageDeserializer.deserialize(topic, headers, bytes));

    // specific -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, OPTIONAL_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(OPTIONAL_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(false, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));

    // dynamic -> specific
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    assertEquals(OPTIONAL_MESSAGE, optionalMessageDeserializer.deserialize(topic, headers, bytes));

    // dynamic -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(OPTIONAL_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(false, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));


    // specific -> specific
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, OPTIONAL_MESSAGE_DEFAULT);
    assertEquals(OPTIONAL_MESSAGE_DEFAULT, optionalMessageDeserializer.deserialize(topic, headers, bytes));

    // specific -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, OPTIONAL_MESSAGE_DEFAULT);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(OPTIONAL_MESSAGE_DEFAULT.getTestString(), getField(message, "test_string"));
    assertEquals(true, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));

    // dynamic -> specific
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    assertEquals(OPTIONAL_MESSAGE_DEFAULT, optionalMessageDeserializer.deserialize(topic, headers, bytes));

    // dynamic -> dynamic
    headers = new RecordHeaders();
    bytes = protobufSerializer.serialize(topic, headers, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, headers, bytes);
    assertEquals(OPTIONAL_MESSAGE_DEFAULT.getTestString(), getField(message, "test_string"));
    assertEquals(true, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));

    DynamicMessage.Builder builder = schema.newMessageBuilder();
    builder.setField(builder.getDescriptorForType().findFieldByName("test_string"), "hi");
    message = builder.build();
    assertEquals(OPTIONAL_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(false, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));

    builder = schema.newMessageBuilder();
    builder.setField(builder.getDescriptorForType().findFieldByName("test_string"), "hi");
    builder.setField(builder.getDescriptorForType().findFieldByName("test_optional_string"), "");
    message = builder.build();
    assertEquals(OPTIONAL_MESSAGE_DEFAULT.getTestString(), getField(message, "test_string"));
    assertEquals(true, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));
  }

  @Test
  public void testCustomOptions() throws Exception {
    String expected = "syntax = \"proto3\";\n"
        + "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option (io.confluent.kafka.serializers.protobuf.test.file_custom) = \"test\";\n"
        + "option (io.confluent.kafka.serializers.protobuf.test.file_custom2) = \"hello\";\n"
        + "option (io.confluent.kafka.serializers.protobuf.test.file_custom2) = \"world\";\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message CustomMessageOptions {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    doc: \"message\"\n"
        + "  };\n"
        + "  option (io.confluent.kafka.serializers.protobuf.test.message_custom) = true;\n"
        + "  option (io.confluent.kafka.serializers.protobuf.test.message_custom2) = true;\n"
        + "  option (io.confluent.kafka.serializers.protobuf.test.message_custom2) = false;\n"
        + "\n"
        + "  string test_string = 1 [\n"
        + "    (io.confluent.kafka.serializers.protobuf.test.field_custom) = 123,\n"
        + "    (io.confluent.kafka.serializers.protobuf.test.field_custom2) = 456,\n"
        + "    (io.confluent.kafka.serializers.protobuf.test.field_custom2) = 789\n"
        + "  ];\n"
        + "  int32 test_int = 2 [(confluent.field_meta) = {\n"
        + "    doc: \"field\"\n"
        + "  }];\n"
        + "}\n"
        + "message MyMessage {\n"
        + "  int32 id = 1;\n"
        + "  float f = 2;\n"
        + "  double d = 3;\n"
        + "  string doc = 4;\n"
        + "  map<string, string> params = 5;\n"
        + "  repeated int32 list = 6;\n"
        + "}\n"
        + "enum CustomEnumOptions {\n"
        + "  option (confluent.enum_meta) = {\n"
        + "    doc: \"enum\"\n"
        + "  };\n"
        + "  option (io.confluent.kafka.serializers.protobuf.test.enum_custom) = {\n"
        + "    d: 456,\n"
        + "    doc: \"hi\",\n"
        + "    f: 123,\n"
        + "    id: 1\n"
        + "  };\n"
        + "  option (io.confluent.kafka.serializers.protobuf.test.enum_custom2) = {\n"
        + "    doc: \"hi\",\n"
        + "    id: 2,\n"
        + "    list: [\n"
        + "      4,\n"
        + "      5,\n"
        + "      6\n"
        + "    ],\n"
        + "    params: {\n"
        + "      key: \"hello\",\n"
        + "      value: \"world\"\n"
        + "    }\n"
        + "  };\n"
        + "  option (io.confluent.kafka.serializers.protobuf.test.enum_custom2) = {\n"
        + "    doc: \"bye\",\n"
        + "    id: 3,\n"
        + "    list: [\n"
        + "      7,\n"
        + "      8,\n"
        + "      9\n"
        + "    ],\n"
        + "    params: {\n"
        + "      key: \"goodbye\",\n"
        + "      value: \"world\"\n"
        + "    }\n"
        + "  };\n"
        + "  CUSTOM0 = 0 [(io.confluent.kafka.serializers.protobuf.test.enum_value_custom) = ENUM1];\n"
        + "  CUSTOM1 = 1 [\n"
        + "    (io.confluent.kafka.serializers.protobuf.test.enum_value_custom2) = ENUM1,\n"
        + "    (io.confluent.kafka.serializers.protobuf.test.enum_value_custom2) = ENUM2\n"
        + "  ];\n"
        + "  CUSTOM2 = 2 [(confluent.enum_value_meta) = {\n"
        + "    doc: \"enum_value\"\n"
        + "  }];\n"
        + "}\n"
        + "enum MyEnum {\n"
        + "  ENUM0 = 0;\n"
        + "  ENUM1 = 1;\n"
        + "  ENUM2 = 2;\n"
        + "}\n"
        + "\n"
        + "extend .google.protobuf.FileOptions {\n"
        + "  string file_custom = 1111;\n"
        + "  repeated string file_custom2 = 1112;\n"
        + "}\n"
        + "extend .google.protobuf.MessageOptions {\n"
        + "  bool message_custom = 1111;\n"
        + "  repeated bool message_custom2 = 1112;\n"
        + "}\n"
        + "extend .google.protobuf.FieldOptions {\n"
        + "  int32 field_custom = 1111;\n"
        + "  repeated int32 field_custom2 = 1112;\n"
        + "}\n"
        + "extend .google.protobuf.EnumOptions {\n"
        + "  .io.confluent.kafka.serializers.protobuf.test.MyMessage enum_custom = 1111;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.MyMessage enum_custom2 = 1112;\n"
        + "}\n"
        + "extend .google.protobuf.EnumValueOptions {\n"
        + "  .io.confluent.kafka.serializers.protobuf.test.MyEnum enum_value_custom = 1111;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.MyEnum enum_value_custom2 = 1112;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(CustomMessageOptions.getDescriptor());
    schema = schema.normalize();
    assertEquals(expected, schema.canonicalString());
    schema = new ProtobufSchema(schema.canonicalString());
    assertEquals(expected, schema.canonicalString());

    expected = "syntax = \"proto3\";\n"
        + "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message CustomMessageOptions {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    doc: \"message\"\n"
        + "  };\n"
        + "\n"
        + "  string test_string = 1;\n"
        + "  int32 test_int = 2 [(confluent.field_meta) = {\n"
        + "    doc: \"field\"\n"
        + "  }];\n"
        + "}\n"
        + "message MyMessage {\n"
        + "  int32 id = 1;\n"
        + "  float f = 2;\n"
        + "  double d = 3;\n"
        + "  string doc = 4;\n"
        + "  map<string, string> params = 5;\n"
        + "  repeated int32 list = 6;\n"
        + "}\n"
        + "enum CustomEnumOptions {\n"
        + "  option (confluent.enum_meta) = {\n"
        + "    doc: \"enum\"\n"
        + "  };\n"
        + "  CUSTOM0 = 0;\n"
        + "  CUSTOM1 = 1;\n"
        + "  CUSTOM2 = 2 [(confluent.enum_value_meta) = {\n"
        + "    doc: \"enum_value\"\n"
        + "  }];\n"
        + "}\n"
        + "enum MyEnum {\n"
        + "  ENUM0 = 0;\n"
        + "  ENUM1 = 1;\n"
        + "  ENUM2 = 2;\n"
        + "}\n";
    String noCustSchema = schema.formattedString(Format.IGNORE_EXTENSIONS.symbol());
    assertEquals(expected, noCustSchema);

    RecordHeaders headers = new RecordHeaders();
    protobufSerializer.serialize(topic, headers, CUSTOM_MESSAGE_OPTIONS);
    ParsedSchema retrievedSchema = schemaRegistry.getSchemaBySubjectAndId(topic + "-value", 1);
    assertEquals(expected, retrievedSchema.canonicalString());
  }

  @Test
  public void testCustomOptions2() throws Exception {
    String expected = "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message FooBar {\n"
        + "  optional int32 foo = 1;\n"
        + "  optional string bar = 2;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.FooBar nested = 3;\n"
        + "\n"
        + "  extensions 100 to 200;\n"
        + "\n"
        + "  extend .google.protobuf.EnumOptions {\n"
        + "    optional string foobar_string = 71001;\n"
        + "  }\n"
        + "\n"
        + "  message More {\n"
        + "    option (io.confluent.kafka.serializers.protobuf.test.my_message_option) = {\n"
        + "      [io.confluent.kafka.serializers.protobuf.test.FooBar.More.more_string]: \"foobar\",\n"
        + "      [io.confluent.kafka.serializers.protobuf.test.rep]: [\n"
        + "        FOO,\n"
        + "        BAR\n"
        + "      ]\n"
        + "    };\n"
        + "  \n"
        + "    repeated int32 serial = 1;\n"
        + "  \n"
        + "    extend .io.confluent.kafka.serializers.protobuf.test.FooBar {\n"
        + "      optional string more_string = 150;\n"
        + "    }\n"
        + "  }\n"
        + "  message More2 {\n"
        + "    option (io.confluent.kafka.serializers.protobuf.test.my_message_option) = {\n"
        + "      [io.confluent.kafka.serializers.protobuf.test.FooBar.More2.more2_string]: \"foobar\",\n"
        + "      foo: 123\n"
        + "    };\n"
        + "  \n"
        + "    repeated int32 serial = 1;\n"
        + "  \n"
        + "    extend .io.confluent.kafka.serializers.protobuf.test.FooBar {\n"
        + "      optional string more2_string = 151;\n"
        + "    }\n"
        + "  }\n"
        + "  enum FooBarBazEnum {\n"
        + "    option (io.confluent.kafka.serializers.protobuf.test.FooBar.foobar_string) = \"foobar\";\n"
        + "    FOO = 1;\n"
        + "    BAR = 2;\n"
        + "    BAZ = 3;\n"
        + "  }\n"
        + "}\n"
        + "\n"
        + "extend .google.protobuf.MessageOptions {\n"
        + "  optional .io.confluent.kafka.serializers.protobuf.test.FooBar my_message_option = 50099;\n"
        + "}\n"
        + "extend .io.confluent.kafka.serializers.protobuf.test.FooBar {\n"
        + "  optional .io.confluent.kafka.serializers.protobuf.test.FooBar.FooBarBazEnum ext = 101;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.FooBar.FooBarBazEnum rep = 102;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(CustomOptions2.FooBar.getDescriptor());
    schema = schema.normalize();
    assertEquals(expected, schema.canonicalString());
    schema = new ProtobufSchema(schema.canonicalString());
    assertEquals(expected, schema.canonicalString());

    expected = "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message FooBar {\n"
        + "  optional int32 foo = 1;\n"
        + "  optional string bar = 2;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.FooBar nested = 3;\n"
        + "\n"
        + "  message More {\n"
        + "    repeated int32 serial = 1;\n"
        + "  }\n"
        + "  message More2 {\n"
        + "    repeated int32 serial = 1;\n"
        + "  }\n"
        + "  enum FooBarBazEnum {\n"
        + "    FOO = 1;\n"
        + "    BAR = 2;\n"
        + "    BAZ = 3;\n"
        + "  }\n"
        + "}\n";
    String noCustSchema = schema.formattedString(Format.IGNORE_EXTENSIONS.symbol());
    assertEquals(expected, noCustSchema);

    RecordHeaders headers = new RecordHeaders();
    protobufSerializer.serialize(topic, headers, FOO_BAR);
    ParsedSchema retrievedSchema = schemaRegistry.getSchemaBySubjectAndId(topic + "-value", 1);
    assertEquals(expected, retrievedSchema.canonicalString());
  }

  @Test
  public void testNormalizeBothPb2andPb3() throws Exception {
    String expected = "syntax = \"proto3\";\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message DecimalValue {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    doc: \"message\"\n"
        + "  };\n"
        + "\n"
        + "  .confluent.type.Decimal value = 1 [(confluent.field_meta) = {\n"
        + "    params: [\n"
        + "      {\n"
        + "        key: \"precision\",\n"
        + "        value: \"8\"\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"scale\",\n"
        + "        value: \"3\"\n"
        + "      }\n"
        + "    ]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(DecimalValue.getDescriptor());
    schema = schema.normalize();
    assertEquals(expected, schema.canonicalString());
    schema = new ProtobufSchema(schema.canonicalString());
    assertEquals(expected, schema.canonicalString());

    expected = "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message DecimalValuePb2 {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    doc: \"message\"\n"
        + "  };\n"
        + "\n"
        + "  optional .confluent.type.Decimal value = 1 [(confluent.field_meta) = {\n"
        + "    params: [\n"
        + "      {\n"
        + "        key: \"precision\",\n"
        + "        value: \"8\"\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"scale\",\n"
        + "        value: \"3\"\n"
        + "      }\n"
        + "    ]\n"
        + "  }];\n"
        + "}\n";
    schema = new ProtobufSchema(DecimalValuePb2.getDescriptor());
    schema = schema.normalize();
    assertEquals(expected, schema.canonicalString());
    schema = new ProtobufSchema(schema.canonicalString());
    assertEquals(expected, schema.canonicalString());
  }

  @Test
  public void testRanges() {
    String expected = "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message FooBar2 {\n"
        + "  reserved 5000 to 6000;\n"
        + "  reserved 10000 to 10001;\n"
        + "  reserved 20000;\n"
        + "\n"
        + "  optional int32 foo = 1;\n"
        + "  optional string bar = 2;\n"
        + "\n"
        + "  extensions 100 to 200;\n"
        + "  extensions 1000 to 1001;\n"
        + "  extensions 2000;\n"
        + "\n"
        + "  enum FooBarBazEnum {\n"
        + "    reserved 100 to 200;\n"
        + "    reserved 1000 to 1001;\n"
        + "    reserved 2000;\n"
        + "    BUG = 0;\n"
        + "    ZAP = 1;\n"
        + "  }\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(Ranges.FooBar2.getDescriptor());
    schema = schema.normalize();
    assertEquals(expected, schema.canonicalString());
    schema = new ProtobufSchema(schema.canonicalString());
    assertEquals(expected, schema.canonicalString());
  }

  @Test
  public void testKafkaProtobufDeserializerWithPreRegisteredUseLatestRecordNameStrategy()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaProtobufSerializerConfig.USE_LATEST_VERSION,
        true,
        KafkaProtobufSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        RecordNameStrategy.class.getName()
    );
    protobufSerializer.configure(configs, false);
    testMessageDeserializer.configure(configs, false);
    ProtobufSchema schema = new ProtobufSchema(TestMessage.getDescriptor());
    schemaRegistry.register("io.confluent.kafka.serializers.protobuf.test.TestMessage", schema);
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = protobufSerializer.serialize(topic, headers, HELLO_WORLD_MESSAGE);
    assertEquals(HELLO_WORLD_MESSAGE, testMessageDeserializer.deserialize(topic, headers, bytes));

    // restore configs
    protobufSerializer.configure(new HashMap(serializerConfig), false);
    testMessageDeserializer.configure(new HashMap(deserializerConfig), false);
  }

  @Test
  public void testDependencyPreregisterRefWithNegativeOne() throws Exception {
    String refSubject = "TestProto.proto";
    String refSchemaString = "syntax = \"proto3\";\n"
        + "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message TestMessage {\n"
        + "  string test_string = 1;\n"
        + "}\n";
    schemaRegistry.register(refSubject, new ProtobufSchema(refSchemaString));
    String subject = topic + "-value";
    String schemaString = "syntax = \"proto3\";\n"
        + "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"TestProto.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message DependencyMessage {\n"
        + "  TestMessage test_message = 1;\n"
        + "  bool is_active = 2;\n"
        + "}";
    SchemaReference ref = new SchemaReference("TestProto.proto", "TestProto.proto", -1);
    ParsedSchema parsedSchema = new ProtobufSchema(
        schemaString, ImmutableList.of(ref), ImmutableMap.of(ref.getName(), refSchemaString), null, null);
    schemaRegistry.register(subject, parsedSchema);

    ParsedSchema schema = schemaRegistry.getSchemaBySubjectAndId("test-value", 2);
    SchemaReference refCopy = new SchemaReference("TestProto.proto", "TestProto.proto", -1);
    schema = schemaRegistry.parseSchema(ProtobufSchema.TYPE, schema.canonicalString(), ImmutableList.of(refCopy)).get();
    int id = schemaRegistry.getId(subject, schema);
    assertEquals(2, id);
  }

}
