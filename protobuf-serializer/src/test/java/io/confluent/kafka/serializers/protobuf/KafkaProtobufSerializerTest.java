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
 */

package io.confluent.kafka.serializers.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.serializers.protobuf.test.TestMessageOptionalProtos.TestMessageOptional;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage2;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.test.TestMessageOptionalProtos;
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


  public KafkaProtobufSerializerTest() {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializerConfig.put(KafkaProtobufSerializerConfig.NORMALIZE_SCHEMAS, true);
    schemaRegistry = new MockSchemaRegistryClient();
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

    Properties testMessageDeserializerConfig = new Properties();
    testMessageDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    testMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(testMessageDeserializerConfig),
        TestMessage.class
    );

    Properties nestedMessageDeserializerConfig = new Properties();
    nestedMessageDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    nestedMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(nestedMessageDeserializerConfig),
        NestedMessage.class
    );

    Properties dependencyMessageDeserializerConfig = new Properties();
    dependencyMessageDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    dependencyMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(dependencyMessageDeserializerConfig),
        DependencyMessage.class
    );

    Properties enumRefDeserializerConfig = new Properties();
    enumRefDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    enumRefDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(enumRefDeserializerConfig),
        EnumReference.class
    );

    Properties innerMessageDeserializerConfig = new Properties();
    innerMessageDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    innerMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(innerMessageDeserializerConfig),
        NestedMessage.InnerMessage.class
    );

    Properties optionalMessageDeserializerConfig = new Properties();
    optionalMessageDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    optionalMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(optionalMessageDeserializerConfig),
        TestMessageOptionalProtos.TestMessageOptional.class
    );

    topic = "test";
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
    bytes = protobufSerializer.serialize(topic, HELLO_WORLD_MESSAGE);
    assertEquals(HELLO_WORLD_MESSAGE, testMessageDeserializer.deserialize(topic, bytes));

    // specific -> derived
    bytes = protobufSerializer.serialize(topic, HELLO_WORLD_MESSAGE);
    assertEquals(HELLO_WORLD_MESSAGE, deriveTypeDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, HELLO_WORLD_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(HELLO_WORLD_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE.getTestInt32(), getField(message, "test_int32"));

    // dynamic -> specific
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(HELLO_WORLD_MESSAGE, testMessageDeserializer.deserialize(topic, bytes));

    // dynamic -> derived
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(HELLO_WORLD_MESSAGE, deriveTypeDeserializer.deserialize(topic, bytes));

    // dynamic -> dynamic
    bytes = protobufSerializer.serialize(topic, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(HELLO_WORLD_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE.getTestInt32(), getField(message, "test_int32"));


    // specific -> derived
    bytes = protobufSerializer.serialize(topic, HELLO_WORLD_MESSAGE2);
    assertEquals(HELLO_WORLD_MESSAGE2, deriveTypeDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, HELLO_WORLD_MESSAGE2);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(HELLO_WORLD_MESSAGE2.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE2.getTestInt32(), getField(message, "test_int32"));

    // dynamic -> derived
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(HELLO_WORLD_MESSAGE2, deriveTypeDeserializer.deserialize(topic, bytes));

    // dynamic -> dynamic
    bytes = protobufSerializer.serialize(topic, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(HELLO_WORLD_MESSAGE2.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE2.getTestInt32(), getField(message, "test_int32"));


    // specific -> specific
    bytes = protobufSerializer.serialize(topic, NESTED_MESSAGE);
    assertEquals(NESTED_MESSAGE, nestedMessageDeserializer.deserialize(topic, bytes));

    // dynamic -> derived
    bytes = protobufSerializer.serialize(topic, NESTED_MESSAGE);
    assertEquals(NESTED_MESSAGE, deriveTypeDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, NESTED_MESSAGE);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(NESTED_MESSAGE.getUserId().getKafkaUserId(),
        getField((DynamicMessage) getField(message, "user_id"), "kafka_user_id")
    );

    // dynamic -> specific
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(NESTED_MESSAGE, nestedMessageDeserializer.deserialize(topic, bytes));

    // dynamic -> derived
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(NESTED_MESSAGE, deriveTypeDeserializer.deserialize(topic, bytes));

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(NESTED_MESSAGE.getUserId().getKafkaUserId(),
        getField((DynamicMessage) getField(message, "user_id"), "kafka_user_id")
    );

    // null -> null
    bytes = protobufSerializer.serialize(topic, null);
    assertEquals(null, protobufDeserializer.deserialize(topic, bytes));
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
    bytes = protobufSerializer.serialize(topic, DEPENDENCY_MESSAGE);
    assertEquals(DEPENDENCY_MESSAGE, dependencyMessageDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, DEPENDENCY_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
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
    bytes = protobufSerializer.serialize(topic, ENUM_REF);
    assertEquals(ENUM_REF, enumRefDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, ENUM_REF);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(ENUM_REF.getEnumRoot().name(), ((EnumValueDescriptor) getField(message, "enum_root")).getName());
  }

  @Test
  public void testInner() {
    byte[] bytes;

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, INNER_MESSAGE);
    assertEquals(INNER_MESSAGE, innerMessageDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, INNER_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
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
    bytes = protobufSerializer.serialize(topic, OPTIONAL_MESSAGE);
    assertEquals(OPTIONAL_MESSAGE, optionalMessageDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, OPTIONAL_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(OPTIONAL_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(false, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));

    // dynamic -> specific
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(OPTIONAL_MESSAGE, optionalMessageDeserializer.deserialize(topic, bytes));

    // dynamic -> dynamic
    bytes = protobufSerializer.serialize(topic, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(OPTIONAL_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(false, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));


    // specific -> specific
    bytes = protobufSerializer.serialize(topic, OPTIONAL_MESSAGE_DEFAULT);
    assertEquals(OPTIONAL_MESSAGE_DEFAULT, optionalMessageDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, OPTIONAL_MESSAGE_DEFAULT);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(OPTIONAL_MESSAGE_DEFAULT.getTestString(), getField(message, "test_string"));
    assertEquals(true, message.hasField(message.getDescriptorForType().findFieldByName("test_optional_string")));

    // dynamic -> specific
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(OPTIONAL_MESSAGE_DEFAULT, optionalMessageDeserializer.deserialize(topic, bytes));

    // dynamic -> dynamic
    bytes = protobufSerializer.serialize(topic, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
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
}
