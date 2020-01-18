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
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.test.DependencyTestProto.DependencyMessage;
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
  private final KafkaProtobufDeserializer testMessageDeserializer;
  private final KafkaProtobufDeserializer nestedMessageDeserializer;
  private final KafkaProtobufDeserializer dependencyMessageDeserializer;
  private final KafkaProtobufDeserializer innerMessageDeserializer;
  private final String topic;

  private static final String TEST_MSG_STRING = "Hello World";
  private static final TestMessage HELLO_WORLD_MESSAGE = TestMessage.newBuilder()
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
  private static final NestedMessage.InnerMessage INNER_MESSAGE =
      NestedMessage.InnerMessage.newBuilder().setId("inner").build();


  public KafkaProtobufSerializerTest() {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    schemaRegistry = new MockSchemaRegistryClient();
    protobufSerializer = new KafkaProtobufSerializer(schemaRegistry, new HashMap(serializerConfig));

    protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

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

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, HELLO_WORLD_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(HELLO_WORLD_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE.getTestInt32(), getField(message, "test_int32"));

    // dynamic -> specific
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(HELLO_WORLD_MESSAGE, testMessageDeserializer.deserialize(topic, bytes));

    // dynamic -> dynamic
    bytes = protobufSerializer.serialize(topic, message);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(HELLO_WORLD_MESSAGE.getTestString(), getField(message, "test_string"));
    assertEquals(HELLO_WORLD_MESSAGE.getTestInt32(), getField(message, "test_int32"));

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, NESTED_MESSAGE);
    assertEquals(NESTED_MESSAGE, nestedMessageDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, NESTED_MESSAGE);
    message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(NESTED_MESSAGE.getUserId().getKafkaUserId(),
        getField((DynamicMessage) getField(message, "user_id"), "kafka_user_id")
    );

    // dynamic -> specific
    bytes = protobufSerializer.serialize(topic, message);
    assertEquals(NESTED_MESSAGE, nestedMessageDeserializer.deserialize(topic, bytes));

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
}
