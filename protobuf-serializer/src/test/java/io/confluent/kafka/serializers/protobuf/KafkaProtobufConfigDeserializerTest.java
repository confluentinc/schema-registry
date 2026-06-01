/*
 * Copyright 2025 Confluent Inc.
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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage;
import io.confluent.kafka.serializers.schema.id.ConfigSchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class KafkaProtobufConfigDeserializerTest {

  private final Properties defaultConfig;
  private final SchemaRegistryClient schemaRegistry;
  private final KafkaProtobufSerializer protobufSerializer;
  private final KafkaProtobufDeserializer protobufDeserializer;
  private final String topic;

  private static final String TEST_MSG_STRING = "Hello World";
  private static final TestMessage HELLO_WORLD_MESSAGE = TestMessage.newBuilder()
      .setTestString(TEST_MSG_STRING)
      .setTestInt32(123)
      .build();

  public KafkaProtobufConfigDeserializerTest() {
    defaultConfig = createSerializerConfig();
    schemaRegistry = new MockSchemaRegistryClient();
    protobufSerializer = new KafkaProtobufSerializer(schemaRegistry, new HashMap(defaultConfig));
    Properties deserializerConfig = createDeserializerConfig();
    protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry, new HashMap(deserializerConfig));
    topic = "test";
  }

  protected Properties createSerializerConfig() {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
    serializerConfig.put(KafkaProtobufSerializerConfig.USE_SCHEMA_ID, 1);
    serializerConfig.put(KafkaProtobufSerializerConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    return serializerConfig;
  }

  protected Properties createDeserializerConfig() {
    Properties deserializerConfig = new Properties();
    deserializerConfig.setProperty(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    deserializerConfig.put(KafkaProtobufSerializerConfig.USE_SCHEMA_ID, 1);
    deserializerConfig.put(ConfigSchemaIdDeserializer.USE_MESSAGE_INDEXES, 0);
    deserializerConfig.put(KafkaProtobufDeserializerConfig.VALUE_SCHEMA_ID_DESERIALIZER,
        ConfigSchemaIdDeserializer.class.getName());
    return deserializerConfig;
  }

  @Test
  public void testKafkaProtobufSerializerWithPreRegisteredUseSchemaId()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaProtobufSerializerConfig.USE_SCHEMA_ID,
        1,
        KafkaProtobufSerializerConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName()
    );
    protobufSerializer.configure(configs, false);
    ProtobufSchema schema = new ProtobufSchema(TestMessage.getDescriptor());
    schemaRegistry.register("io.confluent.kafka.serializers.protobuf.test.TestMessage", schema);
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = protobufSerializer.serialize(topic, headers, HELLO_WORLD_MESSAGE);
    assertEquals(HELLO_WORLD_MESSAGE, protobufDeserializer.deserialize(topic, headers, bytes));
  }
}
