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

package io.confluent.kafka.schemaregistry.protobuf.rest;

import com.criteo.glup.ExampleProtoCriteo;
import com.criteo.glup.MetadataProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.protobuf.test.DependencyTestProto.DependencyMessage;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.ComplexType;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.NestedMessage;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.Status;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.UserId;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage;

import static io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerTest.getField;
import static org.junit.Assert.assertEquals;

public class RestApiSerializerTest extends ClusterTestHarness {

  private final String topic = "test";

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

  private static final MetadataProto.Origin ORIGIN_MESSAGE = MetadataProto.Origin.newBuilder()
      .setDatacenter(MetadataProto.DataCenter.AM5)
      .setIp4(1)
      .setHostname("myhost")
      .setContainerTask("mytask")
      .setContainerApp("myapp")
      .build();
  private static final MetadataProto.Partition PARTITION_MESSAGE =
      MetadataProto.Partition.newBuilder()
      .setTimestampSeconds(1L)
      .setHostPlatform(MetadataProto.Platform.US)
      .setEventType(MetadataProto.EventType.Basket)
      .build();
  private static final MetadataProto.ControlMessage.Watermark WATERMARK_MESSAGE =
      MetadataProto.ControlMessage.Watermark
      .newBuilder()
      .setType("mytype")
      .setHostname("myhost")
      .setKafkaTopic("mytopic")
      .setPartition(1)
      .setPartitionCount(1)
      .setProcessUuid(ByteString.EMPTY)
      .setRegion("myregion")
      .setTimestampSeconds(1)
      .setCluster("mycluster")
      .setEnvironment("myenv")
      .build();
  private static final ExampleProtoCriteo.ClickCas CLICK_CAS_MESSAGE =
      ExampleProtoCriteo.ClickCas.newBuilder()
      .setGlupOrigin(ORIGIN_MESSAGE)
      .setPartition(PARTITION_MESSAGE)
      .setUid("myuid")
      .addControlMessage(WATERMARK_MESSAGE)
      .build();


  public RestApiSerializerTest() {
    super(1, true);
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty("schema.providers", ProtobufSchemaProvider.class.getName());
    return props;
  }

  @Test
  public void testDependency() throws Exception {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig)
    );

    KafkaProtobufDeserializer protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    Properties dependencyMessageDeserializerConfig = new Properties();
    dependencyMessageDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    KafkaProtobufDeserializer dependencyMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(dependencyMessageDeserializerConfig),
        DependencyMessage.class
    );

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

    ParsedSchema schema = schemaRegistry.getSchemaBySubjectAndId("test-value", 6);
    assertEquals(ProtobufSchemaUtils.getSchema(DEPENDENCY_MESSAGE).canonicalString(),
        schema.canonicalString()
    );
  }

  @Test
  public void testDependency2() throws Exception {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig)
    );

    KafkaProtobufDeserializer protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    Properties clickCasDeserializerConfig = new Properties();
    clickCasDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    KafkaProtobufDeserializer clickCasDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(clickCasDeserializerConfig),
        ExampleProtoCriteo.ClickCas.class
    );

    byte[] bytes;

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, CLICK_CAS_MESSAGE);
    assertEquals(CLICK_CAS_MESSAGE, clickCasDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, CLICK_CAS_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(CLICK_CAS_MESSAGE.getGlupOrigin().getHostname(),
        getField((DynamicMessage) getField(message, "glup_origin"), "hostname")
    );

    ParsedSchema schema = schemaRegistry.getSchemaBySubjectAndId("test-value", 4);
    assertEquals(ProtobufSchemaUtils.getSchema(CLICK_CAS_MESSAGE).canonicalString(),
        schema.canonicalString()
    );
  }
}

