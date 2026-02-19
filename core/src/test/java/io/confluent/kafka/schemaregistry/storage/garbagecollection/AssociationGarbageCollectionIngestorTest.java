/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;
import io.confluent.protobuf.events.catalog.v1.MetadataEvent;
import io.confluent.protobuf.events.catalog.v1.OpType;
import io.confluent.protobuf.events.catalog.v1.TopicMetadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class AssociationGarbageCollectionIngestorTest extends ClusterTestHarness {

  private static final String GC_INGESTOR_TOPIC = "assoc-gc-ingestor-test";
  private static final String GC_CONSUMER_GROUP = "assoc-gc-ingestor-test-group";
  private static final String TENANT = "default";
  private static final String TOPIC_ID = "topic-1";
  private static final String PROTOBUF_CONTENT_TYPE = "application/protobuf";

  @Mock
  private GarbageCollector garbageCollector;
  private SchemaRegistry schemaRegistrySpy;

  private AssociationGarbageCollectionIngestor ingestor;
  private AutoCloseable mocks;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUpTest(testInfo);
    mocks = MockitoAnnotations.openMocks(this);

    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    props.put(SchemaRegistryConfig.ASSOC_GC_ENABLE_CONFIG, true);
    props.put(SchemaRegistryConfig.ASSOC_GC_INGESTOR_TOPICS_CONFIG, GC_INGESTOR_TOPIC);
    props.put(SchemaRegistryConfig.ASSOC_GC_INGESTOR_CONSUMER_GROUP_OVERRIDE_CONFIG, GC_CONSUMER_GROUP);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_CORE_POOL_SIZE_CONFIG, 1);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_MAX_POOL_SIZE_CONFIG, 1);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_QUEUE_SIZE_CONFIG, 2);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    schemaRegistrySpy = spy(new KafkaSchemaRegistry(config, new SchemaRegistrySerializer()));
    // Do not init schema registry
    ingestor = new AssociationGarbageCollectionIngestor(garbageCollector, schemaRegistrySpy);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (ingestor != null) {
      try {
        ingestor.close();
      } catch (Exception ex) {
      }
    }
    if (schemaRegistrySpy != null) {
      schemaRegistrySpy.close();
    }
    if (mocks != null) {
      mocks.close();
    }
    super.tearDown();
  }

  /**
   * Spy schema registry (no init). When isLeader() is called, return true.
   * Verify: topic is created, stream is active, send one valid message to Kafka, GC is called.
   */
  @Test
  public void testInitOnLeader_isLeaderTrue() throws Exception {
    doReturn(true).when(schemaRegistrySpy).isLeader();
    ingestor.init();

    // Verify topic is created
    try (AdminClient admin = AdminClient.create(
        Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList))) {
      Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
      assertTrue(names.contains(GC_INGESTOR_TOPIC), "Ingestor topic should exist: " + names);
    }

    // Verify stream is active
    KafkaStreams streams = ingestor.getStreams();
    assertNotNull(streams, "Streams should be created");
    assertTrue(streams.state() == KafkaStreams.State.RUNNING
            || streams.state() == KafkaStreams.State.REBALANCING);

    // Send one valid topic-deletion message to the backing Kafka topic
    CloudEventBuilder cloudEventBuilder = new CloudEventBuilder();
    cloudEventBuilder.withDataContentType("application/json");
    cloudEventBuilder.withExtension("partitionkey", "lkc-1");
    cloudEventBuilder.withExtension("lsrc", TENANT);
    cloudEventBuilder.withTime(OffsetDateTime.now());
    cloudEventBuilder.withId("1");
    cloudEventBuilder.withSubject("topic");
    cloudEventBuilder.withSource(URI.create("crn://confluent.cloud/kafka=lkc-1/topic=topic-1"));
    cloudEventBuilder.withType("DELTA");
    String json = "{\n"
            + "  \"source\" : \"" + "lkc-1" + "\",\n"
            + "  \"op\" : \"DELETE\",\n"
            + "  \"events\" : [ {\n"
            + "    \"topicMetadata\" : {\n"
            + "      \"topicId\" : \"" + TOPIC_ID + "\",\n"
            + "      \"topicName\" : \"" + TOPIC_ID + "\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";
    byte[] data = json.getBytes(StandardCharsets.UTF_8);
    cloudEventBuilder.withData(data);
    CloudEvent cloudEvent = cloudEventBuilder.build();

    try (KafkaProducer<String, CloudEvent> producer = createCloudEventProducer()) {
      producer.send(new ProducerRecord<>(GC_INGESTOR_TOPIC, "lkc-1", cloudEvent)).get(10, TimeUnit.SECONDS);
    }

    // Wait for the stream to consume and the executor to run the handler
    Thread.sleep(1000);

    verify(garbageCollector).processDeletedResource(TENANT, TOPIC_ID);
  }

  @Test
  public void testInitOnLeader_isLeaderFalse() throws Exception {
    doReturn(false).when(schemaRegistrySpy).isLeader();
    ingestor.init();

    // Verify topic is not created
    try (AdminClient admin = AdminClient.create(
            Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList))) {
      Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
      assertFalse(names.contains(GC_INGESTOR_TOPIC), "Ingestor topic should not exist: " + names);
    }

    // Verify stream is not active
    KafkaStreams streams = ingestor.getStreams();
    assertTrue(streams == null || streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams should not be running");

    // Send one valid topic-deletion message to the backing Kafka topic
    CloudEventBuilder cloudEventBuilder = new CloudEventBuilder();
    cloudEventBuilder.withDataContentType("application/json");
    cloudEventBuilder.withExtension("partitionkey", "lkc-1");
    cloudEventBuilder.withExtension("lsrc", TENANT);
    cloudEventBuilder.withTime(OffsetDateTime.now());
    cloudEventBuilder.withId("1");
    cloudEventBuilder.withSubject("topic");
    cloudEventBuilder.withSource(URI.create("crn://confluent.cloud/kafka=lkc-1/topic=topic-1"));
    cloudEventBuilder.withType("DELTA");
    String json = "{\n"
            + "  \"source\" : \"" + "lkc-1" + "\",\n"
            + "  \"op\" : \"DELETE\",\n"
            + "  \"events\" : [ {\n"
            + "    \"topicMetadata\" : {\n"
            + "      \"topicId\" : \"" + TOPIC_ID + "\",\n"
            + "      \"topicName\" : \"" + TOPIC_ID + "\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";
    byte[] data = json.getBytes(StandardCharsets.UTF_8);
    cloudEventBuilder.withData(data);
    CloudEvent cloudEvent = cloudEventBuilder.build();

    try (KafkaProducer<String, CloudEvent> producer = createCloudEventProducer()) {
      producer.send(new ProducerRecord<>(GC_INGESTOR_TOPIC, "lkc-1", cloudEvent)).get(10, TimeUnit.SECONDS);
    }

    // Stream is not running. The message should not be consumed.
    Thread.sleep(1000);

    verify(garbageCollector, never()).processDeletedResource(TENANT, TOPIC_ID);
  }

  @Test
  public void testLeaderChange_CloseOnFollower() throws Exception {
    doReturn(true)
            .doReturn(false)
            .when(schemaRegistrySpy).isLeader();
    ingestor.init();
    ingestor.close();

    // Verify topic is created
    try (AdminClient admin = AdminClient.create(
            Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList))) {
      Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
      assertTrue(names.contains(GC_INGESTOR_TOPIC), "Ingestor topic should not exist: " + names);
    }

    // Verify stream is not active
    KafkaStreams streams = ingestor.getStreams();
    assertTrue(streams == null || streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams should not be running");

    // Send one valid topic-deletion message to the backing Kafka topic
    CloudEventBuilder cloudEventBuilder = new CloudEventBuilder();
    cloudEventBuilder.withDataContentType("application/json");
    cloudEventBuilder.withExtension("partitionkey", "lkc-1");
    cloudEventBuilder.withExtension("lsrc", TENANT);
    cloudEventBuilder.withTime(OffsetDateTime.now());
    cloudEventBuilder.withId("1");
    cloudEventBuilder.withSubject("topic");
    cloudEventBuilder.withSource(URI.create("crn://confluent.cloud/kafka=lkc-1/topic=topic-1"));
    cloudEventBuilder.withType("DELTA");
    String json = "{\n"
            + "  \"source\" : \"" + "lkc-1" + "\",\n"
            + "  \"op\" : \"DELETE\",\n"
            + "  \"events\" : [ {\n"
            + "    \"topicMetadata\" : {\n"
            + "      \"topicId\" : \"" + TOPIC_ID + "\",\n"
            + "      \"topicName\" : \"" + TOPIC_ID + "\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";
    byte[] data = json.getBytes(StandardCharsets.UTF_8);
    cloudEventBuilder.withData(data);
    CloudEvent cloudEvent = cloudEventBuilder.build();

    try (KafkaProducer<String, CloudEvent> producer = createCloudEventProducer()) {
      producer.send(new ProducerRecord<>(GC_INGESTOR_TOPIC, "lkc-1", cloudEvent)).get(10, TimeUnit.SECONDS);
    }

    // Stream is not running. The message should not be consumed.
    Thread.sleep(1000);

    verify(garbageCollector, never()).processDeletedResource(TENANT, TOPIC_ID);
  }

  @Test
  public void testLeaderChange_TwoInitCallsShouldReturnSameKStreams() throws Exception {
    doReturn(true).when(schemaRegistrySpy).isLeader();
    ingestor.init();
    KafkaStreams streamsBefore = ingestor.getStreams();
    ingestor.init();
    KafkaStreams streamsAfter = ingestor.getStreams();
    assertSame(streamsBefore, streamsAfter, "Streams should be the same object");

    // Verify topic is created
    try (AdminClient admin = AdminClient.create(
            Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList))) {
      Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
      assertTrue(names.contains(GC_INGESTOR_TOPIC), "Ingestor topic should not exist: " + names);
    }

    // Verify stream is active
    KafkaStreams streams = ingestor.getStreams();
    assertTrue(streams.state() == KafkaStreams.State.RUNNING
            || streams.state() == KafkaStreams.State.REBALANCING);

    // Send one valid topic-deletion message to the backing Kafka topic
    CloudEventBuilder cloudEventBuilder = new CloudEventBuilder();
    cloudEventBuilder.withDataContentType("application/json");
    cloudEventBuilder.withExtension("partitionkey", "lkc-1");
    cloudEventBuilder.withExtension("lsrc", TENANT);
    cloudEventBuilder.withTime(OffsetDateTime.now());
    cloudEventBuilder.withId("1");
    cloudEventBuilder.withSubject("topic");
    cloudEventBuilder.withSource(URI.create("crn://confluent.cloud/kafka=lkc-1/topic=topic-1"));
    cloudEventBuilder.withType("DELTA");
    String json = "{\n"
            + "  \"source\" : \"" + "lkc-1" + "\",\n"
            + "  \"op\" : \"DELETE\",\n"
            + "  \"events\" : [ {\n"
            + "    \"topicMetadata\" : {\n"
            + "      \"topicId\" : \"" + TOPIC_ID + "\",\n"
            + "      \"topicName\" : \"" + TOPIC_ID + "\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";
    byte[] data = json.getBytes(StandardCharsets.UTF_8);
    cloudEventBuilder.withData(data);
    CloudEvent cloudEvent = cloudEventBuilder.build();

    try (KafkaProducer<String, CloudEvent> producer = createCloudEventProducer()) {
      producer.send(new ProducerRecord<>(GC_INGESTOR_TOPIC, "lkc-1", cloudEvent)).get(10, TimeUnit.SECONDS);
    }

    // Stream is running. The message should be consumed.
    Thread.sleep(1000);

    verify(garbageCollector).processDeletedResource(TENANT, TOPIC_ID);
  }

  private KafkaProducer<String, CloudEvent> createCloudEventProducer() {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        io.cloudevents.kafka.CloudEventSerializer.class.getName());
    return new KafkaProducer<>(producerProps);
  }
}
