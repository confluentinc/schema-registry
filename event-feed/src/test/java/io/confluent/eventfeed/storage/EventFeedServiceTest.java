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

package io.confluent.eventfeed.storage;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.confluent.eventfeed.storage.exceptions.EventFeedIllegalPropertyException;
import io.confluent.eventfeed.storage.exceptions.EventFeedInitializationException;
import io.confluent.eventfeed.storage.exceptions.EventFeedStorageException;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventFeedServiceTest extends ClusterTestHarness {
  private SchemaRegistry schemaRegistry;
  private EventFeedService eventFeedService;
  private EventFeedServerConfig  eventFeedServerConfig;
  private AdminClient adminClient;

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    props.put(EventFeedServerConfig.EVENT_FEED_TOPIC_EVENTS_TOPIC_CONFIG, "topic-events");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    eventFeedServerConfig = new EventFeedServerConfig(config.originalProperties());
    schemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    eventFeedService = new EventFeedService(schemaRegistry);

    Properties adminClientProps = new Properties();
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapBrokers());
    adminClient = AdminClient.create(adminClientProps);
  }

  @Test
  public void testCreateTopic_topicNotExists() throws Exception {
    // Make sure topic doesn't exist
    Set<String> topics = adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
    assertTrue(topics.isEmpty());
    eventFeedService.init();
    topics = adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
    assertEquals(1, topics.size());
  }

  @Test
  public void testVerifyTopic_cleanUpPolicy_compact() throws Exception {
    // Create topic with compact cleanup policy, which is not what this service expects
    NewTopic topic = new NewTopic(
            eventFeedServerConfig.getString(EventFeedServerConfig.EVENT_FEED_TOPIC_EVENTS_TOPIC_CONFIG),
            3, (short) 1);
    topic.configs(Map.of("cleanup.policy", "compact"));
    adminClient.createTopics(Collections.singleton(topic)).all().get();
    try {
      eventFeedService.init();
      fail("topic's clean up policy is compact, but should be delete");
    } catch (Exception e) {
      assertEquals(EventFeedInitializationException.class, e.getClass());
    }
  }

  @Test
  public void testVerifyTopic_cleanUpPolicy_delete() throws Exception {
    NewTopic topic = new NewTopic(
            eventFeedServerConfig.getString(EventFeedServerConfig.EVENT_FEED_TOPIC_EVENTS_TOPIC_CONFIG),
            3, (short) 1);
    topic.configs(Map.of("cleanup.policy", "delete"));
    adminClient.createTopics(Collections.singleton(topic)).all().get();
    eventFeedService.init();
  }

  private CloudEventBuilder getBareMinimumCloudEventByType(String type) {
    CloudEventBuilder cloudEventBuilder = new CloudEventBuilder();
    cloudEventBuilder.withDataContentType("application/protobuf");
    cloudEventBuilder.withExtension("partitionkey", "lkc-1");
    cloudEventBuilder.withExtension("lsrc", "default");
    cloudEventBuilder.withTime(OffsetDateTime.now());
    if (type.equals("DELTA")) {
      cloudEventBuilder.withId("1");
      cloudEventBuilder.withSubject("topic");
      cloudEventBuilder.withSource(URI.create("crn://confluent.cloud/kafka=lkc-1/topic=topic-1"));
      cloudEventBuilder.withType("DELTA");
    } else if (type.equals("SNAPSHOT")) {
      cloudEventBuilder.withId("2");
      cloudEventBuilder.withSubject("topicAndClusterLink");
      cloudEventBuilder.withSource(URI.create("crn://confluent.cloud/kafka=lkc-1/topics-and-cluster-links"));
      cloudEventBuilder.withType("SNAPSHOT");
    } else {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    return cloudEventBuilder;
  }

  private CloudEventBuilder getCloudEventWoExtensionByType(String type) {
    CloudEventBuilder cloudEventBuilder = new CloudEventBuilder();
    if (type.equals("DELTA")) {
      cloudEventBuilder.withDataContentType("application/protobuf");
      cloudEventBuilder.withId("1");
      cloudEventBuilder.withSubject("topic");
      cloudEventBuilder.withSource(URI.create("crn://confluent.cloud/kafka=lkc-1/topic=topic-1"));
      cloudEventBuilder.withType("DELTA");
    } else if (type.equals("SNAPSHOT")) {
      cloudEventBuilder.withDataContentType("application/protobuf");
      cloudEventBuilder.withId("2");
      cloudEventBuilder.withSubject("topicAndClusterLink");
      cloudEventBuilder.withSource(URI.create("crn://confluent.cloud/kafka=lkc-1/topics-and-cluster-links"));
      cloudEventBuilder.withType("SNAPSHOT");
    } else {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    return cloudEventBuilder;
  }

  @Test
  public void testPublishEventAsync_dataValidation() throws Exception {
    // cloudEvent datacontenttype, id, source and type being not null is validated by CloudEventsProvider
    // Below is event feed service custom input validation
    List<String> types =  Arrays.asList("DELTA", "SNAPSHOT");
    for (String type : types) {
      // cloudEvent subject is null
      try {
        CloudEventBuilder cloudEventBuilder = getBareMinimumCloudEventByType(type);
        cloudEventBuilder.withSubject(null);
        CloudEvent event = cloudEventBuilder.build();
        eventFeedService.publishEventAsync(event);
      } catch (Exception e) {
        assertEquals(EventFeedIllegalPropertyException.class, e.getClass());
      }

      // cloudEvent subject is unsupported
      try {
        CloudEventBuilder cloudEventBuilder = getBareMinimumCloudEventByType(type);
        cloudEventBuilder.withSubject("abc");
        CloudEvent event = cloudEventBuilder.build();
        eventFeedService.publishEventAsync(event);
      } catch (Exception e) {
        assertEquals(EventFeedIllegalPropertyException.class, e.getClass());
      }

      // cloudEvent extension partitionkey is null
      try {
        CloudEventBuilder cloudEventBuilder = getCloudEventWoExtensionByType(type);
        CloudEvent event = cloudEventBuilder.build();
        eventFeedService.publishEventAsync(event);
      } catch (Exception e) {
        assertEquals(EventFeedIllegalPropertyException.class, e.getClass());
      }

      // cloudEvent type is unsupported
      try {
        CloudEventBuilder cloudEventBuilder = getBareMinimumCloudEventByType(type);
        cloudEventBuilder.withType("abc");
        CloudEvent event = cloudEventBuilder.build();
        eventFeedService.publishEventAsync(event);
      } catch (Exception e) {
        assertEquals(EventFeedIllegalPropertyException.class, e.getClass());
      }

      // bare minimum cloudEvent
      try {
        CloudEventBuilder cloudEventBuilder = getBareMinimumCloudEventByType(type);
        cloudEventBuilder.withType("abc");
        CloudEvent event = cloudEventBuilder.build();
        eventFeedService.publishEventAsync(event);
      } catch (Exception e) {
        fail("cloudEvent should be valid.");
      }
    }
  }

  @Test
  public void testPublishEventAsync_healthyBroker() throws Exception {
    for (String type : Arrays.asList("DELTA", "SNAPSHOT")) {
      CloudEvent cloudEvent = getBareMinimumCloudEventByType(type).build();
      CompletableFuture<CloudEvent> result = eventFeedService.publishEventAsync(cloudEvent);
      assertEquals(result.get(), cloudEvent);
    }
  }

  @Test
  public void testPublishEventAsync_unhealthyBroker() throws Exception {
    Producer<String, CloudEvent> producerMock = mock(Producer.class);
    when(producerMock.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(
            invocation -> {
      Callback callback = invocation.getArgument(1);
      callback.onCompletion(null, new RuntimeException());
      return null;
    });
    eventFeedService = new EventFeedService(schemaRegistry) {
      @Override
      protected Producer<String, CloudEvent> createProducer() {
        return producerMock;
      }
    };
    CloudEvent cloudEvent = getBareMinimumCloudEventByType("DELTA").build();
    CompletableFuture<CloudEvent> result = eventFeedService.publishEventAsync(cloudEvent);
    try {
      result.get();
      fail("Should have thrown an exception");
    } catch (Exception e) {
      assertEquals(EventFeedStorageException.class, e.getCause().getClass());
    }
  }
}
