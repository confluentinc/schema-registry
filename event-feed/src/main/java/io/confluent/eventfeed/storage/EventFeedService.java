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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.kafka.CloudEventSerializer;
import io.confluent.eventfeed.storage.entities.CloudEventLoggingEntity;
import io.confluent.eventfeed.storage.entities.ValidatedCloudEvent;
import io.confluent.eventfeed.storage.exceptions.EventFeedException;
import io.confluent.eventfeed.storage.exceptions.EventFeedUnsupportedCloudEventException;
import io.confluent.eventfeed.storage.exceptions.EventFeedIllegalPropertyException;
import io.confluent.eventfeed.storage.exceptions.EventFeedInitializationException;
import io.confluent.eventfeed.storage.exceptions.EventFeedStorageException;
import io.confluent.eventfeed.storage.exceptions.EventFeedStorageTimeoutException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.WireEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.confluent.kafka.schemaregistry.storage.KafkaStore.addSchemaRegistryConfigsToClientProperties;

@Singleton
public class EventFeedService {
  private static final Logger log = LoggerFactory.getLogger(EventFeedService.class);

  private final SchemaRegistry schemaRegistry;
  private final EventFeedServerConfig config;
  private Producer<String, CloudEvent> producer;
  private final String topicEventsTopic;
  private final int initTimeout;

  private static final String TOPIC = "topic";
  private static final String TOPIC_AND_CLUSTERLINK = "topicAndClusterLink";

  private static final Map<String, List<String>> SUBJECT_TYPE_ALLOWLIST = Map.of(
          TOPIC, List.of("DELTA"),
          TOPIC_AND_CLUSTERLINK, List.of("SNAPSHOT")
  );
  private static final String PARTITION_KEY = "partitionkey";
  private static final String LSRC = "lsrc";
  private static final int DESIRED_NUM_PARTITIONS = 6;
  private static final int DESIRED_REPLICATION_FACTOR = 3;
  private static final long DESIRED_RETENTION_MS = TimeUnit.DAYS.toMillis(7);

  @Inject
  public EventFeedService(SchemaRegistry schemaRegistry) throws EventFeedInitializationException {
    this.schemaRegistry = schemaRegistry;
    try {
      this.config = new EventFeedServerConfig(schemaRegistry.config().originalProperties());
      this.topicEventsTopic = this.config.getString(
              EventFeedServerConfig.EVENT_FEED_TOPIC_EVENTS_TOPIC_CONFIG);
      this.initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
      this.producer = createProducer();
    } catch (Exception e) {
      throw new EventFeedInitializationException(e.getMessage(), e.getCause());
    }
  }

  @PostConstruct
  public void init() throws EventFeedInitializationException {
    log.info("Starting EventFeedService...");
    AdminClient admin = getAdminClient();
    createOrVerifyTopic(admin);
    log.info("EventFeedService started successfully");
  }

  @PreDestroy
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }

  protected AdminClient getAdminClient() {
    Properties props = new Properties();
    addSchemaRegistryConfigsToClientProperties(config, props);
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapBrokers());
    return AdminClient.create(props);
  }

  private void createOrVerifyTopic(AdminClient admin) throws EventFeedInitializationException {
    try {
      Set<String> allTopics = admin.listTopics().names().get(initTimeout, TimeUnit.MILLISECONDS);
      if (allTopics.contains(topicEventsTopic)) {
        verifyTopic(admin, topicEventsTopic, initTimeout);
      } else {
        createTopic(admin, topicEventsTopic, initTimeout);
      }
    } catch (java.util.concurrent.TimeoutException e) {
      throw new EventFeedInitializationException(
              "Timed out trying to create or validate topic " + topicEventsTopic, e
      );
    } catch (Exception e) {
      throw new EventFeedInitializationException(
              "Failed trying to create or validate topic " + topicEventsTopic, e
      );
    }
  }

  private void verifyTopic(AdminClient admin, String topic, int initTimeout)
          throws ExecutionException, InterruptedException,
          java.util.concurrent.TimeoutException, EventFeedInitializationException {
    log.info("Validating topic {}", topic);
    Set<String> topics = Collections.singleton(topic);
    Map<String, TopicDescription> topicDescription;
    try {
      topicDescription = admin.describeTopics(topics).allTopicNames().get(initTimeout, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        log.warn("Could not validate existing topic {}", topic);
        return;
      } else {
        throw e;
      }
    }
    TopicDescription description = topicDescription.get(topic);
    final int numPartitions = description.partitions().size();
    if (numPartitions < DESIRED_NUM_PARTITIONS) {
      log.warn("The number of partitions for the topic "
              + topic
              + " is less than the desired value of "
              + DESIRED_NUM_PARTITIONS
              + ".");
    }
    if (description.partitions().get(0).replicas().size() < DESIRED_REPLICATION_FACTOR) {
      log.warn("The replication factor of the topic "
              + topic
              + " is less than the desired one of "
              + DESIRED_REPLICATION_FACTOR
              + ". If this is a production environment, it's crucial to add more brokers and "
              + "increase the replication factor of the topic.");
    }
    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Config> configs =
            admin.describeConfigs(Collections.singleton(topicResource)).all()
                    .get(initTimeout, TimeUnit.MILLISECONDS);
    Config topicConfigs = configs.get(topicResource);
    String cleanupPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
    if (!TopicConfig.CLEANUP_POLICY_DELETE.equals(cleanupPolicy)) {
      String message = "The retention policy of the topic " + topic + " is not 'delete'. "
              + "You must configure the topic to 'delete' cleanup policy to avoid Kafka "
              + "replacing your old data with new data with the same key";
      log.error(message);
      throw new EventFeedInitializationException("The retention policy of the topic " + topic
              + " is incorrect. Expected cleanup.policy to be "
              + "'delete' but it is " + cleanupPolicy);
    }
    long retentionMs = Long.parseLong(
            topicConfigs.get(TopicConfig.RETENTION_MS_CONFIG).value());
    if (retentionMs < DESIRED_RETENTION_MS) {
      String message = String.format("The retention ms for the topic %s is %s, " +
              "which is less than the desired time %s.",
              topic, retentionMs, DESIRED_RETENTION_MS);
      log.warn(message);
    }
  }

  private void createTopic(AdminClient admin, String topic, int initTimeout)
          throws StoreInitializationException,
          InterruptedException,
          ExecutionException,
          java.util.concurrent.TimeoutException {
    log.info("Creating ingestor topic {}", topic);

    int numLiveBrokers = admin.describeCluster().nodes()
            .get(initTimeout, TimeUnit.MILLISECONDS).size();
    if (numLiveBrokers <= 0) {
      throw new StoreInitializationException("No live Kafka brokers");
    }

    int topicReplicationFactor = Math.min(numLiveBrokers, DESIRED_REPLICATION_FACTOR);
    if (topicReplicationFactor < DESIRED_REPLICATION_FACTOR) {
      log.warn("Creating the ingestor topic "
              + topic
              + " using a replication factor of "
              + topicReplicationFactor
              + ", which is less than the desired one of "
              + DESIRED_REPLICATION_FACTOR + ". If this is a production environment, it's "
              + "crucial to add more brokers and increase the replication factor of the topic.");
    }

    SchemaRegistryConfig config = schemaRegistry.config();
    NewTopic schemaTopicRequest =
            new NewTopic(topic, DESIRED_NUM_PARTITIONS, (short) topicReplicationFactor);
    Map topicConfigs = new HashMap(config.originalsWithPrefix("kafkastore.topic.config."));
    schemaTopicRequest.configs(topicConfigs);
    try {
      admin.createTopics(Collections.singleton(schemaTopicRequest)).all()
              .get(initTimeout, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof TopicExistsException)) {
        throw e;
      }
    }
  }

  protected Producer<String, CloudEvent> createProducer() {
    Properties props = new Properties();
    addSchemaRegistryConfigsToClientProperties(config, props);
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapBrokers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
    return new KafkaProducer<>(props);
  }

  private final Map<String, Validator> subjectValidators = Map.of(
          TOPIC, this::validateTopicCloudEvent,
          TOPIC_AND_CLUSTERLINK, this::validateTopicCloudEvent
  );

  @FunctionalInterface
  private interface Validator {
    ValidatedCloudEvent validate(CloudEvent cloudEvent) throws EventFeedUnsupportedCloudEventException;
  }

  private ValidatedCloudEvent validateTopicCloudEvent(CloudEvent cloudEvent)
          throws EventFeedIllegalPropertyException, EventFeedUnsupportedCloudEventException {
    if (cloudEvent.getSpecVersion() != SpecVersion.V1) {
      throw new EventFeedIllegalPropertyException("cloud event spec version", "must version 1.0");
    }
    Object lsrc = cloudEvent.getExtension(LSRC);
    if (lsrc == null || lsrc.toString().isEmpty()) {
      throw new EventFeedIllegalPropertyException("cloud event extension lsrc");
    }
    Object partitionKeyObj = cloudEvent.getExtension(PARTITION_KEY);
    if (partitionKeyObj == null || partitionKeyObj.toString().isEmpty()) {
      throw new EventFeedIllegalPropertyException("cloud event extension partitionkey");
    }
    OffsetDateTime time = cloudEvent.getTime();
    if (time == null) {
      throw new EventFeedIllegalPropertyException("cloud event time");
    }

    String subject = cloudEvent.getSubject();
    if (subject == null || subject.isEmpty() || !SUBJECT_TYPE_ALLOWLIST.containsKey(subject)) {
      throw new EventFeedUnsupportedCloudEventException(
              String.format("cloud event subject=%s is not supported by the server.", subject));
    }
    String type = cloudEvent.getType();
    if (type == null || type.isEmpty() || !SUBJECT_TYPE_ALLOWLIST.get(subject).contains(type)) {
      throw new EventFeedUnsupportedCloudEventException(
              String.format("cloud event subject=%s and type=%s combination " +
                      "is not supported by the server.", subject, type));
    }
    String partitionKey = partitionKeyObj.toString();
    return new ValidatedCloudEvent(topicEventsTopic, partitionKey, cloudEvent);
  }

  public CompletableFuture<CloudEvent> publishEventAsync(CloudEvent cloudEvent) {
    log.debug("start processing {}", CloudEventLoggingEntity.of(cloudEvent));
    if (cloudEvent == null) {
      return CompletableFuture.failedFuture(
              new EventFeedIllegalPropertyException("cloud event"));
    }
    String subject = cloudEvent.getSubject();
    if (subject == null || subject.isEmpty()) {
      return CompletableFuture.failedFuture(
              new EventFeedIllegalPropertyException("cloud event subject"));
    }
    // Based on the subject, find the right event validator
    Validator validator = subjectValidators.get(subject);
    ValidatedCloudEvent validatedEvent;
    try {
      validatedEvent = validator.validate(cloudEvent);
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }

    log.debug("about to publish {}", CloudEventLoggingEntity.of(cloudEvent));
    CompletableFuture<CloudEvent> future = new CompletableFuture<>();
    ProducerRecord<String, CloudEvent> record =
            new ProducerRecord<>(validatedEvent.getTopic(),
                    validatedEvent.getPartitionKey(),
                    validatedEvent.getCloudEvent());

    producer.send(record, (metadata, e) -> {
      log.debug("received response from broker, {}, {}.", metadata, e);
      if (e == null) {
        log.debug("Event published: topic={}, partition={}, offset={}, subject={}",
                metadata.topic(), metadata.partition(), metadata.offset(), subject);
        future.complete(cloudEvent);
      } else if (e instanceof TimeoutException) {
        future.completeExceptionally(
                new EventFeedStorageTimeoutException(
                        String.format("Timed out while publishing event to topic=%s", topicEventsTopic),
                        cloudEvent, e));
      } else {
        future.completeExceptionally(
                new EventFeedStorageException(
                        String.format("Failed to publish event to topic=%s", topicEventsTopic),
                        cloudEvent, e));
      }
    });
    return future;
  }
}
