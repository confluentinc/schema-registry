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

import com.google.common.annotations.VisibleForTesting;
import io.cloudevents.CloudEvent;
import io.confluent.kafka.schemaregistry.exceptions.GarbageCollectionException;
import io.confluent.kafka.schemaregistry.exceptions.GarbageCollectionInitializationException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.WireEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.serde.CloudEventSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.confluent.kafka.schemaregistry.storage.KafkaStore.addSchemaRegistryConfigsToClientProperties;

public class AssociationGarbageCollectionIngestor extends AssociationGarbageCollectionManager {
  private KafkaStreams streams;
  private List<String> topics;
  private SchemaRegistry schemaRegistry;
  private SchemaRegistryConfig config;
  private static final int STREAM_CLOSE_TIMEOUT_MILLIS = 60 * 1000;
  private static final Logger log =
      LoggerFactory.getLogger(AssociationGarbageCollectionIngestor.class);

  public AssociationGarbageCollectionIngestor(GarbageCollector garbageCollector,
      SchemaRegistry schemaRegistry) {
    super(garbageCollector, schemaRegistry);
    log.info("Initializing GarbageCollectionIngestor.");
    this.schemaRegistry = schemaRegistry;
    config = schemaRegistry.config();
    this.topics = config.getList(
        SchemaRegistryConfig.ASSOC_GC_INGESTOR_TOPICS_CONFIG);
    addLeaderChangeListener();
  }

  public AssociationGarbageCollectionIngestor(SchemaRegistry schemaRegistry) {
    this(new AssociationKafkaGarbageCollector(schemaRegistry), schemaRegistry);
  }

  private void addLeaderChangeListener() {
    schemaRegistry.addLeaderChangeListener(isLeader -> {
      synchronized (this) {
        try {
          if (isLeader) {
            init();
          } else {
            close();
          }
        } catch (Exception e) {
          throw new RuntimeException(
              "Failure occurred during GarbageCollectionIngestor leader change process.", e);
        }
      }
    });
  }

  private boolean isKafkaStreamsActive(KafkaStreams streams) {
    return streams != null
        && (streams.state() == KafkaStreams.State.CREATED
        || streams.state() == KafkaStreams.State.REBALANCING
        || streams.state() == KafkaStreams.State.RUNNING);
  }

  @Override
  public void init() throws GarbageCollectionException {
    // only runs on leader pod.
    if (schemaRegistry.isLeader()) {
      super.init();
      if (topics == null || topics.isEmpty()) {
        throw new GarbageCollectionInitializationException(
            "No topics specified for GarbageCollectionIngestor.");
      }
      try {
        // Check topics existence and create topics if needed
        createTopics();
        // Create and start KStreams app
        if (!isKafkaStreamsActive(streams)) {
          Properties streamsConfig = createStreamsConfig();
          streams = createStream(streamsConfig, topics);
          streams.start();
        }
      } catch (Exception e) {
        throw new GarbageCollectionInitializationException(
            "Failed to initialize GarbageCollectionIngestor", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (streams != null) {
      streams.close(Duration.ofMillis(STREAM_CLOSE_TIMEOUT_MILLIS));
      streams = null;
    }
  }

  private void createTopics() throws StoreInitializationException {
    Properties props = new Properties();
    addSchemaRegistryConfigsToClientProperties(config, props);
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapBrokers());

    int initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
    try (AdminClient admin = AdminClient.create(props)) {
      Set<String> allTopics = admin.listTopics().names().get(initTimeout, TimeUnit.MILLISECONDS);
      for (String topic : topics) {
        if (!allTopics.contains(topic)) {
          createTopic(admin, topic, initTimeout);
        }
      }
    } catch (TimeoutException e) {
      throw new StoreInitializationException(
              "Timed out trying to create or validate ingestor topic configuration",
              e
      );
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new StoreInitializationException(
              "Failed trying to create or validate ingestor topic configuration",
              e
      );
    }
  }

  private void createTopic(AdminClient admin, String topic, int initTimeout)
          throws StoreInitializationException,
          InterruptedException,
          ExecutionException,
          TimeoutException {
    log.info("Creating association gc ingestor topic {}", topic);

    int numLiveBrokers = admin.describeCluster().nodes()
            .get(initTimeout, TimeUnit.MILLISECONDS).size();
    if (numLiveBrokers <= 0) {
      throw new StoreInitializationException("No live Kafka brokers");
    }

    int desiredReplicationFactor = 3;
    int topicReplicationFactor = Math.min(numLiveBrokers, desiredReplicationFactor);
    if (topicReplicationFactor < desiredReplicationFactor) {
      log.warn("Creating the ingestor topic "
              + topic
              + " using a replication factor of "
              + topicReplicationFactor
              + ", which is less than the desired one of "
              + desiredReplicationFactor + ". If this is a production environment, it's "
              + "crucial to add more brokers and increase the replication factor of the topic.");
    }

    int desiredPartitionCount = 6;
    NewTopic schemaTopicRequest =
            new NewTopic(topic, desiredPartitionCount, (short) topicReplicationFactor);
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

  private Properties createStreamsConfig() {
    String groupId = config.getString(
            SchemaRegistryConfig.ASSOC_GC_INGESTOR_CONSUMER_GROUP_OVERRIDE_CONFIG);
    if (groupId == null) {
      throw new IllegalArgumentException("Missing config"
          + SchemaRegistryConfig.ASSOC_GC_INGESTOR_CONSUMER_GROUP_OVERRIDE_CONFIG
          + "for GarbageCollectionIngestor.");
    }
    String applicationId = groupId + "-ingest";

    Properties streamsConfig = new Properties();
    addSchemaRegistryConfigsToClientProperties(config, streamsConfig);
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapBrokers());
    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class.getName());
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            CloudEventSerde.class.getName());
    streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);  // 6 partitions, 3 SR nodes
    streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    streamsConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.class);
    streamsConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
            config.getInt(SchemaRegistryConfig.ASSOC_GC_INGESTOR_MAX_POLL_INTERVAL_MS_CONFIG));

    return streamsConfig;
  }

  private KafkaStreams createStream(Properties streamsConfig, List<String> topics) {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, CloudEvent> input = builder.stream(topics);
    input.process(ProcessInput::new);

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
    streams.setUncaughtExceptionHandler(exception ->
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
    return streams;
  }

  @VisibleForTesting
  protected KafkaStreams getStreams() {
    return streams;
  }

  private final class ProcessInput implements Processor<String, CloudEvent, Void, Void> {

    public ProcessInput() {
    }

    @Override
    public void init(final ProcessorContext context) {
    }

    @Override
    public void process(Record<String, CloudEvent> record) {
      log.info(String.format("Ingestor processing input record at ts: %s", record.timestamp()));
      Map<String, String> headersMap = new HashMap<>();
      for (Header header : record.headers()) {
        headersMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
      }
      WireEvent wireEvent = new WireEvent(headersMap, record.value());
      try {
        processEvent(wireEvent);
      } catch (Exception e) {
        log.error("Failed to process record", e);
      }
    }

    @Override
    public void close() {
    }
  }
}
