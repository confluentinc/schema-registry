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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.confluent.kafka.schemaregistry.exceptions.GarbageCollectionException;
import io.confluent.kafka.schemaregistry.exceptions.InconsistentSnapshotPageException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.GarbageCollectionEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.WireEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.serde.MetadataChangeDeserializer;
import io.confluent.protobuf.events.catalog.v1.LogicalClusterMetadata;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;
import io.confluent.protobuf.events.catalog.v1.MetadataEvent;
import io.confluent.protobuf.events.catalog.v1.OpType;
import io.confluent.protobuf.events.catalog.v1.TopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class AssociationGarbageCollectionManager implements GarbageCollectionManager {
  private final GarbageCollector garbageCollector;
  private ExecutorService executorService;
  private final Cache<String, CacheValue> snapshotCache;
  private final SchemaRegistryConfig config;
  private final int topicSnapshotBackoffMs;
  private final int kafkaClusterSnapshotBackoffMs;
  private final RejectedExecutionHandler handler;
  private final GarbageCollectionEventFactory gcEventFactory;

  // executor service
  private static final long KEEP_ALIVE_SECONDS = 120; // 2min

  // subjects
  private static final String TOPIC = "topic";
  private static final String TOPIC_AND_CLUSTERLINK = "topicAndClusterLink";
  private static final String KAFKA_CLUSTER = "kafkaCluster";

  private static final Logger log =
      LoggerFactory.getLogger(AssociationGarbageCollectionManager.class);

  public AssociationGarbageCollectionManager(GarbageCollector garbageCollector,
                                             SchemaRegistry schemaRegistry,
                                             RejectedExecutionHandler handler) {
    this.garbageCollector = garbageCollector;
    this.config = schemaRegistry.config();
    // cache
    // 5min
    int entryExpirationSeconds = this.config.getInt(
            SchemaRegistryConfig.ASSOC_GC_CACHE_EXPIRY_SECONDS_CONFIG);
    this.snapshotCache = Caffeine.newBuilder()
            .expireAfterWrite(entryExpirationSeconds, TimeUnit.SECONDS)
            .build();
    this.executorService = createExecutorService(handler);
    this.topicSnapshotBackoffMs = this.config.getInt(
        SchemaRegistryConfig.ASSOC_GC_TOPIC_SNAPSHOT_BACKOFF_SECS_CONFIG) * 1000;
    this.kafkaClusterSnapshotBackoffMs = this.config.getInt(
        SchemaRegistryConfig.ASSOC_GC_KAFKA_CLUSTER_SNAPSHOT_BACKOFF_SECS_CONFIG) * 1000;
    this.handler = handler;
    this.gcEventFactory = new GarbageCollectionEventFactory(new MetadataChangeDeserializer());
  }

  public AssociationGarbageCollectionManager(GarbageCollector garbageCollector,
                                             SchemaRegistry schemaRegistry) {
    this(garbageCollector, schemaRegistry, new ThreadPoolExecutor.CallerRunsPolicy());
  }

  private ExecutorService createExecutorService(RejectedExecutionHandler handler) {
    int corePoolSize = config.getInt(
        SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_CORE_POOL_SIZE_CONFIG);
    int maxPoolSize = config.getInt(
        SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_MAX_POOL_SIZE_CONFIG);
    int queueSize = config.getInt(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_QUEUE_SIZE_CONFIG);
    return new ThreadPoolExecutor(corePoolSize, maxPoolSize, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(queueSize), handler);
  }

  @FunctionalInterface
  private interface Handler {
    void handle(GarbageCollectionEvent event) throws GarbageCollectionException;
  }

  private static class EventHandlerKey {
    private final String subject;
    private final OpType opType;

    public EventHandlerKey(String subject, OpType opType) {
      this.subject = subject;
      this.opType = opType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EventHandlerKey that = (EventHandlerKey) o;
      return subject.equals(that.subject) && opType.equals(that.opType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, opType);
    }

    public static EventHandlerKey createFrom(GarbageCollectionEvent gcEvent) {
      return new EventHandlerKey(gcEvent.getSubject(), gcEvent.getMetadataChange().getOp());
    }
  }

  private final Map<EventHandlerKey, Handler> eventHandlers = Map.of(
          // Topic snapshot
          new EventHandlerKey(TOPIC_AND_CLUSTERLINK, OpType.SNAPSHOT), this::handleTopicSnapshot,
          // Kafka cluster snapshot
          new EventHandlerKey(KAFKA_CLUSTER, OpType.SNAPSHOT), this::handleKafkaClusterSnapshot,
          // Topic delta
          new EventHandlerKey(TOPIC, OpType.DELETE), this::handleTopicDeletion,
          // Kafka cluster delete delta
          new EventHandlerKey(KAFKA_CLUSTER, OpType.DELETE), this::handleKafkaClusterDeletion
  );

  public void handleTopicSnapshot(GarbageCollectionEvent gcEvent)
          throws GarbageCollectionException {
    validateSnapshotMetadata(gcEvent);
    MetadataChange metadataChange = gcEvent.getMetadataChange();
    if (metadataChange == null) {
      throw new GarbageCollectionException("MetadataChange is null for topic snapshot.");
    }
    String snapshotId = gcEvent.getSnapshotId();
    String tenant = gcEvent.getTenant();
    long timestamp = gcEvent.getTimestampMs();

    CacheValue cacheValue = snapshotCache.get(snapshotId, k -> new CacheValue(tenant));
    if (cacheValue == null) {
      throw new GarbageCollectionException(
              String.format("Retrieved null cacheValue for snapshotId=%s "
                      + "from cloud event: id=%s, subject=%s, type=%s",
                      snapshotId, gcEvent.getId(), gcEvent.getSubject(), gcEvent.getType()));
    }
    cacheValue.update(gcEvent, GarbageCollectionEvent::isLastPage);

    if (cacheValue.allPagesReceived()) {
      // Gather all the topic ids in the snapshot
      Set<String> topics = new HashSet<>();
      for (int i = 0; i < cacheValue.totalPages; i++) {
        metadataChange.getEventsList()
                .stream()  // ← Stream the list first
                .filter(event ->
                        event.getMetadataCase() == MetadataEvent.MetadataCase.TOPIC_METADATA)
                .map(MetadataEvent::getTopicMetadata)
                .forEach(topicMetadata -> {
                  topics.add(topicMetadata.getTopicId());
                });
      }
      garbageCollector.processResourceSnapshot(tenant, topics, timestamp - topicSnapshotBackoffMs);
    }
  }

  public void handleKafkaClusterSnapshot(GarbageCollectionEvent gcEvent)
          throws GarbageCollectionException {
    validateSnapshotMetadata(gcEvent);
    MetadataChange metadataChange = gcEvent.getMetadataChange();
    if (metadataChange == null) {
      throw new GarbageCollectionException("MetadataChange is null for kafka cluster snapshot.");
    }
    validateLogicalClusterMetadata(metadataChange, true);

    String snapshotId = gcEvent.getSnapshotId();
    String tenant = gcEvent.getTenant();
    long timestamp = gcEvent.getTimestampMs();

    CacheValue cacheValue = snapshotCache.get(snapshotId, k -> new CacheValue(tenant));
    if (cacheValue == null) {
      throw new GarbageCollectionException(
              String.format("Retrieved null cacheValue for snapshotId=%s "
                              + "from cloud event: id=%s, subject=%s, type=%s",
                      snapshotId, gcEvent.getId(), gcEvent.getSubject(), gcEvent.getType()));
    }
    cacheValue.update(gcEvent,
            event -> gcEvent.getPage() == gcEvent.getTotalPages() - 1);

    if (cacheValue.allPagesReceived()) {
      // Gather all the topic ids in the snapshot
      try {
        Set<String> clusters = new HashSet<>();
        for (int i = 0; i < cacheValue.totalPages; i++) {
          metadataChange.getEventsList()
                  .stream()  // ← Stream the list first
                  .filter(event -> event.getMetadataCase()
                      == MetadataEvent.MetadataCase.LOGICAL_CLUSTER_METADATA)
                  .map(MetadataEvent::getLogicalClusterMetadata)
                  .forEach(clusterMetadata -> {
                    clusters.add(clusterMetadata.getClusterId());
                  });
        }
        garbageCollector.processResourceNamespaceSnapshot(tenant, clusters,
                timestamp - kafkaClusterSnapshotBackoffMs);
      } catch (Exception e) {
        log.error("Failed to process kafka cluster snapshot.", e);
      }
    }
  }

  public void handleTopicDeletion(GarbageCollectionEvent gcEvent)
          throws GarbageCollectionException {
    String tenant = gcEvent.getTenant();
    MetadataChange metadataChange = gcEvent.getMetadataChange();
    // Call garbageCollector to delete each topic for that tenant
    List<TopicMetadata> topics = metadataChange.getEventsList()
            .stream()
            .filter(event -> event.getMetadataCase()
                == MetadataEvent.MetadataCase.TOPIC_METADATA)
            .map(MetadataEvent::getTopicMetadata)
            .toList();
    for (TopicMetadata topic : topics) {
      garbageCollector.processDeletedResource(tenant, topic.getTopicId());
    }
  }

  public void handleKafkaClusterDeletion(GarbageCollectionEvent gcEvent)
          throws GarbageCollectionException {
    MetadataChange metadataChange = gcEvent.getMetadataChange();
    validateLogicalClusterMetadata(metadataChange, false);

    String tenant = gcEvent.getTenant();
    // Call garbageCollector to delete each resourceNamespace for that tenant
    List<LogicalClusterMetadata> clusters = metadataChange.getEventsList()
            .stream()
            .filter(event ->
                    event.getMetadataCase() == MetadataEvent.MetadataCase.LOGICAL_CLUSTER_METADATA)
            .map(MetadataEvent::getLogicalClusterMetadata)
            .toList();
    for (LogicalClusterMetadata cluster : clusters) {
      garbageCollector.processDeletedResourceNamespace(tenant, cluster.getClusterId());
    }
  }

  private boolean validateLogicalClusterMetadata(MetadataChange metadataChange,
                                                 boolean shouldBeActive) {
    /*
      Performs the following checks:
      - LogicalClusterMetadata environment == MetadataChange source
      - If cluster shouldBeActive, deactivated field must be empty; Otherwise must not be empty.
     */
    List<LogicalClusterMetadata> clusters = metadataChange.getEventsList()
            .stream()
            .filter(event ->
                    event.getMetadataCase() == MetadataEvent.MetadataCase.LOGICAL_CLUSTER_METADATA)
            .map(MetadataEvent::getLogicalClusterMetadata)
            .collect(Collectors.toList());

    String source = metadataChange.getSource();

    for (LogicalClusterMetadata clusterMetadata : clusters) {
      String environment = clusterMetadata.getEnvironment();
      if (!environment.equals(source)) {
        throw new IllegalArgumentException(
            String.format("LogicalClusterMetadata environment doesn't match source. "
                + "environment: %s, source: %s", source, environment));
      }
      if (shouldBeActive && clusterMetadata.hasDeactivated()) {
        throw new IllegalArgumentException(
            "LogicalClusterMetadata should be active but was deactivated. Source: " + source);
      }
      if (!shouldBeActive && !clusterMetadata.hasDeactivated()) {
        throw new IllegalArgumentException(
            "LogicalClusterMetadata should be deactivated but was active. Source: " + source);
      }
    }
    return true;
  }

  private void validateSnapshotMetadata(GarbageCollectionEvent gcEvent) {
    String subject = gcEvent.getSubject();
    switch (subject) {
      case TOPIC_AND_CLUSTERLINK:
        if (gcEvent.isLastPage() == null) {
          throw new IllegalArgumentException(
              String.format("Snapshot header ce_lastpage is missing for cloudEvent: "
                  + "id={}, subject={}, source={}", gcEvent.getId(), gcEvent.getSubject(),
                  gcEvent.getMetadataChange().getSource()));
        }
        break;
      case KAFKA_CLUSTER:
        if (gcEvent.getTotalPages() == null) {
          throw new IllegalArgumentException(
              String.format("Snapshot header ce_total is missing for cloudEvent: "
                  + "id={}, subject={}, source={}", gcEvent.getId(), gcEvent.getSubject(),
                  gcEvent.getMetadataChange().getSource()));
        }
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown snapshot subject: %s", subject));
    }
  }

  @Override
  public void init() throws GarbageCollectionException {
    garbageCollector.init();
    if (executorService == null || executorService.isShutdown() || executorService.isTerminated()) {
      executorService = createExecutorService(handler);
    }
  }

  @Override
  public void processEvent(WireEvent wireEvent) throws GarbageCollectionException {
    try {
      log.info(String.format("Garbage collection processing event %s", wireEvent));
      if (wireEvent == null) {
        log.warn("Received null wireEvent");
        return;
      }

      GarbageCollectionEvent gcEvent = gcEventFactory.createFrom(wireEvent);

      String type = gcEvent.getType();
      String subject = gcEvent.getSubject();
      // Find the event handler based on op and subject
      Handler eventHandler = eventHandlers.get(EventHandlerKey.createFrom(gcEvent));
      if (eventHandler == null) {
        log.debug("No handler is found for type {} and subject {}", type, subject);
        return;
      }
      log.info("Get eventHandler for type {} and subject {}", type, subject);
      executorService.submit(() -> {
        try {
          eventHandler.handle(gcEvent);
        } catch (Exception e) {
          log.error("Event handler failed to process garbage collection event.", e);
        }
      });
    } catch (Exception e) {
      log.error("processEvent failed to process wire event", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (executorService != null) {
      executorService.shutdown();
    }
    if (garbageCollector != null) {
      garbageCollector.close();
    }
    // The cache entries will expire automatically; do nothing.
  }

  protected static class CacheValue {
    private Map<Integer, MetadataChange> pages;
    private int totalPages;
    private String tenant;

    public CacheValue(String tenant) {
      this.pages = new ConcurrentHashMap<>();
      this.totalPages = -1;
      this.tenant = tenant;
    }

    public Map<Integer, MetadataChange> getPages() {
      return this.pages;
    }

    public String getTenant() {
      return this.tenant;
    }

    public boolean allPagesReceived() {
      if (totalPages == -1) {
        return false;
      }
      if (pages.size() != totalPages) {
        return false;
      }
      for (int i = 0; i < this.pages.size(); i++) {
        if (!pages.containsKey(i)) {
          return false;
        }
      }
      return true;
    }

    public int getTotalPages() {
      return totalPages;
    }

    public MetadataChange getPage(int pageNumber) {
      return pages.get(pageNumber);
    }

    public synchronized void update(GarbageCollectionEvent gcEvent,
                                    Predicate<GarbageCollectionEvent> isLastPage)
            throws InconsistentSnapshotPageException {
      // Add the metadata change
      int pageNumber = gcEvent.getPage();
      MetadataChange metadataChange = gcEvent.getMetadataChange();
      if (pages.containsKey(pageNumber) && !pages.get(pageNumber).equals(metadataChange)) {
        throw new InconsistentSnapshotPageException(
            "Received the same snapshot page twice with different content. "
                + String.format("source: %s, pageNumber: %s",
                metadataChange.getSource(), pageNumber));
      }
      pages.put(pageNumber, metadataChange);

      // Update totalPages if needed
      if (isLastPage.test(gcEvent)) {
        int totalPages = pageNumber + 1;
        if (this.totalPages != -1 && this.totalPages != totalPages) {
          throw new InconsistentSnapshotPageException(
              String.format("totalPages was already set to %s. "
                  + "It was tried to be updated to %s.", this.totalPages, totalPages));
        }
        this.totalPages = totalPages;
      }
    }
  }
}
