package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.cloudevents.CloudEvent;
import io.confluent.kafka.schemaregistry.exceptions.GarbageCollectionException;
import io.confluent.kafka.schemaregistry.exceptions.InconsistentSnapshotPageException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.protobuf.events.catalog.v1.LogicalClusterMetadata;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;
import io.confluent.protobuf.events.catalog.v1.MetadataEvent;
import io.confluent.protobuf.events.catalog.v1.OpType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DefaultGarbageCollectionManager implements GarbageCollectionManager {
  private GarbageCollector garbageCollector;
  private ExecutorService executorService;
  private Cache<String, Map<String, CacheValue>> snapshotCache;
  private SchemaRegistryConfig config;
  private int topicSnapshotBackoffSecs;
  private int kafkaClusterSnapshotBackoffSecs;

  private static ObjectMapper MAPPER = new ObjectMapper();
  // cache
 private static final int ENTRY_EXPIRATION_SECONDS = 300; // 5min
  // executor service
  private static final long KEEP_ALIVE_SECONDS = 120; // 2min
  // headers/cloudevent fields needed for processing
  public static final String LSRC_HEADER = "_lsrc";
  private static final String PAGE_HEADER = "ce_page";
  private static final String PAGE_TOTAL_HEADER = "ce_total";
  private static final String LAST_PAGE_HEADER = "ce_lastpage";
  private static final String SNAPSHOT_ID_HEADER = "ce_snapshotid";
  private static final String CLOUD_EVENT_TS = "cloud_event_ts";
  private static final List<String> HEADERS_FOR_PROCESSING = List.of(LSRC_HEADER, PAGE_HEADER,
          PAGE_TOTAL_HEADER, LAST_PAGE_HEADER, SNAPSHOT_ID_HEADER);
  // subjects
  private static final String TOPIC = "topic"; // cloudEvent subject for topic delta event
  private static final String TOPIC_AND_CLUSTERLINK = "topicAndClusterLink"; // cloudEvent subject for topic snapshot
  private static final String KAFKA_CLUSTER = "kafkaCluster"; // cloudEvent subject for kafkaCluster delta event and snapshot
  private static final Set<String> CLOUDEVENT_GC_SUBJECTS = Set.of(
          TOPIC, TOPIC_AND_CLUSTERLINK,  KAFKA_CLUSTER);
  // op
  private static final String DELETE = "delete";
  private static final String DELTA = "DELTA";
  private static final String SNAPSHOT = "SNAPSHOT";
  private static final Set<String> CLOUDEVENT_GC_TYPE = Set.of(DELETE, DELTA,  SNAPSHOT);
  // data format
  private static final String PROTOBUF = "protobuf";
  private static final String JSON = "json";

  private static Logger log = LoggerFactory.getLogger(DefaultGarbageCollectionManager.class);

  public DefaultGarbageCollectionManager(GarbageCollector garbageCollector, SchemaRegistry schemaRegistry) {
    this.garbageCollector = garbageCollector;
    this.config = schemaRegistry.config();
    this.snapshotCache = Caffeine.newBuilder()
            .expireAfterWrite(ENTRY_EXPIRATION_SECONDS, TimeUnit.SECONDS)
            .build();
    this.executorService = createExecutorService();
    this.topicSnapshotBackoffSecs = this.config.getInt(
            SchemaRegistryConfig.ASSOC_GC_TOPIC_SNAPSHOT_BACKOFF_SECS_CONFIG);
    this.kafkaClusterSnapshotBackoffSecs = this.config.getInt(
            SchemaRegistryConfig.ASSOC_GC_KAFKA_CLUSTER_SNAPSHOT_BACKOFF_SECS_CONFIG);
  }

  private ExecutorService createExecutorService() {
    int corePoolSize = config.getInt(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_CORE_POOL_SIZE_CONFIG);
    int maxPoolSize = config.getInt(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_MAX_POOL_SIZE_CONFIG);
    int queueSize = config.getInt(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_QUEUE_SIZE_CONFIG);
    return new ThreadPoolExecutor(corePoolSize, maxPoolSize, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueSize), new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @FunctionalInterface
  private interface Handler {
    void handle(MetadataChange metadataChange, Map<String, String> properties)
            throws GarbageCollectionException;
  }
  private final Map<String, Handler> eventHandlers = Map.of(
          // Topic snapshot
          getEventHandlerKey(SNAPSHOT, TOPIC_AND_CLUSTERLINK), this::handleTopicSnapshot,
          // Kafka cluster snapshot
          getEventHandlerKey(SNAPSHOT, KAFKA_CLUSTER), this::handleKafkaClusterSnapshot,
          // Topic delta
          getEventHandlerKey(DELTA, TOPIC), this::handleTopicDeletion,
          // Kafka cluster delete delta
          getEventHandlerKey(DELETE, KAFKA_CLUSTER), this::handleKafkaClusterDeletion
  );

  private String getEventHandlerKey(String type, String subject) {
    return String.format("%s-%s", type, subject);
  }

  public void handleTopicSnapshot(MetadataChange metadataChange, Map<String, String> eventProperties)
          throws GarbageCollectionException {
    validateSnapshotPageAndProperties(metadataChange, eventProperties);
    Map<String, CacheValue> topicSnapshotCache = snapshotCache.get(TOPIC,
            key -> new ConcurrentHashMap<>());
    boolean isLastPage = Boolean.valueOf(eventProperties.get(LAST_PAGE_HEADER));

    updateCache(metadataChange, eventProperties, topicSnapshotCache, isLastPage);

    String snapshotId = eventProperties.get(SNAPSHOT_ID_HEADER);
    String tenant = eventProperties.get(LSRC_HEADER);
    long timestamp = Long.valueOf(eventProperties.get(CLOUD_EVENT_TS));

    CacheValue value = topicSnapshotCache.get(snapshotId);
    if (value.allPagesReceived()) {
      executorService.submit(() -> {
        try {
          // Gather all the topic ids in the snapshot
          Set<String> topics = new HashSet<>();
          for (int i = 0; i < value.totalPages; i++) {
            metadataChange.getEventsList()
                    .stream()  // ← Stream the list first
                    .filter(event ->
                            event.getMetadataCase() == MetadataEvent.MetadataCase.TOPIC_METADATA)
                    .map(MetadataEvent::getTopicMetadata)
                    .forEach(topicMetadata -> {
                      topics.add(topicMetadata.getTopicId());
                    });
          }
          garbageCollector.processResourceSnapshot(tenant, topics, timestamp - topicSnapshotBackoffSecs);
        } catch (Exception e) {
          log.error("Failed to process topic snapshot.", e);
        }
      });
    }
  }

  public void handleKafkaClusterSnapshot(MetadataChange metadataChange,
                                         Map<String, String> eventProperties)
          throws GarbageCollectionException {
    validateSnapshotPageAndProperties(metadataChange, eventProperties);
    validateLogicalClusterMetadata(metadataChange, true);

    Map<String, CacheValue> clusterSnapshotCache = snapshotCache.get(KAFKA_CLUSTER,
            key -> new ConcurrentHashMap<>());

    int totalPages = Integer.valueOf(eventProperties.get(PAGE_TOTAL_HEADER));
    int page = Integer.valueOf(eventProperties.get(PAGE_HEADER));
    boolean isLastPage = page == totalPages - 1 ? true : false;

    updateCache(metadataChange, eventProperties, clusterSnapshotCache, isLastPage);
    String snapshotId = eventProperties.get(SNAPSHOT_ID_HEADER);
    String tenant = eventProperties.get(LSRC_HEADER);
    long timestamp = Long.valueOf(eventProperties.get(CLOUD_EVENT_TS));

    CacheValue value = clusterSnapshotCache.get(snapshotId);
    if (value.allPagesReceived()) {
      executorService.submit(() -> {
        // Gather all the topic ids in the snapshot
        try {
          Set<String> clusters = new HashSet<>();
          for (int i = 0; i < value.totalPages; i++) {
            metadataChange.getEventsList()
                    .stream()  // ← Stream the list first
                    .filter(event ->
                            event.getMetadataCase() == MetadataEvent.MetadataCase.LOGICAL_CLUSTER_METADATA)
                    .map(MetadataEvent::getLogicalClusterMetadata)
                    .forEach(clusterMetadata -> {
                      clusters.add(clusterMetadata.getClusterId());
                    });
          }
          garbageCollector.processResourceNamespaceSnapshot(tenant, clusters,
                  timestamp - kafkaClusterSnapshotBackoffSecs);
        } catch (Exception e) {
          log.error("Failed to process kafka cluster snapshot.", e);
        }
      });
    }
  }

  public void handleTopicDeletion(MetadataChange metadataChange, Map<String, String> eventProperties)
          throws GarbageCollectionException {
    if (metadataChange.getOp() != OpType.DELETE) {
      log.debug("Received non-delete topic delta event. Dropped the event.");
    }
    validateDeletionDeltaEventAndProperties(metadataChange, eventProperties);

    String tenant = eventProperties.get(LSRC_HEADER);
    // Call garbageCollector to delete each topic for that tenant
    metadataChange.getEventsList()
            .stream()  // ← Stream the list first
            .filter(event -> event.getMetadataCase() == MetadataEvent.MetadataCase.TOPIC_METADATA)
            .map(MetadataEvent::getTopicMetadata)
            .forEach(topicMetadata -> {
              executorService.submit(() -> {
                try {
                  garbageCollector.processDeletedResource(tenant, topicMetadata.getTopicId());
                } catch (Exception e) {
                  log.error("Failed to process topic deletion delta event", e);
                }
              });
            });

  }

  public void handleKafkaClusterDeletion(MetadataChange metadataChange, Map<String, String> eventProperties)
          throws GarbageCollectionException {
    validateDeletionDeltaEventAndProperties(metadataChange, eventProperties);
    validateLogicalClusterMetadata(metadataChange, false);

    String tenant = eventProperties.get(LSRC_HEADER);
    // Call garbageCollector to delete each resourceNamespace for that tenant
    metadataChange.getEventsList()
            .stream()
            .filter(event ->
                    event.getMetadataCase() == MetadataEvent.MetadataCase.LOGICAL_CLUSTER_METADATA)
            .map(MetadataEvent::getLogicalClusterMetadata)
            .collect(Collectors.toList())
            .forEach(cluster -> {
              executorService.submit(() -> {
                try {
                  garbageCollector.processDeletedResourceNamespace(tenant, cluster.getClusterId());
                } catch (Exception e) {
                  log.error("Failed to process kafka cluster deletion delta event.", e);
                }
              });
            });
    }

  private void validateSnapshotPageAndProperties(MetadataChange metadataChange, Map<String, String> eventProperties) {
    /*
      Make sure:
      - metadataChange Op == SNAPSHOT
      - Has LSRC_HEADER
      - Has SNAPSHOT_ID_HEADER
      - PAGE_HEADER is integer
      - CLOUD_EVENT_TS is long
     */
    if (metadataChange.getOp() != OpType.SNAPSHOT) {
      throw new IllegalArgumentException("Snapshot metadataChange Op must be SNAPSHOT. Received: "
              + metadataChange.getOp());
    }
    String tenant = eventProperties.get(LSRC_HEADER);
    String snapshotId = eventProperties.get(SNAPSHOT_ID_HEADER);
    if (tenant == null || snapshotId == null) {
      throw new IllegalArgumentException(String.format("Snapshot page headers %s and %s are required",
              LSRC_HEADER, SNAPSHOT_ID_HEADER));
    }
    try {
      Integer.valueOf(eventProperties.get(PAGE_HEADER));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("Snapshot page header %s must be an integer",
              PAGE_HEADER));
    }
    try {
      if (eventProperties.get(PAGE_TOTAL_HEADER) != null) {
        Integer.valueOf(eventProperties.get(PAGE_TOTAL_HEADER));
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("Snapshot page header %s must be an integer",
              PAGE_TOTAL_HEADER));
    }
    try {
      Long.valueOf(eventProperties.get(CLOUD_EVENT_TS));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("CloudEvent timestamp must be present and can be parsed to long.");
    }
  }

  private void updateCache(MetadataChange metadataChange, Map<String, String> eventProperties,
                                         Map<String, CacheValue> subjectCache, boolean isLastPage)
          throws GarbageCollectionException {
      String tenant = eventProperties.get(LSRC_HEADER);
      String snapshotId = eventProperties.get(SNAPSHOT_ID_HEADER);
      int page = Integer.valueOf(eventProperties.get(PAGE_HEADER));

      CacheValue value = subjectCache.computeIfAbsent(snapshotId, key -> new CacheValue(tenant));
      value.addPageIfNotExist(page, metadataChange);

      if (isLastPage) {
        int totalPages = page + 1;
        try {
          value.setTotalPagesIfUndefined(totalPages);
        } catch (InconsistentSnapshotPageException e) {
          throw new GarbageCollectionException(
                  String.format("Two different totalPages are received for the same snapshotId. " +
                          "MetadataChange: %s, eventProperties: %s" +
                          "old total pages: %s", metadataChange, eventProperties, value.getTotalPages()));
        }
      }
  }

  private boolean validateLogicalClusterMetadata(MetadataChange metadataChange, boolean shouldBeActive) {
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
        throw new IllegalArgumentException(String.format("LogicalClusterMetadata environment doesn't match source." +
                        "environment: %s, source: %s", source, environment));
      }
      if (shouldBeActive && clusterMetadata.hasDeactivated()) {
        throw new IllegalArgumentException(
                "LogicalClusterMetadata should be active but was deactivated. Source: " + source );
      }
      if (!shouldBeActive && !clusterMetadata.hasDeactivated()) {
        throw new IllegalArgumentException(
                "LogicalClusterMetadata should be deactivated but was active. Source: " + source );
      }
    }
    return true;
  }

  private void validateDeletionDeltaEventAndProperties(MetadataChange metadataChange, Map<String, String> eventProperties) {
    if (metadataChange.getOp() != OpType.DELETE) {
      throw new IllegalArgumentException("Deletion metadataChange Op must be DELETE. Received: "
              + metadataChange.getOp());
    }
    String tenant = eventProperties.get(LSRC_HEADER);
    if (tenant == null) {
      throw new IllegalArgumentException(String.format("Snapshot page headers %s and %s are required",
              LSRC_HEADER, SNAPSHOT_ID_HEADER));
    }
  }

  @Override
  public void init() throws GarbageCollectionException {
    garbageCollector.init();
    if (executorService == null || executorService.isShutdown() || executorService.isTerminated()) {
      executorService = createExecutorService();
    }
  }

  private void validateWireEvent(WireEvent wireEvent) {
    CloudEvent cloudEvent = wireEvent.getCloudEvent();
    if (cloudEvent == null) {
      throw new IllegalArgumentException("WireEvent missing required field: cloudEvent");
    }
  }

  private void validateCloudEvent(CloudEvent cloudEvent) {
    String type = cloudEvent.getType();
    String subject = cloudEvent.getSubject();
    String contentType = cloudEvent.getDataContentType();
    byte[] data = cloudEvent.getData().toBytes();
    OffsetDateTime time = cloudEvent.getTime();
    if (type == null || subject == null || contentType == null || data == null || time == null) {
      throw new IllegalArgumentException("CloudEvent missing required fields: " +
              "type/subject/datacontenttype/data/time");
    }
  }

  private void validateMetadataChange(MetadataChange metadataChange) {
    String source = metadataChange.getSource();
    String op = metadataChange.getOp().toString();
    List<MetadataEvent> events = metadataChange.getEventsList();
    if (source == null || op == null || events == null || events.isEmpty()) {
      throw new IllegalArgumentException("MetadataChange missing required fields: " +
              "source/op/events");
    }
  }

  private MetadataChange deserializeMetadataChange(byte[] data, String contentType) {
    MetadataChange metadataChange;
    try {
      if (contentType.toLowerCase().contains(PROTOBUF)) {
        metadataChange = MetadataChange.parseFrom(data);
      } else {
        metadataChange = MAPPER.readValue(data, MetadataChange.class);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Can't parse metadataChange with format " + contentType, e);
    }
    return metadataChange;
  }

  private Handler getEventHandler(String type, String subject) {
    Handler handler = eventHandlers.get(getEventHandlerKey(type, subject));
    if (handler == null) {
      throw new IllegalArgumentException("Unknown event handler for type " + type + " and subject " + subject);
    }
    return handler;
  }

  @Override
  public void processEvent(WireEvent wireEvent) throws GarbageCollectionException {
    try {
      validateWireEvent(wireEvent);
      CloudEvent cloudEvent = wireEvent.getCloudEvent();
      validateCloudEvent(cloudEvent);

      String type = cloudEvent.getType();
      String subject = cloudEvent.getSubject();
      String contentType = cloudEvent.getDataContentType();
      byte[] data = cloudEvent.getData().toBytes();

      if (!CLOUDEVENT_GC_SUBJECTS.contains(subject) || !CLOUDEVENT_GC_TYPE.contains(type)) {
        log.debug("Dropping event with subject {} and type {} (not for GC)", subject, type);
        return;
      }

      MetadataChange metadataChange = deserializeMetadataChange(data, contentType);
      validateMetadataChange(metadataChange);

      // Create metadata map which contain fields from envelopes: wire event headers and cloudEvent
      Map<String, String> eventProperties = new HashMap<>();
      for (String headerKey : HEADERS_FOR_PROCESSING) {
        eventProperties.put(headerKey, wireEvent.getHeaders().get(headerKey));
      }
      if (cloudEvent.getTime() != null) {
        eventProperties.put(CLOUD_EVENT_TS, String.valueOf(cloudEvent.getTime().toInstant().toEpochMilli()));
      }

      // Find the event handler based on the type and the subject
      Handler eventHandler = getEventHandler(type, subject);
      eventHandler.handle(metadataChange, eventProperties);
    } catch (Exception e) {
      log.error(e.getMessage(), e.getStackTrace());
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

  public static class CacheValue {
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

    public synchronized void addPageIfNotExist(int pageNumber, MetadataChange metadataChange)
            throws InconsistentSnapshotPageException {
      if (pages.containsKey(pageNumber) && !pages.get(pageNumber).equals(metadataChange)) {
        throw new InconsistentSnapshotPageException("Received the same snapshot page twice with different content. " +
                String.format("source: %s, pageNumber: %s", metadataChange.getSource(), pageNumber));
      }
      pages.put(pageNumber, metadataChange);
    }

    public void setTotalPagesIfUndefined(int totalPages)
            throws InconsistentSnapshotPageException {
      if (this.totalPages != -1 && totalPages != this.totalPages) {
        throw new InconsistentSnapshotPageException(
                String.format("totalPages was already set to %s. " +
                        "It was tried to be updated to %s.", this.totalPages, totalPages));
      }
      this.totalPages = totalPages;
    }
  }
}
