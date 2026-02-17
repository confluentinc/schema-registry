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
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.exceptions.InconsistentSnapshotPageException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.GarbageCollectionEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.WireEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.serde.MetadataChangeDeserializer;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;
import io.confluent.protobuf.events.catalog.v1.MetadataEvent;
import io.confluent.protobuf.events.catalog.v1.OpType;
import io.confluent.protobuf.events.catalog.v1.TopicMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AssociationGarbageCollectionManagerTest extends ClusterTestHarness {

  private static final String TENANT = "tenant-1";
  private static final String TOPIC_ID_1 = "topic-1";
  private static final String TOPIC_ID_2 = "topic-2";
  private static final String TOPIC_ID_3 = "topic-3";
  private static final String SUBJECT_TOPIC = "topic";
  private static final String SUBJECT_TOPIC_AND_CLUSTERLINK = "topicAndClusterLink";
  private static final String CE_TYPE = "io.confluent.catalog.v1.metadata.change";
  private static final String DELTA = "DELTA";
  private static final String SNAPSHOT = "SNAPSHOT";
  private static final String PROTOBUF_CONTENT_TYPE = "application/protobuf";

  @Mock
  private GarbageCollector garbageCollector;

  private SchemaRegistry schemaRegistry;
  private AssociationGarbageCollectionManager manager;
  private AutoCloseable mocks;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUpTest(testInfo);
    mocks = MockitoAnnotations.openMocks(this);

    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    props.put(SchemaRegistryConfig.ASSOC_GC_ENABLE_CONFIG, false);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_CORE_POOL_SIZE_CONFIG, 1);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_MAX_POOL_SIZE_CONFIG, 1);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_QUEUE_SIZE_CONFIG, 1);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    schemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    schemaRegistry.init();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (manager != null) {
      manager.close();
    }
    if (schemaRegistry != null) {
      schemaRegistry.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  private WireEvent wireEventTopicDeletion(String topicId) {
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setSource("lkc-1")
        .setOp(OpType.DELETE)
        .addEvents(MetadataEvent.newBuilder()
            .setTopicMetadata(TopicMetadata.newBuilder()
                .setTopicId(topicId)
                .setTopicName(topicId)
                .build())
            .build())
        .build();
    CloudEvent cloudEvent = buildCloudEvent("id-" + topicId, DELTA, SUBJECT_TOPIC,
        PROTOBUF_CONTENT_TYPE, metadataChange.toByteArray(), OffsetDateTime.now(), null);
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TENANT);
    return new WireEvent(headers, cloudEvent);
  }

  private WireEvent wireEventTopicSnapshot(String snapshotId, int page, int totalPages,
                                           boolean isLastPage, String... topicIds) {
    MetadataChange.Builder builder = MetadataChange.newBuilder()
        .setSource("lkc-1")
        .setOp(OpType.SNAPSHOT);
    for (String topicId : topicIds) {
      builder.addEvents(MetadataEvent.newBuilder()
          .setTopicMetadata(TopicMetadata.newBuilder()
              .setTopicId(topicId)
              .setTopicName(topicId)
              .build())
          .build());
    }
    MetadataChange metadataChange = builder.build();
    Map<String, Object> extensions = new HashMap<>();
    extensions.put("snapshotid", snapshotId);
    extensions.put("page", page);
    extensions.put("total", totalPages);
    extensions.put("lastpage", isLastPage);
    CloudEvent cloudEvent = buildCloudEvent("id-" + snapshotId + "-" + page, SNAPSHOT,
        SUBJECT_TOPIC_AND_CLUSTERLINK, PROTOBUF_CONTENT_TYPE, metadataChange.toByteArray(),
        OffsetDateTime.now(), extensions);
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TENANT);
    return new WireEvent(headers, cloudEvent);
  }

  private WireEvent wireEventUnknownSubject() {
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setSource("lkc-1")
        .setOp(OpType.DELETE)
        .addEvents(MetadataEvent.newBuilder()
            .setTopicMetadata(TopicMetadata.newBuilder().setTopicId("t").setTopicName("t").build())
            .build())
        .build();
    CloudEvent cloudEvent = buildCloudEvent("id", DELTA, "unknownSubject",
        PROTOBUF_CONTENT_TYPE, metadataChange.toByteArray(), OffsetDateTime.now(), null);
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TENANT);
    return new WireEvent(headers, cloudEvent);
  }

  private CloudEvent buildCloudEvent(String id, String type, String subject, String contentType,
                                     byte[] data, OffsetDateTime time, Map<String, Object> extensions) {
    TestingCloudEvent event = new TestingCloudEvent();
    event.setProperty("id", id);
    event.setProperty("type", type);
    event.setProperty("subject", subject);
    event.setProperty("datacontenttype", contentType);
    event.setProperty("data", data);
    event.setProperty("time", time);
    event.setProperty("source", URI.create("http://test"));
    if (extensions != null) {
      event.setExtensions(extensions);
    }
    return event;
  }

  private void createManagerWithCallerRunsPolicy() {
    manager = new AssociationGarbageCollectionManager(garbageCollector, schemaRegistry,
        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
  }

  private void awaitHandler() throws InterruptedException {
    Thread.sleep(150);
  }

  @Test
  public void testProcessEvent_TopicDeletion() throws Exception {
    createManagerWithCallerRunsPolicy();
    WireEvent wireEvent = wireEventTopicDeletion(TOPIC_ID_1);

    manager.processEvent(wireEvent);

    awaitHandler();
    verify(garbageCollector, times(1)).processDeletedResource(TENANT, TOPIC_ID_1);
  }

  @Test
  public void testProcessEvent_TopicSnapshot_SinglePage() throws Exception {
    createManagerWithCallerRunsPolicy();
    WireEvent wireEvent = wireEventTopicSnapshot("snap-1", 0, 1, true, TOPIC_ID_1);

    manager.processEvent(wireEvent);

    awaitHandler();
    verify(garbageCollector, times(1)).processResourceSnapshot(eq(TENANT), any(), anyLong());
  }

  @Test
  public void testProcessEvent_TopicSnapshot_MultiplePages() throws Exception {
    createManagerWithCallerRunsPolicy();
    WireEvent page0 = wireEventTopicSnapshot("snap-2", 0, 2, false, TOPIC_ID_1);
    WireEvent page1 = wireEventTopicSnapshot("snap-2", 1, 2, true, TOPIC_ID_2);

    manager.processEvent(page0);
    manager.processEvent(page1);

    awaitHandler();
    awaitHandler();
    verify(garbageCollector, times(1)).processResourceSnapshot(eq(TENANT), any(), anyLong());
  }

  @Test
  public void testProcessEvent_UnknownEventType() throws Exception {
    createManagerWithCallerRunsPolicy();
    WireEvent wireEvent = wireEventUnknownSubject();

    manager.processEvent(wireEvent);

    awaitHandler();
    verify(garbageCollector, never()).processDeletedResource(any(), any());
    verify(garbageCollector, never()).processResourceSnapshot(any(), any(), anyLong());
  }

  @Test
  public void testProcessEvent_NullWireEvent() throws Exception {
    createManagerWithCallerRunsPolicy();

    manager.processEvent(null);

    verify(garbageCollector, never()).processDeletedResource(any(), any());
  }

  @Test
  public void testUpdateCache_InconsistentTotalPages() throws Exception {
    AssociationGarbageCollectionManager.CacheValue cache =
        new AssociationGarbageCollectionManager.CacheValue(TENANT);
    GarbageCollectionEventFactory factory = new GarbageCollectionEventFactory(
        new MetadataChangeDeserializer());
    WireEvent page0Last = wireEventTopicSnapshot("snap-3", 0, 1, true, TOPIC_ID_1);
    WireEvent page1Last = wireEventTopicSnapshot("snap-3", 1, 2, true, TOPIC_ID_2);
    GarbageCollectionEvent event0 = factory.createFrom(page0Last);
    GarbageCollectionEvent event1 = factory.createFrom(page1Last);
    cache.update(event0, e -> e.isLastPage() != null && e.isLastPage());

    assertThrows(InconsistentSnapshotPageException.class, () ->
        cache.update(event1, e -> e.isLastPage() != null && e.isLastPage()));
  }

  @Test
  public void testUpdateCache_DuplicatePageDifferentContent() throws Exception {
    AssociationGarbageCollectionManager.CacheValue cache =
        new AssociationGarbageCollectionManager.CacheValue(TENANT);
    GarbageCollectionEventFactory factory = new GarbageCollectionEventFactory(
        new MetadataChangeDeserializer());
    WireEvent page0a = wireEventTopicSnapshot("snap-4", 0, 2, false, TOPIC_ID_1);
    WireEvent page0b = wireEventTopicSnapshot("snap-4", 0, 2, false, TOPIC_ID_2);
    GarbageCollectionEvent event0a = factory.createFrom(page0a);
    GarbageCollectionEvent event0b = factory.createFrom(page0b);
    cache.update(event0a, GarbageCollectionEvent::isLastPage);

    assertThrows(InconsistentSnapshotPageException.class, () ->
        cache.update(event0b, GarbageCollectionEvent::isLastPage));
  }

  /**
   * Verifies that when the executor queue is full, CallerRunsPolicy runs the rejected handler
   * in the caller thread. With queue size 1: first event runs on worker, second is queued,
   * third is rejected and runs in caller. The first handler sleeps 2s so the queue fills.
   */
  @Test
  public void testUpdateCache_ExecutorQueueIsFullAndCallerRunsTheJob() throws Exception {
    createManagerWithCallerRunsPolicy();
    CopyOnWriteArrayList<Long> threadsThatCalledGc = new CopyOnWriteArrayList<>();

    doAnswer(inv -> {
      threadsThatCalledGc.add(Thread.currentThread().getId());
      Thread.sleep(1000);
      return null;
    }).when(garbageCollector).processDeletedResource(any(), any());

    WireEvent event1 = wireEventTopicDeletion(TOPIC_ID_1);
    WireEvent event2 = wireEventTopicDeletion(TOPIC_ID_2);
    WireEvent event3 = wireEventTopicDeletion(TOPIC_ID_3);

    manager.processEvent(event1);
    manager.processEvent(event2);
    manager.processEvent(event3);

    awaitHandler();
    awaitHandler();
    awaitHandler();

    verify(garbageCollector, times(1)).processDeletedResource(TENANT, TOPIC_ID_1);
    verify(garbageCollector, times(1)).processDeletedResource(TENANT, TOPIC_ID_2);
    verify(garbageCollector, times(1)).processDeletedResource(TENANT, TOPIC_ID_3);
    assertEquals(3, threadsThatCalledGc.size(), "GC should have been called three times");
    assertEquals(2, threadsThatCalledGc.stream().distinct().count(),
        "Two different threads should have called GC (pool thread and caller thread)");
  }

  /**
   * With a short cache expiration, the first page's cache entry expires before the last page
   * arrives. GC should not run because the evicted entry leaves only one page (incomplete snapshot).
   */
  @Test
  public void testUpdateCache_CacheEntryExpires() throws Exception {
    if (manager != null) {
      manager.close();
    }
    if (schemaRegistry != null) {
      schemaRegistry.close();
    }

    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    props.put(SchemaRegistryConfig.ASSOC_GC_ENABLE_CONFIG, false);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_CORE_POOL_SIZE_CONFIG, 1);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_MAX_POOL_SIZE_CONFIG, 1);
    props.put(SchemaRegistryConfig.ASSOC_GC_EXECUTOR_SERVICE_QUEUE_SIZE_CONFIG, 1);
    props.put(SchemaRegistryConfig.ASSOC_GC_CACHE_EXPIRY_SECONDS_CONFIG, 1);

    SchemaRegistryConfig shortExpiryConfig = new SchemaRegistryConfig(props);
    SchemaRegistry testSchemaRegistry = new KafkaSchemaRegistry(shortExpiryConfig, new SchemaRegistrySerializer());
    testSchemaRegistry.init();

    AssociationGarbageCollectionManager testManager = new AssociationGarbageCollectionManager(
        garbageCollector, testSchemaRegistry, new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());

    try {
      WireEvent page0 = wireEventTopicSnapshot("snap-exp", 0, 2, false, TOPIC_ID_1);
      testManager.processEvent(page0);
      awaitHandler();

      Thread.sleep(TimeUnit.SECONDS.toMillis(1));

      WireEvent page1 = wireEventTopicSnapshot("snap-exp", 1, 2, true, TOPIC_ID_2);
      testManager.processEvent(page1);
      awaitHandler();

      verify(garbageCollector, never()).processResourceSnapshot(any(), any(), anyLong());
    } finally {
      testManager.close();
      testSchemaRegistry.close();
    }
  }

  private static class TestingCloudEvent implements CloudEvent {
    private final Map<String, Object> properties = new HashMap<>();
    private Map<String, Object> extensions = new HashMap<>();

    void setProperty(String key, Object value) {
      properties.put(key, value);
    }

    void setExtensions(Map<String, Object> ext) {
      this.extensions = ext != null ? new HashMap<>(ext) : new HashMap<>();
    }

    @Override
    public CloudEventData getData() {
      Object data = properties.get("data");
      if (data == null) return null;
      if (data instanceof CloudEventData) return (CloudEventData) data;
      byte[] bytes = (byte[]) data;
      return bytes != null ? new CloudEventData() {
        @Override
        public byte[] toBytes() { return bytes; }
      } : null;
    }

    @Override
    public SpecVersion getSpecVersion() { return SpecVersion.V1; }

    @Override
    public String getId() { return (String) properties.get("id"); }

    @Override
    public String getType() { return (String) properties.get("type"); }

    @Override
    public URI getSource() { return (URI) properties.get("source"); }

    @Override
    public String getDataContentType() { return (String) properties.get("datacontenttype"); }

    @Override
    public URI getDataSchema() { return null; }

    @Override
    public String getSubject() { return (String) properties.get("subject"); }

    @Override
    public OffsetDateTime getTime() { return (OffsetDateTime) properties.get("time"); }

    @Override
    public Object getAttribute(String name) { return null; }

    @Override
    public Object getExtension(String name) { return extensions.get(name); }

    @Override
    public Set<String> getExtensionNames() {
      return extensions.isEmpty() ? Collections.emptySet() : extensions.keySet();
    }
  }
}
