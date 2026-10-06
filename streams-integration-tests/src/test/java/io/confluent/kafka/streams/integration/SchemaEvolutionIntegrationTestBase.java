/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.streams.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.integration.avro.SensorKey;
import io.confluent.kafka.streams.integration.avro.SensorReadingV1;
import io.confluent.kafka.streams.integration.avro.SensorReadingV2;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

/**
 * Shared setup for the header-based schema-evolution integration tests: a 1-broker cluster with an
 * embedded Schema Registry, plus helpers for topics, producers, Streams apps and store polling.
 */
public abstract class SchemaEvolutionIntegrationTestBase extends ClusterTestHarness {

  protected SchemaEvolutionIntegrationTestBase() {
    super(1, true);
  }

  // --- Key schemas ---
  protected static final Schema KEY_SCHEMA_V1 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"}"
          + "]"
          + "}");

  protected static final Schema KEY_SCHEMA_V2 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"},"
          + "  {\"name\":\"region\",\"type\":\"string\",\"default\":\"us-east\"}"
          + "]"
          + "}");

  // Differs from KEY_SCHEMA_V1 only in `doc`. Avro does not write `doc` to the binary,
  // so the same logical key serializes to identical bytes under either schema.
  protected static final Schema KEY_SCHEMA_V1_DOC_CHANGED = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"doc\":\"Updated documentation for sensor key\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"}"
          + "]"
          + "}");

  // --- Value schemas ---
  protected static final Schema VALUE_SCHEMA_V1 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"}"
          + "]"
          + "}");

  protected static final Schema VALUE_SCHEMA_V2 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"},"
          + "  {\"name\":\"humidity\",\"type\":\"double\",\"default\":0.0}"
          + "]"
          + "}");

  // v3: adds `pressure` with default — backward-compatible with v2.
  protected static final Schema VALUE_SCHEMA_V3 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"},"
          + "  {\"name\":\"humidity\",\"type\":\"double\",\"default\":0.0},"
          + "  {\"name\":\"pressure\",\"type\":\"double\",\"default\":1013.0}"
          + "]"
          + "}");

  protected GenericAvroSerde createKeySerde() {
    GenericAvroSerde serde = new GenericAvroSerde();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    serde.configure(config, true);
    return serde;
  }

  protected GenericAvroSerde createValueSerde() {
    GenericAvroSerde serde = new GenericAvroSerde();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    serde.configure(config, false);
    return serde;
  }

  protected KafkaProducer<GenericRecord, GenericRecord> createHeaderProducer() {
    return new KafkaProducer<>(baseProducerProps());
  }

  protected SpecificAvroSerde<SensorKey> createSpecificKeySerde() {
    SpecificAvroSerde<SensorKey> serde = new SpecificAvroSerde<>();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    serde.configure(config, true);
    return serde;
  }

  protected SpecificAvroSerde<SensorReadingV2> createSpecificValueSerde() {
    SpecificAvroSerde<SensorReadingV2> serde = new SpecificAvroSerde<>();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    // Pin the reader class. Without this, KafkaAvroDeserializer picks the Java
    // class by the writer schema's full name, so v1 bytes would deserialize to
    // SensorReadingV1 regardless of the serde's type parameter.
    config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG,
        SensorReadingV2.class.getName());
    serde.configure(config, false);
    return serde;
  }

  /** Producer that can only send v1 records. */
  protected KafkaProducer<SensorKey, SensorReadingV1> createV1Producer() {
    return new KafkaProducer<>(baseProducerProps());
  }

  /** Producer that can only send v2 records. */
  protected KafkaProducer<SensorKey, SensorReadingV2> createV2Producer() {
    return new KafkaProducer<>(baseProducerProps());
  }

  protected void createTopics(String... topics) throws Exception {
    Properties adminProps = new Properties();
    adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient admin = AdminClient.create(adminProps)) {
      admin
          .createTopics(
              Arrays.stream(topics)
                  .map(t -> new NewTopic(t, 1, (short) 1))
                  .collect(Collectors.toList()))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
  }

  /** Producer props that write the key and value schema IDs into record headers. */
  protected Properties baseProducerProps() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    props.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    return props;
  }

  protected KafkaStreams startStreams(StreamsBuilder builder, String appId) throws Exception {
    return startStreams(builder, appId, null);
  }

  /**
   * Starts the Streams app under {@code appId}. If {@code stateDir} is non-null it is used as the
   * state directory, so tests can wipe it to force a changelog restore.
   */
  protected KafkaStreams startStreams(StreamsBuilder builder, String appId, Path stateDir)
      throws Exception {
    return startStreams(builder, appId, stateDir, null);
  }

  /** Same as above, with a listener attached before start to observe changelog restoration. */
  protected KafkaStreams startStreams(StreamsBuilder builder, String appId, Path stateDir,
      StateRestoreListener restoreListener) throws Exception {
    Properties streamsProps = new Properties();
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    streamsProps.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    if (stateDir != null) {
      streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    }

    CountDownLatch startedLatch = new CountDownLatch(1);
    KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
    if (restoreListener != null) {
      streams.setGlobalStateRestoreListener(restoreListener);
    }
    streams.setStateListener(
        (newState, oldState) -> {
          if (newState == KafkaStreams.State.RUNNING) {
            startedLatch.countDown();
          }
        });
    streams.start();
    assertTrue(
        startedLatch.await(60, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");
    return streams;
  }

  protected static <K, V> int countStoreEntries(
      ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> store) {
    int count = 0;
    try (KeyValueIterator<K, ValueTimestampHeaders<V>> iter = store.all()) {
      while (iter.hasNext()) {
        iter.next();
        count++;
      }
    }
    return count;
  }

  protected static <K, V> void waitForStoreEntryCount(
      ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> store, int expectedCount)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      if (countStoreEntries(store) == expectedCount) {
        return;
      }
      Thread.sleep(200);
    }
    throw new AssertionError(
        "Store did not reach " + expectedCount + " entries within timeout; had "
            + countStoreEntries(store));
  }

  protected void waitForStoreToContainKeys(KafkaStreams streams, String storeName,
      int expectedCount) throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      try {
        ReadOnlyKeyValueStore<Object, ValueTimestampHeaders<Object>> store =
            streams.store(StoreQueryParameters.fromNameAndType(
                storeName, new TimestampedKeyValueStoreWithHeadersType<>()));
        if (countStoreEntries(store) >= expectedCount) {
          return;
        }
      } catch (Exception e) {
        // Store may not be ready yet
      }
      Thread.sleep(200);
    }
    throw new AssertionError(
        "Store did not contain " + expectedCount + " entries within timeout");
  }

  protected static void awaitCondition(BooleanSupplier condition, String description)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 30_000;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) {
        throw new AssertionError("Timed out waiting for " + description);
      }
      Thread.sleep(200);
    }
  }

  protected static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  protected void assertSchemaIdHeaders(Headers headers, String topic, String context) {
    assertGuidHeaderRegistered(headers, SchemaId.KEY_SCHEMA_ID_HEADER, topic + "-key",
        context + " key");
    assertGuidHeaderRegistered(headers, SchemaId.VALUE_SCHEMA_ID_HEADER, topic + "-value",
        context + " value");
  }

  protected void assertGuidHeaderRegistered(Headers headers, String headerName, String subject,
      String context) {
    Header header = headers.lastHeader(headerName);
    assertNotNull(header, context + ": should have " + headerName + " header");
    byte[] bytes = header.value();
    assertEquals(17, bytes.length, context + ": GUID header should be 17 bytes");
    assertEquals(SchemaId.MAGIC_BYTE_V1, bytes[0], context + ": header should have V1 magic byte");

    ByteBuffer bb = ByteBuffer.wrap(bytes, 1, 16);
    String headerGuid = new UUID(bb.getLong(), bb.getLong()).toString();

    // Older records keep their schema version's GUID, so check every version of the subject.
    Set<String> registeredGuids = new HashSet<>();
    try {
      for (int version : restApp.restClient.getAllVersions(subject)) {
        registeredGuids.add(restApp.restClient.getVersion(subject, version).getGuid());
      }
    } catch (Exception e) {
      fail(context + ": failed to look up subject " + subject + " in Schema Registry: "
          + e.getMessage());
    }
    assertTrue(registeredGuids.contains(headerGuid),
        context + ": header GUID " + headerGuid + " is not registered under subject " + subject
            + " (registered: " + registeredGuids + ")");
  }

  /** Counts the records restored from the changelog, to tell a restore from a reprocess. */
  protected static class CountingRestoreListener implements StateRestoreListener {

    private final AtomicLong restored = new AtomicLong();

    long restoredRecords() {
      return restored.get();
    }

    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset,
        long endingOffset) {
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName,
        long batchEndOffset, long numRestored) {
      restored.addAndGet(numRestored);
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
    }
  }

  protected static class TimestampedKeyValueStoreWithHeadersType<K, V>
      implements QueryableStoreType<ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>>> {

    @Override
    public boolean accepts(final StateStore stateStore) {
      return stateStore instanceof TimestampedKeyValueStoreWithHeaders
          && stateStore instanceof ReadOnlyKeyValueStore;
    }

    @Override
    public ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> create(
        final StateStoreProvider storeProvider, final String storeName) {
      return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
    }
  }
}
