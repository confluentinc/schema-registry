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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.integration.avro.SensorKey;
import io.confluent.kafka.streams.integration.avro.SensorReadingV1;
import io.confluent.kafka.streams.integration.avro.SensorReadingV2;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for key and value schema evolution on a header-aware
 * {@link TimestampedWindowStoreWithHeaders}. A processor writes each input record into the window
 * store, and the tests read it back with point fetches and {@code all()}.
 */
public class KafkaStreamsHeaderWindowStoreSchemaEvolutionIntegrationTest
    extends SchemaEvolutionIntegrationTestBase {

  private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
  private static final Duration RETENTION_PERIOD = Duration.ofHours(1);
  private static final String STORE_NAME = "window-schema-evolution-store";
  private static final String SPECIFIC_STORE_NAME = "window-specific-record-evolution-store";

  // Record timestamps, one per 5-minute window; fetch() takes the window start.
  private static final long WINDOW_0 = 0L;
  private static final long WINDOW_5 = 300_000L;
  private static final long WINDOW_10 = 600_000L;

  // Tests wait on this instead of querying mid-processing, which can corrupt the stored headers.
  private final AtomicInteger processed = new AtomicInteger();

  /**
   * Value schema evolves v1 to v2 to v3 across windows. Each window keeps the shape it was written
   * with, and a later write to the same key and window replaces the earlier value.
   */
  @Test
  public void shouldReadOldAndNewValuesAfterValueSchemaEvolution() throws Exception {
    String inputTopic = "window-value-evolution-input";
    String appId = "window-value-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startWindowApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord key1 = sensorKey("sensor-1");
      GenericRecord key2 = sensorKey("sensor-2");
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, key1, valueV1(35.5, 1000L));
        send(producer, inputTopic, WINDOW_0, key2, valueV1(22.0, 2000L));
      }
      awaitProcessed(2);
      ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          windowStore(streams, STORE_NAME);

      // Registering v2 without writing it changes nothing in the store.
      restApp.restClient.registerSchema(VALUE_SCHEMA_V2.toString(), inputTopic + "-value");
      ValueTimestampHeaders<GenericRecord> v1Entry = store.fetch(key1, WINDOW_0);
      assertEquals(35.5, v1Entry.value().get("temperature"));
      assertThrows(AvroRuntimeException.class, () -> v1Entry.value().get("humidity"),
          "entry is still v1 bytes, so humidity should not be present");
      assertSchemaIdHeaders(v1Entry.headers(), inputTopic, "key1 window 0 v1");

      // Re-read the same v1 bytes off the input topic with the v2 reader schema; Avro schema
      // resolution should fill humidity with the v2 default.
      assertV1BytesReadAsV2(inputTopic, 2);

      // v2 write in a new window, and a v2 overwrite of an existing key and window.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_5, key1, valueV2(36.0, 3000L, 65.0));
        send(producer, inputTopic, WINDOW_0, key2, valueV2(24.0, 2500L, 50.0));
      }
      awaitProcessed(4);

      ValueTimestampHeaders<GenericRecord> stillV1 = store.fetch(key1, WINDOW_0);
      assertEquals(35.5, stillV1.value().get("temperature"));
      assertThrows(AvroRuntimeException.class, () -> stillV1.value().get("humidity"));
      ValueTimestampHeaders<GenericRecord> v2NewWindow = store.fetch(key1, WINDOW_5);
      assertEquals(65.0, v2NewWindow.value().get("humidity"));
      assertSchemaIdHeaders(v2NewWindow.headers(), inputTopic, "key1 window 5 v2");
      ValueTimestampHeaders<GenericRecord> v2Overwrite = store.fetch(key2, WINDOW_0);
      assertEquals(50.0, v2Overwrite.value().get("humidity"));
      assertSchemaIdHeaders(v2Overwrite.headers(), inputTopic, "key2 window 0 v2");

      // v3 write adds pressure.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_10, key1,
            new GenericRecordBuilder(VALUE_SCHEMA_V3).set("temperature", 18.0)
                .set("timestamp", 5000L).set("humidity", 55.0).set("pressure", 1020.0).build());
      }
      awaitProcessed(5);
      ValueTimestampHeaders<GenericRecord> v3 = store.fetch(key1, WINDOW_10);
      assertEquals(1020.0, v3.value().get("pressure"));
      assertSchemaIdHeaders(v3.headers(), inputTopic, "key1 window 10 v3");
      assertThrows(AvroRuntimeException.class,
          () -> store.fetch(key1, WINDOW_5).value().get("pressure"),
          "the v2 entry should not gain a pressure field");

      assertEquals(4, countEntries(store), "key1 in 3 windows plus key2 in window 0");
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * The same logical key under an evolved key schema serializes to different bytes, so the same
   * window holds two separate rows.
   */
  @Test
  public void shouldStoreSameLogicalKeyAsTwoRowsAfterKeySchemaEvolution() throws Exception {
    String inputTopic = "window-key-evolution-input";
    String appId = "window-key-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startWindowApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord keyV1 = sensorKey("sensor-1");
      GenericRecord keyV2 = new GenericRecordBuilder(KEY_SCHEMA_V2)
          .set("sensorId", "sensor-1").set("region", "us-east").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyV1, valueV1(35.5, 1000L));
      }
      awaitProcessed(1);
      ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          windowStore(streams, STORE_NAME);

      // Registering key v2 without writing it leaves the v1 row readable.
      restApp.restClient.registerSchema(KEY_SCHEMA_V2.toString(), inputTopic + "-key");
      assertEquals(35.5, store.fetch(keyV1, WINDOW_0).value().get("temperature"));
      assertNull(store.fetch(keyV2, WINDOW_0), "no row exists yet for the v2 key bytes");

      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyV2, valueV1(40.0, 2000L));
      }
      awaitProcessed(2);

      ValueTimestampHeaders<GenericRecord> rowV1 = store.fetch(keyV1, WINDOW_0);
      ValueTimestampHeaders<GenericRecord> rowV2 = store.fetch(keyV2, WINDOW_0);
      assertEquals(35.5, rowV1.value().get("temperature"));
      assertEquals(40.0, rowV2.value().get("temperature"));
      assertSchemaIdHeaders(rowV1.headers(), inputTopic, "v1 key row");
      assertSchemaIdHeaders(rowV2.headers(), inputTopic, "v2 key row");
      assertEquals(2, countEntries(store), "v1 and v2 keys should be separate rows");
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * Key schemas that differ only in metadata serialize to identical bytes, so reads and writes
   * under either schema address the same row.
   */
  @Test
  public void shouldShareRowWhenKeySchemasProduceIdenticalBytes() throws Exception {
    String inputTopic = "window-doc-only-evolution-input";
    String appId = "window-doc-only-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startWindowApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord keyV1 = sensorKey("sensor-1");
      GenericRecord keyDocChanged = new GenericRecordBuilder(KEY_SCHEMA_V1_DOC_CHANGED)
          .set("sensorId", "sensor-1").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyV1, valueV1(35.5, 1000L));
      }
      awaitProcessed(1);
      ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          windowStore(streams, STORE_NAME);

      ValueTimestampHeaders<GenericRecord> viaDocChanged = store.fetch(keyDocChanged, WINDOW_0);
      assertNotNull(viaDocChanged, "doc-changed key should find the v1 row");
      assertEquals(35.5, viaDocChanged.value().get("temperature"));
      assertSchemaIdHeaders(viaDocChanged.headers(), inputTopic, "doc-changed lookup");

      // A write under the doc-changed schema replaces the row rather than adding one.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyDocChanged, valueV1(40.0, 2000L));
      }
      awaitProcessed(2);
      assertEquals(40.0, store.fetch(keyDocChanged, WINDOW_0).value().get("temperature"));
      assertEquals(1, countEntries(store));
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * A tombstone deletes only the row whose key bytes match. A key with a different schema and
   * different bytes is untouched, and a doc-only schema change still matches.
   */
  @Test
  public void shouldDeleteOnlyMatchingByteKeyRowOnTombstone() throws Exception {
    String inputTopic = "window-tombstone-evolution-input";
    String appId = "window-tombstone-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startWindowApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord keyV1 = sensorKey("sensor-1");
      GenericRecord keyV2 = new GenericRecordBuilder(KEY_SCHEMA_V2)
          .set("sensorId", "sensor-1").set("region", "us-east").build();
      GenericRecord key2V1 = sensorKey("sensor-2");
      GenericRecord key2DocChanged = new GenericRecordBuilder(KEY_SCHEMA_V1_DOC_CHANGED)
          .set("sensorId", "sensor-2").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyV1, valueV1(35.5, 1000L));
        send(producer, inputTopic, WINDOW_0, key2V1, valueV1(37.0, 2000L));
        send(producer, inputTopic, WINDOW_0, keyV2, valueV1(40.0, 3000L));
      }
      awaitProcessed(3);
      ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          windowStore(streams, STORE_NAME);
      assertEquals(3, countEntries(store));

      // Tombstone with the v1 key removes only the v1 row, not the v2 row.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyV1, null);
      }
      awaitProcessed(4);
      assertNull(store.fetch(keyV1, WINDOW_0));
      ValueTimestampHeaders<GenericRecord> survivingV2 = store.fetch(keyV2, WINDOW_0);
      assertNotNull(survivingV2, "the v2 key row has different bytes and should remain");
      assertEquals(40.0, survivingV2.value().get("temperature"));
      assertSchemaIdHeaders(survivingV2.headers(), inputTopic, "surviving v2 key row");
      assertNotNull(store.fetch(key2V1, WINDOW_0));
      assertEquals(2, countEntries(store));

      // Tombstone under the doc-changed schema has identical bytes, so it deletes sensor-2.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, key2DocChanged, null);
      }
      awaitProcessed(5);
      assertNull(store.fetch(key2V1, WINDOW_0));
      assertNotNull(store.fetch(keyV2, WINDOW_0), "the v2 key row should be the one left");
      assertEquals(1, countEntries(store));
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * After the local state is wiped, the store is rebuilt from the changelog and the schema-id
   * headers survive the restore.
   */
  @Test
  public void shouldRestoreWindowStoreFromChangelogPreservingHeaderSchemaIds() throws Exception {
    String inputTopic = "window-restore-evolution-input";
    String appId = "window-restore-evolution-test-" + System.currentTimeMillis();
    Path stateDir = Files.createTempDirectory("kstreams-window-evolution-");
    createTopics(inputTopic);

    // Wall-clock windows: on restore the observed stream time becomes the current time, so
    // windows near the epoch would be dropped as expired by the 1h retention.
    long window0 = (System.currentTimeMillis() / WINDOW_SIZE.toMillis()) * WINDOW_SIZE.toMillis();
    long window1 = window0 + WINDOW_SIZE.toMillis();
    GenericRecord key1 = sensorKey("sensor-1");
    GenericRecord key2 = sensorKey("sensor-2");
    KafkaStreams streams = startWindowApp(inputTopic, appId, STORE_NAME,
        createKeySerde(), createValueSerde(), stateDir);
    try {
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, window0, key1, valueV1(35.5, 1000L));
        send(producer, inputTopic, window1, key2, valueV2(28.0, 4000L, 70.0));
      }
      awaitProcessed(2);
    } finally {
      streams.close(Duration.ofSeconds(10));
    }

    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);

    streams = startWindowApp(inputTopic, appId, STORE_NAME,
        createKeySerde(), createValueSerde(), stateDir);
    try {
      ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          awaitWindowEntry(streams, STORE_NAME, key1, window0);
      awaitWindowEntry(streams, STORE_NAME, key2, window1);

      ValueTimestampHeaders<GenericRecord> restored1 = store.fetch(key1, window0);
      assertEquals(35.5, restored1.value().get("temperature"));
      assertSchemaIdHeaders(restored1.headers(), inputTopic, "restored key1");
      ValueTimestampHeaders<GenericRecord> restored2 = store.fetch(key2, window1);
      assertEquals(70.0, restored2.value().get("humidity"));
      assertSchemaIdHeaders(restored2.headers(), inputTopic, "restored key2");
      assertEquals(2, countEntries(store));
      assertEquals(2, processed.get(), "the store should come from the changelog, not reprocessing");
    } finally {
      streams.close(Duration.ofSeconds(10));
      deleteRecursively(stateDir);
    }
  }

  /**
   * Null for a non-nullable field fails Avro serialization (key and value), and so does a field
   * left unset on a raw record. A field omitted through the record builder falls back to the
   * schema default.
   */
  @Test
  public void shouldRejectExplicitNullsAndDefaultOmittedFields() throws Exception {
    String inputTopic = "window-null-default-evolution-input";
    String appId = "window-null-default-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startWindowApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord key1 = sensorKey("sensor-1");
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, key1, valueV1(35.5, 1000L));
      }
      awaitProcessed(1);

      GenericRecord nullRegionKey = new GenericData.Record(KEY_SCHEMA_V2);
      nullRegionKey.put("sensorId", "sensor-2");
      nullRegionKey.put("region", null);
      GenericRecord nullTemperature = new GenericData.Record(VALUE_SCHEMA_V1);
      nullTemperature.put("temperature", null);
      nullTemperature.put("timestamp", 1500L);
      GenericRecord unsetHumidity = new GenericData.Record(VALUE_SCHEMA_V2);
      unsetHumidity.put("temperature", 30.0);
      unsetHumidity.put("timestamp", 1600L);
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        assertThrows(SerializationException.class, () -> producer.send(
                new ProducerRecord<>(inputTopic, nullRegionKey, valueV1(41.0, 2100L))),
            "null for a non-nullable key field should fail Avro serialization");
        assertThrows(SerializationException.class, () -> producer.send(
                new ProducerRecord<>(inputTopic, key1, nullTemperature)),
            "null for a non-nullable value field should fail Avro serialization");
        assertThrows(SerializationException.class, () -> producer.send(
                new ProducerRecord<>(inputTopic, key1, unsetHumidity)),
            "a raw record with an unset field does not get the default and should fail");
      }

      // The rejected sends never reached the topic, so the store is unchanged.
      ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          windowStore(streams, STORE_NAME);
      assertEquals(35.5, store.fetch(key1, WINDOW_0).value().get("temperature"));
      assertEquals(1, countEntries(store));

      // The record builder fills omitted fields with their defaults.
      GenericRecord withDefaults = new GenericRecordBuilder(VALUE_SCHEMA_V3)
          .set("temperature", 45.0).set("timestamp", 2500L).build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_5, key1, withDefaults);
      }
      awaitProcessed(2);
      ValueTimestampHeaders<GenericRecord> defaulted = store.fetch(key1, WINDOW_5);
      assertEquals(45.0, defaulted.value().get("temperature"));
      assertEquals(0.0, defaulted.value().get("humidity"));
      assertEquals(1013.0, defaulted.value().get("pressure"));
      assertSchemaIdHeaders(defaulted.headers(), inputTopic, "defaulted entry");
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * Reader upgraded to v2 while the writer still produces v1: the app reads v1 bytes into the v2
   * class and the new field takes its default.
   */
  @Test
  public void shouldReadV1WritesAfterReaderUpgrade() throws Exception {
    String inputTopic = "window-reader-upgrade-input";
    String appId = "window-reader-upgrade-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      try (KafkaProducer<SensorKey, SensorReadingV1> v1Producer = createV1Producer()) {
        send(v1Producer, inputTopic, WINDOW_0, new SensorKey("sensor-1"),
            SensorReadingV1.newBuilder().setTemperature(35.5).setTimestamp(1000L).build());
        send(v1Producer, inputTopic, WINDOW_5, new SensorKey("sensor-2"),
            SensorReadingV1.newBuilder().setTemperature(22.0).setTimestamp(2000L).build());
      }

      streams = startWindowApp(inputTopic, appId, SPECIFIC_STORE_NAME,
          createSpecificKeySerde(), createSpecificValueSerde(), null);
      SensorKey key1 = new SensorKey("sensor-1");
      SensorKey key2 = new SensorKey("sensor-2");
      awaitProcessed(2);
      ReadOnlyWindowStore<SensorKey, ValueTimestampHeaders<SensorReadingV2>> store =
          windowStore(streams, SPECIFIC_STORE_NAME);

      ValueTimestampHeaders<SensorReadingV2> r1 = store.fetch(key1, WINDOW_0);
      assertEquals(35.5, r1.value().getTemperature());
      assertEquals(0.0, r1.value().getHumidity(),
          "humidity should fall back to the v2 default for v1-written bytes");
      assertSpecificSchemaIdHeaders(r1.headers(), inputTopic, appId, "sensor-1");
      ValueTimestampHeaders<SensorReadingV2> r2 = store.fetch(key2, WINDOW_5);
      assertEquals(22.0, r2.value().getTemperature());
      assertEquals(0.0, r2.value().getHumidity());
      assertSpecificSchemaIdHeaders(r2.headers(), inputTopic, appId, "sensor-2");
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * Rolling upgrade with the reader already on v2: a v1 writer, then a v2 writer, then an old v1
   * writer that has not upgraded yet. The local state is wiped before each restart so the store
   * is rebuilt from the changelog.
   */
  @Test
  public void shouldReadBothV1AndV2WritesDuringWriterRollout() throws Exception {
    String inputTopic = "window-writer-upgrade-input";
    String appId = "window-writer-upgrade-test-" + System.currentTimeMillis();
    Path stateDir = Files.createTempDirectory("kstreams-window-specific-");
    createTopics(inputTopic);

    // Wall-clock window: windows near the epoch are dropped as expired on restore.
    long window = (System.currentTimeMillis() / WINDOW_SIZE.toMillis()) * WINDOW_SIZE.toMillis();
    SensorKey key1 = new SensorKey("sensor-1");
    SensorKey key2 = new SensorKey("sensor-2");
    SensorKey key3 = new SensorKey("sensor-3");
    SensorKey key4 = new SensorKey("sensor-4");

    // Step 1: v1 writer, v2 reader.
    try (KafkaProducer<SensorKey, SensorReadingV1> v1Producer = createV1Producer()) {
      send(v1Producer, inputTopic, window, key1,
          SensorReadingV1.newBuilder().setTemperature(35.5).setTimestamp(1000L).build());
      send(v1Producer, inputTopic, window, key2,
          SensorReadingV1.newBuilder().setTemperature(22.0).setTimestamp(2000L).build());
    }
    KafkaStreams streams = startWindowApp(inputTopic, appId, SPECIFIC_STORE_NAME,
        createSpecificKeySerde(), createSpecificValueSerde(), stateDir);
    try {
      awaitProcessed(2);
      ReadOnlyWindowStore<SensorKey, ValueTimestampHeaders<SensorReadingV2>> store =
          windowStore(streams, SPECIFIC_STORE_NAME);
      // The v1 bytes are read through the v2 class, so the new field takes its default.
      assertSensor(store, key1, window, 35.5, 0.0, inputTopic, appId);
      assertSensor(store, key2, window, 22.0, 0.0, inputTopic, appId);
    } finally {
      streams.close(Duration.ofSeconds(10));
    }

    // Step 2: wipe state, the writer upgrades to v2.
    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);
    try (KafkaProducer<SensorKey, SensorReadingV2> v2Producer = createV2Producer()) {
      send(v2Producer, inputTopic, window, key1, SensorReadingV2.newBuilder()
          .setTemperature(36.0).setTimestamp(3000L).setHumidity(65.0).build());
      send(v2Producer, inputTopic, window, key3, SensorReadingV2.newBuilder()
          .setTemperature(28.0).setTimestamp(4000L).setHumidity(70.0).build());
    }
    streams = startWindowApp(inputTopic, appId, SPECIFIC_STORE_NAME,
        createSpecificKeySerde(), createSpecificValueSerde(), stateDir);
    try {
      awaitProcessed(4);
      ReadOnlyWindowStore<SensorKey, ValueTimestampHeaders<SensorReadingV2>> store =
          windowStore(streams, SPECIFIC_STORE_NAME);

      // sensor-1 was overwritten by v2, sensor-2 was restored from the changelog as v1.
      assertSensor(store, key1, window, 36.0, 65.0, inputTopic, appId);
      assertSensor(store, key2, window, 22.0, 0.0, inputTopic, appId);
      assertSensor(store, key3, window, 28.0, 70.0, inputTopic, appId);
      assertEquals(3, countEntries(store));
      assertEquals(4, processed.get(), "sensor-2 should be restored, not reprocessed");
    } finally {
      streams.close(Duration.ofSeconds(10));
    }

    // Step 3: wipe state again, an old v1 producer that has not upgraded writes after the rollout.
    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);
    try (KafkaProducer<SensorKey, SensorReadingV1> v1Producer = createV1Producer()) {
      send(v1Producer, inputTopic, window, key3,
          SensorReadingV1.newBuilder().setTemperature(40.0).setTimestamp(4500L).build());
      send(v1Producer, inputTopic, window, key4,
          SensorReadingV1.newBuilder().setTemperature(45.0).setTimestamp(5000L).build());
    }
    streams = startWindowApp(inputTopic, appId, SPECIFIC_STORE_NAME,
        createSpecificKeySerde(), createSpecificValueSerde(), stateDir);
    try {
      awaitProcessed(6);
      ReadOnlyWindowStore<SensorKey, ValueTimestampHeaders<SensorReadingV2>> store =
          windowStore(streams, SPECIFIC_STORE_NAME);

      assertSensor(store, key1, window, 36.0, 65.0, inputTopic, appId);
      assertSensor(store, key2, window, 22.0, 0.0, inputTopic, appId);
      // sensor-3's v2 value was overwritten by the old v1 writer, so humidity is the default again.
      assertSensor(store, key3, window, 40.0, 0.0, inputTopic, appId);
      assertSensor(store, key4, window, 45.0, 0.0, inputTopic, appId);
      assertEquals(4, countEntries(store));
      assertEquals(6, processed.get(), "earlier sensors should be restored, not reprocessed");
    } finally {
      streams.close(Duration.ofSeconds(10));
      deleteRecursively(stateDir);
    }
  }

  private void assertSensor(
      ReadOnlyWindowStore<SensorKey, ValueTimestampHeaders<SensorReadingV2>> store,
      SensorKey key, long windowStart, double temperature, double humidity, String inputTopic,
      String appId) {
    ValueTimestampHeaders<SensorReadingV2> entry = store.fetch(key, windowStart);
    assertNotNull(entry, key + " should be in the store");
    assertEquals(temperature, entry.value().getTemperature(), key + " temperature");
    assertEquals(humidity, entry.value().getHumidity(), key + " humidity");
    assertSpecificSchemaIdHeaders(entry.headers(), inputTopic, appId, key.toString());
  }

  // The store re-serializes values as SensorReadingV2, so the value GUID is registered under the
  // changelog subject, not the input topic's.
  private void assertSpecificSchemaIdHeaders(Headers headers, String inputTopic, String appId,
      String context) {
    assertSchemaIdHeaders(headers, inputTopic + "-key",
        appId + "-" + SPECIFIC_STORE_NAME + "-changelog-value", context);
  }

  private <K, V> KafkaStreams startWindowApp(String inputTopic, String appId, String storeName,
      Serde<K> keySerde, Serde<V> valueSerde, Path stateDir) throws Exception {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .addStateStore(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(
                    storeName, RETENTION_PERIOD, WINDOW_SIZE, false),
                keySerde,
                valueSerde))
        .stream(inputTopic, Consumed.with(keySerde, valueSerde))
        .process(() -> new PutProcessor<K, V>(storeName, processed), storeName);
    return startStreams(builder, appId, stateDir);
  }

  /** Reads the v1-written input records and decodes them with the v2 schema as the reader. */
  private void assertV1BytesReadAsV2(String topic, int count) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "v2-reader-" + System.currentTimeMillis());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    List<ConsumerRecord<byte[], byte[]>> raw = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      long end = System.currentTimeMillis() + 15_000;
      while (raw.size() < count && System.currentTimeMillis() < end) {
        consumer.poll(Duration.ofMillis(500)).forEach(raw::add);
      }
    }
    assertEquals(count, raw.size(), "should have consumed the v1-written input records");

    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    try (KafkaAvroDeserializer v2Reader = new KafkaAvroDeserializer()) {
      v2Reader.configure(config, false);
      for (ConsumerRecord<byte[], byte[]> r : raw) {
        GenericRecord asV2 = (GenericRecord) v2Reader.deserialize(
            topic, r.headers(), r.value(), VALUE_SCHEMA_V2);
        assertNotNull(asV2, "v1 bytes should be decodable with the v2 reader schema");
        assertEquals(VALUE_SCHEMA_V2, asV2.getSchema(), "projection should be v2-shaped");
        assertEquals(0.0, asV2.get("humidity"),
            "humidity should be filled in from the v2 default when reading v1 bytes as v2");
      }
    }
  }

  private void awaitProcessed(int expected) throws InterruptedException {
    awaitCondition(() -> processed.get() >= expected, expected + " records to be processed");
  }

  /** Polls until the entry is visible, then returns the queryable store. */
  private <K, V> ReadOnlyWindowStore<K, ValueTimestampHeaders<V>> awaitWindowEntry(
      KafkaStreams streams, String storeName, K key, long windowStart)
      throws InterruptedException {
    AtomicReference<Exception> lastError = new AtomicReference<>();
    try {
      awaitCondition(() -> {
        try {
          return this.<K, V>windowStore(streams, storeName).fetch(key, windowStart) != null;
        } catch (Exception e) {
          lastError.set(e);
          return false;
        }
      }, "window entry for " + key + " at " + windowStart);
    } catch (AssertionError e) {
      throw new AssertionError(e.getMessage() + "; last query error: " + lastError.get(), e);
    }
    return windowStore(streams, storeName);
  }

  private <K, V> ReadOnlyWindowStore<K, ValueTimestampHeaders<V>> windowStore(
      KafkaStreams streams, String storeName) {
    return streams.store(StoreQueryParameters.fromNameAndType(
        storeName, new TimestampedWindowStoreWithHeadersType<K, V>()));
  }

  private static <K, V> int countEntries(ReadOnlyWindowStore<K, ValueTimestampHeaders<V>> store) {
    int count = 0;
    try (KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> iter = store.all()) {
      while (iter.hasNext()) {
        iter.next();
        count++;
      }
    }
    return count;
  }

  private static <K, V> void send(KafkaProducer<K, V> producer, String topic, long timestamp,
      K key, V value) throws Exception {
    producer.send(new ProducerRecord<>(topic, null, timestamp, key, value)).get();
    producer.flush();
  }

  private static void closeQuietly(KafkaStreams streams) {
    if (streams != null) {
      streams.close(Duration.ofSeconds(10));
    }
  }

  private static GenericRecord sensorKey(String sensorId) {
    return new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", sensorId).build();
  }

  private static GenericRecord valueV1(double temperature, long timestamp) {
    return new GenericRecordBuilder(VALUE_SCHEMA_V1)
        .set("temperature", temperature).set("timestamp", timestamp).build();
  }

  private static GenericRecord valueV2(double temperature, long timestamp, double humidity) {
    return new GenericRecordBuilder(VALUE_SCHEMA_V2)
        .set("temperature", temperature).set("timestamp", timestamp)
        .set("humidity", humidity).build();
  }

  /** Writes every input record into the window store, or deletes the row on a null value. */
  private static class PutProcessor<K, V> implements Processor<K, V, Void, Void> {

    private final String storeName;
    private final AtomicInteger processed;
    private TimestampedWindowStoreWithHeaders<K, V> store;

    PutProcessor(String storeName, AtomicInteger processed) {
      this.storeName = storeName;
      this.processed = processed;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
      this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<K, V> record) {
      long windowStart = (record.timestamp() / WINDOW_SIZE.toMillis()) * WINDOW_SIZE.toMillis();
      if (record.value() == null) {
        store.put(record.key(), null, windowStart);
      } else {
        store.put(record.key(),
            ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()),
            windowStart);
      }
      processed.incrementAndGet();
    }
  }

  private static class TimestampedWindowStoreWithHeadersType<K, V>
      implements QueryableStoreType<ReadOnlyWindowStore<K, ValueTimestampHeaders<V>>> {

    @Override
    public boolean accepts(final StateStore stateStore) {
      return stateStore instanceof TimestampedWindowStoreWithHeaders
          && stateStore instanceof ReadOnlyWindowStore;
    }

    @Override
    public ReadOnlyWindowStore<K, ValueTimestampHeaders<V>> create(
        final StateStoreProvider storeProvider, final String storeName) {
      return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
    }
  }
}
