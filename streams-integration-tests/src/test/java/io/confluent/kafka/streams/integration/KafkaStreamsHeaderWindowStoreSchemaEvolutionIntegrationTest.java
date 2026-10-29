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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.streams.integration.avro.SensorKey;
import io.confluent.kafka.streams.integration.avro.SensorReadingV1;
import io.confluent.kafka.streams.integration.avro.SensorReadingV2;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
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
import org.apache.kafka.streams.state.WindowStoreIterator;
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

      // v2 write in a new window, and a v2 overwrite of an existing key and window.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_5, key1, valueV2(36.0, 3000L, 65.0));
        send(producer, inputTopic, WINDOW_0, key2, valueV2(24.0, 2500L, 50.0));
      }
      awaitProcessed(4);

      // v3 write adds pressure.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_10, key1,
            new GenericRecordBuilder(VALUE_SCHEMA_V3).set("temperature", 18.0)
                .set("timestamp", 5000L).set("humidity", 55.0).set("pressure", 1020.0).build());
      }
      awaitProcessed(5);

      // One time-range fetch returns the three schema versions of key1 through a single iterator.
      try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter = store.fetch(
          key1, Instant.ofEpochMilli(WINDOW_0), Instant.ofEpochMilli(WINDOW_10))) {
        KeyValue<Long, ValueTimestampHeaders<GenericRecord>> first = iter.next();
        assertEquals(WINDOW_0, first.key);
        assertEquals(35.5, first.value.value().get("temperature"));
        assertThrows(AvroRuntimeException.class, () -> first.value.value().get("humidity"),
            "a v1-written value does not have humidity");
        assertSchemaIdHeaders(first.value.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V1,
            "key1 window 0 v1");

        KeyValue<Long, ValueTimestampHeaders<GenericRecord>> second = iter.next();
        assertEquals(WINDOW_5, second.key);
        assertEquals(36.0, second.value.value().get("temperature"));
        assertEquals(65.0, second.value.value().get("humidity"));
        assertThrows(AvroRuntimeException.class, () -> second.value.value().get("pressure"),
            "a v2-written value does not have pressure");
        assertSchemaIdHeaders(second.value.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V2,
            "key1 window 5 v2");

        KeyValue<Long, ValueTimestampHeaders<GenericRecord>> third = iter.next();
        assertEquals(WINDOW_10, third.key);
        assertEquals(18.0, third.value.value().get("temperature"));
        assertEquals(55.0, third.value.value().get("humidity"));
        assertEquals(1020.0, third.value.value().get("pressure"));
        assertSchemaIdHeaders(third.value.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V3,
            "key1 window 10 v3");
        assertFalse(iter.hasNext(), "key1 should have exactly three windows");
      }

      // key2's window 0 value was replaced by the v2 write.
      ValueTimestampHeaders<GenericRecord> overwritten = store.fetch(key2, WINDOW_0);
      assertEquals(24.0, overwritten.value().get("temperature"));
      assertEquals(50.0, overwritten.value().get("humidity"));
      assertSchemaIdHeaders(overwritten.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V2,
          "key2 window 0 v2");

      // all() returns every row; check each row's window, value and headers.
      Map<String, Double> expectedTemperature = new HashMap<>();
      Map<String, Schema> expectedValueSchema = new HashMap<>();
      expectedTemperature.put("sensor-1@" + WINDOW_0, 35.5);
      expectedValueSchema.put("sensor-1@" + WINDOW_0, VALUE_SCHEMA_V1);
      expectedTemperature.put("sensor-1@" + WINDOW_5, 36.0);
      expectedValueSchema.put("sensor-1@" + WINDOW_5, VALUE_SCHEMA_V2);
      expectedTemperature.put("sensor-1@" + WINDOW_10, 18.0);
      expectedValueSchema.put("sensor-1@" + WINDOW_10, VALUE_SCHEMA_V3);
      expectedTemperature.put("sensor-2@" + WINDOW_0, 24.0);
      expectedValueSchema.put("sensor-2@" + WINDOW_0, VALUE_SCHEMA_V2);
      int rows = 0;
      try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
               store.all()) {
        while (iter.hasNext()) {
          KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> row = iter.next();
          String id = row.key.key().get("sensorId") + "@" + row.key.window().start();
          assertTrue(expectedTemperature.containsKey(id), "unexpected row " + id);
          assertEquals(expectedTemperature.get(id), row.value.value().get("temperature"), id);
          assertSchemaIdHeaders(row.value.headers(), inputTopic, KEY_SCHEMA_V1,
              expectedValueSchema.get(id), id);
          rows++;
        }
      }
      assertEquals(4, rows, "key1 in 3 windows plus key2 in window 0");
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

      assertNull(store.fetch(keyV2, WINDOW_0), "no row exists yet for the v2 key bytes");

      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyV2, valueV1(40.0, 2000L));
      }
      awaitProcessed(2);

      ValueTimestampHeaders<GenericRecord> rowV1 = store.fetch(keyV1, WINDOW_0);
      ValueTimestampHeaders<GenericRecord> rowV2 = store.fetch(keyV2, WINDOW_0);
      assertEquals(35.5, rowV1.value().get("temperature"));
      assertEquals(40.0, rowV2.value().get("temperature"));
      assertSchemaIdHeaders(rowV1.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V1, "v1 key row");
      assertSchemaIdHeaders(rowV2.headers(), inputTopic, KEY_SCHEMA_V2, VALUE_SCHEMA_V1, "v2 key row");
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
      assertSchemaIdHeaders(viaDocChanged.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V1,
          "doc-changed lookup");

      // A write under the doc-changed schema replaces the row rather than adding one.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyDocChanged, valueV1(40.0, 2000L));
      }
      awaitProcessed(2);
      ValueTimestampHeaders<GenericRecord> replaced = store.fetch(keyDocChanged, WINDOW_0);
      assertEquals(40.0, replaced.value().get("temperature"));
      assertSchemaIdHeaders(replaced.headers(), inputTopic, KEY_SCHEMA_V1_DOC_CHANGED,
          VALUE_SCHEMA_V1, "doc-changed row");
      assertEquals(40.0, store.fetch(keyV1, WINDOW_0).value().get("temperature"));
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
        send(producer, inputTopic, WINDOW_5, keyV1, valueV1(36.0, 4000L));
      }
      awaitProcessed(4);
      ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          windowStore(streams, STORE_NAME);
      assertEquals(4, countEntries(store));

      // Tombstone with the v1 key in window 0 removes only that row: not the v2 key's row, and
      // not the same v1 key's row in window 5.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, keyV1, null);
      }
      awaitProcessed(5);
      assertNull(store.fetch(keyV1, WINDOW_0));
      ValueTimestampHeaders<GenericRecord> otherWindow = store.fetch(keyV1, WINDOW_5);
      assertNotNull(otherWindow, "the v1 key's row in window 5 should remain");
      assertEquals(36.0, otherWindow.value().get("temperature"));
      assertSchemaIdHeaders(otherWindow.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V1,
          "v1 key window 5");
      ValueTimestampHeaders<GenericRecord> survivingV2 = store.fetch(keyV2, WINDOW_0);
      assertNotNull(survivingV2, "the v2 key row has different bytes and should remain");
      assertEquals(40.0, survivingV2.value().get("temperature"));
      assertSchemaIdHeaders(survivingV2.headers(), inputTopic, KEY_SCHEMA_V2, VALUE_SCHEMA_V1,
          "surviving v2 key row");
      ValueTimestampHeaders<GenericRecord> key2Row = store.fetch(key2V1, WINDOW_0);
      assertEquals(37.0, key2Row.value().get("temperature"));
      assertSchemaIdHeaders(key2Row.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V1,
          "sensor-2 row");
      assertEquals(3, countEntries(store));

      // Tombstone under the doc-changed schema has identical bytes, so it deletes sensor-2.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, key2DocChanged, null);
      }
      awaitProcessed(6);
      assertNull(store.fetch(key2V1, WINDOW_0));
      assertNotNull(store.fetch(keyV2, WINDOW_0), "the v2 key row should remain");
      assertNotNull(store.fetch(keyV1, WINDOW_5), "the v1 key's window 5 row should remain");
      assertEquals(2, countEntries(store));
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

    // Windows near the epoch were not restored in an earlier run, so use windows aligned to now.
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
      assertTrue(streams.close(Duration.ofSeconds(10)), "the app should close before the restart");
    }

    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);

    CountingRestoreListener restoreListener = new CountingRestoreListener();
    streams = startWindowApp(inputTopic, appId, STORE_NAME,
        createKeySerde(), createValueSerde(), stateDir, restoreListener);
    try {
      ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          awaitWindowEntry(streams, STORE_NAME, key1, window0);
      awaitWindowEntry(streams, STORE_NAME, key2, window1);

      ValueTimestampHeaders<GenericRecord> restored1 = store.fetch(key1, window0);
      assertEquals(35.5, restored1.value().get("temperature"));
      assertSchemaIdHeaders(restored1.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V1,
          "restored key1");
      ValueTimestampHeaders<GenericRecord> restored2 = store.fetch(key2, window1);
      assertEquals(70.0, restored2.value().get("humidity"));
      assertSchemaIdHeaders(restored2.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V2,
          "restored key2");
      assertEquals(2, countEntries(store));
      assertEquals(2, restoreListener.restoredRecords(),
          "the 2 changelog records should be restored, not reprocessed from the input");
    } finally {
      streams.close(Duration.ofSeconds(10));
      deleteRecursively(stateDir);
    }
  }

  /**
   * Fields omitted when the record is built take their schema defaults, and the window store
   * returns the record with those defaults and the right schema-id headers.
   */
  @Test
  public void shouldStoreDefaultsForFieldsOmittedFromTheRecord() throws Exception {
    String inputTopic = "window-defaults-evolution-input";
    String appId = "window-defaults-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startWindowApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord key1 = sensorKey("sensor-1");
      GenericRecord withDefaults = new GenericRecordBuilder(VALUE_SCHEMA_V3)
          .set("temperature", 45.0).set("timestamp", 2500L).build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, WINDOW_0, key1, withDefaults);
      }
      awaitProcessed(1);

      ValueTimestampHeaders<GenericRecord> stored =
          this.<GenericRecord, GenericRecord>windowStore(streams, STORE_NAME).fetch(key1, WINDOW_0);
      assertNotNull(stored, "the record should be in the store");
      assertEquals(45.0, stored.value().get("temperature"));
      assertEquals(0.0, stored.value().get("humidity"));
      assertEquals(1013.0, stored.value().get("pressure"));
      assertSchemaIdHeaders(stored.headers(), inputTopic, KEY_SCHEMA_V1, VALUE_SCHEMA_V3,
          "record with omitted fields");
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

    // Windows near the epoch were not restored in an earlier run, so use a window aligned to now.
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
      assertTrue(streams.close(Duration.ofSeconds(10)), "the app should close before the restart");
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
    CountingRestoreListener restoredAfterStep1 = new CountingRestoreListener();
    streams = startWindowApp(inputTopic, appId, SPECIFIC_STORE_NAME,
        createSpecificKeySerde(), createSpecificValueSerde(), stateDir, restoredAfterStep1);
    try {
      awaitProcessed(4);
      ReadOnlyWindowStore<SensorKey, ValueTimestampHeaders<SensorReadingV2>> store =
          windowStore(streams, SPECIFIC_STORE_NAME);

      // sensor-1 was overwritten by v2, sensor-2 was restored from the changelog as v1.
      assertSensor(store, key1, window, 36.0, 65.0, inputTopic, appId);
      assertSensor(store, key2, window, 22.0, 0.0, inputTopic, appId);
      assertSensor(store, key3, window, 28.0, 70.0, inputTopic, appId);
      assertEquals(3, countEntries(store));
      assertEquals(2, restoredAfterStep1.restoredRecords(),
          "sensor-1 and sensor-2 should be restored from the changelog");
      assertEquals(4, processed.get(), "sensor-2 should be restored, not reprocessed");
    } finally {
      assertTrue(streams.close(Duration.ofSeconds(10)), "the app should close before the restart");
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
    CountingRestoreListener restoredAfterStep3 = new CountingRestoreListener();
    streams = startWindowApp(inputTopic, appId, SPECIFIC_STORE_NAME,
        createSpecificKeySerde(), createSpecificValueSerde(), stateDir, restoredAfterStep3);
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
      assertEquals(4, restoredAfterStep3.restoredRecords(),
          "the 4 changelog records written so far should be restored");
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
    assertEquals(SensorReadingV2.class, entry.value().getClass(),
        "the reader should return the v2 class");
    assertEquals(temperature, entry.value().getTemperature(), key + " temperature");
    assertEquals(humidity, entry.value().getHumidity(), key + " humidity");
    assertSpecificSchemaIdHeaders(entry.headers(), inputTopic, appId, key.toString());
  }

  // The store re-serializes values as SensorReadingV2, so the value GUID is registered under the
  // changelog subject, not the input topic's.
  private void assertSpecificSchemaIdHeaders(Headers headers, String inputTopic, String appId,
      String context) {
    assertSchemaIdHeaders(headers, inputTopic + "-key", SensorKey.getClassSchema(),
        appId + "-" + SPECIFIC_STORE_NAME + "-changelog-value", SensorReadingV2.getClassSchema(),
        context);
  }

  private <K, V> KafkaStreams startWindowApp(String inputTopic, String appId, String storeName,
      Serde<K> keySerde, Serde<V> valueSerde, Path stateDir) throws Exception {
    return startWindowApp(inputTopic, appId, storeName, keySerde, valueSerde, stateDir, null);
  }

  private <K, V> KafkaStreams startWindowApp(String inputTopic, String appId, String storeName,
      Serde<K> keySerde, Serde<V> valueSerde, Path stateDir,
      CountingRestoreListener restoreListener) throws Exception {
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
    return startStreams(builder, appId, stateDir, restoreListener);
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
