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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for key and value schema evolution on a header-aware
 * {@link SessionStoreWithHeaders}. A processor writes each input record into the session store as
 * a single-point session, and the tests read it back with {@code fetch(key)}.
 */
public class KafkaStreamsHeaderSessionStoreSchemaEvolutionIntegrationTest
    extends SchemaEvolutionIntegrationTestBase {

  private static final Duration RETENTION_PERIOD = Duration.ofHours(1);
  private static final String STORE_NAME = "session-schema-evolution-store";
  private static final String SPECIFIC_STORE_NAME = "session-specific-record-evolution-store";

  // Record timestamps. Each (key, timestamp) is its own session.
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 300_000L;
  private static final long TIME_2 = 600_000L;

  // Tests wait on this instead of querying mid-processing, which can corrupt the stored headers.
  private final AtomicInteger processed = new AtomicInteger();

  /**
   * Value schema evolves v1 to v2 to v3 across sessions. Each session keeps the shape it was
   * written with, and a later write to the same key and session replaces the earlier value.
   */
  @Test
  public void shouldReadOldAndNewValuesAfterValueSchemaEvolution() throws Exception {
    String inputTopic = "session-value-evolution-input";
    String appId = "session-value-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startSessionApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord key1 = sensorKey("sensor-1");
      GenericRecord key2 = sensorKey("sensor-2");
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, key1, valueV1(35.5, 1000L));
        send(producer, inputTopic, TIME_0, key2, valueV1(22.0, 2000L));
      }
      awaitProcessed(2);
      ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store =
          sessionStore(streams, STORE_NAME);

      // Registering v2 without writing it changes nothing in the store.
      restApp.restClient.registerSchema(VALUE_SCHEMA_V2.toString(), inputTopic + "-value");
      AggregationWithHeaders<GenericRecord> v1Entry = onlySession(store, key1);
      assertEquals(35.5, v1Entry.aggregation().get("temperature"));
      assertThrows(AvroRuntimeException.class, () -> v1Entry.aggregation().get("humidity"),
          "entry is still v1 bytes, so humidity should not be present");
      assertSchemaIdHeaders(v1Entry.headers(), inputTopic, "key1 session 0 v1");

      // Re-read the same v1 bytes off the input topic with the v2 reader schema; Avro schema
      // resolution should fill humidity with the v2 default.
      assertV1BytesReadAsV2(inputTopic, 2);

      // v2 write in a new session, and a v2 overwrite of an existing key and session.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_1, key1, valueV2(36.0, 3000L, 65.0));
        send(producer, inputTopic, TIME_0, key2, valueV2(24.0, 2500L, 50.0));
      }
      awaitProcessed(4);

      // v3 write adds pressure.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_2, key1,
            new GenericRecordBuilder(VALUE_SCHEMA_V3).set("temperature", 18.0)
                .set("timestamp", 5000L).set("humidity", 55.0).set("pressure", 1020.0).build());
      }
      awaitProcessed(5);

      List<AggregationWithHeaders<GenericRecord>> key1Sessions = sessions(store, key1);
      assertEquals(3, key1Sessions.size(), "key1 should have one session per timestamp");
      AggregationWithHeaders<GenericRecord> stillV1 = key1Sessions.get(0);
      assertEquals(35.5, stillV1.aggregation().get("temperature"));
      assertThrows(AvroRuntimeException.class, () -> stillV1.aggregation().get("humidity"));
      AggregationWithHeaders<GenericRecord> v2 = key1Sessions.get(1);
      assertEquals(65.0, v2.aggregation().get("humidity"));
      assertThrows(AvroRuntimeException.class, () -> v2.aggregation().get("pressure"),
          "the v2 entry should not gain a pressure field");
      AggregationWithHeaders<GenericRecord> v3 = key1Sessions.get(2);
      assertEquals(1020.0, v3.aggregation().get("pressure"));
      assertSchemaIdHeaders(stillV1.headers(), inputTopic, "key1 session 0 v1");
      assertSchemaIdHeaders(v2.headers(), inputTopic, "key1 session 1 v2");
      assertSchemaIdHeaders(v3.headers(), inputTopic, "key1 session 2 v3");

      AggregationWithHeaders<GenericRecord> overwritten = onlySession(store, key2);
      assertEquals(50.0, overwritten.aggregation().get("humidity"));
      assertSchemaIdHeaders(overwritten.headers(), inputTopic, "key2 session 0 v2");
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * The same logical key under an evolved key schema serializes to different bytes, so the same
   * timestamp holds two separate sessions.
   */
  @Test
  public void shouldStoreSameLogicalKeyAsTwoSessionsAfterKeySchemaEvolution() throws Exception {
    String inputTopic = "session-key-evolution-input";
    String appId = "session-key-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startSessionApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord keyV1 = sensorKey("sensor-1");
      GenericRecord keyV2 = new GenericRecordBuilder(KEY_SCHEMA_V2)
          .set("sensorId", "sensor-1").set("region", "us-east").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, keyV1, valueV1(35.5, 1000L));
      }
      awaitProcessed(1);
      ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store =
          sessionStore(streams, STORE_NAME);

      // Registering key v2 without writing it leaves the v1 session readable.
      restApp.restClient.registerSchema(KEY_SCHEMA_V2.toString(), inputTopic + "-key");
      assertEquals(35.5, onlySession(store, keyV1).aggregation().get("temperature"));
      assertEquals(0, sessions(store, keyV2).size(), "no session exists yet for the v2 key bytes");

      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, keyV2, valueV1(40.0, 2000L));
      }
      awaitProcessed(2);

      AggregationWithHeaders<GenericRecord> sessionV1 = onlySession(store, keyV1);
      AggregationWithHeaders<GenericRecord> sessionV2 = onlySession(store, keyV2);
      assertEquals(35.5, sessionV1.aggregation().get("temperature"));
      assertEquals(40.0, sessionV2.aggregation().get("temperature"));
      assertSchemaIdHeaders(sessionV1.headers(), inputTopic, "v1 key session");
      assertSchemaIdHeaders(sessionV2.headers(), inputTopic, "v2 key session");
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * Key schemas that differ only in metadata serialize to identical bytes, so reads and writes
   * under either schema address the same session.
   */
  @Test
  public void shouldShareSessionWhenKeySchemasProduceIdenticalBytes() throws Exception {
    String inputTopic = "session-doc-only-evolution-input";
    String appId = "session-doc-only-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startSessionApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord keyV1 = sensorKey("sensor-1");
      GenericRecord keyDocChanged = new GenericRecordBuilder(KEY_SCHEMA_V1_DOC_CHANGED)
          .set("sensorId", "sensor-1").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, keyV1, valueV1(35.5, 1000L));
      }
      awaitProcessed(1);
      ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store =
          sessionStore(streams, STORE_NAME);

      AggregationWithHeaders<GenericRecord> viaDocChanged = onlySession(store, keyDocChanged);
      assertEquals(35.5, viaDocChanged.aggregation().get("temperature"));
      assertSchemaIdHeaders(viaDocChanged.headers(), inputTopic, "doc-changed lookup");

      // A write under the doc-changed schema replaces the session rather than adding one.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, keyDocChanged, valueV1(40.0, 2000L));
      }
      awaitProcessed(2);
      assertEquals(40.0, onlySession(store, keyV1).aggregation().get("temperature"));
      assertEquals(40.0, onlySession(store, keyDocChanged).aggregation().get("temperature"));
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * A tombstone removes only the session whose key bytes match. A key with a different schema and
   * different bytes is untouched, and a doc-only schema change still matches.
   */
  @Test
  public void shouldRemoveOnlyMatchingByteKeySessionOnTombstone() throws Exception {
    String inputTopic = "session-tombstone-evolution-input";
    String appId = "session-tombstone-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startSessionApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord keyV1 = sensorKey("sensor-1");
      GenericRecord keyV2 = new GenericRecordBuilder(KEY_SCHEMA_V2)
          .set("sensorId", "sensor-1").set("region", "us-east").build();
      GenericRecord key2V1 = sensorKey("sensor-2");
      GenericRecord key2DocChanged = new GenericRecordBuilder(KEY_SCHEMA_V1_DOC_CHANGED)
          .set("sensorId", "sensor-2").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, keyV1, valueV1(35.5, 1000L));
        send(producer, inputTopic, TIME_0, key2V1, valueV1(37.0, 2000L));
        send(producer, inputTopic, TIME_0, keyV2, valueV1(40.0, 3000L));
      }
      awaitProcessed(3);
      ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store =
          sessionStore(streams, STORE_NAME);
      assertEquals(1, sessions(store, keyV1).size());
      assertEquals(1, sessions(store, keyV2).size());
      assertEquals(1, sessions(store, key2V1).size());

      // Tombstone with the v1 key removes only the v1 session, not the v2 one.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, keyV1, null);
      }
      awaitProcessed(4);
      assertEquals(0, sessions(store, keyV1).size());
      AggregationWithHeaders<GenericRecord> survivingV2 = onlySession(store, keyV2);
      assertEquals(40.0, survivingV2.aggregation().get("temperature"));
      assertSchemaIdHeaders(survivingV2.headers(), inputTopic, "surviving v2 key session");
      assertEquals(1, sessions(store, key2V1).size());

      // Tombstone under the doc-changed schema has identical bytes, so it removes sensor-2.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, key2DocChanged, null);
      }
      awaitProcessed(5);
      assertEquals(0, sessions(store, key2V1).size());
      assertEquals(1, sessions(store, keyV2).size(), "the v2 key session should be the one left");
    } finally {
      closeQuietly(streams);
    }
  }

  /**
   * After the local state is wiped, the store is rebuilt from the changelog and the schema-id
   * headers survive the restore.
   */
  @Test
  public void shouldRestoreSessionStoreFromChangelogPreservingHeaderSchemaIds() throws Exception {
    String inputTopic = "session-restore-evolution-input";
    String appId = "session-restore-evolution-test-" + System.currentTimeMillis();
    Path stateDir = Files.createTempDirectory("kstreams-session-evolution-");
    createTopics(inputTopic);

    // Wall-clock times: on restore the observed stream time becomes the current time, so sessions
    // near the epoch would be dropped as expired by the 1h retention.
    long time0 = System.currentTimeMillis();
    long time1 = time0 + 1_000L;
    GenericRecord key1 = sensorKey("sensor-1");
    GenericRecord key2 = sensorKey("sensor-2");
    KafkaStreams streams = startSessionApp(inputTopic, appId, STORE_NAME,
        createKeySerde(), createValueSerde(), stateDir);
    try {
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, time0, key1, valueV1(35.5, 1000L));
        send(producer, inputTopic, time1, key2, valueV2(28.0, 4000L, 70.0));
      }
      awaitProcessed(2);
    } finally {
      streams.close(Duration.ofSeconds(10));
    }

    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);

    streams = startSessionApp(inputTopic, appId, STORE_NAME,
        createKeySerde(), createValueSerde(), stateDir);
    try {
      ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store =
          awaitSessions(streams, STORE_NAME, key1, 1);
      awaitSessions(streams, STORE_NAME, key2, 1);

      AggregationWithHeaders<GenericRecord> restored1 = onlySession(store, key1);
      assertEquals(35.5, restored1.aggregation().get("temperature"));
      assertSchemaIdHeaders(restored1.headers(), inputTopic, "restored key1");
      AggregationWithHeaders<GenericRecord> restored2 = onlySession(store, key2);
      assertEquals(70.0, restored2.aggregation().get("humidity"));
      assertSchemaIdHeaders(restored2.headers(), inputTopic, "restored key2");
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
    String inputTopic = "session-null-default-evolution-input";
    String appId = "session-null-default-evolution-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      streams = startSessionApp(inputTopic, appId, STORE_NAME,
          createKeySerde(), createValueSerde(), null);

      GenericRecord key1 = sensorKey("sensor-1");
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_0, key1, valueV1(35.5, 1000L));
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
      ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store =
          sessionStore(streams, STORE_NAME);
      assertEquals(35.5, onlySession(store, key1).aggregation().get("temperature"));

      // The record builder fills omitted fields with their defaults.
      GenericRecord withDefaults = new GenericRecordBuilder(VALUE_SCHEMA_V3)
          .set("temperature", 45.0).set("timestamp", 2500L).build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        send(producer, inputTopic, TIME_1, key1, withDefaults);
      }
      awaitProcessed(2);
      List<AggregationWithHeaders<GenericRecord>> key1Sessions = sessions(store, key1);
      assertEquals(2, key1Sessions.size());
      AggregationWithHeaders<GenericRecord> defaulted = key1Sessions.get(1);
      assertEquals(45.0, defaulted.aggregation().get("temperature"));
      assertEquals(0.0, defaulted.aggregation().get("humidity"));
      assertEquals(1013.0, defaulted.aggregation().get("pressure"));
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
    String inputTopic = "session-reader-upgrade-input";
    String appId = "session-reader-upgrade-test-" + System.currentTimeMillis();
    createTopics(inputTopic);

    KafkaStreams streams = null;
    try {
      try (KafkaProducer<SensorKey, SensorReadingV1> v1Producer = createV1Producer()) {
        send(v1Producer, inputTopic, TIME_0, new SensorKey("sensor-1"),
            SensorReadingV1.newBuilder().setTemperature(35.5).setTimestamp(1000L).build());
        send(v1Producer, inputTopic, TIME_1, new SensorKey("sensor-2"),
            SensorReadingV1.newBuilder().setTemperature(22.0).setTimestamp(2000L).build());
      }

      streams = startSessionApp(inputTopic, appId, SPECIFIC_STORE_NAME,
          createSpecificKeySerde(), createSpecificValueSerde(), null);
      awaitProcessed(2);
      ReadOnlySessionStore<SensorKey, AggregationWithHeaders<SensorReadingV2>> store =
          sessionStore(streams, SPECIFIC_STORE_NAME);

      assertSensor(store, new SensorKey("sensor-1"), 35.5, 0.0, inputTopic, appId);
      assertSensor(store, new SensorKey("sensor-2"), 22.0, 0.0, inputTopic, appId);
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
    String inputTopic = "session-writer-upgrade-input";
    String appId = "session-writer-upgrade-test-" + System.currentTimeMillis();
    Path stateDir = Files.createTempDirectory("kstreams-session-specific-");
    createTopics(inputTopic);

    // Wall-clock time: sessions near the epoch are dropped as expired on restore.
    long time = System.currentTimeMillis();
    SensorKey key1 = new SensorKey("sensor-1");
    SensorKey key2 = new SensorKey("sensor-2");
    SensorKey key3 = new SensorKey("sensor-3");
    SensorKey key4 = new SensorKey("sensor-4");

    // Step 1: v1 writer, v2 reader.
    try (KafkaProducer<SensorKey, SensorReadingV1> v1Producer = createV1Producer()) {
      send(v1Producer, inputTopic, time, key1,
          SensorReadingV1.newBuilder().setTemperature(35.5).setTimestamp(1000L).build());
      send(v1Producer, inputTopic, time, key2,
          SensorReadingV1.newBuilder().setTemperature(22.0).setTimestamp(2000L).build());
    }
    KafkaStreams streams = startSessionApp(inputTopic, appId, SPECIFIC_STORE_NAME,
        createSpecificKeySerde(), createSpecificValueSerde(), stateDir);
    try {
      awaitProcessed(2);
      ReadOnlySessionStore<SensorKey, AggregationWithHeaders<SensorReadingV2>> store =
          sessionStore(streams, SPECIFIC_STORE_NAME);
      // The v1 bytes are read through the v2 class, so the new field takes its default.
      assertSensor(store, key1, 35.5, 0.0, inputTopic, appId);
      assertSensor(store, key2, 22.0, 0.0, inputTopic, appId);
    } finally {
      streams.close(Duration.ofSeconds(10));
    }

    // Step 2: wipe state, the writer upgrades to v2.
    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);
    try (KafkaProducer<SensorKey, SensorReadingV2> v2Producer = createV2Producer()) {
      send(v2Producer, inputTopic, time, key1, SensorReadingV2.newBuilder()
          .setTemperature(36.0).setTimestamp(3000L).setHumidity(65.0).build());
      send(v2Producer, inputTopic, time, key3, SensorReadingV2.newBuilder()
          .setTemperature(28.0).setTimestamp(4000L).setHumidity(70.0).build());
    }
    CountingRestoreListener restoredAfterStep1 = new CountingRestoreListener();
    streams = startSessionApp(inputTopic, appId, SPECIFIC_STORE_NAME,
        createSpecificKeySerde(), createSpecificValueSerde(), stateDir, restoredAfterStep1);
    try {
      awaitProcessed(4);
      ReadOnlySessionStore<SensorKey, AggregationWithHeaders<SensorReadingV2>> store =
          sessionStore(streams, SPECIFIC_STORE_NAME);

      // sensor-1 was overwritten by v2, sensor-2 was restored from the changelog as v1.
      assertSensor(store, key1, 36.0, 65.0, inputTopic, appId);
      assertSensor(store, key2, 22.0, 0.0, inputTopic, appId);
      assertSensor(store, key3, 28.0, 70.0, inputTopic, appId);
      assertEquals(2, restoredAfterStep1.restoredRecords(),
          "sensor-1 and sensor-2 should be restored from the changelog");
      assertEquals(4, processed.get(), "sensor-2 should be restored, not reprocessed");
    } finally {
      streams.close(Duration.ofSeconds(10));
    }

    // Step 3: wipe state again, an old v1 producer that has not upgraded writes after the rollout.
    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);
    try (KafkaProducer<SensorKey, SensorReadingV1> v1Producer = createV1Producer()) {
      send(v1Producer, inputTopic, time, key3,
          SensorReadingV1.newBuilder().setTemperature(40.0).setTimestamp(4500L).build());
      send(v1Producer, inputTopic, time, key4,
          SensorReadingV1.newBuilder().setTemperature(45.0).setTimestamp(5000L).build());
    }
    CountingRestoreListener restoredAfterStep3 = new CountingRestoreListener();
    streams = startSessionApp(inputTopic, appId, SPECIFIC_STORE_NAME,
        createSpecificKeySerde(), createSpecificValueSerde(), stateDir, restoredAfterStep3);
    try {
      awaitProcessed(6);
      ReadOnlySessionStore<SensorKey, AggregationWithHeaders<SensorReadingV2>> store =
          sessionStore(streams, SPECIFIC_STORE_NAME);

      assertSensor(store, key1, 36.0, 65.0, inputTopic, appId);
      assertSensor(store, key2, 22.0, 0.0, inputTopic, appId);
      // sensor-3's v2 value was overwritten by the old v1 writer, so humidity is the default again.
      assertSensor(store, key3, 40.0, 0.0, inputTopic, appId);
      assertSensor(store, key4, 45.0, 0.0, inputTopic, appId);
      assertEquals(4, restoredAfterStep3.restoredRecords(),
          "the 4 changelog records written so far should be restored");
      assertEquals(6, processed.get(), "earlier sensors should be restored, not reprocessed");
    } finally {
      streams.close(Duration.ofSeconds(10));
      deleteRecursively(stateDir);
    }
  }

  private void assertSensor(
      ReadOnlySessionStore<SensorKey, AggregationWithHeaders<SensorReadingV2>> store,
      SensorKey key, double temperature, double humidity, String inputTopic, String appId) {
    AggregationWithHeaders<SensorReadingV2> entry = onlySession(store, key);
    assertEquals(temperature, entry.aggregation().getTemperature(), key + " temperature");
    assertEquals(humidity, entry.aggregation().getHumidity(), key + " humidity");
    assertSpecificSchemaIdHeaders(entry.headers(), inputTopic, appId, key.toString());
  }

  // The store re-serializes values as SensorReadingV2, so the value GUID is registered under the
  // changelog subject, not the input topic's.
  private void assertSpecificSchemaIdHeaders(Headers headers, String inputTopic, String appId,
      String context) {
    assertGuidHeaderRegistered(headers, SchemaId.KEY_SCHEMA_ID_HEADER, inputTopic + "-key",
        context + " key");
    assertGuidHeaderRegistered(headers, SchemaId.VALUE_SCHEMA_ID_HEADER,
        appId + "-" + SPECIFIC_STORE_NAME + "-changelog-value", context + " value");
  }

  private <K, V> KafkaStreams startSessionApp(String inputTopic, String appId, String storeName,
      Serde<K> keySerde, Serde<V> valueSerde, Path stateDir) throws Exception {
    return startSessionApp(inputTopic, appId, storeName, keySerde, valueSerde, stateDir, null);
  }

  private <K, V> KafkaStreams startSessionApp(String inputTopic, String appId, String storeName,
      Serde<K> keySerde, Serde<V> valueSerde, Path stateDir,
      CountingRestoreListener restoreListener) throws Exception {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .addStateStore(
            Stores.sessionStoreWithHeadersBuilder(
                Stores.persistentSessionStoreWithHeaders(storeName, RETENTION_PERIOD),
                keySerde,
                valueSerde))
        .stream(inputTopic, Consumed.with(keySerde, valueSerde))
        .process(() -> new PutProcessor<K, V>(storeName, processed), storeName);
    return startStreams(builder, appId, stateDir, restoreListener);
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

  /** Polls until the key has the expected number of sessions, then returns the queryable store. */
  private <K, V> ReadOnlySessionStore<K, AggregationWithHeaders<V>> awaitSessions(
      KafkaStreams streams, String storeName, K key, int expected) throws InterruptedException {
    AtomicReference<Exception> lastError = new AtomicReference<>();
    try {
      awaitCondition(() -> {
        try {
          return sessions(this.<K, V>sessionStore(streams, storeName), key).size() == expected;
        } catch (Exception e) {
          lastError.set(e);
          return false;
        }
      }, expected + " session(s) for " + key);
    } catch (AssertionError e) {
      throw new AssertionError(e.getMessage() + "; last query error: " + lastError.get(), e);
    }
    return sessionStore(streams, storeName);
  }

  private <K, V> ReadOnlySessionStore<K, AggregationWithHeaders<V>> sessionStore(
      KafkaStreams streams, String storeName) {
    return streams.store(StoreQueryParameters.fromNameAndType(
        storeName, new SessionStoreWithHeadersType<K, V>()));
  }

  private static <K, V> List<AggregationWithHeaders<V>> sessions(
      ReadOnlySessionStore<K, AggregationWithHeaders<V>> store, K key) {
    List<AggregationWithHeaders<V>> result = new ArrayList<>();
    try (KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> iter = store.fetch(key)) {
      while (iter.hasNext()) {
        KeyValue<Windowed<K>, AggregationWithHeaders<V>> kv = iter.next();
        result.add(kv.value);
      }
    }
    return result;
  }

  private static <K, V> AggregationWithHeaders<V> onlySession(
      ReadOnlySessionStore<K, AggregationWithHeaders<V>> store, K key) {
    List<AggregationWithHeaders<V>> found = sessions(store, key);
    assertEquals(1, found.size(), "expected exactly one session for " + key);
    assertNotNull(found.get(0));
    return found.get(0);
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

  /** Writes every input record into the session store, or removes the session on a null value. */
  private static class PutProcessor<K, V> implements Processor<K, V, Void, Void> {

    private final String storeName;
    private final AtomicInteger processed;
    private SessionStoreWithHeaders<K, V> store;

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
      Windowed<K> sessionKey =
          new Windowed<>(record.key(), new SessionWindow(record.timestamp(), record.timestamp()));
      if (record.value() == null) {
        store.put(sessionKey, null);
      } else {
        store.put(sessionKey, AggregationWithHeaders.make(record.value(), record.headers()));
      }
      processed.incrementAndGet();
    }
  }

  private static class SessionStoreWithHeadersType<K, V>
      implements QueryableStoreType<ReadOnlySessionStore<K, AggregationWithHeaders<V>>> {

    @Override
    public boolean accepts(final StateStore stateStore) {
      return stateStore instanceof SessionStoreWithHeaders
          && stateStore instanceof ReadOnlySessionStore;
    }

    @Override
    public ReadOnlySessionStore<K, AggregationWithHeaders<V>> create(
        final StateStoreProvider storeProvider, final String storeName) {
      return new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
    }
  }
}
