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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.TimestampedWindowKeyWithHeadersQuery;
import org.apache.kafka.streams.query.TimestampedWindowRangeWithHeadersQuery;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;

/**
 * KIP-1356 IQv2 integration test for the headers-aware window and session stores.
 *
 * <p>Verifies that the headers-aware IQv2 query types return the record headers persisted by the
 * KIP-1271 {@link TimestampedWindowStoreWithHeaders} and {@link SessionStoreWithHeaders}:
 * <ul>
 *   <li>{@link TimestampedWindowKeyWithHeadersQuery} — single key across a window-start range
 *       (window store);</li>
 *   <li>{@link TimestampedWindowRangeWithHeadersQuery#withWindowStartRange} — all keys across a
 *       window-start range (window store);</li>
 *   <li>{@link TimestampedWindowRangeWithHeadersQuery#withKey} — all sessions for a key (session
 *       store), where {@link ReadOnlyRecord#timestamp()} is the session window's end.</li>
 * </ul>
 * Records are produced with the schema-id GUID carried in the {@code __key_schema_id} /
 * {@code __value_schema_id} headers (via {@link HeaderSchemaIdSerializer}); each query result is a
 * {@link ReadOnlyRecordIterator} of {@link ReadOnlyRecord} whose headers must be the 17-byte
 * {@code MAGIC_BYTE_V1} GUID.
 *
 * <p>Runs against a real 1-broker cluster + embedded Schema Registry ({@code super(1, true)}).
 */
public class WindowAndSessionStoreWithHeadersIQv2IntegrationTest extends ClusterTestHarness {

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(10);
    private static final Duration RETENTION_PERIOD = Duration.ofHours(1);
    private static final Duration SESSION_GAP = Duration.ofMinutes(30);
    private static final long SESSION_DURATION_MS = Duration.ofSeconds(5).toMillis();
    // The window-store writer stores each value's event-time this far into its window, so that
    // ReadOnlyRecord.timestamp() (the stored event-time) is distinguishable from the window start.
    private static final long EVENT_TIME_OFFSET_MS = 3;
    // Records for the same key within this gap are merged into one (variable-length) session by the
    // merging session writer; records farther apart start a new session.
    private static final long SESSION_MERGE_GAP_MS = 100;

    private static final Schema KEY_SCHEMA = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"SensorKey\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":[{\"name\":\"sensorId\",\"type\":\"string\"}]}");
    private static final Schema VALUE_SCHEMA = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"SensorValue\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":[{\"name\":\"reading\",\"type\":\"long\"}]}");

    public WindowAndSessionStoreWithHeadersIQv2IntegrationTest() {
        super(1, true);
    }

    // ---------------------------------------------------------------------------------------------
    // TimestampedWindowKeyWithHeadersQuery (window store: single key across window-start range)
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldQueryWindowKeyWithHeaders() throws Exception {
        String input = "iqv2-winkey-input";
        String output = "iqv2-winkey-output";
        String storeName = "iqv2-winkey-store";
        String appId = "iqv2-winkey-test";

        createTopics(input, output);
        long base = alignedBase(WINDOW_SIZE.toMillis());
        long w0 = base;
        long w1 = base + 100;
        long w2 = base + 200;
        long w3 = base + 300;
        long w4 = base + 400;

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                // Queried key sensor-1 across four windows: live, live, tombstoned, live.
                producer.send(new ProducerRecord<>(input, null, w0, createKey("sensor-1"), createValue(0))).get();
                producer.send(new ProducerRecord<>(input, null, w1, createKey("sensor-1"), createValue(1))).get();
                producer.send(new ProducerRecord<>(input, null, w2, createKey("sensor-1"), createValue(2))).get();
                producer.send(new ProducerRecord<>(input, null, w2, createKey("sensor-1"), (GenericRecord) null)).get();
                producer.send(new ProducerRecord<>(input, null, w3, createKey("sensor-1"), createValue(3))).get();
                // Noise key sensor-2 must not leak into sensor-1's results; its w0 tombstone is key-scoped
                // and must not drop sensor-1's own w0.
                producer.send(new ProducerRecord<>(input, null, w0, createKey("sensor-2"), createValue(10))).get();
                producer.send(new ProducerRecord<>(input, null, w0, createKey("sensor-2"), (GenericRecord) null)).get();
                producer.send(new ProducerRecord<>(input, null, w1, createKey("sensor-2"), createValue(11))).get();
                producer.send(new ProducerRecord<>(input, null, w4, createKey("sensor-2"), createValue(14))).get();
                producer.flush();
            }
            consumeRecords(output, appId + "-barrier", 9);

            // Full range over sensor-1: w0, w1, w3 in window-start order (w2 tombstoned, sensor-2 excluded).
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = queryWindowed(
                streams, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(w0), Instant.ofEpochMilli(w4)),
                3);
            assertEquals(Arrays.asList(w0, w1, w3),
                records.stream().map(r -> r.key().window().start()).collect(Collectors.toList()),
                "sensor-1 window starts (w2 tombstoned, sensor-2 excluded)");
            assertWindowedRecord(records.get(0), "sensor-1", w0, 0L, "winkey sensor-1 w0");
            assertWindowedRecord(records.get(1), "sensor-1", w1, 1L, "winkey sensor-1 w1");
            assertWindowedRecord(records.get(2), "sensor-1", w3, 3L, "winkey sensor-1 w3");

            // Sub-range [w1, w2]: only w1 (w0 below lower bound, w2 tombstoned, w3 above upper bound).
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> subRange = queryWindowed(
                streams, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(w1), Instant.ofEpochMilli(w2)),
                1);
            assertEquals(1, subRange.size(), "sub-range returns only w1");
            assertWindowedRecord(subRange.get(0), "sensor-1", w1, 1L, "winkey sub-range w1");

            // A never-written key -> empty.
            assertTrue(queryWindowed(streams, storeName,
                    TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                        createKey("sensor-999"), Instant.ofEpochMilli(w0), Instant.ofEpochMilli(w4)), 0)
                    .isEmpty(),
                "never-written key returns no records");
            // A window-start range after sensor-1's last window -> empty.
            assertTrue(queryWindowed(streams, storeName,
                    TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                        createKey("sensor-1"), Instant.ofEpochMilli(w4), Instant.ofEpochMilli(w4 + 100)), 0)
                    .isEmpty(),
                "window-start range excluding all of sensor-1's windows returns no records");

            // Bidirectional isolation: sensor-2 returns only its live windows (w1, w4; w0 tombstoned).
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> key2 = queryWindowed(
                streams, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-2"), Instant.ofEpochMilli(w0), Instant.ofEpochMilli(w4)),
                2);
            assertEquals(Arrays.asList(w1, w4),
                key2.stream().map(r -> r.key().window().start()).collect(Collectors.toList()),
                "sensor-2 window starts (w0 tombstoned)");
            assertWindowedRecord(key2.get(0), "sensor-2", w1, 11L, "winkey sensor-2 w1");
            assertWindowedRecord(key2.get(1), "sensor-2", w4, 14L, "winkey sensor-2 w4");
        } finally {
            closeStreams(streams);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // TimestampedWindowRangeWithHeadersQuery.withWindowStartRange (window store: all keys)
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldQueryWindowRangeWithHeaders() throws Exception {
        String input = "iqv2-winrange-input";
        String output = "iqv2-winrange-output";
        String storeName = "iqv2-winrange-store";
        String appId = "iqv2-winrange-test";

        createTopics(input, output);
        long base = alignedBase(WINDOW_SIZE.toMillis());
        long w0 = base;
        long w1 = base + 100;
        long w2 = base + 200;

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                // sensor-1 across two windows, both live and in range.
                producer.send(new ProducerRecord<>(input, null, w0, createKey("sensor-1"), createValue(0))).get();
                producer.send(new ProducerRecord<>(input, null, w1, createKey("sensor-1"), createValue(1))).get();
                // sensor-2: live in-range at w0, tombstoned in-range at w1, live out-of-range at w2.
                producer.send(new ProducerRecord<>(input, null, w0, createKey("sensor-2"), createValue(10))).get();
                producer.send(new ProducerRecord<>(input, null, w1, createKey("sensor-2"), createValue(11))).get();
                producer.send(new ProducerRecord<>(input, null, w1, createKey("sensor-2"), (GenericRecord) null)).get();
                producer.send(new ProducerRecord<>(input, null, w2, createKey("sensor-2"), createValue(12))).get();
                producer.flush();
            }
            consumeRecords(output, appId + "-barrier", 6);

            // Range [w0, w1]: every key's windows whose start falls in the range.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = queryWindowed(
                streams, storeName,
                TimestampedWindowRangeWithHeadersQuery.withWindowStartRange(
                    Instant.ofEpochMilli(w0), Instant.ofEpochMilli(w1)),
                3);

            // sensor-1: both windows are in range.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> sensor1 =
                recordsForSensor(records, "sensor-1");
            assertEquals(Arrays.asList(w0, w1),
                sensor1.stream().map(r -> r.key().window().start()).collect(Collectors.toList()),
                "sensor-1 windows in range");
            assertWindowedRecord(sensor1.get(0), "sensor-1", w0, 0L, "winrange sensor-1 w0");
            assertWindowedRecord(sensor1.get(1), "sensor-1", w1, 1L, "winrange sensor-1 w1");

            // sensor-2: w0 survives; w1 is tombstoned; w2 is excluded by the range upper bound.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> sensor2 =
                recordsForSensor(records, "sensor-2");
            assertEquals(Arrays.asList(w0),
                sensor2.stream().map(r -> r.key().window().start()).collect(Collectors.toList()),
                "sensor-2 windows in range (w1 tombstoned, w2 out of range)");
            assertWindowedRecord(sensor2.get(0), "sensor-2", w0, 10L, "winrange sensor-2 w0");
        } finally {
            closeStreams(streams);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // TimestampedWindowRangeWithHeadersQuery.withKey (session store: all sessions for a key)
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldQuerySessionByKeyWithHeaders() throws Exception {
        String input = "iqv2-session-input";
        String output = "iqv2-session-output";
        String storeName = "iqv2-session-store";
        String appId = "iqv2-session-test";

        createTopics(input, output);
        long base = alignedBase(WINDOW_SIZE.toMillis());

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildSessionMergingTopology(input, output, storeName), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                // sensor-1: three records within the merge gap collapse into one variable-length session
                // [base, base+55] whose reading is the sum (1+2+3=6) and whose headers come from the last
                // record folded in.
                producer.send(new ProducerRecord<>(input, null, base, createKey("sensor-1"), createValue(1))).get();
                producer.send(new ProducerRecord<>(input, null, base + 30, createKey("sensor-1"), createValue(2))).get();
                producer.send(new ProducerRecord<>(input, null, base + 55, createKey("sensor-1"), createValue(3))).get();
                // A distant record (> gap away) forms a separate, zero-length session [base+500, base+500].
                producer.send(new ProducerRecord<>(input, null, base + 500, createKey("sensor-1"), createValue(4))).get();
                // Noise key sensor-2: a session written then tombstoned -- must not survive or leak.
                producer.send(new ProducerRecord<>(input, null, base, createKey("sensor-2"), createValue(20))).get();
                producer.send(new ProducerRecord<>(input, null, base, createKey("sensor-2"), (GenericRecord) null)).get();
                producer.flush();
            }
            consumeRecords(output, appId + "-barrier", 6);

            // sensor-1 has two variable-length sessions, returned in window-start order.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = recordsForSensor(
                queryWindowed(streams, storeName,
                    TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-1")), 2),
                "sensor-1");
            assertEquals(2, records.size(), "sensor-1 should have two sessions");
            // Merged, variable-length session [base, base+55], summed reading, last writer's headers.
            assertSessionRecord(records.get(0), "sensor-1", base, base + 55, 6L, "session merged");
            // A separate zero-length session -- proves the end round-trips from the stored value.
            assertSessionRecord(records.get(1), "sensor-1", base + 500, base + 500, 4L, "session zero-length");

            // sensor-2's only session was tombstoned -> empty; a never-written key is likewise empty.
            assertTrue(queryWindowed(streams, storeName,
                    TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-2")), 0).isEmpty(),
                "sensor-2's tombstoned session should be excluded");
            assertTrue(queryWindowed(streams, storeName,
                    TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-999")), 0).isEmpty(),
                "never-written key returns no sessions");
        } finally {
            closeStreams(streams);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Restore-from-changelog and multi-partition
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldRestoreWindowStoreFromChangelogPreservingHeaders() throws Exception {
        String input = "iqv2-winrestore-input";
        String output = "iqv2-winrestore-output";
        String storeName = "iqv2-winrestore-store";
        String appId = "iqv2-winrestore-test";

        createTopics(input, output);
        long base = alignedBase(WINDOW_SIZE.toMillis());

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId);
        try {
            produce(input, createKey("sensor-1"), createValue(1), base);
            consumeRecords(output, appId + "-pre", 1);
        } finally {
            closeStreams(streams);
        }

        // Restart with the same APPLICATION_ID; cleanUp() wipes the local state dir so the window
        // store must be rebuilt from the changelog. The restored entry must still carry its headers.
        KafkaStreams restored = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId, 90);
        try {
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = queryWindowed(
                restored, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(base), Instant.ofEpochMilli(base)),
                1);
            assertWindowedRecord(records.get(0), "sensor-1", base, 1L, "restored window sensor-1");
        } finally {
            closeStreams(restored);
        }
    }

    @Test
    public void shouldRestoreSessionStoreFromChangelogPreservingHeaders() throws Exception {
        String input = "iqv2-sessrestore-input";
        String output = "iqv2-sessrestore-output";
        String storeName = "iqv2-sessrestore-store";
        String appId = "iqv2-sessrestore-test";

        createTopics(input, output);
        long base = alignedBase(WINDOW_SIZE.toMillis());
        long sessionEnd = base + SESSION_DURATION_MS;

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildSessionTopology(input, output, storeName), appId);
        try {
            produce(input, createKey("sensor-1"), createValue(42), base);
            consumeRecords(output, appId + "-pre", 1);
        } finally {
            closeStreams(streams);
        }

        // Restart with the same APPLICATION_ID; the session store (backed by AggregationWithHeaders)
        // must be rebuilt from the changelog, and the restored session must still carry its headers.
        KafkaStreams restored = startStreamsAndAwaitRunning(
            buildSessionTopology(input, output, storeName), appId, 90);
        try {
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = queryWindowed(
                restored, storeName,
                TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-1")),
                1);
            assertSessionRecord(
                records.get(0), "sensor-1", base, sessionEnd, 42L, "restored session sensor-1");
        } finally {
            closeStreams(restored);
        }
    }

    @Test
    public void shouldQueryWindowAcrossPartitionsWithHeaders() throws Exception {
        String input = "iqv2-winmp-input";
        String output = "iqv2-winmp-output";
        String storeName = "iqv2-winmp-store";
        String appId = "iqv2-winmp-test";
        int numPartitions = 3;
        int numKeys = 9;

        createTopicsWithPartitions(numPartitions, input, output);
        long base = alignedBase(WINDOW_SIZE.toMillis());

        List<GenericRecord> keys = new ArrayList<>();
        List<GenericRecord> values = new ArrayList<>();
        for (int i = 1; i <= numKeys; i++) {
            keys.add(createKey("sensor-" + i));
            values.add(createValue(i));
        }

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId);
        try {
            produceAll(input, keys, values, base);
            consumeRecords(output, appId + "-barrier", numKeys);

            // withWindowStartRange fans across partitions; aggregate every partition's result.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> all = new ArrayList<>();
            Set<Integer> partitionsSeen = new HashSet<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                all.clear();
                partitionsSeen.clear();
                StateQueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> result =
                    streams.query(StateQueryRequest.inStore(storeName)
                        .withQuery(TimestampedWindowRangeWithHeadersQuery.withWindowStartRange(
                            Instant.ofEpochMilli(base), Instant.ofEpochMilli(base))));
                result.getPartitionResults().forEach((partition, pr) -> {
                    if (pr.isSuccess() && pr.getResult() != null) {
                        try (ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord> it =
                                 pr.getResult()) {
                            while (it.hasNext()) {
                                all.add(it.next());
                                partitionsSeen.add(partition);
                            }
                        }
                    }
                });
                if (all.size() >= numKeys) {
                    break;
                }
                sleepQuietly(200);
            }

            assertEquals(numKeys, all.size(), "should read every key across partitions");
            assertTrue(partitionsSeen.size() > 1,
                "window records should span more than one partition but saw: " + partitionsSeen);
            for (ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> r : all) {
                String sensorId = r.key().key().get("sensorId").toString();
                long reading = Long.parseLong(sensorId.substring("sensor-".length()));
                assertWindowedRecord(r, sensorId, base, reading, "window multi-partition " + sensorId);
            }
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldQuerySessionAcrossPartitionsWithHeaders() throws Exception {
        String input = "iqv2-sessmp-input";
        String output = "iqv2-sessmp-output";
        String storeName = "iqv2-sessmp-store";
        String appId = "iqv2-sessmp-test";
        int numPartitions = 3;
        int numKeys = 9;

        createTopicsWithPartitions(numPartitions, input, output);
        long base = alignedBase(WINDOW_SIZE.toMillis());
        long sessionEnd = base + SESSION_DURATION_MS;

        List<GenericRecord> keys = new ArrayList<>();
        List<GenericRecord> values = new ArrayList<>();
        for (int i = 1; i <= numKeys; i++) {
            keys.add(createKey("sensor-" + i));
            values.add(createValue(i));
        }

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildSessionTopology(input, output, storeName), appId);
        try {
            produceAll(input, keys, values, base);
            consumeRecords(output, appId + "-barrier", numKeys);

            // A session withKey query routes to the key's partition, so query each key and record the
            // partition that served it; collectively the keys must resolve across >1 partition.
            Set<Integer> partitionsSeen = new HashSet<>();
            for (int i = 1; i <= numKeys; i++) {
                String sensorId = "sensor-" + i;
                Map.Entry<Integer, ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> served =
                    querySessionByKeyWithPartition(streams, storeName, createKey(sensorId));
                assertSessionRecord(served.getValue(), sensorId, base, sessionEnd, i,
                    "session multi-partition " + sensorId);
                partitionsSeen.add(served.getKey());
            }
            assertTrue(partitionsSeen.size() > 1,
                "session keys should resolve across more than one partition but saw: " + partitionsSeen);
        } finally {
            closeStreams(streams);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // IQv2 query helper
    // ---------------------------------------------------------------------------------------------

    private List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> queryWindowed(
        KafkaStreams streams, String storeName,
        Query<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> query, int expected) {
        long deadline = System.currentTimeMillis() + 30_000;
        List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> out = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            out.clear();
            StateQueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName).withQuery(query));
            QueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> pr =
                result.getOnlyPartitionResult();
            if (pr != null && pr.isSuccess() && pr.getResult() != null) {
                try (ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord> it = pr.getResult()) {
                    while (it.hasNext()) {
                        out.add(it.next());
                    }
                }
                if (out.size() >= expected) {
                    return out;
                }
            }
            sleepQuietly(200);
        }
        assertEquals(expected, out.size(), "IQv2 windowed query returned an unexpected count");
        return out;
    }

    /**
     * Runs a session {@code withKey} query (which routes to the key's partition) and returns the
     * partition id that served it together with the single session record.
     */
    private Map.Entry<Integer, ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>>
        querySessionByKeyWithPartition(KafkaStreams streams, String storeName, GenericRecord key) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            StateQueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(TimestampedWindowRangeWithHeadersQuery.withKey(key)));
            for (Integer partition : result.getPartitionResults().keySet()) {
                QueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> pr =
                    result.getPartitionResults().get(partition);
                if (pr.isSuccess() && pr.getResult() != null) {
                    try (ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord> it = pr.getResult()) {
                        if (it.hasNext()) {
                            return new AbstractMap.SimpleEntry<>(partition, it.next());
                        }
                    }
                }
            }
            sleepQuietly(200);
        }
        throw new AssertionError("session withKey query never returned a result for " + key);
    }

    private void assertWindowedRecord(ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> record,
        String sensorId, long windowStart, long reading, String context) {
        assertEquals(sensorId, record.key().key().get("sensorId").toString(), context + " key");
        assertEquals(windowStart, record.key().window().start(), context + " window start");
        assertEquals(windowStart + EVENT_TIME_OFFSET_MS, record.timestamp(), context + " event-time");
        assertEquals(reading, record.value().get("reading"), context + " value");
        assertSchemaIdHeaders(record.headers(), context);
    }

    private void assertSessionRecord(ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> record,
        String sensorId, long start, long end, long reading, String context) {
        assertEquals(sensorId, record.key().key().get("sensorId").toString(), context + " key");
        assertEquals(start, record.key().window().start(), context + " session start");
        assertEquals(end, record.key().window().end(), context + " session end");
        // A session carries no per-record event-time; timestamp() is the session window's end.
        assertEquals(end, record.timestamp(), context + " timestamp == session end");
        assertEquals(reading, record.value().get("reading"), context + " value");
        assertSchemaIdHeaders(record.headers(), context);
    }

    private List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> recordsForSensor(
        List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records, String sensorId) {
        return records.stream()
            .filter(r -> sensorId.equals(r.key().key().get("sensorId").toString()))
            .sorted(Comparator.comparingLong(
                (ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> r) -> r.key().window().start()))
            .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------------------------------
    // Topologies + processors
    // ---------------------------------------------------------------------------------------------

    private Topology buildWindowTopology(String input, String output, String storeName) {
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        StoreBuilder<TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord>> storeBuilder =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(
                    storeName, RETENTION_PERIOD, WINDOW_SIZE, false),
                keySerde, valueSerde)
                .withCachingDisabled();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(storeBuilder)
            .stream(input, Consumed.with(keySerde, valueSerde))
            .process(() -> new WindowPutProcessor(storeName), storeName)
            .to(output, Produced.with(keySerde, valueSerde));
        return builder.build();
    }

    private Topology buildSessionTopology(String input, String output, String storeName) {
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        StoreBuilder<SessionStoreWithHeaders<GenericRecord, GenericRecord>> storeBuilder =
            Stores.sessionStoreWithHeadersBuilder(
                Stores.persistentSessionStoreWithHeaders(storeName, SESSION_GAP), keySerde, valueSerde)
                .withCachingDisabled();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(storeBuilder)
            .stream(input, Consumed.with(keySerde, valueSerde))
            .process(() -> new SessionPutProcessor(storeName), storeName)
            .to(output, Produced.with(keySerde, valueSerde));
        return builder.build();
    }

    private Topology buildSessionMergingTopology(String input, String output, String storeName) {
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        StoreBuilder<SessionStoreWithHeaders<GenericRecord, GenericRecord>> storeBuilder =
            Stores.sessionStoreWithHeadersBuilder(
                Stores.persistentSessionStoreWithHeaders(storeName, SESSION_GAP), keySerde, valueSerde)
                .withCachingDisabled();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(storeBuilder)
            .stream(input, Consumed.with(keySerde, valueSerde))
            .process(() -> new SessionMergingPutProcessor(storeName), storeName)
            .to(output, Produced.with(keySerde, valueSerde));
        return builder.build();
    }

    /**
     * Stores each incoming record into the window store at the window start derived from its
     * event-time, preserving the record's headers, then forwards it as a completion barrier.
     */
    private static class WindowPutProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord> store;

        WindowPutProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            // The record timestamp is the window start; a null value tombstones that window. The stored
            // event-time is a few ms into the window so timestamp() is distinguishable from the start.
            long windowStart = record.timestamp();
            if (record.value() == null) {
                store.put(record.key(), null, windowStart);
            } else {
                store.put(record.key(),
                    ValueTimestampHeaders.make(
                        record.value(), windowStart + EVENT_TIME_OFFSET_MS, record.headers()),
                    windowStart);
            }
            context.forward(record);
        }
    }

    /**
     * Stores each incoming record as a fixed-length session (start == event-time,
     * end == event-time + {@link #SESSION_DURATION_MS}), preserving the record's headers, then
     * forwards it as a completion barrier.
     */
    private static class SessionPutProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private SessionStoreWithHeaders<GenericRecord, GenericRecord> store;

        SessionPutProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            long start = record.timestamp();
            long end = start + SESSION_DURATION_MS;
            store.put(new Windowed<>(record.key(), new SessionWindow(start, end)),
                AggregationWithHeaders.make(record.value(), record.headers()));
            context.forward(record);
        }
    }

    /**
     * Real session-windowing writer: merges records for the same key within {@link #SESSION_MERGE_GAP_MS}
     * into a single variable-length session (summing the reading, taking the last folded record's
     * headers), and tombstones every overlapping session on a null value. Used to exercise the session
     * form of {@link TimestampedWindowRangeWithHeadersQuery}.
     */
    private static class SessionMergingPutProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private SessionStoreWithHeaders<GenericRecord, GenericRecord> store;

        SessionMergingPutProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            long ts = record.timestamp();
            List<KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>>> overlapping =
                new ArrayList<>();
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> sessions =
                     store.findSessions(record.key(), ts - SESSION_MERGE_GAP_MS, ts + SESSION_MERGE_GAP_MS)) {
                while (sessions.hasNext()) {
                    overlapping.add(sessions.next());
                }
            }

            if (record.value() == null) {
                // Tombstone every overlapping session via put(..., null) (not remove()) so the store's
                // IQv2 Position still advances.
                for (KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> existing
                         : overlapping) {
                    store.put(existing.key, null);
                }
                context.forward(record);
                return;
            }

            long mergedStart = ts;
            long mergedEnd = ts;
            long mergedReading = 0L;
            for (KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> existing
                     : overlapping) {
                mergedStart = Math.min(mergedStart, existing.key.window().start());
                mergedEnd = Math.max(mergedEnd, existing.key.window().end());
                mergedReading += (Long) existing.value.aggregation().get("reading");
                store.remove(existing.key); // superseded by the merged window written below
            }
            mergedReading += (Long) record.value().get("reading");
            store.put(
                new Windowed<>(record.key(), new SessionWindow(mergedStart, mergedEnd)),
                AggregationWithHeaders.make(newValue(mergedReading), record.headers()));
            context.forward(record);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Shared infrastructure helpers (mirroring the KIP-1271 store integration tests)
    // ---------------------------------------------------------------------------------------------

    private static long alignedBase(long windowMs) {
        return (System.currentTimeMillis() / windowMs) * windowMs;
    }

    private void createTopics(String... topicNames) throws Exception {
        createTopicsWithPartitions(1, topicNames);
    }

    private void createTopicsWithPartitions(int numPartitions, String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.stream(topicNames)
                .map(name -> new NewTopic(name, numPartitions, (short) 1))
                .collect(Collectors.toList());
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        }
    }

    private GenericAvroSerde createKeySerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, true);
        return serde;
    }

    private GenericAvroSerde createValueSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, false);
        return serde;
    }

    private Properties createStreamsProps(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        return props;
    }

    private Properties createProducerProps() {
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

    private void produce(String topic, GenericRecord key, GenericRecord value, long timestamp)
        throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(topic, null, timestamp, key, value)).get();
            producer.flush();
        }
    }

    private void produceAll(String topic, List<GenericRecord> keys, List<GenericRecord> values,
        long timestamp) throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            for (int i = 0; i < keys.size(); i++) {
                producer.send(
                    new ProducerRecord<>(topic, null, timestamp, keys.get(i), values.get(i))).get();
            }
            producer.flush();
        }
    }

    private KafkaStreams startStreamsAndAwaitRunning(Topology topology, String appId) throws Exception {
        return startStreamsAndAwaitRunning(topology, appId, 30);
    }

    private KafkaStreams startStreamsAndAwaitRunning(Topology topology, String appId, int timeoutSeconds)
        throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        KafkaStreams streams = new KafkaStreams(topology, createStreamsProps(appId));
        streams.cleanUp();
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                startedLatch.countDown();
            }
        });
        streams.start();
        boolean running = false;
        try {
            running = startedLatch.await(timeoutSeconds, TimeUnit.SECONDS);
            assertTrue(running, "KafkaStreams should reach RUNNING state");
            return streams;
        } finally {
            if (!running) {
                closeStreams(streams);
            }
        }
    }

    private void closeStreams(KafkaStreams streams) {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
        }
    }

    private List<ConsumerRecord<GenericRecord, GenericRecord>> consumeRecords(
        String topic, String groupId, int expectedCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        List<ConsumerRecord<GenericRecord, GenericRecord>> results = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    results.add(record);
                }
            }
        }
        assertEquals(expectedCount, results.size(),
            "Expected " + expectedCount + " records from " + topic
                + " but got " + results.size() + " within 30s");
        return results;
    }

    private void assertSchemaIdHeaders(Headers headers, String context) {
        Header keySchemaIdHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keySchemaIdHeader, context + ": should have __key_schema_id header");
        byte[] keyHeaderBytes = keySchemaIdHeader.value();
        assertEquals(17, keyHeaderBytes.length, context + ": Key GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, keyHeaderBytes[0], context + ": Key header should have V1 magic byte");

        Header valueSchemaIdHeader = headers.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
        assertNotNull(valueSchemaIdHeader, context + ": should have __value_schema_id header");
        byte[] valueHeaderBytes = valueSchemaIdHeader.value();
        assertEquals(17, valueHeaderBytes.length, context + ": Value GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, valueHeaderBytes[0], context + ": Value header should have V1 magic byte");
    }

    private GenericRecord createKey(String sensorId) {
        GenericRecord key = new GenericData.Record(KEY_SCHEMA);
        key.put("sensorId", sensorId);
        return key;
    }

    private GenericRecord createValue(long reading) {
        return newValue(reading);
    }

    private static GenericRecord newValue(long reading) {
        GenericRecord value = new GenericData.Record(VALUE_SCHEMA);
        value.put("reading", reading);
        return value;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
