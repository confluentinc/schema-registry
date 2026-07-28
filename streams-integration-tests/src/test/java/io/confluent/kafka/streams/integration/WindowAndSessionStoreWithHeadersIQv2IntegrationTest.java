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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
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
 * <p>Shared cluster/serde/lifecycle infrastructure lives in {@link HeadersIQv2IntegrationTestBase}.
 */
public class WindowAndSessionStoreWithHeadersIQv2IntegrationTest
    extends HeadersIQv2IntegrationTestBase {

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(10);
    private static final Duration RETENTION_PERIOD = Duration.ofHours(1);
    // Retention period for the session store (long enough that no session under test expires).
    private static final Duration SESSION_RETENTION = Duration.ofMinutes(30);
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

    // Every record in each test shares this one key/value schema, so every value-bearing record
    // carries the same schema-id GUIDs. Capture them once when producing and assert that IQv2
    // results come back byte-equal to what was produced (see
    // HeadersIQv2IntegrationTestBase#assertSchemaIdHeaders).
    private CapturedSchemaIds valueSchemaIds;

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
                valueSchemaIds = sendAndCapture(producer, new ProducerRecord<>(input, null, w0, createKey("sensor-1"), createValue(0)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w1, createKey("sensor-1"), createValue(1)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w2, createKey("sensor-1"), createValue(2)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w2, createKey("sensor-1"), (GenericRecord) null));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w3, createKey("sensor-1"), createValue(3)));
                // Noise key sensor-2 must not leak into sensor-1's results; its w0 tombstone is key-scoped
                // and must not drop sensor-1's own w0.
                sendAndCapture(producer, new ProducerRecord<>(input, null, w0, createKey("sensor-2"), createValue(10)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w0, createKey("sensor-2"), (GenericRecord) null));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w1, createKey("sensor-2"), createValue(11)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w4, createKey("sensor-2"), createValue(14)));
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
                valueSchemaIds = sendAndCapture(producer, new ProducerRecord<>(input, null, w0, createKey("sensor-1"), createValue(0)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w1, createKey("sensor-1"), createValue(1)));
                // sensor-2: live in-range at w0, tombstoned in-range at w1, live out-of-range at w2.
                sendAndCapture(producer, new ProducerRecord<>(input, null, w0, createKey("sensor-2"), createValue(10)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w1, createKey("sensor-2"), createValue(11)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w1, createKey("sensor-2"), (GenericRecord) null));
                sendAndCapture(producer, new ProducerRecord<>(input, null, w2, createKey("sensor-2"), createValue(12)));
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
                valueSchemaIds = sendAndCapture(producer, new ProducerRecord<>(input, null, base, createKey("sensor-1"), createValue(1)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, base + 30, createKey("sensor-1"), createValue(2)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, base + 55, createKey("sensor-1"), createValue(3)));
                // A distant record (> gap away) forms a separate, zero-length session [base+500, base+500].
                sendAndCapture(producer, new ProducerRecord<>(input, null, base + 500, createKey("sensor-1"), createValue(4)));
                // Noise key sensor-2: a session written then tombstoned -- must not survive or leak.
                sendAndCapture(producer, new ProducerRecord<>(input, null, base, createKey("sensor-2"), createValue(20)));
                sendAndCapture(producer, new ProducerRecord<>(input, null, base, createKey("sensor-2"), (GenericRecord) null));
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
            valueSchemaIds = produce(input, createKey("sensor-1"), createValue(1), base);
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
            valueSchemaIds = produce(input, createKey("sensor-1"), createValue(42), base);
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
            valueSchemaIds = produceAll(input, keys, values, base).get(0);
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
            valueSchemaIds = produceAll(input, keys, values, base).get(0);
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
    // IQv2 query helpers
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
        assertSchemaIdHeaders(record.headers(), valueSchemaIds, context);
    }

    private void assertSessionRecord(ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> record,
        String sensorId, long start, long end, long reading, String context) {
        assertEquals(sensorId, record.key().key().get("sensorId").toString(), context + " key");
        assertEquals(start, record.key().window().start(), context + " session start");
        assertEquals(end, record.key().window().end(), context + " session end");
        // A session carries no per-record event-time; timestamp() is the session window's end.
        assertEquals(end, record.timestamp(), context + " timestamp == session end");
        assertEquals(reading, record.value().get("reading"), context + " value");
        assertSchemaIdHeaders(record.headers(), valueSchemaIds, context);
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
                Stores.persistentSessionStoreWithHeaders(storeName, SESSION_RETENTION),
                keySerde, valueSerde)
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
                Stores.persistentSessionStoreWithHeaders(storeName, SESSION_RETENTION),
                keySerde, valueSerde)
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
    // Avro record factories (schema-specific; the shared infrastructure lives in the base class)
    // ---------------------------------------------------------------------------------------------

    private static long alignedBase(long windowMs) {
        return (System.currentTimeMillis() / windowMs) * windowMs;
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
}
