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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateRestoreListener;
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
 * {@code MAGIC_BYTE_V1} GUID. Because every record shares one Avro key/value schema, the schema-id
 * GUIDs are byte-identical across records and cannot on their own prove a result carries <em>this</em>
 * record's headers; every produced record therefore also carries a distinct {@link #SEQ_HEADER}
 * ({@code seq}) header whose per-record value pins that fidelity.
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

    /**
     * A distinct user header attached to every produced record, carrying that record's own id
     * ({@code seq=<id>}). Because it differs per record -- unlike the schema-id GUIDs, which are
     * byte-identical for every record since they all share one Avro schema -- asserting it on each
     * IQv2 result proves the store returned <em>this</em> record's headers, not another record's.
     */
    private static final String SEQ_HEADER = "seq";

    // Per-record captures, keyed by the record's seq id, so each IQv2 result is asserted against its
    // own produced schema-id GUIDs (see HeadersIQv2IntegrationTestBase#assertSchemaIdHeaders) -- not
    // a single shared value that every record would trivially match.
    private final Map<String, CapturedSchemaIds> capturedBySeq = new HashMap<>();

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
        long base = baseTimestamp();
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
                send(producer, input, w0, "sensor-1", createValue(0), "s1-w0");
                send(producer, input, w1, "sensor-1", createValue(1), "s1-w1");
                send(producer, input, w2, "sensor-1", createValue(2), "s1-w2");
                send(producer, input, w2, "sensor-1", null, "s1-w2-tomb");
                send(producer, input, w3, "sensor-1", createValue(3), "s1-w3");
                // Noise key sensor-2 must not leak into sensor-1's results; its w0 tombstone is key-scoped
                // and must not drop sensor-1's own w0.
                send(producer, input, w0, "sensor-2", createValue(10), "s2-w0");
                send(producer, input, w0, "sensor-2", null, "s2-w0-tomb");
                send(producer, input, w1, "sensor-2", createValue(11), "s2-w1");
                send(producer, input, w4, "sensor-2", createValue(14), "s2-w4");
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
            assertWindowedRecord(records.get(0), "sensor-1", w0, 0L, "s1-w0", "winkey sensor-1 w0");
            assertWindowedRecord(records.get(1), "sensor-1", w1, 1L, "s1-w1", "winkey sensor-1 w1");
            assertWindowedRecord(records.get(2), "sensor-1", w3, 3L, "s1-w3", "winkey sensor-1 w3");

            // Sub-range [w1, w2]: only w1 (w0 below lower bound, w2 tombstoned, w3 above upper bound).
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> subRange = queryWindowed(
                streams, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(w1), Instant.ofEpochMilli(w2)),
                1);
            assertEquals(1, subRange.size(), "sub-range returns only w1");
            assertWindowedRecord(subRange.get(0), "sensor-1", w1, 1L, "s1-w1", "winkey sub-range w1");

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
            assertWindowedRecord(key2.get(0), "sensor-2", w1, 11L, "s2-w1", "winkey sensor-2 w1");
            assertWindowedRecord(key2.get(1), "sensor-2", w4, 14L, "s2-w4", "winkey sensor-2 w4");

            // Revival: re-put sensor-1's tombstoned w2 with a NEW record carrying a distinct seq. The
            // store must serve the re-put's headers, not the stale pre-tombstone w2 headers -- this is
            // where lingering headers would surface, so it proves a tombstoned entry revives with its
            // own headers.
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                send(producer, input, w2, "sensor-1", createValue(22), "s1-w2-revived");
                producer.flush();
            }
            consumeRecords(output, appId + "-revive", 10);
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> revived = queryWindowed(
                streams, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(w2), Instant.ofEpochMilli(w2)),
                1);
            assertEquals(1, revived.size(), "revived w2 returns exactly one record");
            assertWindowedRecord(revived.get(0), "sensor-1", w2, 22L, "s1-w2-revived", "winkey revived w2");

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
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
        long base = baseTimestamp();
        long w0 = base;
        long w1 = base + 100;
        long w2 = base + 200;

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                // sensor-1 across two windows, both live and in range.
                send(producer, input, w0, "sensor-1", createValue(0), "s1-w0");
                send(producer, input, w1, "sensor-1", createValue(1), "s1-w1");
                // sensor-2: live in-range at w0, tombstoned in-range at w1, live out-of-range at w2.
                send(producer, input, w0, "sensor-2", createValue(10), "s2-w0");
                send(producer, input, w1, "sensor-2", createValue(11), "s2-w1");
                send(producer, input, w1, "sensor-2", null, "s2-w1-tomb");
                send(producer, input, w2, "sensor-2", createValue(12), "s2-w2");
                producer.flush();
            }
            consumeRecords(output, appId + "-barrier", 6);

            // Range [w0, w1]: every key's windows whose start falls in the range.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = queryWindowed(
                streams, storeName,
                TimestampedWindowRangeWithHeadersQuery.withWindowStartRange(
                    Instant.ofEpochMilli(w0), Instant.ofEpochMilli(w1)),
                3);
            // Exactly the three expected windows come back: a spurious extra key would otherwise be
            // silently ignored by the per-sensor filters below.
            assertEquals(3, records.size(),
                "range returns exactly sensor-1 w0/w1 and sensor-2 w0");

            // sensor-1: both windows are in range.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> sensor1 =
                recordsForSensor(records, "sensor-1");
            assertEquals(Arrays.asList(w0, w1),
                sensor1.stream().map(r -> r.key().window().start()).collect(Collectors.toList()),
                "sensor-1 windows in range");
            assertWindowedRecord(sensor1.get(0), "sensor-1", w0, 0L, "s1-w0", "winrange sensor-1 w0");
            assertWindowedRecord(sensor1.get(1), "sensor-1", w1, 1L, "s1-w1", "winrange sensor-1 w1");

            // sensor-2: w0 survives; w1 is tombstoned; w2 is excluded by the range upper bound.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> sensor2 =
                recordsForSensor(records, "sensor-2");
            assertEquals(Arrays.asList(w0),
                sensor2.stream().map(r -> r.key().window().start()).collect(Collectors.toList()),
                "sensor-2 windows in range (w1 tombstoned, w2 out of range)");
            assertWindowedRecord(sensor2.get(0), "sensor-2", w0, 10L, "s2-w0", "winrange sensor-2 w0");

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
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
        long base = baseTimestamp();

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildSessionMergingTopology(input, output, storeName), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                // sensor-1: three records within the merge gap collapse into one variable-length session
                // [base, base+55] whose reading is the sum (1+2+3=6) and whose headers are the LAST
                // folded record's -- asserted via its distinct seq (s1-merge-3) below.
                send(producer, input, base, "sensor-1", createValue(1), "s1-merge-1");
                send(producer, input, base + 30, "sensor-1", createValue(2), "s1-merge-2");
                send(producer, input, base + 55, "sensor-1", createValue(3), "s1-merge-3");
                // A distant record (> gap away) forms a separate, zero-length session [base+500, base+500].
                send(producer, input, base + 500, "sensor-1", createValue(4), "s1-distant");
                // Noise key sensor-2: a session written then tombstoned -- must not survive or leak.
                send(producer, input, base, "sensor-2", createValue(20), "s2-sess");
                send(producer, input, base, "sensor-2", null, "s2-tomb");
                producer.flush();
            }
            consumeRecords(output, appId + "-barrier", 6);

            // sensor-1 has two variable-length sessions. The withKey session query documents no
            // iteration order, so sort locally (recordsForSensor) to assert the set order-independently.
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = recordsForSensor(
                queryWindowed(streams, storeName,
                    TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-1")), 2),
                "sensor-1");
            assertEquals(2, records.size(), "sensor-1 should have two sessions");
            // Merged, variable-length session [base, base+55], summed reading, last folded record's
            // headers (seq s1-merge-3).
            assertSessionRecord(records.get(0), "sensor-1", base, base + 55, 6L, "s1-merge-3",
                "session merged");
            // A separate zero-length session -- proves the end round-trips from the stored value.
            assertSessionRecord(records.get(1), "sensor-1", base + 500, base + 500, 4L, "s1-distant",
                "session zero-length");

            // sensor-2's only session was tombstoned -> empty; a never-written key is likewise empty.
            assertTrue(queryWindowed(streams, storeName,
                    TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-2")), 0).isEmpty(),
                "sensor-2's tombstoned session should be excluded");
            assertTrue(queryWindowed(streams, storeName,
                    TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-999")), 0).isEmpty(),
                "never-written key returns no sessions");

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
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
        long base = baseTimestamp();

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId);
        try {
            produceOne(input, base, "sensor-1", createValue(1), "s1-restore");
            consumeRecords(output, appId + "-pre", 1);
            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
        }

        // Restart with the same APPLICATION_ID; cleanUp() wipes the local state dir so the window
        // store must be rebuilt from the changelog. The restore listener proves the rebuild actually
        // happened -- cleanUp() clears local state but not committed offsets, so without it the
        // restart could silently reprocess the input and pass with no restore at all.
        AtomicLong restoredCount = new AtomicLong(0);
        KafkaStreams restored = startRestoredAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId, storeName, restoredCount);
        try {
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = queryWindowed(
                restored, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(base), Instant.ofEpochMilli(base)),
                1);
            assertEquals(1, records.size(), "exactly one restored window entry");
            assertWindowedRecord(records.get(0), "sensor-1", base, 1L, "s1-restore",
                "restored window sensor-1");
            // Pin that the store was genuinely rebuilt from the changelog, not repopulated by a
            // silent reprocess of the input topic.
            assertTrue(restoredCount.get() > 0,
                "window store should have been restored from the changelog (restored "
                    + restoredCount.get() + " records)");
            closeStreams(restored);
        } finally {
            closeStreamsQuietly(restored);
        }
    }

    @Test
    public void shouldRestoreSessionStoreFromChangelogPreservingHeaders() throws Exception {
        String input = "iqv2-sessrestore-input";
        String output = "iqv2-sessrestore-output";
        String storeName = "iqv2-sessrestore-store";
        String appId = "iqv2-sessrestore-test";

        createTopics(input, output);
        long base = baseTimestamp();

        // Use the merging writer so a multi-record (merged) AggregationWithHeaders -- not just a
        // single fixed-length session -- goes through the changelog and back.
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildSessionMergingTopology(input, output, storeName), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                // Three records within the merge gap -> one merged session [base, base+55], reading 6,
                // carrying the last folded record's headers (seq s1-sr-3).
                send(producer, input, base, "sensor-1", createValue(1), "s1-sr-1");
                send(producer, input, base + 30, "sensor-1", createValue(2), "s1-sr-2");
                send(producer, input, base + 55, "sensor-1", createValue(3), "s1-sr-3");
                producer.flush();
            }
            consumeRecords(output, appId + "-pre", 3);
            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
        }

        // Restart with the same APPLICATION_ID; the session store (backed by AggregationWithHeaders)
        // must be rebuilt from the changelog, and the restored merged session must still carry its
        // headers. The restore listener proves the rebuild happened rather than a silent reprocess.
        AtomicLong restoredCount = new AtomicLong(0);
        KafkaStreams restored = startRestoredAndAwaitRunning(
            buildSessionMergingTopology(input, output, storeName), appId, storeName, restoredCount);
        try {
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = recordsForSensor(
                queryWindowed(restored, storeName,
                    TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-1")), 1),
                "sensor-1");
            assertEquals(1, records.size(), "exactly one restored (merged) session");
            assertSessionRecord(records.get(0), "sensor-1", base, base + 55, 6L, "s1-sr-3",
                "restored merged session sensor-1");
            assertTrue(restoredCount.get() > 0,
                "session store should have been restored from the changelog (restored "
                    + restoredCount.get() + " records)");
            closeStreams(restored);
        } finally {
            closeStreamsQuietly(restored);
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
        long base = baseTimestamp();

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                for (int i = 1; i <= numKeys; i++) {
                    String sensorId = "sensor-" + i;
                    send(producer, input, base, sensorId, createValue(i), sensorId);
                }
                producer.flush();
            }
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

            // Assert the distinct key set, not just the count: returning one key twice and dropping
            // another would keep the size at numKeys but change the set.
            Set<String> expectedKeys = new HashSet<>();
            for (int i = 1; i <= numKeys; i++) {
                expectedKeys.add("sensor-" + i);
            }
            Set<String> actualKeys = all.stream()
                .map(r -> r.key().key().get("sensorId").toString())
                .collect(Collectors.toSet());
            assertEquals(numKeys, all.size(), "should read every key across partitions");
            assertEquals(expectedKeys, actualKeys,
                "should read every distinct key across partitions (none dropped or duplicated)");
            assertTrue(partitionsSeen.size() > 1,
                "window records should span more than one partition but saw: " + partitionsSeen);
            for (ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> r : all) {
                String sensorId = r.key().key().get("sensorId").toString();
                long reading = Long.parseLong(sensorId.substring("sensor-".length()));
                assertWindowedRecord(r, sensorId, base, reading, sensorId,
                    "window multi-partition " + sensorId);
            }

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
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
        long base = baseTimestamp();
        long sessionEnd = base + SESSION_DURATION_MS;

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildSessionTopology(input, output, storeName), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                for (int i = 1; i <= numKeys; i++) {
                    String sensorId = "sensor-" + i;
                    send(producer, input, base, sensorId, createValue(i), sensorId);
                }
                producer.flush();
            }
            consumeRecords(output, appId + "-barrier", numKeys);

            // A session withKey query routes to the key's partition, so query each key and record the
            // partition that served it; collectively the keys must resolve across >1 partition.
            Set<Integer> partitionsSeen = new HashSet<>();
            for (int i = 1; i <= numKeys; i++) {
                String sensorId = "sensor-" + i;
                Map.Entry<Integer, ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> served =
                    querySessionByKeyWithPartition(streams, storeName, createKey(sensorId));
                assertSessionRecord(served.getValue(), sensorId, base, sessionEnd, i, sensorId,
                    "session multi-partition " + sensorId);
                partitionsSeen.add(served.getKey());
            }
            assertTrue(partitionsSeen.size() > 1,
                "session keys should resolve across more than one partition but saw: " + partitionsSeen);

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Cache visibility, legacy-supplier controls, retain-duplicates, null-value, header-set,
    // read-only
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldNotServeWindowFromCacheBeforeFlush() throws Exception {
        String input = "iqv2-wincache-input";
        String output = "iqv2-wincache-output";
        String storeName = "iqv2-wincache-store";
        String appId = "iqv2-wincache-test";

        createTopics(input, output);
        long base = baseTimestamp();
        // Caching enabled + a very large commit interval so nothing flushes during the test: the record
        // lives only in the record cache. The headers-aware CachingWindowStore does not override IQv2
        // query(), so the query reads the still-empty persistent store and must return nothing. The
        // barrier proves the processor genuinely put the record -- it is present, just unflushed.
        int tenMinutes = (int) Duration.ofMinutes(10).toMillis();
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName, true, false), appId, 30, tenMinutes);
        try {
            produceOne(input, base, "sensor-1", createValue(1), "s1-cache");
            consumeRecords(output, appId + "-barrier", 1);

            assertTrue(queryWindowed(streams, storeName,
                    TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                        createKey("sensor-1"), Instant.ofEpochMilli(base), Instant.ofEpochMilli(base)), 0)
                    .isEmpty(),
                "cached-but-unflushed window write must be invisible to the IQv2 query");

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
        }
    }

    @Test
    public void shouldNotServeSessionFromCacheBeforeFlush() throws Exception {
        String input = "iqv2-sesscache-input";
        String output = "iqv2-sesscache-output";
        String storeName = "iqv2-sesscache-store";
        String appId = "iqv2-sesscache-test";

        createTopics(input, output);
        long base = baseTimestamp();
        // Same as the window case: CachingSessionStore does not override IQv2 query(), so a
        // cached-but-unflushed session write is invisible to the session query.
        int tenMinutes = (int) Duration.ofMinutes(10).toMillis();
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildSessionTopology(input, output, storeName, true), appId, 30, tenMinutes);
        try {
            produceOne(input, base, "sensor-1", createValue(1), "s1-cache");
            consumeRecords(output, appId + "-barrier", 1);

            assertTrue(queryWindowed(streams, storeName,
                    TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-1")), 0).isEmpty(),
                "cached-but-unflushed session write must be invisible to the IQv2 query");

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
        }
    }

    @Test
    public void shouldRetainDuplicateWindowsWithHeaders() throws Exception {
        String input = "iqv2-windup-input";
        String output = "iqv2-windup-output";
        String storeName = "iqv2-windup-store";
        String appId = "iqv2-windup-test";

        createTopics(input, output);
        long base = baseTimestamp();
        // retainDuplicates=true: two puts to the same key+window coexist (seqnum-suffixed) instead of
        // overwriting, and a null-value put is a no-op rather than a tombstone. (The builder also
        // disables caching under retained duplicates.)
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName, false, true), appId);
        try {
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                send(producer, input, base, "sensor-1", createValue(1), "s1-dup-1");
                send(producer, input, base, "sensor-1", createValue(2), "s1-dup-2");
                // A null value would tombstone a normal store, but is a no-op under retained duplicates.
                send(producer, input, base, "sensor-1", null, "s1-dup-tomb");
                producer.flush();
            }
            consumeRecords(output, appId + "-barrier", 3);

            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = queryWindowed(
                streams, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(base), Instant.ofEpochMilli(base)), 2);
            assertEquals(2, records.size(), "both duplicate windows are retained (null put was a no-op)");
            // Sort by reading so the assertion is independent of duplicate iteration order.
            records.sort(Comparator.comparingLong(
                (ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> r) ->
                    (Long) r.value().get("reading")));
            assertWindowedRecord(records.get(0), "sensor-1", base, 1L, "s1-dup-1", "windup first");
            assertWindowedRecord(records.get(1), "sensor-1", base, 2L, "s1-dup-2", "windup second");

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
        }
    }

    @Test
    public void shouldReturnReadOnlyHeaders() throws Exception {
        long base = baseTimestamp();

        // Window path.
        String winInput = "iqv2-rowin-input";
        String winOutput = "iqv2-rowin-output";
        String winStore = "iqv2-rowin-store";
        String winAppId = "iqv2-rowin-test";
        createTopics(winInput, winOutput);
        KafkaStreams winStreams = startStreamsAndAwaitRunning(
            buildWindowTopology(winInput, winOutput, winStore), winAppId);
        try {
            produceOne(winInput, base, "sensor-1", createValue(1), "s1-ro-win");
            consumeRecords(winOutput, winAppId + "-barrier", 1);
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> winRecords = queryWindowed(
                winStreams, winStore,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(base), Instant.ofEpochMilli(base)), 1);
            Headers winHeaders = winRecords.get(0).headers();
            assertThrows(IllegalStateException.class, () -> winHeaders.add("x", new byte[]{1}),
                "window query should return read-only headers");
            closeStreams(winStreams);
        } finally {
            closeStreamsQuietly(winStreams);
        }

        // Session path.
        String sessInput = "iqv2-rosess-input";
        String sessOutput = "iqv2-rosess-output";
        String sessStore = "iqv2-rosess-store";
        String sessAppId = "iqv2-rosess-test";
        createTopics(sessInput, sessOutput);
        KafkaStreams sessStreams = startStreamsAndAwaitRunning(
            buildSessionTopology(sessInput, sessOutput, sessStore), sessAppId);
        try {
            produceOne(sessInput, base, "sensor-1", createValue(1), "s1-ro-sess");
            consumeRecords(sessOutput, sessAppId + "-barrier", 1);
            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> sessRecords = queryWindowed(
                sessStreams, sessStore,
                TimestampedWindowRangeWithHeadersQuery.withKey(createKey("sensor-1")), 1);
            Headers sessHeaders = sessRecords.get(0).headers();
            assertThrows(IllegalStateException.class, () -> sessHeaders.add("x", new byte[]{1}),
                "session query should return read-only headers");
            closeStreams(sessStreams);
        } finally {
            closeStreamsQuietly(sessStreams);
        }
    }

    @Test
    public void shouldPreserveFullHeaderSetIncludingDuplicatesAndEmpty() throws Exception {
        String input = "iqv2-winheaderset-input";
        String output = "iqv2-winheaderset-output";
        String storeName = "iqv2-winheaderset-store";
        String appId = "iqv2-winheaderset-test";

        createTopics(input, output);
        long base = baseTimestamp();
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildWindowTopology(input, output, storeName), appId);
        try {
            // A header set no serde would produce on its own: an arbitrary user header, a duplicate key
            // ("trace" twice, distinct values) and a zero-length value. The serde adds the schema-id
            // headers during send(); snapshot the full produced set (in order) afterward so the
            // round-trip is asserted against exactly what was written -- count, order, duplicate key
            // and empty value included, and nothing spurious added or dropped.
            List<String> producedHeaders;
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                ProducerRecord<GenericRecord, GenericRecord> record = new ProducerRecord<>(
                    input, null, base, createKey("sensor-1"), createValue(1));
                record.headers().add(SEQ_HEADER, "s1-full".getBytes(StandardCharsets.UTF_8));
                record.headers().add("trace", "A".getBytes(StandardCharsets.UTF_8));
                record.headers().add("trace", "B".getBytes(StandardCharsets.UTF_8));
                record.headers().add("empty", new byte[0]);
                producer.send(record).get();
                producer.flush();
                producedHeaders = headerEntries(record.headers());
            }
            consumeRecords(output, appId + "-barrier", 1);

            List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records = queryWindowed(
                streams, storeName,
                TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
                    createKey("sensor-1"), Instant.ofEpochMilli(base), Instant.ofEpochMilli(base)), 1);
            assertEquals(producedHeaders, headerEntries(records.get(0).headers()),
                "window query should return the full produced header set -- order, the duplicate "
                    + "'trace' key, the empty value and the schema-id headers -- and nothing else");

            closeStreams(streams);
        } finally {
            closeStreamsQuietly(streams);
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
        boolean sawSuccess = false;
        String lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            out.clear();
            StateQueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName).withQuery(query));
            QueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> pr =
                result.getOnlyPartitionResult();
            if (pr != null && pr.isSuccess() && pr.getResult() != null) {
                sawSuccess = true;
                try (ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord> it = pr.getResult()) {
                    while (it.hasNext()) {
                        out.add(it.next());
                    }
                }
                if (out.size() >= expected) {
                    return out;
                }
            } else if (pr != null && pr.isFailure()) {
                lastFailure = pr.getFailureReason() + ": " + pr.getFailureMessage();
            }
            sleepQuietly(200);
        }
        // Require at least one successful query: otherwise an expected==0 caller would pass on a query
        // that failed on every attempt (e.g. an unsupported query type), reading as assertEquals(0, 0).
        assertTrue(sawSuccess, "IQv2 windowed query never succeeded within 30s"
            + (lastFailure != null ? " (last failure: " + lastFailure + ")" : ""));
        assertEquals(expected, out.size(), "IQv2 windowed query returned an unexpected count");
        return out;
    }

    /**
     * Runs a session {@code withKey} query (which routes to the key's partition) and returns the
     * partition id that served it together with the single session record. {@code streams.query}
     * opens an iterator for every local partition result eagerly, so this drains and closes every
     * partition's iterator each attempt -- returning early from inside the loop would leak the
     * iterators of the partitions not yet visited.
     */
    private Map.Entry<Integer, ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>>
        querySessionByKeyWithPartition(KafkaStreams streams, String storeName, GenericRecord key) {
        long deadline = System.currentTimeMillis() + 30_000;
        String lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            StateQueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(TimestampedWindowRangeWithHeadersQuery.withKey(key)));
            Map.Entry<Integer, ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> found = null;
            for (Map.Entry<Integer,
                    QueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>>> entry
                    : result.getPartitionResults().entrySet()) {
                QueryResult<ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord>> pr =
                    entry.getValue();
                if (pr.isSuccess() && pr.getResult() != null) {
                    try (ReadOnlyRecordIterator<Windowed<GenericRecord>, GenericRecord> it =
                             pr.getResult()) {
                        // Take the first match, but keep draining/closing the remaining partitions'
                        // iterators before returning.
                        if (found == null && it.hasNext()) {
                            found = new AbstractMap.SimpleEntry<>(entry.getKey(), it.next());
                        }
                    }
                } else if (pr.isFailure()) {
                    lastFailure = pr.getFailureReason() + ": " + pr.getFailureMessage();
                }
            }
            if (found != null) {
                return found;
            }
            sleepQuietly(200);
        }
        // Surface the IQ failure reason rather than dropping it, so a query that failed on every
        // attempt reports why instead of a bare "never returned a result".
        throw new AssertionError("session withKey query never returned a result for " + key
            + (lastFailure != null ? " (last failure: " + lastFailure + ")" : ""));
    }

    private void assertWindowedRecord(ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> record,
        String sensorId, long windowStart, long reading, String seq, String context) {
        assertEquals(sensorId, record.key().key().get("sensorId").toString(), context + " key");
        assertEquals(windowStart, record.key().window().start(), context + " window start");
        assertEquals(windowStart + EVENT_TIME_OFFSET_MS, record.timestamp(), context + " event-time");
        assertEquals(reading, record.value().get("reading"), context + " value");
        assertRecordHeaders(record, seq, context);
    }

    private void assertSessionRecord(ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> record,
        String sensorId, long start, long end, long reading, String seq, String context) {
        assertEquals(sensorId, record.key().key().get("sensorId").toString(), context + " key");
        assertEquals(start, record.key().window().start(), context + " session start");
        assertEquals(end, record.key().window().end(), context + " session end");
        // A session carries no per-record event-time; timestamp() is the session window's end.
        assertEquals(end, record.timestamp(), context + " timestamp == session end");
        assertEquals(reading, record.value().get("reading"), context + " value");
        assertRecordHeaders(record, seq, context);
    }

    /**
     * Asserts the record carries the exact headers produced for {@code seq}: the schema-id GUIDs
     * byte-equal to what that record's serializer wrote, and the distinct {@code seq} header equal to
     * the id passed here. The {@code seq} check is what makes this a per-record fidelity assertion --
     * every record shares one Avro schema, so the schema-id GUIDs alone are byte-identical across
     * records and could not distinguish one record's headers from another's.
     */
    private void assertRecordHeaders(ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> record,
        String seq, String context) {
        CapturedSchemaIds expected = capturedBySeq.get(seq);
        assertNotNull(expected, context + ": no captured schema-id GUIDs for seq " + seq);
        assertSchemaIdHeaders(record.headers(), expected, context);
        Header seqHeader = record.headers().lastHeader(SEQ_HEADER);
        assertNotNull(seqHeader, context + ": missing " + SEQ_HEADER + " header");
        assertEquals(seq, new String(seqHeader.value(), StandardCharsets.UTF_8),
            context + ": " + SEQ_HEADER + " header should carry this record's own id");
    }

    private List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> recordsForSensor(
        List<ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord>> records, String sensorId) {
        return records.stream()
            .filter(r -> sensorId.equals(r.key().key().get("sensorId").toString()))
            .sorted(Comparator.comparingLong(
                (ReadOnlyRecord<Windowed<GenericRecord>, GenericRecord> r) -> r.key().window().start()))
            .collect(Collectors.toList());
    }

    /**
     * Renders headers as an ordered {@code key=base64(value)} list, preserving insertion order and
     * duplicate keys, so two header sets can be compared for exact equality -- count, order,
     * duplicates and byte-exact values (a zero-length value base64s to {@code ""}).
     */
    private static List<String> headerEntries(Headers headers) {
        List<String> entries = new ArrayList<>();
        for (Header h : headers) {
            entries.add(h.key() + "=" + (h.value() == null
                ? "<null>" : Base64.getEncoder().encodeToString(h.value())));
        }
        return entries;
    }

    // ---------------------------------------------------------------------------------------------
    // Producing (attaches a distinct per-record seq header and captures the produced GUIDs)
    // ---------------------------------------------------------------------------------------------

    /**
     * Sends one record carrying a distinct {@link #SEQ_HEADER} ({@code seq}) so each IQv2 result can
     * be asserted against its own produced record -- not a single shared value every record would
     * trivially match -- and captures the schema-id GUIDs the serializer wrote, keyed by {@code seq}.
     * A null {@code value} produces a tombstone (no value GUID captured); its seq is never asserted.
     */
    private void send(KafkaProducer<GenericRecord, GenericRecord> producer, String topic,
        long timestamp, String sensorId, GenericRecord value, String seq) throws Exception {
        ProducerRecord<GenericRecord, GenericRecord> record =
            new ProducerRecord<>(topic, null, timestamp, createKey(sensorId), value);
        record.headers().add(SEQ_HEADER, seq.getBytes(StandardCharsets.UTF_8));
        capturedBySeq.put(seq, sendAndCapture(producer, record));
    }

    /** Produces a single record (with its own producer) via {@link #send}. */
    private void produceOne(String topic, long timestamp, String sensorId, GenericRecord value,
        String seq) throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            send(producer, topic, timestamp, sensorId, value, seq);
            producer.flush();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Streams lifecycle helpers specific to this test
    // ---------------------------------------------------------------------------------------------

    /**
     * Starts a KafkaStreams that must rebuild its store from the changelog and returns it once
     * RUNNING, wiring a {@link StateRestoreListener} that sums the records restored for
     * {@code storeName} into {@code restoredCount} -- so a test can prove the store was genuinely
     * restored, not silently reprocessed from the input topic. The base start helper cannot set a
     * restore listener, so the instance is started inline here.
     */
    private KafkaStreams startRestoredAndAwaitRunning(Topology topology, String appId,
        String storeName, AtomicLong restoredCount) throws Exception {
        StateRestoreListener restoreListener = new StateRestoreListener() {
            @Override
            public void onRestoreStart(TopicPartition tp, String store, long start, long end) {
            }

            @Override
            public void onBatchRestored(TopicPartition tp, String store, long end, long batch) {
            }

            @Override
            public void onRestoreEnd(TopicPartition tp, String store, long totalRestored) {
                if (store.equals(storeName)) {
                    restoredCount.addAndGet(totalRestored);
                }
            }
        };
        KafkaStreams restored = new KafkaStreams(topology, createStreamsProps(appId, null));
        boolean running = false;
        try {
            restored.cleanUp();
            restored.setGlobalStateRestoreListener(restoreListener);
            CountDownLatch runningLatch = new CountDownLatch(1);
            restored.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    runningLatch.countDown();
                }
            });
            restored.start();
            assertTrue(runningLatch.await(90, TimeUnit.SECONDS),
                "restored KafkaStreams should reach RUNNING within 90s");
            running = true;
            return restored;
        } finally {
            if (!running) {
                closeStreamsQuietly(restored);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Topologies + processors
    // ---------------------------------------------------------------------------------------------

    private Topology buildWindowTopology(String input, String output, String storeName) {
        return buildWindowTopology(input, output, storeName, false, false);
    }

    private Topology buildWindowTopology(String input, String output, String storeName,
        boolean cachingEnabled, boolean retainDuplicates) {
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        StoreBuilder<TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord>> storeBuilder =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(
                    storeName, RETENTION_PERIOD, WINDOW_SIZE, retainDuplicates),
                keySerde, valueSerde);
        storeBuilder = cachingEnabled
            ? storeBuilder.withCachingEnabled()
            : storeBuilder.withCachingDisabled();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(storeBuilder)
            .stream(input, Consumed.with(keySerde, valueSerde))
            .process(() -> new WindowPutProcessor(storeName), storeName)
            .to(output, Produced.with(keySerde, valueSerde));
        return builder.build();
    }

    private Topology buildSessionTopology(String input, String output, String storeName) {
        return buildSessionTopology(input, output, storeName, false);
    }

    private Topology buildSessionTopology(String input, String output, String storeName,
        boolean cachingEnabled) {
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        StoreBuilder<SessionStoreWithHeaders<GenericRecord, GenericRecord>> storeBuilder =
            Stores.sessionStoreWithHeadersBuilder(
                Stores.persistentSessionStoreWithHeaders(storeName, SESSION_RETENTION),
                keySerde, valueSerde);
        storeBuilder = cachingEnabled
            ? storeBuilder.withCachingEnabled()
            : storeBuilder.withCachingDisabled();

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

    /**
     * A recent base event-time for the produced records. It must be recent -- within the stores'
     * retention ({@link #RETENTION_PERIOD} / {@link #SESSION_RETENTION}) -- so no entry expires
     * before it is queried. It is deliberately not aligned to the window size: every processor
     * derives its window/session bounds from {@code record.timestamp()} directly, so aligning the
     * base to a window boundary would have no effect on where the windows fall.
     */
    private static long baseTimestamp() {
        return System.currentTimeMillis();
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
