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

import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.TimestampedKeyWithHeadersQuery;
import org.apache.kafka.streams.query.TimestampedRangeWithHeadersQuery;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;

/**
 * KIP-1356 IQv2 integration test for the timestamped key-value store with headers.
 *
 * <p>Verifies that the headers-aware IQv2 query types —
 * {@link TimestampedKeyWithHeadersQuery} (point) and {@link TimestampedRangeWithHeadersQuery}
 * (range / scan) — return the record headers persisted by a KIP-1271
 * {@link TimestampedKeyValueStoreWithHeaders}. In particular, records are produced with the
 * schema-id GUID carried in the {@code __key_schema_id} / {@code __value_schema_id} headers (via
 * {@link HeaderSchemaIdSerializer}); this test asserts those 17-byte {@code MAGIC_BYTE_V1} GUID
 * headers survive the store and come back through each IQv2 query as a
 * {@link ReadOnlyRecord} / {@link ReadOnlyRecordIterator}.
 *
 * <p>Shared cluster/serde/lifecycle infrastructure lives in {@link HeadersIQv2IntegrationTestBase}.
 */
public class TimestampedKeyValueStoreWithHeadersIQv2IntegrationTest
    extends HeadersIQv2IntegrationTestBase {

    private static final String KEY_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"WordKey\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":["
            + "  {\"name\":\"word\",\"type\":\"string\"}"
            + "]"
            + "}";

    private static final String VALUE_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"WordValue\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":["
            + "  {\"name\":\"count\",\"type\":\"long\"}"
            + "]"
            + "}";

    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    /**
     * A distinct user header attached to every produced record, carrying that record's own key
     * ({@code seq=<word>}). Because it differs per record -- unlike the schema-id GUIDs, which are
     * byte-identical for every record since they all share one schema -- asserting it on each IQv2
     * result proves the store returned <em>this</em> record's headers, not another record's.
     */
    private static final String SEQ_HEADER = "seq";

    // Per-record captures, keyed by word, so each IQv2 result is asserted against its own record's
    // produced schema-id GUIDs (see HeadersIQv2IntegrationTestBase#assertSchemaIdHeaders) and its
    // own produced timestamp -- not a single shared value that every record would trivially match.
    private final Map<String, CapturedSchemaIds> capturedByWord = new HashMap<>();
    private final Map<String, Long> timestampByWord = new HashMap<>();

    // A fixed, explicit base timestamp so each record's timestamp is known and assertable.
    private static final long BASE_TIMESTAMP = 1_600_000_000_000L;

    // ---------------------------------------------------------------------------------------------
    // TimestampedKeyWithHeadersQuery (point)
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldQueryPointWithHeaders() throws Exception {
        String input = "iqv2-point-input";
        String output = "iqv2-point-output";
        String storeName = "iqv2-point-store";
        String appId = "iqv2-point-test";

        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Arrays.asList("word-1", "word-2", "word-3"), Arrays.asList(10L, 20L, 30L),
            true, null);
        try {
            assertPointQuery(streams, storeName, "word-1", 10L, false);
            assertPointQuery(streams, storeName, "word-2", 20L, false);
            assertPointQuery(streams, storeName, "word-3", 30L, false);
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldQueryPointSkipCache() throws Exception {
        String input = "iqv2-skipcache-input";
        String output = "iqv2-skipcache-output";
        String storeName = "iqv2-skipcache-store";
        String appId = "iqv2-skipcache-test";

        // Low commit interval so the cache flushes into the underlying store, making the
        // skipCache (store-served) read visible.
        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Arrays.asList("word-1", "word-2"), Arrays.asList(10L, 20L),
            true, 500);
        try {
            assertPointQuery(streams, storeName, "word-1", 10L, true);
            assertPointQuery(streams, storeName, "word-2", 20L, true);
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldReturnNullAfterTombstoningHeaderedKey() throws Exception {
        String input = "iqv2-tombstone-input";
        String output = "iqv2-tombstone-output";
        String storeName = "iqv2-tombstone-store";
        String appId = "iqv2-tombstone-test";

        createTopics(input, output);
        // Caching enabled + default commit interval: nothing flushes during the test, so the put and
        // the later delete are both served from the record cache (read-your-writes/read-your-deletes).
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildTopology(input, output, storeName, true), appId, 30, null);
        try {
            // Populate word-1 and confirm the header-aware store serves it back with its schema-id
            // headers -- establishing that there is a genuinely headered entry to remove.
            produceRecords(input, Collections.singletonList("word-1"), Collections.singletonList(10L));
            consumeRecords(output, appId + "-pre", 1);
            assertPointQuery(streams, storeName, "word-1", 10L, false);

            // Tombstone word-1 (null value). The header-aware store must drop the previously-headered
            // entry so the point query stops returning it.
            produceTombstone(input, "word-1");
            consumeRecords(output, appId + "-post", 2);
            assertPointReturnsNull(streams, storeName, "word-1", "tombstoned key", false);

            // A key that was never written is likewise absent.
            assertPointReturnsNull(streams, storeName, "no-such-word", "never-written key", false);

            // Re-put word-1 after the tombstone with a NEW record carrying a distinguishing header
            // (phase=after). The pre-tombstone record had no such header, so a store that lingered
            // the old headers instead of taking the re-put's would fail the phase assertion below --
            // this pins that a re-put comes back with its own headers, not the pre-tombstone ones.
            long reputTs = BASE_TIMESTAMP + 100;
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                ProducerRecord<GenericRecord, GenericRecord> reput = new ProducerRecord<>(
                    input, null, reputTs, createKey("word-1"), createValue(99L));
                reput.headers().add(SEQ_HEADER, "word-1".getBytes(StandardCharsets.UTF_8));
                reput.headers().add("phase", "after".getBytes(StandardCharsets.UTF_8));
                capturedByWord.put("word-1", sendAndCapture(producer, reput));
                timestampByWord.put("word-1", reputTs);
                producer.flush();
            }
            consumeRecords(output, appId + "-reput", 3);

            ReadOnlyRecord<GenericRecord, GenericRecord> afterReput =
                queryPointExpectPresent(streams, storeName, createKey("word-1"), false);
            assertEquals(99L, afterReput.value().get("count"), "re-put word-1 value");
            assertRecordHeaders(afterReput, "word-1", "re-put word-1");
            Header phase = afterReput.headers().lastHeader("phase");
            assertNotNull(phase, "re-put word-1: should carry the new record's phase header");
            assertEquals("after", new String(phase.value(), StandardCharsets.UTF_8),
                "re-put word-1: query should return the re-put's headers, not the pre-tombstone ones");
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldServeCacheHitBeforeFlushWithHeaders() throws Exception {
        String input = "iqv2-cachehit-input";
        String output = "iqv2-cachehit-output";
        String storeName = "iqv2-cachehit-store";
        String appId = "iqv2-cachehit-test";

        createTopics(input, output);
        // Caching enabled + a very large commit interval so nothing is committed/flushed during the
        // test: the record lives only in the record cache (the persistent store stays empty).
        int tenMinutes = (int) Duration.ofMinutes(10).toMillis();
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildTopology(input, output, storeName, true), appId, 30, tenMinutes);
        try {
            produceRecords(input, Collections.singletonList("word-1"), Collections.singletonList(10L));
            consumeRecords(output, appId + "-barrier", 1);

            // Read-your-writes: the not-yet-flushed record is served from the cache, with headers.
            assertPointQuery(streams, storeName, "word-1", 10L, false);

            // skipCache bypasses the cache and reads the persistent store, which is still empty; a
            // successful null result positively proves the read above was genuinely cache-served (and
            // covers skipCache). Asserting success first stops a failed skipCache query passing as null.
            assertPointReturnsNull(streams, storeName, "word-1",
                "skipCache before flush (empty store)", true);
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldReturnReadOnlyHeaders() throws Exception {
        String input = "iqv2-readonly-input";
        String output = "iqv2-readonly-output";
        String storeName = "iqv2-readonly-store";
        String appId = "iqv2-readonly-test";

        // Caching disabled so the range query (which bypasses the cache) sees the store-served record.
        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Collections.singletonList("word-1"), Collections.singletonList(10L), false, null);
        try {
            // The query marks the returned headers read-only on both the point and range paths, so
            // an attempt to mutate them must throw.
            ReadOnlyRecord<GenericRecord, GenericRecord> point =
                queryPointExpectPresent(streams, storeName, createKey("word-1"), false);
            assertThrows(IllegalStateException.class,
                () -> point.headers().add("x", new byte[]{1}),
                "point query should return read-only headers");

            List<ReadOnlyRecord<GenericRecord, GenericRecord>> range = queryRange(
                streams, storeName, TimestampedRangeWithHeadersQuery.withNoBounds(), 1);
            assertThrows(IllegalStateException.class,
                () -> range.get(0).headers().add("x", new byte[]{1}),
                "range query should return read-only headers");
        } finally {
            closeStreams(streams);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // TimestampedRangeWithHeadersQuery (range / scan)
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldQueryRangeWithBounds() throws Exception {
        String input = "iqv2-range-input";
        String output = "iqv2-range-output";
        String storeName = "iqv2-range-store";
        String appId = "iqv2-range-test";

        // Caching disabled: a range header-query reads the store directly (it never consults the
        // cache), so writes must be store-served -- this makes them visible immediately.
        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Arrays.asList("word-1", "word-2", "word-3", "word-4", "word-5"),
            Arrays.asList(10L, 20L, 30L, 40L, 50L),
            false, null);
        try {
            List<ReadOnlyRecord<GenericRecord, GenericRecord>> records = queryRange(
                streams, storeName,
                TimestampedRangeWithHeadersQuery
                    .<GenericRecord, GenericRecord>withRange(createKey("word-2"), createKey("word-4"))
                    .withAscendingKeys(),
                3);
            List<String> words = wordsOf(records);
            assertEquals(Arrays.asList("word-2", "word-3", "word-4"), words, "range [word-2, word-4]");
            assertHeadersOnEach(records, "IQv2 range");
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldQueryRangeAscendingAndDescending() throws Exception {
        String input = "iqv2-order-input";
        String output = "iqv2-order-output";
        String storeName = "iqv2-order-store";
        String appId = "iqv2-order-test";

        // Caching disabled: range header-queries bypass the cache and read the store directly.
        // Produce the keys out of order so the asc/desc assertions prove the query orders results by
        // serialized key, not by insertion order.
        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Arrays.asList("word-2", "word-3", "word-1"), Arrays.asList(20L, 30L, 10L),
            false, null);
        try {
            List<ReadOnlyRecord<GenericRecord, GenericRecord>> asc = queryRange(
                streams, storeName,
                TimestampedRangeWithHeadersQuery
                    .<GenericRecord, GenericRecord>withNoBounds().withAscendingKeys(),
                3);
            assertEquals(Arrays.asList("word-1", "word-2", "word-3"), wordsOf(asc), "ascending keys");
            assertHeadersOnEach(asc, "IQv2 ascending");

            List<ReadOnlyRecord<GenericRecord, GenericRecord>> desc = queryRange(
                streams, storeName,
                TimestampedRangeWithHeadersQuery
                    .<GenericRecord, GenericRecord>withNoBounds().withDescendingKeys(),
                3);
            assertEquals(Arrays.asList("word-3", "word-2", "word-1"), wordsOf(desc), "descending keys");
            assertHeadersOnEach(desc, "IQv2 descending");

            // Bounded + descending: the ordering flip must also apply to a bounded range, not just a
            // full scan -- guards against an ordering bug that only surfaces on the bounded path.
            List<ReadOnlyRecord<GenericRecord, GenericRecord>> boundedDesc = queryRange(
                streams, storeName,
                TimestampedRangeWithHeadersQuery
                    .<GenericRecord, GenericRecord>withRange(createKey("word-1"), createKey("word-2"))
                    .withDescendingKeys(),
                2);
            assertEquals(Arrays.asList("word-2", "word-1"), wordsOf(boundedDesc),
                "bounded descending [word-1, word-2]");
            assertHeadersOnEach(boundedDesc, "IQv2 bounded descending");
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldScanAllWithNoBounds() throws Exception {
        String input = "iqv2-scan-input";
        String output = "iqv2-scan-output";
        String storeName = "iqv2-scan-store";
        String appId = "iqv2-scan-test";

        // Caching disabled: range header-queries bypass the cache and read the store directly.
        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Arrays.asList("word-1", "word-2", "word-3"), Arrays.asList(10L, 20L, 30L),
            false, null);
        try {
            List<ReadOnlyRecord<GenericRecord, GenericRecord>> records = queryRange(
                streams, storeName, TimestampedRangeWithHeadersQuery.withNoBounds(), 3);
            // Compare as a sorted List (not a Set) so a scan that emitted a duplicate key would fail.
            List<String> words = wordsOf(records);
            Collections.sort(words);
            assertEquals(Arrays.asList("word-1", "word-2", "word-3"), words,
                "scan should return exactly the three keys, each once");
            assertHeadersOnEach(records, "IQv2 scan");
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldQueryLowerBoundOnly() throws Exception {
        String input = "iqv2-lower-input";
        String output = "iqv2-lower-output";
        String storeName = "iqv2-lower-store";
        String appId = "iqv2-lower-test";

        // Caching disabled: range header-queries bypass the cache and read the store directly.
        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Arrays.asList("word-1", "word-2", "word-3", "word-4"),
            Arrays.asList(10L, 20L, 30L, 40L),
            false, null);
        try {
            List<ReadOnlyRecord<GenericRecord, GenericRecord>> records = queryRange(
                streams, storeName,
                TimestampedRangeWithHeadersQuery
                    .<GenericRecord, GenericRecord>withLowerBound(createKey("word-3"))
                    .withAscendingKeys(),
                2);
            assertEquals(Arrays.asList("word-3", "word-4"), wordsOf(records), "lower bound word-3");
            assertHeadersOnEach(records, "IQv2 lower bound");
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldQueryUpperBoundOnly() throws Exception {
        String input = "iqv2-upper-input";
        String output = "iqv2-upper-output";
        String storeName = "iqv2-upper-store";
        String appId = "iqv2-upper-test";

        // Caching disabled: range header-queries bypass the cache and read the store directly.
        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Arrays.asList("word-1", "word-2", "word-3", "word-4"),
            Arrays.asList(10L, 20L, 30L, 40L),
            false, null);
        try {
            List<ReadOnlyRecord<GenericRecord, GenericRecord>> records = queryRange(
                streams, storeName,
                TimestampedRangeWithHeadersQuery
                    .<GenericRecord, GenericRecord>withUpperBound(createKey("word-2"))
                    .withAscendingKeys(),
                2);
            assertEquals(Arrays.asList("word-1", "word-2"), wordsOf(records), "upper bound word-2");
            assertHeadersOnEach(records, "IQv2 upper bound");
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldReturnEmptyForRangeWithNoMatches() throws Exception {
        String input = "iqv2-emptyrange-input";
        String output = "iqv2-emptyrange-output";
        String storeName = "iqv2-emptyrange-store";
        String appId = "iqv2-emptyrange-test";

        // Caching disabled: range header-queries bypass the cache and read the store directly.
        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Arrays.asList("word-1", "word-2", "word-3"), Arrays.asList(10L, 20L, 30L),
            false, null);
        try {
            // A lower bound past the last stored key selects nothing: the query must still succeed
            // and return an empty iterator -- not fail, and not spuriously return the stored keys.
            List<ReadOnlyRecord<GenericRecord, GenericRecord>> none = queryRange(
                streams, storeName,
                TimestampedRangeWithHeadersQuery.withLowerBound(createKey("word-9")), 0);
            assertTrue(none.isEmpty(),
                "range with a lower bound past the last key should return no records");
        } finally {
            closeStreams(streams);
        }
    }

    @Test
    public void shouldNotServeRangeFromCacheBeforeFlush() throws Exception {
        String input = "iqv2-rangecache-input";
        String output = "iqv2-rangecache-output";
        String storeName = "iqv2-rangecache-store";
        String appId = "iqv2-rangecache-test";

        createTopics(input, output);
        // Caching enabled + a very large commit interval so nothing flushes during the test: the
        // records live only in the record cache; the persistent store stays empty.
        int tenMinutes = (int) Duration.ofMinutes(10).toMillis();
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildTopology(input, output, storeName, true), appId, 30, tenMinutes);
        try {
            produceRecords(input, Arrays.asList("word-1", "word-2", "word-3"),
                Arrays.asList(10L, 20L, 30L));
            consumeRecords(output, appId + "-barrier", 3);

            // Positive control: a point query serves the not-yet-flushed records from the cache, so
            // they are genuinely present -- just unflushed.
            assertPointQuery(streams, storeName, "word-1", 10L, false);

            // The range path bypasses the cache and reads the still-empty persistent store, so it
            // must return nothing. This turns the "range never consults the cache" comment on the
            // range tests into an asserted guarantee, catching a future change that starts consulting
            // the cache. Emptiness is asserted from a single successful query -- there is nothing to
            // wait for.
            StateQueryResult<ReadOnlyRecordIterator<GenericRecord, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(TimestampedRangeWithHeadersQuery.withNoBounds()));
            QueryResult<ReadOnlyRecordIterator<GenericRecord, GenericRecord>> pr =
                result.getOnlyPartitionResult();
            assertNotNull(pr, "range query should return a partition result");
            assertTrue(pr.isSuccess(), "range query should succeed but failed: "
                + (pr.isFailure() ? pr.getFailureReason() + ": " + pr.getFailureMessage() : ""));
            List<String> served = new ArrayList<>();
            ReadOnlyRecordIterator<GenericRecord, GenericRecord> iter = pr.getResult();
            if (iter != null) {
                try (ReadOnlyRecordIterator<GenericRecord, GenericRecord> it = iter) {
                    while (it.hasNext()) {
                        served.add(it.next().key().get("word").toString());
                    }
                }
            }
            assertTrue(served.isEmpty(),
                "range query must not serve unflushed cached entries but returned: " + served);
        } finally {
            closeStreams(streams);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Restore-from-changelog and multi-partition
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldRestoreFromChangelogThenQueryIQv2WithHeaders() throws Exception {
        String input = "iqv2-restore-input";
        String output = "iqv2-restore-output";
        String storeName = "iqv2-restore-store";
        String appId = "iqv2-restore-test";

        createTopics(input, output);

        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildTopology(input, output, storeName, true), appId, 30, null);
        try {
            produceRecords(input, Arrays.asList("word-1", "word-2", "word-3"),
                Arrays.asList(10L, 20L, 30L));
            consumeRecords(output, "iqv2-restore-pre", 3);
        } finally {
            closeStreams(streams);
        }

        // Restart with the same APPLICATION_ID; cleanUp() wipes the local state dir so the store must
        // be rebuilt from the changelog. Attach a StateRestoreListener BEFORE start() to prove the
        // rebuild actually happened: cleanUp() clears local state but not committed offsets, so
        // without this the restart could silently reprocess the input and pass with no restore at all.
        // (The base start helper can't set a restore listener, so this instance is started inline.)
        AtomicLong restoredCount = new AtomicLong(0);
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
        KafkaStreams restored = new KafkaStreams(
            buildTopology(input, output, storeName, true), createStreamsProps(appId, null));
        restored.cleanUp();
        restored.setGlobalStateRestoreListener(restoreListener);
        CountDownLatch restoredRunning = new CountDownLatch(1);
        restored.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                restoredRunning.countDown();
            }
        });
        restored.start();
        try {
            assertTrue(restoredRunning.await(90, TimeUnit.SECONDS),
                "restored KafkaStreams should reach RUNNING");

            ReadOnlyRecord<GenericRecord, GenericRecord> word1 =
                queryPointExpectPresent(restored, storeName, createKey("word-1"), false);
            assertEquals(10L, word1.value().get("count"), "restored word-1 value");
            assertRecordHeaders(word1, "word-1", "restored word-1");

            List<ReadOnlyRecord<GenericRecord, GenericRecord>> all = queryRange(
                restored, storeName, TimestampedRangeWithHeadersQuery.withNoBounds(), 3);
            assertEquals(new HashSet<>(Arrays.asList("word-1", "word-2", "word-3")),
                new HashSet<>(wordsOf(all)), "restored scan keys");
            assertHeadersOnEach(all, "restored scan");

            // Pin that the store was genuinely rebuilt from the changelog, not repopulated by a
            // silent reprocess of the input topic.
            assertTrue(restoredCount.get() > 0,
                "store should have been restored from the changelog (restored "
                    + restoredCount.get() + " records)");
        } finally {
            closeStreams(restored);
        }
    }

    @Test
    public void shouldQueryAcrossPartitionsWithHeaders() throws Exception {
        String input = "iqv2-mp-input";
        String output = "iqv2-mp-output";
        String storeName = "iqv2-mp-store";
        String appId = "iqv2-mp-test";
        int numPartitions = 3;
        int numKeys = 9;

        createTopicsWithPartitions(numPartitions, input, output);

        List<String> words = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        for (int i = 1; i <= numKeys; i++) {
            words.add("word-" + i);
            counts.add(i * 10L);
        }

        // Caching disabled: the scan reads each partition's store directly, so writes are visible
        // immediately without waiting for a cache flush.
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildTopology(input, output, storeName, false), appId, 30, null);
        try {
            produceRecords(input, words, counts);
            consumeRecords(output, "iqv2-mp-consumer", numKeys);

            // Scan every locally-available partition; collect and assert headers on every record.
            List<ReadOnlyRecord<GenericRecord, GenericRecord>> all = new ArrayList<>();
            Set<Integer> partitionsSeen = new HashSet<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                all.clear();
                partitionsSeen.clear();
                StateQueryResult<ReadOnlyRecordIterator<GenericRecord, GenericRecord>> result =
                    streams.query(StateQueryRequest.inStore(storeName)
                        .withQuery(TimestampedRangeWithHeadersQuery.withNoBounds()));
                for (Map.Entry<Integer, QueryResult<ReadOnlyRecordIterator<GenericRecord, GenericRecord>>> e :
                    result.getPartitionResults().entrySet()) {
                    QueryResult<ReadOnlyRecordIterator<GenericRecord, GenericRecord>> pr = e.getValue();
                    if (pr.isSuccess() && pr.getResult() != null) {
                        try (ReadOnlyRecordIterator<GenericRecord, GenericRecord> it = pr.getResult()) {
                            while (it.hasNext()) {
                                all.add(it.next());
                                partitionsSeen.add(e.getKey());
                            }
                        }
                    }
                }
                if (all.size() >= numKeys) {
                    break;
                }
                sleepQuietly(200);
            }

            assertEquals(numKeys, all.size(), "should read every key across partitions");
            assertTrue(partitionsSeen.size() > 1,
                "records should span more than one partition but saw: " + partitionsSeen);
            assertHeadersOnEach(all, "IQv2 multi-partition scan");
        } finally {
            closeStreams(streams);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // IQv2 query helpers
    // ---------------------------------------------------------------------------------------------

    private ReadOnlyRecord<GenericRecord, GenericRecord> queryPointExpectPresent(
        KafkaStreams streams, String storeName, GenericRecord key, boolean skipCache) {
        TimestampedKeyWithHeadersQuery<GenericRecord, GenericRecord> query =
            TimestampedKeyWithHeadersQuery.withKey(key);
        if (skipCache) {
            query = query.skipCache();
        }
        long deadline = System.currentTimeMillis() + 30_000;
        String lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            StateQueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName).withQuery(query));
            QueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> pr = result.getOnlyPartitionResult();
            if (pr != null && pr.isSuccess() && pr.getResult() != null) {
                return pr.getResult();
            }
            if (pr != null && pr.isFailure()) {
                lastFailure = pr.getFailureReason() + ": " + pr.getFailureMessage();
            }
            sleepQuietly(200);
        }
        throw new AssertionError("IQv2 point query never returned a result for key " + key
            + (lastFailure != null ? " (last failure: " + lastFailure + ")" : ""));
    }

    private void assertPointQuery(KafkaStreams streams, String storeName, String word,
        long expectedCount, boolean skipCache) {
        ReadOnlyRecord<GenericRecord, GenericRecord> record =
            queryPointExpectPresent(streams, storeName, createKey(word), skipCache);
        String context = "IQv2 point " + word + (skipCache ? " (skipCache)" : "");
        assertEquals(word, record.key().get("word").toString(), "IQv2 point key " + word);
        assertEquals(expectedCount, record.value().get("count"), "IQv2 point value " + word);
        assertEquals(timestampByWord.get(word), record.timestamp(), "IQv2 point timestamp " + word);
        assertRecordHeaders(record, word, context);
    }

    /**
     * Asserts a point query for {@code word} succeeds but yields a null result. A success-with-null is
     * filtered out of {@link StateQueryResult#getOnlyPartitionResult()} (which returns null), so this
     * inspects the per-partition result directly to distinguish "succeeded, absent" from a failure.
     */
    private void assertPointReturnsNull(KafkaStreams streams, String storeName, String word,
        String context, boolean skipCache) {
        TimestampedKeyWithHeadersQuery<GenericRecord, GenericRecord> query =
            TimestampedKeyWithHeadersQuery.withKey(createKey(word));
        if (skipCache) {
            query = query.skipCache();
        }
        StateQueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> result =
            streams.query(StateQueryRequest.inStore(storeName).withQuery(query));
        Map<Integer, QueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>>> partitionResults =
            result.getPartitionResults();
        assertFalse(partitionResults.isEmpty(), context + " query should return a partition result");
        QueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> pr =
            partitionResults.values().iterator().next();
        // Assert success first: getOnlyPartitionResult() filters out FAILED results too, so a query
        // that failed outright would otherwise masquerade as a legitimate "absent key" null.
        assertTrue(pr.isSuccess(), context + " query should succeed but failed: "
            + (pr.isFailure() ? pr.getFailureReason() + ": " + pr.getFailureMessage() : ""));
        assertNull(pr.getResult(), context + " should yield a null result");
    }

    private List<ReadOnlyRecord<GenericRecord, GenericRecord>> queryRange(
        KafkaStreams streams, String storeName,
        TimestampedRangeWithHeadersQuery<GenericRecord, GenericRecord> query, int expected) {
        long deadline = System.currentTimeMillis() + 30_000;
        List<ReadOnlyRecord<GenericRecord, GenericRecord>> out = new ArrayList<>();
        String lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            out.clear();
            StateQueryResult<ReadOnlyRecordIterator<GenericRecord, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName).withQuery(query));
            QueryResult<ReadOnlyRecordIterator<GenericRecord, GenericRecord>> pr =
                result.getOnlyPartitionResult();
            if (pr != null && pr.isSuccess() && pr.getResult() != null) {
                try (ReadOnlyRecordIterator<GenericRecord, GenericRecord> it = pr.getResult()) {
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
        assertEquals(expected, out.size(), "IQv2 range query returned an unexpected count"
            + (lastFailure != null ? " (last failure: " + lastFailure + ")" : ""));
        return out;
    }

    private static List<String> wordsOf(List<ReadOnlyRecord<GenericRecord, GenericRecord>> records) {
        return records.stream()
            .map(r -> r.key().get("word").toString())
            .collect(Collectors.toList());
    }

    private void assertHeadersOnEach(
        List<ReadOnlyRecord<GenericRecord, GenericRecord>> records, String context) {
        for (int i = 0; i < records.size(); i++) {
            ReadOnlyRecord<GenericRecord, GenericRecord> record = records.get(i);
            String word = record.key().get("word").toString();
            assertRecordHeaders(record, word, context + " entry " + i);
        }
    }

    /**
     * Asserts the record carries the exact headers produced for {@code word}: the schema-id GUIDs
     * byte-equal to what that record's serializer wrote, and the distinct {@code seq} header equal to
     * the word itself. The {@code seq} check is what makes this a per-record fidelity assertion -- a
     * store that returned another record's headers, or one shared {@code Headers} instance for every
     * entry, would carry the wrong {@code seq} and fail here.
     */
    private void assertRecordHeaders(ReadOnlyRecord<GenericRecord, GenericRecord> record,
        String word, String context) {
        CapturedSchemaIds expected = capturedByWord.get(word);
        assertNotNull(expected, context + ": no captured schema-id GUIDs for " + word);
        assertSchemaIdHeaders(record.headers(), expected, context);
        Header seq = record.headers().lastHeader(SEQ_HEADER);
        assertNotNull(seq, context + ": missing " + SEQ_HEADER + " header");
        assertEquals(word, new String(seq.value(), StandardCharsets.UTF_8),
            context + ": " + SEQ_HEADER + " header should carry this record's own key");
    }

    // ---------------------------------------------------------------------------------------------
    // Topology + populate helpers
    // ---------------------------------------------------------------------------------------------

    private Topology buildTopology(String input, String output, String storeName,
        boolean cachingEnabled) {
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        StoreBuilder<TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord>> storeBuilder =
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName), keySerde, valueSerde);
        storeBuilder = cachingEnabled
            ? storeBuilder.withCachingEnabled()
            : storeBuilder.withCachingDisabled();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(storeBuilder)
            .stream(input, Consumed.with(keySerde, valueSerde))
            .process(() -> new PutProcessor(storeName), storeName)
            .to(output, Produced.with(keySerde, valueSerde));
        return builder.build();
    }

    private KafkaStreams startAndPopulate(String storeName, String input, String output, String appId,
        List<String> words, List<Long> counts, boolean cachingEnabled, Integer commitIntervalMs)
        throws Exception {
        createTopics(input, output);
        KafkaStreams streams = startStreamsAndAwaitRunning(
            buildTopology(input, output, storeName, cachingEnabled), appId, 30, commitIntervalMs);
        boolean populated = false;
        try {
            produceRecords(input, words, counts);
            consumeRecords(output, appId + "-populate-consumer", words.size());
            populated = true;
            return streams;
        } finally {
            if (!populated) {
                closeStreams(streams);
            }
        }
    }

    private void produceRecords(String topic, List<String> words, List<Long> counts) throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            for (int i = 0; i < words.size(); i++) {
                String word = words.get(i);
                long timestamp = BASE_TIMESTAMP + i;
                ProducerRecord<GenericRecord, GenericRecord> record = new ProducerRecord<>(
                    topic, null, timestamp, createKey(word), createValue(counts.get(i)));
                // Distinct per-record user header so each IQv2 result can be checked against its own
                // record -- see SEQ_HEADER. The serde adds the schema-id headers during send().
                record.headers().add(SEQ_HEADER, word.getBytes(StandardCharsets.UTF_8));
                capturedByWord.put(word, sendAndCapture(producer, record));
                timestampByWord.put(word, timestamp);
            }
            producer.flush();
        }
    }

    /** Produces a tombstone (null value) for {@code word}, deleting it from the header-aware store. */
    private void produceTombstone(String topic, String word) throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            sendAndCapture(producer, new ProducerRecord<>(topic, createKey(word), (GenericRecord) null));
            producer.flush();
        }
    }

    /**
     * Processor that stores each incoming record (value, timestamp, headers) into the header-aware
     * store and forwards the stored record downstream, so the output topic acts as a completion
     * barrier for the produced batch.
     */
    private static class PutProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

        PutProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            if (record.value() == null) {
                // Tombstone the header-aware entry (mirrors the window/session writers' null handling)
                // and forward the null-value record so the output topic still acts as a completion
                // barrier for the batch.
                store.put(record.key(), null);
                context.forward(record);
                return;
            }
            store.put(record.key(),
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
            ValueTimestampHeaders<GenericRecord> stored = store.get(record.key());
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Avro record factories (schema-specific; the shared infrastructure lives in the base class)
    // ---------------------------------------------------------------------------------------------

    private GenericRecord createKey(String word) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", word);
        return key;
    }

    private GenericRecord createValue(long count) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("count", count);
        return value;
    }
}
