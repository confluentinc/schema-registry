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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
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
 * <p>Runs against a real 1-broker cluster + embedded Schema Registry ({@code super(1, true)}).
 */
public class TimestampedKeyValueStoreWithHeadersIQv2IntegrationTest extends ClusterTestHarness {

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
            + "  {\"name\":\"count\",\"type\":\"long\"},"
            + "  {\"name\":\"operation\",\"type\":\"string\",\"default\":\"PUT\"}"
            + "]"
            + "}";

    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    public TimestampedKeyValueStoreWithHeadersIQv2IntegrationTest() {
        super(1, true);
    }

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
    public void shouldReturnNullForAbsentKey() throws Exception {
        String input = "iqv2-absent-input";
        String output = "iqv2-absent-output";
        String storeName = "iqv2-absent-store";
        String appId = "iqv2-absent-test";

        KafkaStreams streams = startAndPopulate(
            storeName, input, output, appId,
            Collections.singletonList("word-1"), Collections.singletonList(10L),
            true, null);
        try {
            StateQueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(TimestampedKeyWithHeadersQuery.withKey(createKey("no-such-word"))));

            // A success-with-null (absent key) is filtered out of getOnlyPartitionResult(), so it
            // returns null; assert precisely via the per-partition result instead.
            QueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> pr =
                result.getPartitionResults().values().iterator().next();
            assertTrue(pr.isSuccess(), "absent-key query should succeed");
            assertNull(pr.getResult(), "absent key should yield a null result");
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

            // skipCache bypasses the cache and reads the persistent store, which is still empty; a null
            // result positively proves the read above was genuinely cache-served (and covers skipCache).
            assertNull(queryPointOnce(streams, storeName, createKey("word-1"), true),
                "skipCache should read the empty store and return null before any flush");
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
                TimestampedRangeWithHeadersQuery.withRange(createKey("word-2"), createKey("word-4")),
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
            assertEquals(new HashSet<>(Arrays.asList("word-1", "word-2", "word-3")),
                new HashSet<>(wordsOf(records)), "scan should return all keys");
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
                TimestampedRangeWithHeadersQuery.withLowerBound(createKey("word-3")), 2);
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
                TimestampedRangeWithHeadersQuery.withUpperBound(createKey("word-2")), 2);
            assertEquals(Arrays.asList("word-1", "word-2"), wordsOf(records), "upper bound word-2");
            assertHeadersOnEach(records, "IQv2 upper bound");
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

        // Restart with the same APPLICATION_ID; cleanUp() wipes the local state dir so the store
        // must be rebuilt from the changelog. The restored entries must still carry their headers.
        KafkaStreams restored = startStreamsAndAwaitRunning(
            buildTopology(input, output, storeName, true), appId, 90, null);
        try {
            ReadOnlyRecord<GenericRecord, GenericRecord> word1 =
                queryPointExpectPresent(restored, storeName, createKey("word-1"), false);
            assertEquals(10L, word1.value().get("count"), "restored word-1 value");
            assertSchemaIdHeaders(word1.headers(), "restored word-1");

            List<ReadOnlyRecord<GenericRecord, GenericRecord>> all = queryRange(
                restored, storeName, TimestampedRangeWithHeadersQuery.withNoBounds(), 3);
            assertEquals(new HashSet<>(Arrays.asList("word-1", "word-2", "word-3")),
                new HashSet<>(wordsOf(all)), "restored scan keys");
            assertHeadersOnEach(all, "restored scan");
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
        while (System.currentTimeMillis() < deadline) {
            StateQueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> result =
                streams.query(StateQueryRequest.inStore(storeName).withQuery(query));
            QueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> pr = result.getOnlyPartitionResult();
            if (pr != null && pr.isSuccess() && pr.getResult() != null) {
                return pr.getResult();
            }
            sleepQuietly(200);
        }
        throw new AssertionError("IQv2 point query never returned a result for key " + key);
    }

    private void assertPointQuery(KafkaStreams streams, String storeName, String word,
        long expectedCount, boolean skipCache) {
        ReadOnlyRecord<GenericRecord, GenericRecord> record =
            queryPointExpectPresent(streams, storeName, createKey(word), skipCache);
        assertEquals(word, record.key().get("word").toString(), "IQv2 point key " + word);
        assertEquals(expectedCount, record.value().get("count"), "IQv2 point value " + word);
        assertTrue(record.timestamp() >= 0, "IQv2 point timestamp should be non-negative: " + word);
        assertSchemaIdHeaders(record.headers(), "IQv2 point " + word + (skipCache ? " (skipCache)" : ""));
    }

    /** Single point query, no retry. Returns null for an absent/tombstoned key (or empty store). */
    private ReadOnlyRecord<GenericRecord, GenericRecord> queryPointOnce(
        KafkaStreams streams, String storeName, GenericRecord key, boolean skipCache) {
        TimestampedKeyWithHeadersQuery<GenericRecord, GenericRecord> query =
            TimestampedKeyWithHeadersQuery.withKey(key);
        if (skipCache) {
            query = query.skipCache();
        }
        StateQueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> result =
            streams.query(StateQueryRequest.inStore(storeName).withQuery(query));
        QueryResult<ReadOnlyRecord<GenericRecord, GenericRecord>> pr = result.getOnlyPartitionResult();
        return pr == null ? null : pr.getResult();
    }

    private List<ReadOnlyRecord<GenericRecord, GenericRecord>> queryRange(
        KafkaStreams streams, String storeName,
        TimestampedRangeWithHeadersQuery<GenericRecord, GenericRecord> query, int expected) {
        long deadline = System.currentTimeMillis() + 30_000;
        List<ReadOnlyRecord<GenericRecord, GenericRecord>> out = new ArrayList<>();
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
            }
            sleepQuietly(200);
        }
        assertEquals(expected, out.size(), "IQv2 range query returned an unexpected count");
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
            assertSchemaIdHeaders(records.get(i).headers(), context + " entry " + i);
        }
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
            store.put(record.key(),
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
            ValueTimestampHeaders<GenericRecord> stored = store.get(record.key());
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Shared infrastructure helpers (mirroring the KIP-1271 store integration tests)
    // ---------------------------------------------------------------------------------------------

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

    private Properties createStreamsProps(String appId, Integer commitIntervalMs) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        if (commitIntervalMs != null) {
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        }
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

    private void produceRecords(String topic, List<String> words, List<Long> counts) throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            for (int i = 0; i < words.size(); i++) {
                producer.send(new ProducerRecord<>(
                    topic, createKey(words.get(i)), createValue(counts.get(i), "PUT"))).get();
            }
            producer.flush();
        }
    }

    private KafkaStreams startStreamsAndAwaitRunning(
        Topology topology, String appId, int timeoutSeconds, Integer commitIntervalMs) throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        KafkaStreams streams = new KafkaStreams(topology, createStreamsProps(appId, commitIntervalMs));
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

    private GenericRecord createKey(String word) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", word);
        return key;
    }

    private GenericRecord createValue(long count, String operation) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("count", count);
        value.put("operation", operation);
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
