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

package io.confluent.kafka.streams.integration.dsl;

import static org.junit.jupiter.api.Assertions.*;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * DSL integration test for {@link TimestampedKeyValueStoreWithHeaders}.
 */
@Tag("IntegrationTest")
public class TimestampedKeyValueStoreWithHeadersDslIntegrationTest extends TimestampedKeyValueStoreDslTestBase {

    /**
     * Verifies `groupByKey()` and `count()` work correctly using headers-aware stores,
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGroupCountWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-count-input" + suffix;
        String outputTopic = "dsl-count-output" + suffix;
        String storeName = "dsl-count-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde))
            .count(Materialized.<GenericRecord, Long>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(keySerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, Serdes.Long()));

        String applicationId = "dsl-count-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("kafka"), createTextLine("first")),
                KeyValue.pair(createKey("kafka"), createTextLine("second")),
                KeyValue.pair(createKey("kafka"), createTextLine("third")),
                KeyValue.pair(createKey("streams"), createTextLine("first")),
                KeyValue.pair(createKey("streams"), createTextLine("second")),
                KeyValue.pair(createKey("hello"), createTextLine("first")));

            int maxExpected = 6;
            List<ConsumerRecord<GenericRecord, Long>> results =
                consumeRecords(
                    outputTopic, "dsl-count-consumer" + suffix, maxExpected, org.apache.kafka.common.serialization.LongDeserializer.class);

            Map<String, Long> finalCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, Long> record : results) {
                finalCounts.put(record.key().get("word").toString(), record.value());
            }
            assertEquals(3L, finalCounts.get("kafka"), "kafka should have count 3");
            assertEquals(2L, finalCounts.get("streams"), "streams should have count 2");
            assertEquals(1L, finalCounts.get("hello"), "hello should have count 1");

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<Long>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            ValueTimestampHeaders<Long> kafkaResult = store.get(createKey("kafka"));
            assertNotNull(kafkaResult, "IQv1: kafka should exist in store");
            assertEquals(3L, kafkaResult.value(), "IQv1: kafka count should be 3");
            assertKeySchemaIdHeader(kafkaResult.headers(), changelogTopic, "IQv1 get kafka");

            ValueTimestampHeaders<Long> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals(2L, streamsResult.value(), "IQv1: streams count should be 2");
            assertKeySchemaIdHeader(streamsResult.headers(), changelogTopic, "IQv1 get streams");

            ValueTimestampHeaders<Long> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals(1L, helloResult.value(), "IQv1: hello count should be 1");
            assertKeySchemaIdHeader(helloResult.headers(), changelogTopic, "IQv1 get hello");

            // count() skips null input values — the existing entry must not be tombstoned.
            produce(inputTopic, KeyValue.pair(createKey("hello"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<Long> v = store.get(createKey("hello"));
                    return v != null && v.value() != null && v.value() == 1L;
                },
                10_000,
                "hello count should remain 1 after null input (count skips nulls)");

            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeRecords(changelogTopic, "dsl-count-changelog-consumer" + suffix, 6, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                if (record.value() != null) {
                    assertKeySchemaIdHeader(record.headers(), changelogTopic,
                        "changelog " + record.key().get("word"));
                }
            }
        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `flatMapValues()`, `groupBy()` and `count()` work correctly using headers-aware stores.
     * Also verify if null value in `flatMapValues()` causes the Streams thread to crash, since null values are not allowed in `flatMapValues()`.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCrashOnNullValueInFlatMapValues(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-count-supplier-input" + suffix;
        String outputTopic = "dsl-count-supplier-output" + suffix;
        String storeName = "dsl-count-supplier-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .flatMapValues(value ->
                Arrays.asList(value.get("line").toString().toLowerCase().split("\\W+")))
            .groupBy((key, word) -> {
                GenericRecord wordKey = new GenericData.Record(keySchema);
                wordKey.put("word", word);
                return wordKey;
            }, Grouped.with(keySerde, Serdes.String()))
            .count(Materialized.<GenericRecord, Long>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(keySerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, Serdes.Long()));

        String applicationId = "dsl-count-supplier-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("key1"), createTextLine("hello kafka streams")),
                KeyValue.pair(createKey("key2"), createTextLine("all streams lead to kafka")),
                KeyValue.pair(createKey("key3"), createTextLine("join kafka summit")));

            int maxExpected = 11;
            List<ConsumerRecord<GenericRecord, Long>> results =
                consumeRecords(
                    outputTopic, "dsl-count-supplier-consumer" + suffix, maxExpected, org.apache.kafka.common.serialization.LongDeserializer.class);

            // Verify final counts from output records
            Map<String, Long> finalCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, Long> record : results) {
                finalCounts.put(record.key().get("word").toString(), record.value());
            }
            assertEquals(3L, finalCounts.get("kafka"), "kafka should have count 3");
            assertEquals(2L, finalCounts.get("streams"), "streams should have count 2");
            assertEquals(1L, finalCounts.get("hello"), "hello should have count 1");

            // IQv1 verification
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<Long>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            // Verify key headers are preserved in the store.
            ValueTimestampHeaders<Long> kafkaResult = store.get(createKey("kafka"));
            assertNotNull(kafkaResult, "IQv1: kafka should exist in store");
            assertEquals(3L, kafkaResult.value(), "IQv1: kafka count should be 3");
            assertKeySchemaIdHeader(kafkaResult.headers(), changelogTopic, "IQv1 get kafka");

            ValueTimestampHeaders<Long> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals(2L, streamsResult.value(), "IQv1: streams count should be 2");
            assertKeySchemaIdHeader(streamsResult.headers(), changelogTopic, "IQv1 get streams");

            ValueTimestampHeaders<Long> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals(1L, helloResult.value(), "IQv1: hello count should be 1");
            assertKeySchemaIdHeader(helloResult.headers(), changelogTopic, "IQv1 get hello");

            // Wait for the cache to flush to the changelog before the poison-pill (next test step) and verify changelog.
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords = new ArrayList<>();
            TestUtils.waitForCondition(
                () -> {
                    changelogRecords.clear();
                    changelogRecords.addAll(consumeRecords(
                        changelogTopic,
                        "dsl-count-flush-consumer-" + System.nanoTime() + suffix, 8, org.apache.kafka.common.serialization.ByteArrayDeserializer.class));
                    return changelogRecords.size() >= 8;
                },
                10_000,
                "Changelog should have at least 8 records before crash");

            // Send a record with value null for k1,
            // which should cause flatMapValues to throw and the Streams thread to crash.
            KafkaStreams finalStreams = streams;
            produce(inputTopic, KeyValue.pair(createKey("key1"), null));

            TestUtils.waitForCondition(
                () -> finalStreams.state() == KafkaStreams.State.ERROR,
                30_000,
                "Streams should be in ERROR state after null value hits flatMapValues");

            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                if (record.value() != null) {
                    assertKeySchemaIdHeader(record.headers(), changelogTopic,
                        "changelog " + record.key().get("word"));
                }
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `groupBy()` and `aggregate()` works correctly with headers-aware stores.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGroupAndAggregateWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-aggregate-input" + suffix;
        String outputTopic = "dsl-aggregate-output" + suffix;
        String storeName = "dsl-aggregate-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde))
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                (key, value, agg) -> {
                    // Null aggregation: returning null tombstones the key
                    if ("DELETE".equals(value.get("line").toString())) {
                        return null;
                    }
                    GenericRecord updated = new GenericData.Record(aggSchema);
                    updated.put("word", key.get("word").toString());
                    updated.put("count", (long) agg.get("count") + 1L);
                    return updated;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, aggSerde));

        String applicationId = "dsl-aggregate-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            // Send 3 records for "kafka", 2 for "streams", 1 for "hello"
            produce(inputTopic,
                KeyValue.pair(createKey("kafka"), createTextLine("first")),
                KeyValue.pair(createKey("kafka"), createTextLine("second")),
                KeyValue.pair(createKey("kafka"), createTextLine("third")),
                KeyValue.pair(createKey("streams"), createTextLine("first")),
                KeyValue.pair(createKey("streams"), createTextLine("second")),
                KeyValue.pair(createKey("hello"), createTextLine("first")));

            int maxExpected = 6;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-aggregate-consumer" + suffix, maxExpected, KafkaAvroDeserializer.class);

            // Verify final aggregated counts from output records
            Map<String, Long> finalCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                finalCounts.put(
                    record.key().get("word").toString(),
                    (long) record.value().get("count"));
                assertSchemaIdHeaders(record.headers(), outputTopic, "aggregate output " + record.key());
            }
            assertEquals(3L, finalCounts.get("kafka"), "kafka should have count 3");
            assertEquals(2L, finalCounts.get("streams"), "streams should have count 2");
            assertEquals(1L, finalCounts.get("hello"), "hello should have count 1");

            // IQv1 verification
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> kafkaResult = store.get(createKey("kafka"));
            assertNotNull(kafkaResult, "IQv1: kafka should exist in store");
            assertEquals(3L, kafkaResult.value().get("count"), "IQv1: kafka count should be 3");
            assertSchemaIdHeaders(kafkaResult.headers(), changelogTopic, "IQv1 get kafka");

            ValueTimestampHeaders<GenericRecord> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals(2L, streamsResult.value().get("count"),
                "IQv1: streams count should be 2");
            assertSchemaIdHeaders(streamsResult.headers(), changelogTopic, "IQv1 get streams");

            ValueTimestampHeaders<GenericRecord> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals(1L, helloResult.value().get("count"), "IQv1: hello count should be 1");
            assertSchemaIdHeaders(helloResult.headers(), changelogTopic, "IQv1 get hello");

            // Add a record with value null for k1, which shouldn't work since "aggregate" operation skips null values.
            produce(inputTopic, KeyValue.pair(createKey("hello"), null));
            final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> storeBeforeDelete = store;
            boolean helloChanged = false;
            try {
                TestUtils.waitForCondition(
                    () -> {
                        ValueTimestampHeaders<GenericRecord> v = storeBeforeDelete.get(createKey("hello"));
                        return v == null || v.value() == null
                            || (long) v.value().get("count") != 1L;
                    },
                    2_000,
                    "");
                helloChanged = true;
            } catch (AssertionError expectedTimeout) {
                // expected — null input should be skipped by aggregate
            }
            assertFalse(helloChanged,
                "IQv1: hello should not be tombstoned and count should remain 1 after null input (aggregate skips nulls)");

            // Null aggregation: aggregator returns null for "DELETE" value, which tombstones the key
            produce(inputTopic, KeyValue.pair(createKey("hello"), createTextLine("DELETE")));
            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = storeBeforeDelete.get(createKey("hello"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1: hello should be tombstoned after aggregator returned null");

            // Re-fetch store reference in case of rebalance
            store = headersStore(streams, storeName);
            ValueTimestampHeaders<GenericRecord> helloDeleted = store.get(createKey("hello"));
            assertTrue(helloDeleted == null || helloDeleted.value() == null,
                "IQv1: hello should be tombstoned after aggregator returned null");
            // kafka and streams should still exist
            ValueTimestampHeaders<GenericRecord> kafkaRecord = store.get(createKey("kafka"));
            assertNotNull(kafkaRecord, "IQv1: kafka should still exist after hello null aggregation");
            assertEquals(3L, kafkaRecord.value().get("count"), "IQv1: kafka count should still be 3");

            // Changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> changelogRecords =
                consumeRecords(changelogTopic, "dsl-aggregate-changelog-consumer" + suffix, 7, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> lastByKey = lastRecordPerKey(changelogRecords);
            assertEquals(3, lastByKey.size(), "changelog should have exactly 3 unique keys, got " + lastByKey.keySet());
            assertEquals(3L, lastByKey.get("kafka").value().get("count"), "changelog kafka final count should be 3");
            assertEquals(2L, lastByKey.get("streams").value().get("count"), "changelog streams final count should be 2");
            assertNull(lastByKey.get("hello").value(), "changelog hello final should be tombstoned");

            assertChangelogHeaders(changelogRecords, changelogTopic,
                Collections.singleton("hello"), "changelog");

            if (!cachingEnabled) {
                assertEquals(7, changelogRecords.size(),
                    "Changelog (uncached) should have exactly 7 records (6 puts + 1 tombstone)");
            }
        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `groupBy()` and `reduce()` works correctly with headers-aware stores.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGroupAndReduceWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-reduce-input" + suffix;
        String outputTopic = "dsl-reduce-output" + suffix;
        String storeName = "dsl-reduce-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde))
            .reduce(
                (oldValue, newValue) -> {
                    // Null aggregation: returning null from reducer tombstones the key
                    if ("DELETE".equals(newValue.get("line").toString())) {
                        return null;
                    }
                    return newValue;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        String applicationId = "dsl-reduce-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("k1"), createTextLine("first value")),
                KeyValue.pair(createKey("k1"), createTextLine("second value")),
                KeyValue.pair(createKey("k2"), createTextLine("only value")));

            int maxExpected = 3;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-reduce-consumer" + suffix, maxExpected, KafkaAvroDeserializer.class);

            // Verify final reduced values from output records
            Map<String, String> finalValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                finalValues.put(
                    record.key().get("word").toString(),
                    record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), outputTopic, "reduce output " + record.key());
            }
            assertEquals("second value", finalValues.get("k1"),
                "k1 should be reduced to the latest value");
            assertEquals("only value", finalValues.get("k2"),
                "k2 should have its only value");

            // IQv1 verification
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> k1Result = store.get(createKey("k1"));
            assertNotNull(k1Result, "IQv1: k1 should exist in store");
            assertEquals("second value", k1Result.value().get("line").toString(),
                "IQv1: k1 should have latest reduced value");
            assertSchemaIdHeaders(k1Result.headers(), changelogTopic, "IQv1 get k1");

            ValueTimestampHeaders<GenericRecord> k2Result = store.get(createKey("k2"));
            assertNotNull(k2Result, "IQv1: k2 should exist in store");
            assertEquals("only value", k2Result.value().get("line").toString(),
                "IQv1: k2 should have its value");
            assertSchemaIdHeaders(k2Result.headers(), changelogTopic, "IQv1 get k2");

            // Add a tombstone record for k1, which shouldn't work since "reduce" operation skips null values.
            produce(inputTopic, KeyValue.pair(createKey("k1"), null));
            final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> storeBeforeDelete = store;
            boolean k1Tombstoned = false;
            try {
                TestUtils.waitForCondition(
                    () -> {
                        ValueTimestampHeaders<GenericRecord> v = storeBeforeDelete.get(createKey("k1"));
                        return v == null || v.value() == null;
                    },
                    2_000,
                    "");
                k1Tombstoned = true;
            } catch (AssertionError expectedTimeout) {
                // expected — null input should be skipped by reduce
            }
            assertFalse(k1Tombstoned,
                "IQv1: k1 should not be tombstoned after null input (reduce skips nulls)");

            ValueTimestampHeaders<GenericRecord> k2Record = storeBeforeDelete.get(createKey("k2"));
            assertNotNull(k2Record, "IQv1: k2 should still exist after k1 tombstone");
            assertEquals("only value", k2Record.value().get("line").toString());

            // Null aggregation: reducer returns null for "DELETE" value, which tombstones the key
            produce(inputTopic, KeyValue.pair(createKey("k2"), createTextLine("DELETE")));
            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = storeBeforeDelete.get(createKey("k2"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1: k2 should be tombstoned after reducer returned null");

            // Re-fetch store reference in case of rebalance
            store = headersStore(streams, storeName);
            ValueTimestampHeaders<GenericRecord> k2Deleted = store.get(createKey("k2"));
            assertTrue(k2Deleted == null || k2Deleted.value() == null,
                "IQv1: k2 should be tombstoned after reducer returned null");
            // k1 should still be unaffected
            ValueTimestampHeaders<GenericRecord> k1Record = store.get(createKey("k1"));
            assertNotNull(k1Record, "IQv1: k1 should still exist after k2 null aggregation");
            assertEquals("second value", k1Record.value().get("line").toString());

            // Changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> changelogRecords =
                consumeRecords(changelogTopic, "dsl-reduce-changelog-consumer" + suffix, 4, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> lastByKey = lastRecordPerKey(changelogRecords);
            assertEquals(2, lastByKey.size(), "changelog should have exactly 2 unique keys, got " + lastByKey.keySet());
            assertEquals("second value", lastByKey.get("k1").value().get("line").toString(),
                "changelog k1 final value should be 'second value'");
            assertNull(lastByKey.get("k2").value(), "changelog k2 final should be tombstoned");

            assertChangelogHeaders(changelogRecords, changelogTopic,
                Collections.singleton("k2"), "changelog");

            if (!cachingEnabled) {
                assertEquals(4, changelogRecords.size(),
                    "Changelog (uncached) should have exactly 4 records (3 puts + 1 tombstone)");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `mapValues()` on a KTable works correctly with headers-aware stores.
     * When {@code materialized=true}, the mapValues result is written to its own
     * headers-aware store and that store + its changelog are verified too.
     */
    @ParameterizedTest
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void shouldMapValuesWithHeaders(boolean cachingEnabled, boolean materialized) throws Exception {
        String suffix = suffixOf(cachingEnabled)
            + (materialized ? "-mat" : "-nomat");
        String inputTopic = "dsl-mapvalues-input" + suffix;
        String outputTopic = "dsl-mapvalues-output" + suffix;
        String sourceStoreName = "dsl-mapvalues-source-store" + suffix;
        String mappedStoreName = "dsl-mapvalues-mapped-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde mappedSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KTable<GenericRecord, GenericRecord> sourceTable = builder.table(
            inputTopic, Consumed.with(keySerde, valueSerde),
            Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(sourceStoreName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));

        ValueMapper<GenericRecord, GenericRecord> mapper = value -> {
            GenericRecord mapped = new GenericData.Record(mapValueSchema);
            mapped.put("firstWord", value.get("line").toString().split("\\W+")[0]);
            mapped.put("count", (long) value.get("line").toString().length());
            return mapped;
        };

        KTable<GenericRecord, GenericRecord> mappedTable = materialized
            ? sourceTable.mapValues(mapper,
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(mappedStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(mappedSerde))
            : sourceTable.mapValues(mapper);

        mappedTable.toStream().to(outputTopic, Produced.with(keySerde, mappedSerde));

        String applicationId = "dsl-mapvalues-test" + suffix;
        String sourceChangelogTopic = changelogTopicFor(applicationId, sourceStoreName);
        String mappedChangelogTopic = changelogTopicFor(applicationId, mappedStoreName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("hello"), createTextLine("hello kafka streams")),
                KeyValue.pair(createKey("streams"), createTextLine("kafka streams")));

            int expected = 2;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-mapvalues-consumer" + suffix, expected, KafkaAvroDeserializer.class);

            assertEquals(expected, results.size(),
                "Should have " + expected + " output records, got " + results.size());

            Map<String, String> firstWordMappedValues = new HashMap<>();
            Map<String, Long> lineLengthMappedValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                String key = record.key().get("word").toString();
                firstWordMappedValues.put(key, record.value().get("firstWord").toString());
                lineLengthMappedValues.put(key, (long) record.value().get("count"));
                assertSchemaIdHeaders(record.headers(), outputTopic, "mapValues output " + record.key());
            }
            assertEquals("hello", firstWordMappedValues.get("hello"));
            assertEquals("kafka", firstWordMappedValues.get("streams"));
            assertEquals(19L, lineLengthMappedValues.get("hello"));
            assertEquals(13L, lineLengthMappedValues.get("streams"));

            // IQv1 on the source store (raw TextLine values).
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> sourceStore =
                headersStore(streams, sourceStoreName);
            assertNotNull(sourceStore, "Source store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> helloSource = sourceStore.get(createKey("hello"));
            assertNotNull(helloSource, "IQv1 source store: hello should exist");
            assertEquals("hello kafka streams", helloSource.value().get("line").toString());
            assertSchemaIdHeaders(helloSource.headers(), sourceChangelogTopic, "IQv1 source get hello");

            ValueTimestampHeaders<GenericRecord> streamsSource = sourceStore.get(createKey("streams"));
            assertNotNull(streamsSource, "IQv1 source store: streams should exist");
            assertEquals("kafka streams", streamsSource.value().get("line").toString());
            assertSchemaIdHeaders(streamsSource.headers(), sourceChangelogTopic, "IQv1 source get streams");

            // IQv1 on the materialized mapValues store if mapped value store is enabled.
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> mappedStore = null;
            if (materialized) {
                mappedStore = headersStore(streams, mappedStoreName);
                assertNotNull(mappedStore, "mapValues output store should be accessible via IQv1");

                ValueTimestampHeaders<GenericRecord> helloMapped = mappedStore.get(createKey("hello"));
                assertNotNull(helloMapped, "IQv1 mapped store: hello should exist");
                assertEquals("hello", helloMapped.value().get("firstWord").toString());
                assertEquals(19L, (long) helloMapped.value().get("count"));
                assertSchemaIdHeaders(helloMapped.headers(), mappedChangelogTopic, "IQv1 mapped get hello");

                ValueTimestampHeaders<GenericRecord> streamsMapped = mappedStore.get(createKey("streams"));
                assertNotNull(streamsMapped, "IQv1 mapped store: streams should exist");
                assertEquals("kafka", streamsMapped.value().get("firstWord").toString());
                assertEquals(13L, (long) streamsMapped.value().get("count"));
                assertSchemaIdHeaders(streamsMapped.headers(), mappedChangelogTopic, "IQv1 mapped get streams");
            }

            // Source tombstone — must propagate through mapValues and reach the mapped store if materialized.
            produce(inputTopic, KeyValue.pair(createKey("hello"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = sourceStore.get(createKey("hello"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1 source store: hello should be tombstoned");

            ValueTimestampHeaders<GenericRecord> streamsRecord = sourceStore.get(createKey("streams"));
            assertNotNull(streamsRecord, "IQv1 source store: streams should still exist");
            assertEquals("kafka streams", streamsRecord.value().get("line").toString());

            if (materialized) {
                final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> finalMapped = mappedStore;
                TestUtils.waitForCondition(
                    () -> {
                        ValueTimestampHeaders<GenericRecord> v = finalMapped.get(createKey("hello"));
                        return v == null || v.value() == null;
                    },
                    10_000,
                    "IQv1 mapped store: hello should be tombstoned");
                ValueTimestampHeaders<GenericRecord> streamsMappedRecord = finalMapped.get(createKey("streams"));
                assertNotNull(streamsMappedRecord, "IQv1 mapped store: streams should still exist");
                assertEquals(13L, (long) streamsMappedRecord.value().get("count"));
            }

            // Source changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> sourceChangelogRecords =
                consumeRecords(sourceChangelogTopic,
                    "dsl-mapvalues-source-changelog-consumer" + suffix, 3, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> sourceLastByKey = lastRecordPerKey(sourceChangelogRecords);
            assertEquals(2, sourceLastByKey.size(),
                "source changelog should have exactly 2 unique keys, got " + sourceLastByKey.keySet());
            assertEquals("kafka streams", sourceLastByKey.get("streams").value().get("line").toString(),
                "source changelog streams final value should be 'kafka streams'");
            assertNull(sourceLastByKey.get("hello").value(), "source changelog hello final should be tombstoned");

            assertChangelogHeaders(sourceChangelogRecords, sourceChangelogTopic,
                Collections.singleton("hello"), "source changelog");

            if (!cachingEnabled) {
                assertEquals(3, sourceChangelogRecords.size(),
                    "Source changelog (uncached) should have exactly 3 records");
            }

            // Mapped changelog verification (only if materialized)
            if (materialized) {
                List<ConsumerRecord<GenericRecord, GenericRecord>> mappedChangelogRecords =
                    consumeRecords(mappedChangelogTopic,
                        "dsl-mapvalues-mapped-changelog-consumer" + suffix, 3, KafkaAvroDeserializer.class);
                Map<String, ConsumerRecord<GenericRecord, GenericRecord>> mappedLastByKey = lastRecordPerKey(mappedChangelogRecords);
                assertEquals(2, mappedLastByKey.size(),
                    "mapped changelog should have exactly 2 unique keys, got " + mappedLastByKey.keySet());
                assertEquals("kafka", mappedLastByKey.get("streams").value().get("firstWord").toString(),
                    "mapped changelog streams firstWord should be 'kafka'");
                assertEquals(13L, mappedLastByKey.get("streams").value().get("count"),
                    "mapped changelog streams count should be 13");
                assertNull(mappedLastByKey.get("hello").value(),
                    "mapped changelog hello final should be tombstoned");

                assertChangelogHeaders(mappedChangelogRecords, mappedChangelogTopic,
                    Collections.singleton("hello"), "mapped changelog");

                if (!cachingEnabled) {
                    assertEquals(3, mappedChangelogRecords.size(),
                        "Mapped changelog (uncached) should have exactly 3 records");
                }
            }

        } finally {
            closeStreams(streams);
        }
    }


    /**
     * Verify `filter()` and `filterNot()` works correctly with a headers-aware store.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFilterAndFilterNotWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-filter-input" + suffix;
        String outputTopic = "dsl-filter-output" + suffix;
        String filterStoreName = "dsl-filter-store" + suffix;
        String sourceStoreName = "dsl-filter-source-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        // The source table is materialized with a headers-aware store.
        // filterNot is not materialized, filter is materialized.
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(sourceStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .filterNot((key, value) -> value.get("line").toString().contains("kafka"))
            .filter((key, value) -> value.get("line").toString().length() > 10,
                Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(filterStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        String applicationId = "dsl-filter-test" + suffix;
        String sourceChangelogTopic = changelogTopicFor(applicationId, sourceStoreName);
        String filterChangelogTopic = changelogTopicFor(applicationId, filterStoreName);

        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("long"), createTextLine("this is a long long line")),
                KeyValue.pair(createKey("long2"), createTextLine("this is another long line")),
                KeyValue.pair(createKey("long3"), createTextLine("this is another long line with kafka")),
                KeyValue.pair(createKey("short"), createTextLine("line")));

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-filter-consumer" + suffix, 2, KafkaAvroDeserializer.class);

            assertEquals(2, results.size(), "Only the long lines without 'kafka' should pass filter");
            assertEquals("long", results.get(0).key().get("word").toString());
            assertEquals("this is a long long line", results.get(0).value().get("line").toString());
            assertSchemaIdHeaders(results.get(0).headers(), outputTopic, "filter output");
            assertEquals("long2", results.get(1).key().get("word").toString());
            assertEquals("this is another long line", results.get(1).value().get("line").toString());
            assertSchemaIdHeaders(results.get(1).headers(), outputTopic, "filter output2");

            // IQv1 verification — source store (all 4 records)
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> sourceStore =
                headersStore(streams, sourceStoreName);
            assertNotNull(sourceStore, "Source store should be queryable");

            ValueTimestampHeaders<GenericRecord> srcLong = sourceStore.get(createKey("long"));
            assertNotNull(srcLong, "source store: 'long' should exist");
            assertEquals("this is a long long line", srcLong.value().get("line").toString());
            assertSchemaIdHeaders(srcLong.headers(), sourceChangelogTopic, "source store: long");

            ValueTimestampHeaders<GenericRecord> srcLong2 = sourceStore.get(createKey("long2"));
            assertNotNull(srcLong2, "source store: 'long2' should exist");
            assertEquals("this is another long line", srcLong2.value().get("line").toString());
            assertSchemaIdHeaders(srcLong2.headers(), sourceChangelogTopic, "source store: long2");

            ValueTimestampHeaders<GenericRecord> srcLong3 = sourceStore.get(createKey("long3"));
            assertNotNull(srcLong3, "source store: 'long3' should exist");
            assertEquals("this is another long line with kafka", srcLong3.value().get("line").toString());
            assertSchemaIdHeaders(srcLong3.headers(), sourceChangelogTopic, "source store: long3");

            ValueTimestampHeaders<GenericRecord> srcShort = sourceStore.get(createKey("short"));
            assertNotNull(srcShort, "source store: 'short' should exist");
            assertEquals("line", srcShort.value().get("line").toString());
            assertSchemaIdHeaders(srcShort.headers(), sourceChangelogTopic, "source store: short");

            // IQv1 verification — filter store (only long lines without 'kafka')
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> filterStore =
                headersStore(streams, filterStoreName);
            assertNotNull(filterStore, "Filter store should be queryable");

            ValueTimestampHeaders<GenericRecord> longResult = filterStore.get(createKey("long"));
            assertNotNull(longResult, "filter store: 'long' should exist");
            assertSchemaIdHeaders(longResult.headers(), filterChangelogTopic, "filter store: long");
            ValueTimestampHeaders<GenericRecord> longResult2 = filterStore.get(createKey("long2"));
            assertNotNull(longResult2, "filter store: 'long2' should exist");
            assertSchemaIdHeaders(longResult2.headers(), filterChangelogTopic, "filter store: long2");

            ValueTimestampHeaders<GenericRecord> shortResult = filterStore.get(createKey("short"));
            assertTrue(shortResult == null || shortResult.value() == null,
                "filter store: 'short' should be filtered out");
            ValueTimestampHeaders<GenericRecord> long3Result = filterStore.get(createKey("long3"));
            assertTrue(long3Result == null || long3Result.value() == null,
                "filter store: 'long3' should be filtered out (contains 'kafka')");

            // Tombstone delete "long" and verify it is removed from the filter store
            produce(inputTopic, KeyValue.pair(createKey("long"), null));
            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = filterStore.get(createKey("long"));
                    return v == null || v.value() == null;
                },
                10_000,
                "filter store: 'long' should be tombstoned");

            ValueTimestampHeaders<GenericRecord> longTombstoned = filterStore.get(createKey("long"));
            assertTrue(longTombstoned == null || longTombstoned.value() == null,
                "filter store: 'long' should be tombstoned");
            ValueTimestampHeaders<GenericRecord> long2Record = filterStore.get(createKey("long2"));
            assertNotNull(long2Record, "filter store: 'long2' should still exist");
            assertSchemaIdHeaders(long2Record.headers(), filterChangelogTopic, "filter store: long2 after tombstone");

            // Source changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> sourceChangelogRecords =
                consumeRecords(sourceChangelogTopic,
                    "dsl-filter-source-changelog-consumer" + suffix, 5, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> sourceLastByKey = lastRecordPerKey(sourceChangelogRecords);
            assertEquals(4, sourceLastByKey.size(),
                "source changelog should have exactly 4 unique keys, got " + sourceLastByKey.keySet());
            assertNull(sourceLastByKey.get("long").value(),
                "source changelog long final should be tombstoned");
            assertEquals("this is another long line", sourceLastByKey.get("long2").value().get("line").toString(),
                "source changelog long2 final value");
            assertEquals("this is another long line with kafka", sourceLastByKey.get("long3").value().get("line").toString(),
                "source changelog long3 final value");
            assertEquals("line", sourceLastByKey.get("short").value().get("line").toString(),
                "source changelog short final value");

            assertChangelogHeaders(sourceChangelogRecords, sourceChangelogTopic,
                Collections.singleton("long"), "source changelog");

            if (!cachingEnabled) {
                assertEquals(5, sourceChangelogRecords.size(),
                    "Source changelog (uncached) should have exactly 5 records (4 puts + 1 tombstone)");
            }

            // Filter changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> changelogRecords =
                consumeRecords(filterChangelogTopic, "dsl-filter-changelog-consumer" + suffix, 3, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> filterLastByKey = lastRecordPerKey(changelogRecords);
            assertEquals(2, filterLastByKey.size(),
                "filter changelog should have exactly 2 unique keys, got " + filterLastByKey.keySet());
            assertNull(filterLastByKey.get("long").value(),
                "filter changelog long final should be tombstoned");
            assertEquals("this is another long line", filterLastByKey.get("long2").value().get("line").toString(),
                "filter changelog long2 final value");

            assertChangelogHeaders(changelogRecords, filterChangelogTopic,
                Collections.singleton("long"), "filter changelog");

            if (!cachingEnabled) {
                assertEquals(3, changelogRecords.size(),
                    "Filter changelog (uncached) should have exactly 3 records (2 puts + 1 tombstone)");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies stateless {@code KStream.filter()} / {@code filterNot()} preserve
     * schema-id headers end-to-end. Mirrors {@link #shouldFilterAndFilterNotWithHeaders}
     * — same inputs and same predicate composition — but on a KStream pipeline
     * with no state store and no changelog.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFilterAndFilterNotKStreamStatelessWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-kstream-filter-input" + suffix;
        String outputTopic = "dsl-kstream-filter-output" + suffix;

        createTopics(inputTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .filterNot((key, value) -> value.get("line").toString().contains("kafka"))
            .filter((key, value) -> value.get("line").toString().length() > 10)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        String applicationId = "dsl-kstream-filter-test" + suffix;

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("long"), createTextLine("this is a long long line")),
                KeyValue.pair(createKey("long2"), createTextLine("this is another long line")),
                KeyValue.pair(createKey("long3"), createTextLine("this is another long line with kafka")),
                KeyValue.pair(createKey("short"), createTextLine("line")));

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-kstream-filter-consumer" + suffix,
                    2, KafkaAvroDeserializer.class);
            assertEquals(2, results.size(),
                "Only the long lines without 'kafka' should pass filter");
            assertEquals("long", results.get(0).key().get("word").toString());
            assertEquals("this is a long long line", results.get(0).value().get("line").toString());
            assertSchemaIdHeaders(results.get(0).headers(), outputTopic, "filter output");
            assertEquals("long2", results.get(1).key().get("word").toString());
            assertEquals("this is another long line", results.get(1).value().get("line").toString());
            assertSchemaIdHeaders(results.get(1).headers(), outputTopic, "filter output2");

            // Send a null-value record. KStream.filter() does not skip nulls — the
            // predicate runs and NPEs, which crashes the stream thread.
            KafkaStreams finalStreams = streams;
            produce(inputTopic, KeyValue.pair(createKey("nullVal"), null));

            TestUtils.waitForCondition(
                () -> finalStreams.state() == KafkaStreams.State.ERROR,
                30_000,
                "Streams should be in ERROR state after null value hits filter predicate");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies KTable-KTable `join()` works correctly with headers-aware stores.
     * Left table contains full names, right table contains ages.
     * The join combines them into "FullName, age Age".
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldJoinTablesWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String namesTopic = "dsl-join-names" + suffix;
        String agesTopic = "dsl-join-ages" + suffix;
        String innerJoinOutputTopic = "dsl-inner-join-output" + suffix;
        String leftJoinOutputTopic = "dsl-left-join-output" + suffix;
        String outerJoinOutputTopic = "dsl-outer-join-output" + suffix;
        String innerJoinStoreName = "dsl-join-inner-store" + suffix;
        String leftJoinStoreName = "dsl-join-left-store" + suffix;
        String outerJoinStoreName = "dsl-join-outer-store" + suffix;

        createTopics(namesTopic, agesTopic, innerJoinOutputTopic, leftJoinOutputTopic, outerJoinOutputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KTable<GenericRecord, GenericRecord> namesTable =
            builder.table(namesTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders("names-store"))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));
        KTable<GenericRecord, GenericRecord> agesTable =
            builder.table(agesTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders("ages-store"))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        // inner join (returns null when age is "0" to test null join result)
        namesTable.join(agesTable,
                (name, age) -> {
                    if ("0".equals(age.get("line").toString())) {
                        return null;  // null join result tombstones the key
                    }
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    joined.put("line", name.get("line") + ", age " + age.get("line"));
                    return joined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(innerJoinStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(innerJoinOutputTopic, Produced.with(keySerde, valueSerde));

        // left join
        namesTable.leftJoin(agesTable,
                (name, age) -> {
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    String ageStr = age != null ? age.get("line").toString() : "unknown";
                    joined.put("line", name.get("line") + ", age " + ageStr);
                    return joined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(leftJoinStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(leftJoinOutputTopic, Produced.with(keySerde, valueSerde));

        // outer join
        namesTable.outerJoin(agesTable,
                (name, age) -> {
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    String nameStr = name != null ? name.get("line").toString() : "unknown";
                    String ageStr = age != null ? age.get("line").toString() : "unknown";
                    joined.put("line", nameStr + ", age " + ageStr);
                    return joined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(outerJoinStoreName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde))
                .toStream()
                .to(outerJoinOutputTopic, Produced.with(keySerde, valueSerde));

        String applicationId = "dsl-join-test" + suffix;
        String namesStoreChangelog = applicationId + "-names-store-changelog";
        String agesStoreChangelog = applicationId + "-ages-store-changelog";
        String innerChangelogTopic = changelogTopicFor(applicationId, innerJoinStoreName);
        String leftChangelogTopic = changelogTopicFor(applicationId, leftJoinStoreName);
        String outerChangelogTopic = changelogTopicFor(applicationId, outerJoinStoreName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            // Names table
            produce(namesTopic,
                KeyValue.pair(createKey("alice"), createTextLine("Alice Smith")),
                KeyValue.pair(createKey("bob"), createTextLine("Bob Jones")),
                KeyValue.pair(createKey("carol"), createTextLine("Carol White")));

            // Ages table
            produce(agesTopic,
                KeyValue.pair(createKey("alice"), createTextLine("30")),
                KeyValue.pair(createKey("bob"), createTextLine("25")));

            // Verify inner join
            List<ConsumerRecord<GenericRecord, GenericRecord>> innerJoinResults =
                consumeRecords(innerJoinOutputTopic, "dsl-inner-join-consumer" + suffix, 2, KafkaAvroDeserializer.class);

            assertEquals(2, innerJoinResults.size(),
                "Should have 2 inner joined records");

            Map<String, String> innerJoinValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : innerJoinResults) {
                String key = record.key().get("word").toString();
                innerJoinValues.put(key, record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), innerJoinOutputTopic, "join output " + key);
            }
            assertEquals("Alice Smith, age 30", innerJoinValues.get("alice"));
            assertEquals("Bob Jones, age 25", innerJoinValues.get("bob"));
            assertFalse(innerJoinValues.containsKey("carol"), "carol should not appear in inner join output (no age record)");

            // IQv1 verification — names source store
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> namesStore =
                headersStore(streams, "names-store");
            assertNotNull(namesStore, "Names store should be queryable");

            ValueTimestampHeaders<GenericRecord> aliceName = namesStore.get(createKey("alice"));
            assertNotNull(aliceName, "names store: alice should exist");
            assertEquals("Alice Smith", aliceName.value().get("line").toString());
            assertSchemaIdHeaders(aliceName.headers(), namesStoreChangelog, "names store: alice");

            ValueTimestampHeaders<GenericRecord> bobName = namesStore.get(createKey("bob"));
            assertNotNull(bobName, "names store: bob should exist");
            assertEquals("Bob Jones", bobName.value().get("line").toString());
            assertSchemaIdHeaders(bobName.headers(), namesStoreChangelog, "names store: bob");

            ValueTimestampHeaders<GenericRecord> carolName = namesStore.get(createKey("carol"));
            assertNotNull(carolName, "names store: carol should exist");
            assertEquals("Carol White", carolName.value().get("line").toString());
            assertSchemaIdHeaders(carolName.headers(), namesStoreChangelog, "names store: carol");

            // IQv1 verification — ages source store
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> agesStore =
                headersStore(streams, "ages-store");
            assertNotNull(agesStore, "Ages store should be queryable");

            ValueTimestampHeaders<GenericRecord> aliceAge = agesStore.get(createKey("alice"));
            assertNotNull(aliceAge, "ages store: alice should exist");
            assertEquals("30", aliceAge.value().get("line").toString());
            assertSchemaIdHeaders(aliceAge.headers(), agesStoreChangelog, "ages store: alice");

            ValueTimestampHeaders<GenericRecord> bobAge = agesStore.get(createKey("bob"));
            assertNotNull(bobAge, "ages store: bob should exist");
            assertEquals("25", bobAge.value().get("line").toString());
            assertSchemaIdHeaders(bobAge.headers(), agesStoreChangelog, "ages store: bob");

            ValueTimestampHeaders<GenericRecord> carolAge = agesStore.get(createKey("carol"));
            assertNull(carolAge, "ages store: carol should not exist (no age provided)");

            // IQv1 verification — inner join store
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                headersStore(streams, innerJoinStoreName);
            assertNotNull(store, "Inner join store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> aliceResult = store.get(createKey("alice"));
            assertNotNull(aliceResult, "IQv1 for inner join: alice should exist");
            assertEquals("Alice Smith, age 30", aliceResult.value().get("line").toString());
            assertSchemaIdHeaders(aliceResult.headers(), innerChangelogTopic, "IQv1 inner join get alice");

            ValueTimestampHeaders<GenericRecord> bobResult = store.get(createKey("bob"));
            assertNotNull(bobResult, "IQv1 for inner join: bob should exist");
            assertEquals("Bob Jones, age 25", bobResult.value().get("line").toString());
            assertSchemaIdHeaders(bobResult.headers(), innerChangelogTopic, "IQv1 inner join get bob");

            ValueTimestampHeaders<GenericRecord> carol = store.get(createKey("carol"));
            assertNull(carol, "IQv1 for inner join: carol should not exist");

            //Verify left join
            List<ConsumerRecord<GenericRecord, GenericRecord>> leftJoinResults =
                consumeRecords(leftJoinOutputTopic, "dsl-left-join-consumer" + suffix, 5, KafkaAvroDeserializer.class);

            Map<String, String> leftJoinValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : leftJoinResults) {
                String key = record.key().get("word").toString();
                leftJoinValues.put(key, record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), leftJoinOutputTopic, "left join output " + key);
            }
            assertEquals("Alice Smith, age 30", leftJoinValues.get("alice"));
            assertEquals("Bob Jones, age 25", leftJoinValues.get("bob"));
            assertEquals("Carol White, age unknown", leftJoinValues.get("carol"));

            // IQv1 verification for left join
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> leftJoinStore =
                headersStore(streams, leftJoinStoreName);
            assertNotNull(leftJoinStore, "Left join store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> aliceResultLeft = leftJoinStore.get(createKey("alice"));
            assertNotNull(aliceResultLeft, "IQv1 for inner join: alice should exist");
            assertEquals("Alice Smith, age 30", aliceResultLeft.value().get("line").toString());
            assertSchemaIdHeaders(aliceResultLeft.headers(), leftChangelogTopic, "IQv1 left join get alice");

            ValueTimestampHeaders<GenericRecord> bobResultLeft = leftJoinStore.get(createKey("bob"));
            assertNotNull(bobResultLeft, "IQv1 for left join: bob should exist");
            assertEquals("Bob Jones, age 25", bobResultLeft.value().get("line").toString());
            assertSchemaIdHeaders(bobResultLeft.headers(), leftChangelogTopic, "IQv1 left join get bob");

            ValueTimestampHeaders<GenericRecord> carolLeft = leftJoinStore.get(createKey("carol"));
            assertNotNull(carolLeft, "IQv1 for left join: carol should exist");
            assertEquals("Carol White, age unknown", carolLeft.value().get("line").toString());
            assertSchemaIdHeaders(carolLeft.headers(), leftChangelogTopic, "IQv1 left join get carol");

            // Verify outer join
            List<ConsumerRecord<GenericRecord, GenericRecord>> outerJoinResults =
                consumeRecords(outerJoinOutputTopic, "dsl-outer-join-consumer" + suffix, 5, KafkaAvroDeserializer.class);

            Map<String, String> outerJoinValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : outerJoinResults) {
                String key = record.key().get("word").toString();
                outerJoinValues.put(key, record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), outerJoinOutputTopic, "outer join output " + key);
            }
            assertEquals("Alice Smith, age 30", outerJoinValues.get("alice"));
            assertEquals("Bob Jones, age 25", outerJoinValues.get("bob"));
            assertEquals("Carol White, age unknown", outerJoinValues.get("carol"));

            // Verify IQv1 for outer join
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> outerJoinStore =
                headersStore(streams, outerJoinStoreName);
            assertNotNull(outerJoinStore, "Outer join store should be accessible via IQv1");
            ValueTimestampHeaders<GenericRecord> aliceResultOuter = outerJoinStore.get(createKey("alice"));
            assertNotNull(aliceResultOuter, "IQv1 for outer join: alice should exist");
            assertEquals("Alice Smith, age 30", aliceResultOuter.value().get("line").toString());
            assertSchemaIdHeaders(aliceResultOuter.headers(), outerChangelogTopic, "IQv1 outer join get alice");
            ValueTimestampHeaders<GenericRecord> bobResultOuter = outerJoinStore.get(createKey("bob"));
            assertNotNull(bobResultOuter, "IQv1 for outer join: bob should exist");
            assertEquals("Bob Jones, age 25", bobResultOuter.value().get("line").toString());
            assertSchemaIdHeaders(bobResultOuter.headers(), outerChangelogTopic, "IQv1 outer join get bob");
            ValueTimestampHeaders<GenericRecord> carolOuter = outerJoinStore.get(createKey("carol"));
            assertNotNull(carolOuter, "IQv1 for outer join: carol should exist");
            assertEquals("Carol White, age unknown", carolOuter.value().get("line").toString());
            assertSchemaIdHeaders(carolOuter.headers(), outerChangelogTopic, "IQv1 outer join get carol");

            // Tombstone: delete alice's age and verify join stores update
            produce(agesTopic, KeyValue.pair(createKey("alice"), null));
            final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> innerStoreRef = store;
            final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> leftStoreRef = leftJoinStore;
            final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> outerStoreRef = outerJoinStore;
            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> inner = innerStoreRef.get(createKey("alice"));
                    ValueTimestampHeaders<GenericRecord> left = leftStoreRef.get(createKey("alice"));
                    ValueTimestampHeaders<GenericRecord> outer = outerStoreRef.get(createKey("alice"));
                    return (inner == null || inner.value() == null)
                        && left != null && left.value() != null
                        && "Alice Smith, age unknown".equals(left.value().get("line").toString())
                        && outer != null && outer.value() != null
                        && "Alice Smith, age unknown".equals(outer.value().get("line").toString());
                },
                10_000,
                "Alice's age tombstone should propagate: inner join removes alice, left/outer join show 'age unknown'");

            // Inner join: alice should be removed (no age = no match)
            ValueTimestampHeaders<GenericRecord> aliceInnerTombstoned = store.get(createKey("alice"));
            assertTrue(aliceInnerTombstoned == null || aliceInnerTombstoned.value() == null,
                "IQv1 inner join: alice should be removed after age tombstone");
            ValueTimestampHeaders<GenericRecord> bobInnerRecord = store.get(createKey("bob"));
            assertNotNull(bobInnerRecord, "IQv1 inner join: bob should still exist");
            assertEquals("Bob Jones, age 25", bobInnerRecord.value().get("line").toString());

            // Left join: alice should still exist but with "unknown" age
            ValueTimestampHeaders<GenericRecord> aliceLeftTombstoned = leftJoinStore.get(createKey("alice"));
            assertNotNull(aliceLeftTombstoned, "IQv1 left join: alice should still exist");
            assertEquals("Alice Smith, age unknown", aliceLeftTombstoned.value().get("line").toString(),
                "IQv1 left join: alice age should be unknown after tombstone");
            assertSchemaIdHeaders(aliceLeftTombstoned.headers(), leftChangelogTopic, "IQv1 left join tombstone get alice");

            // Outer join: alice should still exist but with "unknown" age
            ValueTimestampHeaders<GenericRecord> aliceOuterTombstoned = outerJoinStore.get(createKey("alice"));
            assertNotNull(aliceOuterTombstoned, "IQv1 outer join: alice should still exist");
            assertEquals("Alice Smith, age unknown", aliceOuterTombstoned.value().get("line").toString(),
                "IQv1 outer join: alice age should be unknown after tombstone");
            assertSchemaIdHeaders(aliceOuterTombstoned.headers(), outerChangelogTopic, "IQv1 outer join tombstone get alice");

            // Null join: send age "0" for bob, which makes the inner joiner return null
            produce(agesTopic, KeyValue.pair(createKey("bob"), createTextLine("0")));
            final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> innerStoreRef2 = store;
            final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> leftStoreRef2 = leftJoinStore;
            final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> outerStoreRef2 = outerJoinStore;
            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> inner = innerStoreRef2.get(createKey("bob"));
                    ValueTimestampHeaders<GenericRecord> left = leftStoreRef2.get(createKey("bob"));
                    ValueTimestampHeaders<GenericRecord> outer = outerStoreRef2.get(createKey("bob"));
                    return (inner == null || inner.value() == null)
                        && left != null && left.value() != null
                        && "Bob Jones, age 0".equals(left.value().get("line").toString())
                        && outer != null && outer.value() != null
                        && "Bob Jones, age 0".equals(outer.value().get("line").toString());
                },
                10_000,
                "Bob's age=0 should propagate: inner join tombstones bob, left/outer show 'age 0'");

            // Re-fetch store references in case of rebalance
            store = headersStore(streams, innerJoinStoreName);
            leftJoinStore = headersStore(streams, leftJoinStoreName);
            outerJoinStore = headersStore(streams, outerJoinStoreName);

            // Inner join: bob should be tombstoned because joiner returned null for age "0"
            ValueTimestampHeaders<GenericRecord> bobInnerNullJoin = store.get(createKey("bob"));
            assertTrue(bobInnerNullJoin == null || bobInnerNullJoin.value() == null,
                "IQv1 inner join: bob should be tombstoned after null join result");

            // Left join: bob should still exist (left joiner doesn't return null for age "0")
            ValueTimestampHeaders<GenericRecord> bobLeftNullJoin = leftJoinStore.get(createKey("bob"));
            assertNotNull(bobLeftNullJoin, "IQv1 left join: bob should still exist after null inner join");
            assertEquals("Bob Jones, age 0", bobLeftNullJoin.value().get("line").toString(),
                "IQv1 left join: bob should have age 0");

            // Outer join: bob should still exist
            ValueTimestampHeaders<GenericRecord> bobOuterNullJoin = outerJoinStore.get(createKey("bob"));
            assertNotNull(bobOuterNullJoin, "IQv1 outer join: bob should still exist after null inner join");
            assertEquals("Bob Jones, age 0", bobOuterNullJoin.value().get("line").toString(),
                "IQv1 outer join: bob should have age 0");

            // Inner join changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> innerChangelogRecords =
                consumeRecords(innerChangelogTopic, "dsl-join-inner-changelog-consumer" + suffix, 4, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> innerLastByKey = lastRecordPerKey(innerChangelogRecords);
            assertEquals(2, innerLastByKey.size(),
                "inner join changelog should have exactly 2 unique keys, got " + innerLastByKey.keySet());
            assertNull(innerLastByKey.get("alice").value(),
                "inner join changelog alice final should be tombstoned");
            assertNull(innerLastByKey.get("bob").value(),
                "inner join changelog bob final should be tombstoned");

            assertChangelogHeaders(innerChangelogRecords, innerChangelogTopic,
                Set.of("alice", "bob"), "inner join changelog");

            if (!cachingEnabled) {
                assertEquals(4, innerChangelogRecords.size(),
                    "Inner join changelog (uncached) should have exactly 4 records (2 puts + 2 tombstones)");
            }

            // Left join changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> leftChangelogRecords =
                consumeRecords(leftChangelogTopic, "dsl-join-left-changelog-consumer" + suffix, 7, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> leftLastByKey = lastRecordPerKey(leftChangelogRecords);
            assertEquals(3, leftLastByKey.size(),
                "left join changelog should have exactly 3 unique keys, got " + leftLastByKey.keySet());
            assertEquals("Alice Smith, age unknown", leftLastByKey.get("alice").value().get("line").toString(),
                "left join changelog alice final value");
            assertEquals("Bob Jones, age 0", leftLastByKey.get("bob").value().get("line").toString(),
                "left join changelog bob final value");
            assertEquals("Carol White, age unknown", leftLastByKey.get("carol").value().get("line").toString(),
                "left join changelog carol final value");

            assertChangelogHeaders(leftChangelogRecords, leftChangelogTopic,
                leftLastByKey.keySet(), "left join changelog");

            if (!cachingEnabled) {
                assertEquals(7, leftChangelogRecords.size(),
                    "Left join changelog (uncached) should have exactly 7 records");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies KStream-KTable {@code join()} and {@code leftJoin()} with a headers-aware
     * table store. The stream side carries no state; only the table is materialized.
     * Covers: match found (inner + left), no match (inner skips, left emits with null
     * table side), table update mid-stream, and table tombstone.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldStreamTableJoinWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String streamTopic = "dsl-stjoin-stream" + suffix;
        String tableTopic = "dsl-stjoin-table" + suffix;
        String innerOutputTopic = "dsl-stjoin-inner-output" + suffix;
        String leftOutputTopic = "dsl-stjoin-left-output" + suffix;
        String tableStoreName = "dsl-stjoin-table-store" + suffix;

        createTopics(streamTopic, tableTopic, innerOutputTopic, leftOutputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();

        KTable<GenericRecord, GenericRecord> joinTable = builder.table(
            tableTopic, Consumed.with(keySerde, valueSerde),
            Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(tableStoreName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));

        KStream<GenericRecord, GenericRecord> inputStream =
            builder.stream(streamTopic, Consumed.with(keySerde, valueSerde));

        // Inner join
        inputStream.join(joinTable,
                (streamVal, tableVal) -> {
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    joined.put("line", streamVal.get("line") + " | " + tableVal.get("line"));
                    return joined;
                },
                Joined.with(keySerde, valueSerde, valueSerde))
            .to(innerOutputTopic, Produced.with(keySerde, valueSerde));

        // Left join
        inputStream.leftJoin(joinTable,
                (streamVal, tableVal) -> {
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    String tableStr = tableVal != null ? tableVal.get("line").toString() : "no-match";
                    joined.put("line", streamVal.get("line") + " | " + tableStr);
                    return joined;
                },
                Joined.with(keySerde, valueSerde, valueSerde))
            .to(leftOutputTopic, Produced.with(keySerde, valueSerde));

        String applicationId = "dsl-stjoin-test" + suffix;
        String tableChangelog = changelogTopicFor(applicationId, tableStoreName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

            // Populate the table first so stream events have something to join against.
            produce(tableTopic,
                KeyValue.pair(createKey("alice"), createTextLine("table-alice")),
                KeyValue.pair(createKey("bob"), createTextLine("table-bob")));

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> tableStore =
                headersStore(streams, tableStoreName);
            assertNotNull(tableStore, "Table store should be queryable");

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> a = tableStore.get(createKey("alice"));
                    ValueTimestampHeaders<GenericRecord> b = tableStore.get(createKey("bob"));
                    return a != null && a.value() != null
                        && b != null && b.value() != null;
                },
                10_000,
                "table store should be populated with alice and bob before stream events");

            // Produce stream events: alice & bob have table matches, carol does not.
            produce(streamTopic,
                KeyValue.pair(createKey("alice"), createTextLine("event-1")),
                KeyValue.pair(createKey("bob"), createTextLine("event-2")),
                KeyValue.pair(createKey("carol"), createTextLine("event-3")));

            // Inner join: only alice and bob have rows.
            List<ConsumerRecord<GenericRecord, GenericRecord>> innerResults =
                consumeRecords(innerOutputTopic, "dsl-stjoin-inner-consumer" + suffix, 2,
                    KafkaAvroDeserializer.class);
            assertEquals(2, innerResults.size(),
                "Inner join should produce exactly 2 records (alice, bob); carol has no match");
            Map<String, String> innerJoined = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> r : innerResults) {
                innerJoined.put(r.key().get("word").toString(), r.value().get("line").toString());
                assertSchemaIdHeaders(r.headers(), innerOutputTopic,
                    "inner stream-table join output " + r.key().get("word"));
            }
            assertEquals("event-1 | table-alice", innerJoined.get("alice"));
            assertEquals("event-2 | table-bob", innerJoined.get("bob"));
            assertFalse(innerJoined.containsKey("carol"),
                "carol should not appear in inner join output");

            // Left join: all 3 events have rows.
            List<ConsumerRecord<GenericRecord, GenericRecord>> leftResults =
                consumeRecords(leftOutputTopic, "dsl-stjoin-left-consumer" + suffix, 3,
                    KafkaAvroDeserializer.class);
            assertEquals(3, leftResults.size(),
                "Left join should produce exactly 3 records (alice, bob, carol)");
            Map<String, String> leftJoined = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> r : leftResults) {
                leftJoined.put(r.key().get("word").toString(), r.value().get("line").toString());
                assertSchemaIdHeaders(r.headers(), leftOutputTopic,
                    "left stream-table join output " + r.key().get("word"));
            }
            assertEquals("event-1 | table-alice", leftJoined.get("alice"));
            assertEquals("event-2 | table-bob", leftJoined.get("bob"));
            assertEquals("event-3 | no-match", leftJoined.get("carol"));

            // IQv1 verification on the table store (the only materialized state).
            ValueTimestampHeaders<GenericRecord> aliceTable = tableStore.get(createKey("alice"));
            assertNotNull(aliceTable, "table store: alice should exist");
            assertEquals("table-alice", aliceTable.value().get("line").toString());
            assertSchemaIdHeaders(aliceTable.headers(), tableChangelog, "table store: alice");

            ValueTimestampHeaders<GenericRecord> bobTable = tableStore.get(createKey("bob"));
            assertNotNull(bobTable, "table store: bob should exist");
            assertEquals("table-bob", bobTable.value().get("line").toString());
            assertSchemaIdHeaders(bobTable.headers(), tableChangelog, "table store: bob");

            assertNull(tableStore.get(createKey("carol")),
                "table store: carol should not exist (never written)");

            // Update bob's table entry to new value.
            produce(tableTopic, KeyValue.pair(createKey("bob"), createTextLine("table-bob-v2")));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> b = tableStore.get(createKey("bob"));
                    return b != null && b.value() != null
                        && "table-bob-v2".equals(b.value().get("line").toString());
                },
                10_000,
                "table store: bob should be updated to table-bob-v2");

            produce(streamTopic, KeyValue.pair(createKey("bob"), createTextLine("event-4")));

            // Wait for the v2-joined record to appear in inner join records.
            List<ConsumerRecord<GenericRecord, GenericRecord>> innerAfterUpdate =
                consumeRecords(innerOutputTopic, "dsl-stjoin-inner-consumer-v2" + suffix, 3,
                    KafkaAvroDeserializer.class);
            assertEquals(3, innerAfterUpdate.size(),
                "Inner join should now have 3 records (alice, bob, bob-v2)");
            ConsumerRecord<GenericRecord, GenericRecord> bobV2 = innerAfterUpdate.get(2);
            assertEquals("bob", bobV2.key().get("word").toString());
            assertEquals("event-4 | table-bob-v2", bobV2.value().get("line").toString());
            assertSchemaIdHeaders(bobV2.headers(), innerOutputTopic, "inner output bob v2");

            // Tombstone alice's table entry.
            produce(tableTopic, KeyValue.pair(createKey("alice"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> a = tableStore.get(createKey("alice"));
                    return a == null || a.value() == null;
                },
                10_000,
                "table store: alice should be tombstoned");

            produce(streamTopic, KeyValue.pair(createKey("alice"), createTextLine("event-5")));

            // Left join should show "no-match" for alice's post-tombstone event.
            List<ConsumerRecord<GenericRecord, GenericRecord>> leftAfterTombstone =
                consumeRecords(leftOutputTopic, "dsl-stjoin-left-consumer-final" + suffix, 5,
                    KafkaAvroDeserializer.class);
            assertEquals(5, leftAfterTombstone.size(),
                "Left join should have 5 records after the tombstone-then-event sequence");
            ConsumerRecord<GenericRecord, GenericRecord> aliceNoMatch = leftAfterTombstone.get(4);
            assertEquals("alice", aliceNoMatch.key().get("word").toString());
            assertEquals("event-5 | no-match", aliceNoMatch.value().get("line").toString(),
                "left join: alice's stream event after tombstone should join with 'no-match'");
            assertSchemaIdHeaders(aliceNoMatch.headers(), leftOutputTopic,
                "left output alice post-tombstone");

            // Inner join should tombstone alice's record.
            List<ConsumerRecord<GenericRecord, GenericRecord>> tableChangelogRecords =
                consumeRecords(tableChangelog, "dsl-stjoin-table-changelog-consumer" + suffix, 4,
                    KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> tableLastByKey =
                lastRecordPerKey(tableChangelogRecords);
            assertEquals(2, tableLastByKey.size(),
                "table changelog should have 2 unique keys, got " + tableLastByKey.keySet());
            assertNull(tableLastByKey.get("alice").value(),
                "table changelog alice final should be tombstoned");
            assertEquals("table-bob-v2", tableLastByKey.get("bob").value().get("line").toString(),
                "table changelog bob final should be table-bob-v2");

            assertChangelogHeaders(tableChangelogRecords, tableChangelog,
                Collections.singleton("alice"), "table changelog");

            if (!cachingEnabled) {
                assertEquals(4, tableChangelogRecords.size(),
                    "Table changelog (uncached) should have exactly 4 records (3 puts + 1 tombstone)");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that headers survive through toStream() and merge() operations.
     * Two KTables are materialized with headers-aware stores, converted to streams,
     * merged, and the output is checked for schema ID headers.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMergeStreamsWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic1 = "dsl-merge-input1" + suffix;
        String inputTopic2 = "dsl-merge-input2" + suffix;
        String outputTopic = "dsl-merge-output" + suffix;
        String storeName1 = "dsl-merge-store1" + suffix;
        String storeName2 = "dsl-merge-store2" + suffix;

        createTopics(inputTopic1, inputTopic2, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KTable<GenericRecord, GenericRecord> table1 = builder.table(inputTopic1,
            Consumed.with(keySerde, valueSerde),
            Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName1))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));

        KTable<GenericRecord, GenericRecord> table2 = builder.table(inputTopic2,
            Consumed.with(keySerde, valueSerde),
            Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName2))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));

        table1.toStream()
            .merge(table2.toStream())
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        String applicationId = "dsl-merge-test" + suffix;
        String changelogTopic1 = changelogTopicFor(applicationId, storeName1);
        String changelogTopic2 = changelogTopicFor(applicationId, storeName2);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic1,
                KeyValue.pair(createKey("alice"), createTextLine("alice table 1")),
                KeyValue.pair(createKey("bob"), createTextLine("bob table 1")));
            produce(inputTopic2,
                KeyValue.pair(createKey("bob"), createTextLine("bob table 2")),
                KeyValue.pair(createKey("carol"), createTextLine("carol table 2")),
                KeyValue.pair(createKey("dave"), createTextLine("dave table 2")));
            produce(inputTopic1,
                KeyValue.pair(createKey("alice"), createTextLine("alice table 1 again")));

            // With caching, the two alice records may be deduplicated in the cache,
            // so we may get 5 or 6 records. Without caching, all 6 come through.
            int minExpected = cachingEnabled ? 5 : 6;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-merge-consumer" + suffix, minExpected, KafkaAvroDeserializer.class);

            assertTrue(results.size() >= minExpected,
                "Should have at least " + minExpected + " merged records, got " + results.size());

            Map<String, String> mergedValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                String key = record.key().get("word").toString();
                mergedValues.put(key, record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), outputTopic, "merge output " + key);
            }
            assertEquals("alice table 1 again", mergedValues.get("alice"));
            assertEquals("bob table 2", mergedValues.get("bob"));
            assertEquals("carol table 2", mergedValues.get("carol"));
            assertEquals("dave table 2", mergedValues.get("dave"));

            // IQv1 verification for store 1
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store1 =
                headersStore(streams, storeName1);
            assertNotNull(store1, "Store1 should be accessible via IQv1");

            // verify store 1
            ValueTimestampHeaders<GenericRecord> aliceResult = store1.get(createKey("alice"));
            assertNotNull(aliceResult, "IQv1: alice should exist in store1");
            assertEquals("alice table 1 again", aliceResult.value().get("line").toString());
            assertSchemaIdHeaders(aliceResult.headers(), changelogTopic1, "IQv1 store1 get alice");

            ValueTimestampHeaders<GenericRecord> bobResult1 = store1.get(createKey("bob"));
            assertNotNull(bobResult1, "IQv1: bob should exist in store1");
            assertEquals("bob table 1", bobResult1.value().get("line").toString());
            assertSchemaIdHeaders(bobResult1.headers(), changelogTopic1, "IQv1 store1 get bob");

            // IQv1 verification for store 2
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store2 =
                headersStore(streams, storeName2);
            assertNotNull(store2, "Store2 should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> bobResult2 = store2.get(createKey("bob"));
            assertNotNull(bobResult2, "IQv1: bob should exist in store2");
            assertEquals("bob table 2", bobResult2.value().get("line").toString());
            assertSchemaIdHeaders(bobResult2.headers(), changelogTopic2, "IQv1 store2 get bob");

            ValueTimestampHeaders<GenericRecord> carolResult = store2.get(createKey("carol"));
            assertNotNull(carolResult, "IQv1: carol should exist in store2");
            assertEquals("carol table 2", carolResult.value().get("line").toString());
            assertSchemaIdHeaders(carolResult.headers(), changelogTopic2, "IQv1 store2 get carol");

            ValueTimestampHeaders<GenericRecord> daveResult = store2.get(createKey("dave"));
            assertNotNull(daveResult, "IQv1: dave should exist in store2");
            assertEquals("dave table 2", daveResult.value().get("line").toString());
            assertSchemaIdHeaders(daveResult.headers(), changelogTopic2, "IQv1 store2 get dave");

            // Tombstone alice from table1 and carol from table2.
            produce(inputTopic1, KeyValue.pair(createKey("alice"), null));
            produce(inputTopic2, KeyValue.pair(createKey("carol"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = store1.get(createKey("alice"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1 store1: alice should be tombstoned");
            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = store2.get(createKey("carol"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1 store2: carol should be tombstoned");

            ValueTimestampHeaders<GenericRecord> bobRecord = store1.get(createKey("bob"));
            assertNotNull(bobRecord, "IQv1: bob should still exist in store1");
            assertEquals("bob table 1", bobRecord.value().get("line").toString());
            ValueTimestampHeaders<GenericRecord> daveRecord = store2.get(createKey("dave"));
            assertNotNull(daveRecord, "IQv1: dave should still exist in store2");
            assertEquals("dave table 2", daveRecord.value().get("line").toString());

            // Store1 changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> store1Changelog =
                consumeRecords(changelogTopic1,
                    "dsl-merge-store1-changelog-consumer" + suffix, 4, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> store1LastByKey = lastRecordPerKey(store1Changelog);
            assertEquals(2, store1LastByKey.size(),
                "store1 changelog should have exactly 2 unique keys, got " + store1LastByKey.keySet());
            assertNull(store1LastByKey.get("alice").value(),
                "store1 changelog alice final should be tombstoned");
            assertEquals("bob table 1", store1LastByKey.get("bob").value().get("line").toString(),
                "store1 changelog bob final value");

            assertChangelogHeaders(store1Changelog, changelogTopic1,
                Collections.singleton("alice"), "store1 changelog");

            if (!cachingEnabled) {
                assertEquals(4, store1Changelog.size(),
                    "Store1 changelog (uncached) should have exactly 4 records (3 puts + 1 tombstone)");
            }

            // Store2 changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> store2Changelog =
                consumeRecords(changelogTopic2,
                    "dsl-merge-store2-changelog-consumer" + suffix, 4, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> store2LastByKey = lastRecordPerKey(store2Changelog);
            assertEquals(3, store2LastByKey.size(),
                "store2 changelog should have exactly 3 unique keys, got " + store2LastByKey.keySet());
            assertEquals("bob table 2", store2LastByKey.get("bob").value().get("line").toString(),
                "store2 changelog bob final value");
            assertNull(store2LastByKey.get("carol").value(),
                "store2 changelog carol final should be tombstoned");
            assertEquals("dave table 2", store2LastByKey.get("dave").value().get("line").toString(),
                "store2 changelog dave final value");

            assertChangelogHeaders(store2Changelog, changelogTopic2,
                Collections.singleton("carol"), "store2 changelog");

            if (!cachingEnabled) {
                assertEquals(4, store2Changelog.size(),
                    "Store2 changelog (uncached) should have exactly 4 records (3 puts + 1 tombstone)");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `transformValues()` on a KTable works correctly with headers-aware stores.
     * When {@code materialized=true}, the transformValues result is written to its own
     * headers-aware store and that store + its changelog are verified too.
     */
    @ParameterizedTest
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void shouldTransformValuesWithHeaders(boolean cachingEnabled, boolean materialized) throws Exception {
        String suffix = suffixOf(cachingEnabled)
            + (materialized ? "-mat" : "-nomat");
        String inputTopic = "dsl-transform-input" + suffix;
        String outputTopic = "dsl-transform-output" + suffix;
        String sourceStoreName = "dsl-transform-source-store" + suffix;
        String transformStoreName = "dsl-transform-output-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde mappedSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KTable<GenericRecord, GenericRecord> sourceTable = builder.table(
            inputTopic, Consumed.with(keySerde, valueSerde),
            Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(sourceStoreName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));

        ValueTransformerWithKeySupplier<GenericRecord, GenericRecord, GenericRecord> supplier =
            () -> new ValueTransformerWithKey<GenericRecord, GenericRecord, GenericRecord>() {
                @Override
                public void init(ProcessorContext context) {}

                @Override
                public GenericRecord transform(GenericRecord key, GenericRecord value) {
                    if (value == null) {
                        return null;
                    }
                    GenericRecord mapped = new GenericData.Record(mapValueSchema);
                    mapped.put("firstWord", value.get("line").toString().split("\\W+")[0]);
                    mapped.put("count", (long) value.get("line").toString().length());
                    return mapped;
                }

                @Override
                public void close() {}
            };

        KTable<GenericRecord, GenericRecord> transformedTable = materialized
            ? sourceTable.transformValues(supplier,
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(transformStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(mappedSerde))
            : sourceTable.transformValues(supplier);

        transformedTable.toStream().to(outputTopic, Produced.with(keySerde, mappedSerde));

        String applicationId = "dsl-transform-test" + suffix;
        String sourceChangelogTopic = changelogTopicFor(applicationId, sourceStoreName);
        String transformChangelogTopic = changelogTopicFor(applicationId, transformStoreName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("hello"), createTextLine("hello kafka streams")),
                KeyValue.pair(createKey("streams"), createTextLine("kafka streams")));

            int expected = 2;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-transform-consumer" + suffix, expected, KafkaAvroDeserializer.class);

            assertEquals(expected, results.size(),
                "Should have " + expected + " output records, got " + results.size());

            Map<String, String> firstWords = new HashMap<>();
            Map<String, Long> lengths = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                String key = record.key().get("word").toString();
                firstWords.put(key, record.value().get("firstWord").toString());
                lengths.put(key, (long) record.value().get("count"));
                assertSchemaIdHeaders(record.headers(), outputTopic, "transformValues output " + key);
            }
            assertEquals("hello", firstWords.get("hello"));
            assertEquals("kafka", firstWords.get("streams"));
            assertEquals(19L, lengths.get("hello"));
            assertEquals(13L, lengths.get("streams"));

            // IQv1 on the source store (raw TextLine values).
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> sourceStore =
                headersStore(streams, sourceStoreName);
            assertNotNull(sourceStore, "Source store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> helloSource = sourceStore.get(createKey("hello"));
            assertNotNull(helloSource, "IQv1 source store: hello should exist");
            assertEquals("hello kafka streams", helloSource.value().get("line").toString());
            assertSchemaIdHeaders(helloSource.headers(), sourceChangelogTopic, "IQv1 source get hello");

            ValueTimestampHeaders<GenericRecord> streamsSource = sourceStore.get(createKey("streams"));
            assertNotNull(streamsSource, "IQv1 source store: streams should exist");
            assertEquals("kafka streams", streamsSource.value().get("line").toString());
            assertSchemaIdHeaders(streamsSource.headers(), sourceChangelogTopic, "IQv1 source get streams");

            // IQv1 on the materialized transformValues store if transform output store is enabled.
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> transformStore = null;
            if (materialized) {
                transformStore = headersStore(streams, transformStoreName);
                assertNotNull(transformStore, "transformValues output store should be accessible via IQv1");

                ValueTimestampHeaders<GenericRecord> helloTransformed = transformStore.get(createKey("hello"));
                assertNotNull(helloTransformed, "IQv1 transform store: hello should exist");
                assertEquals("hello", helloTransformed.value().get("firstWord").toString());
                assertEquals(19L, (long) helloTransformed.value().get("count"));
                assertSchemaIdHeaders(helloTransformed.headers(), transformChangelogTopic, "IQv1 transform get hello");

                ValueTimestampHeaders<GenericRecord> streamsTransformed = transformStore.get(createKey("streams"));
                assertNotNull(streamsTransformed, "IQv1 transform store: streams should exist");
                assertEquals("kafka", streamsTransformed.value().get("firstWord").toString());
                assertEquals(13L, (long) streamsTransformed.value().get("count"));
                assertSchemaIdHeaders(streamsTransformed.headers(), transformChangelogTopic, "IQv1 transform get streams");
            }

            // Source tombstone — must propagate through transformValues and reach the transform store if materialized.
            produce(inputTopic, KeyValue.pair(createKey("hello"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = sourceStore.get(createKey("hello"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1 source store: hello should be tombstoned");

            ValueTimestampHeaders<GenericRecord> streamsRecord = sourceStore.get(createKey("streams"));
            assertNotNull(streamsRecord, "IQv1 source store: streams should still exist");
            assertEquals("kafka streams", streamsRecord.value().get("line").toString());

            if (materialized) {
                final ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> finalTransform = transformStore;
                TestUtils.waitForCondition(
                    () -> {
                        ValueTimestampHeaders<GenericRecord> v = finalTransform.get(createKey("hello"));
                        return v == null || v.value() == null;
                    },
                    10_000,
                    "IQv1 transform store: hello should be tombstoned");
                ValueTimestampHeaders<GenericRecord> streamsTransformedRecord = finalTransform.get(createKey("streams"));
                assertNotNull(streamsTransformedRecord, "IQv1 transform store: streams should still exist");
                assertEquals(13L, (long) streamsTransformedRecord.value().get("count"));
            }

            // Source changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> sourceChangelogRecords =
                consumeRecords(sourceChangelogTopic,
                    "dsl-transform-source-changelog-consumer" + suffix, 3, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> sourceLastByKey = lastRecordPerKey(sourceChangelogRecords);
            assertEquals(2, sourceLastByKey.size(),
                "source changelog should have exactly 2 unique keys, got " + sourceLastByKey.keySet());
            assertNull(sourceLastByKey.get("hello").value(),
                "source changelog hello final should be tombstoned");
            assertEquals("kafka streams", sourceLastByKey.get("streams").value().get("line").toString(),
                "source changelog streams final value");

            assertChangelogHeaders(sourceChangelogRecords, sourceChangelogTopic,
                Collections.singleton("hello"), "source changelog");

            if (!cachingEnabled) {
                assertEquals(3, sourceChangelogRecords.size(),
                    "Source changelog (uncached) should have exactly 3 records");
            }

            // Transform changelog verification (only if materialized)
            if (materialized) {
                List<ConsumerRecord<GenericRecord, GenericRecord>> transformChangelogRecords =
                    consumeRecords(transformChangelogTopic,
                        "dsl-transform-output-changelog-consumer" + suffix, 3, KafkaAvroDeserializer.class);
                Map<String, ConsumerRecord<GenericRecord, GenericRecord>> transformLastByKey = lastRecordPerKey(transformChangelogRecords);
                assertEquals(2, transformLastByKey.size(),
                    "transform changelog should have exactly 2 unique keys, got " + transformLastByKey.keySet());
                assertNull(transformLastByKey.get("hello").value(),
                    "transform changelog hello final should be tombstoned");
                assertEquals("kafka", transformLastByKey.get("streams").value().get("firstWord").toString(),
                    "transform changelog streams firstWord should be 'kafka'");
                assertEquals(13L, transformLastByKey.get("streams").value().get("count"),
                    "transform changelog streams count should be 13");

                assertChangelogHeaders(transformChangelogRecords, transformChangelogTopic,
                    Collections.singleton("hello"), "transform changelog");

                if (!cachingEnabled) {
                    assertEquals(3, transformChangelogRecords.size(),
                        "Transform changelog (uncached) should have exactly 3 records");
                }
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that {@code prefixScan()} works correctly on a headers-aware store
     * with Schema Registry header-based serialization.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPrefixScanWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-prefixscan-input" + suffix;
        String outputTopic = "dsl-prefixscan-output" + suffix;
        String storeName = "dsl-prefixscan-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        // Materialize a KTable with a headers-aware store, then stream it out.
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        String applicationId = "dsl-prefixscan-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("ka"), createTextLine("value ka")),
                KeyValue.pair(createKey("kb"), createTextLine("value kb")),
                KeyValue.pair(createKey("zz"), createTextLine("value zz")));

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-prefixscan-consumer" + suffix, 3, KafkaAvroDeserializer.class);
            assertEquals(3, results.size(), "Should have 3 output records");

            // IQv1: get the store
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            org.apache.kafka.common.serialization.Serializer<GenericRecord> prefixKeySerializer =
                (t, v) -> keySerde.serializer().serialize(
                    inputTopic, new org.apache.kafka.common.header.internals.RecordHeaders(), v);

            // prefixScan with "ka" — verifies SR serialization works for prefix key
            // and headers are correctly deserialized on the return path.
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> kaResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey("ka"), prefixKeySerializer)) {
                while (iter.hasNext()) {
                    kaResults.add(iter.next());
                }
            }

            assertEquals(1, kaResults.size(), "prefixScan('ka') should return 1 entry");
            assertEquals("ka", kaResults.get(0).key.get("word").toString());
            assertEquals("value ka", kaResults.get(0).value.value().get("line").toString());
            assertSchemaIdHeaders(kaResults.get(0).value.headers(), changelogTopic, "prefixScan ka");

            // prefixScan with "kb"
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> kbResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey("kb"), prefixKeySerializer)) {
                while (iter.hasNext()) {
                    kbResults.add(iter.next());
                }
            }

            assertEquals(1, kbResults.size(), "prefixScan('kb') should return 1 entry");
            assertEquals("kb", kbResults.get(0).key.get("word").toString());
            assertEquals("value kb", kbResults.get(0).value.value().get("line").toString());
            assertSchemaIdHeaders(kbResults.get(0).value.headers(), changelogTopic, "prefixScan kb");

            // prefixScan with "zz"
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> zzResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey("zz"), prefixKeySerializer)) {
                while (iter.hasNext()) {
                    zzResults.add(iter.next());
                }
            }
            assertEquals(1, zzResults.size(), "prefixScan('zz') should return 1 entry");
            assertEquals("zz", zzResults.get(0).key.get("word").toString());
            assertEquals("value zz", zzResults.get(0).value.value().get("line").toString());
            assertSchemaIdHeaders(zzResults.get(0).value.headers(), changelogTopic, "prefixScan zz");

            // prefixScan with "k"
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> multiResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey("k"), prefixKeySerializer)) {
                while (iter.hasNext()) {
                    multiResults.add(iter.next());
                }
            }
            assertEquals(0, multiResults.size(), "prefixScan('k') should return 0 entry");

            // prefixScan with "nope"
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> nopeResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey("nope"), prefixKeySerializer)) {
                while (iter.hasNext()) {
                    nopeResults.add(iter.next());
                }
            }
            assertEquals(0, nopeResults.size(), "prefixScan('nope') should return 0 entry");

            // prefixScan with empty byte[] prefix
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> emptyResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey(""), prefixKeySerializer)) {
                while (iter.hasNext()) {
                    emptyResults.add(iter.next());
                }
            }
            assertEquals(0, emptyResults.size(), "prefixScan('') should return 0 entry");

            // IQv1 verification
            ValueTimestampHeaders<GenericRecord> kaGet = store.get(createKey("ka"));
            assertNotNull(kaGet, "IQv1: ka should exist in store");
            assertEquals("value ka", kaGet.value().get("line").toString());
            assertSchemaIdHeaders(kaGet.headers(), changelogTopic, "IQv1 get ka");

            ValueTimestampHeaders<GenericRecord> kbGet = store.get(createKey("kb"));
            assertNotNull(kbGet, "IQv1: kb should exist in store");
            assertEquals("value kb", kbGet.value().get("line").toString());
            assertSchemaIdHeaders(kbGet.headers(), changelogTopic, "IQv1 get kb");

            ValueTimestampHeaders<GenericRecord> zzGet = store.get(createKey("zz"));
            assertNotNull(zzGet, "IQv1: zz should exist in store");
            assertEquals("value zz", zzGet.value().get("line").toString());
            assertSchemaIdHeaders(zzGet.headers(), changelogTopic, "IQv1 get zz");

            // Changelog verification — 3 distinct keys, no tombstones → count is deterministic regardless of caching.
            List<ConsumerRecord<GenericRecord, GenericRecord>> changelogRecords =
                consumeRecords(changelogTopic, "dsl-prefixscan-changelog-consumer" + suffix, 3, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> lastByKey = lastRecordPerKey(changelogRecords);
            assertEquals(3, lastByKey.size(),
                "changelog should have exactly 3 unique keys, got " + lastByKey.keySet());
            assertEquals("value ka", lastByKey.get("ka").value().get("line").toString(),
                "changelog ka final value");
            assertEquals("value kb", lastByKey.get("kb").value().get("line").toString(),
                "changelog kb final value");
            assertEquals("value zz", lastByKey.get("zz").value().get("line").toString(),
                "changelog zz final value");

            assertChangelogHeaders(changelogRecords, changelogTopic,
                Collections.emptySet(), "changelog");

            assertEquals(3, changelogRecords.size(), "Changelog should have exactly 3 records");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that {@code toTable()} works correctly with headers-aware stores.
     * toTable() materializes a stream into a table where the last write wins per
     * key and null values tombstone the key.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldStreamToTableWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-stream-totable-input" + suffix;
        String outputTopic = "dsl-stream-totable-output" + suffix;
        String storeName = "dsl-stream-totable-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .toTable(Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        String applicationId = "dsl-stream-totable-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("kafka"), createTextLine("first")),
                KeyValue.pair(createKey("kafka"), createTextLine("second")),
                KeyValue.pair(createKey("streams"), createTextLine("only")),
                KeyValue.pair(createKey("hello"), createTextLine("first")));

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                headersStore(streams, storeName);
            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = store.get(createKey("hello"));
                    return v != null && v.value() != null
                        && "first".equals(v.value().get("line").toString());
                },
                10_000,
                "store: hello should have latest value 'first'"
            );

            Map<String, GenericRecord> values = new HashMap<>();
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-stream-totable-phase1-" + suffix, 4, KafkaAvroDeserializer.class);
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                values.put(record.key().get("word").toString(), record.value());
                assertSchemaIdHeaders(record.headers(), outputTopic, "stream->table output " + record.key().get("word"));
            }
            assertNotNull(values.get("kafka"), "kafka should appear in output");
            assertEquals("second", values.get("kafka").get("line").toString(), "kafka latest value should be 'second'");
            assertNotNull(values.get("streams"), "streams should appear in output");
            assertEquals("only", values.get("streams").get("line").toString(), "streams value should be 'only'");
            assertNotNull(values.get("hello"), "hello should be populated");
            assertEquals("first", values.get("hello").get("line").toString(), "hello value should be 'first'");

            // Send a tombstone for "hello"
            produce(inputTopic, KeyValue.pair(createKey("hello"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = store.get(createKey("hello"));
                    return v == null || v.value() == null;
                },
                10_000,
                "store: hello should be tombstoned");

            results = consumeRecords(outputTopic, "dsl-stream-totable-phase2-" + suffix + "-" + System.nanoTime(), 5, KafkaAvroDeserializer.class);

            // Last write wins per key in the resulting table.
            Map<String, GenericRecord> finalValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                finalValues.put(record.key().get("word").toString(), record.value());
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), outputTopic,
                        "stream->table output " + record.key().get("word"));
                } else {
                    assertKeySchemaIdHeader(record.headers(), outputTopic,
                        "stream->table tombstone " + record.key().get("word"));
                    assertNull(record.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER),
                        "tombstone should not carry __value_schema_id");
                }
            }
            assertNotNull(finalValues.get("kafka"), "kafka should appear in output");
            assertEquals("second", finalValues.get("kafka").get("line").toString(),
                "kafka latest value should be 'second'");
            assertNotNull(finalValues.get("streams"), "streams should appear in output");
            assertEquals("only", finalValues.get("streams").get("line").toString(),
                "streams value should be 'only'");
            assertTrue(finalValues.containsKey("hello"),
                "hello should appear in output (tombstone)");
            assertNull(finalValues.get("hello"), "hello should be tombstoned in output");

            // IQv1 verification on the store after all updates and the tombstone.
            ValueTimestampHeaders<GenericRecord> kafkaResult = store.get(createKey("kafka"));
            assertNotNull(kafkaResult, "IQv1: kafka should exist in store");
            assertEquals("second", kafkaResult.value().get("line").toString(),
                "IQv1: kafka should have latest value 'second'");
            assertSchemaIdHeaders(kafkaResult.headers(), changelogTopic, "IQv1 get kafka");

            ValueTimestampHeaders<GenericRecord> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals("only", streamsResult.value().get("line").toString(),
                "IQv1: streams should have value 'only'");
            assertSchemaIdHeaders(streamsResult.headers(), changelogTopic, "IQv1 get streams");

            assertNull(store.get(createKey("hello")),
                "IQv1: hello should be tombstoned in store");

            // Changelog: 5 records uncached, possibly fewer cached due to coalescing.
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeRecords(changelogTopic,
                    "dsl-stream-totable-changelog-consumer" + suffix, 5,
                    org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                String key = record.key().get("word").toString();
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), changelogTopic, "changelog " + key);
                } else {
                    assertEquals("hello", key, "Only hello should be tombstoned in changelog");
                    assertKeySchemaIdHeader(record.headers(), changelogTopic, "changelog tombstone " + key);
                }
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that {@code cogroup()} works correctly with headers-aware stores.
     * cogroup() merges multiple grouped streams into a single aggregation per key.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldKStreamCogroupWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic1 = "dsl-kstream-cogroup-input1" + suffix;
        String inputTopic2 = "dsl-kstream-cogroup-input2" + suffix;
        String outputTopic = "dsl-kstream-cogroup-output" + suffix;
        String storeName = "dsl-kstream-cogroup-store" + suffix;

        createTopics(inputTopic1, inputTopic2, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KGroupedStream<GenericRecord, GenericRecord> grouped1 = builder
            .stream(inputTopic1, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde));
        KGroupedStream<GenericRecord, GenericRecord> grouped2 = builder
            .stream(inputTopic2, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde));
        Aggregator<GenericRecord, GenericRecord, GenericRecord> groupAggregator = (key, value, agg) -> {
            if ("DELETE".equals(value.get("line").toString())) {
                return null;
            }
            GenericRecord updated = new GenericData.Record(aggSchema);
            updated.put("word", key.get("word").toString());
            updated.put("count", (long)agg.get("count") + 1);
            return updated;
        };
        grouped1.cogroup(groupAggregator)
            .cogroup(grouped2,groupAggregator)
            .aggregate(
                () -> {
                GenericRecord init = new GenericData.Record(aggSchema);
                init.put("word","");
                init.put("count", 0L);
                return init;
            },
                Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, aggSerde));

        String applicationId = "dsl-kstream-cogroup-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);
            // Send 3 records for "kafka", 2 for "streams", 1 for "hello"
            produce(inputTopic1,
                KeyValue.pair(createKey("kafka"), createTextLine("first")),
                KeyValue.pair(createKey("kafka"), createTextLine("second")));
            produce(inputTopic2, KeyValue.pair(createKey("kafka"), createTextLine("third")));
            produce(inputTopic1, KeyValue.pair(createKey("streams"), createTextLine("first")));
            produce(inputTopic2, KeyValue.pair(createKey("streams"), createTextLine("second")));
            produce(inputTopic1,
                KeyValue.pair(createKey("hello"), createTextLine("first")),
                // Send a null value for "hello", should not be treated as a tombstone.
                KeyValue.pair(createKey("hello"), null));
            produce(inputTopic2, KeyValue.pair(createKey("hi"), createTextLine("first")));

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-kstream-cogroup-consumer" + suffix, 7, KafkaAvroDeserializer.class);

            Map<String, Long> finalCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                finalCounts.put(record.key().get("word").toString(), (long)record.value().get("count"));
            }
            assertEquals(3L, finalCounts.get("kafka"), "kafka should have count 3");
            assertEquals(2L, finalCounts.get("streams"), "streams should have count 2");
            assertEquals(1L, finalCounts.get("hello"), "hello should have count 1");
            assertEquals(1L, finalCounts.get("hi"), "hi should have count 1");

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> kafkaResult = store.get(createKey("kafka"));
            assertNotNull(kafkaResult, "IQv1: kafka should exist in store");
            assertEquals(3L, kafkaResult.value().get("count"), "IQv1: kafka count should be 3");
            assertSchemaIdHeaders(kafkaResult.headers(), changelogTopic, "IQv1 get kafka");

            ValueTimestampHeaders<GenericRecord> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals(2L, streamsResult.value().get("count"), "IQv1: streams count should be 2");
            assertSchemaIdHeaders(streamsResult.headers(), changelogTopic, "IQv1 get streams");

            ValueTimestampHeaders<GenericRecord> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals(1L, helloResult.value().get("count"), "IQv1: hello count should be 1");
            assertSchemaIdHeaders(helloResult.headers(), changelogTopic, "IQv1 get hello");

            ValueTimestampHeaders<GenericRecord> hiResult = store.get(createKey("hi"));
            assertNotNull(hiResult, "IQv1: hi should exist in store");
            assertEquals(1L, hiResult.value().get("count"), "IQv1: hi count should be 1");
            assertSchemaIdHeaders(hiResult.headers(), changelogTopic, "IQv1 get hi");

            // Tombstone "kafka" by sending a record with value null, which should trigger a delete in the store.
            produce(inputTopic1, KeyValue.pair(createKey("kafka"), createTextLine("DELETE")));
            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = store.get(createKey("kafka"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1 store1: 'kafka' should be tombstoned");

            assertNull(store.get(createKey("kafka")), "IQv1: kafka should be tombstoned in store after DELETE");

            ValueTimestampHeaders<GenericRecord> streamsAfter = store.get(createKey("streams"));
            assertNotNull(streamsAfter, "IQv1: streams should still exist after DELETE");
            assertEquals(2L, streamsAfter.value().get("count"),
                "IQv1: streams count should still be 2 after DELETE");
            assertSchemaIdHeaders(streamsAfter.headers(), changelogTopic, "IQv1 get streams post-tombstone");

            ValueTimestampHeaders<GenericRecord> helloAfter = store.get(createKey("hello"));
            assertNotNull(helloAfter, "IQv1: hello should still exist after DELETE");
            assertEquals(1L, helloAfter.value().get("count"),
                "IQv1: hello count should still be 1 after DELETE");
            assertSchemaIdHeaders(helloAfter.headers(), changelogTopic, "IQv1 get hello post-tombstone");

            ValueTimestampHeaders<GenericRecord> hiAfter = store.get(createKey("hi"));
            assertNotNull(hiAfter, "IQv1: hi should still exist after DELETE");
            assertEquals(1L, hiAfter.value().get("count"),
                "IQv1: hi count should still be 1 after DELETE");
            assertSchemaIdHeaders(hiAfter.headers(), changelogTopic, "IQv1 get hi post-tombstone");

            // Changelog verification
            List<ConsumerRecord<GenericRecord, GenericRecord>> changelogRecords =
                consumeRecords(changelogTopic,
                    "dsl-kstream-cogroup-changelog-consumer" + suffix, 8, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> lastByKey = lastRecordPerKey(changelogRecords);
            assertEquals(4, lastByKey.size(),
                "changelog should have exactly 4 unique keys, got " + lastByKey.keySet());
            assertNull(lastByKey.get("kafka").value(), "changelog kafka final should be tombstoned");
            assertEquals(2L, lastByKey.get("streams").value().get("count"),
                "changelog streams final count should be 2");
            assertEquals(1L, lastByKey.get("hello").value().get("count"),
                "changelog hello final count should be 1");
            assertEquals(1L, lastByKey.get("hi").value().get("count"),
                "changelog hi final count should be 1");

            assertChangelogHeaders(changelogRecords, changelogTopic,
                Collections.singleton("kafka"), "changelog");

            if (!cachingEnabled) {
                assertEquals(8, changelogRecords.size(),
                    "Changelog (uncached) should have exactly 8 records (7 puts + 1 tombstone)");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `globalTable()` materialized with a headers-aware store. Global tables don't
     * have a separate changelog topic — the source topic IS the changelog — so all schema-id
     * checks resolve against the input topic's subjects.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGlobalTableWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-globaltable-input" + suffix;
        String storeName = "dsl-globaltable-store" + suffix;

        createTopics(inputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable(inputTopic, Consumed.with(keySerde, valueSerde),
            Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));

        String applicationId = "dsl-globaltable-test" + suffix;

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("alice"), createTextLine("alice value")),
                KeyValue.pair(createKey("bob"), createTextLine("bob value")),
                KeyValue.pair(createKey("carol"), createTextLine("carol value")));

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Global store should be accessible via IQv1");

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = store.get(createKey("carol"));
                    return v != null && v.value() != null;
                },
                10_000,
                "global store: carol should be populated");

            ValueTimestampHeaders<GenericRecord> alice = store.get(createKey("alice"));
            assertNotNull(alice, "global store: alice should exist");
            assertEquals("alice value", alice.value().get("line").toString());
            assertSchemaIdHeaders(alice.headers(), inputTopic, "global store: alice");

            ValueTimestampHeaders<GenericRecord> bob = store.get(createKey("bob"));
            assertNotNull(bob, "global store: bob should exist");
            assertEquals("bob value", bob.value().get("line").toString());
            assertSchemaIdHeaders(bob.headers(), inputTopic, "global store: bob");

            ValueTimestampHeaders<GenericRecord> carol = store.get(createKey("carol"));
            assertNotNull(carol, "global store: carol should exist");
            assertEquals("carol value", carol.value().get("line").toString());
            assertSchemaIdHeaders(carol.headers(), inputTopic, "global store: carol");

            // Tombstone alice — must propagate to the global store.
            produce(inputTopic, KeyValue.pair(createKey("alice"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = store.get(createKey("alice"));
                    return v == null || v.value() == null;
                },
                10_000,
                "global store: alice should be tombstoned after null input");

            assertNull(store.get(createKey("alice")));
            ValueTimestampHeaders<GenericRecord> bobAfter = store.get(createKey("bob"));
            assertNotNull(bobAfter, "global store: bob should still exist");
            assertEquals("bob value", bobAfter.value().get("line").toString());
            assertSchemaIdHeaders(bobAfter.headers(), inputTopic, "global store: bob after tombstone");

            // Add back tombstoned alice, add new bob record. Both should update in the global store.
            produce(inputTopic,
                KeyValue.pair(createKey("alice"), createTextLine("alice value 2")),
                KeyValue.pair(createKey("bob"), createTextLine("bob value 2")));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> vAlice = store.get(createKey("alice"));
                    ValueTimestampHeaders<GenericRecord> vBob = store.get(createKey("bob"));
                    return vAlice.value() != null
                        && vBob.value() != null
                        && "bob value 2".equals(vBob.value().get("line").toString());
                },
                10_000,
                "global store: alice should be updated to value 2, bob should be updated to value 2");

            ValueTimestampHeaders<GenericRecord> aliceAfter = store.get(createKey("alice"));
            assertNotNull(aliceAfter, "global store: alice should exist after update");
            assertEquals("alice value 2", aliceAfter.value().get("line").toString());
            assertSchemaIdHeaders(aliceAfter.headers(), inputTopic, "global store: alice after update");

            ValueTimestampHeaders<GenericRecord> bobAfterUpdate = store.get(createKey("bob"));
            assertNotNull(bobAfterUpdate, "global store: bob should exist after update");
            assertEquals("bob value 2", bobAfterUpdate.value().get("line").toString());
            assertSchemaIdHeaders(bobAfterUpdate.headers(), inputTopic, "global store: bob after update");

            ValueTimestampHeaders<GenericRecord> carolAfter = store.get(createKey("carol"));
            assertNotNull(carolAfter, "global store: carol should still exist");
            assertEquals("carol value", carolAfter.value().get("line").toString());
            assertSchemaIdHeaders(carolAfter.headers(), inputTopic, "global store: carol");

            // Input topic verification (acts as the changelog for global tables)
            List<ConsumerRecord<GenericRecord, GenericRecord>> inputRecords =
                consumeRecords(inputTopic, "dsl-globaltable-input-consumer" + suffix, 6, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> lastByKey = lastRecordPerKey(inputRecords);
            assertEquals(3, lastByKey.size(),
                "input topic should have exactly 3 unique keys, got " + lastByKey.keySet());
            assertEquals("alice value 2", lastByKey.get("alice").value().get("line").toString(),
                "input topic alice final value");
            assertEquals("bob value 2", lastByKey.get("bob").value().get("line").toString(),
                "input topic bob final value");
            assertEquals("carol value", lastByKey.get("carol").value().get("line").toString(),
                "input topic carol final value");

            assertChangelogHeaders(inputRecords, inputTopic,
                Collections.singleton("alice"), "input");

            assertEquals(6, inputRecords.size(),
                "Input topic should have exactly 6 records (5 puts + 1 tombstone)");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that {@code KStream.process()} works correctly with a headers-aware
     * store attached via the PAPI-on-DSL pattern. The processor reads/updates the
     * store directly and forwards a count per key.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldProcessWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-process-input" + suffix;
        String outputTopic = "dsl-process-output" + suffix;
        String storeName = "dsl-process-store" + suffix;

        createTopics(inputTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                keySerde,
                Serdes.Long()));

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(
                (org.apache.kafka.streams.processor.api.ProcessorSupplier<GenericRecord, GenericRecord, GenericRecord, Long>) () ->
                    new org.apache.kafka.streams.processor.api.Processor<GenericRecord, GenericRecord, GenericRecord, Long>() {
                        private TimestampedKeyValueStoreWithHeaders<GenericRecord, Long> store;
                        private org.apache.kafka.streams.processor.api.ProcessorContext<GenericRecord, Long> ctx;

                        @Override
                        public void init(org.apache.kafka.streams.processor.api.ProcessorContext<GenericRecord, Long> context) {
                            this.ctx = context;
                            this.store = context.getStateStore(storeName);
                        }

                        @Override
                        public void process(org.apache.kafka.streams.processor.api.Record<GenericRecord, GenericRecord> record) {
                            if (record.value() == null) {
                                store.delete(record.key());
                                ctx.forward(new org.apache.kafka.streams.processor.api.Record<>(
                                    record.key(), null, record.timestamp(), record.headers()));
                                return;
                            }
                            ValueTimestampHeaders<Long> existing = store.get(record.key());
                            long current = (existing != null && existing.value() != null) ? existing.value() : 0L;
                            long updated = current + 1L;
                            store.put(record.key(),
                                ValueTimestampHeaders.make(updated, record.timestamp(), record.headers()));
                            ctx.forward(new org.apache.kafka.streams.processor.api.Record<>(
                                record.key(), updated, record.timestamp(), record.headers()));
                        }
                    },
                storeName)
            .to(outputTopic, Produced.with(keySerde, Serdes.Long()));

        String applicationId = "dsl-process-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("kafka"), createTextLine("first")),
                KeyValue.pair(createKey("kafka"), createTextLine("second")),
                KeyValue.pair(createKey("kafka"), createTextLine("third")),
                KeyValue.pair(createKey("streams"), createTextLine("first")),
                KeyValue.pair(createKey("streams"), createTextLine("second")),
                KeyValue.pair(createKey("hello"), createTextLine("first")));

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<Long>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<Long> v = store.get(createKey("kafka"));
                    return v != null && v.value() != null && v.value() == 3L;
                },
                30_000,
                "IQv1: kafka should reach count 3");

            ValueTimestampHeaders<Long> kafkaResult = store.get(createKey("kafka"));
            assertEquals(3L, kafkaResult.value(), "IQv1: kafka count should be 3");
            assertKeySchemaIdHeader(kafkaResult.headers(), inputTopic, "IQv1 get kafka");

            ValueTimestampHeaders<Long> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals(2L, streamsResult.value(), "IQv1: streams count should be 2");
            assertKeySchemaIdHeader(streamsResult.headers(), inputTopic, "IQv1 get streams");

            ValueTimestampHeaders<Long> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals(1L, helloResult.value(), "IQv1: hello count should be 1");
            assertKeySchemaIdHeader(helloResult.headers(), inputTopic, "IQv1 get hello");

            List<ConsumerRecord<GenericRecord, Long>> results =
                consumeRecords(outputTopic, "dsl-process-consumer" + suffix, 6,
                    org.apache.kafka.common.serialization.LongDeserializer.class);
            assertEquals(6, results.size(), "Output should have exactly 6 records");
            Map<String, Long> latestCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, Long> record : results) {
                latestCounts.put(record.key().get("word").toString(), record.value());
                assertKeySchemaIdHeader(record.headers(), outputTopic,
                    "process output " + record.key().get("word"));
            }
            assertEquals(3L, latestCounts.get("kafka"), "kafka latest count should be 3");
            assertEquals(2L, latestCounts.get("streams"), "streams latest count should be 2");
            assertEquals(1L, latestCounts.get("hello"), "hello latest count should be 1");

            produce(inputTopic, KeyValue.pair(createKey("kafka"), null));

            TestUtils.waitForCondition(
                () -> store.get(createKey("kafka")) == null,
                30_000,
                "IQv1: kafka should be tombstoned");

            assertNull(store.get(createKey("kafka")),
                "IQv1: kafka should be deleted after tombstone");

            ValueTimestampHeaders<Long> streamsAfter = store.get(createKey("streams"));
            assertNotNull(streamsAfter, "IQv1: streams should still exist after tombstone");
            assertEquals(2L, streamsAfter.value(),
                "IQv1: streams count should still be 2 after tombstone");
            assertKeySchemaIdHeader(streamsAfter.headers(), inputTopic, "IQv1 get streams post-tombstone");

            ValueTimestampHeaders<Long> helloAfter = store.get(createKey("hello"));
            assertNotNull(helloAfter, "IQv1: hello should still exist after tombstone");
            assertEquals(1L, helloAfter.value(),
                "IQv1: hello count should still be 1 after tombstone");
            assertKeySchemaIdHeader(helloAfter.headers(), inputTopic, "IQv1 get hello post-tombstone");

            // Changelog verification
            List<ConsumerRecord<GenericRecord, Long>> changelogRecords =
                consumeRecords(changelogTopic, "dsl-process-changelog-consumer" + suffix, 7,
                    org.apache.kafka.common.serialization.LongDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, Long>> lastByKey = lastRecordPerKey(changelogRecords);
            assertEquals(3, lastByKey.size(),
                "changelog should have exactly 3 unique keys, got " + lastByKey.keySet());
            assertNull(lastByKey.get("kafka").value(), "changelog kafka final should be tombstoned");
            assertEquals(2L, lastByKey.get("streams").value(), "changelog streams final count should be 2");
            assertEquals(1L, lastByKey.get("hello").value(), "changelog hello final count should be 1");

            for (ConsumerRecord<GenericRecord, Long> r : changelogRecords) {
                String key = r.key().get("word").toString();
                assertKeySchemaIdHeader(r.headers(), changelogTopic, "changelog " + key);
                if (r.value() == null) {
                    assertEquals("kafka", key, "Only kafka should be tombstoned, got tombstone for " + key);
                }
            }

            if (!cachingEnabled) {
                assertEquals(7, changelogRecords.size(),
                    "Changelog (uncached) should have exactly 7 records (6 puts + 1 tombstone)");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that {@code KStream.processValues()} works correctly with a
     * headers-aware store attached via the PAPI-on-DSL pattern. The fixed-key
     * processor reads/updates the store directly and forwards a count per key.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldProcessValuesWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-processvalues-input" + suffix;
        String outputTopic = "dsl-processvalues-output" + suffix;
        String storeName = "dsl-processvalues-store" + suffix;

        createTopics(inputTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                keySerde,
                Serdes.Long()));

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .processValues(
                (org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier<GenericRecord, GenericRecord, Long>) () ->
                    new org.apache.kafka.streams.processor.api.FixedKeyProcessor<GenericRecord, GenericRecord, Long>() {
                        private TimestampedKeyValueStoreWithHeaders<GenericRecord, Long> store;
                        private org.apache.kafka.streams.processor.api.FixedKeyProcessorContext<GenericRecord, Long> ctx;

                        @Override
                        public void init(org.apache.kafka.streams.processor.api.FixedKeyProcessorContext<GenericRecord, Long> context) {
                            this.ctx = context;
                            this.store = context.getStateStore(storeName);
                        }

                        @Override
                        public void process(org.apache.kafka.streams.processor.api.FixedKeyRecord<GenericRecord, GenericRecord> record) {
                            if (record.value() == null) {
                                store.delete(record.key());
                                ctx.forward(record.withValue(null));
                                return;
                            }
                            ValueTimestampHeaders<Long> existing = store.get(record.key());
                            long current = (existing != null && existing.value() != null) ? existing.value() : 0L;
                            long updated = current + 1L;
                            store.put(record.key(),
                                ValueTimestampHeaders.make(updated, record.timestamp(), record.headers()));
                            ctx.forward(record.withValue(updated));
                        }
                    },
                storeName)
            .to(outputTopic, Produced.with(keySerde, Serdes.Long()));

        String applicationId = "dsl-processvalues-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("kafka"), createTextLine("first")),
                KeyValue.pair(createKey("kafka"), createTextLine("second")),
                KeyValue.pair(createKey("kafka"), createTextLine("third")),
                KeyValue.pair(createKey("streams"), createTextLine("first")),
                KeyValue.pair(createKey("streams"), createTextLine("second")),
                KeyValue.pair(createKey("hello"), createTextLine("first")));

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<Long>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<Long> v = store.get(createKey("kafka"));
                    return v != null && v.value() != null && v.value() == 3L;
                },
                30_000,
                "IQv1: kafka should reach count 3");

            ValueTimestampHeaders<Long> kafkaResult = store.get(createKey("kafka"));
            assertEquals(3L, kafkaResult.value(), "IQv1: kafka count should be 3");
            assertKeySchemaIdHeader(kafkaResult.headers(), inputTopic, "IQv1 get kafka");

            ValueTimestampHeaders<Long> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals(2L, streamsResult.value(), "IQv1: streams count should be 2");
            assertKeySchemaIdHeader(streamsResult.headers(), inputTopic, "IQv1 get streams");

            ValueTimestampHeaders<Long> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals(1L, helloResult.value(), "IQv1: hello count should be 1");
            assertKeySchemaIdHeader(helloResult.headers(), inputTopic, "IQv1 get hello");

            List<ConsumerRecord<GenericRecord, Long>> results =
                consumeRecords(outputTopic, "dsl-processvalues-consumer" + suffix, 6,
                    org.apache.kafka.common.serialization.LongDeserializer.class);
            assertEquals(6, results.size(), "Output should have exactly 6 records");
            Map<String, Long> latestCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, Long> record : results) {
                latestCounts.put(record.key().get("word").toString(), record.value());
                assertKeySchemaIdHeader(record.headers(), outputTopic,
                    "processValues output " + record.key().get("word"));
            }
            assertEquals(3L, latestCounts.get("kafka"), "kafka latest count should be 3");
            assertEquals(2L, latestCounts.get("streams"), "streams latest count should be 2");
            assertEquals(1L, latestCounts.get("hello"), "hello latest count should be 1");

            produce(inputTopic, KeyValue.pair(createKey("kafka"), null));

            TestUtils.waitForCondition(
                () -> store.get(createKey("kafka")) == null,
                30_000,
                "IQv1: kafka should be tombstoned");

            assertNull(store.get(createKey("kafka")),
                "IQv1: kafka should be deleted after tombstone");

            ValueTimestampHeaders<Long> streamsAfter = store.get(createKey("streams"));
            assertNotNull(streamsAfter, "IQv1: streams should still exist after tombstone");
            assertEquals(2L, streamsAfter.value(),
                "IQv1: streams count should still be 2 after tombstone");
            assertKeySchemaIdHeader(streamsAfter.headers(), inputTopic, "IQv1 get streams post-tombstone");

            ValueTimestampHeaders<Long> helloAfter = store.get(createKey("hello"));
            assertNotNull(helloAfter, "IQv1: hello should still exist after tombstone");
            assertEquals(1L, helloAfter.value(),
                "IQv1: hello count should still be 1 after tombstone");
            assertKeySchemaIdHeader(helloAfter.headers(), inputTopic, "IQv1 get hello post-tombstone");

            // Changelog verification
            List<ConsumerRecord<GenericRecord, Long>> changelogRecords =
                consumeRecords(changelogTopic, "dsl-processvalues-changelog-consumer" + suffix, 7,
                    org.apache.kafka.common.serialization.LongDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, Long>> lastByKey = lastRecordPerKey(changelogRecords);
            assertEquals(3, lastByKey.size(),
                "changelog should have exactly 3 unique keys, got " + lastByKey.keySet());
            assertNull(lastByKey.get("kafka").value(), "changelog kafka final should be tombstoned");
            assertEquals(2L, lastByKey.get("streams").value(), "changelog streams final count should be 2");
            assertEquals(1L, lastByKey.get("hello").value(), "changelog hello final count should be 1");

            for (ConsumerRecord<GenericRecord, Long> r : changelogRecords) {
                String key = r.key().get("word").toString();
                assertKeySchemaIdHeader(r.headers(), changelogTopic, "changelog " + key);
                if (r.value() == null) {
                    assertEquals("kafka", key, "Only kafka should be tombstoned, got tombstone for " + key);
                }
            }

            if (!cachingEnabled) {
                assertEquals(7, changelogRecords.size(),
                    "Changelog (uncached) should have exactly 7 records (6 puts + 1 tombstone)");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies foreign-key KTable-KTable join works correctly with headers-aware stores.
     *
     * <p>Names table: key=person, value.line=FK string into the ages table.
     * Ages table: key=FK string, value.line=age value.
     * Joiner emits "FK string, age VALUE". Because names values carry only the FK (no
     * actual person name), joined results read like "age30, age 30".
     *
     * <p>Test steps:
     * <ol>
     *   <li>initial population (3 join emissions),</li>
     *   <li>FK re-route (carol changes FK target from age30 → age25),</li>
     *   <li>FK target value update (age30 → "31", refreshes alice),</li>
     *   <li>FK target tombstone (delete age30, tombstones alice),</li>
     *   <li>names-side tombstone (delete bob from names, tombstones bob),</li>
     *   <li>null-joiner tombstone (age25 → "0", joiner returns null, tombstones carol).</li>
     * </ol>
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldForeignKeyJoinWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String namesTopic = "dsl-fkjoin-names" + suffix;
        String agesTopic = "dsl-fkjoin-ages" + suffix;
        String outputTopic = "dsl-fkjoin-output" + suffix;
        String namesStoreName = "dsl-fkjoin-names-store" + suffix;
        String agesStoreName = "dsl-fkjoin-ages-store" + suffix;
        String joinStoreName = "dsl-fkjoin-result-store" + suffix;

        createTopics(namesTopic, agesTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();

        // Names table: key=person, value.line=age key (FK into ages table)
        KTable<GenericRecord, GenericRecord> namesTable =
            builder.table(namesTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(namesStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        // Ages table: key=age key, value.line=age value
        KTable<GenericRecord, GenericRecord> agesTable =
            builder.table(agesTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(agesStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        namesTable.join(agesTable,
                // FK extractor: names value's "line" field is the FK (age key).
                nameValue -> {
                    GenericRecord fk = new GenericData.Record(keySchema);
                    fk.put("word", nameValue.get("line").toString());
                    return fk;
                },
                // Joiner: returns null when age value is "0" to exercise the null-join
                // tombstone path; otherwise concatenates the FK string with the age value.
                (nameValue, ageValue) -> {
                    if ("0".equals(ageValue.get("line").toString())) {
                        return null;
                    }
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    joined.put("line", nameValue.get("line") + ", age " + ageValue.get("line"));
                    return joined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(joinStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        String applicationId = "dsl-fkjoin-test" + suffix;
        String namesChangelog = changelogTopicFor(applicationId, namesStoreName);
        String agesChangelog = changelogTopicFor(applicationId, agesStoreName);
        String joinChangelog = changelogTopicFor(applicationId, joinStoreName);

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            // Phase 1 — populate ages first so when names lands the FK lookup hits an
            // existing ages entry rather than a transient miss.
            produce(agesTopic,
                KeyValue.pair(createKey("age30"), createTextLine("30")),
                KeyValue.pair(createKey("age25"), createTextLine("25")));

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> agesStore =
                headersStore(streams, agesStoreName);
            assertNotNull(agesStore, "ages store should be queryable");
            TestUtils.waitForCondition(
                () -> agesStore.get(createKey("age30")) != null
                    && agesStore.get(createKey("age25")) != null,
                10_000,
                "ages store should be populated before producing names");

            produce(namesTopic,
                KeyValue.pair(createKey("alice"), createTextLine("age30")),
                KeyValue.pair(createKey("bob"), createTextLine("age25")),
                KeyValue.pair(createKey("carol"), createTextLine("age30")));

            // Initial 3 join emissions.
            List<ConsumerRecord<GenericRecord, GenericRecord>> initialResults =
                consumeRecords(outputTopic, "dsl-fkjoin-consumer-initial" + suffix, 3,
                    KafkaAvroDeserializer.class);
            assertEquals(3, initialResults.size(),
                "Should have exactly 3 initial FK join results");

            Map<String, String> initialJoined = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : initialResults) {
                initialJoined.put(record.key().get("word").toString(),
                    record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), outputTopic,
                    "FK join initial output " + record.key().get("word"));
            }
            assertEquals("age30, age 30", initialJoined.get("alice"));
            assertEquals("age25, age 25", initialJoined.get("bob"));
            assertEquals("age30, age 30", initialJoined.get("carol"));

            // IQv1 verification — names source store.
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> namesStore =
                headersStore(streams, namesStoreName);
            assertNotNull(namesStore, "names store should be queryable");

            ValueTimestampHeaders<GenericRecord> aliceName = namesStore.get(createKey("alice"));
            assertNotNull(aliceName, "names store: alice should exist");
            assertEquals("age30", aliceName.value().get("line").toString());
            assertSchemaIdHeaders(aliceName.headers(), namesChangelog, "names store: alice");

            ValueTimestampHeaders<GenericRecord> bobName = namesStore.get(createKey("bob"));
            assertNotNull(bobName, "names store: bob should exist");
            assertEquals("age25", bobName.value().get("line").toString());
            assertSchemaIdHeaders(bobName.headers(), namesChangelog, "names store: bob");

            ValueTimestampHeaders<GenericRecord> carolName = namesStore.get(createKey("carol"));
            assertNotNull(carolName, "names store: carol should exist");
            assertEquals("age30", carolName.value().get("line").toString());
            assertSchemaIdHeaders(carolName.headers(), namesChangelog, "names store: carol");

            // IQv1 verification — ages source store.
            ValueTimestampHeaders<GenericRecord> age30Result = agesStore.get(createKey("age30"));
            assertNotNull(age30Result, "ages store: age30 should exist");
            assertEquals("30", age30Result.value().get("line").toString());
            assertSchemaIdHeaders(age30Result.headers(), agesChangelog, "ages store: age30");

            ValueTimestampHeaders<GenericRecord> age25Result = agesStore.get(createKey("age25"));
            assertNotNull(age25Result, "ages store: age25 should exist");
            assertEquals("25", age25Result.value().get("line").toString());
            assertSchemaIdHeaders(age25Result.headers(), agesChangelog, "ages store: age25");

            // IQv1 verification — join result store.
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> joinStore =
                headersStore(streams, joinStoreName);
            assertNotNull(joinStore, "FK join store should be queryable");

            ValueTimestampHeaders<GenericRecord> aliceResult = joinStore.get(createKey("alice"));
            assertNotNull(aliceResult, "IQv1: alice should exist in FK join store");
            assertEquals("age30, age 30", aliceResult.value().get("line").toString());
            assertSchemaIdHeaders(aliceResult.headers(), joinChangelog, "IQv1 FK join get alice");

            ValueTimestampHeaders<GenericRecord> bobResult = joinStore.get(createKey("bob"));
            assertNotNull(bobResult, "IQv1: bob should exist in FK join store");
            assertEquals("age25, age 25", bobResult.value().get("line").toString());
            assertSchemaIdHeaders(bobResult.headers(), joinChangelog, "IQv1 FK join get bob");

            // Phase 2 — FK re-route. carol's FK changes age30 → age25; the join entry
            // for carol must update to reflect the new FK target.
            produce(namesTopic, KeyValue.pair(createKey("carol"), createTextLine("age25")));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = joinStore.get(createKey("carol"));
                    return v != null && v.value() != null
                        && "age25, age 25".equals(v.value().get("line").toString());
                },
                10_000,
                "IQv1 FK join: carol should re-route to age25");

            List<ConsumerRecord<GenericRecord, GenericRecord>> reRouteResults =
                consumeRecords(outputTopic, "dsl-fkjoin-consumer-reroute" + suffix, 4,
                    KafkaAvroDeserializer.class);
            assertEquals(4, reRouteResults.size(),
                "Output should have 4 records after re-route (3 initial + 1 carol update)");
            ConsumerRecord<GenericRecord, GenericRecord> carolReroute = reRouteResults.get(3);
            assertEquals("carol", carolReroute.key().get("word").toString());
            assertEquals("age25, age 25", carolReroute.value().get("line").toString());
            assertSchemaIdHeaders(carolReroute.headers(), outputTopic, "FK join carol re-route");

            // Phase 3 — FK target value update. age30 → "31". Only alice still points at
            // age30 (carol re-routed in phase 2), so only alice refreshes.
            produce(agesTopic, KeyValue.pair(createKey("age30"), createTextLine("31")));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = joinStore.get(createKey("alice"));
                    return v != null && v.value() != null
                        && "age30, age 31".equals(v.value().get("line").toString());
                },
                10_000,
                "IQv1 FK join: alice should refresh to age 31 after age30 value update");

            List<ConsumerRecord<GenericRecord, GenericRecord>> updateResults =
                consumeRecords(outputTopic, "dsl-fkjoin-consumer-fkupdate" + suffix, 5,
                    KafkaAvroDeserializer.class);
            assertEquals(5, updateResults.size(),
                "Output should have 5 records after FK value update (4 + 1 alice refresh)");
            ConsumerRecord<GenericRecord, GenericRecord> aliceRefresh = updateResults.get(4);
            assertEquals("alice", aliceRefresh.key().get("word").toString());
            assertEquals("age30, age 31", aliceRefresh.value().get("line").toString());
            assertSchemaIdHeaders(aliceRefresh.headers(), outputTopic, "FK join alice refresh");

            // Phase 4 — FK target tombstone. delete age30; alice (still FK→age30) gets
            // tombstoned. bob (FK→age25) and carol (re-routed FK→age25) are unaffected.
            produce(agesTopic, KeyValue.pair(createKey("age30"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = joinStore.get(createKey("alice"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1 FK join: alice should be tombstoned after age30 deletion");

            ValueTimestampHeaders<GenericRecord> bobAfterAge30Tombstone = joinStore.get(createKey("bob"));
            assertNotNull(bobAfterAge30Tombstone, "IQv1 FK join: bob should still exist");
            assertEquals("age25, age 25", bobAfterAge30Tombstone.value().get("line").toString());

            List<ConsumerRecord<GenericRecord, GenericRecord>> tombstoneResults =
                consumeRecords(outputTopic, "dsl-fkjoin-consumer-tombstone" + suffix, 6,
                    KafkaAvroDeserializer.class);
            assertEquals(6, tombstoneResults.size(),
                "Output should have 6 records after age30 tombstone");
            ConsumerRecord<GenericRecord, GenericRecord> aliceTombstone = tombstoneResults.get(5);
            assertEquals("alice", aliceTombstone.key().get("word").toString());
            assertNull(aliceTombstone.value(), "alice tombstone should have null value");
            assertKeySchemaIdHeader(aliceTombstone.headers(), outputTopic,
                "FK join alice tombstone");

            // Phase 5 — names-side tombstone. delete bob from names; bob is removed from
            // the join store regardless of what the ages side shows.
            produce(namesTopic, KeyValue.pair(createKey("bob"), null));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = joinStore.get(createKey("bob"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1 FK join: bob should be tombstoned after names-side deletion");

            List<ConsumerRecord<GenericRecord, GenericRecord>> namesDeleteResults =
                consumeRecords(outputTopic, "dsl-fkjoin-consumer-namesdelete" + suffix, 7,
                    KafkaAvroDeserializer.class);
            assertEquals(7, namesDeleteResults.size(),
                "Output should have 7 records after names-side bob deletion");
            ConsumerRecord<GenericRecord, GenericRecord> bobTombstone = namesDeleteResults.get(6);
            assertEquals("bob", bobTombstone.key().get("word").toString());
            assertNull(bobTombstone.value(), "bob tombstone should have null value");
            assertKeySchemaIdHeader(bobTombstone.headers(), outputTopic,
                "FK join bob tombstone");

            // Phase 6 — null joiner result. age25 → "0"; the joiner returns null, which
            // tombstones carol (bob is already gone via the names-side tombstone).
            produce(agesTopic, KeyValue.pair(createKey("age25"), createTextLine("0")));

            TestUtils.waitForCondition(
                () -> {
                    ValueTimestampHeaders<GenericRecord> v = joinStore.get(createKey("carol"));
                    return v == null || v.value() == null;
                },
                10_000,
                "IQv1 FK join: carol should be tombstoned after null joiner result");

            List<ConsumerRecord<GenericRecord, GenericRecord>> nullJoinResults =
                consumeRecords(outputTopic, "dsl-fkjoin-consumer-nulljoin" + suffix, 8,
                    KafkaAvroDeserializer.class);
            assertEquals(8, nullJoinResults.size(),
                "Output should have 8 records after null joiner result");
            ConsumerRecord<GenericRecord, GenericRecord> carolTombstone = nullJoinResults.get(7);
            assertEquals("carol", carolTombstone.key().get("word").toString());
            assertNull(carolTombstone.value(), "carol tombstone should have null value");
            assertKeySchemaIdHeader(carolTombstone.headers(), outputTopic,
                "FK join carol tombstone");

            // Changelog verification — join result store: 8 records (3 initial + 1 re-route
            // + 1 alice refresh + 3 tombstones for alice/bob/carol).
            List<ConsumerRecord<GenericRecord, GenericRecord>> joinChangelogRecords =
                consumeRecords(joinChangelog,
                    "dsl-fkjoin-result-changelog-consumer" + suffix, 8, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> joinLastByKey =
                lastRecordPerKey(joinChangelogRecords);
            assertEquals(3, joinLastByKey.size(),
                "join changelog should have 3 unique keys, got " + joinLastByKey.keySet());
            assertNull(joinLastByKey.get("alice").value(),
                "join changelog alice final should be tombstoned (FK age30 deleted)");
            assertNull(joinLastByKey.get("bob").value(),
                "join changelog bob final should be tombstoned (names-side delete)");
            assertNull(joinLastByKey.get("carol").value(),
                "join changelog carol final should be tombstoned (null joiner)");

            assertChangelogHeaders(joinChangelogRecords, joinChangelog,
                Set.of("alice", "bob", "carol"), "FK join changelog");

            if (!cachingEnabled) {
                assertEquals(8, joinChangelogRecords.size(),
                    "Join changelog (uncached) should have exactly 8 records");
            }

            // Changelog verification — names source store: 5 records
            // (alice/bob/carol initial + carol re-route + bob tombstone).
            List<ConsumerRecord<GenericRecord, GenericRecord>> namesChangelogRecords =
                consumeRecords(namesChangelog,
                    "dsl-fkjoin-names-changelog-consumer" + suffix, 5, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> namesLastByKey =
                lastRecordPerKey(namesChangelogRecords);
            assertEquals(3, namesLastByKey.size(),
                "names changelog should have 3 unique keys, got " + namesLastByKey.keySet());
            assertEquals("age30", namesLastByKey.get("alice").value().get("line").toString(),
                "names changelog alice final value");
            assertNull(namesLastByKey.get("bob").value(),
                "names changelog bob final should be tombstoned");
            assertEquals("age25", namesLastByKey.get("carol").value().get("line").toString(),
                "names changelog carol final value (re-routed)");

            assertChangelogHeaders(namesChangelogRecords, namesChangelog,
                Collections.singleton("bob"), "names changelog");

            if (!cachingEnabled) {
                assertEquals(5, namesChangelogRecords.size(),
                    "Names changelog (uncached) should have exactly 5 records");
            }

            // Changelog verification — ages source store: 5 records
            // (age30: 30, age25: 25, age30: 31, age30 tombstone, age25: 0).
            List<ConsumerRecord<GenericRecord, GenericRecord>> agesChangelogRecords =
                consumeRecords(agesChangelog,
                    "dsl-fkjoin-ages-changelog-consumer" + suffix, 5, KafkaAvroDeserializer.class);
            Map<String, ConsumerRecord<GenericRecord, GenericRecord>> agesLastByKey =
                lastRecordPerKey(agesChangelogRecords);
            assertEquals(2, agesLastByKey.size(),
                "ages changelog should have 2 unique keys, got " + agesLastByKey.keySet());
            assertNull(agesLastByKey.get("age30").value(),
                "ages changelog age30 final should be tombstoned");
            assertEquals("0", agesLastByKey.get("age25").value().get("line").toString(),
                "ages changelog age25 final value (set to 0 to trigger null joiner)");

            assertChangelogHeaders(agesChangelogRecords, agesChangelog,
                Collections.singleton("age30"), "ages changelog");

            if (!cachingEnabled) {
                assertEquals(5, agesChangelogRecords.size(),
                    "Ages changelog (uncached) should have exactly 5 records");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that {@code suppress()} works correctly with headers-aware stores.
     * suppress() buffers records internally and emits only after the time limit.
     */
    @Disabled("suppress() does not yet propagate headers correctly with headers-aware stores")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldSuppressWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String inputTopic = "dsl-suppress-input" + suffix;
        String outputTopic = "dsl-suppress-output" + suffix;
        String storeName = "dsl-suppress-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        String afterSuppressStoreName = "dsl-suppress-after-store" + suffix;
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde))
            .count(Materialized.<GenericRecord, Long>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(keySerde))
            .suppress(Suppressed.untilTimeLimit(
                Duration.ofMillis(100),
                Suppressed.BufferConfig.maxRecords(100).emitEarlyWhenFull()))
            .toStream()
            .toTable(Materialized.<GenericRecord, Long>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(afterSuppressStoreName))
                .withKeySerde(keySerde)
                .withValueSerde(Serdes.Long()))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, Serdes.Long()));

        String applicationId = "dsl-suppress-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);
        String afterSuppressChangelog = applicationId + "-" + afterSuppressStoreName + "-changelog";

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), applicationId, cachingEnabled);

            produce(inputTopic,
                KeyValue.pair(createKey("hello"), createTextLine("first")),
                KeyValue.pair(createKey("hello"), createTextLine("second")),
                KeyValue.pair(createKey("world"), createTextLine("first")));

            Thread.sleep(500);
            produce(inputTopic, KeyValue.pair(createKey("dummy"), createTextLine("advance-time")));

            // Suppress emits on stream-time advance (dummy record triggers emission of hello+world).
            // dummy itself may or may not emit depending on time advancement → 2-3 records in either caching mode.
            List<ConsumerRecord<GenericRecord, Long>> results =
                consumeRecords(outputTopic, "dsl-suppress-consumer" + suffix, 3, org.apache.kafka.common.serialization.LongDeserializer.class);

            assertTrue(results.size() >= 2 && results.size() <= 3,
                "Suppress output should have 2-3 records, got " + results.size());

            Map<String, Long> finalCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, Long> record : results) {
                finalCounts.put(record.key().get("word").toString(), record.value());
                assertKeySchemaIdHeader(record.headers(), outputTopic,
                    "suppress output " + record.key().get("word"));
                // Count output is <GenericRecord, Long> — Long doesn't use SR,
                // so __value_schema_id should NOT be present. If it is, suppress
                // is leaking input context headers instead of proper serialization.
                assertNull(record.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER),
                    "suppress output " + record.key().get("word")
                        + ": should NOT have __value_schema_id (value is Long, not Avro)");
            }
            assertEquals(2L, finalCounts.get("hello"), "hello should have count 2");
            assertEquals(1L, finalCounts.get("world"), "world should have count 1");

            // IQv1 verification on the count store (upstream of suppress)
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<Long>> store =
                headersStore(streams, storeName);
            assertNotNull(store, "Store should be accessible via IQv1");

            ValueTimestampHeaders<Long> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals(2L, helloResult.value(), "IQv1: hello count should be 2");
            assertKeySchemaIdHeader(helloResult.headers(), changelogTopic, "IQv1 get hello");

            // IQv1 verification on the store after suppress
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<Long>> afterSuppressStore =
                headersStore(streams, afterSuppressStoreName);
            assertNotNull(afterSuppressStore, "After-suppress store should be accessible via IQv1");

            ValueTimestampHeaders<Long> helloAfterSuppress = afterSuppressStore.get(createKey("hello"));
            assertNotNull(helloAfterSuppress, "IQv1: hello should exist in after-suppress store");
            assertEquals(2L, helloAfterSuppress.value(), "IQv1: hello count after suppress should be 2");
            assertKeySchemaIdHeader(helloAfterSuppress.headers(), afterSuppressChangelog, "IQv1 after-suppress get hello");

            ValueTimestampHeaders<Long> worldAfterSuppress = afterSuppressStore.get(createKey("world"));
            assertNotNull(worldAfterSuppress, "IQv1: world should exist in after-suppress store");
            assertEquals(1L, worldAfterSuppress.value(), "IQv1: world count after suppress should be 1");
            assertKeySchemaIdHeader(worldAfterSuppress.headers(), afterSuppressChangelog, "IQv1 after-suppress get world");

            // Changelog verification — count store has updates for hello (x2), world (x1), dummy (x1)
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeRecords(changelogTopic,
                    "dsl-suppress-changelog-consumer" + suffix, 4, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                String key = record.key().get("word").toString();
                if (record.value() != null) {
                    assertKeySchemaIdHeader(record.headers(), changelogTopic,
                        "changelog " + key);
                }
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies stream-stream left join and outer join work correctly
     * with headers-aware stores.
     */
    @Disabled("Stream-stream left/outer joins do not yet propagate headers correctly with headers-aware stores")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldStreamStreamJoinWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = suffixOf(cachingEnabled);
        String leftTopic = "dsl-streamjoin-left" + suffix;
        String rightTopic = "dsl-streamjoin-right" + suffix;
        String leftJoinOutputTopic = "dsl-streamjoin-left-output" + suffix;
        String outerJoinOutputTopic = "dsl-streamjoin-outer-output" + suffix;

        createTopics(leftTopic, rightTopic, leftJoinOutputTopic, outerJoinOutputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<GenericRecord, GenericRecord> leftStream =
            builder.stream(leftTopic, Consumed.with(keySerde, valueSerde));
        KStream<GenericRecord, GenericRecord> rightStream =
            builder.stream(rightTopic, Consumed.with(keySerde, valueSerde));

        // Left join
        leftStream.leftJoin(rightStream,
                (leftValue, rightValue) -> {
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    String rightStr = rightValue != null
                        ? rightValue.get("line").toString() : "null";
                    joined.put("line", leftValue.get("line") + "," + rightStr);
                    return joined;
                },
                JoinWindows.ofTimeDifferenceAndGrace(
                    Duration.ofSeconds(10), Duration.ofSeconds(10)),
                StreamJoined.with(keySerde, valueSerde, valueSerde))
            .to(leftJoinOutputTopic, Produced.with(keySerde, valueSerde));

        // Outer join
        leftStream.outerJoin(rightStream,
                (leftValue, rightValue) -> {
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    String leftStr = leftValue != null
                        ? leftValue.get("line").toString() : "null";
                    String rightStr = rightValue != null
                        ? rightValue.get("line").toString() : "null";
                    joined.put("line", leftStr + "," + rightStr);
                    return joined;
                },
                JoinWindows.ofTimeDifferenceAndGrace(
                    Duration.ofSeconds(10), Duration.ofSeconds(10)),
                StreamJoined.with(keySerde, valueSerde, valueSerde))
            .to(outerJoinOutputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-streamjoin-test" + suffix, cachingEnabled);

            // Send left records
            produce(leftTopic,
                KeyValue.pair(createKey("k1"), createTextLine("left1")),
                KeyValue.pair(createKey("k2"), createTextLine("left2")));

            // Send right records (k1 matches, k3 is right-only)
            produce(rightTopic,
                KeyValue.pair(createKey("k1"), createTextLine("right1")),
                KeyValue.pair(createKey("k3"), createTextLine("right3")));

            // Wait for join window processing
            Thread.sleep(3000);

            // Left join: k1 joined, k2 left-only (right=null) → exactly 2.
            List<ConsumerRecord<GenericRecord, GenericRecord>> leftJoinResults =
                consumeRecords(leftJoinOutputTopic,
                    "dsl-streamjoin-left-consumer" + suffix, 2, KafkaAvroDeserializer.class);

            assertEquals(2, leftJoinResults.size(),
                "Should have exactly 2 left join results, got " + leftJoinResults.size());

            Map<String, String> leftJoinValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : leftJoinResults) {
                leftJoinValues.put(
                    record.key().get("word").toString(),
                    record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), leftJoinOutputTopic,
                    "stream left join output " + record.key().get("word"));
            }
            assertEquals("left1,right1", leftJoinValues.get("k1"));
            assertEquals("left2,null", leftJoinValues.get("k2"));

            // Outer join: k1 joined, k2 left-only, k3 right-only → exactly 3.
            List<ConsumerRecord<GenericRecord, GenericRecord>> outerJoinResults =
                consumeRecords(outerJoinOutputTopic,
                    "dsl-streamjoin-outer-consumer" + suffix, 3, KafkaAvroDeserializer.class);

            assertEquals(3, outerJoinResults.size(),
                "Should have exactly 3 outer join results, got " + outerJoinResults.size());

            Map<String, String> outerJoinValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : outerJoinResults) {
                outerJoinValues.put(
                    record.key().get("word").toString(),
                    record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), outerJoinOutputTopic,
                    "stream outer join output " + record.key().get("word"));
            }
            assertEquals("left1,right1", outerJoinValues.get("k1"));
            assertEquals("left2,null", outerJoinValues.get("k2"));
            assertEquals("null,right3", outerJoinValues.get("k3"));

        } finally {
            closeStreams(streams);
        }
    }

    private static String changelogTopicFor(String applicationId, String storeName) {
        return applicationId + "-" + storeName + "-changelog";
    }
}
