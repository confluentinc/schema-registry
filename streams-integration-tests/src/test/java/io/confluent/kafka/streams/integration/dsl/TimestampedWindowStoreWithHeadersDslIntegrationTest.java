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

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
public class TimestampedWindowStoreWithHeadersDslIntegrationTest extends TimestampedWindowStoreDslTestBase {

    // Parameter provider for cache + grace combinations
    private static Stream<Arguments> cacheAndGraceParams() {
        return Stream.of(
            Arguments.of(true, true),    // cached + grace
            Arguments.of(true, false),   // cached + no grace
            Arguments.of(false, true),   // uncached + grace
            Arguments.of(false, false)   // uncached + no grace
        );
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldCountWithTumblingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-input" + suffix;
        String storeName = "window-store" + suffix;
        String applicationId = "window-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);

        TimeWindows windows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(windows)
            .count(Materialized.<GenericRecord, Long>as(
                    new WindowStoreSupplierWithHeaders(
                        Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                .withKeySerde(keySerde)
                .withValueSerde(Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 1000000L;
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("hello world from kafka")),
            at(baseTime + 1000, createKey("kafka"), createTextLine("processing streams in real time")));

        // 1. Verify IQv1 Fetch - initial count
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = windowStore(streams, storeName);

        long windowStart = (baseTime / windowSize.toMillis()) * windowSize.toMillis();
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store1 = store;
        TestUtils.waitForCondition(
            () -> {
                Long v = getWindowValue(store1, createKey("kafka"), windowStart, windowSize.toMillis());
                return v != null && v == 2L;
            },
            10_000, "kafka window count should reach 2");
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
            assertTrue(it.hasNext(), "Should find windowed result");
            KeyValue<Long, ValueTimestampHeaders<Long>> next = it.next();
            assertEquals(2L, next.value.value(), "Initial count should be 2");
            assertKeySchemaIdHeader(next.value.headers(), changelogTopic, "IQv1 Window Header");
        }

        // 2. Test null value handling - null inputs are counted as events
        produce(inputTopic,
            // Send null value for "kafka" key - counted as an event by count()
            at(baseTime + 3000, createKey("kafka"), null),
            // Send normal value for "streams" key
            at(baseTime + 4000, createKey("streams"), createTextLine("streams value")));

        // Re-fetch store
        store = windowStore(streams, storeName);

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store2 = store;
        TestUtils.waitForCondition(
            () -> {
                Long v = getWindowValue(store2, createKey("kafka"), windowStart, windowSize.toMillis());
                return v != null && v == 3L;
            },
            10_000, "kafka window count should reach 3 after null event");

        // Verify "kafka" count is 3
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
            assertTrue(it.hasNext(), "kafka window should still exist after null value");
            KeyValue<Long, ValueTimestampHeaders<Long>> next = it.next();
            assertEquals(3L, next.value.value(), "kafka count should be 3 (null value counted as event)");
            assertNotNull(next.value.value(), "kafka count should not be null");
            assertKeySchemaIdHeader(next.value.headers(), changelogTopic, "IQv1 after null input");
        }

        // Verify "streams" has count of 1
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
            assertTrue(it.hasNext(), "streams window should exist");
            assertEquals(1L, it.next().value.value(), "streams count should be 1");
        }

        // 3. Test grace period if enabled
        if (graceEnabled) {
            // Advance stream time past window end but within grace
            // Window: [1000000, 1010000], ends at 1010000, grace until 1015000
            // Then send late record (timestamp in original window, but stream time already advanced)
            produce(inputTopic,
                at(baseTime + 12000, createKey("other"), createTextLine("time advance message")),
                at(baseTime + 5000, createKey("kafka"), createTextLine("within grace period")));

            store = windowStore(streams, storeName);

            ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store3 = store;
            TestUtils.waitForCondition(
                () -> {
                    Long v = getWindowValue(store3, createKey("kafka"), windowStart, windowSize.toMillis());
                    return v != null && v == 4L;
                },
                10_000, "kafka window count should reach 4 after late record within grace");

            // Verify late record was counted (count should be 4: 3 original + 1 late)
            try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
                assertTrue(it.hasNext(), "Should still find windowed result");
                KeyValue<Long, ValueTimestampHeaders<Long>> next = it.next();
                assertEquals(4L, next.value.value(), "Count should be 4 after late arrival within grace");
                assertKeySchemaIdHeader(next.value.headers(), changelogTopic, "IQv1 after grace period");
            }

            // Send too-late record beyond grace period
            // Advance stream time beyond window + grace (past 1015000), then send too-late
            // record (stream time is past window + grace)
            produce(inputTopic,
                at(baseTime + 20000, createKey("other2"), createTextLine("expire grace now")),
                at(baseTime + 8000, createKey("kafka"), createTextLine("too late rejected")));

            // Re-fetch store
            store = windowStore(streams, storeName);

            ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store4 = store;
            // Wait for the time-advance record ("other2") to land — too-late record stays dropped, kafka stays at 4.
            long other2WindowStart = ((baseTime + 20000) / windowSize.toMillis()) * windowSize.toMillis();
            TestUtils.waitForCondition(
                () -> {
                    Long v = getWindowValue(store4, createKey("other2"), other2WindowStart, windowSize.toMillis());
                    return v != null && v >= 1L;
                },
                10_000, "other2 should be processed (signals too-late record was also processed and dropped)");

            // Verify too-late record was dropped (count still 4)
            try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
                assertTrue(it.hasNext(), "Should still find windowed result");
                KeyValue<Long, ValueTimestampHeaders<Long>> next = it.next();
                assertEquals(4L, next.value.value(), "Count should still be 4, too-late record dropped");
            }
        }
        closeStreams(streams);

        // 3. Verify Changelog Headers
        int expectedRecords = cachingEnabled ? (graceEnabled ? 4 : 2) : (graceEnabled ? 7 : 4);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-cg-" + testId, expectedRecords, ByteArrayDeserializer.class, ByteArrayDeserializer.class);

        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Changelog header for windowed key");
            }
        }
    }


    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldCountWithHoppingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-hop-input" + suffix;
        String storeName = "window-hop-store" + suffix;
        String applicationId = "window-hop-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration advanceBy = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(5);

        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(advanceBy)
            : TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceBy);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(timeWindows)
            .count(Materialized.<GenericRecord, Long>as(
                    new WindowStoreSupplierWithHeaders(
                        Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                .withKeySerde(keySerde)
                .withValueSerde(Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 3000000L;
        // Send records at specific times to create overlapping windows for "kafka"
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("quick brown fox jumps")),
            at(baseTime + 3000, createKey("kafka"), createTextLine("windowed by time")),
            at(baseTime + 7000, createKey("kafka"), createTextLine("late events within grace")),
            at(baseTime + 2000, createKey("streams"), createTextLine("quick brown fox jumps")),
            at(baseTime + 6000, createKey("streams"), createTextLine("windowed by time")));

        // Verify multiple overlapping windows exist for "kafka"
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = windowStore(streams, storeName);

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> hopStore0 = store;
        TestUtils.waitForCondition(
            () -> {
                Long v = getWindowValue(hopStore0, createKey("streams"), 0L, Long.MAX_VALUE);
                return v != null && v >= 1L;
            },
            10_000, "streams hopping windows should be populated");

        List<KeyValue<Long, ValueTimestampHeaders<Long>>> windows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<Long>> window = it.next();
                windows.add(window);
                assertKeySchemaIdHeader(window.value.headers(), changelogTopic, "IQv1 hopping window " + window.key);
            }
        }

        // With hopping windows (10s size, 5s advance), we should have 3 overlapping windows
        assertTrue(windows.size() >= 3, "Should have at least 3 overlapping windows for kafka, got " + windows.size());

        // Verify each window has correct count based on which records fall within it
        for (KeyValue<Long, ValueTimestampHeaders<Long>> window : windows) {
            long count = window.value.value();
            assertTrue(count >= 1 && count <= 3, "kafka window count should be between 1 and 3, got " + count);
        }

        // Verify "streams" key also has overlapping windows
        List<KeyValue<Long, ValueTimestampHeaders<Long>>> streamsWindows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<Long>> window = it.next();
                streamsWindows.add(window);
                assertKeySchemaIdHeader(window.value.headers(), changelogTopic, "IQv1 hopping window for streams " + window.key);
            }
        }

        assertTrue(streamsWindows.size() >= 2, "Should have at least 2 overlapping windows for streams, got " + streamsWindows.size());

        // Verify each streams window has correct count
        for (KeyValue<Long, ValueTimestampHeaders<Long>> window : streamsWindows) {
            long count = window.value.value();
            assertTrue(count >= 1 && count <= 2, "streams window count should be between 1 and 2, got " + count);
        }

        // Test grace period if enabled
        if (graceEnabled) {
            // Advance stream time past window end but within grace, then send late record
            // for "other-late" key with old timestamp (within grace)
            produce(inputTopic,
                at(baseTime + 12000, createKey("other"), createTextLine("advance stream time")),
                at(baseTime + 4000, createKey("other-late"), createTextLine("late arrival event")));

            store = windowStore(streams, storeName);

            ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> hopStore = store;
            TestUtils.waitForCondition(
                () -> {
                    Long v = getWindowValue(hopStore, createKey("other-late"), 0L, Long.MAX_VALUE);
                    return v != null && v >= 1L;
                },
                10_000, "other-late hopping window should be populated");

            // Verify "other" key
            List<KeyValue<Long, ValueTimestampHeaders<Long>>> otherWindows = new ArrayList<>();
            try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("other"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                while (it.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<Long>> window = it.next();
                    otherWindows.add(window);
                    assertKeySchemaIdHeader(window.value.headers(), changelogTopic, "IQv1 hopping window for other " + window.key);
                }
            }
            assertEquals(2, otherWindows.size(), "Should have 2 windows for other key");
            for (KeyValue<Long, ValueTimestampHeaders<Long>> window : otherWindows) {
                assertEquals(1L, window.value.value(), "other window count should be 1");
            }

            // Verify "other-late" key (late-arriving record within grace)
            List<KeyValue<Long, ValueTimestampHeaders<Long>>> otherLateWindows = new ArrayList<>();
            try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("other-late"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                while (it.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<Long>> window = it.next();
                    otherLateWindows.add(window);
                    assertKeySchemaIdHeader(window.value.headers(), changelogTopic, "IQv1 hopping window for other-late " + window.key);
                }
            }
            assertEquals(1, otherLateWindows.size(), "Should have 1 window for other-late (earlier window closed), got " + otherLateWindows.size());
            for (KeyValue<Long, ValueTimestampHeaders<Long>> window : otherLateWindows) {
                assertEquals(1L, window.value.value(), "other-late window count should be 1");
            }
        }

        closeStreams(streams);

        // Verify Changelog Headers
        int expectedRecords = cachingEnabled ? (graceEnabled ? 9 : 5) : (graceEnabled ? 13 : 9);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-hop-cg-" + testId, expectedRecords, ByteArrayDeserializer.class, ByteArrayDeserializer.class);

        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Changelog header for hopping window key");
            }
        }
    }

    /**
     * Verifies {@code count()} on sliding windows with a headers-aware window store.
     * Sliding windows produce one new window per record advancing by 1ms; multiple
     * overlapping windows naturally form for nearby records.
     */
    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldCountWithSlidingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-sliding-input" + suffix;
        String storeName = "window-sliding-store" + suffix;
        String applicationId = "window-sliding-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration grace = Duration.ofSeconds(5);
        SlidingWindows slidingWindows = graceEnabled
            ? SlidingWindows.ofTimeDifferenceAndGrace(windowSize, grace)
            : SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(slidingWindows)
            .count(Materialized.<GenericRecord, Long>as(
                    new WindowStoreSupplierWithHeaders(
                        Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                .withKeySerde(keySerde)
                .withValueSerde(Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 15000000L;
        // Advance stream time so emit triggers; SlidingWindows requires this for in-window updates to surface.
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("first")),
            at(baseTime + 2000, createKey("kafka"), createTextLine("second")),
            at(baseTime + 4000, createKey("kafka"), createTextLine("third")),
            at(baseTime + 30000, createKey("advance"), createTextLine("advance time")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<Long>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    while (it.hasNext()) { it.next(); n++; }
                    return n >= 2;
                }
            },
            10_000, "kafka should appear in at least 2 sliding windows");

        List<KeyValue<Long, ValueTimestampHeaders<Long>>> windows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it =
                 store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<Long>> w = it.next();
                windows.add(w);
                assertKeySchemaIdHeader(w.value.headers(), changelogTopic, "IQv1 sliding kafka " + w.key);
                long count = w.value.value();
                assertTrue(count >= 1 && count <= 3, "kafka sliding window count should be 1-3, got " + count);
            }
        }
        assertTrue(windows.size() >= 2, "kafka should be in at least 2 sliding windows, got " + windows.size());

        closeStreams(streams);

        int expectedRecords = cachingEnabled ? 6 : 7;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-sliding-cg-" + testId, expectedRecords,
            ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Sliding window changelog");
            } else {
                String keyAsString = new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(keyAsString.contains("kafka"), "Tombstone key bytes should contain 'kafka', got: " + keyAsString);
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Sliding window changelog");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldAggregateWithTumblingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-agg-input" + suffix;
        String storeName = "window-agg-store" + suffix;
        String applicationId = "window-agg-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);

        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(timeWindows)
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                (key, value, agg) -> {
                    // Null aggregation: returning null tombstones the window
                    if ("DELETE".equals(value.get("line").toString())) {
                        return null;
                    }
                    GenericRecord updated = new GenericData.Record(aggSchema);
                    updated.put("word", key.get("word").toString());
                    updated.put("count", (long) agg.get("count") + 1L);
                    return updated;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 2000000L;
        // Send 3 records for "kafka" and 2 for "streams" in the same window
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("hello world from kafka")),
            at(baseTime + 1000, createKey("kafka"), createTextLine("processing streams in real time")),
            at(baseTime + 2000, createKey("kafka"), createTextLine("headers are preserved")),
            at(baseTime + 3000, createKey("streams"), createTextLine("hello world from kafka")),
            at(baseTime + 4000, createKey("streams"), createTextLine("processing streams in real time")));

        // Verify IQv1
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = windowStore(streams, storeName);

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> aggStore = store;
        TestUtils.waitForCondition(
            () -> {
                GenericRecord v = getWindowValue(aggStore, createKey("kafka"), 0L, Long.MAX_VALUE);
                return v != null && (long) v.get("count") == 3L;
            },
            10_000, "kafka aggregate count should reach 3");

        // Verify IQv1 Fetch for "kafka"
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed aggregate for kafka");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertEquals(3L, next.value.value().get("count"), "kafka count should be 3");
            assertEquals("kafka", next.value.value().get("word").toString());
            assertSchemaIdHeaders(next.value.headers(), changelogTopic, "IQv1 kafka aggregate");
        }

        // Verify IQv1 Fetch for "streams"
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed aggregate for streams");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertEquals(2L, next.value.value().get("count"), "streams count should be 2");
            assertEquals("streams", next.value.value().get("word").toString());
            assertSchemaIdHeaders(next.value.headers(), changelogTopic, "IQv1 streams aggregate");
        }

        // Null aggregation: send DELETE value to tombstone the window
        produce(inputTopic,
            at(baseTime + 10000, createKey("hello"), createTextLine("hello world from kafka")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> aggStore2 = store;
        TestUtils.waitForCondition(
            () -> {
                GenericRecord v = getWindowValue(aggStore2, createKey("hello"), 0L, Long.MAX_VALUE);
                return v != null && (long) v.get("count") == 1L;
            },
            10_000, "hello aggregate count should reach 1");

        // Verify "hello" exists
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("hello"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "hello should exist before DELETE");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertEquals(1L, next.value.value().get("count"));
            assertSchemaIdHeaders(next.value.headers(), changelogTopic, "IQv1 hello aggregate");
        }

        // Send DELETE to tombstone "hello" window
        produce(inputTopic, at(baseTime + 11000, createKey("hello"), createTextLine("DELETE")));

        // Re-fetch store reference in case of rebalance
        store = windowStore(streams, storeName);

        // Verify "hello" is tombstoned
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> finalStore = store;
        TestUtils.waitForCondition(
            () -> getWindowValue(finalStore, createKey("hello"), 0L, Long.MAX_VALUE) == null,
            10_000,
            "IQv1 store: hello should be tombstoned");

        // Verify other keys still exist.
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "kafka should still exist after hello DELETE");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> kafkaResult = it.next();
            assertEquals(3L, kafkaResult.value.value().get("count"));
            assertSchemaIdHeaders(kafkaResult.value.headers(), changelogTopic, "IQv1 kafka aggregate");
        }

        closeStreams(streams);

        // Verify changelog headers
        int expectedRecords = cachingEnabled ? 2 : 7;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords =
            consumeRecords(changelogTopic, "agg-changelog-cg-" + testId, expectedRecords, ByteArrayDeserializer.class, ByteArrayDeserializer.class);

        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        boolean sawTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), changelogTopic, "Aggregate changelog");
            } else {
                sawTombstone = true;
                String keyAsString = new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(keyAsString.contains("hello"), "Tombstone key bytes should contain 'hello', got: " + keyAsString);
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Aggregate changelog");
            }
        }

        if (!cachingEnabled) {
            assertTrue(sawTombstone, "uncached changelog should contain a tombstone for hello");
        }
    }

    /**
     * Verifies {@code aggregate()} on hopping windows with a headers-aware window store.
     * A single input record contributes to multiple overlapping windows.
     */
    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldAggregateWithHoppingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-hop-agg-input" + suffix;
        String storeName = "window-hop-agg-store" + suffix;
        String applicationId = "window-hop-agg-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration advance = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(5);
        TimeWindows hoppingWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(advance)
            : TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advance);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(hoppingWindows)
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                (key, value, agg) -> {
                    // Null aggregation: returning null tombstones the window (consistent with tumbling agg test).
                    if ("DELETE".equals(value.get("line").toString())) {
                        return null;
                    }
                    GenericRecord updated = new GenericData.Record(aggSchema);
                    updated.put("word", key.get("word").toString());
                    updated.put("count", (long) agg.get("count") + 1L);
                    return updated;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 11000000L;
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("first")),
            at(baseTime + 3000, createKey("kafka"), createTextLine("second")),
            at(baseTime + 7000, createKey("kafka"), createTextLine("third")),
            at(baseTime + 1000, createKey("streams"), createTextLine("first")),
            at(baseTime + 4000, createKey("streams"), createTextLine("second")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    while (it.hasNext()) { it.next(); n++; }
                    return n >= 2;
                }
            },
            10_000, "kafka should appear in at least 2 hopping windows");

        List<KeyValue<Long, ValueTimestampHeaders<GenericRecord>>> windows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                windows.add(w);
                assertSchemaIdHeaders(w.value.headers(), changelogTopic, "IQv1 hopping agg " + w.key);
                long count = (long) w.value.value().get("count");
                assertTrue(count >= 1 && count <= 3, "kafka hopping window count should be 1-3, got " + count);
            }
        }
        assertTrue(windows.size() >= 2, "kafka should be in at least 2 overlapping windows, got " + windows.size());

        int kafkaWindowsBefore = windows.size();

        // Tombstone kafka via DELETE.
        produce(inputTopic, at(baseTime + 8000, createKey("kafka"), createTextLine("DELETE")));

        // Wait for kafka windows to be tombstoned.
        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    boolean sawNull = false;
                    while (it.hasNext()) {
                        KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                        if (w.value == null || w.value.value() == null) sawNull = true;
                        n++;
                    }
                    return sawNull || n < kafkaWindowsBefore;
                }
            },
            10_000, "kafka hopping windows should be tombstoned or removed after DELETE");

        // Check other keys still exist.
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                 store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "streams should still exist after kafka DELETE");
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                assertNotNull(w.value, "streams window value should not be null after kafka DELETE");
                assertNotNull(w.value.value(), "streams window inner value should not be null");
                long c = (long) w.value.value().get("count");
                assertTrue(c >= 1 && c <= 2, "streams hopping window count should be 1-2 after kafka DELETE, got " + c);
                assertSchemaIdHeaders(w.value.headers(), changelogTopic, "IQv1 hopping agg streams post-tombstone " + w.key);
            }
        }

        closeStreams(streams);

        int expectedRecords = cachingEnabled ? (graceEnabled ? 3 : 2) : (graceEnabled ? 12 : 10);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-hop-agg-cg-" + testId, expectedRecords,
            ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        boolean sawTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), changelogTopic, "Hopping aggregate changelog");
            } else {
                sawTombstone = true;
                String keyAsString = new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(keyAsString.contains("kafka"), "Tombstone key bytes should contain 'kafka', got: " + keyAsString);
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Hopping aggregate changelog tombstone");
            }
        }
        // Uncached: tombstone for kafka window(s) reaches changelog
        if (!cachingEnabled) {
            assertTrue(sawTombstone, "uncached changelog should contain a tombstone for kafka");
        }
    }



    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldReduceWithTumblingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-reduce-input" + suffix;
        String storeName = "window-reduce-store" + suffix;
        String applicationId = "window-reduce-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);

        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(timeWindows)
            .reduce(
                (v1, v2) -> {
                    // Null aggregation: returning null tombstones the window.
                    if ("DELETE".equals(v2.get("line").toString())) {
                        return null;
                    }
                    // Concatenate "line" fields from both values
                    GenericRecord combined = new GenericData.Record(valueSchema);
                    String line1 = v1.get("line").toString();
                    String line2 = v2.get("line").toString();
                    combined.put("line", line1 + "," + line2);
                    return combined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 4000000L;
        // Send 3 records for "kafka" and 2 for "streams" in the same window
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("hello world from kafka")),
            at(baseTime + 1000, createKey("kafka"), createTextLine("processing streams in real time")),
            at(baseTime + 2000, createKey("kafka"), createTextLine("headers are preserved")),
            at(baseTime + 3000, createKey("streams"), createTextLine("reduce first value")),
            at(baseTime + 4000, createKey("streams"), createTextLine("reduce second value")));

        // Verify IQv1 Fetch result
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                GenericRecord v = getWindowValue(store, createKey("streams"), 0L, Long.MAX_VALUE);
                return v != null && v.get("line").toString().contains("reduce second value");
            },
            10_000, "reduce should have processed both streams records");

        // Verify IQv1 Fetch result for "kafka"
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed reduce result for kafka");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            String reducedLine = next.value.value().get("line").toString();
            assertTrue(reducedLine.contains("hello world from kafka") && reducedLine.contains("processing streams in real time") && reducedLine.contains("headers are preserved"),
                "kafka reduced line should contain all three values: " + reducedLine);
            assertSchemaIdHeaders(next.value.headers(), changelogTopic, "IQv1 kafka reduce");
        }

        // Verify IQv1 Fetch for "streams"
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed reduce result for streams");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            String reducedLine = next.value.value().get("line").toString();
            assertTrue(reducedLine.contains("reduce first value") && reducedLine.contains("reduce second value"),
                "streams reduced line should contain both values: " + reducedLine);
            assertSchemaIdHeaders(next.value.headers(), changelogTopic, "IQv1 streams reduce");
        }

        // Tombstone kafka via DELETE.
        produce(inputTopic, at(baseTime + 5000, createKey("kafka"), createTextLine("DELETE")));

        // Wait for kafka window to be tombstoned
        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    if (!it.hasNext()) return true;
                    KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                    return w.value == null || w.value.value() == null;
                }
            },
            10_000, "kafka window should be tombstoned or removed after DELETE");

        // other keys remain.
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "streams should still exist after kafka DELETE");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertNotNull(next.value, "streams window value should not be null");
            assertNotNull(next.value.value(), "streams window inner value should not be null");
            String reducedLine = next.value.value().get("line").toString();
            assertTrue(reducedLine.contains("reduce first value") && reducedLine.contains("reduce second value"),
                "streams reduced line should still contain both values after kafka DELETE: " + reducedLine);
            assertSchemaIdHeaders(next.value.headers(), changelogTopic, "IQv1 streams reduce post-tombstone");
        }

        closeStreams(streams);

        int expectedRecords = cachingEnabled ? 1 : 6;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-reduce-cg-" + testId, expectedRecords, ByteArrayDeserializer.class, ByteArrayDeserializer.class);

        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        boolean sawTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), changelogTopic, "Changelog header for windowed reduce");
            } else {
                sawTombstone = true;
                String keyAsString = new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(keyAsString.contains("kafka"), "Tombstone key bytes should contain 'kafka', got: " + keyAsString);
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Reduce changelog tombstone");
            }
        }
        if (!cachingEnabled) {
            assertTrue(sawTombstone, "uncached changelog should contain a tombstone for kafka");
        }
    }

    /**
     * Verifies {@code reduce()} on hopping windows with a headers-aware window store.
     */
    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldReduceWithHoppingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-hop-reduce-input" + suffix;
        String storeName = "window-hop-reduce-store" + suffix;
        String applicationId = "window-hop-reduce-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration advance = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(5);
        TimeWindows hoppingWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(advance)
            : TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advance);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(hoppingWindows)
            .reduce(
                (oldVal, newVal) -> {
                    // Null aggregation: returning null tombstones the window.
                    if ("DELETE".equals(newVal.get("line").toString())) {
                        return null;
                    }
                    GenericRecord combined = new GenericData.Record(valueSchema);
                    combined.put("line", oldVal.get("line") + "|" + newVal.get("line"));
                    return combined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 12000000L;
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("a")),
            at(baseTime + 3000, createKey("kafka"), createTextLine("b")),
            at(baseTime + 7000, createKey("kafka"), createTextLine("c")),
            at(baseTime + 1000, createKey("streams"), createTextLine("x")),
            at(baseTime + 4000, createKey("streams"), createTextLine("y")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    while (it.hasNext()) { it.next(); n++; }
                    return n >= 2;
                }
            },
            10_000, "kafka should appear in at least 2 hopping windows after reduce");

        List<KeyValue<Long, ValueTimestampHeaders<GenericRecord>>> windows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                windows.add(w);
                assertSchemaIdHeaders(w.value.headers(), changelogTopic, "IQv1 hopping reduce " + w.key);
                String reduced = w.value.value().get("line").toString();
                assertTrue(reduced.length() >= 1, "kafka hopping reduce line should be non-empty, got: " + reduced);
            }
        }
        assertTrue(windows.size() >= 2, "kafka should be in at least 2 overlapping windows, got " + windows.size());

        int kafkaWindowsBefore = windows.size();

        // Tombstone kafka via DELETE.
        produce(inputTopic, at(baseTime + 8000, createKey("kafka"), createTextLine("DELETE")));

        // Wait for kafka windows to be tombstoned or removed.
        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    boolean sawNull = false;
                    while (it.hasNext()) {
                        KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                        if (w.value == null || w.value.value() == null) sawNull = true;
                        n++;
                    }
                    return sawNull || n < kafkaWindowsBefore;
                }
            },
            10_000, "kafka hopping windows should be tombstoned or removed after DELETE");

        // other keys remain.
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                 store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "streams should still exist after kafka DELETE");
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                assertNotNull(w.value, "streams window value should not be null");
                assertNotNull(w.value.value(), "streams window inner value should not be null");
                String reduced = w.value.value().get("line").toString();
                assertTrue(reduced.length() >= 1, "streams hopping reduce line should be non-empty");
                assertSchemaIdHeaders(w.value.headers(), changelogTopic, "IQv1 hopping reduce streams post-tombstone " + w.key);
            }
        }

        closeStreams(streams);

        int expectedRecords = cachingEnabled ? (graceEnabled ? 3 : 2) : (graceEnabled ? 12 : 10);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-hop-reduce-cg-" + testId, expectedRecords,
            ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        boolean sawTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), changelogTopic, "Hopping reduce changelog");
            } else {
                sawTombstone = true;
                String keyAsString = new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(keyAsString.contains("kafka"), "Tombstone key bytes should contain 'kafka', got: " + keyAsString);
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Hopping reduce changelog tombstone");
            }
        }
        if (!cachingEnabled) {
            assertTrue(sawTombstone, "uncached changelog should contain a tombstone for kafka");
        }
    }

    /**
     * Verifies that {@code cogroup()} with a windowed aggregate works with a
     * headers-aware window store. Two grouped streams from separate topics are
     * merged into one windowed aggregation.
     */
    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldCogroupWithTumblingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic1 = "window-cogroup-input1" + suffix;
        String inputTopic2 = "window-cogroup-input2" + suffix;
        String storeName = "window-cogroup-store" + suffix;
        String applicationId = "window-cogroup-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic1, inputTopic2);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);
        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        Aggregator<GenericRecord, GenericRecord, GenericRecord> aggregator = (key, value, agg) -> {
            if ("DELETE".equals(value.get("line").toString())) {
                return null;
            }
            GenericRecord updated = new GenericData.Record(aggSchema);
            updated.put("word", key.get("word").toString());
            updated.put("count", (long) agg.get("count") + 1L);
            return updated;
        };

        KGroupedStream<GenericRecord, GenericRecord> grouped1 = builder
            .stream(inputTopic1, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde));
        KGroupedStream<GenericRecord, GenericRecord> grouped2 = builder
            .stream(inputTopic2, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde));

        grouped1.cogroup(aggregator)
            .cogroup(grouped2, aggregator)
            .windowedBy(timeWindows)
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 7000000L;
        produce(inputTopic1,
            at(baseTime, createKey("kafka"), createTextLine("first")),
            at(baseTime + 1000, createKey("kafka"), createTextLine("second")),
            at(baseTime + 3000, createKey("streams"), createTextLine("first")));
        produce(inputTopic2,
            at(baseTime + 2000, createKey("kafka"), createTextLine("third")),
            at(baseTime + 4000, createKey("streams"), createTextLine("second")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                GenericRecord v = getWindowValue(store, createKey("kafka"), 0L, Long.MAX_VALUE);
                return v != null && (long) v.get("count") == 3L;
            },
            10_000, "kafka cogroup window count should reach 3");

        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed cogroup result for kafka");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertEquals(3L, next.value.value().get("count"), "kafka count should be 3 (cogrouped from both topics)");
            assertSchemaIdHeaders(next.value.headers(), changelogTopic, "IQv1 cogroup kafka");
        }

        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed cogroup result for streams");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertEquals(2L, next.value.value().get("count"), "streams count should be 2");
            assertSchemaIdHeaders(next.value.headers(), changelogTopic, "IQv1 cogroup streams");
        }

        // Tombstone kafka via DELETE.
        produce(inputTopic1, at(baseTime + 5000, createKey("kafka"), createTextLine("DELETE")));

        TestUtils.waitForCondition(
            () -> getWindowValue(store, createKey("kafka"), 0L, Long.MAX_VALUE) == null,
            10_000, "IQv1 store: kafka should be tombstoned after DELETE");

        // other keys remain.
        GenericRecord streamsAfter = getWindowValue(store, createKey("streams"), 0L, Long.MAX_VALUE);
        assertNotNull(streamsAfter, "streams should still exist after kafka DELETE");
        assertEquals(2L, streamsAfter.get("count"), "streams count should still be 2 after kafka DELETE");

        closeStreams(streams);

        // Changelog verification
        int expectedRecords = cachingEnabled ? 1 : 6;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-cogroup-cg-" + testId, expectedRecords, ByteArrayDeserializer.class, ByteArrayDeserializer.class);

        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        boolean sawTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), changelogTopic, "Cogroup changelog");
            } else {
                sawTombstone = true;
                String keyAsString = new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(keyAsString.contains("kafka"), "Tombstone key bytes should contain 'kafka', got: " + keyAsString);
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Cogroup changelog tombstone");
            }
        }
        // uncached: tombstone always reaches output; cached coalesces put-then-delete.
        if (!cachingEnabled) {
            assertTrue(sawTombstone, "changelog should contain a tombstone for kafka");
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldStreamStreamsJoinWithHeaders(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String leftTopic = "join-left-input" + suffix;
        String rightTopic = "join-right-input" + suffix;
        String outputTopic = "join-output" + suffix;

        createTopics(leftTopic, rightTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration joinWindow = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(2);
        Duration storeWindowSize = joinWindow.multipliedBy(2);
        Duration retention = storeWindowSize.plus(graceEnabled ? gracePeriod : Duration.ZERO);

        // Choose join window type based on graceEnabled parameter
        JoinWindows joinWindows = graceEnabled
            ? JoinWindows.ofTimeDifferenceAndGrace(joinWindow, gracePeriod)
            : JoinWindows.ofTimeDifferenceWithNoGrace(joinWindow);

        KStream<GenericRecord, GenericRecord> leftStream = builder.stream(leftTopic, Consumed.with(keySerde, valueSerde));
        KStream<GenericRecord, GenericRecord> rightStream = builder.stream(rightTopic, Consumed.with(keySerde, valueSerde));

        String joinStoreName = "my-join" + suffix;
        String leftJoinStore = joinStoreName + "-left";
        String rightJoinStore = joinStoreName + "-right";

        // Create custom window store suppliers for join stores with headers
        WindowBytesStoreSupplier leftStoreSupplier =
            new WindowStoreSupplierWithHeaders(
                Stores.persistentTimestampedWindowStoreWithHeaders(
                    leftJoinStore,
                    retention,
                    storeWindowSize,
                    true));

        WindowBytesStoreSupplier rightStoreSupplier =
            new WindowStoreSupplierWithHeaders(
                Stores.persistentTimestampedWindowStoreWithHeaders(
                    rightJoinStore,
                    retention,
                    storeWindowSize,
                    true));

        // Inner join
        leftStream.join(
            rightStream,
            (leftValue, rightValue) -> {
                // Create aggregate record combining both sides
                GenericRecord joined = new GenericData.Record(aggSchema);
                joined.put("word", leftValue.get("line").toString() + "-" + rightValue.get("line").toString());
                joined.put("count", 1L);
                return joined;
            },
            joinWindows,
            StreamJoined.with(keySerde, valueSerde, valueSerde)
                .withThisStoreSupplier(leftStoreSupplier)
                .withOtherStoreSupplier(rightStoreSupplier)
        ).to(outputTopic, Produced.with(keySerde, aggSerde));

        String applicationId = "stream-join-test" + suffix;
        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 5000000L;
        produce(leftTopic,
            at(baseTime, createKey("kafka"), createTextLine("left stream value")),
            at(baseTime + 3000, createKey("streams"), createTextLine("another left value")),
            // Non-matching record (outside join window)
            at(baseTime + 20000, createKey("other"), createTextLine("other record no match")));
        produce(rightTopic,
            at(baseTime + 1000, createKey("kafka"), createTextLine("right stream value")),
            at(baseTime + 4000, createKey("streams"), createTextLine("another right value")));

        // Verify IQv1 queries on join stores before consuming output topic
        Properties consumerProps = createConsumerProps("join-output-cg-" + testId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final int expectedOutputRecordCount = 2;
        final Instant pollDeadline = Instant.now().plus(Duration.ofSeconds(10));
        List<ConsumerRecord<GenericRecord, GenericRecord>> outputRecords = new ArrayList<>();
        List<ConsumerRecord<GenericRecord, GenericRecord>> afterNull = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(consumerProps, keySerde.deserializer(), aggSerde.deserializer())) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            while (outputRecords.size() < expectedOutputRecordCount && Instant.now().isBefore(pollDeadline)) {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(outputRecords::add);
            }

            // Stream-stream join skips null-value inputs on either side
            produce(leftTopic, at(baseTime + 50000, createKey("kafka"), null));
            produce(rightTopic, at(baseTime + 50000, createKey("kafka"), null));
            long afterNullDeadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < afterNullDeadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(afterNull::add);
            }
        }

        assertEquals(expectedOutputRecordCount, outputRecords.size(), "Should have 2 join results (kafka and streams)");

        // Collect results by key for verification
        Map<String, ConsumerRecord<GenericRecord, GenericRecord>> resultsByKey = new HashMap<>();
        for (ConsumerRecord<GenericRecord, GenericRecord> record : outputRecords) {
            String keyWord = record.key().get("word").toString();
            resultsByKey.put(keyWord, record);
        }

        // Verify "kafka" join result
        assertTrue(resultsByKey.containsKey("kafka"), "Should have join result for kafka key");
        ConsumerRecord<GenericRecord, GenericRecord> kafkaResult = resultsByKey.get("kafka");
        assertEquals("kafka", kafkaResult.key().get("word").toString(), "Key should be kafka");
        assertEquals("left stream value-right stream value", kafkaResult.value().get("word").toString(),
            "Joined value should be 'left stream value-right stream value'");
        assertEquals(1L, kafkaResult.value().get("count"), "Count should be 1");
        assertSchemaIdHeaders(kafkaResult.headers(), outputTopic, "Join output for kafka");

        // Verify "streams" join result
        assertTrue(resultsByKey.containsKey("streams"), "Should have join result for streams key");
        ConsumerRecord<GenericRecord, GenericRecord> streamsResult = resultsByKey.get("streams");
        assertEquals("streams", streamsResult.key().get("word").toString(), "Key should be streams");
        assertEquals("another left value-another right value", streamsResult.value().get("word").toString(),
            "Joined value should be 'another left value-another right value'");
        assertEquals(1L, streamsResult.value().get("count"), "Count should be 1");
        assertSchemaIdHeaders(streamsResult.headers(), outputTopic, "Join output for streams");

        // Verify "other" key did not join
        assertFalse(resultsByKey.containsKey("other"), "Should not have joined result for other key");

        assertEquals(0, afterNull.size(),
            "Null-value inputs should be dropped; no new output records should appear");

        closeStreams(streams);

        // Verify changelog topics for left and right join stores
        String leftChangelog = changelogTopicFor(applicationId, leftJoinStore);
        String rightChangelog = changelogTopicFor(applicationId, rightJoinStore);

        List<ConsumerRecord<byte[], byte[]>> leftChangelogRecords =
            consumeRecords(leftChangelog, "join-left-changelog-cg-" + testId, 3, ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(leftChangelogRecords.size() >= 1, "Left join changelog should have at least 1 record, got " + leftChangelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : leftChangelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), leftChangelog, "Left join store changelog");
            }
        }

        List<ConsumerRecord<byte[], byte[]>> rightChangelogRecords =
            consumeRecords(rightChangelog, "join-right-changelog-cg-" + testId, 3, ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(rightChangelogRecords.size() >= 1, "Right join changelog should have at least 1 record, got " + rightChangelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : rightChangelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), rightChangelog, "Right join store changelog");
            }
        }
    }

    /**
     * Verifies stream-stream {@code leftJoin()} on a windowed join with a headers-aware
     * window store. Left-join emits a result for every left record, with right=null when
     * no right-side match exists in the join window.
     */
    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldStreamStreamsLeftJoinWithHeaders(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String leftTopic = "leftjoin-left-input" + suffix;
        String rightTopic = "leftjoin-right-input" + suffix;
        String outputTopic = "leftjoin-output" + suffix;

        createTopics(leftTopic, rightTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration joinWindow = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(2);
        Duration storeWindowSize = joinWindow.multipliedBy(2);
        Duration retention = storeWindowSize.plus(graceEnabled ? gracePeriod : Duration.ZERO);

        JoinWindows joinWindows = graceEnabled
            ? JoinWindows.ofTimeDifferenceAndGrace(joinWindow, gracePeriod)
            : JoinWindows.ofTimeDifferenceWithNoGrace(joinWindow);

        KStream<GenericRecord, GenericRecord> leftStream = builder.stream(leftTopic, Consumed.with(keySerde, valueSerde));
        KStream<GenericRecord, GenericRecord> rightStream = builder.stream(rightTopic, Consumed.with(keySerde, valueSerde));

        String joinStoreName = "my-leftjoin" + suffix;
        String leftJoinStore = joinStoreName + "-left";
        String rightJoinStore = joinStoreName + "-right";

        WindowBytesStoreSupplier leftStoreSupplier = new WindowStoreSupplierWithHeaders(
            Stores.persistentTimestampedWindowStoreWithHeaders(leftJoinStore, retention, storeWindowSize, true));
        WindowBytesStoreSupplier rightStoreSupplier = new WindowStoreSupplierWithHeaders(
            Stores.persistentTimestampedWindowStoreWithHeaders(rightJoinStore, retention, storeWindowSize, true));

        leftStream.leftJoin(
            rightStream,
            (leftValue, rightValue) -> {
                GenericRecord joined = new GenericData.Record(aggSchema);
                String left = leftValue.get("line").toString();
                String right = rightValue == null ? "<none>" : rightValue.get("line").toString();
                joined.put("word", left + "-" + right);
                joined.put("count", rightValue == null ? 0L : 1L);
                return joined;
            },
            joinWindows,
            StreamJoined.with(keySerde, valueSerde, valueSerde)
                .withThisStoreSupplier(leftStoreSupplier)
                .withOtherStoreSupplier(rightStoreSupplier)
        ).to(outputTopic, Produced.with(keySerde, aggSerde));

        String applicationId = "stream-leftjoin-test" + suffix;
        Map<String, Object> extraProps = new HashMap<>();
        extraProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // Disable the wall-clock throttle on outer-emission scans so the unmatched left record
        // is emitted on the very next record arrival rather than waiting 1s of wall-clock time.
        extraProps.put("__emit.interval.ms.kstreams.outer.join.spurious.results.fix__", 0L);
        // Wait for cross-topic alignment so records get processed in event-time order; otherwise
        // a late right-side record can arrive after stream time has advanced and be dropped.
        extraProps.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 1000L);
        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled, extraProps);

        long baseTime = 13000000L;
        produce(leftTopic,
            at(baseTime, createKey("kafka"), createTextLine("L1")),
            at(baseTime + 3000, createKey("only-left"), createTextLine("L2")),
            // Drain advances stream time past only-left's join-window-close to trigger left-only emission.
            at(baseTime + 30000, createKey("drain"), createTextLine("drain")));
        produce(rightTopic,
            at(baseTime + 1000, createKey("kafka"), createTextLine("R1")),
            at(baseTime + 30000, createKey("drain"), createTextLine("drain")));

        Properties consumerProps = createConsumerProps("leftjoin-output-cg-" + testId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<GenericRecord, GenericRecord>> outputRecords = new ArrayList<>();
        List<ConsumerRecord<GenericRecord, GenericRecord>> afterNull = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
                 new KafkaConsumer<>(consumerProps, keySerde.deserializer(), aggSerde.deserializer())) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            consumer.poll(Duration.ofMillis(100));
            // Wait until the deterministic kafka join match appears.
            TestUtils.waitForCondition(
                () -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(outputRecords::add);
                    return outputRecords.stream()
                        .anyMatch(r -> "kafka".equals(r.key().get("word").toString()));
                },
                60_000,
                "kafka join result should appear");
            // Drain remaining outputs (only-left, drain).
            long drainDeadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < drainDeadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(outputRecords::add);
            }

            // Stream-stream leftJoin skips null-value inputs on either side
            produce(leftTopic, at(baseTime + 50000, createKey("kafka"), null));
            produce(rightTopic, at(baseTime + 50000, createKey("kafka"), null));
            long afterNullDeadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < afterNullDeadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(afterNull::add);
            }
        }
        assertTrue(outputRecords.size() >= 2,
            "leftJoin should emit kafka(both), only-left(left-only), drain(both); got " + outputRecords.size());

        Map<String, ConsumerRecord<GenericRecord, GenericRecord>> byKey = new HashMap<>();
        for (ConsumerRecord<GenericRecord, GenericRecord> record : outputRecords) {
            byKey.put(record.key().get("word").toString(), record);
            assertSchemaIdHeaders(record.headers(), outputTopic,
                "leftJoin output " + record.key().get("word"));
        }
        assertEquals("L1-R1", byKey.get("kafka").value().get("word").toString());
        assertEquals(1L, byKey.get("kafka").value().get("count"));

        assertTrue(byKey.containsKey("only-left"), "only-left should appear with right=<none>");
        assertEquals("L2-<none>", byKey.get("only-left").value().get("word").toString());
        assertEquals(0L, byKey.get("only-left").value().get("count"));

        assertEquals(0, afterNull.size(),
            "Null-value inputs should be dropped; no new output records should appear");

        closeStreams(streams);

        // Verify changelog store records for left, right and outer-left join stores.
        String leftChangelog = changelogTopicFor(applicationId, leftJoinStore);
        String rightChangelog = changelogTopicFor(applicationId, rightJoinStore);
        String outerLeftChangelog = changelogTopicFor(applicationId, leftJoinStore + "-left-shared-join-store");

        List<ConsumerRecord<byte[], byte[]>> leftChangelogRecords = consumeRecords(
            leftChangelog, "leftjoin-left-cg-" + testId, 3, ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(leftChangelogRecords.size() >= 1, "Left changelog should have records, got " + leftChangelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : leftChangelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), leftChangelog, "Left leftJoin store changelog");
            }
        }

        List<ConsumerRecord<byte[], byte[]>> rightChangelogRecords = consumeRecords(
            rightChangelog, "leftjoin-right-cg-" + testId, 2, ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(rightChangelogRecords.size() >= 1, "Right changelog should have records, got " + rightChangelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : rightChangelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), rightChangelog, "Right leftJoin store changelog");
            }
        }

        List<ConsumerRecord<byte[], byte[]>> outerLeftRecords = consumeRecords(
            outerLeftChangelog, "leftjoin-outer-cg-" + testId, 5, ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        boolean sawOnlyLeft = false;
        for (ConsumerRecord<byte[], byte[]> record : outerLeftRecords) {
            if (record.key() != null
                && new String(record.key(), java.nio.charset.StandardCharsets.UTF_8).contains("only-left")) {
                sawOnlyLeft = true;
                break;
            }
        }
        assertTrue(sawOnlyLeft,
            "outer-join store changelog (" + outerLeftChangelog + ") should contain the unmatched 'only-left' key; "
                + "got " + outerLeftRecords.size() + " records");
    }

    /**
     * Verifies stream-stream {@code outerJoin()} on a windowed join with a headers-aware
     * window store. Outer-join emits results for unmatched records on either side once
     * the join window has closed and stream time has advanced.
     */
    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldStreamStreamsOuterJoinWithHeaders(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String leftTopic = "outerjoin-left-input" + suffix;
        String rightTopic = "outerjoin-right-input" + suffix;
        String outputTopic = "outerjoin-output" + suffix;

        createTopics(leftTopic, rightTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration joinWindow = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(2);
        Duration storeWindowSize = joinWindow.multipliedBy(2);
        Duration retention = storeWindowSize.plus(graceEnabled ? gracePeriod : Duration.ZERO);

        JoinWindows joinWindows = graceEnabled
            ? JoinWindows.ofTimeDifferenceAndGrace(joinWindow, gracePeriod)
            : JoinWindows.ofTimeDifferenceWithNoGrace(joinWindow);

        KStream<GenericRecord, GenericRecord> leftStream = builder.stream(leftTopic, Consumed.with(keySerde, valueSerde));
        KStream<GenericRecord, GenericRecord> rightStream = builder.stream(rightTopic, Consumed.with(keySerde, valueSerde));

        String joinStoreName = "my-outerjoin" + suffix;
        String leftJoinStore = joinStoreName + "-left";
        String rightJoinStore = joinStoreName + "-right";

        WindowBytesStoreSupplier leftStoreSupplier = new WindowStoreSupplierWithHeaders(
            Stores.persistentTimestampedWindowStoreWithHeaders(leftJoinStore, retention, storeWindowSize, true));
        WindowBytesStoreSupplier rightStoreSupplier = new WindowStoreSupplierWithHeaders(
            Stores.persistentTimestampedWindowStoreWithHeaders(rightJoinStore, retention, storeWindowSize, true));

        leftStream.outerJoin(
            rightStream,
            (leftValue, rightValue) -> {
                GenericRecord joined = new GenericData.Record(aggSchema);
                String left = leftValue == null ? "<none>" : leftValue.get("line").toString();
                String right = rightValue == null ? "<none>" : rightValue.get("line").toString();
                joined.put("word", left + "-" + right);
                joined.put("count", (leftValue != null && rightValue != null) ? 1L : 0L);
                return joined;
            },
            joinWindows,
            StreamJoined.with(keySerde, valueSerde, valueSerde)
                .withThisStoreSupplier(leftStoreSupplier)
                .withOtherStoreSupplier(rightStoreSupplier)
        ).to(outputTopic, Produced.with(keySerde, aggSerde));

        // Produce inputs BEFORE starting streams so the consumer reads them deterministically from offset 0.
        long baseTime = 14000000L;
        // Drain advances stream time past join+grace on both sides to trigger left-/right-only emissions.
        produce(leftTopic,
            at(baseTime, createKey("both"), createTextLine("L1")),
            at(baseTime + 3000, createKey("only-left"), createTextLine("L2")),
            at(baseTime + 30000, createKey("drain"), createTextLine("drain")));
        produce(rightTopic,
            at(baseTime + 1000, createKey("both"), createTextLine("R1")),
            at(baseTime + 4000, createKey("only-right"), createTextLine("R2")),
            at(baseTime + 30000, createKey("drain"), createTextLine("drain")));

        String applicationId = "stream-outerjoin-test" + suffix;
        Map<String, Object> extraProps = new HashMap<>();
        extraProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // Disable the wall-clock throttle on outer-emission scans so unmatched-side records
        // are emitted on the very next record arrival rather than waiting 1s of wall-clock time.
        extraProps.put("__emit.interval.ms.kstreams.outer.join.spurious.results.fix__", 0L);
        // Wait for cross-topic alignment so records get processed in event-time order; otherwise
        // a late record on either side can arrive after stream time has advanced and be dropped.
        extraProps.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 1000L);
        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled, extraProps);

        Properties consumerProps = createConsumerProps("outerjoin-output-cg-" + testId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<GenericRecord, GenericRecord>> outputRecords = new ArrayList<>();
        List<ConsumerRecord<GenericRecord, GenericRecord>> afterNull = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
                 new KafkaConsumer<>(consumerProps, keySerde.deserializer(), aggSerde.deserializer())) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            consumer.poll(Duration.ofMillis(100));
            // Wait until the deterministic both-keys join match appears.
            TestUtils.waitForCondition(
                () -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(outputRecords::add);
                    return outputRecords.stream()
                        .anyMatch(r -> "both".equals(r.key().get("word").toString()));
                },
                60_000,
                "both-keys join result should appear");
            // Drain remaining outputs (only-left, only-right, drain).
            long drainDeadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < drainDeadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(outputRecords::add);
            }

            // Stream-stream outerJoin skips null-value inputs on either side
            produce(leftTopic, at(baseTime + 50000, createKey("both"), null));
            produce(rightTopic, at(baseTime + 50000, createKey("both"), null));
            long afterNullDeadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < afterNullDeadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(afterNull::add);
            }
        }
        assertTrue(outputRecords.size() >= 3,
            "outerJoin should emit at least 3 results (both, only-left, only-right, drain); got " + outputRecords.size());

        Map<String, ConsumerRecord<GenericRecord, GenericRecord>> byKey = new HashMap<>();
        for (ConsumerRecord<GenericRecord, GenericRecord> record : outputRecords) {
            byKey.put(record.key().get("word").toString(), record);
            assertSchemaIdHeaders(record.headers(), outputTopic,
                "outerJoin output " + record.key().get("word"));
        }
        assertEquals("L1-R1", byKey.get("both").value().get("word").toString());
        assertEquals(1L, byKey.get("both").value().get("count"));

        assertTrue(byKey.containsKey("only-left"), "only-left should appear");
        assertEquals("L2-<none>", byKey.get("only-left").value().get("word").toString());

        assertTrue(byKey.containsKey("only-right"), "only-right should appear");
        assertEquals("<none>-R2", byKey.get("only-right").value().get("word").toString());

        assertEquals(0, afterNull.size(),
            "Null-value inputs should be dropped; no new output records should appear");

        closeStreams(streams);

        // Verify changelog store records for left, right and outer-join stores.
        String leftChangelog = changelogTopicFor(applicationId, leftJoinStore);
        String rightChangelog = changelogTopicFor(applicationId, rightJoinStore);
        String outerSharedChangelog = changelogTopicFor(applicationId, leftJoinStore + "-outer-shared-join-store");

        List<ConsumerRecord<byte[], byte[]>> leftChangelogRecords = consumeRecords(
            leftChangelog, "outerjoin-left-cg-" + testId, 3, ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(leftChangelogRecords.size() >= 1, "Left changelog should have records, got " + leftChangelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : leftChangelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), leftChangelog, "Left outerJoin store changelog");
            }
        }

        List<ConsumerRecord<byte[], byte[]>> rightChangelogRecords = consumeRecords(
            rightChangelog, "outerjoin-right-cg-" + testId, 3, ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(rightChangelogRecords.size() >= 1, "Right changelog should have records, got " + rightChangelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : rightChangelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), rightChangelog, "Right outerJoin store changelog");
            }
        }

        List<ConsumerRecord<byte[], byte[]>> outerRecords = consumeRecords(
            outerSharedChangelog, "outerjoin-outer-cg-" + testId, 6, ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        boolean sawOnlyLeft = false;
        boolean sawOnlyRight = false;
        for (ConsumerRecord<byte[], byte[]> record : outerRecords) {
            if (record.key() == null) continue;
            String keyStr = new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);
            if (keyStr.contains("only-left")) sawOnlyLeft = true;
            if (keyStr.contains("only-right")) sawOnlyRight = true;
        }
        assertTrue(sawOnlyLeft,
            "outer-join shared store changelog (" + outerSharedChangelog + ") should contain unmatched 'only-left' key; got " + outerRecords.size() + " records");
        assertTrue(sawOnlyRight,
            "outer-join shared store changelog (" + outerSharedChangelog + ") should contain unmatched 'only-right' key; got " + outerRecords.size() + " records");
    }

    /**
     * Verifies {@code aggregate()} on sliding windows with a headers-aware window store.
     */
    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldAggregateWithSlidingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-sliding-agg-input" + suffix;
        String storeName = "window-sliding-agg-store" + suffix;
        String applicationId = "window-sliding-agg-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration grace = Duration.ofSeconds(5);
        SlidingWindows slidingWindows = graceEnabled
            ? SlidingWindows.ofTimeDifferenceAndGrace(windowSize, grace)
            : SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(slidingWindows)
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                (key, value, agg) -> {
                    // Tombstone via DELETE.
                    if ("DELETE".equals(value.get("line").toString())) {
                        return null;
                    }
                    GenericRecord updated = new GenericData.Record(aggSchema);
                    updated.put("word", key.get("word").toString());
                    updated.put("count", (long) agg.get("count") + 1L);
                    return updated;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 16000000L;
        // Monotonically non-decreasing timestamps so NoGrace sliding windows don't drop late records.
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("first")),
            at(baseTime + 1000, createKey("streams"), createTextLine("first")),
            at(baseTime + 2000, createKey("kafka"), createTextLine("second")),
            at(baseTime + 3000, createKey("streams"), createTextLine("second")),
            at(baseTime + 4000, createKey("kafka"), createTextLine("third")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    while (it.hasNext()) { it.next(); n++; }
                    return n >= 2;
                }
            },
            10_000, "kafka should appear in at least 2 sliding windows after aggregate");

        List<KeyValue<Long, ValueTimestampHeaders<GenericRecord>>> windows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                 store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                windows.add(w);
                assertSchemaIdHeaders(w.value.headers(), changelogTopic, "IQv1 sliding agg " + w.key);
                long count = (long) w.value.value().get("count");
                assertTrue(count >= 1 && count <= 3, "kafka sliding window count should be 1-3, got " + count);
            }
        }
        assertTrue(windows.size() >= 2, "kafka should be in at least 2 sliding windows, got " + windows.size());
        int kafkaWindowsBefore = windows.size();

        // Tombstone kafka via DELETE.
        produce(inputTopic, at(baseTime + 5000, createKey("kafka"), createTextLine("DELETE")));

        // Wait for kafka windows to be tombstoned or removed.
        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    boolean sawNull = false;
                    while (it.hasNext()) {
                        KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                        if (w.value == null || w.value.value() == null) sawNull = true;
                        n++;
                    }
                    return sawNull || n < kafkaWindowsBefore;
                }
            },
            10_000, "kafka sliding windows should be tombstoned or removed after DELETE");

        // other keys remain.
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                 store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "streams should still exist after kafka DELETE");
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                assertNotNull(w.value, "streams sliding window value should not be null");
                assertNotNull(w.value.value(), "streams sliding window inner value should not be null");
                long c = (long) w.value.value().get("count");
                assertTrue(c >= 1 && c <= 2, "streams sliding window count should be 1-2 after kafka DELETE, got " + c);
                assertSchemaIdHeaders(w.value.headers(), changelogTopic, "IQv1 sliding agg streams post-tombstone " + w.key);
            }
        }

        closeStreams(streams);

        int expectedRecords = cachingEnabled ? 6 : 13;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-sliding-agg-cg-" + testId, expectedRecords,
            ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertEquals(expectedRecords, changelogRecords.size(),
            "Should have " + expectedRecords + " changelog records");
        boolean sawTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), changelogTopic, "Sliding aggregate changelog");
            } else {
                sawTombstone = true;
                String keyAsString = new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(keyAsString.contains("kafka"), "Tombstone key bytes should contain 'kafka', got: " + keyAsString);
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Sliding aggregate changelog tombstone");
            }
        }
        if (!cachingEnabled) {
            assertTrue(sawTombstone, "uncached changelog should contain a tombstone for kafka");
        }
    }

    /**
     * Verifies {@code reduce()} on sliding windows with a headers-aware window store.
     */
    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldReduceWithSlidingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "window-sliding-reduce-input" + suffix;
        String storeName = "window-sliding-reduce-store" + suffix;
        String applicationId = "window-sliding-reduce-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration grace = Duration.ofSeconds(5);
        SlidingWindows slidingWindows = graceEnabled
            ? SlidingWindows.ofTimeDifferenceAndGrace(windowSize, grace)
            : SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(slidingWindows)
            .reduce(
                (oldVal, newVal) -> {
                    if ("DELETE".equals(newVal.get("line").toString())) {
                        return null;
                    }
                    GenericRecord combined = new GenericData.Record(valueSchema);
                    combined.put("line", oldVal.get("line") + "|" + newVal.get("line"));
                    return combined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 17000000L;
        // Monotonically non-decreasing timestamps so NoGrace sliding windows don't drop late records.
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("a")),
            at(baseTime + 1000, createKey("streams"), createTextLine("x")),
            at(baseTime + 2000, createKey("kafka"), createTextLine("b")),
            at(baseTime + 3000, createKey("streams"), createTextLine("y")),
            at(baseTime + 4000, createKey("kafka"), createTextLine("c")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    while (it.hasNext()) { it.next(); n++; }
                    return n >= 2;
                }
            },
            10_000, "kafka should appear in at least 2 sliding windows after reduce");

        List<KeyValue<Long, ValueTimestampHeaders<GenericRecord>>> windows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                 store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                windows.add(w);
                assertSchemaIdHeaders(w.value.headers(), changelogTopic, "IQv1 sliding reduce " + w.key);
                String reduced = w.value.value().get("line").toString();
                assertTrue(reduced.length() >= 1, "kafka sliding reduce line should be non-empty, got: " + reduced);
            }
        }
        assertTrue(windows.size() >= 2, "kafka should be in at least 2 sliding windows, got " + windows.size());
        int kafkaWindowsBefore = windows.size();

        // Tombstone kafka via DELETE.
        produce(inputTopic, at(baseTime + 5000, createKey("kafka"), createTextLine("DELETE")));

        // Wait for kafka windows to be tombstoned or removed.
        TestUtils.waitForCondition(
            () -> {
                try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                         store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                    int n = 0;
                    boolean sawNull = false;
                    while (it.hasNext()) {
                        KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                        if (w.value == null || w.value.value() == null) sawNull = true;
                        n++;
                    }
                    return sawNull || n < kafkaWindowsBefore;
                }
            },
            10_000, "kafka sliding windows should be tombstoned or removed after DELETE");

        // other keys remain.
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it =
                 store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "streams should still exist after kafka DELETE");
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> w = it.next();
                assertNotNull(w.value, "streams sliding window value should not be null");
                assertNotNull(w.value.value(), "streams sliding window inner value should not be null");
                String reduced = w.value.value().get("line").toString();
                assertTrue(reduced.length() >= 1, "streams sliding reduce line should be non-empty");
                assertSchemaIdHeaders(w.value.headers(), changelogTopic, "IQv1 sliding reduce streams post-tombstone " + w.key);
            }
        }

        closeStreams(streams);

        int expectedMinRecords = cachingEnabled ? 2 : 4;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-sliding-reduce-cg-" + testId, expectedMinRecords + 6,
            ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Should have at least " + expectedMinRecords + " changelog records, got " + changelogRecords.size());
        boolean sawTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), changelogTopic, "Sliding reduce changelog");
            } else {
                sawTombstone = true;
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Sliding reduce changelog tombstone");
            }
        }
        if (!cachingEnabled) {
            assertTrue(sawTombstone, "uncached changelog should contain a tombstone for kafka");
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldSuppressWithHeaders(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, graceEnabled, testId);
        String inputTopic = "suppress-input" + suffix;
        String outputTopic = "suppress-output" + suffix;
        String storeName = "suppress-store" + suffix;
        String applicationId = "suppress-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(2);

        // Choose window type based on graceEnabled parameter
        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        // Count with suppression - results only emitted when window closes
        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(timeWindows)
            .count(Materialized.<GenericRecord, Long>as(
                    new WindowStoreSupplierWithHeaders(
                        Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                .withKeySerde(keySerde))
            .suppress(Suppressed.untilWindowCloses(
                Suppressed.BufferConfig.unbounded()))
            .toStream()
            .to(outputTopic, Produced.with(
                new WindowedSerdes.TimeWindowedSerde<>(keySerde, windowSize.toMillis()),
                Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled,
            Collections.singletonMap(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L));

        long baseTime = 6000000L;

        // Send records within first window - should be suppressed (not emitted yet)
        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("quick brown fox jumps")),
            at(baseTime + 1000, createKey("kafka"), createTextLine("windowed by time")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> suppressStore = windowStore(streams, storeName);
        TestUtils.waitForCondition(
            () -> {
                Long v = getWindowValue(suppressStore, createKey("kafka"), 0L, Long.MAX_VALUE);
                return v != null && v == 2L;
            },
            10_000, "kafka should have count 2 in store before suppress release");

        // Verify no output yet (suppressed)
        Properties consumerProps = createConsumerProps("suppress-output-cg-" + testId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<byte[], byte[]>> outputRecords = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer())) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            ConsumerRecords<byte[], byte[]> records = consumer.poll(java.time.Duration.ofSeconds(3));
            records.forEach(outputRecords::add);
        }

        assertEquals(0, outputRecords.size(), "Should have no output yet (suppressed until window closes)");

        // Advance stream time past window close + grace period to trigger suppression release
        // (record far in future advances stream time)
        produce(inputTopic, at(baseTime + 10000, createKey("other"), createTextLine("advance stream time")));

        // The output consumer below polls for up to 10s for the suppressed release.

        // Verify suppressed results are emitted with headers
        outputRecords.clear();
        Properties consumerProps2 = createConsumerProps("suppress-output-cg2-" + testId);
        consumerProps2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final Instant suppressPollDeadline = Instant.now().plus(Duration.ofSeconds(10));
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps2,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer())) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            while (outputRecords.size() < 1 && Instant.now().isBefore(suppressPollDeadline)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(outputRecords::add);
            }
        }

        assertEquals(1, outputRecords.size(), "Should have 1 suppressed result released, got " + outputRecords.size());

        // Verify headers in suppressed output
        for (ConsumerRecord<byte[], byte[]> record : outputRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), outputTopic, "Suppressed output");
            }
        }

        // Verify IQv1 query shows headers
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = windowStore(streams, storeName);

        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find kafka window in store");
            KeyValue<Long, ValueTimestampHeaders<Long>> result = it.next();
            assertEquals(2L, result.value.value(), "kafka count should be 2");
            assertKeySchemaIdHeader(result.value.headers(), changelogTopic, "IQv1 suppressed store");
        }

        // Test null value handling
        produce(inputTopic,
            at(baseTime + 20000, createKey("nullvalue"), createTextLine("null value")),
            at(baseTime + 24000, createKey("nullvalue"), null));

        // Re-fetch store
        store = windowStore(streams, storeName);
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> nullValueStore = store;
        TestUtils.waitForCondition(
            () -> {
                Long v = getWindowValue(nullValueStore, createKey("nullvalue"), 0L, Long.MAX_VALUE);
                return v != null && v == 2L;
            },
            10_000, "nullvalue count should reach 2 (count() treats null value as event)");

        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("nullvalue"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find nullvalue in store");
            KeyValue<Long, ValueTimestampHeaders<Long>> result = it.next();
            assertEquals(2L, result.value.value(), "nullvalue count should be 2 (null value is counted as an event)");
            assertKeySchemaIdHeader(result.value.headers(), changelogTopic, "IQv1 null value handling in suppressed store");
        }

        closeStreams(streams);

        // Verify changelog headers
        int expectedMinRecords = cachingEnabled ? 3 : 5; // kafka updates + other
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "suppress-changelog-cg-" + testId, expectedMinRecords, ByteArrayDeserializer.class, ByteArrayDeserializer.class);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Should have at least " + expectedMinRecords + " changelog records, got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Changelog with suppression");
            }
        }
    }

    /**
     * Verifies that {@code KStream.process()} works correctly with a headers-aware
     * window store attached via the PAPI-on-DSL pattern. The processor reads/writes
     * the window store directly (one window per record's bucket-aligned timestamp)
     * and forwards a windowed count.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldProcessWithWindowStore(boolean cachingEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, testId);
        String inputTopic = "window-process-input" + suffix;
        String outputTopic = "window-process-output" + suffix;
        String storeName = "window-process-store" + suffix;
        String applicationId = "window-process-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        Duration windowSize = Duration.ofSeconds(10);
        Duration retention = Duration.ofMinutes(5);

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(storeName, retention, windowSize, false),
                keySerde,
                Serdes.Long()));

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(
                (org.apache.kafka.streams.processor.api.ProcessorSupplier<GenericRecord, GenericRecord, GenericRecord, Long>) () ->
                    new org.apache.kafka.streams.processor.api.Processor<GenericRecord, GenericRecord, GenericRecord, Long>() {
                        private TimestampedWindowStoreWithHeaders<GenericRecord, Long> store;
                        private org.apache.kafka.streams.processor.api.ProcessorContext<GenericRecord, Long> ctx;

                        @Override
                        public void init(org.apache.kafka.streams.processor.api.ProcessorContext<GenericRecord, Long> context) {
                            this.ctx = context;
                            this.store = context.getStateStore(storeName);
                        }

                        @Override
                        public void process(org.apache.kafka.streams.processor.api.Record<GenericRecord, GenericRecord> record) {
                            long windowStart = (record.timestamp() / windowSize.toMillis()) * windowSize.toMillis();
                            if (record.value() == null) {
                                store.put(record.key(), null, windowStart);
                                ctx.forward(new org.apache.kafka.streams.processor.api.Record<>(
                                    record.key(), null, record.timestamp(), record.headers()));
                                return;
                            }
                            ValueTimestampHeaders<Long> existing = store.fetch(record.key(), windowStart);
                            long current = (existing != null && existing.value() != null) ? existing.value() : 0L;
                            long updated = current + 1L;
                            store.put(record.key(),
                                ValueTimestampHeaders.make(updated, record.timestamp(), record.headers()),
                                windowStart);
                            ctx.forward(new org.apache.kafka.streams.processor.api.Record<>(
                                record.key(), updated, record.timestamp(), record.headers()));
                        }
                    },
                storeName)
            .to(outputTopic, Produced.with(keySerde, Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 9000000L;
        long windowStart = (baseTime / windowSize.toMillis()) * windowSize.toMillis();

        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("first")),
            at(baseTime + 1000, createKey("kafka"), createTextLine("second")),
            at(baseTime + 2000, createKey("kafka"), createTextLine("third")),
            at(baseTime + 3000, createKey("streams"), createTextLine("first")),
            at(baseTime + 4000, createKey("streams"), createTextLine("second")),
            at(baseTime + 5000, createKey("hello"), createTextLine("first")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                Long v = getWindowValue(store, createKey("kafka"), windowStart, windowSize.toMillis());
                return v != null && v == 3L;
            },
            10_000, "kafka window count should reach 3");

        ValueTimestampHeaders<Long> kafkaResult = store.fetch(createKey("kafka"), windowStart);
        assertEquals(3L, kafkaResult.value(), "IQv1: kafka count should be 3");
        assertKeySchemaIdHeader(kafkaResult.headers(), inputTopic, "IQv1 process kafka");

        ValueTimestampHeaders<Long> streamsResult = store.fetch(createKey("streams"), windowStart);
        assertNotNull(streamsResult, "IQv1: streams should exist");
        assertEquals(2L, streamsResult.value(), "IQv1: streams count should be 2");
        assertKeySchemaIdHeader(streamsResult.headers(), inputTopic, "IQv1 process streams");

        ValueTimestampHeaders<Long> helloResult = store.fetch(createKey("hello"), windowStart);
        assertNotNull(helloResult, "IQv1: hello should exist");
        assertEquals(1L, helloResult.value(), "IQv1: hello count should be 1");
        assertKeySchemaIdHeader(helloResult.headers(), inputTopic, "IQv1 process hello");

        // Tombstone kafka via null-value record (processor deletes the window).
        produce(inputTopic, at(baseTime + 6000, createKey("kafka"), null));

        TestUtils.waitForCondition(
            () -> getWindowValue(store, createKey("kafka"), windowStart, windowSize.toMillis()) == null,
            10_000, "IQv1: kafka should be tombstoned after null-value record");

        ValueTimestampHeaders<Long> streamsAfter = store.fetch(createKey("streams"), windowStart);
        assertNotNull(streamsAfter, "IQv1: streams should still exist after kafka tombstone");
        assertEquals(2L, streamsAfter.value(), "IQv1: streams should still be 2 after tombstone");
        ValueTimestampHeaders<Long> helloAfter = store.fetch(createKey("hello"), windowStart);
        assertNotNull(helloAfter, "IQv1: hello should still exist after kafka tombstone");
        assertEquals(1L, helloAfter.value(), "IQv1: hello should still be 1 after tombstone");

        closeStreams(streams);

        // Output: 6 phase-1 forwards + 1 tombstone forward.
        List<ConsumerRecord<GenericRecord, Long>> outputRecords = consumeRecords(
            outputTopic, "window-process-output-cg-" + testId, 7,
            KafkaAvroDeserializer.class, org.apache.kafka.common.serialization.LongDeserializer.class);
        assertEquals(7, outputRecords.size(), "Should have 7 forwarded output records (6 puts + 1 tombstone)");
        Map<String, Long> latestCounts = new HashMap<>();
        boolean sawOutputTombstone = false;
        for (ConsumerRecord<GenericRecord, Long> record : outputRecords) {
            String key = record.key().get("word").toString();
            if (record.value() == null) {
                sawOutputTombstone = true;
                assertEquals("kafka", key, "only kafka should be tombstoned in output");
                assertKeySchemaIdHeader(record.headers(), outputTopic, "process tombstone output " + key);
            } else {
                latestCounts.put(key, record.value());
                assertKeySchemaIdHeader(record.headers(), outputTopic, "process output " + key);
            }
        }
        assertTrue(sawOutputTombstone, "output should contain a tombstone for kafka");
        assertEquals(3L, latestCounts.get("kafka"), "kafka final non-tombstone count should be 3");
        assertEquals(2L, latestCounts.get("streams"), "streams latest count should be 2");
        assertEquals(1L, latestCounts.get("hello"), "hello latest count should be 1");

        // Changelog: 6 puts + 1 tombstone uncached; cached coalesces.
        int expectedMinRecords = cachingEnabled ? 3 : 7;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-process-cg-" + testId, expectedMinRecords + 2,
            ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Should have at least " + expectedMinRecords + " changelog records, got " + changelogRecords.size());
        boolean sawChangelogTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Process changelog");
            } else {
                sawChangelogTombstone = true;
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "Process changelog tombstone");
            }
        }
        // uncached: tombstone always reaches changelog; cached may coalesce.
        if (!cachingEnabled) {
            assertTrue(sawChangelogTombstone, "uncached changelog should contain a tombstone for kafka");
        }
    }

    /**
     * Verifies that {@code KStream.processValues()} works correctly with a headers-aware
     * window store attached via the PAPI-on-DSL pattern. Same shape as the
     * {@code process()} test but using the fixed-key processor API.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldProcessValuesWithWindowStore(boolean cachingEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = suffixOf(cachingEnabled, testId);
        String inputTopic = "window-pv-input" + suffix;
        String outputTopic = "window-pv-output" + suffix;
        String storeName = "window-pv-store" + suffix;
        String applicationId = "window-pv-test" + suffix;
        String changelogTopic = changelogTopicFor(applicationId, storeName);

        createTopics(inputTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        Duration windowSize = Duration.ofSeconds(10);
        Duration retention = Duration.ofMinutes(5);

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(storeName, retention, windowSize, false),
                keySerde,
                Serdes.Long()));

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .processValues(
                (org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier<GenericRecord, GenericRecord, Long>) () ->
                    new org.apache.kafka.streams.processor.api.FixedKeyProcessor<GenericRecord, GenericRecord, Long>() {
                        private TimestampedWindowStoreWithHeaders<GenericRecord, Long> store;
                        private org.apache.kafka.streams.processor.api.FixedKeyProcessorContext<GenericRecord, Long> ctx;

                        @Override
                        public void init(org.apache.kafka.streams.processor.api.FixedKeyProcessorContext<GenericRecord, Long> context) {
                            this.ctx = context;
                            this.store = context.getStateStore(storeName);
                        }

                        @Override
                        public void process(org.apache.kafka.streams.processor.api.FixedKeyRecord<GenericRecord, GenericRecord> record) {
                            long windowStart = (record.timestamp() / windowSize.toMillis()) * windowSize.toMillis();
                            if (record.value() == null) {
                                store.put(record.key(), null, windowStart);
                                ctx.forward(record.withValue(null));
                                return;
                            }
                            ValueTimestampHeaders<Long> existing = store.fetch(record.key(), windowStart);
                            long current = (existing != null && existing.value() != null) ? existing.value() : 0L;
                            long updated = current + 1L;
                            store.put(record.key(),
                                ValueTimestampHeaders.make(updated, record.timestamp(), record.headers()),
                                windowStart);
                            ctx.forward(record.withValue(updated));
                        }
                    },
                storeName)
            .to(outputTopic, Produced.with(keySerde, Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 10000000L;
        long windowStart = (baseTime / windowSize.toMillis()) * windowSize.toMillis();

        produce(inputTopic,
            at(baseTime, createKey("kafka"), createTextLine("first")),
            at(baseTime + 1000, createKey("kafka"), createTextLine("second")),
            at(baseTime + 2000, createKey("kafka"), createTextLine("third")),
            at(baseTime + 3000, createKey("streams"), createTextLine("first")),
            at(baseTime + 4000, createKey("streams"), createTextLine("second")),
            at(baseTime + 5000, createKey("hello"), createTextLine("first")));

        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = windowStore(streams, storeName);

        TestUtils.waitForCondition(
            () -> {
                Long v = getWindowValue(store, createKey("kafka"), windowStart, windowSize.toMillis());
                return v != null && v == 3L;
            },
            10_000, "kafka window count should reach 3");

        ValueTimestampHeaders<Long> kafkaResult = store.fetch(createKey("kafka"), windowStart);
        assertEquals(3L, kafkaResult.value(), "IQv1: kafka count should be 3");
        assertKeySchemaIdHeader(kafkaResult.headers(), inputTopic, "IQv1 processValues kafka");

        ValueTimestampHeaders<Long> streamsResult = store.fetch(createKey("streams"), windowStart);
        assertNotNull(streamsResult, "IQv1: streams should exist");
        assertEquals(2L, streamsResult.value(), "IQv1: streams count should be 2");
        assertKeySchemaIdHeader(streamsResult.headers(), inputTopic, "IQv1 processValues streams");

        ValueTimestampHeaders<Long> helloResult = store.fetch(createKey("hello"), windowStart);
        assertNotNull(helloResult, "IQv1: hello should exist");
        assertEquals(1L, helloResult.value(), "IQv1: hello count should be 1");
        assertKeySchemaIdHeader(helloResult.headers(), inputTopic, "IQv1 processValues hello");

        // Tombstone kafka via null-value record (processor deletes the window).
        produce(inputTopic, at(baseTime + 6000, createKey("kafka"), null));

        TestUtils.waitForCondition(
            () -> getWindowValue(store, createKey("kafka"), windowStart, windowSize.toMillis()) == null,
            10_000, "IQv1: kafka should be tombstoned after null-value record");

        ValueTimestampHeaders<Long> streamsAfter = store.fetch(createKey("streams"), windowStart);
        assertNotNull(streamsAfter, "IQv1: streams should still exist after kafka tombstone");
        assertEquals(2L, streamsAfter.value(), "IQv1: streams should still be 2 after tombstone");
        ValueTimestampHeaders<Long> helloAfter = store.fetch(createKey("hello"), windowStart);
        assertNotNull(helloAfter, "IQv1: hello should still exist after kafka tombstone");
        assertEquals(1L, helloAfter.value(), "IQv1: hello should still be 1 after tombstone");

        closeStreams(streams);

        // Output: 6 phase-1 forwards + 1 tombstone forward.
        List<ConsumerRecord<GenericRecord, Long>> outputRecords = consumeRecords(
            outputTopic, "window-pv-output-cg-" + testId, 7,
            KafkaAvroDeserializer.class, org.apache.kafka.common.serialization.LongDeserializer.class);
        assertEquals(7, outputRecords.size(), "Should have 7 forwarded output records (6 puts + 1 tombstone)");
        Map<String, Long> latestCounts = new HashMap<>();
        boolean sawOutputTombstone = false;
        for (ConsumerRecord<GenericRecord, Long> record : outputRecords) {
            String key = record.key().get("word").toString();
            if (record.value() == null) {
                sawOutputTombstone = true;
                assertEquals("kafka", key, "only kafka should be tombstoned in output");
                assertKeySchemaIdHeader(record.headers(), outputTopic, "processValues tombstone output " + key);
            } else {
                latestCounts.put(key, record.value());
                assertKeySchemaIdHeader(record.headers(), outputTopic, "processValues output " + key);
            }
        }
        assertTrue(sawOutputTombstone, "output should contain a tombstone for kafka");
        assertEquals(3L, latestCounts.get("kafka"));
        assertEquals(2L, latestCounts.get("streams"));
        assertEquals(1L, latestCounts.get("hello"));

        int expectedMinRecords = cachingEnabled ? 3 : 7;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRecords(
            changelogTopic, "window-pv-cg-" + testId, expectedMinRecords + 2,
            ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Should have at least " + expectedMinRecords + " changelog records, got " + changelogRecords.size());
        boolean sawChangelogTombstone = false;
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "ProcessValues changelog");
            } else {
                sawChangelogTombstone = true;
                assertKeySchemaIdHeader(record.headers(), changelogTopic, "ProcessValues changelog tombstone");
            }
        }
        if (!cachingEnabled) {
            assertTrue(sawChangelogTombstone, "uncached changelog should contain a tombstone for kafka");
        }
    }

}