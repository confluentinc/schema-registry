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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.stream.Stream;

@Tag("IntegrationTest")
public class SessionStoreWithHeadersDslIntegrationTest extends ClusterTestHarness {

    private static final String KEY_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"WordKey\",\"fields\":[{\"name\":\"word\",\"type\":\"string\"}]}";
    private static final String VALUE_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"TextLine\",\"fields\":[{\"name\":\"line\",\"type\":\"string\"}]}";
    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    public SessionStoreWithHeadersDslIntegrationTest() {
        super(1, true);
    }

    private static Stream<Arguments> cacheAndGraceParams() {
        return Stream.of(
            Arguments.of(true, true),
            Arguments.of(true, false),
            Arguments.of(false, true),
            Arguments.of(false, false)
        );
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldCountWithSessionWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "session-input" + suffix;
        String storeName = "session-store" + suffix;

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration inactivityGap = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);
        Duration retentionPeriod = Duration.ofMinutes(5);

        SessionWindows sessionWindows = graceEnabled
            ? SessionWindows.ofInactivityGapAndGrace(inactivityGap, gracePeriod)
            : SessionWindows.ofInactivityGapWithNoGrace(inactivityGap);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(sessionWindows)
            .count(Materialized.<GenericRecord, Long>as(
                    Stores.persistentSessionStoreWithHeaders(storeName, retentionPeriod))
                .withKeySerde(keySerde)
                .withValueSerde(Serdes.Long()));

        String changelog = "session-test" + suffix + "-" + storeName + "-changelog";

        try {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "pre-check-" + testId);
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
                consumer.subscribe(Collections.singletonList(changelog));
                consumer.poll(Duration.ofMillis(100));
            }
        } catch (Exception e) {
            System.out.println("  PRE-START: Error: " + e.getMessage());
        }

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "session-test" + suffix, cachingEnabled);

        long baseTime = 1000000L;

        // Test 1: Session merging
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("first"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 5000), createKey("kafka"), createTextLine("second"))).get();
            producer.flush();
        }

        Thread.sleep(5000);

        ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<Long>> store = streams.store(
            StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));

        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<Long>> it = store.fetch(createKey("kafka"))) {
            assertTrue(it.hasNext());
            KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<Long>> next = it.next();
            assertEquals(2L, next.value.aggregation());
            assertEquals(baseTime, next.key.window().start());
            assertEquals(baseTime + 5000, next.key.window().end());
            assertKeySchemaIdHeader(next.value.headers(), "Merged session");
            assertFalse(it.hasNext());
        }

        // Test 2: Separate sessions
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 20000), createKey("kafka"), createTextLine("third"))).get();
            producer.flush();
        }

        Thread.sleep(5000);

        store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));
        List<KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<Long>>> sessions = new ArrayList<>();
        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<Long>> it = store.fetch(createKey("kafka"))) {
            while (it.hasNext()) {
                sessions.add(it.next());
            }
        }

        assertEquals(2, sessions.size(), "Should have 2 separate sessions");

        // Verify first session [0, 5000], count=2
        assertEquals(2L, sessions.get(0).value.aggregation(), "First session count");
        assertEquals(baseTime, sessions.get(0).key.window().start(), "First session start");
        assertEquals(baseTime + 5000, sessions.get(0).key.window().end(), "First session end");
        assertKeySchemaIdHeader(sessions.get(0).value.headers(), "First session headers");

        // Verify second session [20000, 20000], count=1
        assertEquals(1L, sessions.get(1).value.aggregation(), "Second session count");
        assertEquals(baseTime + 20000, sessions.get(1).key.window().start(), "Second session start");
        assertEquals(baseTime + 20000, sessions.get(1).key.window().end(), "Second session end");
        assertKeySchemaIdHeader(sessions.get(1).value.headers(), "Second session headers");

        // Test 3: Null values counted
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 30000), createKey("streams"), (GenericRecord) null)).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 33000), createKey("streams"), (GenericRecord) null)).get();
            producer.flush();
        }

        Thread.sleep(5000);

        store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));
        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<Long>> it = store.fetch(createKey("streams"))) {
            assertTrue(it.hasNext(), "streams session should exist");
            KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<Long>> next = it.next();
            assertEquals(2L, next.value.aggregation(), "Null values should be counted");
            assertNotNull(next.value.aggregation(), "Count should not be null");
            assertEquals(baseTime + 30000, next.key.window().start(), "streams session start");
            assertEquals(baseTime + 33000, next.key.window().end(), "streams session end");
            assertKeySchemaIdHeader(next.value.headers(), "Null values counted");
            assertFalse(it.hasNext(), "Should have only one streams session");
        }

        // Test 4: Grace period
        if (graceEnabled) {
            try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
                // Advance stream time
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 47000), createKey("grace-advance"), createTextLine("advance"))).get();

                // Within grace: end = baseTime + 34000 > baseTime + 32000 (windowCloseTime) → ACCEPTED
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 34000), createKey("grace-within"), createTextLine("within"))).get();

                // Beyond grace: end = baseTime + 31000 < baseTime + 32000 (windowCloseTime) → DROPPED
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 31000), createKey("grace-beyond"), createTextLine("beyond"))).get();

                producer.flush();
            }
            Thread.sleep(3000);

            store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));

            // Verify grace-advance session exists
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<Long>> it = store.fetch(createKey("grace-advance"))) {
                assertTrue(it.hasNext(), "Grace-advance session should exist");
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<Long>> next = it.next();
                assertEquals(1L, next.value.aggregation());
                assertKeySchemaIdHeader(next.value.headers(), "Grace-advance session");
            }

            // Verify grace-within accepted
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<Long>> it = store.fetch(createKey("grace-within"))) {
                assertTrue(it.hasNext(), "Grace-within should be accepted");
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<Long>> next = it.next();
                assertEquals(1L, next.value.aggregation());
                assertKeySchemaIdHeader(next.value.headers(), "Grace-within session");
            }

            // Verify grace-beyond dropped
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<Long>> it = store.fetch(createKey("grace-beyond"))) {
                assertFalse(it.hasNext(), "Grace-beyond should be dropped");
            }
        }


        List<ConsumerRecord<byte[], byte[]>> beforeClose = consumeRawChangelog(changelog, "before-close-" + testId, 20);
        for (int i = 0; i < beforeClose.size(); i++) {
        }

        closeStreams(streams);
        Thread.sleep(2000);
        // 1. kafka: t=0 creates [0,0], t=5000 merges to [0,5000]
        // 2. kafka: t=20000 creates [20000,20000]
        // 3. streams: t=30000 creates [30000,30000], t=33000 merges to [30000,33000]
        //
        // Cached mode: only final states written after flush
        //   - kafka [0,5000] count=2
        //   - kafka [20000,20000] count=1
        //   - streams [30000,33000] count=2
        //   = 3 records (no grace) or more with grace
        //
        // Uncached mode: all intermediate updates written
        //   - kafka [0,0] count=1
        //   - kafka [0,5000] count=2
        //   - kafka [0,0] tombstone
        //   - kafka [20000,20000] count=1
        //   - streams [30000,30000] count=1
        //   - streams [30000,33000] count=2
        //   - streams [30000,30000] tombstone
        //   = 7 records (no grace)
        // Grace test (if enabled) adds:
        //   grace-advance [47000], grace-within [34000]
        //   Cached: +2, Uncached: +2
        int expectedMinRecords = cachingEnabled ? (graceEnabled ? 5 : 3) : (graceEnabled ? 9 : 7);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(changelog, "session-cg-" + testId + "-" + System.currentTimeMillis(), expectedMinRecords + 5);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Expected at least " + expectedMinRecords + " changelog records but got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), "Changelog");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldReduceWithSessionWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "session-reduce-input" + suffix;
        String storeName = "session-reduce-store" + suffix;

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration inactivityGap = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);
        Duration retentionPeriod = Duration.ofMinutes(5);

        SessionWindows sessionWindows = graceEnabled
            ? SessionWindows.ofInactivityGapAndGrace(inactivityGap, gracePeriod)
            : SessionWindows.ofInactivityGapWithNoGrace(inactivityGap);

        // Reducer: concatenate text lines with comma
        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(sessionWindows)
            .reduce(
                (v1, v2) -> {
                    GenericRecord combined = new GenericData.Record(valueSchema);
                    String line1 = v1.get("line").toString();
                    String line2 = v2.get("line").toString();
                    combined.put("line", line1 + "," + line2);
                    return combined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentSessionStoreWithHeaders(storeName, retentionPeriod))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        String changelog = "session-reduce-test" + suffix + "-" + storeName + "-changelog";

        try {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "reduce-pre-check-" + testId);
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
                consumer.subscribe(Collections.singletonList(changelog));
                consumer.poll(Duration.ofMillis(100));
            }
        } catch (Exception e) {
            System.out.println("  REDUCE PRE-START: Error: " + e.getMessage());
        }

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "session-reduce-test" + suffix, cachingEnabled);

        long baseTime = 2000000L;

        // Test 1: Session merging with reduce
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("hello"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 5000), createKey("kafka"), createTextLine("world"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 8000), createKey("kafka"), createTextLine("streams"))).get();
            producer.flush();
        }

        Thread.sleep(3000);

        ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store = streams.store(
            StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));

        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("kafka"))) {
            assertTrue(it.hasNext());
            KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
            String reducedLine = next.value.aggregation().get("line").toString();
            assertTrue(reducedLine.contains("hello") && reducedLine.contains("world") && reducedLine.contains("streams"),
                "Reduced value should contain all three: " + reducedLine);
            assertEquals(baseTime, next.key.window().start());
            assertEquals(baseTime + 8000, next.key.window().end());
            assertSchemaIdHeaders(next.value.headers(), "Reduced session");
            assertFalse(it.hasNext());
        }

        // Test 2: Separate sessions
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 25000), createKey("kafka"), createTextLine("separate"))).get();
            producer.flush();
        }

        Thread.sleep(3000);

        store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));
        List<KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>>> sessions = new ArrayList<>();
        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("kafka"))) {
            while (it.hasNext()) {
                sessions.add(it.next());
            }
        }

        assertEquals(2, sessions.size(), "Should have 2 separate sessions");

        // Verify first session [0, 8000] with merged values
        KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> session1 = sessions.get(0);
        assertEquals(baseTime, session1.key.window().start(), "First session start");
        assertEquals(baseTime + 8000, session1.key.window().end(), "First session end");
        String reducedLine = session1.value.aggregation().get("line").toString();
        assertTrue(reducedLine.contains("hello") && reducedLine.contains("world") && reducedLine.contains("streams"),
            "First session should contain all merged values: " + reducedLine);
        assertSchemaIdHeaders(session1.value.headers(), "First session headers");

        // Verify second session [25000, 25000] with single value
        KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> session2 = sessions.get(1);
        assertEquals(baseTime + 25000, session2.key.window().start(), "Second session start");
        assertEquals(baseTime + 25000, session2.key.window().end(), "Second session end");
        assertEquals("separate", session2.value.aggregation().get("line").toString(), "Second session value");
        assertSchemaIdHeaders(session2.value.headers(), "Second session headers");

        // Test 3: Grace period
        if (graceEnabled) {
            try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
                // Advance stream time
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 40000), createKey("grace-advance"), createTextLine("advance"))).get();

                // Within grace: end = baseTime + 26000 > baseTime + 25000 → ACCEPTED
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 26000), createKey("grace-within"), createTextLine("within"))).get();

                // Beyond grace: end = baseTime + 24000 < baseTime + 25000 → DROPPED
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 24000), createKey("grace-beyond"), createTextLine("beyond"))).get();

                producer.flush();
            }
            Thread.sleep(3000);

            store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));

            // Verify grace-advance session exists
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("grace-advance"))) {
                assertTrue(it.hasNext(), "Grace-advance session should exist");
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
                assertEquals("advance", next.value.aggregation().get("line").toString());
                assertSchemaIdHeaders(next.value.headers(), "Grace-advance session");
            }

            // Verify within-grace accepted
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("grace-within"))) {
                assertTrue(it.hasNext(), "Within-grace should be accepted");
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
                assertEquals("within", next.value.aggregation().get("line").toString());
                assertSchemaIdHeaders(next.value.headers(), "Within-grace session");
            }

            // Verify beyond-grace dropped
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("grace-beyond"))) {
                assertFalse(it.hasNext(), "Beyond-grace should be dropped");
            }
        }

        closeStreams(streams);
        Thread.sleep(2000);
        // Cached mode: only final states
        //   - kafka [0,8000] with "hello,world,streams"
        //   - kafka [25000,25000] with "separate"
        //   = 2 records (no grace)
        // Uncached mode: all intermediate updates
        //   - kafka [0,0] "hello"
        //   - kafka [0,5000] "hello,world" (merged [0,0] and [5000,5000])
        //   - kafka [0,0] tombstone
        //   - kafka [0,8000] "hello,world,streams" (merged [0,5000] and [8000,8000])
        //   - kafka [0,5000] tombstone
        //   - kafka [25000,25000] "separate"
        //   = 6 records (no grace)
        // Grace test adds: grace-advance [40000], grace-within [26000]
        //   Cached: +2, Uncached: +2
        int expectedMinRecords = cachingEnabled ? (graceEnabled ? 4 : 2) : (graceEnabled ? 8 : 6);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(changelog, "session-reduce-cg-" + testId, expectedMinRecords + 5);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Expected at least " + expectedMinRecords + " changelog records but got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), "Reduce changelog");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldAggregateWithSessionWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "session-agg-input" + suffix;
        String storeName = "session-agg-store" + suffix;

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        String aggSchemaJson = "{\"type\":\"record\",\"name\":\"WordCount\",\"fields\":[{\"name\":\"word\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"long\"}]}";
        Schema aggSchema = new Schema.Parser().parse(aggSchemaJson);
        GenericAvroSerde aggSerde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        aggSerde.configure(config, false);

        StreamsBuilder builder = new StreamsBuilder();
        Duration inactivityGap = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);
        Duration retentionPeriod = Duration.ofMinutes(5);

        SessionWindows sessionWindows = graceEnabled
            ? SessionWindows.ofInactivityGapAndGrace(inactivityGap, gracePeriod)
            : SessionWindows.ofInactivityGapWithNoGrace(inactivityGap);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(sessionWindows)
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                (key, value, agg) -> {
                    // Test null aggregation: DELETE tombstones the session
                    if (value != null && "DELETE".equals(value.get("line").toString())) {
                        return null;
                    }
                    GenericRecord updated = new GenericData.Record(aggSchema);
                    updated.put("word", key.get("word").toString());
                    updated.put("count", (long) agg.get("count") + 1L);
                    return updated;
                },
                (key, agg1, agg2) -> {
                    // Session merger: combine counts when sessions merge
                    GenericRecord merged = new GenericData.Record(aggSchema);
                    merged.put("word", key.get("word").toString());
                    merged.put("count", (long) agg1.get("count") + (long) agg2.get("count"));
                    return merged;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentSessionStoreWithHeaders(storeName, retentionPeriod))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde));

        String changelog = "session-agg-test" + suffix + "-" + storeName + "-changelog";

        try {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "agg-pre-check-" + testId);
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
                consumer.subscribe(Collections.singletonList(changelog));
                consumer.poll(Duration.ofMillis(100));
            }
        } catch (Exception e) {
            System.out.println("  AGG PRE-START: Error: " + e.getMessage());
        }

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "session-agg-test" + suffix, cachingEnabled);

        long baseTime = 3000000L;

        // Test 1: Session merging with aggregate
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("msg1"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 5000), createKey("kafka"), createTextLine("msg2"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 8000), createKey("kafka"), createTextLine("msg3"))).get();
            producer.flush();
        }

        Thread.sleep(3000);

        ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store = streams.store(
            StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));

        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("kafka"))) {
            assertTrue(it.hasNext());
            KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
            assertEquals(3L, next.value.aggregation().get("count"), "Aggregated count should be 3");
            assertEquals("kafka", next.value.aggregation().get("word").toString());
            assertEquals(baseTime, next.key.window().start(), "kafka session start");
            assertEquals(baseTime + 8000, next.key.window().end(), "kafka session end");
            assertSchemaIdHeaders(next.value.headers(), "Aggregated session");
            assertFalse(it.hasNext(), "Should have only one kafka session");
        }

        // Test 2: Null aggregation
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 20000), createKey("test"), createTextLine("create"))).get();
            producer.flush();
        }
        Thread.sleep(2000);

        store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));
        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("test"))) {
            assertTrue(it.hasNext(), "test session should exist");
            KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
            assertEquals(1L, next.value.aggregation().get("count"));
            assertEquals("test", next.value.aggregation().get("word").toString());
            assertEquals(baseTime + 20000, next.key.window().start());
            assertEquals(baseTime + 20000, next.key.window().end());
            assertSchemaIdHeaders(next.value.headers(), "test session");
        }

        // Send DELETE to tombstone
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 21000), createKey("test"), createTextLine("DELETE"))).get();
            producer.flush();
        }
        Thread.sleep(2000);

        store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));
        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("test"))) {
            if (it.hasNext()) {
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
                assertNull(next.value.aggregation(), "Aggregation should be null (tombstoned)");
                // Window should still exist
                assertEquals(baseTime + 20000, next.key.window().start(), "Tombstoned session window start");
                assertEquals(baseTime + 21000, next.key.window().end(), "Tombstoned session window end (merged with DELETE)");
            }
        }

        // Verify kafka session still exists
        try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("kafka"))) {
            assertTrue(it.hasNext(), "kafka session should still exist");
            KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
            assertNotNull(next.value.aggregation(), "kafka session should not be tombstoned");
            assertEquals(3L, next.value.aggregation().get("count"), "kafka count should still be 3");
        }

        // Test 3: Grace period
        if (graceEnabled) {
            try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
                // Advance stream time
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 36000), createKey("grace-advance"), createTextLine("advance"))).get();

                // Within grace: end = baseTime + 22000 > baseTime + 21000 → ACCEPTED
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 22000), createKey("grace-within"), createTextLine("within"))).get();

                // Beyond grace: end = baseTime + 20000 < baseTime + 21000 → DROPPED
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 20000), createKey("grace-beyond"), createTextLine("beyond"))).get();

                producer.flush();
            }
            Thread.sleep(3000);

            store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new SessionStoreWithHeadersType<>()));

            // Verify grace-advance session exists
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("grace-advance"))) {
                assertTrue(it.hasNext(), "Grace-advance session should exist");
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
                assertEquals(1L, next.value.aggregation().get("count"));
                assertEquals("grace-advance", next.value.aggregation().get("word").toString());
                assertSchemaIdHeaders(next.value.headers(), "Grace-advance session");
            }

            // Verify within-grace accepted
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("grace-within"))) {
                assertTrue(it.hasNext(), "Within-grace should be accepted");
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> next = it.next();
                assertEquals(1L, next.value.aggregation().get("count"));
                assertEquals("grace-within", next.value.aggregation().get("word").toString());
                assertSchemaIdHeaders(next.value.headers(), "Within-grace session");
            }

            // Verify beyond-grace dropped
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it = store.fetch(createKey("grace-beyond"))) {
                assertFalse(it.hasNext(), "Beyond-grace should be dropped");
            }
        }

        closeStreams(streams);
        Thread.sleep(2000);

        // Cached mode: only final states
        //   - kafka [0,8000] count=3
        //   - test [20000,21000] count=null (tombstoned by DELETE)
        //   = 2 records (no grace)
        // Uncached mode: all intermediate updates
        //   - kafka [0,0] count=1
        //   - kafka [0,5000] count=2
        //   - kafka [0,0] tombstone
        //   - kafka [0,8000] count=3
        //   - kafka [0,5000] tombstone
        //   - test [20000,20000] count=1
        //   - test [20000,21000] count=null (tombstoned)
        //   - test [20000,20000] tombstone
        //   = 8 records (no grace)
        // Grace test adds: grace-advance [36000], grace-within [22000]
        //   Cached: +2, Uncached: +2
        int expectedMinRecords = cachingEnabled ? (graceEnabled ? 4 : 2) : (graceEnabled ? 10 : 8);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(changelog, "session-agg-cg-" + testId, expectedMinRecords + 5);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Expected at least " + expectedMinRecords + " changelog records but got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), "Aggregate changelog");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldSuppressWithSessionWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "session-suppress-input" + suffix;
        String outputTopic = "session-suppress-output" + suffix;
        String storeName = "session-suppress-store" + suffix;

        createTopics(inputTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration inactivityGap = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);
        Duration retentionPeriod = Duration.ofMinutes(5);

        SessionWindows sessionWindows = graceEnabled
            ? SessionWindows.ofInactivityGapAndGrace(inactivityGap, gracePeriod)
            : SessionWindows.ofInactivityGapWithNoGrace(inactivityGap);

        // Reduce with suppression - results only emitted when window closes
        // Using reduce to preserve value type (GenericRecord) and thus value headers
        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(sessionWindows)
            .reduce(
                (v1, v2) -> {
                    GenericRecord combined = new GenericData.Record(valueSchema);
                    String line1 = v1.get("line").toString();
                    String line2 = v2.get("line").toString();
                    combined.put("line", line1 + "," + line2);
                    return combined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentSessionStoreWithHeaders(storeName, retentionPeriod))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .to(outputTopic, Produced.with(
                new WindowedSerdes.SessionWindowedSerde<>(keySerde),
                valueSerde));

        String changelog = "session-suppress-test" + suffix + "-" + storeName + "-changelog";

        try {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "suppress-pre-check-" + testId);
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
                consumer.subscribe(Collections.singletonList(changelog));
                consumer.poll(Duration.ofMillis(100));
            }
        } catch (Exception e) {
            System.out.println("  SUPPRESS PRE-START: Error: " + e.getMessage());
        }

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "session-suppress-test" + suffix, cachingEnabled);

        long baseTime = 4000000L;

        // Send records within first session
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("first"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 3000), createKey("kafka"), createTextLine("second"))).get();
            producer.flush();
        }

        Thread.sleep(2000);

        // Verify all should be suppressed
        Properties consumerProps = createConsumerProps("suppress-output-cg-" + testId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        List<ConsumerRecord<byte[], byte[]>> outputRecords = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(3));
            records.forEach(outputRecords::add);
        }

        assertEquals(0, outputRecords.size(), "Should have no output yet (suppressed until window closes)");

        // Advance stream time past window close + grace period to trigger suppression release
        long advanceTime = graceEnabled ? baseTime + 20000 : baseTime + 15000;
        long finalAdvanceTime = graceEnabled ? baseTime + 36000 : baseTime + 26000;
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            // Send record to advance stream time and close kafka session
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(advanceTime), createKey("other"), createTextLine("advance"))).get();
            // Send another record to advance further so 'other' session can also be emitted
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(finalAdvanceTime), createKey("final"), createTextLine("final"))).get();
            producer.flush();
        }

        Thread.sleep(5000);

        // Verify suppressed results are emitted with headers
        outputRecords.clear();
        Properties consumerProps2 = createConsumerProps("suppress-output-cg2-" + testId);
        consumerProps2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps2)) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            long end = System.currentTimeMillis() + 10000;
            while (outputRecords.size() < 2 && System.currentTimeMillis() < end) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(outputRecords::add);
            }
        }

        assertTrue(outputRecords.size() >= 2, "Should have at least 2 output records (kafka session + other session)");

        // Verify headers on suppressed output - both key and value schema ID headers
        for (ConsumerRecord<byte[], byte[]> record : outputRecords) {
            assertSchemaIdHeaders(record.headers(), "Suppressed output");
        }

        closeStreams(streams);
        Thread.sleep(2000);

        int expectedMinRecords = cachingEnabled ? 2 : 4;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(changelog, "session-suppress-cg-" + testId, expectedMinRecords + 5);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Expected at least " + expectedMinRecords + " changelog records but got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertSchemaIdHeaders(record.headers(), "Suppress changelog");
            }
        }
    }

    private static class SessionStoreWithHeadersType<K, V>
        implements QueryableStoreType<ReadOnlySessionStore<K, AggregationWithHeaders<V>>> {
        @Override public boolean accepts(StateStore s) {
            return s instanceof SessionStoreWithHeaders && s instanceof ReadOnlySessionStore;
        }
        @Override public ReadOnlySessionStore<K, AggregationWithHeaders<V>> create(StateStoreProvider p, String n) {
            return new CompositeReadOnlySessionStore<>(p, this, n);
        }
    }

    private void createTopics(String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(Arrays.stream(topicNames).map(n -> new NewTopic(n, 1, (short) 1)).collect(Collectors.toList())).all().get(30, TimeUnit.SECONDS);
        }
    }

    private GenericAvroSerde createKeySerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, true);
        return serde;
    }

    private GenericAvroSerde createValueSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, false);
        return serde;
    }

    private Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        return props;
    }

    private KafkaStreams startStreamsAndAwaitRunning(org.apache.kafka.streams.Topology topology, String appId, boolean cachingEnabled) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);
        if (!cachingEnabled) props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((n, o) -> { if (n == KafkaStreams.State.RUNNING) latch.countDown(); });
        streams.start();
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        return streams;
    }

    private Properties createConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        return props;
    }

    private void assertKeySchemaIdHeader(Headers headers, String context) {
        Header keySchemaIdHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keySchemaIdHeader, context + ": should have __key_schema_id header");
        byte[] keyHeaderBytes = keySchemaIdHeader.value();
        assertEquals(17, keyHeaderBytes.length, context + ": Key GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, keyHeaderBytes[0], context + ": Key header should have V1 magic byte");
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
        GenericRecord r = new GenericData.Record(keySchema);
        r.put("word", word);
        return r;
    }

    private GenericRecord createTextLine(String line) {
        GenericRecord r = new GenericData.Record(valueSchema);
        r.put("line", line);
        return r;
    }

    private List<ConsumerRecord<byte[], byte[]>> consumeRawChangelog(String topic, String group, int count) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        List<ConsumerRecord<byte[], byte[]>> results = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(Collections.singletonList(topic));

            // First poll to trigger partition assignment
            for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(100))) {
                results.add(r);
            }

            long end = System.currentTimeMillis() + 15000;
            while (results.size() < count && System.currentTimeMillis() < end) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) results.add(r);
            }
        }
        return results;
    }

    private void closeStreams(KafkaStreams streams) {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
        }
    }
}