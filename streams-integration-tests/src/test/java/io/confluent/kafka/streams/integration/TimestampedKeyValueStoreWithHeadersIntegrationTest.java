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
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link TimestampedKeyValueStoreWithHeaders} that verifies store operations
 * work correctly with header-based schema ID transport.
 */
public class TimestampedKeyValueStoreWithHeadersIntegrationTest extends ClusterTestHarness {

    private static final String INPUT_TOPIC = "word-plaintext-input";
    private static final String OUTPUT_TOPIC = "word-count-output";
    private static final String STORE_NAME = "words-store";

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

    public TimestampedKeyValueStoreWithHeadersIntegrationTest() {
        super(1, true);
    }

    /**
     * Test KeyValueStore operations (put, get, delete, putIfAbsent, putAll) with header-based schema ID transport.
     */
    @Test
    public void shouldPerformAllStoreOperationsWithHeaders() throws Exception {
        createTopics(INPUT_TOPIC, OUTPUT_TOPIC);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    keySerde,
                    valueSerde))
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .process(() -> new WordCountProcessor(STORE_NAME), STORE_NAME)
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "kv-store-integration-test");

            GenericRecord word1Key = createKey("word-1");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // Test 1: PUT - put word-1:1
                producer.send(new ProducerRecord<>(INPUT_TOPIC, word1Key, createValue(1L, "PUT"))).get();

                // Test 2: PUT - put word-1:1 again, aggregates to word-1:2
                producer.send(new ProducerRecord<>(INPUT_TOPIC, word1Key, createValue(1L, "PUT"))).get();

                // Test 3: PUT_IF_ABSENT - should not overwrite existing "word-1"
                producer.send(new ProducerRecord<>(INPUT_TOPIC, word1Key, createValue(100L, "PUT_IF_ABSENT"))).get();

                // Test 4: PUT_IF_ABSENT - new key "word-2" should be inserted
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("word-2"), createValue(50L, "PUT_IF_ABSENT"))).get();

                // Test 5: DELETE - delete "word-1"
                producer.send(new ProducerRecord<>(INPUT_TOPIC, word1Key, createValue(0L, "DELETE"))).get();

                // Test 6: DELETE - delete non-existing key
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("word-99"), createValue(0L, "DELETE"))).get();

                // Test 7: PUT_ALL - batch insert 3 words (hardcoded in processor)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("trigger"), createValue(0L, "PUT_ALL"))).get();

                // Test 8: DELETE non-existing key (should produce no output)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("nonexistent"), createValue(0L, "DELETE"))).get();

                producer.flush();
            }

            // Expect 8 records: PUT(1) + PUT(2) + PUT_IF_ABSENT(existing) + PUT_IF_ABSENT(new) + DELETE + PUT_ALL(3)
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(OUTPUT_TOPIC, "kv-store-test-consumer", 8);

            assertEquals(8, results.size(), "Should have 8 output records");

            // Verify PUT 1
            assertEquals("word-1", results.get(0).key().get("word").toString());
            assertEquals(1L, results.get(0).value().get("count"));
            assertSchemaIdHeaders(results.get(0), "PUT 1");

            // Verify PUT 1+1
            assertEquals("word-1", results.get(1).key().get("word").toString());
            assertEquals(2L, results.get(1).value().get("count"));
            assertSchemaIdHeaders(results.get(1), "PUT 2 (aggregated)");

            // Verify PUT_IF_ABSENT existing
            assertEquals("word-1", results.get(2).key().get("word").toString());
            assertEquals(2L, results.get(2).value().get("count"));
            assertSchemaIdHeaders(results.get(2), "PUT_IF_ABSENT existing");

            // Verify PUT_IF_ABSENT new key
            assertEquals("word-2", results.get(3).key().get("word").toString());
            assertEquals(50L, results.get(3).value().get("count"));
            assertSchemaIdHeaders(results.get(3), "PUT_IF_ABSENT new");

            // Verify DELETE
            assertEquals("word-1", results.get(4).key().get("word").toString());
            assertEquals(2L, results.get(4).value().get("count"));
            assertSchemaIdHeaders(results.get(4), "DELETE");

            // Verify DELETE non-existing key (produce no result)

            // Verify PUT_ALL 3 words
            assertEquals("word-3", results.get(5).key().get("word").toString());
            assertEquals(25L, results.get(5).value().get("count"));
            assertSchemaIdHeaders(results.get(5), "PUT_ALL word-3");

            assertEquals("word-4", results.get(6).key().get("word").toString());
            assertEquals(50L, results.get(6).value().get("count"));
            assertSchemaIdHeaders(results.get(6), "PUT_ALL word-4");

            assertEquals("word-5", results.get(7).key().get("word").toString());
            assertEquals(75L, results.get(7).value().get("count"));
            assertSchemaIdHeaders(results.get(7), "PUT_ALL word-5");

            // Verify DELETE non-existing key (should produce no output)

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * IQv1 verification for TimestampedKeyValueStoreWithHeaders.
     * Tests get, all, reverseAll, range, reverseRange, approximateNumEntries.
     */
    @Test
    public void shouldVerifyIQv1Operations() throws Exception {
        String iqv1InputTopic = "iqv1-input";
        String iqv1OutputTopic = "iqv1-output";
        String iqv1StoreName = "iqv1-store";

        createTopics(iqv1InputTopic, iqv1OutputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(iqv1StoreName),
                    keySerde,
                    valueSerde))
            .stream(iqv1InputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new WordCountProcessor(iqv1StoreName), iqv1StoreName)
            .to(iqv1OutputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "iqv1-test");

            GenericRecord word1Key = createKey("word-1");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // Test 1: PUT - put word-1:1
                producer.send(new ProducerRecord<>(iqv1InputTopic, word1Key, createValue(1L, "PUT"))).get();

                // Test 2: PUT - put word-1:1 again, aggregates to word-1:2
                producer.send(new ProducerRecord<>(iqv1InputTopic, word1Key, createValue(1L, "PUT"))).get();

                // Test 3: PUT_IF_ABSENT - should not overwrite existing "word-1"
                producer.send(new ProducerRecord<>(iqv1InputTopic, word1Key, createValue(100L, "PUT_IF_ABSENT"))).get();

                // Test 4: PUT_IF_ABSENT - new key "word-2" should be inserted
                producer.send(new ProducerRecord<>(iqv1InputTopic, createKey("word-2"), createValue(50L, "PUT_IF_ABSENT"))).get();

                // Test 5: DELETE - delete "word-1"
                producer.send(new ProducerRecord<>(iqv1InputTopic, word1Key, createValue(0L, "DELETE"))).get();

                // Test 6: DELETE - delete non-existing key (should produce no output)
                producer.send(new ProducerRecord<>(iqv1InputTopic, createKey("word-99"), createValue(0L, "DELETE"))).get();

                // Test 7: PUT_ALL - batch insert 3 words (hardcoded in processor)
                producer.send(new ProducerRecord<>(iqv1InputTopic, createKey("trigger"), createValue(0L, "PUT_ALL"))).get();

                producer.flush();
            }

            // Expect 8 records: PUT(1) + PUT(2) + PUT_IF_ABSENT(existing) + PUT_IF_ABSENT(new) + DELETE + PUT_ALL(3)
            List<ConsumerRecord<GenericRecord, GenericRecord>> iqv1Results =
                consumeRecords(iqv1OutputTopic, "iqv1-consumer", 8);
            assertEquals(8, iqv1Results.size(),
                "Should have 8 output records before IQv1 verification");

            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(iqv1StoreName, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

            // Verify DELETE: "word-1" should not exist (was deleted)
            ValueTimestampHeaders<GenericRecord> word1Result = store.get(word1Key);
            assertTrue(word1Result == null || word1Result.value() == null,
                "IQv1: word-1 should not exist after delete");

            // Verify PUT_IF_ABSENT: "word-2" should exist with count 50
            ValueTimestampHeaders<GenericRecord> word2Result = store.get(createKey("word-2"));
            assertNotNull(word2Result, "IQv1: word-2 should exist in store");
            assertEquals(50L, word2Result.value().get("count"), "IQv1: word-2 count should be 50");
            assertSchemaIdHeaders(word2Result.headers(), "IQv1 get word-2");

            // Verify PUT_ALL: all 3 word entries should exist
            ValueTimestampHeaders<GenericRecord> word3Result = store.get(createKey("word-3"));
            assertNotNull(word3Result, "IQv1: word-3 should exist in store");
            assertEquals(25L, word3Result.value().get("count"), "IQv1: word-3 count should be 25");
            assertSchemaIdHeaders(word3Result.headers(), "IQv1 get word-3");

            ValueTimestampHeaders<GenericRecord> word4Result = store.get(createKey("word-4"));
            assertNotNull(word4Result, "IQv1: word-4 should exist in store");
            assertEquals(50L, word4Result.value().get("count"), "IQv1: word-4 count should be 50");
            assertSchemaIdHeaders(word4Result.headers(), "IQv1 get word-4");

            ValueTimestampHeaders<GenericRecord> word5Result = store.get(createKey("word-5"));
            assertNotNull(word5Result, "IQv1: word-5 should exist in store");
            assertEquals(75L, word5Result.value().get("count"), "IQv1: word-5 count should be 75");
            assertSchemaIdHeaders(word5Result.headers(), "IQv1 get word-5");

            // GET non-existent key
            assertNull(store.get(createKey("word-99")), "IQv1: word-99 should return null");

            // ALL
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
                verifyKeyValueList(iter, Arrays.asList("word-2", "word-3", "word-4", "word-5"),
                    Arrays.asList(50L, 25L, 50L, 75L), "all");
            }

            // REVERSE ALL
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.reverseAll()) {
                verifyKeyValueList(iter, Arrays.asList("word-5", "word-4", "word-3", "word-2"),
                    Arrays.asList(75L, 50L, 25L, 50L), "reverseAll");
            }

            // RANGE - from word-2 to word-4
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.range(createKey("word-2"), createKey("word-4"))) {
                verifyKeyValueList(iter, Arrays.asList("word-2", "word-3", "word-4"),
                    Arrays.asList(50L, 25L, 50L), "range");
            }

            // REVERSE RANGE - from word-2 to word-4 in reverse
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.reverseRange(createKey("word-2"), createKey("word-4"))) {
                verifyKeyValueList(iter, Arrays.asList("word-4", "word-3", "word-2"),
                    Arrays.asList(50L, 25L, 50L), "reverseRange");
            }

            // APPROXIMATE NUM ENTRIES
            assertTrue(store.approximateNumEntries() >= 4, "approximateNumEntries should be at least 4");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Test ReadOnlyKeyValueStore operations (all, reverseAll, range, reverseRange, prefixScan, approximateNumEntries)
     */
    @Test
    public void shouldTestReadOnlyAndIteratorOperationsWithHeaders() throws Exception {
        String inputTopic = "iterator-input";
        String outputTopic = "iterator-output";
        String storeName = "iterator-store";

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                    keySerde,
                    valueSerde))
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new IteratorTestProcessor(storeName, keySerde.serializer()), storeName)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "iterator-integration-test");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                for (int i = 1; i <= 5; i++) {
                    producer.send(new ProducerRecord<>(inputTopic,
                        createKey("word-" + i), createValue(i * 10L, "PUT"))).get();
                }

                // Test ALL - iterate all 5 entries
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("all"), createValue(0L, "ALL"))).get();

                // Test REVERSE_ALL - iterate all 5 entries in reverse
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("reverse_all"), createValue(0L, "REVERSE_ALL"))).get();

                // Test RANGE - from word-2 to word-4
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("range"), createValue(0L, "RANGE"))).get();

                // Test REVERSE_RANGE - from word-2 to word-4 in reverse
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("reverse_range"), createValue(0L, "REVERSE_RANGE"))).get();

                // Test PREFIX_SCAN - keys starting with "word-"
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("prefix_scan"), createValue(0L, "PREFIX_SCAN"))).get();

                // Test APPROXIMATE_NUM_ENTRIES
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("approximate_num_entries"), createValue(0L, "APPROXIMATE_NUM_ENTRIES"))).get();

                // Test iterator methods: peekNextKey, hasNext, next
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("iterator"), createValue(0L, "ITERATOR_METHODS"))).get();

                producer.flush();
            }

            // Expected output counts:
            // PUT: 5
            // ALL: 5
            // REVERSE_ALL: 5
            // RANGE (word-2 to word-4): 3 (word-2, word-3, word-4)
            // REVERSE_RANGE: 3 (word-4, word-3, word-2)
            // PREFIX_SCAN: 5
            // APPROXIMATE_NUM_ENTRIES: 1
            // ITERATOR_METHODS: 1
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "iterator-test-consumer", 28);

            assertEquals(28, results.size(), "Should have 28 output records");

            int idx = 0;

            // Verify PUT results (5 entries)
            for (int i = 1; i <= 5; i++) {
                assertEquals("word-" + i, results.get(idx).key().get("word").toString());
                assertEquals(i * 10L, results.get(idx).value().get("count"));
                assertSchemaIdHeaders(results.get(idx), "PUT word-" + i);
                idx++;
            }

            // Verify ALL results
            for (int i = 1; i <= 5; i++) {
                assertEquals("word-" + i, results.get(idx).key().get("word").toString());
                assertSchemaIdHeaders(results.get(idx), "ALL word-" + i);
                idx++;
            }

            // Verify REVERSE_ALL results
            for (int i = 5; i >= 1; i--) {
                assertEquals("word-" + i, results.get(idx).key().get("word").toString());
                assertSchemaIdHeaders(results.get(idx), "REVERSE_ALL word-" + i);
                idx++;
            }

            // Verify RANGE results
            assertEquals("word-2", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "RANGE word-2");
            assertEquals("word-3", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "RANGE word-3");
            assertEquals("word-4", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "RANGE word-4");

            // Verify REVERSE_RANGE results
            assertEquals("word-4", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "REVERSE_RANGE word-4");
            assertEquals("word-3", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "REVERSE_RANGE word-3");
            assertEquals("word-2", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "REVERSE_RANGE word-2");

            // Verify PREFIX_SCAN results
            for (int i = 1; i <= 5; i++) {
                assertEquals("word-" + i, results.get(idx).key().get("word").toString());
                assertSchemaIdHeaders(results.get(idx), "PREFIX_SCAN word-" + i);
                idx++;
            }

            // Verify APPROXIMATE_NUM_ENTRIES
            assertTrue((Long) results.get(idx).value().get("count") >= 5,
                "approximateNumEntries should be at least 5");
            idx++;

            // Verify ITERATOR_METHODS
            assertEquals("iterator_test_passed", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx), "ITERATOR_METHODS");

            // Query store via IQv1
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(storeName, new TimestampedKeyValueStoreWithHeadersType<>()));

            // Test IQv1 all()
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> allIter = store.all()) {
                int count = 0;
                while (allIter.hasNext()) {
                    KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = allIter.next();
                    assertNotNull(kv.value, "IQv1 all() value should not be null");
                    assertSchemaIdHeaders(kv.value.headers(), "IQv1 all() entry " + count);
                    count++;
                }
                assertEquals(5, count, "IQv1 all() should return 5 entries");
            }

            // Test IQv1 range()
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> rangeIter =
                     store.range(createKey("word-2"), createKey("word-4"))) {
                List<String> rangeKeys = new ArrayList<>();
                while (rangeIter.hasNext()) {
                    KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = rangeIter.next();
                    rangeKeys.add(kv.key.get("word").toString());
                    assertSchemaIdHeaders(kv.value.headers(), "IQv1 range() " + kv.key.get("word"));
                }
                assertEquals(Arrays.asList("word-2", "word-3", "word-4"), rangeKeys,
                    "IQv1 range(word-2, word-4) should return word-2, word-3, word-4");
            }

            // Test IQv1 approximateNumEntries()
            assertTrue(store.approximateNumEntries() >= 5, "IQv1 approximateNumEntries should be at least 5");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Test store state operations: name(), isOpen(), persistent()
     */
    @Test
    public void shouldTestStoreStateOperations() throws Exception {
        String inputTopic = "state-input";
        String outputTopic = "state-output";
        String storeName = "state-store";

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                    keySerde,
                    valueSerde))
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new StateStoreTestProcessor(storeName), storeName)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "state-integration-test");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("trigger"), createValue(0L, "TEST_STORE_STATE"))).get();
                producer.flush();
            }

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "state-test-consumer", 1);

            assertEquals(1, results.size(), "Should have 1 output record");
            assertEquals(1L, results.get(0).value().get("count"),
                "Store state checks should pass (isOpen=true, persistent=true, name matches)");
            assertEquals("state-store", results.get(0).key().get("word").toString());

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Test delete by putting null value, putIfAbsent with null, and get on non-existent/deleted entries.
     */

    @Test
    public void shouldDeleteWithNullValueAndGetNonExistent() throws Exception {
        String inputTopic = "delete-input";
        String outputTopic = "delete-output";
        String storeName = "delete-store";

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                        keySerde,
                        valueSerde)
                    .withCachingDisabled())  // Disable caching for easier debugging
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new DeleteTestProcessor(storeName), storeName)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "delete-integration-test");

            GenericRecord word1Key = createKey("word-1");
            GenericRecord word2Key = createKey("word-2");
            GenericRecord word3Key = createKey("word-3");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // PUT word-1:10, word-2:20, word-3:30
                producer.send(new ProducerRecord<>(inputTopic, word1Key, createValue(10L, "PUT"))).get();
                producer.send(new ProducerRecord<>(inputTopic, word2Key, createValue(20L, "PUT"))).get();
                producer.send(new ProducerRecord<>(inputTopic, word3Key, createValue(30L, "PUT"))).get();

                // GET word-1 (should exist)
                producer.send(new ProducerRecord<>(inputTopic, word1Key, createValue(0L, "GET"))).get();

                // PUT_NULL word-1 (delete via put null)
                producer.send(new ProducerRecord<>(inputTopic, word1Key, createValue(0L, "PUT_NULL"))).get();

                // GET word-1 after delete (should be null)
                producer.send(new ProducerRecord<>(inputTopic, word1Key, createValue(0L, "GET"))).get();

                // PUT_NULL word-99 (delete non-existent)
                producer.send(new ProducerRecord<>(inputTopic, createKey("word-99"), createValue(0L, "PUT_NULL"))).get();

                // GET word-99 (should be null)
                producer.send(new ProducerRecord<>(inputTopic, createKey("word-99"), createValue(0L, "GET"))).get();

                // PUT_IF_ABSENT_NULL on existing word-2 (should not delete, returns existing value)
                producer.send(new ProducerRecord<>(inputTopic, word2Key, createValue(0L, "PUT_IF_ABSENT_NULL"))).get();

                // GET word-2 (should still be 20)
                producer.send(new ProducerRecord<>(inputTopic, word2Key, createValue(0L, "GET"))).get();

                // PUT_IF_ABSENT_NULL on non-existent word-98 (inserts null)
                producer.send(new ProducerRecord<>(inputTopic, createKey("word-98"), createValue(0L, "PUT_IF_ABSENT_NULL"))).get();

                // GET word-98 (should be null since null was inserted)
                producer.send(new ProducerRecord<>(inputTopic, createKey("word-98"), createValue(0L, "GET"))).get();

                // PUT word-3:100 (overwrite)
                producer.send(new ProducerRecord<>(inputTopic, word3Key, createValue(100L, "PUT"))).get();

                // GET word-3 (should be 100)
                producer.send(new ProducerRecord<>(inputTopic, word3Key, createValue(0L, "GET"))).get();

                // PUT_NULL word-3 (delete)
                producer.send(new ProducerRecord<>(inputTopic, word3Key, createValue(0L, "PUT_NULL"))).get();

                // GET word-3 after delete (should be null)
                producer.send(new ProducerRecord<>(inputTopic, word3Key, createValue(0L, "GET"))).get();

                // PUT word-4:40
                GenericRecord word4Key = createKey("word-4");
                producer.send(new ProducerRecord<>(inputTopic, word4Key, createValue(40L, "PUT"))).get();

                // DELETE word-4 (should return deleted value)
                producer.send(new ProducerRecord<>(inputTopic, word4Key, createValue(0L, "DELETE"))).get();

                // GET word-4 after delete (should be null)
                producer.send(new ProducerRecord<>(inputTopic, word4Key, createValue(0L, "GET"))).get();

                // DELETE word-97 (non-existent, no output)
                producer.send(new ProducerRecord<>(inputTopic, createKey("word-97"), createValue(0L, "DELETE"))).get();

                // GET word-97 (should be null)
                producer.send(new ProducerRecord<>(inputTopic, createKey("word-97"), createValue(0L, "GET"))).get();

                producer.flush();
            }

            // PUT(3) + GET(4) + PUT_IF_ABSENT_NULL(0) + PUT(1) + GET(2) + PUT(1) + DELETE(1) + GET(4) = 16
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "delete-test-consumer", 16);

            assertEquals(16, results.size());

            int idx = 0;

            // Verify 3 PUTs
            assertEquals("word-1", results.get(idx).key().get("word").toString());
            assertEquals(10L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT word-1");

            assertEquals("word-2", results.get(idx).key().get("word").toString());
            assertEquals(20L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT word-2");

            assertEquals("word-3", results.get(idx).key().get("word").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT word-3");

            // GET word-1 before delete
            assertEquals("word-1", results.get(idx).key().get("word").toString());
            assertEquals(10L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "GET word-1 before delete");

            // GET word-1 after delete (count=-1, null)
            assertEquals("word-1", results.get(idx).key().get("word").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));
            idx++;

            // GET word-99 (non-existent, count=-1, null)
            assertEquals("word-99", results.get(idx).key().get("word").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));
            idx++;

            // PUT_IF_ABSENT_NULL on existing word-2
            assertEquals("word-2", results.get(idx).key().get("word").toString());
            assertEquals(20L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT_IF_ABSENT_NULL existing");

            // GET word-2
            assertEquals("word-2", results.get(idx).key().get("word").toString());
            assertEquals(20L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "GET word-2 after PUT_IF_ABSENT_NULL");

            // GET word-98 (null was inserted, count=-1)
            assertEquals("word-98", results.get(idx).key().get("word").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));
            idx++;

            // PUT word-3:100
            assertEquals("word-3", results.get(idx).key().get("word").toString());
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT word-3 overwrite");

            // GET word-3 after overwrite
            assertEquals("word-3", results.get(idx).key().get("word").toString());
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "GET word-3 after overwrite");

            // GET word-3 after delete (count=-1, null)
            assertEquals("word-3", results.get(idx).key().get("word").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));
            idx++;

            // PUT word-4
            assertEquals("word-4", results.get(idx).key().get("word").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT word-4");

            // DELETE word-4 (returns deleted value)
            assertEquals("word-4", results.get(idx).key().get("word").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "DELETE word-4");

            // GET word-4 after delete (count=-1, null)
            assertEquals("word-4", results.get(idx).key().get("word").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));
            idx++;

            // GET word-97 (non-existent, count=-1, null)
            assertEquals("word-97", results.get(idx).key().get("word").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));

            // Verify changelog topic tombstones have key schema ID header
            String changelogTopic = "delete-integration-test-delete-store-changelog";

            List<ConsumerRecord<byte[], byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic, "changelog-consumer", 4);

            int tombstoneCount = 0;
            for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
                if (record.value() == null) {
                    // Verify key schema ID header is present in tombstone record
                    tombstoneCount++;
                    Header keySchemaIdHeader = record.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
                    assertNotNull(keySchemaIdHeader,
                        "Tombstone record should have key schema ID header");
                }
            }
            assertEquals(6, tombstoneCount,
                "Should have exactly 6 tombstone records: PUT_NULL word-1, PUT_NULL word-99, "
                    + "PUT_IF_ABSENT_NULL word-98, PUT_NULL word-3, DELETE word-4, DELETE word-97");

            // Verify which keys have been deleted
            KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer();
            Map<String, Object> deserializerConfig = new HashMap<>();
            deserializerConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                restApp.restConnect);
            keyDeserializer.configure(deserializerConfig, true);
            List<String> deletedKeys = new ArrayList<>();
            for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
                if (record.value() == null) {
                    GenericRecord key = (GenericRecord) keyDeserializer.deserialize(
                        changelogTopic, record.headers(), record.key());
                    deletedKeys.add(key.get("word").toString());
                }
            }
            assertTrue(deletedKeys.containsAll(
                Arrays.asList("word-1", "word-99", "word-3", "word-4")),
                "Deleted keys should include word-1, word-99, word-3, word-4 but got: "
                    + deletedKeys);

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Processor for testing iterator and read-only operations.
     */
    private static class IteratorTestProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private final Serializer<GenericRecord> keySerializer;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

        public IteratorTestProcessor(String storeName, Serializer<GenericRecord> keySerializer) {
            this.storeName = storeName;
            this.keySerializer = keySerializer;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            String operation = record.value().get("operation").toString();

            switch (operation) {
                case "PUT":
                    handlePut(record);
                    break;
                case "ALL":
                    handleTestAll(record);
                    break;
                case "REVERSE_ALL":
                    handleTestReverseAll(record);
                    break;
                case "RANGE":
                    handleTestRange(record);
                    break;
                case "REVERSE_RANGE":
                    handleTestReverseRange(record);
                    break;
                case "PREFIX_SCAN":
                    handleTestPrefixScan(record);
                    break;
                case "APPROXIMATE_NUM_ENTRIES":
                    handleTestApproximateNumEntries(record);
                    break;
                case "ITERATOR_METHODS":
                    handleTestIteratorMethods(record);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record) {
            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
            store.put(record.key(), toStore);

            ValueTimestampHeaders<GenericRecord> stored = store.get(record.key());
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }

        private void handleTestAll(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
                while (iter.hasNext()) {
                    KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key, kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        private void handleTestReverseAll(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.reverseAll()) {
                while (iter.hasNext()) {
                    KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key, kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        private void handleTestRange(Record<GenericRecord, GenericRecord> record) {
            GenericRecord fromKey = createKeyWithSchema(record.key().getSchema(), "word-2");
            GenericRecord toKey = createKeyWithSchema(record.key().getSchema(), "word-4");

            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.range(fromKey, toKey)) {
                while (iter.hasNext()) {
                    KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key, kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        private void handleTestReverseRange(Record<GenericRecord, GenericRecord> record) {
            GenericRecord fromKey = createKeyWithSchema(record.key().getSchema(), "word-2");
            GenericRecord toKey = createKeyWithSchema(record.key().getSchema(), "word-4");

            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.reverseRange(fromKey, toKey)) {
                while (iter.hasNext()) {
                    KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key, kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        private void handleTestPrefixScan(Record<GenericRecord, GenericRecord> record) {
            GenericRecord prefixKey = createKeyWithSchema(record.key().getSchema(), "word-");

            // PrefixScan relies on the keySerde's byte format: serialize a real key and trim its trailing
            // distinguishing byte to get a shared prefix. Works because all stored keys are the same length
            // (Avro string field encodes [length-byte][utf8]; identical length -> identical length-byte).
            // Would break if stored keys had different lengths.
            GenericRecord sampleKey = createKeyWithSchema(record.key().getSchema(), "word-1");
            byte[] sampleBytes = keySerializer.serialize("iterator-input", new RecordHeaders(), sampleKey);
            final byte[] prefixBytes = Arrays.copyOf(sampleBytes, sampleBytes.length - 1);

            Serializer<GenericRecord> prefixSerializer = (topic, data) -> prefixBytes;

            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(prefixKey, prefixSerializer)) {
                while (iter.hasNext()) {
                    KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key, kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        private void handleTestApproximateNumEntries(Record<GenericRecord, GenericRecord> record) {
            long count = store.approximateNumEntries();

            GenericRecord resultValue = new GenericData.Record(record.value().getSchema());
            resultValue.put("count", count);
            resultValue.put("operation", "RESULT");

            context.forward(new Record<>(record.key(), resultValue, record.timestamp(), record.headers()));
        }

        private void handleTestIteratorMethods(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
                // Test hasNext
                assertTrue(iter.hasNext(), "Iterator should have next");

                // Test peekNextKey
                GenericRecord peekedKey = iter.peekNextKey();
                assertNotNull(peekedKey, "peekNextKey should return a key");

                // Verify peekNextKey doesn't advance
                GenericRecord peekedAgain = iter.peekNextKey();
                assertEquals(peekedKey.get("word").toString(), peekedAgain.get("word").toString());

                // Test next
                KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> nextKv = iter.next();
                assertEquals(peekedKey.get("word").toString(), nextKv.key.get("word").toString());

                // Create success result
                GenericRecord successKey = createKeyWithSchema(record.key().getSchema(), "iterator_test_passed");
                GenericRecord successValue = new GenericData.Record(record.value().getSchema());
                successValue.put("count", 1L);
                successValue.put("operation", "RESULT");

                context.forward(new Record<>(
                    successKey, successValue, record.timestamp(), nextKv.value.headers()));
            }
        }

        private static GenericRecord createKeyWithSchema(Schema schema, String word) {
            GenericRecord key = new GenericData.Record(schema);
            key.put("word", word);
            return key;
        }
    }

    /**
     * Processor for testing store state operations.
     */
    private static class StateStoreTestProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

        public StateStoreTestProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            if ("TEST_STORE_STATE".equals(record.value().get("operation").toString())) {
                String name = store.name();
                boolean isOpen = store.isOpen();
                boolean isPersistent = store.persistent();

                long result = (storeName.equals(name) && isOpen && isPersistent) ? 1L : 0L;

                GenericRecord resultKey = new GenericData.Record(record.key().getSchema());
                resultKey.put("word", name);

                GenericRecord resultValue = new GenericData.Record(record.value().getSchema());
                resultValue.put("count", result);
                resultValue.put("operation", "RESULT");

                context.forward(new Record<>(resultKey, resultValue, record.timestamp(), record.headers()));
            }
        }
    }

    /**
     * Processor for testing delete operations (put null, putIfAbsent null) and get.
     */
    private static class DeleteTestProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

        public DeleteTestProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            String operation = record.value().get("operation").toString();

            switch (operation) {
                case "PUT":
                    handlePut(record);
                    break;
                case "PUT_NULL":
                    handlePutNull(record);
                    break;
                case "DELETE":
                    handleDelete(record);
                    break;
                case "PUT_IF_ABSENT_NULL":
                    handlePutIfAbsentNull(record);
                    break;
                case "GET":
                    handleGet(record);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record) {
            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
            store.put(record.key(), toStore);

            ValueTimestampHeaders<GenericRecord> stored = store.get(record.key());
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }

        // Strip schema-id headers from the incoming record's headers before performing tombstone-producing
        // store ops, so the changelog tombstone is written without a stale value schema id.
        // We mutate the record's headers in place — acceptable here because no downstream operator reads
        // these headers after this processor; if that ever changes, copy with new RecordHeaders(...).
        private void handlePutNull(Record<GenericRecord, GenericRecord> record) {
            record.headers().remove(SchemaId.KEY_SCHEMA_ID_HEADER);
            record.headers().remove(SchemaId.VALUE_SCHEMA_ID_HEADER);
            store.put(record.key(), null);
        }

        private void handleDelete(Record<GenericRecord, GenericRecord> record) {
            record.headers().remove(SchemaId.KEY_SCHEMA_ID_HEADER);
            record.headers().remove(SchemaId.VALUE_SCHEMA_ID_HEADER);
            ValueTimestampHeaders<GenericRecord> deleted = store.delete(record.key());

            if (deleted != null) {
                context.forward(new Record<>(
                    record.key(), deleted.value(), deleted.timestamp(), deleted.headers()));
            }
        }

        private void handlePutIfAbsentNull(Record<GenericRecord, GenericRecord> record) {
            record.headers().remove(SchemaId.KEY_SCHEMA_ID_HEADER);
            record.headers().remove(SchemaId.VALUE_SCHEMA_ID_HEADER);

            ValueTimestampHeaders<GenericRecord> existingValue = store.putIfAbsent(record.key(), null);

            if (existingValue != null) {
                context.forward(new Record<>(
                    record.key(), existingValue.value(), existingValue.timestamp(), existingValue.headers()));
            }
        }

        private void handleGet(Record<GenericRecord, GenericRecord> record) {
            ValueTimestampHeaders<GenericRecord> result = store.get(record.key());

            GenericRecord resultValue = new GenericData.Record(record.value().getSchema());
            if (result != null && result.value() != null) {
                resultValue.put("count", result.value().get("count"));
                resultValue.put("operation", "GET_RESULT");
                context.forward(new Record<>(
                    record.key(), resultValue, result.timestamp(), result.headers()));
            } else {
                // Return -1 to indicate null
                resultValue.put("count", -1L);
                resultValue.put("operation", "GET_RESULT_NULL");
                context.forward(new Record<>(
                    record.key(), resultValue, record.timestamp(), record.headers()));
            }
        }
    }

    /**
     * Processor that performs store operations based on the "operation" field.
     */
    private static class WordCountProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

        public WordCountProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            String operation = record.value().get("operation").toString();

            switch (operation) {
                case "PUT":
                    handlePut(record);
                    break;
                case "DELETE":
                    handleDelete(record);
                    break;
                case "PUT_IF_ABSENT":
                    handlePutIfAbsent(record);
                    break;
                case "PUT_ALL":
                    handlePutAll(record);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record) {
            ValueTimestampHeaders<GenericRecord> existingRecord = store.get(record.key());

            long newCount = (Long) record.value().get("count");
            if (existingRecord != null && existingRecord.value() != null) {
                newCount += (Long) existingRecord.value().get("count");
            }

            GenericRecord updatedValue = new GenericData.Record(record.value().getSchema());
            updatedValue.put("count", newCount);
            updatedValue.put("operation", "PUT");

            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(updatedValue, record.timestamp(), record.headers());
            store.put(record.key(), toStore);

            ValueTimestampHeaders<GenericRecord> storedRecord = store.get(record.key());
            context.forward(new Record<>(
                record.key(), storedRecord.value(), storedRecord.timestamp(), storedRecord.headers()));
        }

        private void handleDelete(Record<GenericRecord, GenericRecord> record) {
            record.headers().remove(SchemaId.KEY_SCHEMA_ID_HEADER);
            record.headers().remove(SchemaId.VALUE_SCHEMA_ID_HEADER);
            ValueTimestampHeaders<GenericRecord> deletedRecord = store.delete(record.key());

            if (deletedRecord != null) {
                context.forward(new Record<>(
                    record.key(), deletedRecord.value(), deletedRecord.timestamp(), deletedRecord.headers()));
            }
        }

        private void handlePutIfAbsent(Record<GenericRecord, GenericRecord> record) {
            ValueTimestampHeaders<GenericRecord> recordToInsert =
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
            ValueTimestampHeaders<GenericRecord> previousRecord = store.putIfAbsent(record.key(), recordToInsert);

            if (previousRecord != null) {
                context.forward(new Record<>(
                    record.key(), previousRecord.value(), previousRecord.timestamp(), previousRecord.headers()));
            } else {
                ValueTimestampHeaders<GenericRecord> insertedRecord = store.get(record.key());
                context.forward(new Record<>(
                    record.key(), insertedRecord.value(), insertedRecord.timestamp(), insertedRecord.headers()));
            }
        }

        private void handlePutAll(Record<GenericRecord, GenericRecord> record) {
            Schema keySchema = record.key().getSchema();
            Schema valueSchema = record.value().getSchema();

            // Hardcoded batch entries: word-3:25, word-4:50, word-5:75
            String[] words = {"word-3", "word-4", "word-5"};
            long[] counts = {25L, 50L, 75L};

            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> entries = new ArrayList<>();

            for (int i = 0; i < words.length; i++) {
                GenericRecord entryKey = new GenericData.Record(keySchema);
                entryKey.put("word", words[i]);

                GenericRecord entryValue = new GenericData.Record(valueSchema);
                entryValue.put("count", counts[i]);
                entryValue.put("operation", "PUT_ALL");

                ValueTimestampHeaders<GenericRecord> toStore =
                    ValueTimestampHeaders.make(entryValue, record.timestamp(), record.headers());
                entries.add(new KeyValue<>(entryKey, toStore));
            }

            store.putAll(entries);

            // Forward each stored entry
            for (KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> entry : entries) {
                ValueTimestampHeaders<GenericRecord> storedRecord = store.get(entry.key);
                context.forward(new Record<>(
                    entry.key, storedRecord.value(), storedRecord.timestamp(), storedRecord.headers()));
            }
        }
    }


    private void createTopics(String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.stream(topicNames)
                .map(name -> new NewTopic(name, 1, (short) 1))
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

    private Properties createConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        return props;
    }

    private KafkaStreams startStreamsAndAwaitRunning(Topology topology, String appId) throws Exception {
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
            running = startedLatch.await(30, TimeUnit.SECONDS);
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
        List<ConsumerRecord<GenericRecord, GenericRecord>> results = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
                 new KafkaConsumer<>(createConsumerProps(groupId))) {
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
            "Expected " + expectedCount + " records but got " + results.size());
        return results;
    }

    private Properties createChangelogConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    private List<ConsumerRecord<byte[], byte[]>> consumeChangelogRecords(
        String topic, String groupId, int expectedCount) {
        List<ConsumerRecord<byte[], byte[]>> results = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer =
                 new KafkaConsumer<>(createChangelogConsumerProps(groupId))) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    results.add(record);
                }
            }
        }
        return results;
    }

    private void assertSchemaIdHeaders(ConsumerRecord<GenericRecord, GenericRecord> record, String context) {
        assertSchemaIdHeaders(record.headers(), context);
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

    private void verifyKeyValueList(
        KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter,
        List<String> expectedKeys,
        List<Long> expectedCounts,
        String assertionContext) {
        List<String> keys = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        int idx = 0;
        while (iter.hasNext()) {
            KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
            keys.add(kv.key.get("word").toString());
            counts.add((Long) kv.value.value().get("count"));
            assertSchemaIdHeaders(kv.value.headers(), assertionContext + " entry " + idx);
            idx++;
        }
        assertEquals(expectedKeys.size(), idx, assertionContext + " number of records");
        assertEquals(expectedKeys, keys, assertionContext + " keys");
        assertEquals(expectedCounts, counts, assertionContext + " counts");
    }

    /**
     * Custom QueryableStoreType for querying TimestampedKeyValueStoreWithHeaders directly
     * without facade wrapping. This returns the full ValueTimestampHeaders wrapper.
     */
    private static class TimestampedKeyValueStoreWithHeadersType<K, V>
        implements QueryableStoreType<ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>>> {

        @Override
        public boolean accepts(final StateStore stateStore) {
            return stateStore instanceof TimestampedKeyValueStoreWithHeaders
                && stateStore instanceof ReadOnlyKeyValueStore;
        }

        @Override
        public ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> create(
            final StateStoreProvider storeProvider,
            final String storeName) {
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }
    }
}
