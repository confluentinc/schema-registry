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
import java.util.stream.Collectors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link TimestampedKeyValueStoreWithHeaders} that verifies store operations
 * (put, get, delete, putIfAbsent, putAll) work correctly with header-based schema ID transport.
 * All store operations are performed inside a processor.
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
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    keySerde,
                    valueSerde))
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .process(() -> new WordCountProcessor(STORE_NAME), STORE_NAME)
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "kv-store-integration-test");

            GenericRecord helloKey = createKey("hello");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // Test 1: PUT - put hello:1
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, createValue(1L, "PUT"))).get();

                // Test 2: PUT - put hello:1 again, aggregates to hello:2
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, createValue(1L, "PUT"))).get();

                // Test 3: PUT_IF_ABSENT - should not overwrite existing "hello"
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, createValue(100L, "PUT_IF_ABSENT"))).get();

                // Test 4: PUT_IF_ABSENT - new key "world" should be inserted
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("world"), createValue(50L, "PUT_IF_ABSENT"))).get();

                // Test 5: DELETE - delete "hello"
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, createValue(0L, "DELETE"))).get();

                // Test 6: DELETE - delete non-existing key (should produce no output)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("non-existing"), createValue(0L, "DELETE"))).get();

                // Test 7: PUT_ALL - batch insert 3 words (hardcoded in processor) - at end since temporarily not supported
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("trigger"), createValue(0L, "PUT_ALL"))).get();

                producer.flush();
            }

            // Expect 8 records: PUT(1) + PUT(2) + PUT_IF_ABSENT(existing) + PUT_IF_ABSENT(new) + DELETE + PUT_ALL(3)
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(OUTPUT_TOPIC, "kv-store-test-consumer", 8);

            assertEquals(8, results.size(), "Should have 8 output records");

            // Verify PUT 1
            assertEquals("hello", results.get(0).key().get("word").toString());
            assertEquals(1L, results.get(0).value().get("count"));
            assertSchemaIdHeaders(results.get(0), "PUT 1");

            // Verify PUT 1+1
            assertEquals("hello", results.get(1).key().get("word").toString());
            assertEquals(2L, results.get(1).value().get("count"));
            assertSchemaIdHeaders(results.get(1), "PUT 2 (aggregated)");

            // Verify PUT_IF_ABSENT existing
            assertEquals("hello", results.get(2).key().get("word").toString());
            assertEquals(2L, results.get(2).value().get("count"));
            assertSchemaIdHeaders(results.get(2), "PUT_IF_ABSENT existing");

            // Verify PUT_IF_ABSENT new key
            assertEquals("world", results.get(3).key().get("word").toString());
            assertEquals(50L, results.get(3).value().get("count"));
            assertSchemaIdHeaders(results.get(3), "PUT_IF_ABSENT new");

            // Verify DELETE
            assertEquals("hello", results.get(4).key().get("word").toString());
            assertEquals(2L, results.get(4).value().get("count"));
            assertSchemaIdHeaders(results.get(4), "DELETE");

            // Verify PUT_ALL 3 words
            assertEquals("word1", results.get(5).key().get("word").toString());
            assertEquals(25L, results.get(5).value().get("count"));
            assertSchemaIdHeaders(results.get(5), "PUT_ALL word1");

            assertEquals("word2", results.get(6).key().get("word").toString());
            assertEquals(50L, results.get(6).value().get("count"));
            assertSchemaIdHeaders(results.get(6), "PUT_ALL word2");

            assertEquals("word3", results.get(7).key().get("word").toString());
            assertEquals(75L, results.get(7).value().get("count"));
            assertSchemaIdHeaders(results.get(7), "PUT_ALL word3");

            // Query store via IQv1 to verify final state
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore()));

            assertNotNull(store, "Store should be accessible via IQv1");

            // Verify DELETE: "hello" should not exist (was deleted)
            ValueTimestampHeaders<GenericRecord> helloResult = store.get(helloKey);
            assertTrue(helloResult == null || helloResult.value() == null,
                "IQv1: hello should not exist after delete");

            // Verify PUT_IF_ABSENT: "world" should exist with count 50
            ValueTimestampHeaders<GenericRecord> worldResult = store.get(createKey("world"));
            assertNotNull(worldResult, "IQv1: world should exist in store");
            assertEquals(50L, worldResult.value().get("count"), "IQv1: world count should be 50");
            assertSchemaIdHeaders(worldResult.headers(), "IQv1 world");

            // Verify PUT_ALL: all 3 word entries should exist
            ValueTimestampHeaders<GenericRecord> batch1Result = store.get(createKey("word1"));
            assertNotNull(batch1Result, "IQv1: word1 should exist in store");
            assertEquals(25L, batch1Result.value().get("count"), "IQv1: word1 count should be 25");
            assertSchemaIdHeaders(batch1Result.headers(), "IQv1 word1");

            ValueTimestampHeaders<GenericRecord> batch2Result = store.get(createKey("word2"));
            assertNotNull(batch2Result, "IQv1: word2 should exist in store");
            assertEquals(50L, batch2Result.value().get("count"), "IQv1: word2 count should be 50");
            assertSchemaIdHeaders(batch2Result.headers(), "IQv1 word2");

            ValueTimestampHeaders<GenericRecord> batch3Result = store.get(createKey("word3"));
            assertNotNull(batch3Result, "IQv1: word3 should exist in store");
            assertEquals(75L, batch3Result.value().get("count"), "IQv1: word3 count should be 75");
            assertSchemaIdHeaders(batch3Result.headers(), "IQv1 word3");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Test ReadOnlyKeyValueStore operations (all, reverseAll, range, reverseRange, prefixScan, approximateNumEntries)
     */
    @Test
    @Disabled
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
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                    keySerde,
                    valueSerde))
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new IteratorTestProcessor(storeName), storeName)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "iterator-integration-test");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                for (int i = 1; i <= 5; i++) {
                    producer.send(new ProducerRecord<>(inputTopic,
                        createKey("word-" + i), createValue(i * 10L, "PUT_SIMPLE"))).get();
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
            // PUT_SIMPLE: 5
            // ALL: 5
            // REVERSE_ALL: 5
            // RANGE (word-2 to word-4): 2 (word-2, word-3)
            // REVERSE_RANGE: 2 (word-3, word-2)
            // PREFIX_SCAN: 5
            // APPROXIMATE_NUM_ENTRIES: 1
            // ITERATOR_METHODS: 1
            // Total: 26
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "iterator-test-consumer", 26);

            assertEquals(26, results.size(), "Should have 26 output records");

            int idx = 0;

            // Verify PUT_SIMPLE results (5 entries)
            for (int i = 1; i <= 5; i++) {
                assertEquals("word-" + i, results.get(idx).key().get("word").toString());
                assertEquals(i * 10L, results.get(idx).value().get("count"));
                assertSchemaIdHeaders(results.get(idx), "PUT_SIMPLE word-" + i);
                idx++;
            }

            // Verify ALL results (5 entries, sorted order)
            for (int i = 1; i <= 5; i++) {
                assertEquals("word-" + i, results.get(idx).key().get("word").toString());
                assertSchemaIdHeaders(results.get(idx), "ALL word-" + i);
                idx++;
            }

            // Verify REVERSE_ALL results (5 entries, reverse order)
            for (int i = 5; i >= 1; i--) {
                assertEquals("word-" + i, results.get(idx).key().get("word").toString());
                assertSchemaIdHeaders(results.get(idx), "REVERSE_ALL word-" + i);
                idx++;
            }

            // Verify RANGE results (word-2 to word-4, exclusive end = word-2, word-3)
            assertEquals("word-2", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "RANGE word-2");
            assertEquals("word-3", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "RANGE word-3");

            // Verify REVERSE_RANGE results (word-3, word-2)
            assertEquals("word-3", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "REVERSE_RANGE word-3");
            assertEquals("word-2", results.get(idx).key().get("word").toString());
            assertSchemaIdHeaders(results.get(idx++), "REVERSE_RANGE word-2");

            // Verify PREFIX_SCAN results (5 entries with prefix "word-")
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
                    StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

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
                assertEquals(Arrays.asList("word-2", "word-3"), rangeKeys,
                    "IQv1 range(word-2, word-4) should return word-2, word-3");
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
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
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

    @Test
    public void shouldReturnNullForNonExistentKey() throws Exception {
        String inputTopic = "nonexistent-input";
        String outputTopic = "nonexistent-output";
        String storeName = "nonexistent-store";

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                    keySerde,
                    valueSerde))
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new GetNonExistentProcessor(storeName), storeName)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "nonexistent-integration-test");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("nonexistent"), createValue(0L, "GET_NONEXISTENT"))).get();
                producer.flush();
            }

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "nonexistent-consumer", 1);

            assertEquals(1, results.size(), "Should have 1 output record");
            assertEquals(1L, results.get(0).value().get("count"),
                "get() on non-existent key should return null");

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
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

        public IteratorTestProcessor(String storeName) {
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
                case "PUT_SIMPLE":
                    handlePutSimple(record);
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

        private void handlePutSimple(Record<GenericRecord, GenericRecord> record) {
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

            Serializer<GenericRecord> prefixSerializer = (topic, data) -> {
                if (data == null) return null;
                return data.get("word").toString().getBytes();
            };

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
     * Processor for testing get on non-existent key.
     */
    private static class GetNonExistentProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

        public GetNonExistentProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            if ("GET_NONEXISTENT".equals(record.value().get("operation").toString())) {
                ValueTimestampHeaders<GenericRecord> result = store.get(record.key());
                long checkResult = (result == null) ? 1L : 0L;

                GenericRecord resultValue = new GenericData.Record(record.value().getSchema());
                resultValue.put("count", checkResult);
                resultValue.put("operation", "RESULT");

                context.forward(new Record<>(record.key(), resultValue, record.timestamp(), record.headers()));
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

            // Hardcoded batch entries: batch1:25, batch2:50, batch3:75
            String[] words = {"word1", "word2", "word3"};
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
        assertTrue(startedLatch.await(30, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");
        return streams;
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
}