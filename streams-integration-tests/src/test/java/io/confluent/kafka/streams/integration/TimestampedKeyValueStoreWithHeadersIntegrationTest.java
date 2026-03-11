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
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
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

                // Test 1: put word:1
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, createValue(1L, "PUT"))).get();

                // Test 2: put wor:1, aggregates to word:2
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, createValue(1L, "PUT"))).get();

                // Test 3: PUT_IF_ABSENT - should not overwrite existing "hello"
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, createValue(100L, "PUT_IF_ABSENT"))).get();

                // Test 4: PUT_IF_ABSENT - new key "world" should be inserted
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("world"), createValue(50L, "PUT_IF_ABSENT"))).get();

                // Test 5: PUT_ALL - batch insert 3 words (hardcoded in processor)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("batch-word"), createValue(0L, "PUT_ALL"))).get();

                // Test 6: DELETE - delete "hello"
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, createValue(0L, "DELETE"))).get();

                // Test 7: DELETE - delete non-existing key (should produce no output)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, createKey("non-existing"), createValue(0L, "DELETE"))).get();

                producer.flush();
            }

            // Expect 8 records: PUT(1) + PUT(2) + PUT_IF_ABSENT(existing) + PUT_IF_ABSENT(new) + PUT_ALL(3) + DELETE
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

            // Verify PUT_ALL 3 batch entries
            assertEquals("batch1", results.get(4).key().get("word").toString());
            assertEquals(25L, results.get(4).value().get("count"));
            assertSchemaIdHeaders(results.get(4), "PUT_ALL batch1");

            assertEquals("batch2", results.get(5).key().get("word").toString());
            assertEquals(50L, results.get(5).value().get("count"));
            assertSchemaIdHeaders(results.get(5), "PUT_ALL batch2");

            assertEquals("batch3", results.get(6).key().get("word").toString());
            assertEquals(75L, results.get(6).value().get("count"));
            assertSchemaIdHeaders(results.get(6), "PUT_ALL batch3");

            // Verify DELETE
            assertEquals("hello", results.get(7).key().get("word").toString());
            assertEquals(2L, results.get(7).value().get("count"));
            assertSchemaIdHeaders(results.get(7), "DELETE");

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

            // Verify PUT_ALL: all 3 batch entries should exist
            ValueTimestampHeaders<GenericRecord> batch1Result = store.get(createKey("batch1"));
            assertNotNull(batch1Result, "IQv1: batch1 should exist in store");
            assertEquals(25L, batch1Result.value().get("count"), "IQv1: batch1 count should be 25");
            assertSchemaIdHeaders(batch1Result.headers(), "IQv1 batch1");

            ValueTimestampHeaders<GenericRecord> batch2Result = store.get(createKey("batch2"));
            assertNotNull(batch2Result, "IQv1: batch2 should exist in store");
            assertEquals(50L, batch2Result.value().get("count"), "IQv1: batch2 count should be 50");
            assertSchemaIdHeaders(batch2Result.headers(), "IQv1 batch2");

            ValueTimestampHeaders<GenericRecord> batch3Result = store.get(createKey("batch3"));
            assertNotNull(batch3Result, "IQv1: batch3 should exist in store");
            assertEquals(75L, batch3Result.value().get("count"), "IQv1: batch3 count should be 75");
            assertSchemaIdHeaders(batch3Result.headers(), "IQv1 batch3");

        } finally {
            closeStreams(streams);
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

        // TODO: Test on range(), reverseRange(), all(), reverseAll(),  prefixScan() once those are supported by TimestampedKeyValueStoreWithHeaders
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
            String[] words = {"batch1", "batch2", "batch3"};
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