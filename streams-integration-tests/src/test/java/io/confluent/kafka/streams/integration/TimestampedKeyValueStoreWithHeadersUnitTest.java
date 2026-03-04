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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link TimestampedKeyValueStoreWithHeaders} using {@link TopologyTestDriver}
 * with {@link MockSchemaRegistryClient}. Tests store APIs: put, get, delete, putIfAbsent.
 */
public class TimestampedKeyValueStoreWithHeadersUnitTest {

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
            + "  {\"name\":\"count\",\"type\":\"long\"}"
            + "]"
            + "}";

    private TopologyTestDriver testDriver;
    private TestInputTopic<GenericRecord, GenericRecord> inputTopic;
    private TestOutputTopic<GenericRecord, GenericRecord> outputTopic;
    private SchemaRegistryClient mockSchemaRegistry;
    private Schema keySchema;
    private Schema valueSchema;

    @BeforeEach
    void setup() {
        mockSchemaRegistry = new MockSchemaRegistryClient();

        keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
        valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

        // Create serdes with mock SR and header-based schema ID
        GenericAvroSerde keySerde = createSerde(true);
        GenericAvroSerde valueSerde = createSerde(false);

        // Build topology with TimestampedKeyValueStoreWithHeaders
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    keySerde,
                    valueSerde))
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .process(WordCountProcessor::new, STORE_NAME)
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        // Configure test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-unit-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock-server:9092");

        testDriver = new TopologyTestDriver(builder.build(), props);

        // Create test topics
        inputTopic = testDriver.createInputTopic(
            INPUT_TOPIC, keySerde.serializer(), valueSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
            OUTPUT_TOPIC, keySerde.deserializer(), valueSerde.deserializer());
    }

    @AfterEach
    void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    private GenericAvroSerde createSerde(boolean isKey) {
        GenericAvroSerde serde = new GenericAvroSerde(mockSchemaRegistry);
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test");
        config.put(
            isKey ? AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER
                : AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, isKey);
        return serde;
    }

    @Test
    void shouldStoreAndForwardWordCounts() {
        // Send "hello" with count 1
        GenericRecord helloKey = new GenericData.Record(keySchema);
        helloKey.put("word", "hello");
        GenericRecord helloValue = new GenericData.Record(valueSchema);
        helloValue.put("count", 1L);
        inputTopic.pipeInput(helloKey, helloValue);

        // Send "world" with count 2
        GenericRecord worldKey = new GenericData.Record(keySchema);
        worldKey.put("word", "world");
        GenericRecord worldValue = new GenericData.Record(valueSchema);
        worldValue.put("count", 2L);
        inputTopic.pipeInput(worldKey, worldValue);

        // Send "world" with count 2 again
        inputTopic.pipeInput(worldKey, worldValue);

        // Verify 3 output records
        assertEquals(3, outputTopic.getQueueSize());

        // Verify word counts from output
        Map<String, Long> wordCounts = new HashMap<>();
        while (!outputTopic.isEmpty()) {
            KeyValue<GenericRecord, GenericRecord> record = outputTopic.readKeyValue();
            String word = record.key.get("word").toString();
            Long count = (Long) record.value.get("count");
            wordCounts.put(word, count);
        }
        assertEquals(1L, wordCounts.get("hello"), "hello should have count 1");
        assertEquals(4L, wordCounts.get("world"), "world should have count 4");

        // Verify store state and headers
        KeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
            testDriver.getTimestampedKeyValueStoreWithHeaders(STORE_NAME);

        ValueTimestampHeaders<GenericRecord> helloStored = store.get(helloKey);
        assertNotNull(helloStored, "hello should be in store");
        assertEquals(1L, helloStored.value().get("count"));
        assertNotNull(helloStored.headers(), "hello should have headers preserved");
        assertSchemaIdHeaders(helloStored, "hello after put()");

        ValueTimestampHeaders<GenericRecord> worldStored = store.get(worldKey);
        assertNotNull(worldStored, "world should be in store");
        assertEquals(4L, worldStored.value().get("count"));
        assertNotNull(worldStored.headers(), "world should have headers preserved");
        assertSchemaIdHeaders(worldStored, "world after put()");
    }

    @Test
    void shouldDeleteWithHeaders() {
        // Put a word
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", "deleted-word");
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("count", 5L);
        inputTopic.pipeInput(key, value);

        // Access store and test delete
        KeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
            testDriver.getTimestampedKeyValueStoreWithHeaders(STORE_NAME);

        // Verify it exists
        assertNotNull(store.get(key), "key should exist before delete");

        // Delete value and verify returned deleted value
        ValueTimestampHeaders<GenericRecord> deleted = store.delete(key);
        assertNotNull(deleted, "delete() should return previous value");
        assertEquals(5L, deleted.value().get("count"));
        assertNotNull(deleted.headers(), "deleted value");
        // Verify headers present in delete return value
        assertSchemaIdHeaders(deleted, "delete() return");

        // Verify deleted
        assertNull(store.get(key), "get() after delete() should return null");
    }

    @Test
    void shouldPutIfAbsentWithHeaders() {
        // Put initial word
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", "existing-word");
        GenericRecord value1 = new GenericData.Record(valueSchema);
        value1.put("count", 10L);
        inputTopic.pipeInput(key, value1);

        KeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
            testDriver.getTimestampedKeyValueStoreWithHeaders(STORE_NAME);

        // Test putIfAbsent of existing key
        GenericRecord value2 = new GenericData.Record(valueSchema);
        value2.put("count", 20L);
        ValueTimestampHeaders<GenericRecord> insertedValue =
            ValueTimestampHeaders.make(value2, System.currentTimeMillis(), null);

        ValueTimestampHeaders<GenericRecord> previous = store.putIfAbsent(key, insertedValue);

        assertNotNull(previous, "putIfAbsent() should return existing value");
        assertEquals(10L, previous.value().get("count"));
        assertSchemaIdHeaders(previous, "putIfAbsent() return value");

        // Verify old value not overwritten
        ValueTimestampHeaders<GenericRecord> stored = store.get(key);
        assertEquals(10L, stored.value().get("count"),
            "putIfAbsent() should not overwrite existing value");
        // Verify headers still present
        assertSchemaIdHeaders(stored, "after putIfAbsent() existing key");
    }

    @Test
    void shouldPutIfAbsentOnNewKey() {
        // Put a record
        GenericRecord existingKey = new GenericData.Record(keySchema);
        existingKey.put("word", "existing-word");
        GenericRecord existingValue = new GenericData.Record(valueSchema);
        existingValue.put("count", 1L);
        inputTopic.pipeInput(existingKey, existingValue);

        KeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
            testDriver.getTimestampedKeyValueStoreWithHeaders(STORE_NAME);

        // Get headers from the existing record to use for putIfAbsent
        ValueTimestampHeaders<GenericRecord> existingStored = store.get(existingKey);
        assertSchemaIdHeaders(existingStored, "existing record before putIfAbsent()");

        // Test putIfAbsent on new key
        GenericRecord newKey = new GenericData.Record(keySchema);
        newKey.put("word", "new-word");
        GenericRecord newValue = new GenericData.Record(valueSchema);
        newValue.put("count", 99L);
        ValueTimestampHeaders<GenericRecord> insertedValue =
            ValueTimestampHeaders.make(newValue, System.currentTimeMillis(), existingStored.headers());
        ValueTimestampHeaders<GenericRecord> previous = store.putIfAbsent(newKey, insertedValue);
        assertNull(previous, "putIfAbsent() should return null for new key");

        // Verify inserted new value
        ValueTimestampHeaders<GenericRecord> stored = store.get(newKey);
        assertNotNull(stored, "new key should be in store");
        assertEquals(99L, stored.value().get("count"));
        // Verify headers preserved
        assertSchemaIdHeaders(stored, "putIfAbsent() should preserve headers");
    }

    /**
     * Verify that the stored record has both __key_schema_id and __value_schema_id headers are present,
     * with V1 magic byte (GUID format, 17 bytes)
     */
    private void assertSchemaIdHeaders(ValueTimestampHeaders<GenericRecord> stored, String context) {
        assertNotNull(stored, context + ": stored value should not be null");
        assertNotNull(stored.headers(), context + ": headers should not be null");

        Header keyHeader = stored.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keyHeader, context + ": should have __key_schema_id header");
        assertEquals(17, keyHeader.value().length, context + ": key GUID should be 17 bytes");

        Header valueHeader = stored.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
        assertNotNull(valueHeader, context + ": should have __value_schema_id header");
        assertEquals(17, valueHeader.value().length, context + ": value GUID should be 17 bytes");
    }

    /**
     * Processor that aggregates word counts and stores in TimestampedKeyValueStoreWithHeaders.
     * Preserves headers through the state store.
     */
    private static class WordCountProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            // Get existing count
            ValueTimestampHeaders<GenericRecord> existing = store.get(record.key());

            // Aggregate count
            long newCount = (Long) record.value().get("count");
            if (existing != null && existing.value() != null) {
                newCount += (Long) existing.value().get("count");
            }

            // Create updated value
            GenericRecord updatedValue = new GenericData.Record(record.value().getSchema());
            updatedValue.put("count", newCount);

            // Store with headers preserved
            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(updatedValue, record.timestamp(), record.headers());
            store.put(record.key(), toStore);

            // Forward result
            ValueTimestampHeaders<GenericRecord> stored = store.get(record.key());
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }
    }
}
