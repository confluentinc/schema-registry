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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.HashMap;
import java.util.List;
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
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link TimestampedKeyValueStoreWithHeaders} using {@link TopologyTestDriver}
 * with {@link MockSchemaRegistryClient}. Tests store APIs: put, get, delete, putIfAbsent.
 */
public class TimestampedKeyValueStoreWithHeadersUnitTest {

    private static final String APPLICATION_ID = "word-count-unit-test";
    private static final String INPUT_TOPIC = "word-plaintext-input";
    private static final String OUTPUT_TOPIC = "word-count-output";
    private static final String STORE_NAME = "words-store";
    private static final String CHANGELOG_TOPIC = APPLICATION_ID + "-" + STORE_NAME + "-changelog";

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

    private TopologyTestDriver testDriver;
    private TestInputTopic<GenericRecord, GenericRecord> inputTopic;
    private TestOutputTopic<GenericRecord, GenericRecord> outputTopic;
    private TestOutputTopic<GenericRecord, GenericRecord> changelogTopic;
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock-server:9092");

        testDriver = new TopologyTestDriver(builder.build(), props);

        // Create test topics
        inputTopic = testDriver.createInputTopic(
            INPUT_TOPIC, keySerde.serializer(), valueSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
            OUTPUT_TOPIC, keySerde.deserializer(), valueSerde.deserializer());
        changelogTopic = testDriver.createOutputTopic(
            CHANGELOG_TOPIC, keySerde.deserializer(), valueSerde.deserializer());
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
        helloValue.put("operation", "PUT");
        inputTopic.pipeInput(helloKey, helloValue);

        // Send "world" with count 2
        GenericRecord worldKey = new GenericData.Record(keySchema);
        worldKey.put("word", "world");
        GenericRecord worldValue = new GenericData.Record(valueSchema);
        worldValue.put("count", 2L);
        worldValue.put("operation", "PUT");
        inputTopic.pipeInput(worldKey, worldValue);

        // Send "world" with count 2 again
        inputTopic.pipeInput(worldKey, worldValue);

        // Verify 3 output records
        assertEquals(3, outputTopic.getQueueSize());

        // Verify store state and headers
        TestRecord<GenericRecord, GenericRecord> putRecord1 = outputTopic.readRecord();
        assertEquals("hello", putRecord1.key().get("word").toString(), "first record should be for 'hello'");
        assertEquals(1L, putRecord1.value().get("count"), "first record count should be 1");
        assertSchemaIdHeadersOnRecord(putRecord1, "after processing 'hello'");

        TestRecord<GenericRecord, GenericRecord> putRecord2 = outputTopic.readRecord();
        assertEquals("world", putRecord2.key().get("word").toString(), "second record should be for 'world'");
        assertEquals(2L, putRecord2.value().get("count"), "second record count should be 2");
        assertSchemaIdHeadersOnRecord(putRecord2, "after processing 'world'");

        TestRecord<GenericRecord, GenericRecord> putRecord3 = outputTopic.readRecord();
        assertEquals("world", putRecord3.key().get("word").toString(), "third record should be for 'world'");
        assertEquals(4L, putRecord3.value().get("count"), "third record count should be 4");
        assertSchemaIdHeadersOnRecord(putRecord3, "after processing 'world'");
    }

    @Test
    void shouldDeleteWithHeaders() {
        // Put a word
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", "deleted-word");
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("count", 5L);
        value.put("operation", "PUT");
        inputTopic.pipeInput(key, value);

        // Verify PUT output and headers
        TestRecord<GenericRecord, GenericRecord> putRecord = outputTopic.readRecord();
        assertEquals(5L, putRecord.value().get("count"), "initial count should be 5");
        assertSchemaIdHeadersOnRecord(putRecord, "after put()");

        // delete an existing key
        GenericRecord deleteValue = new GenericData.Record(valueSchema);
        deleteValue.put("count", 0L); // value is ignored for delete, but we need to provide a valid record
        deleteValue.put("operation", "DELETE");
        inputTopic.pipeInput(key, deleteValue);

        TestRecord<GenericRecord, GenericRecord> deleteRecord = outputTopic.readRecord();
        assertEquals(5L, deleteRecord.getValue().get("count"), "Should return deleted value");
        assertSchemaIdHeadersOnRecord(deleteRecord, "after delete() on existing key");

        // delete a non-existing key
        GenericRecord nonExistingKey = new GenericData.Record(keySchema);
        nonExistingKey.put("word", "non-existing-word");
        inputTopic.pipeInput(nonExistingKey, deleteValue);

        assertTrue(outputTopic.isEmpty(), "Deleting non-existing key should not produce output");
    }

    @Test
    void shouldPutIfAbsentWithHeaders() {
        // Put initial word
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", "existing-word");
        GenericRecord value1 = new GenericData.Record(valueSchema);
        value1.put("count", 10L);
        value1.put("operation", "PUT");
        inputTopic.pipeInput(key, value1);
        outputTopic.readRecord();

        // put_if_absent on existing key
        GenericRecord putIfAbsentValue = new GenericData.Record(valueSchema);
        putIfAbsentValue.put("count", 20L);
        putIfAbsentValue.put("operation", "PUT_IF_ABSENT");
        inputTopic.pipeInput(key, putIfAbsentValue);

        // Verify old value not overwritten
        TestRecord<GenericRecord, GenericRecord> outputRecord = outputTopic.readRecord();
        assertEquals(10L, outputRecord.getValue().get("count"), "Should return existing value");
        assertSchemaIdHeadersOnRecord(outputRecord, "after putIfAbsent() on existing key");

        // put_if_absent on non-existing key
        GenericRecord newKey = new GenericData.Record(keySchema);
        newKey.put("word", "new-word");
        GenericRecord newValue = new GenericData.Record(valueSchema);
        newValue.put("count", 30L);
        newValue.put("operation", "PUT_IF_ABSENT");
        inputTopic.pipeInput(newKey, newValue);

        // Verify new value inserted
        TestRecord<GenericRecord, GenericRecord> newOutput = outputTopic.readRecord();
        assertEquals(30L, newOutput.getValue().get("count"), "Should insert new value");
        assertSchemaIdHeadersOnRecord(newOutput, "after putIfAbsent() on new key");
    }

    @Test
    void shouldWriteHeadersToChangelogTopic() {
        // Put a word
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", "changelog-word");
        GenericRecord value1 = new GenericData.Record(valueSchema);
        value1.put("count", 5L);
        value1.put("operation", "PUT");
        inputTopic.pipeInput(key, value1);
        GenericRecord value2 = new GenericData.Record(valueSchema);
        value2.put("count", 7L);
        value2.put("operation", "PUT");
        inputTopic.pipeInput(key, value2);

        List<TestRecord<GenericRecord, GenericRecord>> changelogRecords = changelogTopic.readRecordsToList();

        assertEquals(2, changelogRecords.size(), "changelog should have 2 records");

        // Verify first record value
        TestRecord<GenericRecord, GenericRecord> changelogRecord1 = changelogRecords.get(0);
        assertEquals(5L, changelogRecord1.value().get("count"));
        // Verify headers on first changelog record
        assertNotNull(changelogRecord1.headers(), "first changelog record should have headers");
        Header keyHeader1 = changelogRecord1.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keyHeader1, "first changelog record should have __key_schema_id header");
        assertSchemaIdHeadersOnRecord(changelogRecord1, "first changelog record");

        // Verify second record value
        TestRecord<GenericRecord, GenericRecord> changelogRecord2 = changelogRecords.get(1);
        assertEquals(12L, changelogRecord2.value().get("count"));
        // Verify headers on second changelog record
        assertNotNull(changelogRecord2.headers(), "second changelog record should have headers");
        assertSchemaIdHeadersOnRecord(changelogRecord2, "second changelog record");
    }

    /**
     * Verify that the stored record has both __key_schema_id and __value_schema_id headers are present,
     * with V1 magic byte (GUID format, 17 bytes)
     */
    private void assertSchemaIdHeadersOnRecord(TestRecord<GenericRecord, GenericRecord> record, String context) {
        assertNotNull(record.headers(), context + ": headers should not be null");

        Header keyHeader = record.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keyHeader, context + ": should have __key_schema_id header");
        assertEquals(17, keyHeader.value().length, context + ": key GUID should be 17 bytes");

        Header valueHeader = record.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
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
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record) {
           // Get existing count
            ValueTimestampHeaders<GenericRecord> existingRecord = store.get(record.key());

            // Aggregate count
            long newCount = (Long) record.value().get("count");
            if (existingRecord != null && existingRecord.value() != null) {
                newCount += (Long) existingRecord.value().get("count");
            }

            // Create updated value
            GenericRecord updatedValue = new GenericData.Record(record.value().getSchema());
            updatedValue.put("count", newCount);
            updatedValue.put("operation", "PUT");

            // Store with headers preserved
            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(updatedValue, record.timestamp(), record.headers());
            store.put(record.key(), toStore);

            // Forward result
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

            if (previousRecord != null) { // Key already exists, return existing record
                context.forward(new Record<>(
                    record.key(), previousRecord.value(), previousRecord.timestamp(), previousRecord.headers()));
            } else { // Key inserted, forward the new record
                ValueTimestampHeaders<GenericRecord> insertedRecord = store.get(record.key());
                context.forward(new Record<>(
                    record.key(), insertedRecord.value(), insertedRecord.timestamp(), insertedRecord.headers()));
            }
        }
    }
}
