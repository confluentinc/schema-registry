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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies Kafka Streams with {@link TimestampedKeyValueStoreWithHeaders}
 * works correctly with {@link GenericAvroSerde} configured to use {@link HeaderSchemaIdSerializer}
 * for header-based schema ID transport. Both keys and values use Avro with header-based schema IDs.
 *
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
            + "  {\"name\":\"count\",\"type\":\"long\"}"
            + "]"
            + "}";

    public TimestampedKeyValueStoreWithHeadersIntegrationTest() {
        super(1, true);
    }

    @Test
    public void shouldStoreAndForwardWithSchemaIdHeaders() throws Exception {
        Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
        Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

        // Create topics
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin
                .createTopics(
                    Arrays.asList(
                        new NewTopic(INPUT_TOPIC, 1, (short) 1),
                        new NewTopic(OUTPUT_TOPIC, 1, (short) 1)))
                .all()
                .get(30, TimeUnit.SECONDS);
        }

        // Configure serdes with header-based schema ID
        Map<String, Object> keySerdeConfig = new HashMap<>();
        keySerdeConfig.put(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        keySerdeConfig.put(
            AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());

        GenericAvroSerde keySerde = new GenericAvroSerde();
        keySerde.configure(keySerdeConfig, true);

        Map<String, Object> valueSerdeConfig = new HashMap<>();
        valueSerdeConfig.put(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        valueSerdeConfig.put(
            AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());

        GenericAvroSerde valueSerde = new GenericAvroSerde();
        valueSerde.configure(valueSerdeConfig, false);

        // Build topology using PAPI with TimestampedKeyValueStoreWithHeaders
        StreamsBuilder builder = new StreamsBuilder();

        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    keySerde,
                    valueSerde))
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .process(() -> new HeadersStoreProcessor(), STORE_NAME)
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        Properties streamsProps = new Properties();
        streamsProps.put(
            StreamsConfig.APPLICATION_ID_CONFIG, "timestamped-kv-headers-integration-test");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        streamsProps.put(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);

        KafkaStreams streams = null;
        try {
            CountDownLatch startedLatch = new CountDownLatch(1);
            streams = new KafkaStreams(builder.build(), streamsProps);
            streams.cleanUp();
            streams.setStateListener(
                (newState, oldState) -> {
                    if (newState == KafkaStreams.State.RUNNING) {
                        startedLatch.countDown();
                    }
                });
            streams.start();
            assertTrue(
                startedLatch.await(30, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");

            // Produce test records with header-based schema IDs
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            producerProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            producerProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            producerProps.put(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
            producerProps.put(
                AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
                HeaderSchemaIdSerializer.class.getName());
            producerProps.put(
                AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
                HeaderSchemaIdSerializer.class.getName());

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(producerProps)) {
                // Send "hello" with count 1
                GenericRecord helloKey = new GenericData.Record(keySchema);
                helloKey.put("word", "hello");
                GenericRecord helloValue = new GenericData.Record(valueSchema);
                helloValue.put("count", 1L);
                producer.send(new ProducerRecord<>(INPUT_TOPIC, helloKey, helloValue)).get();

                // Send "world" with count 2
                GenericRecord worldKey = new GenericData.Record(keySchema);
                worldKey.put("word", "world");
                GenericRecord worldValue = new GenericData.Record(valueSchema);
                worldValue.put("count", 2L);
                producer.send(new ProducerRecord<>(INPUT_TOPIC, worldKey, worldValue)).get();

                // Send "world" with count 2 again to verify multiple records with same key work
                producer.send(new ProducerRecord<>(INPUT_TOPIC, worldKey, worldValue)).get();

                producer.flush();
            }

            // Consume and verify
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "headers-store-test-consumer");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            consumerProps.put(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
            consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

            List<ConsumerRecord<GenericRecord, GenericRecord>> results = new ArrayList<>();
            try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
                     new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

                long deadline = System.currentTimeMillis() + 30_000;
                while (results.size() < 3 && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<GenericRecord, GenericRecord> records =
                        consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                        results.add(record);
                    }
                }
            }

            // Verify record number
            assertEquals(3, results.size(), "Should have 3 output records");

            // Verify data
            Map<String, Long> wordCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                String word = record.key().get("word").toString();
                Long count = (Long) record.value().get("count");
                wordCounts.put(word, count);
            }
            assertEquals(1L, wordCounts.get("hello"), "hello should have count 1");
            assertEquals(4L, wordCounts.get("world"), "world should have count 4");

            // Verify __key_schema_id header is present with V1 magic byte (GUID format, 17 bytes)
            ConsumerRecord<GenericRecord, GenericRecord> lastRecord = results.get(results.size() - 1);

            Header keySchemaIdHeader = lastRecord.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
            assertNotNull(keySchemaIdHeader, "Output record should have __key_schema_id header");
            byte[] keyHeaderBytes = keySchemaIdHeader.value();
            assertEquals(17, keyHeaderBytes.length, "Key GUID header should be 17 bytes");
            assertEquals(
                SchemaId.MAGIC_BYTE_V1,
                keyHeaderBytes[0],
                "Key header should start with V1 magic byte for GUID format");

            // Verify __value_schema_id header is present with V1 magic byte (GUID format, 17 bytes)
            Header valueSchemaIdHeader =
                lastRecord.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
            assertNotNull(valueSchemaIdHeader, "Output record should have __value_schema_id header");
            byte[] valueHeaderBytes = valueSchemaIdHeader.value();
            assertEquals(17, valueHeaderBytes.length, "Value GUID header should be 17 bytes");
            assertEquals(
                SchemaId.MAGIC_BYTE_V1,
                valueHeaderBytes[0],
                "Value header should start with V1 magic byte for GUID format");

        } finally {
            if (streams != null) {
                streams.close(Duration.ofSeconds(10));
            }
        }
    }

    /**
     * Processor that stores records in TimestampedKeyValueStoreWithHeaders and forwards them.
     * This preserves headers (including schema ID headers) through the state store.
     */
    private static class HeadersStoreProcessor
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
            ValueTimestampHeaders<GenericRecord> existingRecord = store.get(record.key());

            long newCount = (Long) record.value().get("count");
            if (existingRecord != null && existingRecord.value() != null) {
                newCount += (Long) existingRecord.value().get("count");
            }
            GenericRecord updatedValue = new GenericData.Record(record.value().getSchema());
            updatedValue.put("count", newCount);

            ValueTimestampHeaders<GenericRecord> valueTimestampHeaders = ValueTimestampHeaders.make(updatedValue, record.timestamp(), record.headers());
            store.put(record.key(), valueTimestampHeaders);

            ValueTimestampHeaders<GenericRecord> stored = store.get(record.key());
            if (stored != null) {
                context.forward(
                    new Record<>(
                        record.key(),
                        stored.value(),
                        stored.timestamp(),
                        stored.headers()));
            }
        }
    }
}
