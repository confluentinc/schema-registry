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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import java.util.stream.Collectors;
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Shared infrastructure for the KIP-1356 headers-aware IQv2 integration tests. Holds the boilerplate
 * that would otherwise be copy-pasted across every store-type-specific test: topic creation,
 * header-mode Avro serdes, a {@link KafkaStreams} start/await/close lifecycle, an output-topic
 * completion-barrier consumer, and the schema-id GUID header assertion.
 *
 * <p>Every concrete test produces records whose schema-id GUID is carried in the
 * {@code __key_schema_id} / {@code __value_schema_id} headers (via {@link HeaderSchemaIdSerializer}).
 * When a record is produced through {@link #sendAndCapture}, the exact GUID bytes the serializer wrote
 * are captured, so {@link #assertSchemaIdHeaders} can assert that the bytes returned by an IQv2 query
 * are byte-for-byte the GUID that was produced -- not merely a well-formed one.
 *
 * <p>Runs against a real 1-broker cluster + embedded Schema Registry ({@code super(1, true)}).
 */
public abstract class HeadersIQv2IntegrationTestBase extends ClusterTestHarness {

    /** GUID bytes the producer wrote for the key / value schema, captured on send (see below). */
    private byte[] expectedKeyGuid;
    private byte[] expectedValueGuid;

    protected HeadersIQv2IntegrationTestBase() {
        super(1, true);
    }

    // ---------------------------------------------------------------------------------------------
    // Topics
    // ---------------------------------------------------------------------------------------------

    protected void createTopics(String... topicNames) throws Exception {
        createTopicsWithPartitions(1, topicNames);
    }

    protected void createTopicsWithPartitions(int numPartitions, String... topicNames)
        throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.stream(topicNames)
                .map(name -> new NewTopic(name, numPartitions, (short) 1))
                .collect(Collectors.toList());
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Header-mode serdes and client configs
    // ---------------------------------------------------------------------------------------------

    protected GenericAvroSerde createKeySerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, true);
        return serde;
    }

    protected GenericAvroSerde createValueSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, false);
        return serde;
    }

    protected Properties createProducerProps() {
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

    protected Properties createStreamsProps(String appId, Integer commitIntervalMs) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        if (commitIntervalMs != null) {
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        }
        return props;
    }

    // ---------------------------------------------------------------------------------------------
    // Streams lifecycle
    // ---------------------------------------------------------------------------------------------

    protected KafkaStreams startStreamsAndAwaitRunning(Topology topology, String appId)
        throws Exception {
        return startStreamsAndAwaitRunning(topology, appId, 30, null);
    }

    protected KafkaStreams startStreamsAndAwaitRunning(Topology topology, String appId,
        int timeoutSeconds) throws Exception {
        return startStreamsAndAwaitRunning(topology, appId, timeoutSeconds, null);
    }

    protected KafkaStreams startStreamsAndAwaitRunning(Topology topology, String appId,
        int timeoutSeconds, Integer commitIntervalMs) throws Exception {
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

    protected void closeStreams(KafkaStreams streams) {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Producing (captures the produced schema-id GUID for byte-equality assertions)
    // ---------------------------------------------------------------------------------------------

    /**
     * Sends {@code record} synchronously and captures the schema-id GUID bytes the serializer wrote
     * into the record's headers, so later IQv2 results can be asserted byte-equal to what was
     * produced. A tombstone (null value) writes no value header and leaves the captured value GUID
     * untouched.
     */
    protected void sendAndCapture(KafkaProducer<GenericRecord, GenericRecord> producer,
        ProducerRecord<GenericRecord, GenericRecord> record) throws Exception {
        producer.send(record).get();
        Header keyHeader = record.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        if (keyHeader != null) {
            expectedKeyGuid = keyHeader.value().clone();
        }
        Header valueHeader = record.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
        if (valueHeader != null) {
            expectedValueGuid = valueHeader.value().clone();
        }
    }

    protected void produce(String topic, GenericRecord key, GenericRecord value, long timestamp)
        throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            sendAndCapture(producer, new ProducerRecord<>(topic, null, timestamp, key, value));
            producer.flush();
        }
    }

    protected void produceAll(String topic, List<GenericRecord> keys, List<GenericRecord> values,
        long timestamp) throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            for (int i = 0; i < keys.size(); i++) {
                sendAndCapture(producer,
                    new ProducerRecord<>(topic, null, timestamp, keys.get(i), values.get(i)));
            }
            producer.flush();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Consuming (output-topic completion barrier)
    // ---------------------------------------------------------------------------------------------

    protected List<ConsumerRecord<GenericRecord, GenericRecord>> consumeRecords(
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
                ConsumerRecords<GenericRecord, GenericRecord> records =
                    consumer.poll(Duration.ofMillis(500));
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

    // ---------------------------------------------------------------------------------------------
    // Header assertions
    // ---------------------------------------------------------------------------------------------

    /**
     * Asserts the record carries a well-formed 17-byte {@code MAGIC_BYTE_V1} schema-id GUID in both
     * the {@code __key_schema_id} and {@code __value_schema_id} headers and, once a producer has
     * recorded the GUID it wrote (via {@link #sendAndCapture}), that the bytes are exactly those
     * produced.
     */
    protected void assertSchemaIdHeaders(Headers headers, String context) {
        byte[] keyBytes = assertGuidHeader(headers, SchemaId.KEY_SCHEMA_ID_HEADER, "Key", context);
        if (expectedKeyGuid != null) {
            assertArrayEquals(expectedKeyGuid, keyBytes,
                context + ": Key GUID header should equal the GUID the producer wrote");
        }
        byte[] valueBytes =
            assertGuidHeader(headers, SchemaId.VALUE_SCHEMA_ID_HEADER, "Value", context);
        if (expectedValueGuid != null) {
            assertArrayEquals(expectedValueGuid, valueBytes,
                context + ": Value GUID header should equal the GUID the producer wrote");
        }
    }

    private static byte[] assertGuidHeader(Headers headers, String headerName, String which,
        String context) {
        Header header = headers.lastHeader(headerName);
        assertNotNull(header, context + ": should have " + headerName + " header");
        byte[] bytes = header.value();
        assertEquals(17, bytes.length, context + ": " + which + " GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, bytes[0],
            context + ": " + which + " header should have V1 magic byte");
        return bytes;
    }

    // ---------------------------------------------------------------------------------------------
    // Misc
    // ---------------------------------------------------------------------------------------------

    protected static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
