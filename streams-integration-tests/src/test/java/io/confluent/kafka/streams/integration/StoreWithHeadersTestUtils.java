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

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
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
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

final class StoreWithHeadersTestUtils {

    private StoreWithHeadersTestUtils() {
    }

    static void createTopics(String brokerList, String... topicNames) throws Exception {
        createTopics(brokerList, 1, topicNames);
    }

    static void createTopics(String brokerList, int numPartitions, String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.stream(topicNames)
                .map(name -> new NewTopic(name, numPartitions, (short) 1))
                .collect(Collectors.toList());
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        }
    }

    static GenericAvroSerde createKeySerde(String schemaRegistryUrl) {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, true);
        return serde;
    }

    static GenericAvroSerde createValueSerde(String schemaRegistryUrl) {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, false);
        return serde;
    }

    static Properties createStreamsProps(String appId, String brokerList, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return props;
    }

    static Properties createProducerProps(String brokerList, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        return props;
    }

    static KafkaStreams startStreamsAndAwaitRunning(
        Topology topology, String appId, String brokerList, String schemaRegistryUrl) throws Exception {
        return startStreamsAndAwaitRunning(topology, appId, brokerList, schemaRegistryUrl, 30);
    }

    static KafkaStreams startStreamsAndAwaitRunning(
        Topology topology, String appId, String brokerList, String schemaRegistryUrl,
        int timeoutSeconds) throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        KafkaStreams streams = new KafkaStreams(
            topology, createStreamsProps(appId, brokerList, schemaRegistryUrl));
        streams.cleanUp();
        AtomicReference<KafkaStreams.State> lastState =
            new AtomicReference<>(KafkaStreams.State.CREATED);
        streams.setStateListener((newState, oldState) -> {
            lastState.set(newState);
            if (newState == KafkaStreams.State.RUNNING) {
                startedLatch.countDown();
            }
        });
        streams.start();
        boolean running = false;
        try {
            running = startedLatch.await(timeoutSeconds, TimeUnit.SECONDS);
            assertTrue(running,
                "KafkaStreams should reach RUNNING state (last observed state: "
                    + lastState.get() + ")");
            return streams;
        } finally {
            if (!running) {
                closeStreams(streams);
            }
        }
    }

    static void closeStreams(KafkaStreams streams) {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
        }
    }

    static <K, V> List<ConsumerRecord<K, V>> consumeRecords(
        String brokerList, String schemaRegistryUrl, String topic, String groupId,
        int expectedCount, Class<?> keyDeserializerClass, Class<?> valueDeserializerClass) {
        List<ConsumerRecord<K, V>> results = pollRecords(
            brokerList, schemaRegistryUrl, topic, groupId, expectedCount,
            keyDeserializerClass, valueDeserializerClass);
        assertEquals(expectedCount, results.size(),
            "Expected " + expectedCount + " records from " + topic
                + " but got " + results.size() + " within 30s");
        return results;
    }

    static <K, V> List<ConsumerRecord<K, V>> consumeAtLeastRecords(
        String brokerList, String schemaRegistryUrl, String topic, String groupId,
        int minCount, Class<?> keyDeserializerClass, Class<?> valueDeserializerClass) {
        return pollRecords(brokerList, schemaRegistryUrl, topic, groupId, minCount,
            keyDeserializerClass, valueDeserializerClass);
    }

    private static <K, V> List<ConsumerRecord<K, V>> pollRecords(
        String brokerList, String schemaRegistryUrl, String topic, String groupId,
        int minCount, Class<?> keyDeserializerClass, Class<?> valueDeserializerClass) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        List<ConsumerRecord<K, V>> results = new ArrayList<>();
        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < minCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<K, V> record : records) {
                    results.add(record);
                }
            }
        }
        return results;
    }

    static void assertSchemaIdHeaders(ConsumerRecord<?, ?> record, String context) {
        assertSchemaIdHeaders(record.headers(), context);
    }

    static void assertSchemaIdHeaders(Headers headers, String context) {
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

    static GenericRecord createKey(Schema keySchema, String fieldName, String value) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put(fieldName, value);
        return key;
    }

    static GenericRecord createValue(Schema valueSchema, long count, String operation) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("count", count);
        value.put("operation", operation);
        return value;
    }
}
