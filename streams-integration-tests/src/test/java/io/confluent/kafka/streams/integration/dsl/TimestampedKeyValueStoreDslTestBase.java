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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.nio.ByteBuffer;
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

/**
 * Shared infrastructure for DSL integration tests of {@link TimestampedKeyValueStoreWithHeaders}.
 *
 * <p>Subclasses get Avro schemas, Schema-Registry-aware producer/consumer/serde factories,
 * Kafka Streams lifecycle helpers, schema-id header assertions, and a small set of
 * test-shape helpers ({@link #produce}, {@link #headersStore}, {@link #lastRecordPerKey},
 * {@link #assertChangelogHeaders}, {@link #suffixOf}).
 */
abstract class TimestampedKeyValueStoreDslTestBase extends ClusterTestHarness {

    protected static final String KEY_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"WordKey\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration.dsl\","
            + "\"fields\":["
            + "  {\"name\":\"word\",\"type\":\"string\"}"
            + "]"
            + "}";

    protected static final String VALUE_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"TextLine\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration.dsl\","
            + "\"fields\":["
            + "  {\"name\":\"line\",\"type\":\"string\"}"
            + "]"
            + "}";

    protected static final String AGG_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"WordCount\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration.dsl\","
            + "\"fields\":["
            + "  {\"name\":\"word\",\"type\":\"string\"},"
            + "  {\"name\":\"count\",\"type\":\"long\"}"
            + "]"
            + "}";

    protected static final String MAP_VALUE_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"MapWord\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration.dsl\","
            + "\"fields\":["
            + "  {\"name\":\"firstWord\",\"type\":\"string\"},"
            + "  {\"name\":\"count\",\"type\":\"long\"}"
            + "]"
            + "}";

    protected final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    protected final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);
    protected final Schema aggSchema = new Schema.Parser().parse(AGG_SCHEMA_JSON);
    protected final Schema mapValueSchema = new Schema.Parser().parse(MAP_VALUE_SCHEMA_JSON);

    protected TimestampedKeyValueStoreDslTestBase() {
        super(1, true);
    }

    protected void createTopics(String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.stream(topicNames)
                .map(name -> new NewTopic(name, 1, (short) 1))
                .collect(Collectors.toList());
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        }
    }

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

    protected Properties createStreamsProps(String appId, boolean cachingEnabled) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        props.put(StreamsConfig.STATE_DIR_CONFIG,
            System.getProperty("java.io.tmpdir") + "/kafka-streams-" + appId + "-" + UUID.randomUUID());
        if (cachingEnabled) {
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        } else {
            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        }
        return props;
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

    protected KafkaStreams startStreamsAndAwaitRunning(
        Topology topology, String appId, boolean cachingEnabled) throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        KafkaStreams streams = new KafkaStreams(topology, createStreamsProps(appId, cachingEnabled));
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

    protected void closeStreams(KafkaStreams streams) {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
        }
    }

    @SafeVarargs
    protected final void produce(
        String topic, KeyValue<GenericRecord, GenericRecord>... records) throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            for (KeyValue<GenericRecord, GenericRecord> kv : records) {
                producer.send(new ProducerRecord<>(topic, kv.key, kv.value)).get();
            }
            producer.flush();
        }
    }

    protected <V> ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<V>> headersStore(
        KafkaStreams streams, String storeName) {
        return streams.store(StoreQueryParameters.fromNameAndType(
            storeName, new TimestampedKeyValueStoreWithHeadersType<GenericRecord, V>()));
    }

    protected <V> Map<String, ConsumerRecord<GenericRecord, V>> lastRecordPerKey(
        List<ConsumerRecord<GenericRecord, V>> records) {
        Map<String, ConsumerRecord<GenericRecord, V>> result = new HashMap<>();
        for (ConsumerRecord<GenericRecord, V> r : records) {
            result.put(r.key().get("word").toString(), r);
        }
        return result;
    }

    protected static String suffixOf(boolean cachingEnabled) {
        return cachingEnabled ? "-cached" : "-uncached";
    }

    protected <V> void assertChangelogHeaders(
        List<ConsumerRecord<GenericRecord, V>> records,
        String changelogTopic,
        Set<String> expectedTombstoneKeys,
        String context) {
        for (ConsumerRecord<GenericRecord, V> r : records) {
            String key = r.key().get("word").toString();
            if (r.value() != null) {
                assertSchemaIdHeaders(r.headers(), changelogTopic, context + " " + key);
            } else {
                assertTrue(expectedTombstoneKeys.contains(key),
                    "Unexpected tombstone for key " + key + " in " + context);
                assertKeySchemaIdHeader(r.headers(), changelogTopic,
                    context + " tombstone for " + key);
            }
        }
    }

    protected void assertSchemaIdHeaders(Headers headers, String topic, String context) {
        Header keyHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keyHeader, context + ": should have __key_schema_id header");
        assertHeaderGuidMatchesSubject(keyHeader.value(), topic + "-key", context + " key");

        Header valueHeader = headers.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
        assertNotNull(valueHeader, context + ": should have __value_schema_id header");
        assertHeaderGuidMatchesSubject(valueHeader.value(), topic + "-value", context + " value");
    }

    protected void assertKeySchemaIdHeader(Headers headers, String topic, String context) {
        Header keyHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keyHeader, context + ": should have __key_schema_id header");
        assertHeaderGuidMatchesSubject(keyHeader.value(), topic + "-key", context + " key");
    }

    // Cross-checks the schema-id header bytes against Schema Registry: decodes the GUID from
    // the 17-byte V1 header and asserts it matches the latest registered GUID for the subject.
    protected void assertHeaderGuidMatchesSubject(byte[] headerBytes, String subject, String context) {
        assertEquals(17, headerBytes.length, context + ": GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, headerBytes[0],
            context + ": header should have V1 magic byte");

        ByteBuffer bb = ByteBuffer.wrap(headerBytes, 1, 16);
        UUID headerGuid = new UUID(bb.getLong(), bb.getLong());

        try {
            io.confluent.kafka.schemaregistry.client.rest.entities.Schema registered =
                restApp.restClient.getLatestVersion(subject);
            assertEquals(registered.getGuid(), headerGuid.toString(),
                context + ": header GUID does not match latest registered GUID for subject " + subject);
        } catch (Exception e) {
            fail(context + ": failed to look up subject " + subject + " in Schema Registry: "
                + e.getMessage());
        }
    }

    protected GenericRecord createKey(String word) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", word);
        return key;
    }

    protected GenericRecord createTextLine(String line) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("line", line);
        return value;
    }

    protected <V> List<ConsumerRecord<GenericRecord, V>> consumeRecords(
        String topic, String groupId, int expectedCount, Class<?> valueDeserializerClass) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        List<ConsumerRecord<GenericRecord, V>> results = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, V> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<GenericRecord, V> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, V> record : records) {
                    results.add(record);
                }
            }
        }
        return results;
    }

    /**
     * Custom QueryableStoreType for querying TimestampedKeyValueStoreWithHeaders directly
     * without facade wrapping. This returns the full ValueTimestampHeaders wrapper.
     */
    protected static class TimestampedKeyValueStoreWithHeadersType<K, V>
        implements QueryableStoreType<ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>>> {

        @Override
        public boolean accepts(final StateStore stateStore) {
            return stateStore instanceof TimestampedKeyValueStoreWithHeaders
                && stateStore instanceof ReadOnlyKeyValueStore;
        }

        @Override
        public ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> create(
            final StateStoreProvider storeProvider, final String storeName) {
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }
    }
}
