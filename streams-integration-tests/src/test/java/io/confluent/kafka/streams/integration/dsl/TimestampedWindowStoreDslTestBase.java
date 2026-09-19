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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.HeadersBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared infrastructure for DSL integration tests of {@link TimestampedWindowStoreWithHeaders}.
 */
abstract class TimestampedWindowStoreDslTestBase extends ClusterTestHarness {

    private static final Logger log = LoggerFactory.getLogger(TimestampedWindowStoreDslTestBase.class);

    protected static final String KEY_SCHEMA_JSON =
        "{\"type\":\"record\",\"name\":\"WordKey\",\"fields\":[{\"name\":\"word\",\"type\":\"string\"}]}";
    protected static final String VALUE_SCHEMA_JSON =
        "{\"type\":\"record\",\"name\":\"TextLine\",\"fields\":[{\"name\":\"line\",\"type\":\"string\"}]}";
    protected static final String AGG_SCHEMA_JSON =
        "{\"type\":\"record\",\"name\":\"WordCount\",\"fields\":[{\"name\":\"word\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"long\"}]}";

    protected final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    protected final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);
    protected final Schema aggSchema = new Schema.Parser().parse(AGG_SCHEMA_JSON);

    // Per-test resources released in @AfterEach to avoid JVM-level accumulation
    // (RocksDB native handles, schema-registry HTTP clients, on-disk state dirs)
    // across the parameterized invocations and other tests in the file.
    protected final List<KafkaStreams> openStreams = new ArrayList<>();
    protected final List<GenericAvroSerde> openSerdes = new ArrayList<>();

    protected TimestampedWindowStoreDslTestBase() {
        super(1, true);
    }

    @AfterEach
    public void cleanUpStreamsResources() {
        for (KafkaStreams streams : openStreams) {
            try {
                if (streams.state() != KafkaStreams.State.NOT_RUNNING) {
                    streams.close(Duration.ofSeconds(30));
                }
                streams.cleanUp();
            } catch (Exception e) {
                log.warn("Failed to clean up KafkaStreams instance", e);
            }
        }
        openStreams.clear();
        for (GenericAvroSerde serde : openSerdes) {
            try {
                serde.close();
            } catch (Exception e) {
                log.warn("Failed to close GenericAvroSerde", e);
            }
        }
        openSerdes.clear();
    }

    protected void createTopics(String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(Arrays.stream(topicNames)
                    .map(n -> new NewTopic(n, 1, (short) 1))
                    .collect(Collectors.toList()))
                .all().get(30, TimeUnit.SECONDS);
        }
    }

    protected GenericAvroSerde createKeySerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, true);
        openSerdes.add(serde);
        return serde;
    }

    protected GenericAvroSerde createValueSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, false);
        openSerdes.add(serde);
        return serde;
    }

    protected GenericAvroSerde createAggSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, false);
        openSerdes.add(serde);
        return serde;
    }

    protected Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        return props;
    }

    protected Properties createConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        return props;
    }

    protected KafkaStreams startStreamsAndAwaitRunning(
        Topology topology, String appId, boolean cachingEnabled) throws Exception {
        return startStreamsAndAwaitRunning(topology, appId, cachingEnabled, Collections.emptyMap());
    }

    protected KafkaStreams startStreamsAndAwaitRunning(
        Topology topology, String appId, boolean cachingEnabled,
        Map<String, Object> extraProps) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        if (!cachingEnabled) props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putAll(extraProps);

        CountDownLatch startedLatch = new CountDownLatch(1);
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                startedLatch.countDown();
            }
        });
        streams.start();
        assertTrue(startedLatch.await(30, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");
        openStreams.add(streams);
        return streams;
    }

    protected void closeStreams(KafkaStreams streams) {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
        }
    }

    /** Stamped key-value triple for {@link #produce(String, TimedKv...)}. */
    protected static final class TimedKv {
        final long timestampMs;
        final GenericRecord key;
        final GenericRecord value;

        TimedKv(long timestampMs, GenericRecord key, GenericRecord value) {
            this.timestampMs = timestampMs;
            this.key = key;
            this.value = value;
        }
    }

    protected static TimedKv at(long timestampMs, GenericRecord key, GenericRecord value) {
        return new TimedKv(timestampMs, key, value);
    }

    @SafeVarargs
    protected final void produce(String topic, TimedKv... records) throws Exception {
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                 new KafkaProducer<>(createProducerProps())) {
            for (TimedKv r : records) {
                producer.send(new ProducerRecord<>(topic, 0, r.timestampMs, r.key, r.value)).get();
            }
            producer.flush();
        }
    }

    protected <V> ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<V>> windowStore(
        KafkaStreams streams, String storeName) {
        return streams.store(StoreQueryParameters.fromNameAndType(
            storeName, new TimestampedWindowStoreWithHeadersType<>()));
    }

    protected static String suffixOf(boolean cachingEnabled, boolean graceEnabled, String testId) {
        return (cachingEnabled ? "-cached" : "-uncached")
            + (graceEnabled ? "-grace" : "-nograce")
            + "-" + testId;
    }

    protected static String suffixOf(boolean cachingEnabled, String testId) {
        return (cachingEnabled ? "-cached" : "-uncached") + "-" + testId;
    }

    protected static String changelogTopicFor(String applicationId, String storeName) {
        return applicationId + "-" + storeName + "-changelog";
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
        GenericRecord r = new GenericData.Record(keySchema);
        r.put("word", word);
        return r;
    }

    protected GenericRecord createTextLine(String line) {
        GenericRecord r = new GenericData.Record(valueSchema);
        r.put("line", line);
        return r;
    }

    /** Returns the value from the first window for {@code key}, or null if absent/tombstoned. */
    protected <V> V getWindowValue(ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<V>> store,
                                   GenericRecord key, long windowStart, long windowSizeMs) {
        try (WindowStoreIterator<ValueTimestampHeaders<V>> it =
                 store.fetch(key, Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSizeMs))) {
            if (!it.hasNext()) return null;
            ValueTimestampHeaders<V> v = it.next().value;
            return (v == null) ? null : v.value();
        }
    }

    protected <K, V> List<ConsumerRecord<K, V>> consumeRecords(
        String topic, String groupId, int expectedCount,
        Class<?> keyDeserializerClass, Class<?> valueDeserializerClass) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        List<ConsumerRecord<K, V>> results = new ArrayList<>();
        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            for (ConsumerRecord<K, V> record : consumer.poll(Duration.ofMillis(100))) {
                results.add(record);
            }

            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<K, V> record : records) {
                    results.add(record);
                }
            }
        }
        return results;
    }

    /** Custom QueryableStoreType for a {@link TimestampedWindowStoreWithHeaders}. */
    protected static class TimestampedWindowStoreWithHeadersType<K, V>
        implements QueryableStoreType<ReadOnlyWindowStore<K, ValueTimestampHeaders<V>>> {
        @Override public boolean accepts(StateStore s) {
            return s instanceof TimestampedWindowStoreWithHeaders;
        }
        @Override public ReadOnlyWindowStore<K, ValueTimestampHeaders<V>> create(StateStoreProvider p, String n) {
            return new CompositeReadOnlyWindowStore<>(p, this, n);
        }
    }

    /**
     * Wrapper for {@link WindowBytesStoreSupplier} that also implements
     * {@link HeadersBytesStoreSupplier}. Needed because {@code RocksDbWindowBytesStoreSupplier}
     * doesn't implement {@code HeadersBytesStoreSupplier} even when configured to create
     * header-aware stores, which would otherwise cause the DSL to use the wrong builder.
     */
    protected static class WindowStoreSupplierWithHeaders
        implements WindowBytesStoreSupplier, HeadersBytesStoreSupplier {
        private final WindowBytesStoreSupplier delegate;

        WindowStoreSupplierWithHeaders(WindowBytesStoreSupplier delegate) {
            this.delegate = delegate;
        }

        @Override public String name() { return delegate.name(); }
        @Override public WindowStore<Bytes, byte[]> get() { return delegate.get(); }
        @Override public String metricsScope() { return delegate.metricsScope(); }
        @Override public long segmentIntervalMs() { return delegate.segmentIntervalMs(); }
        @Override public long windowSize() { return delegate.windowSize(); }
        @Override public boolean retainDuplicates() { return delegate.retainDuplicates(); }
        @Override public long retentionPeriod() { return delegate.retentionPeriod(); }
    }
}
