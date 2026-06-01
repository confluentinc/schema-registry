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
import java.time.Instant;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link TimestampedWindowStoreWithHeaders} that verifies windowed store
 * operations work correctly with header-based schema ID transport.
 */
public class TimestampedWindowStoreWithHeadersIntegrationTest extends ClusterTestHarness {

    private static final String INPUT_TOPIC = "events-input";
    private static final String OUTPUT_TOPIC = "windowed-output";
    private static final String STORE_NAME = "event-window-store";
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static final Duration RETENTION_PERIOD = Duration.ofHours(1);

    private static final String KEY_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"EventKey\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":["
            + "  {\"name\":\"eventId\",\"type\":\"string\"}"
            + "]"
            + "}";

    private static final String VALUE_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"EventValue\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":["
            + "  {\"name\":\"count\",\"type\":\"long\"},"
            + "  {\"name\":\"operation\",\"type\":\"string\",\"default\":\"PUT\"}"
            + "]"
            + "}";

    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    public TimestampedWindowStoreWithHeadersIntegrationTest() {
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
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(
                        STORE_NAME, RETENTION_PERIOD, WINDOW_SIZE, false),
                    keySerde,
                    valueSerde))
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .process(() -> new WindowedEventProcessor(STORE_NAME), STORE_NAME)
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "window-store-integration-test");

            GenericRecord event1Key = createKey("event-1");
            GenericRecord event2Key = createKey("event-2");
            GenericRecord event3Key = createKey("event-3");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // Send PUT records to set up store state
                sendWindowedPutRecords(producer, INPUT_TOPIC, event1Key, event2Key, event3Key);

                // FETCH at t=8min for event-3 - should return event-3:50 from window 5-10min
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 480000L, event3Key, createValue(0L, "FETCH"))).get();

                // FETCH at t=10min - should return from window 10-15min
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 600000L, event1Key, createValue(0L, "FETCH"))).get();

                // FETCH_RANGE for event-1 from 4-11min - should return 2 windows (Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, event1Key, createValue(0L, "FETCH_RANGE_1"))).get();
                // FETCH_RANGE for event-1 from 4-11min - should return 2 windows (long)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, event1Key, createValue(0L, "FETCH_RANGE_1_LONG"))).get();

                // FETCH_RANGE for event-2 from 6-11min - should return 1 window (Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, event2Key, createValue(0L, "FETCH_RANGE_2"))).get();
                // FETCH_RANGE for event-2 from 6-11min - should return 1 window (long)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, event2Key, createValue(0L, "FETCH_RANGE_2_LONG"))).get();

                // BACKWARD_FETCH for event-1 from 4-11min - should return 2 windows in reverse (Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, event1Key, createValue(0L, "BACKWARD_FETCH"))).get();
                // BACKWARD_FETCH for event-1 from 4-11min - should return 2 windows in reverse (long)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, event1Key, createValue(0L, "BACKWARD_FETCH_LONG"))).get();

                // FETCH_ALL from 3-12min - should return 5 entries (Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, createKey("trigger"), createValue(0L, "FETCH_ALL"))).get();
                // FETCH_ALL from 3-12min - should return 5 entries (long)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, createKey("trigger"), createValue(0L, "FETCH_ALL_LONG"))).get();

                // BACKWARD_FETCH_ALL from 5-15min - should return 5 entries in reverse (Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, createKey("trigger"), createValue(0L, "BACKWARD_FETCH_ALL"))).get();
                // BACKWARD_FETCH_ALL from 5-15min - should return 5 entries in reverse (long)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, createKey("trigger"), createValue(0L, "BACKWARD_FETCH_ALL_LONG"))).get();

                // FETCH_KEY_RANGE from event-1 to event-2, 4-11min - should return 4 entries (Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, createKey("trigger"), createValue(0L, "FETCH_KEY_RANGE"))).get();
                // FETCH_KEY_RANGE from event-1 to event-2, 4-11min - should return 4 entries (long)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, createKey("trigger"), createValue(0L, "FETCH_KEY_RANGE_LONG"))).get();

                // BACKWARD_FETCH_KEY_RANGE from event-1 to event-2, 5-11min - should return 4 entries in reverse (Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, createKey("trigger"), createValue(0L, "BACKWARD_FETCH_KEY_RANGE"))).get();
                // BACKWARD_FETCH_KEY_RANGE from event-1 to event-2, 5-11min - should return 4 entries in reverse (long)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, createKey("trigger"), createValue(0L, "BACKWARD_FETCH_KEY_RANGE_LONG"))).get();

                producer.flush();
            }

            // 8 PUT + 2 FETCH + (2+2) FETCH_RANGE_1 + (1+1) FETCH_RANGE_2 + (2+2) BACKWARD_FETCH
            // + (5+5) FETCH_ALL + (5+5) BACKWARD_FETCH_ALL + (4+4) FETCH_KEY_RANGE + (4+4) BACKWARD_FETCH_KEY_RANGE = 56
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(OUTPUT_TOPIC, "window-store-test-consumer", 56, KafkaAvroDeserializer.class);

            assertEquals(56, results.size(), "Should have 56 output records");

            int idx = 0;

            // Verify 8 PUTs
            // Window 0-5min
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(10L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-1 window 0-5min");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(20L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-2 window 0-5min");

            // Window 5-10min
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-1 window 5-10min (boundary)");

            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(35L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-3 window 5-10min");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-2 window 5-10min");

            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(50L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-3 window 5-10min");

            // Window 10-15min
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-1 window 10-15min (boundary)");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-2 window 10-15min");

            // Verify FETCH at t=8min for event-3 - should return event-3 from window 5-10min (count=50)
            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(50L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH t=8min");

            // Verify FETCH at t=10min for event-1 - should return event-1 from window 10-15min (count=60)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH t=10min");

            // Verify FETCH_RANGE_1 for event-1 from 4-11min (Instant - 2 windows)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_RANGE_1 Instant window 5-10min");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_RANGE_1 Instant window 10-15min");

            // Verify FETCH_RANGE_1_LONG for event-1 from 4-11min (long - 2 windows)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_RANGE_1_LONG window 5-10min");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_RANGE_1_LONG window 10-15min");

            // Verify FETCH_RANGE_2 for event-2 from 6-11min (Instant - 1 window)
            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_RANGE_2 Instant window 10-15min");

            // Verify FETCH_RANGE_2_LONG for event-2 from 6-11min (long - 1 window)
            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_RANGE_2_LONG window 10-15min");

            // Verify BACKWARD_FETCH for event-1 from 4-11min (Instant - 2 windows in reverse)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH Instant window 10-15min");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH Instant window 5-10min");

            // Verify BACKWARD_FETCH_LONG for event-1 from 4-11min (long - 2 windows in reverse)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_LONG window 10-15min");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_LONG window 5-10min");

            // Verify FETCH_ALL from 3-12min (Instant - 5 entries)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL Instant event-1:30");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL Instant event-1:60");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL Instant event-2:40");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL Instant event-2:70");

            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(50L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL Instant event-3:50");

            // Verify FETCH_ALL_LONG from 3-12min (long - 5 entries)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL_LONG event-1:30");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL_LONG event-1:60");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL_LONG event-2:40");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL_LONG event-2:70");

            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(50L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_ALL_LONG event-3:50");

            // Verify BACKWARD_FETCH_ALL from 5-15min (Instant - 5 entries in reverse)
            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(50L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL Instant event-3:50");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL Instant event-2:70");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL Instant event-2:40");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL Instant event-1:60");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL Instant event-1:30");

            // Verify BACKWARD_FETCH_ALL_LONG from 5-15min (long - 5 entries in reverse)
            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(50L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL_LONG event-3:50");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL_LONG event-2:70");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL_LONG event-2:40");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL_LONG event-1:60");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_ALL_LONG event-1:30");

            // Verify FETCH_KEY_RANGE from event-1 to event-2 (Instant - 4 entries)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_KEY_RANGE Instant event-1:30");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_KEY_RANGE Instant event-1:60");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_KEY_RANGE Instant event-2:40");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_KEY_RANGE Instant event-2:70");

            // Verify FETCH_KEY_RANGE_LONG from event-1 to event-2 (long - 4 entries)
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_KEY_RANGE_LONG event-1:30");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_KEY_RANGE_LONG event-1:60");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_KEY_RANGE_LONG event-2:40");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_KEY_RANGE_LONG event-2:70");

            // Verify BACKWARD_FETCH_KEY_RANGE from event-1 to event-2 (Instant - 4 entries in reverse)
            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_KEY_RANGE Instant event-2:70");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_KEY_RANGE Instant event-2:40");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_KEY_RANGE Instant event-1:60");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_KEY_RANGE Instant event-1:30");

            // Verify BACKWARD_FETCH_KEY_RANGE_LONG from event-1 to event-2 (long - 4 entries in reverse)
            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(70L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_KEY_RANGE_LONG event-2:70");

            assertEquals("event-2", results.get(idx).key().get("eventId").toString());
            assertEquals(40L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_KEY_RANGE_LONG event-2:40");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(60L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_KEY_RANGE_LONG event-1:60");

            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "BACKWARD_FETCH_KEY_RANGE_LONG event-1:30");

            ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(STORE_NAME, new TimestampedWindowStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * IQv1 verification for TimestampedWindowStoreWithHeaders.
     * fetch, range fetch, fetchAll, all, backwardFetch, backwardAll, backwardFetchAll.
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
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(
                        iqv1StoreName, RETENTION_PERIOD, WINDOW_SIZE, false),
                    keySerde,
                    valueSerde))
            .stream(iqv1InputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new WindowedEventProcessor(iqv1StoreName), iqv1StoreName)
            .to(iqv1OutputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "iqv1-test");

            GenericRecord event1Key = createKey("event-1");
            GenericRecord event2Key = createKey("event-2");
            GenericRecord event3Key = createKey("event-3");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                sendWindowedPutRecords(producer, iqv1InputTopic, event1Key, event2Key, event3Key);
                producer.flush();
            }

            List<ConsumerRecord<GenericRecord, GenericRecord>> iqv1Results =
                consumeRecords(iqv1OutputTopic, "iqv1-consumer", 8, KafkaAvroDeserializer.class);
            assertEquals(8, iqv1Results.size(),
                "Should have 8 output records before IQv1 verification");

            ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(iqv1StoreName, new TimestampedWindowStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

            // FETCH
            // Window 0-5min (start=0): event-1:10, event-2:20
            ValueTimestampHeaders<GenericRecord> e1w0 = store.fetch(event1Key, 0L);
            assertNotNull(e1w0, "event-1 in window 0-5min should exist");
            assertEquals(10L, e1w0.value().get("count"));
            assertSchemaIdHeaders(e1w0.headers(), "IQv1 point fetch");

            ValueTimestampHeaders<GenericRecord> e2w0 = store.fetch(event2Key, 0L);
            assertNotNull(e2w0, "event-2 in window 0-5min should exist");
            assertEquals(20L, e2w0.value().get("count"));
            assertSchemaIdHeaders(e2w0.headers(), "IQv1 point fetch e2w0");

            assertNull(store.fetch(event3Key, 0L), "event-3 should not exist in window 0-5min");

            // Window 5-10min (start=300000): event-1:30, event-2:40, event-3:50
            ValueTimestampHeaders<GenericRecord> e1w5 = store.fetch(event1Key, 300000L);
            assertNotNull(e1w5, "event-1 in window 5-10min should exist");
            assertEquals(30L, e1w5.value().get("count"));
            assertSchemaIdHeaders(e1w5.headers(), "IQv1 point fetch e1w5");

            ValueTimestampHeaders<GenericRecord> e2w5 = store.fetch(event2Key, 300000L);
            assertNotNull(e2w5, "event-2 in window 5-10min should exist");
            assertEquals(40L, e2w5.value().get("count"));
            assertSchemaIdHeaders(e2w5.headers(), "IQv1 point fetch e2w5");

            ValueTimestampHeaders<GenericRecord> e3w5 = store.fetch(event3Key, 300000L);
            assertNotNull(e3w5, "event-3 in window 5-10min should exist");
            assertEquals(50L, e3w5.value().get("count"));
            assertSchemaIdHeaders(e3w5.headers(), "IQv1 point fetch e3w5");

            // Window 10-15min (start=600000): event-1:60, event-2:70
            ValueTimestampHeaders<GenericRecord> e1w10 = store.fetch(event1Key, 600000L);
            assertNotNull(e1w10, "event-1 in window 10-15min should exist");
            assertEquals(60L, e1w10.value().get("count"));
            assertSchemaIdHeaders(e1w10.headers(), "IQv1 point fetch e1w10");

            ValueTimestampHeaders<GenericRecord> e2w10 = store.fetch(event2Key, 600000L);
            assertNotNull(e2w10, "event-2 in window 10-15min should exist");
            assertEquals(70L, e2w10.value().get("count"));
            assertSchemaIdHeaders(e2w10.headers(), "IQv1 point fetch e2w10");

            assertNull(store.fetch(event3Key, 600000L), "event-3 should not exist in window 10-15min");
            assertNull(store.fetch(createKey("event-99"), 300000L), "event-99 should not exist");
            assertNull(store.fetch(event1Key, 900000L), "window 15-20min should not exist");

            // FETCH timerange for single key
            // event-1 from window 0-5min to 5-10min (window starts 0 to 300000)
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(event1Key, Instant.ofEpochMilli(0L), Instant.ofEpochMilli(300000L))) {
                verifyKeyValueList(iter, Arrays.asList(10L, 30L), "Range fetch for a single key");
            }

            // event-1 all windows (t=0 to t=14min) → 3 windows
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(event1Key, Instant.ofEpochMilli(0L), Instant.ofEpochMilli(840000L))) {
                verifyKeyValueList(iter, Arrays.asList(10L, 30L, 60L), "All windows fetch for a single key");
            }

            // FETCH for multiple keys for time range
            // fetch all keys (event-1 to event-3) in window 5-10min
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(event1Key, event3Key, Instant.ofEpochMilli(300000L), Instant.ofEpochMilli(500000L))) {
                verifyKeyValueList(iter, Arrays.asList("event-1", "event-2", "event-3"), Arrays.asList(30L, 40L, 50L), "Multiple keys single window");
            }

            // BACKWARD FETCH
            // backward fetch for event-1 from t=0 to t=14min → 3 windows in reverse order
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter =
                     store.backwardFetch(event1Key, Instant.ofEpochMilli(0L), Instant.ofEpochMilli(840000L))) {
                verifyKeyValueList(iter, Arrays.asList(60L, 30L, 10L), "Backward fetch for event-1");
            }

            // Empty range for event-3
            int emptyCount = 0;
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(event3Key, Instant.ofEpochMilli(0L), Instant.ofEpochMilli(240000L))) {
                while (iter.hasNext()) {
                    iter.next();
                    emptyCount++;
                }
            }
            assertEquals(0, emptyCount, "event-3 should not exist in window 0-5min range");

            // Non-existent window 15-20min
            int noWindowCount = 0;
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetchAll(Instant.ofEpochMilli(900000L), Instant.ofEpochMilli(1200000L))) {
                while (iter.hasNext()) {
                    iter.next();
                    noWindowCount++;
                }
            }
            assertEquals(0, noWindowCount, "No entries should exist in window 15-20min");

            // FETCH ALL for time range
            // fetchAll from t=4min to t=13min → 5 entries, key-first order
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetchAll(Instant.ofEpochMilli(240000L), Instant.ofEpochMilli(780000L))) {
                verifyKeyValueList(iter, Arrays.asList("event-1", "event-1", "event-2", "event-2", "event-3"), Arrays.asList(30L, 60L, 40L, 70L, 50L),
                    "FetchAll with multiple keys");
            }

            // BACKWARD FETCH ALL
            // backwardFetchAll from t=4min to t=13min → 5 entries in reverse key-first order
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.backwardFetchAll(Instant.ofEpochMilli(240000L), Instant.ofEpochMilli(780000L))) {
                verifyKeyValueList(iter, Arrays.asList("event-3", "event-2", "event-2", "event-1", "event-1"), Arrays.asList(50L, 70L, 40L, 60L, 30L),
                    "BackwardFetchAll");
            }

            // ALL
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.all()) {
                verifyKeyValueList(iter, Arrays.asList("event-1", "event-1", "event-1", "event-2", "event-2", "event-2", "event-3"),
                    Arrays.asList(10L, 30L, 60L, 20L, 40L, 70L, 50L), "all");
            }

            // BACKWARD ALL
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.backwardAll()) {
                verifyKeyValueList(iter, Arrays.asList("event-3", "event-2", "event-2", "event-2", "event-1", "event-1", "event-1"),
                    Arrays.asList(50L, 70L, 40L, 20L, 60L, 30L, 10L), "backwardAll");
            }

            // RANGE FETCH with multiple keys
            // fetch event-1 to event-2 from t=4min to t=10min, windows 5-10min and 10-15min
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(event1Key, event2Key, Instant.ofEpochMilli(240000L), Instant.ofEpochMilli(600000L))) {
                verifyKeyValueList(iter, Arrays.asList("event-1", "event-1", "event-2", "event-2"), Arrays.asList(30L, 60L, 40L, 70L), "Multi-key range fetch");
            }

            // MULTI-KEY BACKWARD FETCH
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.backwardFetch(event1Key, event2Key, Instant.ofEpochMilli(300000L), Instant.ofEpochMilli(600000L))) {
                verifyKeyValueList(iter, Arrays.asList("event-2", "event-2", "event-1", "event-1"), Arrays.asList(70L, 40L, 60L, 30L), "Multi-key backward range fetch");
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Test delete by putting null value, and fetch on non-existent/deleted entries.
     */
    @Test
    public void shouldDeleteWithNullValueAndFetchNonExistent() throws Exception {
        String inputTopic = "delete-input";
        String outputTopic = "delete-output";
        String storeName = "delete-store";

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(
                        storeName, RETENTION_PERIOD, WINDOW_SIZE, false),
                    keySerde,
                    valueSerde))
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new DeleteTestProcessor(storeName), storeName)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "delete-integration-test");
            GenericRecord event1Key = createKey("event-1");
            GenericRecord event2Key = createKey("event-2");
            GenericRecord event3Key = createKey("event-3");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                sendWindowedPutRecords(producer, inputTopic, event1Key, event2Key, event3Key);

                // FETCH event-1 at window 0-5min
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event1Key, createValue(0L, "FETCH"))).get();

                // DELETE event-1 at window 0-5min
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event1Key, createValue(0L, "PUT_NULL"))).get();

                // FETCH event-1 after delete
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event1Key, createValue(0L, "FETCH"))).get();

                // DELETE event-3 at window 0-5min (non-existent)
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event3Key, createValue(0L, "PUT_NULL"))).get();

                // FETCH event-3 after delete on non-existent
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event3Key, createValue(0L, "FETCH"))).get();

                // PUT event-3 at window 0-5min
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event3Key, createValue(100L, "PUT"))).get();

                // FETCH event-3 after put
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event3Key, createValue(0L, "FETCH"))).get();

                // DELETE event-3 at window 0-5min
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event3Key, createValue(0L, "PUT_NULL"))).get();

                // FETCH event-3 after delete
                producer.send(new ProducerRecord<>(inputTopic, null, 60000L,
                    event3Key, createValue(0L, "FETCH"))).get();

                producer.flush();
            }

            // PUT(8) + FETCH(6) = 14
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "delete-test-consumer", 14, KafkaAvroDeserializer.class);

            assertEquals(14, results.size());

            int idx = 0;

            // Verify 8 PUTs from sendWindowedPutRecords
            for (int i = 0; i < 8; i++) {
                assertSchemaIdHeaders(results.get(idx++), "PUT " + i);
            }

            // FETCH event-1 before delete
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(10L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH event-1 before delete");

            // FETCH event-1 after delete
            assertEquals("event-1", results.get(idx).key().get("eventId").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));
            idx++;

            // FETCH event-3 after delete on non-existent
            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));
            idx++;

            // PUT event-3
            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT event-3");

            // FETCH event-3 after put
            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH event-3 after put");

            // FETCH event-3 after delete
            assertEquals("event-3", results.get(idx).key().get("eventId").toString());
            assertEquals(-1L, results.get(idx).value().get("count"));

            // Verify changelog topic tombstones have key schema ID header
            String changelogTopic = "delete-integration-test-delete-store-changelog";

            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeRecords(changelogTopic, "changelog-consumer", 12, ByteArrayDeserializer.class);

            int tombstoneCount = 0;
            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                if (record.value() == null) {
                    tombstoneCount++;
                    Header keySchemaIdHeader = record.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
                    assertNotNull(keySchemaIdHeader,
                        "Tombstone record should have key schema ID header");
                }
            }
            assertTrue(tombstoneCount >= 3, "Should have at least 3 tombstone records");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Test all() iterator and iterator methods (hasNext, next, peekNextKey).
     */
    @Test
    public void shouldTestAllIteratorAndIteratorMethods() throws Exception {
        String storeName = "iterator-store";
        String inputTopic = "iterator-input";
        String outputTopic = "iterator-output";

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(
                        storeName, RETENTION_PERIOD, WINDOW_SIZE, false),
                    keySerde,
                    valueSerde))
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new IteratorTestProcessor(storeName), storeName)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "iterator-integration-test");

            GenericRecord event1Key = createKey("event-1");
            GenericRecord event2Key = createKey("event-2");
            GenericRecord event3Key = createKey("event-3");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // Send PUT records to set up store state
                sendWindowedPutRecords(producer, inputTopic, event1Key, event2Key, event3Key);

                // Test ALL - iterate all entries
                producer.send(new ProducerRecord<>(inputTopic, null, 0L,
                    createKey("trigger"), createValue(0L, "ALL"))).get();

                // Test ITERATOR_METHODS - test hasNext, next, peekNextKey
                producer.send(new ProducerRecord<>(inputTopic, null, 0L,
                    createKey("trigger"), createValue(0L, "ITERATOR_METHODS"))).get();

                producer.flush();
            }

            // PUT(8) + ALL(7) + ITERATOR_METHODS(1) = 16
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "iterator-test-consumer", 16, KafkaAvroDeserializer.class);

            assertEquals(16, results.size(), "Should have 16 output records");

            int idx = 0;

            // Verify 8 PUTs
            for (int i = 0; i < 8; i++) {
                assertSchemaIdHeaders(results.get(idx++), "PUT " + i);
            }

            // Verify ALL (7 entries)
            for (int i = 0; i < 7; i++) {
                assertSchemaIdHeaders(results.get(idx++), "ALL " + i);
            }

            // Verify ITERATOR_METHODS passed
            assertEquals("iterator_test_passed", results.get(idx).key().get("eventId").toString());
            assertSchemaIdHeaders(results.get(idx), "ITERATOR_METHODS");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Processor for testing delete with null value.
     */
    private static class DeleteTestProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord> store;

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
            long windowStart = calculateWindowStartTime(record.timestamp());

            switch (operation) {
                case "PUT":
                    handlePut(record, windowStart);
                    break;
                case "PUT_NULL":
                    handlePutNull(record, windowStart);
                    break;
                case "FETCH":
                    handleFetch(record, windowStart);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record, long windowStart) {
            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
            store.put(record.key(), toStore, windowStart);
            ValueTimestampHeaders<GenericRecord> stored = store.fetch(record.key(), windowStart);
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }

        private void handlePutNull(Record<GenericRecord, GenericRecord> record, long windowStart) {
            record.headers().remove(SchemaId.KEY_SCHEMA_ID_HEADER);
            record.headers().remove(SchemaId.VALUE_SCHEMA_ID_HEADER);
            store.put(record.key(), null, windowStart);
        }

        private void handleFetch(Record<GenericRecord, GenericRecord> record, long windowStart) {
            ValueTimestampHeaders<GenericRecord> fetched = store.fetch(record.key(), windowStart);

            if (fetched != null && fetched.value() != null) {
                context.forward(new Record<>(
                    record.key(), fetched.value(), fetched.timestamp(), fetched.headers()));
            } else {
                GenericRecord resultValue = new GenericData.Record(record.value().getSchema());
                resultValue.put("count", -1L);
                resultValue.put("operation", "NULL_RESULT");
                context.forward(new Record<>(
                    record.key(), resultValue, record.timestamp(), record.headers()));
            }
        }
    }

    /**
     * Processor for testing iterator methods.
     */
    private static class IteratorTestProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord> store;

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
            long windowStart = calculateWindowStartTime(record.timestamp());

            switch (operation) {
                case "PUT":
                    handlePut(record, windowStart);
                    break;
                case "ALL":
                    handleAll(record);
                    break;
                case "ITERATOR_METHODS":
                    handleIteratorMethods(record);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record, long windowStart) {
            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
            store.put(record.key(), toStore, windowStart);

            ValueTimestampHeaders<GenericRecord> stored = store.fetch(record.key(), windowStart);
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }

        private void handleAll(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        private void handleIteratorMethods(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
                // Test hasNext
                assertTrue(iter.hasNext(), "Iterator should have next");

                // Test peekNextKey
                Windowed<GenericRecord> peekedKey = iter.peekNextKey();
                assertNotNull(peekedKey, "peekNextKey should return a key");

                // Verify peekNextKey doesn't advance
                Windowed<GenericRecord> peekedAgain = iter.peekNextKey();
                assertEquals(peekedKey.key().get("eventId").toString(),
                    peekedAgain.key().get("eventId").toString(),
                    "peekNextKey should not advance iterator");

                // Test next
                KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> nextKv = iter.next();
                assertEquals(peekedKey.key().get("eventId").toString(),
                    nextKv.key.key().get("eventId").toString(),
                    "next() should return same key as peekNextKey()");

                // Create success result
                GenericRecord successKey = new GenericData.Record(record.key().getSchema());
                successKey.put("eventId", "iterator_test_passed");

                GenericRecord successValue = new GenericData.Record(record.value().getSchema());
                successValue.put("count", 1L);
                successValue.put("operation", "RESULT");

                context.forward(new Record<>(
                    successKey, successValue, record.timestamp(), nextKv.value.headers()));
            }
        }
    }

    /**
     * Processor that aggregates events in time windows using TimestampedWindowStoreWithHeaders.
     */
    private static class WindowedEventProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord> store;

        public WindowedEventProcessor(String storeName) {
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
                case "FETCH":
                    handleFetch(record);
                    break;
                // Instant variants
                case "FETCH_RANGE_1":
                    handleFetchRange1(record);
                    break;
                case "FETCH_RANGE_2":
                    handleFetchRange2(record);
                    break;
                case "BACKWARD_FETCH":
                    handleBackwardFetch(record);
                    break;
                case "FETCH_ALL":
                    handleFetchAll(record);
                    break;
                case "BACKWARD_FETCH_ALL":
                    handleBackwardFetchAll(record);
                    break;
                case "FETCH_KEY_RANGE":
                    handleFetchKeyRange(record);
                    break;
                case "BACKWARD_FETCH_KEY_RANGE":
                    handleBackwardFetchKeyRange(record);
                    break;
                // Long timestamp variants
                case "FETCH_RANGE_1_LONG":
                    handleFetchRange1Long(record);
                    break;
                case "FETCH_RANGE_2_LONG":
                    handleFetchRange2Long(record);
                    break;
                case "BACKWARD_FETCH_LONG":
                    handleBackwardFetchLong(record);
                    break;
                case "FETCH_ALL_LONG":
                    handleFetchAllLong(record);
                    break;
                case "BACKWARD_FETCH_ALL_LONG":
                    handleBackwardFetchAllLong(record);
                    break;
                case "FETCH_KEY_RANGE_LONG":
                    handleFetchKeyRangeLong(record);
                    break;
                case "BACKWARD_FETCH_KEY_RANGE_LONG":
                    handleBackwardFetchKeyRangeLong(record);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record) {
            long windowStart = calculateWindowStartTime(record.timestamp());

            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
            store.put(record.key(), toStore, windowStart);

            ValueTimestampHeaders<GenericRecord> stored = store.fetch(record.key(), windowStart);
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }

        private void handleFetch(Record<GenericRecord, GenericRecord> record) {
            long windowStart = calculateWindowStartTime(record.timestamp());

            ValueTimestampHeaders<GenericRecord> fetched = store.fetch(record.key(), windowStart);
            if (fetched != null) {
                context.forward(new Record<>(
                    record.key(), fetched.value(), fetched.timestamp(), fetched.headers()));
            }
        }

        /**
         * Fetch range for event-1 from 4-11min (240000ms to 660000ms).
         * Should return 2 windows: 5-10min (start=300000) and 10-15min (start=600000).
         */
        private void handleFetchRange1(Record<GenericRecord, GenericRecord> record) {
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iterator =
                     store.fetch(record.key(), Instant.ofEpochMilli(240000L), Instant.ofEpochMilli(660000L))) {
                while (iterator.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<GenericRecord>> entry = iterator.next();
                    ValueTimestampHeaders<GenericRecord> value = entry.value;
                    context.forward(new Record<>(
                        record.key(), value.value(), value.timestamp(), value.headers()));
                }
            }
        }

        /**
         * Fetch range for event-2 from 6-11min (240000ms to 660000ms).
         * Should return 1 windows: 10-15min (start=600000).
         */
        private void handleFetchRange2(Record<GenericRecord, GenericRecord> record) {
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iterator =
                     store.fetch(record.key(), Instant.ofEpochMilli(360000L), Instant.ofEpochMilli(660000L))) {
                while (iterator.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<GenericRecord>> entry = iterator.next();
                    ValueTimestampHeaders<GenericRecord> value = entry.value;
                    context.forward(new Record<>(
                        record.key(), value.value(), value.timestamp(), value.headers()));
                }
            }
        }

        /**
         * Fetch all entries in the store from 3-12min (180000ms to 720000ms).
         * Should return 5 entries: window 5-10min (3 entries) + window 10-15min (2 entries).
         * Excludes window 0-5min.
         */
        private void handleFetchAll(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iterator =
                     store.fetchAll(Instant.ofEpochMilli(180000L), Instant.ofEpochMilli(720000L))) {
                while (iterator.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> next = iterator.next();
                    ValueTimestampHeaders<GenericRecord> value = next.value;
                    context.forward(new Record<>(
                        next.key.key(), value.value(), value.timestamp(), value.headers()));
                }
            }
        }

        /**
         * Fetch windows for event-1 in reverse order from 4-11min (240000ms to 660000ms).
         * Should return 2 windows in reverse: 10-15min (start=600000), 5-10min (start=300000).
         */
        private void handleBackwardFetch(Record<GenericRecord, GenericRecord> record) {
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iterator =
                     store.backwardFetch(record.key(), Instant.ofEpochMilli(240000L), Instant.ofEpochMilli(660000L))) {
                while (iterator.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<GenericRecord>> entry = iterator.next();
                    ValueTimestampHeaders<GenericRecord> value = entry.value;
                    context.forward(new Record<>(
                        record.key(), value.value(), value.timestamp(), value.headers()));
                }
            }
        }

        /**
         * Fetch all entries in reverse from 5-15min (300000ms to 900000ms).
         * Should return 5 entries in reverse order.
         */
        private void handleBackwardFetchAll(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iterator =
                     store.backwardFetchAll(Instant.ofEpochMilli(300000L), Instant.ofEpochMilli(900000L))) {
                while (iterator.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> entry = iterator.next();
                    ValueTimestampHeaders<GenericRecord> value = entry.value;
                    context.forward(new Record<>(
                        entry.key.key(), value.value(), value.timestamp(), value.headers()));
                }
            }
        }

        /**
         * Fetch key range from event-1 to event-2, 4-11min (Instant variant).
         * Should return 4 entries: event-1:30, event-1:60, event-2:40, event-2:70.
         */
        private void handleFetchKeyRange(Record<GenericRecord, GenericRecord> record) {
            Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
            GenericRecord event1Key = new GenericData.Record(keySchema);
            event1Key.put("eventId", "event-1");
            GenericRecord event2Key = new GenericData.Record(keySchema);
            event2Key.put("eventId", "event-2");

            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(event1Key, event2Key, Instant.ofEpochMilli(240000L), Instant.ofEpochMilli(660000L))) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        /**
         * Backward fetch key range from event-1 to event-2, 5-11min (Instant variant).
         * Should return 4 entries in reverse: event-2:70, event-2:40, event-1:60, event-1:30.
         */
        private void handleBackwardFetchKeyRange(Record<GenericRecord, GenericRecord> record) {
            Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
            GenericRecord event1Key = new GenericData.Record(keySchema);
            event1Key.put("eventId", "event-1");
            GenericRecord event2Key = new GenericData.Record(keySchema);
            event2Key.put("eventId", "event-2");

            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.backwardFetch(event1Key, event2Key, Instant.ofEpochMilli(300000L), Instant.ofEpochMilli(660000L))) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        /**
         * Fetch range for event-1 from 4-11min (long timestamp variant).
         */
        private void handleFetchRange1Long(Record<GenericRecord, GenericRecord> record) {
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(record.key(), 240000L, 660000L)) {
                while (iter.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        record.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        /**
         * Fetch range for event-2 from 6-11min (long timestamp variant).
         */
        private void handleFetchRange2Long(Record<GenericRecord, GenericRecord> record) {
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(record.key(), 360000L, 660000L)) {
                while (iter.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        record.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        /**
         * Backward fetch for event-1 from 4-11min (long timestamp variant).
         */
        private void handleBackwardFetchLong(Record<GenericRecord, GenericRecord> record) {
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter =
                     store.backwardFetch(record.key(), 240000L, 660000L)) {
                while (iter.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        record.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        /**
         * Fetch all from 3-12min (long timestamp variant).
         */
        private void handleFetchAllLong(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetchAll(180000L, 720000L)) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        /**
         * Backward fetch all from 5-15min (long timestamp variant).
         */
        private void handleBackwardFetchAllLong(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.backwardFetchAll(300000L, 900000L)) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        /**
         * Fetch key range from event-1 to event-2, 4-11min (long timestamp variant).
         */
        private void handleFetchKeyRangeLong(Record<GenericRecord, GenericRecord> record) {
            Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
            GenericRecord event1Key = new GenericData.Record(keySchema);
            event1Key.put("eventId", "event-1");
            GenericRecord event2Key = new GenericData.Record(keySchema);
            event2Key.put("eventId", "event-2");

            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.fetch(event1Key, event2Key, 240000L, 660000L)) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
            }
        }

        /**
         * Backward fetch key range from event-1 to event-2, 5-11min (long timestamp variant).
         */
        private void handleBackwardFetchKeyRangeLong(Record<GenericRecord, GenericRecord> record) {
            Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
            GenericRecord event1Key = new GenericData.Record(keySchema);
            event1Key.put("eventId", "event-1");
            GenericRecord event2Key = new GenericData.Record(keySchema);
            event2Key.put("eventId", "event-2");

            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter =
                     store.backwardFetch(event1Key, event2Key, 300000L, 660000L)) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
                    context.forward(new Record<>(
                        kv.key.key(), kv.value.value(), kv.value.timestamp(), kv.value.headers()));
                }
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

    private <V> List<ConsumerRecord<GenericRecord, V>> consumeRecords(
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
        assertEquals(expectedCount, results.size(),
            "Expected " + expectedCount + " records from " + topic
                + " but got " + results.size() + " within 30s");
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

    private GenericRecord createKey(String eventId) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("eventId", eventId);
        return key;
    }

    private GenericRecord createValue(long count, String operation) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("count", count);
        value.put("operation", operation);
        return value;
    }

    /**
     * Sends PUT records to set up the windowed store state:
     * - Window 0-5min: event-1:10, event-2:20
     * - Window 5-10min: event-1:30, event-3:35, event-2:40, event-3:50 (overwritten to 50)
     * - Window 10-15min: event-1:60, event-2:70
     */
    private void sendWindowedPutRecords(
        KafkaProducer<GenericRecord, GenericRecord> producer,
        String topic,
        GenericRecord event1Key,
        GenericRecord event2Key,
        GenericRecord event3Key) throws Exception {
        // Window 0-5min
        producer.send(new ProducerRecord<>(topic, null, 60000L, event1Key, createValue(10L, "PUT"))).get();
        producer.send(new ProducerRecord<>(topic, null, 240000L, event2Key, createValue(20L, "PUT"))).get();
        // Window 5-10min
        producer.send(new ProducerRecord<>(topic, null, 300000L, event1Key, createValue(30L, "PUT"))).get();
        producer.send(new ProducerRecord<>(topic, null, 300000L, event3Key, createValue(35L, "PUT"))).get();
        producer.send(new ProducerRecord<>(topic, null, 420000L, event2Key, createValue(40L, "PUT"))).get();
        producer.send(new ProducerRecord<>(topic, null, 540000L, event3Key, createValue(50L, "PUT"))).get();
        // Window 10-15min
        producer.send(new ProducerRecord<>(topic, null, 600000L, event1Key, createValue(60L, "PUT"))).get();
        producer.send(new ProducerRecord<>(topic, null, 840000L, event2Key, createValue(70L, "PUT"))).get();
    }

    private void verifyKeyValueList(KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iter, List<String> expectedKeys, List<Long> expectedCounts, String assertionContext) {
        int idx = 0;
        List<String> multiKeyBackwardKeys = new ArrayList<>();
        List<Long> multiKeyBackwardCounts = new ArrayList<>();
        while (iter.hasNext()) {
            KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
            multiKeyBackwardKeys.add(kv.key.key().get("eventId").toString());
            multiKeyBackwardCounts.add((Long) kv.value.value().get("count"));
            assertSchemaIdHeaders(kv.value.headers(), assertionContext + " entry " + idx);
            idx++;
        }
        assertEquals(expectedKeys.size(), idx, assertionContext + " number of records");
        assertEquals(expectedKeys, multiKeyBackwardKeys, assertionContext + " keys");
        assertEquals(expectedCounts, multiKeyBackwardCounts, assertionContext + " counts");
    }

    private void verifyKeyValueList(WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iter, List<Long> expectedCounts, String assertionContext) {
        int idx = 0;
        List<Long> multiKeyBackwardCounts = new ArrayList<>();
        while (iter.hasNext()) {
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> kv = iter.next();
            multiKeyBackwardCounts.add((Long) kv.value.value().get("count"));
            assertSchemaIdHeaders(kv.value.headers(), assertionContext + " entry " + idx);
            idx++;
        }
        assertEquals(expectedCounts, multiKeyBackwardCounts, assertionContext + " counts");
    }

private static long calculateWindowStartTime(long timestamp) {
        return (timestamp / WINDOW_SIZE.toMillis()) * WINDOW_SIZE.toMillis();
    }

    /**
     * Custom QueryableStoreType for querying TimestampedWindowStoreWithHeaders directly
     * without facade wrapping. This returns the full ValueTimestampHeaders wrapper.
     */
    private static class TimestampedWindowStoreWithHeadersType<K, V>
        implements QueryableStoreType<ReadOnlyWindowStore<K, ValueTimestampHeaders<V>>> {

        @Override
        public boolean accepts(final StateStore stateStore) {
            return stateStore instanceof TimestampedWindowStoreWithHeaders
                && stateStore instanceof ReadOnlyWindowStore;
        }

        @Override
        public ReadOnlyWindowStore<K, ValueTimestampHeaders<V>> create(
            final StateStoreProvider storeProvider,
            final String storeName) {
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }
}
