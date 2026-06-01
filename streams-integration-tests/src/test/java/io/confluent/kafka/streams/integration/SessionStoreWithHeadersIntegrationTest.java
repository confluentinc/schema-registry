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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link SessionStoreWithHeaders} that verifies store operations
 * work correctly with header-based schema ID transport.
 * All store operations are performed inside a processor.
 */
public class SessionStoreWithHeadersIntegrationTest extends ClusterTestHarness {

    private static final String INPUT_TOPIC = "session-input";
    private static final String OUTPUT_TOPIC = "session-output";
    private static final String STORE_NAME = "session-store";

    private static final String KEY_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"SessionKey\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":["
            + "  {\"name\":\"userId\",\"type\":\"string\"}"
            + "]"
            + "}";

    private static final String VALUE_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"SessionValue\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":["
            + "  {\"name\":\"count\",\"type\":\"long\"},"
            + "  {\"name\":\"operation\",\"type\":\"string\",\"default\":\"PUT\"}"
            + "]"
            + "}";

    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    public SessionStoreWithHeadersIntegrationTest() {
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
                Stores.sessionStoreWithHeadersBuilder(
                    Stores.inMemorySessionStore(STORE_NAME, Duration.ofMinutes(30)),
                    keySerde,
                    valueSerde))
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .process(() -> new SessionProcessor(STORE_NAME), STORE_NAME)
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "session-store-integration-test");

            long baseTime = System.currentTimeMillis();

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(100L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 10000,
                    createKey("user1"), createValue(101L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 500000,
                    createKey("user1"), createValue(102L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 1000000,
                    createKey("user1"), createValue(103L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 50000,
                    createKey("user2"), createValue(201L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 600000,
                    createKey("user2"), createValue(202L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 100000,
                    createKey("user3"), createValue(300L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 150000,
                    createKey("user4"), createValue(400L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 200000,
                    createKey("user5"), createValue(500L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 250000,
                    createKey("user6"), createValue(601L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 800000,
                    createKey("user6"), createValue(602L, "PUT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 300000,
                    createKey("user7"), createValue(700L, "PUT"))).get();

                // fetchSession(key, startTime, endTime) - Fetch single specific session
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "FETCH_SESSION_SINGLE"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user4"), createValue(0L, "FETCH_SESSION_SINGLE"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user99"), createValue(0L, "FETCH_SESSION_SINGLE_NONEXISTENT"))).get();

                // findSessions(key, earliestSessionEndTime, latestSessionStartTime) - Find sessions for single key with time filter
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "FIND_SESSIONS_SINGLE_KEY_FIRST_TWO"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "FIND_SESSIONS_SINGLE_KEY_FIRST_THREE"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "FIND_SESSIONS_SINGLE_KEY_ALL"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "FIND_SESSIONS_SINGLE_KEY_NONE"))).get();

                // fetch(key) - Fetch all sessions for single key
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user2"), createValue(0L, "FETCH_SINGLE_KEY"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user5"), createValue(0L, "FETCH_SINGLE_KEY"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user99"), createValue(0L, "FETCH_SINGLE_KEY_NONEXISTENT"))).get();

                // backwardFetch(key) - Backward fetch all sessions for single key
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "BACKWARD_FETCH_SINGLE_KEY"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user99"), createValue(0L, "BACKWARD_FETCH_SINGLE_KEY_NONEXISTENT"))).get();

                // backwardFindSessions(key, time, time) - Backward find sessions for single key
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user2"), createValue(0L, "BACKWARD_FIND_SESSIONS_SINGLE_KEY"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user99"), createValue(0L, "BACKWARD_FIND_SESSIONS_SINGLE_KEY_NONEXISTENT"))).get();

                // fetch(keyFrom, keyTo) - Fetch sessions for key range
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FETCH_KEY_RANGE_USER2_USER4"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FETCH_KEY_RANGE_USER5_USER7"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FETCH_KEY_RANGE_EMPTY"))).get();

                // findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime) - Find sessions for key range
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FIND_SESSIONS_KEY_RANGE_USER3_USER5"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FIND_SESSIONS_KEY_RANGE_EMPTY"))).get();

                // findSessions(earliestSessionEndTime, latestSessionEndTime) - Find ALL sessions by time range only
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FIND_SESSIONS_TIME_RANGE_ALL"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FIND_SESSIONS_TIME_RANGE_PARTIAL"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FIND_SESSIONS_TIME_RANGE_EMPTY"))).get();

                // backwardFetch(keyFrom, keyTo) - Backward fetch for key range
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "BACKWARD_FETCH_KEY_RANGE"))).get();

                // backwardFindSessions(keyFrom, keyTo, time, time) - Backward find for key range
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "BACKWARD_FIND_SESSIONS_KEY_RANGE"))).get();

                // Instant variants
                // fetchSession(key, Instant, Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "FETCH_SESSION_SINGLE_INSTANT"))).get();

                // findSessions(key, Instant, Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "FIND_SESSIONS_SINGLE_KEY_INSTANT"))).get();

                // findSessions(keyFrom, keyTo, Instant, Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FIND_SESSIONS_KEY_RANGE_INSTANT"))).get();

                // backwardFindSessions(key, Instant, Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user1"), createValue(0L, "BACKWARD_FIND_SESSIONS_SINGLE_KEY_INSTANT"))).get();

                // backwardFindSessions(keyFrom, keyTo, Instant, Instant)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "BACKWARD_FIND_SESSIONS_KEY_RANGE_INSTANT"))).get();

                // REMOVE tests
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 100000,
                    createKey("user3"), createValue(0L, "REMOVE"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 250000,
                    createKey("user6"), createValue(0L, "REMOVE"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("user99"), createValue(0L, "REMOVE_NONEXISTENT"))).get();
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime + 100000,
                    createKey("user3"), createValue(0L, "REMOVE_ALREADY_REMOVED"))).get();

                // Verify state after removes with fetch(keyFrom, keyTo)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, baseTime,
                    createKey("trigger"), createValue(0L, "FETCH_KEY_RANGE_AFTER_REMOVE"))).get();

                producer.flush();
            }

            // Expected outputs:
            // 12 PUTs (user1x4, user2x2, user3, user4, user5, user6x2, user7)
            // FETCH_SESSION_SINGLE user1 + user4 = 2 records
            // FIND_SESSIONS_SINGLE_KEY: FIRST_TWO (2) + FIRST_THREE (3) + ALL (4) + NONE (0)
            // FETCH_SINGLE_KEY user2 (2 sessions) + user5 (1 session)
            // BACKWARD_FETCH_SINGLE_KEY user1 = 4 records
            // BACKWARD_FIND_SESSIONS_SINGLE_KEY user2 = 2 records
            // FETCH_KEY_RANGE: USER2_USER4 = 4 records, USER5_USER7 = 4 records
            // FIND_SESSIONS_KEY_RANGE: USER3_USER5 = 3 records
            // FIND_SESSIONS_TIME_RANGE: ALL = 12 records, PARTIAL = 3 records, EMPTY = 0 records
            // BACKWARD_FETCH_KEY_RANGE = 12 records
            // BACKWARD_FIND_SESSIONS_KEY_RANGE = 12 records
            // FETCH_SESSION_SINGLE_INSTANT user1 = 1 record
            // FIND_SESSIONS_SINGLE_KEY_INSTANT user1 (baseTime to baseTime+100000) = 2 records
            // FIND_SESSIONS_KEY_RANGE_INSTANT user3-user5 (baseTime+100000 to baseTime+250000) = 3 records
            // BACKWARD_FIND_SESSIONS_SINGLE_KEY_INSTANT user1 (baseTime to baseTime+600000) = 3 records
            // BACKWARD_FIND_SESSIONS_KEY_RANGE_INSTANT (baseTime+50000 to baseTime+350000) = 6 records
            // REMOVE user3 + user6 = 2 records
            // FETCH_KEY_RANGE_AFTER_REMOVE = 3 records (user4 + user5 + user6 remaining)
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(OUTPUT_TOPIC, "session-store-test-consumer", 102, KafkaAvroDeserializer.class);

            assertEquals(102, results.size(), "Should have 102 output records");

            int idx = 0;

            // Verify PUT user1
            assertEquals("user1", results.get(idx).key().get("userId").toString());
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user1");

            assertEquals("user1", results.get(idx).key().get("userId").toString());
            assertEquals(101L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user1");

            assertEquals("user1", results.get(idx).key().get("userId").toString());
            assertEquals(102L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user1");

            assertEquals("user1", results.get(idx).key().get("userId").toString());
            assertEquals(103L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user1");

            // Verify PUT user2
            assertEquals("user2", results.get(idx).key().get("userId").toString());
            assertEquals(201L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user2");

            assertEquals("user2", results.get(idx).key().get("userId").toString());
            assertEquals(202L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user2");

            // Verify PUT user3
            assertEquals("user3", results.get(idx).key().get("userId").toString());
            assertEquals(300L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user3");

            // Verify PUT user4
            assertEquals("user4", results.get(idx).key().get("userId").toString());
            assertEquals(400L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user4");

            // Verify PUT user5
            assertEquals("user5", results.get(idx).key().get("userId").toString());
            assertEquals(500L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user5");

            // Verify PUT user6
            assertEquals("user6", results.get(idx).key().get("userId").toString());
            assertEquals(601L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user6");

            assertEquals("user6", results.get(idx).key().get("userId").toString());
            assertEquals(602L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user6");

            // Verify PUT user7
            assertEquals("user7", results.get(idx).key().get("userId").toString());
            assertEquals(700L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user7");

            // Verify FETCH_SESSION_SINGLE user1
            assertEquals("user1", results.get(idx).key().get("userId").toString());
            assertEquals(103L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_SESSION_SINGLE user1");

            // Verify FETCH_SESSION_SINGLE user4
            assertEquals("user4", results.get(idx).key().get("userId").toString());
            assertEquals(400L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_SESSION_SINGLE user4");

            // Verify FIND_SESSIONS_SINGLE_KEY_FIRST_TWO user1 (100 and 101)
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_FIRST_TWO user1");

            assertEquals(101L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_FIRST_TWO user1");

            // Verify FIND_SESSIONS_SINGLE_KEY_FIRST_THREE user1 (100, 101, 102)
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_FIRST_THREE user1");
            assertEquals(101L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_FIRST_THREE user1");
            assertEquals(102L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_FIRST_THREE user1");

            // Verify FIND_SESSIONS_SINGLE_KEY_ALL user1 (100, 101, 102, 103)
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_ALL user1");
            assertEquals(101L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_ALL user1");
            assertEquals(102L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_ALL user1");
            assertEquals(103L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FIND_SESSIONS_SINGLE_KEY_ALL user1");

            // FIND_SESSIONS_SINGLE_KEY_NONE user1 -> 0 records

            // Verify FETCH_SINGLE_KEY user2 (201, 202)
            assertEquals("user2", results.get(idx).key().get("userId").toString());
            assertEquals(201L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_SINGLE_KEY user2");
            assertEquals("user2", results.get(idx).key().get("userId").toString());
            assertEquals(202L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_SINGLE_KEY user2");

            // Verify FETCH_SINGLE_KEY user5 (500)
            assertEquals("user5", results.get(idx).key().get("userId").toString());
            assertEquals(500L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_SINGLE_KEY user5");

            // Verify BACKWARD_FETCH_SINGLE_KEY user1 (103, 102, 101, 100 in reverse order)
            idx = assertRangeResults(results, idx,
                new String[]{"user1:103", "user1:102", "user1:101", "user1:100"},
                "BACKWARD_FETCH_SINGLE_KEY user1");

            // Verify BACKWARD_FIND_SESSIONS_SINGLE_KEY user2 (202, 201 in reverse order)
            idx = assertRangeResults(results, idx,
                new String[]{"user2:202", "user2:201"},
                "BACKWARD_FIND_SESSIONS_SINGLE_KEY user2");

            // Verify FETCH_KEY_RANGE user2-user4 (ordered by session end time:
            // user2:201@50s, user3:300@100s, user4:400@150s, user2:202@600s)
            idx = assertRangeResults(results, idx,
                new String[]{"user2:201", "user3:300", "user4:400", "user2:202"},
                "FETCH_KEY_RANGE user2-user4");

            // Verify FETCH_KEY_RANGE user5-user7 (ordered by session end time:
            // user5:500@200s, user6:601@250s, user7:700@300s, user6:602@800s)
            idx = assertRangeResults(results, idx,
                new String[]{"user5:500", "user6:601", "user7:700", "user6:602"},
                "FETCH_KEY_RANGE user5-user7");

            // Verify FIND_SESSIONS_KEY_RANGE user3-user5
            idx = assertRangeResults(results, idx, new String[]{"user3:300", "user4:400", "user5:500"},
                "FIND_SESSIONS_KEY_RANGE user3-user5");

            // Verify FIND_SESSIONS_TIME_RANGE_ALL - all 12 sessions across all keys, in session-end-time order:
            // 0s user1:100, 10s user1:101, 50s user2:201, 100s user3:300, 150s user4:400,
            // 200s user5:500, 250s user6:601, 300s user7:700, 500s user1:102, 600s user2:202,
            // 800s user6:602, 1000s user1:103
            idx = assertRangeResults(results, idx,
                new String[]{
                    "user1:100", "user1:101",
                    "user2:201",
                    "user3:300",
                    "user4:400",
                    "user5:500",
                    "user6:601",
                    "user7:700",
                    "user1:102",
                    "user2:202",
                    "user6:602",
                    "user1:103"
                },
                "FIND_SESSIONS_TIME_RANGE_ALL");

            // Verify FIND_SESSIONS_TIME_RANGE_PARTIAL - sessions ending between baseTime+50000 and baseTime+150000
            // user2 (baseTime+50000, count=201) + user3 (baseTime+100000, count=300) + user4 (baseTime+150000, count=400)
            idx = assertRangeResults(results, idx,
                new String[]{"user2:201", "user3:300", "user4:400"},
                "FIND_SESSIONS_TIME_RANGE_PARTIAL");

            // FIND_SESSIONS_TIME_RANGE_EMPTY - 0 records (no assertions needed)

            // Verify BACKWARD_FETCH_KEY_RANGE (all 12 sessions, latest-to-earliest session-end-time order)
            idx = assertRangeResults(results, idx,
                new String[]{
                    "user1:103",
                    "user6:602",
                    "user2:202",
                    "user1:102",
                    "user7:700",
                    "user6:601",
                    "user5:500",
                    "user4:400",
                    "user3:300",
                    "user2:201",
                    "user1:101",
                    "user1:100"
                },
                "BACKWARD_FETCH_KEY_RANGE");

            // Verify BACKWARD_FIND_SESSIONS_KEY_RANGE (all 12 sessions, latest-to-earliest session-end-time order)
            idx = assertRangeResults(results, idx,
                new String[]{
                    "user1:103",
                    "user6:602",
                    "user2:202",
                    "user1:102",
                    "user7:700",
                    "user6:601",
                    "user5:500",
                    "user4:400",
                    "user3:300",
                    "user2:201",
                    "user1:101",
                    "user1:100"
                },
                "BACKWARD_FIND_SESSIONS_KEY_RANGE");

            // Verify FETCH_SESSION_SINGLE_INSTANT user1
            assertEquals("user1", results.get(idx).key().get("userId").toString());
            assertEquals(100L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_SESSION_SINGLE_INSTANT user1");

            // Verify FIND_SESSIONS_SINGLE_KEY_INSTANT user1 (baseTime to baseTime+100000 = 2 sessions)
            idx = assertRangeResults(results, idx,
                new String[]{"user1:100", "user1:101"},
                "FIND_SESSIONS_SINGLE_KEY_INSTANT user1");

            // Verify FIND_SESSIONS_KEY_RANGE_INSTANT user3-user5 (baseTime+100000 to baseTime+250000 = 3 sessions)
            idx = assertRangeResults(results, idx,
                new String[]{"user3:300", "user4:400", "user5:500"},
                "FIND_SESSIONS_KEY_RANGE_INSTANT user3-user5");

            // Verify BACKWARD_FIND_SESSIONS_SINGLE_KEY_INSTANT user1 (baseTime to baseTime+600000 = 3 sessions in reverse)
            idx = assertRangeResults(results, idx,
                new String[]{"user1:102", "user1:101", "user1:100"},
                "BACKWARD_FIND_SESSIONS_SINGLE_KEY_INSTANT user1");

            // Verify BACKWARD_FIND_SESSIONS_KEY_RANGE_INSTANT (baseTime+50000 to baseTime+350000 = 6 sessions in reverse key order)
            idx = assertRangeResults(results, idx,
                new String[]{
                    "user7:700",
                    "user6:601",
                    "user5:500",
                    "user4:400",
                    "user3:300",
                    "user2:201"
                },
                "BACKWARD_FIND_SESSIONS_KEY_RANGE_INSTANT");

            // Verify REMOVE user3
            assertEquals("user3", results.get(idx).key().get("userId").toString());
            assertEquals(300L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "REMOVE user3");

            // Verify REMOVE user6
            assertEquals("user6", results.get(idx).key().get("userId").toString());
            assertEquals(601L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "REMOVE user6");

            // Verify FETCH_KEY_RANGE user3-user6 after remove
            assertRangeResults(results, idx, new String[]{"user4:400", "user5:500", "user6:602"}, "FETCH_KEY_RANGE user3-user6");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * IQv1 verification for SessionStoreWithHeaders.
     */
    @Test
    public void shouldVerifyIQv1Operations() throws Exception {
        String iqv1InputTopic = "iqv1-session-input";
        String iqv1OutputTopic = "iqv1-session-output";
        String iqv1StoreName = "iqv1-session-store";

        createTopics(iqv1InputTopic, iqv1OutputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.sessionStoreWithHeadersBuilder(
                    Stores.persistentSessionStoreWithHeaders(iqv1StoreName, Duration.ofMinutes(30)),
                    keySerde,
                    valueSerde))
            .stream(iqv1InputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new SessionProcessor(iqv1StoreName), iqv1StoreName)
            .to(iqv1OutputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "iqv1-session-test");

            long baseTime = System.currentTimeMillis();

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // user1: 2 sessions (baseTime and baseTime+500000)
                producer.send(new ProducerRecord<>(iqv1InputTopic, null, baseTime,
                    createKey("user1"), createValue(10L, "PUT"))).get();
                producer.send(new ProducerRecord<>(iqv1InputTopic, null, baseTime + 500000,
                    createKey("user1"), createValue(20L, "PUT"))).get();

                // user2: 1 session
                producer.send(new ProducerRecord<>(iqv1InputTopic, null, baseTime + 100000,
                    createKey("user2"), createValue(30L, "PUT"))).get();

                // user3: 1 session
                producer.send(new ProducerRecord<>(iqv1InputTopic, null, baseTime + 200000,
                    createKey("user3"), createValue(40L, "PUT"))).get();

                // user4: 1 session (will be removed)
                producer.send(new ProducerRecord<>(iqv1InputTopic, null, baseTime + 300000,
                    createKey("user4"), createValue(50L, "PUT"))).get();

                // Remove user4
                producer.send(new ProducerRecord<>(iqv1InputTopic, null, baseTime + 300000,
                    createKey("user4"), createValue(0L, "REMOVE"))).get();

                producer.flush();
            }

            // Consume outputs: 5 PUTs + 1 REMOVE = 6 records
            List<ConsumerRecord<GenericRecord, GenericRecord>> iqv1Results =
                consumeRecords(iqv1OutputTopic, "iqv1-session-consumer", 6, KafkaAvroDeserializer.class);
            assertEquals(6, iqv1Results.size(),
                "Should have 6 output records before IQv1 verification");

            // Query store via IQv1
            ReadOnlySessionStore<GenericRecord, AggregationWithHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(iqv1StoreName, new SessionStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

            // FETCH user1 - should return 2 sessions
            int user1SessionCount = 0;
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(createKey("user1"))) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    assertNotNull(kv.value, "IQv1 fetch user1: value should not be null");
                    assertSchemaIdHeaders(kv.value.headers(), "IQv1 fetch user1 session " + user1SessionCount);
                    user1SessionCount++;
                }
            }
            assertEquals(2, user1SessionCount, "IQv1: user1 should have 2 sessions");

            // FETCH user2 - should return 1 session
            int user2SessionCount = 0;
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(createKey("user2"))) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    assertNotNull(kv.value, "IQv1 fetch user2: value should not be null");
                    assertEquals(30L, kv.value.aggregation().get("count"), "IQv1: user2 count should be 30");
                    assertSchemaIdHeaders(kv.value.headers(), "IQv1 fetch user2");
                    user2SessionCount++;
                }
            }
            assertEquals(1, user2SessionCount, "IQv1: user2 should have 1 session");

            // FETCH user4 (removed) - should return no sessions
            int user4SessionCount = 0;
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(createKey("user4"))) {
                while (iter.hasNext()) {
                    iter.next();
                    user4SessionCount++;
                }
            }
            assertEquals(0, user4SessionCount, "IQv1: user4 should have 0 sessions after remove");

            // FETCH non-existent user
            int user99SessionCount = 0;
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(createKey("user99"))) {
                while (iter.hasNext()) {
                    iter.next();
                    user99SessionCount++;
                }
            }
            assertEquals(0, user99SessionCount, "IQv1: user99 should have 0 sessions");

            // FIND_SESSIONS user1 with time range covering first session only
            int findSessionsCount = 0;
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(createKey("user1"), 0, baseTime + 100000)) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    assertNotNull(kv.value, "IQv1 findSessions: value should not be null");
                    assertEquals(10L, kv.value.aggregation().get("count"), "IQv1: first session count should be 10");
                    assertSchemaIdHeaders(kv.value.headers(), "IQv1 findSessions user1 first");
                    findSessionsCount++;
                }
            }
            assertEquals(1, findSessionsCount, "IQv1: findSessions should return 1 session for time range");

            // FIND_SESSIONS user1 with time range covering all sessions
            int allSessionsCount = 0;
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(createKey("user1"), 0, Long.MAX_VALUE)) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    assertSchemaIdHeaders(kv.value.headers(), "IQv1 findSessions user1 all " + allSessionsCount);
                    allSessionsCount++;
                }
            }
            assertEquals(2, allSessionsCount, "IQv1: findSessions should return 2 sessions for full range");

            // FETCH_SESSION for specific session (user3)
            AggregationWithHeaders<GenericRecord> user3Session = store.fetchSession(
                createKey("user3"), baseTime + 200000, baseTime + 200000);
            assertNotNull(user3Session, "IQv1: user3 session should exist");
            assertEquals(40L, user3Session.aggregation().get("count"), "IQv1: user3 count should be 40");
            assertSchemaIdHeaders(user3Session.headers(), "IQv1 fetchSession user3");

            // FETCH_SESSION for non-existent session
            AggregationWithHeaders<GenericRecord> nonExistent = store.fetchSession(
                createKey("user99"), 0, 0);
            assertTrue(nonExistent == null || nonExistent.aggregation() == null,
                "IQv1: non-existent session should return null");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Processor that performs session store operations based on the "operation" field.
     */
    private static class SessionProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private SessionStoreWithHeaders<GenericRecord, GenericRecord> store;
        private Map<String, Windowed<GenericRecord>> sessionKeys = new HashMap<>();

        public SessionProcessor(String storeName) {
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
                case "REMOVE":
                case "REMOVE_NONEXISTENT":
                case "REMOVE_ALREADY_REMOVED":
                    handleRemove(record);
                    break;
                // fetchSession(key, startTime, endTime) - fetch single specific session
                case "FETCH_SESSION_SINGLE":
                case "FETCH_SESSION_SINGLE_NONEXISTENT":
                    handleFetchSessionSingle(record);
                    break;
                // findSessions(key, earliestSessionEndTime, latestSessionStartTime) - find sessions for single key with time filter
                case "FIND_SESSIONS_SINGLE_KEY_FIRST_TWO":
                    handleFindSessionsSingleKeyFirstTwo(record);
                    break;
                case "FIND_SESSIONS_SINGLE_KEY_FIRST_THREE":
                    handleFindSessionsSingleKeyFirstThree(record);
                    break;
                case "FIND_SESSIONS_SINGLE_KEY_ALL":
                    handleFindSessionsSingleKeyAll(record);
                    break;
                case "FIND_SESSIONS_SINGLE_KEY_NONE":
                    handleFindSessionsSingleKeyNone(record);
                    break;
                // fetch(key) - fetch all sessions for single key
                case "FETCH_SINGLE_KEY":
                case "FETCH_SINGLE_KEY_NONEXISTENT":
                    handleFetchSingleKey(record);
                    break;
                // backwardFetch(key) - backward fetch all sessions for single key
                case "BACKWARD_FETCH_SINGLE_KEY":
                case "BACKWARD_FETCH_SINGLE_KEY_NONEXISTENT":
                    handleBackwardFetchSingleKey(record);
                    break;
                // backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime)
                case "BACKWARD_FIND_SESSIONS_SINGLE_KEY":
                case "BACKWARD_FIND_SESSIONS_SINGLE_KEY_NONEXISTENT":
                    handleBackwardFindSessionsSingleKey(record);
                    break;
                // fetch(keyFrom, keyTo) - fetch sessions for key range
                case "FETCH_KEY_RANGE":
                    handleFetchKeyRange(record);
                    break;
                case "FETCH_KEY_RANGE_USER2_USER4":
                    handleFetchKeyRange(record, "user2", "user4");
                    break;
                case "FETCH_KEY_RANGE_USER5_USER7":
                    handleFetchKeyRange(record, "user5", "user7");
                    break;
                case "FETCH_KEY_RANGE_EMPTY":
                    handleFetchKeyRangeEmpty(record);
                    break;
                case "FETCH_KEY_RANGE_AFTER_REMOVE":
                    handleFetchKeyRange(record, "user3", "user6");
                    break;
                // findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime)
                case "FIND_SESSIONS_KEY_RANGE":
                    handleFindSessionsKeyRange(record);
                    break;
                case "FIND_SESSIONS_KEY_RANGE_USER3_USER5":
                    handleFindSessionsKeyRange(record, "user3", "user5");
                    break;
                case "FIND_SESSIONS_KEY_RANGE_EMPTY":
                    handleFindSessionsKeyRangeEmpty(record);
                    break;
                // backwardFetch(keyFrom, keyTo)
                case "BACKWARD_FETCH_KEY_RANGE":
                    handleBackwardFetchKeyRange(record);
                    break;
                // backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime)
                case "BACKWARD_FIND_SESSIONS_KEY_RANGE":
                    handleBackwardFindSessionsKeyRange(record);
                    break;
                // Instant variants
                case "FETCH_SESSION_SINGLE_INSTANT":
                    handleFetchSessionInstant(record);
                    break;
                case "FIND_SESSIONS_SINGLE_KEY_INSTANT":
                    handleFindSessionsSingleKeyInstant(record);
                    break;
                case "FIND_SESSIONS_KEY_RANGE_INSTANT":
                    handleFindSessionsKeyRangeInstant(record, "user3", "user5");
                    break;
                case "BACKWARD_FIND_SESSIONS_SINGLE_KEY_INSTANT":
                    handleBackwardFindSessionsSingleKeyInstant(record);
                    break;
                case "BACKWARD_FIND_SESSIONS_KEY_RANGE_INSTANT":
                    handleBackwardFindSessionsKeyRangeInstant(record);
                    break;
                // findSessions(earliestSessionEndTime, latestSessionEndTime) - find ALL sessions by time only
                case "FIND_SESSIONS_TIME_RANGE_ALL":
                    handleFindSessionsTimeRangeAll(record);
                    break;
                case "FIND_SESSIONS_TIME_RANGE_PARTIAL":
                    handleFindSessionsTimeRangePartial(record);
                    break;
                case "FIND_SESSIONS_TIME_RANGE_EMPTY":
                    handleFindSessionsTimeRangeEmpty(record);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record) {
            long count = (Long) record.value().get("count");

            GenericRecord valueToStore = new GenericData.Record(record.value().getSchema());
            valueToStore.put("count", count);
            valueToStore.put("operation", "PUT");

            Windowed<GenericRecord> sessionKey = new Windowed<>(
                record.key(),
                new org.apache.kafka.streams.kstream.internals.SessionWindow(record.timestamp(), record.timestamp())
            );

            AggregationWithHeaders<GenericRecord> toStore =
                AggregationWithHeaders.make(valueToStore, record.headers());
            store.put(sessionKey, toStore);

            // Store session key by userId - each new session overrides the previous one
            String keyId = record.key().get("userId").toString();
            sessionKeys.put(keyId, sessionKey);

            // Forward the stored record
            context.forward(new Record<>(
                record.key(), valueToStore, record.timestamp(), record.headers()));
        }

        /**
         * Remove session for this key.
         */
        private void handleRemove(Record<GenericRecord, GenericRecord> record) {
            String keyId = record.key().get("userId").toString();
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(record.key())) {
                if (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> existing = iter.next();
                    store.remove(existing.key);
                    sessionKeys.remove(keyId);

                    if (existing.value != null && existing.value.aggregation() != null) {
                        context.forward(new Record<>(
                            record.key(),
                            existing.value.aggregation(),
                            record.timestamp(),
                            existing.value.headers()));
                    }
                }
            }
        }

        /**
         * fetchSession(key, startTime, endTime) - Fetch a single specific session by key and time.
         */
        private void handleFetchSessionSingle(Record<GenericRecord, GenericRecord> record) {
            String keyId = record.key().get("userId").toString();
            Windowed<GenericRecord> sessionKey = sessionKeys.get(keyId);

            if (sessionKey != null) {
                AggregationWithHeaders<GenericRecord> result = store.fetchSession(
                    record.key(),
                    sessionKey.window().start(),
                    sessionKey.window().end());

                if (result != null && result.aggregation() != null) {
                    context.forward(new Record<>(
                        record.key(),
                        result.aggregation(),
                        record.timestamp(),
                        result.headers()));
                }
            }
        }

        /**
         * findSessions(key, earliestSessionEndTime, latestSessionStartTime) - Find sessions for single key with time filter.
         */
        private void handleFindSessionsSingleKeyFirstTwo(Record<GenericRecord, GenericRecord> record) {
            // Range [0, baseTime+100000] covers only first 2 session
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(record.key(), 0L, record.timestamp() + 100000)) {
                forwardAllSessions(iter, record);
            }
        }

        private void handleFindSessionsSingleKeyFirstThree(Record<GenericRecord, GenericRecord> record) {
            // Range [0, baseTime+500000] covers first three sessions
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(record.key(), 0L, record.timestamp() + 500000)) {
                forwardAllSessions(iter, record);
            }
        }

        private void handleFindSessionsSingleKeyAll(Record<GenericRecord, GenericRecord> record) {
            // Range [0, MAX] covers all sessions
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(record.key(), 0L, Long.MAX_VALUE)) {
                forwardAllSessions(iter, record);
            }
        }

        private void handleFindSessionsSingleKeyNone(Record<GenericRecord, GenericRecord> record) {
            // Range [baseTime+2000000, MAX] covers no sessions
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(record.key(), record.timestamp() + 2000000, Long.MAX_VALUE)) {
                forwardAllSessions(iter, record);
            }
        }

        private void forwardAllSessions(KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter,
                                        Record<GenericRecord, GenericRecord> record) {
            while (iter.hasNext()) {
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                if (kv.value != null && kv.value.aggregation() != null) {
                    context.forward(new Record<>(
                        record.key(),
                        kv.value.aggregation(),
                        record.timestamp(),
                        kv.value.headers()));
                }
            }
        }

        /**
         * fetch(key) - Fetch all sessions for a single key.
         */
        private void handleFetchSingleKey(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(record.key())) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    if (kv.value != null && kv.value.aggregation() != null) {
                        context.forward(new Record<>(
                            record.key(),
                            kv.value.aggregation(),
                            record.timestamp(),
                            kv.value.headers()));
                    }
                }
            }
        }

        /**
         * backwardFetch(key) - Fetch all sessions for a single key in reverse order.
         */
        private void handleBackwardFetchSingleKey(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.backwardFetch(record.key())) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    if (kv.value != null && kv.value.aggregation() != null) {
                        context.forward(new Record<>(
                            record.key(),
                            kv.value.aggregation(),
                            record.timestamp(),
                            kv.value.headers()));
                    }
                }
            }
        }

        /**
         * backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime) - Find sessions for single key in reverse order.
         */
        private void handleBackwardFindSessionsSingleKey(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.backwardFindSessions(record.key(), 0L, Long.MAX_VALUE)) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    if (kv.value != null && kv.value.aggregation() != null) {
                        context.forward(new Record<>(
                            record.key(),
                            kv.value.aggregation(),
                            record.timestamp(),
                            kv.value.headers()));
                    }
                }
            }
        }

        /**
         * fetch(keyFrom, keyTo) - Fetch sessions for a key range (null to null = all keys).
         */
        private void handleFetchKeyRange(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(null, null)) {
                forwardAll(iter, record);
            }
        }

        private void handleFetchKeyRange(Record<GenericRecord, GenericRecord> record, String fromUser, String toUser) {
            GenericRecord keyFrom = createKeyRecord(fromUser);
            GenericRecord keyTo = createKeyRecord(toUser);
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(keyFrom, keyTo)) {
                forwardAll(iter, record);
            }
        }

        private void handleFetchKeyRangeEmpty(Record<GenericRecord, GenericRecord> record) {
            GenericRecord keyFrom = createKeyRecord("user99");
            GenericRecord keyTo = createKeyRecord("user99z");
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.fetch(keyFrom, keyTo)) {
                forwardAll(iter, record);
            }
        }

        /**
         * findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime) - Find sessions for key range with time filter.
         */
        private void handleFindSessionsKeyRange(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(null, null, 0L, Long.MAX_VALUE)) {
                forwardAll(iter, record);
            }
        }

        private void handleFindSessionsKeyRange(Record<GenericRecord, GenericRecord> record, String fromUser, String toUser) {
            GenericRecord keyFrom = createKeyRecord(fromUser);
            GenericRecord keyTo = createKeyRecord(toUser);
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(keyFrom, keyTo, 0L, Long.MAX_VALUE)) {
                forwardAll(iter, record);
            }
        }

        private void handleFindSessionsKeyRangeEmpty(Record<GenericRecord, GenericRecord> record) {
            GenericRecord keyFrom = createKeyRecord("user99");
            GenericRecord keyTo = createKeyRecord("user99z");
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(keyFrom, keyTo, 0L, Long.MAX_VALUE)) {
                forwardAll(iter, record);
            }
        }

        /**
         * backwardFetch(keyFrom, keyTo) - Backward fetch sessions for a key range (null to null = all).
         */
        private void handleBackwardFetchKeyRange(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.backwardFetch(null, null)) {

                forwardAll(iter, record);
            }
        }

        /**
         * backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime) - Backward find sessions for key range.
         */
        private void handleBackwardFindSessionsKeyRange(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.backwardFindSessions(null, null, 0L, Long.MAX_VALUE)) {
                forwardAll(iter, record);
            }
        }

        /**
         * fetchSession(key, Instant, Instant) - Fetch specific session using Instant.
         */
        private void handleFetchSessionInstant(Record<GenericRecord, GenericRecord> record) {
            AggregationWithHeaders<GenericRecord> result = store.fetchSession(
                record.key(), Instant.ofEpochMilli(record.timestamp()), Instant.ofEpochMilli(record.timestamp()));
            if (result != null && result.aggregation() != null) {
                context.forward(new Record<>(
                    record.key(), result.aggregation(), record.timestamp(), result.headers()));
            }
        }

        /**
         * findSessions(key, Instant, Instant) - Find sessions for single key using Instant.
         * Time range: baseTime to baseTime+100000 -> should return user1:100, user1:101 (2 sessions)
         */
        private void handleFindSessionsSingleKeyInstant(Record<GenericRecord, GenericRecord> record) {
            long baseTime = record.timestamp();
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(record.key(), Instant.ofEpochMilli(baseTime), Instant.ofEpochMilli(baseTime + 100000))) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    if (kv.value != null && kv.value.aggregation() != null) {
                        context.forward(new Record<>(
                            record.key(), kv.value.aggregation(), record.timestamp(), kv.value.headers()));
                    }
                }
            }
        }

        /**
         * findSessions(keyFrom, keyTo, Instant, Instant) - Find sessions for key range using Instant.
         * Time range: baseTime+100000 to baseTime+250000 -> should return user3:300, user4:400, user5:500 (3 sessions)
         */
        private void handleFindSessionsKeyRangeInstant(Record<GenericRecord, GenericRecord> record, String fromUser, String toUser) {
            long baseTime = record.timestamp();
            GenericRecord keyFrom = createKeyRecord(fromUser);
            GenericRecord keyTo = createKeyRecord(toUser);
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(keyFrom, keyTo, Instant.ofEpochMilli(baseTime + 100000), Instant.ofEpochMilli(baseTime + 250000))) {
                forwardAll(iter, record);
            }
        }

        /**
         * backwardFindSessions(key, Instant, Instant) - Find sessions for single key in reverse using Instant.
         * Time range: baseTime to baseTime+600000 -> should return user1:102, user1:101, user1:100 (3 sessions in reverse)
         */
        private void handleBackwardFindSessionsSingleKeyInstant(Record<GenericRecord, GenericRecord> record) {
            long baseTime = record.timestamp();
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.backwardFindSessions(record.key(), Instant.ofEpochMilli(baseTime), Instant.ofEpochMilli(baseTime + 600000))) {
                while (iter.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                    if (kv.value != null && kv.value.aggregation() != null) {
                        context.forward(new Record<>(
                            record.key(), kv.value.aggregation(), record.timestamp(), kv.value.headers()));
                    }
                }
            }
        }

        /**
         * backwardFindSessions(keyFrom, keyTo, Instant, Instant) - Find sessions for key range in reverse using Instant.
         * Time range: baseTime+50000 to baseTime+350000 -> should return 6 sessions in reverse key order
         * (user7:700, user6:601, user5:500, user4:400, user3:300, user2:201)
         */
        private void handleBackwardFindSessionsKeyRangeInstant(Record<GenericRecord, GenericRecord> record) {
            long baseTime = record.timestamp();
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.backwardFindSessions(null, null, Instant.ofEpochMilli(baseTime + 50000), Instant.ofEpochMilli(baseTime + 350000))) {
                forwardAll(iter, record);
            }
        }

        /**
         * findSessions(earliestSessionEndTime, latestSessionEndTime) - Find ALL sessions by time range only (no key filter).
         */
        private void handleFindSessionsTimeRangeAll(Record<GenericRecord, GenericRecord> record) {
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(0L, Long.MAX_VALUE)) {
                forwardAll(iter, record);
            }
        }

        /**
         * findSessions(earliestSessionEndTime, latestSessionEndTime) - Find sessions in a partial time range.
         */
        private void handleFindSessionsTimeRangePartial(Record<GenericRecord, GenericRecord> record) {
            long time = record.timestamp();
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(time + 50000, time + 150000)) {
                forwardAll(iter, record);
            }
        }

        /**
         * findSessions(earliestSessionEndTime, latestSessionEndTime) - Find sessions in an empty time range.
         */
        private void handleFindSessionsTimeRangeEmpty(Record<GenericRecord, GenericRecord> record) {
            // Use a time range far in the future where no sessions exist
            try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter =
                     store.findSessions(Long.MAX_VALUE - 1000, Long.MAX_VALUE)) {
                forwardAll(iter, record);
            }
        }

        private void forwardAll(KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> iter,
                                Record<GenericRecord, GenericRecord> record) {
            while (iter.hasNext()) {
                KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = iter.next();
                if (kv.value != null && kv.value.aggregation() != null) {
                    context.forward(new Record<>(
                        kv.key.key(),
                        kv.value.aggregation(),
                        record.timestamp(),
                        kv.value.headers()));
                }
            }
        }

        private static final Schema SESSION_KEY_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"SessionKey\",\"namespace\":\"io.confluent.kafka.streams.integration\",\"fields\":[{\"name\":\"userId\",\"type\":\"string\"}]}");

        private GenericRecord createKeyRecord(String userId) {
            GenericRecord key = new GenericData.Record(SESSION_KEY_SCHEMA);
            key.put("userId", userId);
            return key;
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
        assertTrue(startedLatch.await(30, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");
        return streams;
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

    private int assertRangeResults(List<ConsumerRecord<GenericRecord, GenericRecord>> results,
                                   int startIdx, String[] expectedPairs, String context) {
        for (int i = 0; i < expectedPairs.length; i++) {
            String key = results.get(startIdx + i).key().get("userId").toString();
            Long count = (Long) results.get(startIdx + i).value().get("count");
            assertEquals(expectedPairs[i], key + ":" + count,
                context + ": record at position " + i);
            assertSchemaIdHeaders(results.get(startIdx + i), context);
        }
        return startIdx + expectedPairs.length;
    }

    private GenericRecord createKey(String userId) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("userId", userId);
        return key;
    }

    private GenericRecord createValue(long count, String operation) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("count", count);
        value.put("operation", operation);
        return value;
    }

@Test
    public void shouldRemoveSessionAndVerifyChangelogHeaders() throws Exception {
        String inputTopic = "session-delete-input";
        String outputTopic = "session-delete-output";
        String storeName = "session-delete-store";

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.sessionStoreWithHeadersBuilder(
                    Stores.persistentSessionStoreWithHeaders(storeName, Duration.ofMinutes(30)),
                    keySerde,
                    valueSerde))
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .process(() -> new DeleteSessionProcessor(storeName), storeName)
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "session-delete-integration-test");

            long baseTime = System.currentTimeMillis();

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // PUT user-1:10
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime,
                    createKey("user-1"), createValue(10L, "PUT"))).get();

                // PUT user-2:20
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime + 1000,
                    createKey("user-2"), createValue(20L, "PUT"))).get();

                // PUT user-3:30
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime + 2000,
                    createKey("user-3"), createValue(30L, "PUT"))).get();

                // FETCH_SESSION_SINGLE user-1 (should return 10)
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime + 3000,
                    createKey("user-1"), createValue(0L, "FETCH_SESSION_SINGLE"))).get();

                // REMOVE user-1
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime + 4000,
                    createKey("user-1"), createValue(0L, "REMOVE"))).get();

                // FETCH_SESSION_SINGLE user-1 after remove (should return nothing)
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime + 5000,
                    createKey("user-1"), createValue(0L, "FETCH_SESSION_SINGLE"))).get();

                // REMOVE user-99 (non-existent)
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime + 6000,
                    createKey("user-99"), createValue(0L, "REMOVE"))).get();

                // REMOVE user-2
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime + 7000,
                    createKey("user-2"), createValue(0L, "REMOVE"))).get();

                // PUT_NULL user-3 (put null to delete existing session)
                producer.send(new ProducerRecord<>(inputTopic, null, baseTime + 2000,
                    createKey("user-3"), createValue(0L, "PUT_NULL"))).get();

                producer.flush();
            }

            // Expected: 3 PUTs + 1 FETCH_SESSION_SINGLE (user-1) + 1 REMOVE (user-1) + 1 REMOVE (user-2) + 1 PUT_NULL (user-3) = 7
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "session-delete-test-consumer", 7, KafkaAvroDeserializer.class);

            assertEquals(7, results.size());

            int idx = 0;

            // Verify PUT user-1
            assertEquals("user-1", results.get(idx).key().get("userId").toString());
            assertEquals(10L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user-1");

            // Verify PUT user-2
            assertEquals("user-2", results.get(idx).key().get("userId").toString());
            assertEquals(20L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user-2");

            // Verify PUT user-3
            assertEquals("user-3", results.get(idx).key().get("userId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT user-3");

            // Verify FETCH_SESSION_SINGLE user-1
            assertEquals("user-1", results.get(idx).key().get("userId").toString());
            assertEquals(10L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "FETCH_SESSION_SINGLE user-1");

            // Verify REMOVE user-1
            assertEquals("user-1", results.get(idx).key().get("userId").toString());
            assertEquals(10L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "REMOVE user-1");

            // Verify REMOVE user-2
            assertEquals("user-2", results.get(idx).key().get("userId").toString());
            assertEquals(20L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "REMOVE user-2");

            // Verify PUT_NULL user-3
            assertEquals("user-3", results.get(idx).key().get("userId").toString());
            assertEquals(30L, results.get(idx).value().get("count"));
            assertSchemaIdHeaders(results.get(idx++), "PUT_NULL user-3");

            // Verify changelog topic tombstones have key schema ID header
            String changelogTopic = "session-delete-integration-test-session-delete-store-changelog";

            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeRecords(changelogTopic, "session-changelog-consumer", 6, ByteArrayDeserializer.class);

            int tombstoneCount = 0;
            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                if (record.value() == null) {
                    tombstoneCount++;
                    Header keySchemaIdHeader = record.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
                    assertNotNull(keySchemaIdHeader,
                        "Tombstone record should have key schema ID header");
                }
            }
            assertTrue(tombstoneCount >= 2, "Should have at least 2 tombstone records, but found " + tombstoneCount);

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Processor for testing session delete operations.
     */
    private static class DeleteSessionProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private SessionStoreWithHeaders<GenericRecord, GenericRecord> store;
        private Map<String, Windowed<GenericRecord>> lastSessionKeys = new HashMap<>();

        public DeleteSessionProcessor(String storeName) {
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
                case "PUT_NULL":
                    handlePutNull(record);
                    break;
                case "REMOVE":
                    handleRemove(record);
                    break;
                case "FETCH_SESSION_SINGLE":
                    handleFetchSessionSingle(record);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record) {
            long count = (Long) record.value().get("count");

            GenericRecord valueToStore = new GenericData.Record(record.value().getSchema());
            valueToStore.put("count", count);
            valueToStore.put("operation", "PUT");

            Windowed<GenericRecord> sessionKey = new Windowed<>(
                record.key(),
                new SessionWindow(record.timestamp(), record.timestamp())
            );

            AggregationWithHeaders<GenericRecord> toStore =
                AggregationWithHeaders.make(valueToStore, record.headers());
            store.put(sessionKey, toStore);

            String keyId = record.key().get("userId").toString();
            lastSessionKeys.put(keyId, sessionKey);

            context.forward(new Record<>(
                record.key(), valueToStore, record.timestamp(), record.headers()));
        }

        private void handlePutNull(Record<GenericRecord, GenericRecord> record) {
            record.headers().remove(SchemaId.KEY_SCHEMA_ID_HEADER);
            record.headers().remove(SchemaId.VALUE_SCHEMA_ID_HEADER);

            Windowed<GenericRecord> sessionKey = new Windowed<>(
                record.key(),
                new SessionWindow(record.timestamp(), record.timestamp())
            );

            AggregationWithHeaders<GenericRecord> existing = store.fetchSession(
                record.key(),
                sessionKey.window().start(),
                sessionKey.window().end());

            store.put(sessionKey, null);

            String keyId = record.key().get("userId").toString();
            lastSessionKeys.put(keyId, sessionKey);

            if (existing != null && existing.aggregation() != null) {
                context.forward(new Record<>(
                    record.key(),
                    existing.aggregation(),
                    record.timestamp(),
                    existing.headers()));
            }
        }

        private void handleRemove(Record<GenericRecord, GenericRecord> record) {
            record.headers().remove(SchemaId.KEY_SCHEMA_ID_HEADER);
            record.headers().remove(SchemaId.VALUE_SCHEMA_ID_HEADER);

            String keyId = record.key().get("userId").toString();
            Windowed<GenericRecord> sessionKey = lastSessionKeys.get(keyId);

            if (sessionKey != null) {
                AggregationWithHeaders<GenericRecord> existing = store.fetchSession(
                    record.key(),
                    sessionKey.window().start(),
                    sessionKey.window().end());

                store.remove(sessionKey);
                lastSessionKeys.remove(keyId);

                if (existing != null && existing.aggregation() != null) {
                    context.forward(new Record<>(
                        record.key(),
                        existing.aggregation(),
                        record.timestamp(),
                        existing.headers()));
                }
            }
        }

        /**
         * fetchSession(key, startTime, endTime) - Fetch a single specific session.
         */
        private void handleFetchSessionSingle(Record<GenericRecord, GenericRecord> record) {
            String keyId = record.key().get("userId").toString();
            Windowed<GenericRecord> sessionKey = lastSessionKeys.get(keyId);

            if (sessionKey != null) {
                AggregationWithHeaders<GenericRecord> result = store.fetchSession(
                    record.key(),
                    sessionKey.window().start(),
                    sessionKey.window().end());

                if (result != null && result.aggregation() != null) {
                    context.forward(new Record<>(
                        record.key(),
                        result.aggregation(),
                        record.timestamp(),
                        result.headers()));
                }
            }
        }
    }

    /**
     * Custom QueryableStoreType for querying SessionStoreWithHeaders directly
     * without facade wrapping. This returns the full AggregationWithHeaders wrapper.
     */
    private static class SessionStoreWithHeadersType<K, V>
        implements QueryableStoreType<ReadOnlySessionStore<K, AggregationWithHeaders<V>>> {

        @Override
        public boolean accepts(final StateStore stateStore) {
            return stateStore instanceof SessionStoreWithHeaders
                && stateStore instanceof ReadOnlySessionStore;
        }

        @Override
        public ReadOnlySessionStore<K, AggregationWithHeaders<V>> create(
            final StateStoreProvider storeProvider,
            final String storeName) {
            return new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
        }
    }
}
