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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;

/**
 * Demonstrates the {@code __key_schema_id} row-collapse hazard on a header-aware
 * {@link TimestampedWindowStoreWithHeaders} (KIP-1271).
 *
 * <p>Identical to the key-value case, but for a windowed store: two DIFFERENT key schemas whose
 * values encode to identical bytes ({@code RowKeyA{id}} and {@code RowKeyB{label}}) collapse into the
 * SAME windowed row when placed in the same window, because in header mode the schema id lives in the
 * {@code __key_schema_id} header and not in the key {@code byte[]}. The processor counts records per
 * {@code (record.key(), window)} in a single fixed window; {@code RowKeyA{id:"k1"}} then
 * {@code RowKeyB{label:"k1"}} reach count 2 (collapse), while {@code RowKeyA{id:"k2"}} is a separate
 * row (count 1). Distinct schemas are registered under {@link RecordNameStrategy}.
 */
public class HeaderSchemaIdWindowStoreRowCollapseIntegrationTest extends ClusterTestHarness {

  private static final String INPUT_TOPIC = "collapse-window-input";
  private static final String OUTPUT_TOPIC = "collapse-window-output";
  private static final String STORE_NAME = "collapse-window-store";
  private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
  private static final Duration RETENTION_PERIOD = Duration.ofHours(1);

  private static final Schema KEY_SCHEMA_A = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"RowKeyA\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
  private static final Schema KEY_SCHEMA_B = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"RowKeyB\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":[{\"name\":\"label\",\"type\":\"string\"}]}");
  private static final Schema VALUE_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"CountValue\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":[{\"name\":\"count\",\"type\":\"long\"}]}");

  public HeaderSchemaIdWindowStoreRowCollapseIntegrationTest() {
    super(1, true);
  }

  @Test
  public void differentKeySchemasWithEqualValuesCollapseIntoOneWindowedRow() throws Exception {
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
        .process(() -> new CountProcessor(STORE_NAME), STORE_NAME)
        .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

    KafkaStreams streams = null;
    try {
      streams = startStreamsAndAwaitRunning(builder.build(), "collapse-window-test");

      // Produce all records with one fixed, recent timestamp so they fall in the same window and
      // stay within the store's retention relative to stream time.
      long ts = System.currentTimeMillis();
      try (KafkaProducer<GenericRecord, GenericRecord> producer =
          new KafkaProducer<>(createProducerProps())) {
        producer.send(new ProducerRecord<>(INPUT_TOPIC, null, ts, key(KEY_SCHEMA_A, "id", "k1"), value())).get();
        producer.send(new ProducerRecord<>(INPUT_TOPIC, null, ts, key(KEY_SCHEMA_B, "label", "k1"), value())).get();
        producer.send(new ProducerRecord<>(INPUT_TOPIC, null, ts, key(KEY_SCHEMA_A, "id", "k2"), value())).get();
        producer.flush();
      }

      List<ConsumerRecord<GenericRecord, GenericRecord>> results =
          consumeRecords(OUTPUT_TOPIC, "collapse-window-consumer", 3);

      assertEquals(1L, results.get(0).value().get("count"), "first key: fresh windowed row");
      assertEquals("RowKeyB", results.get(1).key().getSchema().getName(),
          "second record's key is the other schema");
      assertEquals(2L, results.get(1).value().get("count"),
          "row collapse: a different key schema with an equal value shares the windowed row");
      assertEquals(1L, results.get(2).value().get("count"),
          "control: a different key value is a separate windowed row");

      assertNotEquals(results.get(0).key().getSchema().getName(),
          results.get(1).key().getSchema().getName(),
          "the collapsing keys use two different schemas");
    } finally {
      closeStreams(streams);
    }
  }

  /** Counts records per {@code (record.key(), fixed window)} in a header-aware window store. */
  private static class CountProcessor
      implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

    private final String storeName;
    private ProcessorContext<GenericRecord, GenericRecord> context;
    private TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord> store;

    CountProcessor(String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
      this.context = context;
      this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<GenericRecord, GenericRecord> record) {
      // Floor the record timestamp to a window start; all records share one timestamp, hence one
      // window, so the collapse is visible within a single windowed row.
      long windowStart = record.timestamp() - (record.timestamp() % WINDOW_SIZE.toMillis());
      ValueTimestampHeaders<GenericRecord> existing = store.fetch(record.key(), windowStart);
      long prev = existing == null ? 0L : (Long) existing.value().get("count");
      GenericRecord newValue = new GenericData.Record(record.value().getSchema());
      newValue.put("count", prev + 1);
      store.put(record.key(),
          ValueTimestampHeaders.make(newValue, record.timestamp(), record.headers()), windowStart);
      context.forward(new Record<>(record.key(), newValue, record.timestamp(), record.headers()));
    }
  }

  // ---- helpers (mirroring the KIP-1271 store integration tests) ----

  private GenericRecord key(Schema schema, String field, String value) {
    GenericRecord key = new GenericData.Record(schema);
    key.put(field, value);
    return key;
  }

  private GenericRecord value() {
    GenericRecord value = new GenericData.Record(VALUE_SCHEMA);
    value.put("count", 0L);
    return value;
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
    config.put(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY,
        RecordNameStrategy.class.getName());
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
    props.put(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY,
        RecordNameStrategy.class.getName());
    return props;
  }

  private Properties createStreamsProps(String appId) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    return props;
  }

  private KafkaStreams startStreamsAndAwaitRunning(Topology topology, String appId)
      throws Exception {
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

  private List<ConsumerRecord<GenericRecord, GenericRecord>> consumeRecords(
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
        ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
          results.add(record);
        }
      }
    }
    assertEquals(expectedCount, results.size(),
        "Expected " + expectedCount + " records from " + topic + " but got " + results.size());
    return results;
  }
}
