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
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;

/**
 * Demonstrates the {@code __key_schema_id} row-collapse hazard on a header-aware
 * {@link TimestampedKeyValueStoreWithHeaders} (KIP-1271).
 *
 * <p>In header mode ({@link HeaderSchemaIdSerializer}) the key {@code byte[]} is the raw Avro
 * payload with no schema-id bytes; the schema GUID rides in the {@code __key_schema_id} header and
 * plays no part in row identity. Two DIFFERENT key schemas whose values encode to identical bytes
 * (here {@code RowKeyA{id}} and {@code RowKeyB{label}}, both a single string field) therefore map to
 * the SAME store row, even though they carry different GUIDs and are logically distinct keys.
 *
 * <p>The processor counts records per {@code record.key()}. Producing {@code RowKeyA{id:"k1"}} then
 * {@code RowKeyB{label:"k1"}} makes the count reach 2 — the two distinct logical keys were
 * aggregated into one row (collapse) — while a genuinely different key ({@code RowKeyA{id:"k2"}})
 * lands in its own row (count 1). The distinct schemas are registered under
 * {@link RecordNameStrategy} so both can be produced to a single input topic without a subject
 * compatibility clash; header mode resolves each schema by its GUID on read.
 */
public class HeaderSchemaIdKeyValueStoreRowCollapseIntegrationTest extends ClusterTestHarness {

  private static final String INPUT_TOPIC = "collapse-kv-input";
  private static final String OUTPUT_TOPIC = "collapse-kv-output";
  private static final String STORE_NAME = "collapse-kv-store";

  // Two same-shaped but differently-named key schemas: equal string values encode to byte-identical
  // payloads while the schemas (and their registered GUIDs) differ.
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

  public HeaderSchemaIdKeyValueStoreRowCollapseIntegrationTest() {
    super(1, true);
  }

  @Test
  public void differentKeySchemasWithEqualValuesCollapseIntoOneRow() throws Exception {
    createTopics(INPUT_TOPIC, OUTPUT_TOPIC);

    GenericAvroSerde keySerde = createKeySerde();
    GenericAvroSerde valueSerde = createValueSerde();

    StreamsBuilder builder = new StreamsBuilder();
    builder
        .addStateStore(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                keySerde,
                valueSerde))
        .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
        .process(() -> new CountProcessor(STORE_NAME), STORE_NAME)
        .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

    KafkaStreams streams = null;
    try {
      streams = startStreamsAndAwaitRunning(builder.build(), "collapse-kv-test");

      try (KafkaProducer<GenericRecord, GenericRecord> producer =
          new KafkaProducer<>(createProducerProps())) {
        // 1) RowKeyA{id:"k1"}                    -> new row, count 1
        producer.send(new ProducerRecord<>(INPUT_TOPIC, key(KEY_SCHEMA_A, "id", "k1"), value())).get();
        // 2) RowKeyB{label:"k1"} (same bytes)    -> COLLAPSE into row 1, count 2
        producer.send(new ProducerRecord<>(INPUT_TOPIC, key(KEY_SCHEMA_B, "label", "k1"), value())).get();
        // 3) RowKeyA{id:"k2"} (different value)  -> separate row, count 1
        producer.send(new ProducerRecord<>(INPUT_TOPIC, key(KEY_SCHEMA_A, "id", "k2"), value())).get();
        producer.flush();
      }

      List<ConsumerRecord<GenericRecord, GenericRecord>> results =
          consumeRecords(OUTPUT_TOPIC, "collapse-kv-consumer", 3);

      // Record 1: RowKeyA{id:"k1"} -> count 1
      assertEquals(1L, results.get(0).value().get("count"), "first key: fresh row");
      // Record 2: RowKeyB{label:"k1"} -> count 2 : a DIFFERENT schema collapsed into the same row.
      assertEquals("RowKeyB", results.get(1).key().getSchema().getName(),
          "second record's key is the other schema");
      assertEquals(2L, results.get(1).value().get("count"),
          "row collapse: a different key schema with an equal value shares the row");
      // Record 3: RowKeyA{id:"k2"} -> count 1 : a genuinely different value gets its own row.
      assertEquals(1L, results.get(2).value().get("count"),
          "control: a different key value is a separate row");

      // Sanity: the two collapsing keys really are different logical keys (different schemas).
      assertNotEquals(results.get(0).key().getSchema().getName(),
          results.get(1).key().getSchema().getName(),
          "the collapsing keys use two different schemas");
    } finally {
      closeStreams(streams);
    }
  }

  /** Counts records per {@code record.key()} in a header-aware timestamped KV store. */
  private static class CountProcessor
      implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

    private final String storeName;
    private ProcessorContext<GenericRecord, GenericRecord> context;
    private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

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
      ValueTimestampHeaders<GenericRecord> existing = store.get(record.key());
      long prev = existing == null ? 0L : (Long) existing.value().get("count");
      GenericRecord newValue = new GenericData.Record(record.value().getSchema());
      newValue.put("count", prev + 1);
      store.put(record.key(),
          ValueTimestampHeaders.make(newValue, record.timestamp(), record.headers()));
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
    // Two distinct key schemas share one topic, so give each its own subject.
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
