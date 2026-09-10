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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
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
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the header-aware stores (KIP-1271) — {@link TimestampedKeyValueStoreWithHeaders},
 * {@link TimestampedWindowStoreWithHeaders}, {@link SessionStoreWithHeaders} — <b>preserve key
 * identity under re-keying</b>: a key reconstructed on read-back carries its own (correct) schema,
 * not the in-flight record's.
 *
 * <p>Setup (a re-keying / secondary-index pattern): records arrive keyed by {@code OrderKey{orderId}}
 * and a Processor-API processor stores each value under a key <em>derived from the value</em>,
 * {@code CustomerKey{customerId}} — a store row key whose schema differs from the incoming record's
 * key. In header mode ({@link HeaderSchemaIdSerializer}) the row key bytes carry no schema id.
 *
 * <p>What makes this safe: the header-aware stores keep <b>each entry's headers</b> alongside its
 * value and deserialize a stored key using <b>those stored per-entry headers</b> (see
 * {@code MeteredTimestampedKeyValueStoreWithHeaders} iterator), not
 * {@code internalContext.headers()}. Because the store key serde stamps the key's own
 * {@code __key_schema_id} into those headers on {@code put}, the read-back reconstructs the correct
 * {@code CustomerKey} — even though an unrelated {@code OrderKey} record is in flight. (This is the
 * opposite of a plain, non-header store keyed by a header-mode serde, which would reconstruct the key
 * under the in-flight {@code OrderKey} id and get it wrong.)
 */
public class HeaderAwareStoreRecoversRekeyedKeyIntegrationTest extends ClusterTestHarness {

  private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
  private static final Duration RETENTION_PERIOD = Duration.ofHours(1);
  private static final Duration SESSION_GAP = Duration.ofMinutes(30);

  private static final Schema ORDER_KEY_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"OrderKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"}]}");
  private static final Schema ORDER_VALUE_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"OrderValue\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":[{\"name\":\"customerId\",\"type\":\"string\"}]}");
  private static final Schema CUSTOMER_KEY_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"CustomerKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":[{\"name\":\"customerId\",\"type\":\"string\"}]}");
  private static final Schema REPORT_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"SweepReport\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":[{\"name\":\"reconstructedKeySchema\",\"type\":\"string\"},"
          + "{\"name\":\"reconstructedValue\",\"type\":\"string\"}]}");

  public HeaderAwareStoreRecoversRekeyedKeyIntegrationTest() {
    super(1, true);
  }

  @Test
  public void keyValueStore_recoversRekeyedKeyWithItsOwnSchema() throws Exception {
    String input = "rekey-kv-input";
    String output = "rekey-kv-output";
    String storeName = "rekey-kv-store";
    createTopics(input, output);

    GenericAvroSerde keySerde = createKeySerde();
    GenericAvroSerde valueSerde = createValueSerde();

    StreamsBuilder builder = new StreamsBuilder();
    builder
        .addStateStore(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                keySerde,
                valueSerde))
        .stream(input, Consumed.with(keySerde, valueSerde))
        .process(() -> new KeyValueRekeyProcessor(storeName), storeName)
        .to(output, Produced.with(keySerde, valueSerde));

    runAndAssertKeyRecovered(builder.build(), "rekey-kv-test", input, output, "rekey-kv-consumer");
  }

  @Test
  public void windowStore_recoversRekeyedKeyWithItsOwnSchema() throws Exception {
    String input = "rekey-window-input";
    String output = "rekey-window-output";
    String storeName = "rekey-window-store";
    createTopics(input, output);

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
        .stream(input, Consumed.with(keySerde, valueSerde))
        .process(() -> new WindowRekeyProcessor(storeName), storeName)
        .to(output, Produced.with(keySerde, valueSerde));

    runAndAssertKeyRecovered(builder.build(), "rekey-window-test", input, output,
        "rekey-window-consumer");
  }

  @Test
  public void sessionStore_recoversRekeyedKeyWithItsOwnSchema() throws Exception {
    String input = "rekey-session-input";
    String output = "rekey-session-output";
    String storeName = "rekey-session-store";
    createTopics(input, output);

    GenericAvroSerde keySerde = createKeySerde();
    GenericAvroSerde valueSerde = createValueSerde();

    StreamsBuilder builder = new StreamsBuilder();
    builder
        .addStateStore(
            Stores.sessionStoreWithHeadersBuilder(
                Stores.persistentSessionStoreWithHeaders(storeName, SESSION_GAP),
                keySerde,
                valueSerde))
        .stream(input, Consumed.with(keySerde, valueSerde))
        .process(() -> new SessionRekeyProcessor(storeName), storeName)
        .to(output, Produced.with(keySerde, valueSerde));

    runAndAssertKeyRecovered(builder.build(), "rekey-session-test", input, output,
        "rekey-session-consumer");
  }

  /**
   * Produces one {@code OrderKey} record whose value carries {@code customerId="cust-1"}. The
   * processor stores it under a value-derived {@code CustomerKey} and immediately reads the key back;
   * the reconstructed key must be a {@code CustomerKey} (recovered from the entry's own stored
   * headers), not the in-flight {@code OrderKey}.
   */
  private void runAndAssertKeyRecovered(Topology topology, String appId, String input,
      String output, String consumerGroup) throws Exception {
    KafkaStreams streams = null;
    try {
      streams = startStreamsAndAwaitRunning(topology, appId);

      long ts = System.currentTimeMillis();
      try (KafkaProducer<GenericRecord, GenericRecord> producer =
          new KafkaProducer<>(createProducerProps())) {
        producer.send(new ProducerRecord<>(input, null, ts, orderKey("o1"), orderValue("cust-1"))).get();
        producer.flush();
      }

      List<ConsumerRecord<GenericRecord, GenericRecord>> results =
          consumeRecords(output, consumerGroup, 1);

      GenericRecord report = results.get(0).value();
      // The header-aware store deserializes the stored key from its own per-entry headers (which
      // carry the CustomerKey id stamped at put time), so the row key is recovered as CustomerKey —
      // NOT the in-flight OrderKey. (A plain, non-header store keyed by a header-mode serde would
      // instead decode under the in-flight OrderKey id and reconstruct the wrong key.)
      assertEquals("CustomerKey", report.get("reconstructedKeySchema").toString(),
          "header-aware store recovers the stored key's own schema, not the in-flight OrderKey id");
      assertEquals("cust-1", report.get("reconstructedValue").toString(),
          "customerId round-trips correctly");
    } finally {
      closeStreams(streams);
    }
  }

  private static void forwardReport(ProcessorContext<GenericRecord, GenericRecord> context,
      GenericRecord originalKey, GenericRecord reconstructedKey, Record<GenericRecord, GenericRecord> record) {
    GenericRecord report = new GenericData.Record(REPORT_SCHEMA);
    report.put("reconstructedKeySchema", reconstructedKey.getSchema().getName());
    String firstField = reconstructedKey.getSchema().getFields().get(0).name();
    report.put("reconstructedValue", String.valueOf(reconstructedKey.get(firstField)));
    context.forward(new Record<>(originalKey, report, record.timestamp(), record.headers()));
  }

  private static GenericRecord customerKeyFrom(GenericRecord orderValue) {
    GenericRecord customerKey = new GenericData.Record(CUSTOMER_KEY_SCHEMA);
    customerKey.put("customerId", orderValue.get("customerId"));
    return customerKey;
  }

  // ---- processors (one per store type): put under a value-derived key, then read the key back ----

  private static class KeyValueRekeyProcessor
      implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

    private final String storeName;
    private ProcessorContext<GenericRecord, GenericRecord> context;
    private TimestampedKeyValueStoreWithHeaders<GenericRecord, GenericRecord> store;

    KeyValueRekeyProcessor(String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
      this.context = context;
      this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<GenericRecord, GenericRecord> record) {
      store.put(customerKeyFrom(record.value()),
          ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
      try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> it = store.all()) {
        while (it.hasNext()) {
          KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = it.next();
          forwardReport(context, record.key(), kv.key, record);
        }
      }
    }
  }

  private static class WindowRekeyProcessor
      implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

    private final String storeName;
    private ProcessorContext<GenericRecord, GenericRecord> context;
    private TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord> store;

    WindowRekeyProcessor(String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
      this.context = context;
      this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<GenericRecord, GenericRecord> record) {
      long windowStart = record.timestamp() - (record.timestamp() % WINDOW_SIZE.toMillis());
      store.put(customerKeyFrom(record.value()),
          ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()), windowStart);
      try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> it =
          store.all()) {
        while (it.hasNext()) {
          KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = it.next();
          forwardReport(context, record.key(), kv.key.key(), record);
        }
      }
    }
  }

  private static class SessionRekeyProcessor
      implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

    private final String storeName;
    private ProcessorContext<GenericRecord, GenericRecord> context;
    private SessionStoreWithHeaders<GenericRecord, GenericRecord> store;

    SessionRekeyProcessor(String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
      this.context = context;
      this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<GenericRecord, GenericRecord> record) {
      long t = record.timestamp();
      GenericRecord customerKey = customerKeyFrom(record.value());
      store.put(new Windowed<>(customerKey, new SessionWindow(t, t)),
          AggregationWithHeaders.make(record.value(), record.headers()));
      // Read the key back with a keyed session query (keyless findSessions is unreliable here).
      try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it =
          store.findSessions(customerKey, 0L, Long.MAX_VALUE)) {
        while (it.hasNext()) {
          KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = it.next();
          forwardReport(context, record.key(), kv.key.key(), record);
        }
      }
    }
  }

  // ---- helpers (mirroring the KIP-1271 store integration tests) ----

  private GenericRecord orderKey(String orderId) {
    GenericRecord key = new GenericData.Record(ORDER_KEY_SCHEMA);
    key.put("orderId", orderId);
    return key;
  }

  private GenericRecord orderValue(String customerId) {
    GenericRecord value = new GenericData.Record(ORDER_VALUE_SCHEMA);
    value.put("customerId", customerId);
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
