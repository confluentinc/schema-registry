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
import org.apache.kafka.common.header.Headers;
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
 * Demonstrates the re-keying hazard (PR #4409's {@code storedHeaderIdUnrelatedToRowKey} scenario) on
 * the header-aware stores (KIP-1271): {@link TimestampedKeyValueStoreWithHeaders},
 * {@link TimestampedWindowStoreWithHeaders}, and {@link SessionStoreWithHeaders} — one {@code @Test}
 * per store type.
 *
 * <p>A Processor-API processor consumes records keyed by {@code OrderKey{orderId}} and stores each
 * value under a key <em>derived from the value</em>, {@code CustomerKey{customerId}} — a normal
 * secondary-index / re-keying pattern where the store row key ({@code CustomerKey}) has a DIFFERENT
 * schema than the incoming record's key ({@code OrderKey}). In header mode
 * ({@link HeaderSchemaIdSerializer}) the store row bytes carry no schema id; the id used to decode a
 * stored key on read comes from {@code internalContext.headers()} — i.e. the CURRENTLY-processed
 * record's {@code __key_schema_id}, which describes {@code OrderKey}, not the stored
 * {@code CustomerKey}.
 *
 * <p>Each processor therefore sweeps the store (a key-returning read: {@code all()} for KV/window,
 * {@code findSessions(...)} for session) at the START of {@code process()} — before its own
 * {@code put}, so the in-flight headers still carry only {@code OrderKey}'s id — and reports the
 * schema of every reconstructed key. The stored {@code CustomerKey} comes back decoded as
 * {@code OrderKey} (both are a single string field, so the bytes decode silently), landing the
 * {@code customerId} value in an {@code orderId} field: the stored {@code __key_schema_id} described a
 * different key than the row, and the reconstruction is silently wrong.
 *
 * <p>Unlike a row-collapse demonstration, this needs no contrived "two key schemas on one topic":
 * the input topic has a single key schema, and the second schema arises naturally from the
 * value-derived store key (they even register under different subjects — input topic vs. changelog).
 * The hazard is Processor-API specific; the DSL always keys stores by {@code record.key()}.
 */
public class RekeyedStoreKeyDecodesUnderWrongSchemaIdIntegrationTest extends ClusterTestHarness {

  private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
  private static final Duration RETENTION_PERIOD = Duration.ofHours(1);
  private static final Duration SESSION_GAP = Duration.ofMinutes(30);

  // Incoming records are keyed by OrderKey; the store row key is a CustomerKey derived from the
  // value. Both are a single string field, so a CustomerKey's bytes decode without error under the
  // OrderKey schema — the reconstruction is silently wrong rather than a hard failure.
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
  // Report emitted by the sweep: the schema name a stored key was reconstructed under, and the value
  // that landed in that reconstructed key's (first) field.
  private static final Schema REPORT_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"SweepReport\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":[{\"name\":\"reconstructedKeySchema\",\"type\":\"string\"},"
          + "{\"name\":\"reconstructedValue\",\"type\":\"string\"}]}");

  public RekeyedStoreKeyDecodesUnderWrongSchemaIdIntegrationTest() {
    super(1, true);
  }

  @Test
  public void keyValueStore_rekeyedStoreKeyDecodesUnderWrongSchemaId() throws Exception {
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

    runRekeyScenarioAndAssert(builder.build(), "rekey-kv-test", input, output, "rekey-kv-consumer");
  }

  @Test
  public void windowStore_rekeyedStoreKeyDecodesUnderWrongSchemaId() throws Exception {
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

    runRekeyScenarioAndAssert(builder.build(), "rekey-window-test", input, output,
        "rekey-window-consumer");
  }

  @Test
  public void sessionStore_rekeyedStoreKeyDecodesUnderWrongSchemaId() throws Exception {
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

    runRekeyScenarioAndAssert(builder.build(), "rekey-session-test", input, output,
        "rekey-session-consumer");
  }

  /**
   * Produces two OrderKey records (customerId "cust-1" then "cust-2"). The processor stores each under
   * a value-derived CustomerKey, sweeping the store first. The sweep on the second record finds the
   * first CustomerKey and reconstructs it under the in-flight OrderKey id, so the report's
   * reconstructed schema is OrderKey (wrong), with "cust-1" mis-filed into it.
   */
  private void runRekeyScenarioAndAssert(Topology topology, String appId, String input,
      String output, String consumerGroup) throws Exception {
    KafkaStreams streams = null;
    try {
      streams = startStreamsAndAwaitRunning(topology, appId);

      long ts = System.currentTimeMillis();
      try (KafkaProducer<GenericRecord, GenericRecord> producer =
          new KafkaProducer<>(createProducerProps())) {
        producer.send(new ProducerRecord<>(input, null, ts, orderKey("o1"), orderValue("cust-1"))).get();
        producer.send(new ProducerRecord<>(input, null, ts, orderKey("o2"), orderValue("cust-2"))).get();
        producer.flush();
      }

      // Exactly one report: the second record's sweep sees the first stored CustomerKey.
      List<ConsumerRecord<GenericRecord, GenericRecord>> results =
          consumeRecords(output, consumerGroup, 1);

      GenericRecord report = results.get(0).value();
      // OrderKey and CustomerKey are both a single string field, so they share an identical Avro wire
      // format and the CustomerKey bytes decode silently as an OrderKey. Had the two key schemas been
      // wire-incompatible (e.g. long vs string), this decode under the wrong id would instead THROW a
      // SerializationException mid-iteration — a hard failure rather than a silent wrong reconstruction.
      assertEquals("OrderKey", report.get("reconstructedKeySchema").toString(),
          "stored CustomerKey was reconstructed under the in-flight OrderKey id (wrong schema)");
      assertNotEquals("CustomerKey", report.get("reconstructedKeySchema").toString(),
          "the row's real key schema was NOT recovered");
      assertEquals("cust-1", report.get("reconstructedValue").toString(),
          "the customerId value landed in the wrong (orderId) field");
    } finally {
      closeStreams(streams);
    }
  }

  private static void forwardReport(ProcessorContext<GenericRecord, GenericRecord> context,
      GenericRecord originalKey, GenericRecord reconstructedKey, long timestamp, Headers headers) {
    GenericRecord report = new GenericData.Record(REPORT_SCHEMA);
    report.put("reconstructedKeySchema", reconstructedKey.getSchema().getName());
    String firstField = reconstructedKey.getSchema().getFields().get(0).name();
    report.put("reconstructedValue", String.valueOf(reconstructedKey.get(firstField)));
    context.forward(new Record<>(originalKey, report, timestamp, headers));
  }

  private static GenericRecord customerKeyFrom(GenericRecord orderValue) {
    GenericRecord customerKey = new GenericData.Record(CUSTOMER_KEY_SCHEMA);
    customerKey.put("customerId", orderValue.get("customerId"));
    return customerKey;
  }

  // ---- processors (one per store type); each sweeps BEFORE its own put ----

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
      try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> it = store.all()) {
        while (it.hasNext()) {
          KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> kv = it.next();
          forwardReport(context, record.key(), kv.key, record.timestamp(), record.headers());
        }
      }
      store.put(customerKeyFrom(record.value()),
          ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
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
      try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> it =
          store.all()) {
        while (it.hasNext()) {
          KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> kv = it.next();
          forwardReport(context, record.key(), kv.key.key(), record.timestamp(), record.headers());
        }
      }
      long windowStart = record.timestamp() - (record.timestamp() % WINDOW_SIZE.toMillis());
      store.put(customerKeyFrom(record.value()),
          ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()), windowStart);
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
      try (KeyValueIterator<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> it =
          store.findSessions(0L, Long.MAX_VALUE)) {
        while (it.hasNext()) {
          KeyValue<Windowed<GenericRecord>, AggregationWithHeaders<GenericRecord>> kv = it.next();
          forwardReport(context, record.key(), kv.key.key(), record.timestamp(), record.headers());
        }
      }
      long t = record.timestamp();
      Windowed<GenericRecord> sessionKey =
          new Windowed<>(customerKeyFrom(record.value()), new SessionWindow(t, t));
      store.put(sessionKey, AggregationWithHeaders.make(record.value(), record.headers()));
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
