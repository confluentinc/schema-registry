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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies a Kafka Streams stateless filter topology works correctly with
 * {@link GenericAvroSerde} configured to use {@link HeaderSchemaIdSerializer} for header-based
 * schema ID transport. Both keys and values use Avro with header-based schema IDs.
 */
public class StatelessHeaderIntegrationTest extends ClusterTestHarness {

  private static final String INPUT_TOPIC = "sensor-readings-input";

  private static final String KEY_SCHEMA_JSON =
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"}"
          + "]"
          + "}";

  private static final String VALUE_SCHEMA_JSON =
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"}"
          + "]"
          + "}";

  public StatelessHeaderIntegrationTest() {
    super(1, true);
  }

  /**
   * Verifies that header-based schema IDs survive every stateless operator that re-serializes
   * records: filter, mapValues, map, selectKey, flatMapValues, and branch. Each operator's output
   * is written to its own topic via {@code .to(...)} so re-serialization is forced; we then assert
   * the schema-ID headers are present on each output topic.
   */
  @Test
  public void shouldPreserveHeadersAcrossStatelessOperators() throws Exception {
    String filterOutput = "stateless-filter-output";
    String mapValuesOutput = "stateless-mapvalues-output";
    String mapOutput = "stateless-map-output";
    String selectKeyOutput = "stateless-selectkey-output";
    String flatMapValuesOutput = "stateless-flatmapvalues-output";
    String branchHotOutput = "stateless-branch-hot-output";

    Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    Properties adminProps = new Properties();
    adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient admin = AdminClient.create(adminProps)) {
      admin.createTopics(Arrays.asList(
          new NewTopic(INPUT_TOPIC, 1, (short) 1),
          new NewTopic(filterOutput, 1, (short) 1),
          new NewTopic(mapValuesOutput, 1, (short) 1),
          new NewTopic(mapOutput, 1, (short) 1),
          new NewTopic(selectKeyOutput, 1, (short) 1),
          new NewTopic(flatMapValuesOutput, 1, (short) 1),
          new NewTopic(branchHotOutput, 1, (short) 1)
      )).all().get(30, TimeUnit.SECONDS);
    }

    Map<String, Object> keySerdeConfig = new HashMap<>();
    keySerdeConfig.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    keySerdeConfig.put(
        AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    GenericAvroSerde keySerde = new GenericAvroSerde();
    keySerde.configure(keySerdeConfig, true);

    Map<String, Object> valueSerdeConfig = new HashMap<>();
    valueSerdeConfig.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    valueSerdeConfig.put(
        AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    GenericAvroSerde valueSerde = new GenericAvroSerde();
    valueSerde.configure(valueSerdeConfig, false);

    StreamsBuilder builder = new StreamsBuilder();
    KStream<GenericRecord, GenericRecord> source =
        builder.stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde));

    source.filter((key, value) -> (double) value.get("temperature") > 30.0)
        .to(filterOutput, Produced.with(keySerde, valueSerde));

    source.mapValues(value -> value)
        .to(mapValuesOutput, Produced.with(keySerde, valueSerde));

    source.map((key, value) -> new KeyValue<>(key, value))
        .to(mapOutput, Produced.with(keySerde, valueSerde));

    source.selectKey((key, value) -> key)
        .to(selectKeyOutput, Produced.with(keySerde, valueSerde));

    source.flatMapValues(value -> Collections.singletonList(value))
        .to(flatMapValuesOutput, Produced.with(keySerde, valueSerde));

    source.split()
        .branch(
            (key, value) -> (double) value.get("temperature") > 0,
            Branched.withConsumer(
                s -> s.to(branchHotOutput, Produced.with(keySerde, valueSerde))));

    Properties streamsProps = new Properties();
    streamsProps.put(
        StreamsConfig.APPLICATION_ID_CONFIG, "stateless-operators-integration-test");
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    streamsProps.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);

    KafkaStreams streams = null;
    try {
      CountDownLatch startedLatch = new CountDownLatch(1);
      streams = new KafkaStreams(builder.build(), streamsProps);
      streams.setStateListener(
          (newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
              startedLatch.countDown();
            }
          });
      streams.start();
      assertTrue(
          startedLatch.await(30, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");

      Properties producerProps = new Properties();
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
      producerProps.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
      producerProps.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
      producerProps.put(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
      producerProps.put(
          AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
          HeaderSchemaIdSerializer.class.getName());
      producerProps.put(
          AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
          HeaderSchemaIdSerializer.class.getName());

      try (KafkaProducer<GenericRecord, GenericRecord> producer =
               new KafkaProducer<>(producerProps)) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("sensorId", "sensor-1");
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("temperature", 42.0);
        value.put("timestamp", System.currentTimeMillis());
        producer.send(new ProducerRecord<>(INPUT_TOPIC, key, value)).get();
        producer.flush();
      }

      for (String outputTopic : Arrays.asList(
          filterOutput, mapValuesOutput, mapOutput, selectKeyOutput, flatMapValuesOutput, branchHotOutput)) {
        ConsumerRecord<GenericRecord, GenericRecord> result =
            consumeFirstRecord(outputTopic, "stateless-" + outputTopic + "-consumer");
        assertNotNull(result, "Expected one record on " + outputTopic);
        assertSchemaIdHeaders(result.headers(), outputTopic);
      }
    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  private ConsumerRecord<GenericRecord, GenericRecord> consumeFirstRecord(
      String topic, String groupId) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    consumerProps.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

    try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
             new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Collections.singletonList(topic));
      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<GenericRecord, GenericRecord> records =
            consumer.poll(Duration.ofMillis(500));
        if (!records.isEmpty()) {
          return records.iterator().next();
        }
      }
    }
    return null;
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
}
