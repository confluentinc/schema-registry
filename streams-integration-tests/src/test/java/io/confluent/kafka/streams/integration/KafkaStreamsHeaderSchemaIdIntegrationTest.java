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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies a Kafka Streams stateless filter topology works correctly with
 * {@link GenericAvroSerde} configured to use {@link HeaderSchemaIdSerializer} for header-based
 * schema ID transport. Both keys and values use Avro with header-based schema IDs.
 */
public class KafkaStreamsHeaderSchemaIdIntegrationTest extends ClusterTestHarness {

  private static final String INPUT_TOPIC = "sensor-readings-input";
  private static final String OUTPUT_TOPIC = "sensor-readings-output";

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

  public KafkaStreamsHeaderSchemaIdIntegrationTest() {
    super(1, true);
  }

  @Test
  public void shouldFilterRecordsUsingHeaderBasedSchemaId() throws Exception {
    Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    // Need to manually create topics here since we're waiting for app to be in RUNNING state
    // before starting to produce
    Properties adminProps = new Properties();
    adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient admin = AdminClient.create(adminProps)) {
      admin
          .createTopics(
              Arrays.asList(new NewTopic(INPUT_TOPIC, 1, (short) 1),
                  new NewTopic(OUTPUT_TOPIC, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }

    // Build the Kafka Streams topology
    StreamsBuilder builder = new StreamsBuilder();

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

    builder.stream(
            INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
        .filter(
            (key, value) ->
                key.get("sensorId") != null && (double) value.get("temperature") > 30.0)
        .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

    Properties streamsProps = new Properties();
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "header-schema-id-integration-test");
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

      // Produce test records using KafkaAvroSerializer with HeaderSchemaIdSerializer
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
        GenericRecord hotKey = new GenericData.Record(keySchema);
        hotKey.put("sensorId", "sensor-1");

        GenericRecord hotReading = new GenericData.Record(valueSchema);
        hotReading.put("temperature", 35.5);
        hotReading.put("timestamp", System.currentTimeMillis());

        GenericRecord coldKey = new GenericData.Record(keySchema);
        coldKey.put("sensorId", "sensor-2");

        GenericRecord coldReading = new GenericData.Record(valueSchema);
        coldReading.put("temperature", 20.0);
        coldReading.put("timestamp", System.currentTimeMillis());

        producer.send(new ProducerRecord<>(INPUT_TOPIC, hotKey, hotReading)).get();
        producer.send(new ProducerRecord<>(INPUT_TOPIC, coldKey, coldReading)).get();
        producer.flush();
      }

      // Consume from the output topic and verify
      Properties consumerProps = new Properties();
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "header-schema-id-test-consumer");
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      consumerProps.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
      consumerProps.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
      consumerProps.put(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
      consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

      List<ConsumerRecord<GenericRecord, GenericRecord>> results = new ArrayList<>();
      try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
          new KafkaConsumer<>(consumerProps)) {
        consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

        // Poll until we get the expected record or timeout
        long deadline = System.currentTimeMillis() + 30_000;
        while (results.isEmpty() && System.currentTimeMillis() < deadline) {
          ConsumerRecords<GenericRecord, GenericRecord> records =
              consumer.poll(Duration.ofMillis(500));
          for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
            results.add(record);
          }
        }
      }

      // Assert only the hot reading passes the filter
      assertEquals(1, results.size(), "Only one record should pass the temperature > 30 filter");

      ConsumerRecord<GenericRecord, GenericRecord> result = results.get(0);

      // Verify the key
      GenericRecord key = result.key();
      assertNotNull(key, "Key should not be null");
      assertEquals("sensor-1", key.get("sensorId").toString());

      // Verify the value
      GenericRecord value = result.value();
      assertEquals(35.5, value.get("temperature"));

      // Verify __key_schema_id header is present with V1 magic byte (GUID format, 17 bytes)
      Header keySchemaIdHeader = result.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
      assertNotNull(keySchemaIdHeader, "Output record should have __key_schema_id header");
      byte[] keyHeaderBytes = keySchemaIdHeader.value();
      assertEquals(
          17, keyHeaderBytes.length, "Key GUID header should be 17 bytes (1 magic + 16 UUID)");
      assertEquals(
          SchemaId.MAGIC_BYTE_V1,
          keyHeaderBytes[0],
          "Key header should start with V1 magic byte for GUID format");

      // Verify __value_schema_id header is present with V1 magic byte (GUID format, 17 bytes)
      Header valueSchemaIdHeader = result.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
      assertNotNull(valueSchemaIdHeader, "Output record should have __value_schema_id header");
      byte[] valueHeaderBytes = valueSchemaIdHeader.value();
      assertEquals(
          17,
          valueHeaderBytes.length,
          "Value GUID header should be 17 bytes (1 magic + 16 UUID)");
      assertEquals(
          SchemaId.MAGIC_BYTE_V1,
          valueHeaderBytes[0],
          "Value header should start with V1 magic byte for GUID format");
    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }
}
