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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies a Kafka Streams stateless filter topology works correctly with
 * {@link GenericAvroSerde} configured to use {@link HeaderSchemaIdSerializer} for header-based
 * schema ID transport.
 */
public class KafkaStreamsHeaderSchemaIdIntegrationTest extends ClusterTestHarness {

  private static final String INPUT_TOPIC = "sensor-readings-input";
  private static final String OUTPUT_TOPIC = "sensor-readings-output";

  private static final String SENSOR_SCHEMA_JSON =
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"},"
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"}"
          + "]"
          + "}";

  public KafkaStreamsHeaderSchemaIdIntegrationTest() {
    super(1, true);
  }

  @Test
  public void shouldFilterRecordsUsingHeaderBasedSchemaId() throws Exception {
    Schema schema = new Schema.Parser().parse(SENSOR_SCHEMA_JSON);

    // Build the Kafka Streams topology
    StreamsBuilder builder = new StreamsBuilder();

    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    serdeConfig.put(
        AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());

    GenericAvroSerde valueSerde = new GenericAvroSerde();
    valueSerde.configure(serdeConfig, false);

    builder.<String, GenericRecord>stream(
            INPUT_TOPIC, Consumed.with(Serdes.String(), valueSerde))
        .filter((key, value) -> (double) value.get("temperature") > 30.0)
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), valueSerde));

    Properties streamsProps = new Properties();
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "header-schema-id-integration-test");
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    streamsProps.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
    streams.start();

    try {
      // Produce test records using KafkaAvroSerializer with HeaderSchemaIdSerializer
      Properties producerProps = new Properties();
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
      producerProps.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      producerProps.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
      producerProps.put(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
      producerProps.put(
          AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
          HeaderSchemaIdSerializer.class.getName());

      try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerProps)) {
        GenericRecord hotReading = new GenericData.Record(schema);
        hotReading.put("sensorId", "sensor-1");
        hotReading.put("temperature", 35.5);
        hotReading.put("timestamp", System.currentTimeMillis());

        GenericRecord coldReading = new GenericData.Record(schema);
        coldReading.put("sensorId", "sensor-2");
        coldReading.put("temperature", 20.0);
        coldReading.put("timestamp", System.currentTimeMillis());

        producer.send(new ProducerRecord<>(INPUT_TOPIC, "sensor-1", hotReading)).get();
        producer.send(new ProducerRecord<>(INPUT_TOPIC, "sensor-2", coldReading)).get();
        producer.flush();
      }

      // Consume from the output topic and verify
      Properties consumerProps = new Properties();
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "header-schema-id-test-consumer");
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      consumerProps.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      consumerProps.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
      consumerProps.put(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
      consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

      List<ConsumerRecord<String, GenericRecord>> results = new ArrayList<>();
      try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProps)) {
        consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

        // Poll until we get the expected record or timeout
        long deadline = System.currentTimeMillis() + 30_000;
        while (results.size() < 1 && System.currentTimeMillis() < deadline) {
          ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
          for (ConsumerRecord<String, GenericRecord> record : records) {
            results.add(record);
          }
        }
      }

      // Assert only the hot reading passes the filter
      assertEquals(1, results.size(), "Only one record should pass the temperature > 30 filter");

      ConsumerRecord<String, GenericRecord> result = results.get(0);
      assertEquals("sensor-1", result.key());

      GenericRecord value = result.value();
      assertEquals("sensor-1", value.get("sensorId").toString());
      assertEquals(35.5, (double) value.get("temperature"), 0.001);

      // Verify the __value_schema_id header is present with V1 magic byte (GUID format, 17 bytes)
      Header schemaIdHeader = result.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
      assertNotNull(schemaIdHeader, "Output record should have __value_schema_id header");
      byte[] headerBytes = schemaIdHeader.value();
      assertEquals(17, headerBytes.length, "GUID header should be 17 bytes (1 magic + 16 UUID)");
      assertEquals(
          SchemaId.MAGIC_BYTE_V1,
          headerBytes[0],
          "Header should start with V1 magic byte for GUID format");
    } finally {
      streams.close(Duration.ofSeconds(10));
    }
  }
}
