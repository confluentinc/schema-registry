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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for schema evolution with header-based KV state stores.
 *
 * <p>Tests validate that Kafka Streams header-aware state stores correctly handle
 * Avro schema evolution for both keys and values when using {@link HeaderSchemaIdSerializer}.
 *
 */
public class KafkaStreamsHeaderKVStoreSchemaEvolutionIntegrationTest extends ClusterTestHarness {

  private static final String STORE_NAME = "schema-evolution-store";

  // --- Key schemas ---
  private static final Schema KEY_SCHEMA_V1 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"}"
          + "]"
          + "}");

  private static final Schema KEY_SCHEMA_V2 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"},"
          + "  {\"name\":\"region\",\"type\":\"string\",\"default\":\"us-east\"}"
          + "]"
          + "}");

  // Differs from KEY_SCHEMA_V1 only in `doc`. Avro does not write `doc` to the binary,
  // so the same logical key serializes to identical bytes under either schema.
  private static final Schema KEY_SCHEMA_V1_DOC_CHANGED = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"doc\":\"Updated documentation for sensor key\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"}"
          + "]"
          + "}");

  // --- Value schemas ---
  private static final Schema VALUE_SCHEMA_V1 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"}"
          + "]"
          + "}");

  private static final Schema VALUE_SCHEMA_V2 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"},"
          + "  {\"name\":\"humidity\",\"type\":\"double\",\"default\":0.0}"
          + "]"
          + "}");

  // v3: adds `pressure` with default — backward-compatible with v2.
  private static final Schema VALUE_SCHEMA_V3 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"},"
          + "  {\"name\":\"humidity\",\"type\":\"double\",\"default\":0.0},"
          + "  {\"name\":\"pressure\",\"type\":\"double\",\"default\":1013.0}"
          + "]"
          + "}");

  // v4: removes `humidity` from v3.
  private static final Schema VALUE_SCHEMA_V4_NO_HUMIDITY = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"},"
          + "  {\"name\":\"pressure\",\"type\":\"double\",\"default\":1013.0}"
          + "]"
          + "}");

  private static final Schema VALUE_SCHEMA_V3_NO_DEFAULT = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"},"
          + "  {\"name\":\"humidity\",\"type\":\"double\"},"
          + "  {\"name\":\"pressure\",\"type\":\"double\"}"
          + "]"
          + "}");

  // Incompatible: changes `temperature` from double → string.
  // Under BACKWARD compatibility, SR should reject this schema change.
  private static final Schema VALUE_SCHEMA_INCOMPATIBLE = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"string\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"}"
          + "]"
          + "}");

  // Complex-type v1: enum {RED, GREEN}, no tags map.
  private static final Schema VALUE_SCHEMA_ENUM_MAP_V1 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"color\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
          + "     \"symbols\":[\"RED\",\"GREEN\"]}}"
          + "]"
          + "}");

  // Complex-type v2: enum expanded with BLUE (default RED); Map of tags added with default {}.
  private static final Schema VALUE_SCHEMA_ENUM_MAP_V2 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"color\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
          + "     \"symbols\":[\"RED\",\"GREEN\",\"BLUE\"],\"default\":\"RED\"}},"
          + "  {\"name\":\"tags\",\"type\":{\"type\":\"map\",\"values\":\"string\"},"
          + "     \"default\":{}}"
          + "]"
          + "}");

  public KafkaStreamsHeaderKVStoreSchemaEvolutionIntegrationTest() {
    super(1, true);
  }

  /**
   * Value schema evolves (v1 → v2, new field with default). Key schema is unchanged, so
   * key bytes are unchanged and each logical key maps to exactly one store row: old values
   * written under v1 remain readable and new values written under v2 coexist.
   */
  @Test
  public void shouldReadOldAndNewValuesAfterValueSchemaEvolution() throws Exception {
    String inputTopic = "value-evolution-input";
    String appId = "value-evolution-test-" + System.currentTimeMillis();

    KafkaStreams streams = null;
    try {
      streams = startTableApp(inputTopic, appId);

      GenericRecord key1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-1").build();
      GenericRecord key2 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-2").build();
      // Produce records with value schema v1
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {

        GenericRecord val1 = new GenericRecordBuilder(VALUE_SCHEMA_V1)
            .set("temperature", 35.5).set("timestamp", 1000L).build();
        GenericRecord val2 = new GenericRecordBuilder(VALUE_SCHEMA_V1)
            .set("temperature", 22.0).set("timestamp", 2000L).build();

        producer.send(new ProducerRecord<>(inputTopic, key1, val1)).get();
        producer.send(new ProducerRecord<>(inputTopic, key2, val2)).get();
        producer.flush();
      }

      waitForStoreToContainKeys(streams, 2);

      // --- Test 1: Register a new schema, but not producing any new records with it — old values should still be readable under the evolved reader ---
      // Register value schema v2 in SR WITHOUT producing any v2 records
      restApp.restClient.registerSchema(VALUE_SCHEMA_V2.toString(), inputTopic + "-value");

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> storeBeforeV2Writes =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      ValueTimestampHeaders<GenericRecord> sensor1BeforeV2 =
          storeBeforeV2Writes.get(key1);
      assertNotNull(sensor1BeforeV2, "sensor-1 should be readable after SR evolution with no v2 writes");
      assertEquals(35.5, sensor1BeforeV2.value().get("temperature"));
      assertThrows(AvroRuntimeException.class,
          () -> sensor1BeforeV2.value().get("humidity"),
          "humidity should not be present — entry is still v1 bytes, SR evolution alone adds no field");
      assertSchemaIdHeaders(sensor1BeforeV2.headers(), "sensor1BeforeV2");

      ValueTimestampHeaders<GenericRecord> sensor2BeforeV2 =
          storeBeforeV2Writes.get(key2);
      assertNotNull(sensor2BeforeV2, "sensor-2 should be readable after SR evolution with no v2 writes");
      assertEquals(22.0, sensor2BeforeV2.value().get("temperature"));
      assertThrows(AvroRuntimeException.class,
          () -> sensor2BeforeV2.value().get("humidity"),
          "humidity should not be present — entry is still v1 bytes, SR evolution alone adds no field");
      assertSchemaIdHeaders(sensor2BeforeV2.headers(), "sensor2BeforeV2");

      // Re-read the same v1 bytes off the input topic with v2 reader schema,
      // Avro schema resolution should fill humidity with the v2 default (0.0).
      List<ConsumerRecord<byte[], byte[]>> rawV1Records = consumeRawChangelog(
          inputTopic, "v2-reader-" + System.currentTimeMillis(), 2);
      assertEquals(2, rawV1Records.size(), "should have consumed both v1-written input records");
      KafkaAvroDeserializer v2Reader = new KafkaAvroDeserializer();
      Map<String, Object> v2ReaderConfig = new HashMap<>();
      v2ReaderConfig.put(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
      v2Reader.configure(v2ReaderConfig, false);
      try {
        for (ConsumerRecord<byte[], byte[]> r : rawV1Records) {
          GenericRecord asV2 = (GenericRecord) v2Reader.deserialize(
              inputTopic, r.headers(), r.value(), VALUE_SCHEMA_V2);
          assertNotNull(asV2, "v1 bytes should be decodable with v2 reader schema");
          assertEquals(VALUE_SCHEMA_V2, asV2.getSchema(), "projection should be v2-shaped");
          assertEquals(0.0, asV2.get("humidity"),
              "humidity should be filled in from the v2 default (0.0) when reading v1 bytes as v2");
        }
      } finally {
        v2Reader.close();
      }

      // --- Test 2: Produce records with evolved value schema v2, should override the old value with the same key ---
      GenericRecord key3 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-v2").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        // Update sensor-1 with v2 schema
        GenericRecord val1v2 = new GenericRecordBuilder(VALUE_SCHEMA_V2)
            .set("temperature", 36.0).set("timestamp", 3000L).set("humidity", 65.0).build();

        // Add a new sensor with v2 schema
        GenericRecord val3 = new GenericRecordBuilder(VALUE_SCHEMA_V2)
            .set("temperature", 28.0).set("timestamp", 4000L).set("humidity", 70.0).build();

        producer.send(new ProducerRecord<>(inputTopic, key1, val1v2)).get();
        producer.send(new ProducerRecord<>(inputTopic, key3, val3)).get();
        producer.flush();
      }

      waitForStoreToContainKeys(streams, 3);

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      // sensor-1: v2 value overwrote v1 value at the same row
      ValueTimestampHeaders<GenericRecord> result1 = store.get(key1);
      assertNotNull(result1, "sensor-1 should be in the store");
      assertEquals(36.0, result1.value().get("temperature"));
      assertEquals(65.0, result1.value().get("humidity"));
      assertSchemaIdHeaders(result1.headers(), "result1");

      // sensor-2: v1 value readable under the old key schema
      ValueTimestampHeaders<GenericRecord> result2 = store.get(key2);
      assertNotNull(result2, "sensor-2 should still be readable (value written with v1)");
      assertEquals(22.0, result2.value().get("temperature"));
      assertThrows(AvroRuntimeException.class, () -> result2.value().get("humidity"), "humidity should be null for v1-written value when read with v2 schema");
      assertSchemaIdHeaders(result2.headers(), "result2");

      // sensor-v2: written directly with v2
      ValueTimestampHeaders<GenericRecord> result3 = store.get(key3);
      assertNotNull(result3, "sensor-v2 should be in the store");
      assertEquals(28.0, result3.value().get("temperature"));
      assertEquals(70.0, result3.value().get("humidity"));
      assertSchemaIdHeaders(result3.headers(), "result3");

      assertEquals(3, countStoreEntries(store), "Store should contain exactly 3 entries");

      // --- Test 3: Evolve to another new value schema with an added field ---
      GenericRecord key4 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-v3").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        GenericRecord val4 = new GenericRecordBuilder(VALUE_SCHEMA_V3)
            .set("temperature", 18.0).set("timestamp", 5000L).set("humidity", 55.0).set("pressure", 1020.0).build();
        producer.send(new ProducerRecord<>(inputTopic, key4, val4)).get();
        producer.flush();
      }
      waitForStoreToContainKeys(streams, 4);

      // sensor-v3 is v3-shaped
      ValueTimestampHeaders<GenericRecord> resultV3 = store.get(key4);
      assertNotNull(resultV3, "sensor-v3 should be in the store");
      assertEquals(18.0, resultV3.value().get("temperature"));
      assertEquals(55.0, resultV3.value().get("humidity"));
      assertEquals(1020.0, resultV3.value().get("pressure"));
      assertSchemaIdHeaders(resultV3.headers(), "resultV3");

      // sensor-1 is v2-shaped
      ValueTimestampHeaders<GenericRecord> result1AfterV3 = store.get(key1);
      assertEquals(36.0, result1AfterV3.value().get("temperature"));
      assertEquals(65.0, result1AfterV3.value().get("humidity"));
      assertThrows(AvroRuntimeException.class, () -> result1AfterV3.value().get("pressure"),
          "pressure should not exist on v2-written record");
      assertSchemaIdHeaders(result1AfterV3.headers(), "result1AfterV3");

      // sensor-2 is v1-shaped
      ValueTimestampHeaders<GenericRecord> result2AfterV3 =
          store.get(key2);
      assertThrows(AvroRuntimeException.class, () -> result2AfterV3.value().get("humidity"));
      assertThrows(AvroRuntimeException.class, () -> result2AfterV3.value().get("pressure"));
      assertSchemaIdHeaders(result2AfterV3.headers(), "result2AfterV3");

      assertEquals(4, countStoreEntries(store), "Store should contain 4 entries after v3 write");

      // --- Test 4: v4 removes `humidity` field ---
      GenericRecord key5 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-v4").build();
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        GenericRecord valV4 = new GenericRecordBuilder(VALUE_SCHEMA_V4_NO_HUMIDITY)
            .set("temperature", 30.0).set("timestamp", 6000L).set("pressure", 1005.0).build();
        producer.send(new ProducerRecord<>(inputTopic, key5, valV4)).get();
        producer.flush();
      }
      waitForStoreToContainKeys(streams, 5);

      // sensor-v4 has temperature + pressure but no humidity
      ValueTimestampHeaders<GenericRecord> resultV4 = store.get(key5);
      assertNotNull(resultV4, "sensor-v4 should be in the store");
      assertEquals(30.0, resultV4.value().get("temperature"));
      assertEquals(1005.0, resultV4.value().get("pressure"));
      assertThrows(AvroRuntimeException.class, () -> resultV4.value().get("humidity"),
          "humidity was removed in v4 — not present on v4-written record");
      assertSchemaIdHeaders(resultV4.headers(), "resultV4");

      // sensor-1 (v2) still v2-shaped — removing a field in v4 doesn't rewrite existing bytes
      ValueTimestampHeaders<GenericRecord> result1AfterV4 =
          store.get(key1);
      assertEquals(65.0, result1AfterV4.value().get("humidity"));
      assertSchemaIdHeaders(result1AfterV4.headers(), "result1AfterV4");

      assertEquals(5, countStoreEntries(store), "Store should contain 5 entries after v4 write");

      // --- Test 4: incompatible type change in value schema ---
      // ClusterTestHarness defaults compatibility to NONE; switch the subject to BACKWARD,
      // SR should reject incompatible schema changes in this case
      restApp.restClient.updateCompatibility("BACKWARD", inputTopic + "-value");

      assertThrows(RestClientException.class,
          () -> restApp.restClient.registerSchema(
              VALUE_SCHEMA_INCOMPATIBLE.toString(), inputTopic + "-value"),
          "SR should reject registering a schema that changes temperature double → string "
              + "under BACKWARD compatibility");

      // Failed registration should not affect existing store entries
      assertEquals(5, countStoreEntries(store),
          "Store entry count should be unchanged after rejected registration");

      // Change back ClusterTestHarness compatibility to NONE
      restApp.restClient.updateCompatibility("NONE", inputTopic + "-value");
      assertDoesNotThrow(() -> restApp.restClient.registerSchema(
              VALUE_SCHEMA_INCOMPATIBLE.toString(), inputTopic + "-value"),
          "SR should not reject registering a schema that changes temperature double → string "
              + "under NONE compatibility");

      // Should not affect existing store entries
      assertEquals(5, countStoreEntries(store),
          "Store entry count should be unchanged after rejected registration");

      // Overwrite sensor-2 with the new schema that changes the type of temperature
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        GenericRecord newVal2 = new GenericRecordBuilder(VALUE_SCHEMA_INCOMPATIBLE)
            .set("temperature", "25.0").set("timestamp", 7000L).build();
        producer.send(new ProducerRecord<>(inputTopic, key2, newVal2)).get();
        producer.flush();
      }

      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline) {
        ValueTimestampHeaders<GenericRecord> r =
            store.get(key2);
        if (r != null && "25.0".equals(r.value().get("temperature").toString())) {
          break;
        }
        Thread.sleep(200);
      }

      ValueTimestampHeaders<GenericRecord> resultNewKey2 =
          store.get(key2);
      assertNotNull(resultNewKey2, "sensor-2 should be in the store");
      // Avro returns Utf8, not String — compare via toString().
      assertEquals("25.0", resultNewKey2.value().get("temperature").toString());
      assertSchemaIdHeaders(resultNewKey2.headers(), "resultNewKey2");

    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  /**
   * Producing a record with the same {@code sensorId} under an evolved key schema
   * (v1 → v2, adding a field with default) creates a second store row: v2's extra defaulted
   * field causes the key to serialize to different bytes than v1, so v1 and v2 lookups
   * address different rows and {@code all()} yields both.
   */
  @Test
  public void shouldStoreSameLogicalKeyAsTwoRowsAfterKeySchemaEvolution() throws Exception {
    String inputTopic = "key-evolution-input";
    String appId = "key-evolution-test-" + System.currentTimeMillis();

    KafkaStreams streams = null;
    try {
      streams = startTableApp(inputTopic, appId);

      GenericRecord keyV1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-1").build();
      GenericRecord keyV2 = new GenericRecordBuilder(KEY_SCHEMA_V2)
          .set("sensorId", "sensor-1").set("region", "us-east").build();

      // Produce records with key schema v1
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        GenericRecord val = new GenericRecordBuilder(VALUE_SCHEMA_V1)
            .set("temperature", 35.5).set("timestamp", 1000L).build();

        producer.send(new ProducerRecord<>(inputTopic, keyV1, val)).get();
        producer.flush();
      }

      waitForStoreToContainKeys(streams, 1);

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      // Lookup with v1-serialized key works
      ValueTimestampHeaders<GenericRecord> resultV1 = store.get(keyV1);
      assertNotNull(resultV1, "Lookup with v1 key should find the entry");
      assertEquals(35.5, resultV1.value().get("temperature"));
      assertSchemaIdHeaders(resultV1.headers(), "resultV1");

      // Register value schema v2 in SR WITHOUT producing any v2 records
      restApp.restClient.registerSchema(KEY_SCHEMA_V2.toString(), inputTopic + "-value");

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> storeBeforeK2Writes =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      ValueTimestampHeaders<GenericRecord> sensor1BeforeV2 =
          storeBeforeK2Writes.get(keyV1);
      assertNotNull(sensor1BeforeV2, "sensor-1 should be readable after SR evolution with no v2 writes");
      assertEquals(35.5, sensor1BeforeV2.value().get("temperature"));
      assertThrows(AvroRuntimeException.class,
          () -> sensor1BeforeV2.value().get("humidity"),
          "humidity should not be present — entry is still v1 bytes, SR evolution alone adds no field");
      assertSchemaIdHeaders(sensor1BeforeV2.headers(), "sensor1BeforeV2");

      // Produce the same logical key with evolved key schema v2
      // v2 adds "region" field with default "us-east" — this changes the Avro binary
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        GenericRecord val = new GenericRecordBuilder(VALUE_SCHEMA_V1)
            .set("temperature", 40.0).set("timestamp", 2000L).build();

        producer.send(new ProducerRecord<>(inputTopic, keyV2, val)).get();
        producer.flush();
      }

      waitForStoreToContainKeys(streams, 2);

      // Lookup with v2 key finds ONLY the new entry (different byte representation)
      ValueTimestampHeaders<GenericRecord> resultV2 = store.get(keyV2);
      assertNotNull(resultV2, "Lookup with v2 key should find the new entry");
      assertEquals(40.0, resultV2.value().get("temperature"),
          "v2 key lookup should return the value produced with v2 key");
      assertSchemaIdHeaders(resultV2.headers(), "resultV2");

      // Lookup with v1 key STILL finds the old entry (it was not overwritten)
      ValueTimestampHeaders<GenericRecord> resultV1Again = store.get(keyV1);
      assertNotNull(resultV1Again,
          "v1 key entry should still exist — v2 key has different bytes, so it did not overwrite");
      assertEquals(35.5, resultV1Again.value().get("temperature"),
          "Old entry should retain original value");
      assertSchemaIdHeaders(resultV1Again.headers(), "resultV1Again");

      // Iterator shows both entries (duplicate logical key, different byte keys)
      List<String> sensorIds = new ArrayList<>();
      List<Double> temperatures = new ArrayList<>();
      try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
        while (iter.hasNext()) {
          KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>> entry = iter.next();
          sensorIds.add(entry.key.get("sensorId").toString());
          temperatures.add((double) entry.value.value().get("temperature"));
          assertSchemaIdHeaders(entry.value.headers(), "iterator entry " + entry.key.get("sensorId"));
        }
      }
      assertEquals(2, sensorIds.size(),
          "Store should have 2 entries: same logical key 'sensor-1' stored twice "
              + "with different byte representations (v1 and v2 key schemas)");
      assertTrue(
          sensorIds.stream().allMatch("sensor-1"::equals),
          "Both store rows should belong to logical key 'sensor-1', but got: " + sensorIds);
      assertEquals(
          new HashSet<>(Arrays.asList(35.5, 40.0)),
          new HashSet<>(temperatures),
          "The two rows should hold the v1-written and v2-written values (35.5 and 40.0)");

    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  /**
   * Two schemas that differ only in metadata Avro produce identical key bytes.
   * Reads and writes under either schema target the same store row.
   */
  @Test
  public void shouldShareRowWhenKeySchemasProduceIdenticalBytes() throws Exception {
    String inputTopic = "doc-only-evolution-input";
    String appId = "doc-only-evolution-test-" + System.currentTimeMillis();

    KafkaStreams streams = null;
    try {
      streams = startTableApp(inputTopic, appId);

      GenericRecord keyV1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-1").build();
      GenericRecord keyV1DocChanged = new GenericRecordBuilder(KEY_SCHEMA_V1_DOC_CHANGED).set("sensorId", "sensor-1").build();

      // Write with key schema v1
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        producer.send(new ProducerRecord<>(inputTopic, keyV1,
            new GenericRecordBuilder(VALUE_SCHEMA_V1).set("temperature", 35.5).set("timestamp", 1000L).build())).get();
        producer.flush();
      }
      waitForStoreToContainKeys(streams, 1);

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      // Lookup with the original schema and new schema should find v1's row (bytes identical)
      ValueTimestampHeaders<GenericRecord> result = store.get(keyV1);
      assertNotNull(result, "lookup with original schema should find v1's row");
      assertEquals(35.5, result.value().get("temperature"), "lookup should return v1's value");
      assertSchemaIdHeaders(result.headers(), "result");
      ValueTimestampHeaders<GenericRecord> result2 = store.get(keyV1DocChanged);
      assertNotNull(result2, "lookup with doc-changed schema should find v1's row");
      assertEquals(35.5, result.value().get("temperature"),
          "lookup should return v1's value");
      assertSchemaIdHeaders(result2.headers(), "result2");

      // Register value schema v1_doc_changed in SR WITHOUT producing any new schema key records
      restApp.restClient.registerSchema(KEY_SCHEMA_V1_DOC_CHANGED.toString(), inputTopic + "-value");

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> storeBeforeDocChangedWrites =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      ValueTimestampHeaders<GenericRecord> sensor1BeforeV2 =
          storeBeforeDocChangedWrites.get(keyV1);
      assertNotNull(sensor1BeforeV2, "sensor-1 should be readable after SR evolution with no v2 writes");
      assertEquals(35.5, sensor1BeforeV2.value().get("temperature"));
      assertSchemaIdHeaders(sensor1BeforeV2.headers(), "sensor1BeforeV2");

      // Second write with the doc-changed schema should overwrite, not duplicate
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        producer.send(new ProducerRecord<>(inputTopic, keyV1DocChanged,
            new GenericRecordBuilder(VALUE_SCHEMA_V1).set("temperature", 40.0).set("timestamp", 2000L).build())).get();
        producer.flush();
      }

      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline) {
        ValueTimestampHeaders<GenericRecord> r = store.get(keyV1);
        if (r != null && 40.0 == (double) r.value().get("temperature")) {
          break;
        }
        Thread.sleep(200);
      }

      ValueTimestampHeaders<GenericRecord> updated = store.get(keyV1);
      assertNotNull(updated, "lookup with v1 should still find the row");
      assertEquals(40.0, updated.value().get("temperature"), "latest write should win");
      assertSchemaIdHeaders(updated.headers(), "updated");
      ValueTimestampHeaders<GenericRecord> updated2 = store.get(keyV1DocChanged);
      assertNotNull(updated2, "lookup with doc-changed v1 should still find the row");
      assertEquals(40.0, updated2.value().get("temperature"), "latest write should win");
      assertSchemaIdHeaders(updated2.headers(), "updated2");

      int count = 0;
      try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
        while (iter.hasNext()) {
          iter.next();
          count++;
        }
      }
      assertEquals(1, count, "writes should collapse into one row");

    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  /**
   * Test tombstone with a key that should delete the corresponding entry that shares the same bytes representation.
   */
  @Test
  public void shouldDeleteOnlyMatchingByteKeyRowOnTombstone() throws Exception {
    String inputTopic = "tombstone-key-evolution-input";
    String appId = "tombstone-key-evolution-test-" + System.currentTimeMillis();

    KafkaStreams streams = null;
    try {
      streams = startTableApp(inputTopic, appId);

      GenericRecord keyV1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-1").build();
      GenericRecord key2V1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-2").build();
      GenericRecord keyV2 = new GenericRecordBuilder(KEY_SCHEMA_V2)
          .set("sensorId", "sensor-1").set("region", "us-east").build();
      GenericRecord key2V1WithDoc = new GenericRecordBuilder(KEY_SCHEMA_V1_DOC_CHANGED).set("sensorId", "sensor-2").build();

      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        producer.send(new ProducerRecord<>(inputTopic, keyV1,
            new GenericRecordBuilder(VALUE_SCHEMA_V1).set("temperature", 35.5).set("timestamp", 1000L).build())).get();
        producer.send(new ProducerRecord<>(inputTopic, key2V1,
            new GenericRecordBuilder(VALUE_SCHEMA_V1).set("temperature", 37.0).set("timestamp", 1500L).build())).get();
        producer.send(new ProducerRecord<>(inputTopic, keyV2,
            new GenericRecordBuilder(VALUE_SCHEMA_V1).set("temperature", 40.0).set("timestamp", 2000L).build())).get();
        producer.flush();
      }
      waitForStoreToContainKeys(streams, 3);

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      ValueTimestampHeaders<GenericRecord> initKeyV1 = store.get(keyV1);
      assertNotNull(initKeyV1);
      assertSchemaIdHeaders(initKeyV1.headers(), "initKeyV1");
      ValueTimestampHeaders<GenericRecord> initKeyV2 = store.get(keyV2);
      assertNotNull(initKeyV2);
      assertSchemaIdHeaders(initKeyV2.headers(), "initKeyV2");
      assertEquals(3, countStoreEntries(store));

      // Tombstone for key1 with v1 schema
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        producer.send(new ProducerRecord<>(inputTopic, keyV1, null)).get();
        producer.flush();
      }
      waitForStoreEntryCount(store, 2);

      assertNull(store.get(keyV1),
          "Key row with schema v1 should be deleted after tombstone under v1 key");
      ValueTimestampHeaders<GenericRecord> afterTombstoneKey2V1 = store.get(key2V1);
      assertNotNull(afterTombstoneKey2V1, "key2 with v1 schema should exist");
      assertSchemaIdHeaders(afterTombstoneKey2V1.headers(), "afterTombstoneKey2V1");
      ValueTimestampHeaders<GenericRecord> afterTombstoneKeyV2 = store.get(keyV2);
      assertNotNull(afterTombstoneKeyV2,
          "Key row with schema v2 should survive");
      assertSchemaIdHeaders(afterTombstoneKeyV2.headers(), "afterTombstoneKeyV2");
      assertEquals(37.0, afterTombstoneKey2V1.value().get("temperature"));
      assertEquals(40.0, afterTombstoneKeyV2.value().get("temperature"));

      // Tombstone k1 with schema v2 row.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        producer.send(new ProducerRecord<>(inputTopic, keyV2, null)).get();
        producer.flush();
      }
      waitForStoreEntryCount(store, 1);

      assertNull(store.get(keyV1));
      assertNull(store.get(keyV2));
      assertEquals(1, countStoreEntries(store));
      ValueTimestampHeaders<GenericRecord> survivingKey2V1 = store.get(key2V1);
      assertNotNull(survivingKey2V1, "key2 with v1 schema should still exist");
      assertEquals(37.0, survivingKey2V1.value().get("temperature"),
          "key2 with v1 schema should still exist");
      assertSchemaIdHeaders(survivingKey2V1.headers(), "survivingKey2V1");

      // Tombstone with the doc-changed schema that has identical bytes to v2
        try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
            producer.send(new ProducerRecord<>(inputTopic, key2V1WithDoc, null)).get();
            producer.flush();
        }
        waitForStoreEntryCount(store, 0);
        assertNull(store.get(key2V1), "key2 with v1 schema should be deleted by tombstone with doc-changed schema");
        assertEquals(0, countStoreEntries(store), "Store should be empty after all tombstones");

    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  /**
   * Test whether the changelog topic could restore the schema by
   * enforcing a wipe on the local state dir and restarting Streams with the same appId.
   */
  @Test
  public void shouldRestoreStateStoreFromChangelogPreservingHeaderSchemaIds() throws Exception {
    String inputTopic = "changelog-restore-input";
    String appId = "changelog-restore-test-" + System.currentTimeMillis();
    Path stateDir = Files.createTempDirectory("kstreams-restore-");

    KafkaStreams streams1 = null;
    KafkaStreams streams2 = null;
    try {
      // Create a first streams app with state store specified
      createTopics(inputTopic);
      streams1 = startStreams(buildTableTopology(inputTopic), appId, stateDir);

      GenericRecord key1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-1").build();
      GenericRecord key2 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-2").build();
      GenericRecord key3 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-3").build();

      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        producer.send(new ProducerRecord<>(inputTopic, key1,
            new GenericRecordBuilder(VALUE_SCHEMA_V1).set("temperature", 35.5).set("timestamp", 1000L).build())).get();
        producer.send(new ProducerRecord<>(inputTopic, key2,
            new GenericRecordBuilder(VALUE_SCHEMA_V2)
                .set("temperature", 37.0).set("timestamp", 1500L).set("humidity", 60.0).build())).get();
        producer.send(new ProducerRecord<>(inputTopic, key3,
            new GenericRecordBuilder(VALUE_SCHEMA_V3)
                .set("temperature", 40.0).set("timestamp", 2000L)
                .set("humidity", 70.0).set("pressure", 1020.0).build())).get();
        producer.flush();
      }
      waitForStoreToContainKeys(streams1, 3);

      // Verify HeaderSchemaIdSerializer attached schema-id headers on every input record.
      List<ConsumerRecord<byte[], byte[]>> inputRecords = consumeRawChangelog(
          inputTopic, "restore-input-assert-" + System.currentTimeMillis(), 3);
      assertEquals(3, inputRecords.size());
      for (int i = 0; i < inputRecords.size(); i++) {
        assertSchemaIdHeaders(inputRecords.get(i).headers(), "input#" + i);
      }

      // Check all records are in the store before restart
      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store1 =
          streams1.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));
      assertEquals(35.5, store1.get(key1).value().get("temperature"));
      assertEquals(60.0, store1.get(key2).value().get("humidity"));
      assertEquals(1020.0, store1.get(key3).value().get("pressure"));
      assertSchemaIdHeaders(store1.get(key1).headers(), "input#" + key1);
      assertSchemaIdHeaders(store1.get(key2).headers(), "input#" + key2);
      assertSchemaIdHeaders(store1.get(key3).headers(), "input#" + key3);

      streams1.close(Duration.ofSeconds(30));
      streams1 = null;

      // Verify changelog topic
      String changelogTopic = appId + "-" + STORE_NAME + "-changelog";
      List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(
          changelogTopic, "restore-changelog-assert-" + System.currentTimeMillis(), 3);
      assertEquals(3, changelogRecords.size(),
          "Changelog should hold 3 records before restore");
      for (int i = 0; i < changelogRecords.size(); i++) {
        assertSchemaIdHeaders(changelogRecords.get(i).headers(), "changelog#" + i);
      }

      // Wipe local state to guarantees Streams can't warm-start and must replay the
      // changelog to rebuild the store.
      deleteRecursively(stateDir);
      Files.createDirectory(stateDir);

      // Restart the same app, it should restore from changelog.
      streams2 = startStreams(buildTableTopology(inputTopic), appId, stateDir);
      waitForStoreToContainKeys(streams2, 3);

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store2 =
          streams2.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      // Check if sensor-1 is restored under v1 key schema
      ValueTimestampHeaders<GenericRecord> r1 = store2.get(key1);
      assertNotNull(r1, "sensor-1 should be restored");
      assertEquals(35.5, r1.value().get("temperature"));
      assertThrows(AvroRuntimeException.class, () -> r1.value().get("humidity"),
          "v1-written record should not expose humidity after restore");
      assertThrows(AvroRuntimeException.class, () -> r1.value().get("pressure"));
      assertSchemaIdHeaders(r1.headers(), "sensor#" + key1);

      // Check if sensor-2 is restored under v2 key schema
      ValueTimestampHeaders<GenericRecord> r2 = store2.get(key2);
      assertNotNull(r2, "sensor-2 should be restored");
      assertEquals(37.0, r2.value().get("temperature"));
      assertEquals(60.0, r2.value().get("humidity"));
      assertThrows(AvroRuntimeException.class, () -> r2.value().get("pressure"));
      assertSchemaIdHeaders(r2.headers(), "sensor#" + key2);

      // Check if sensor-3 is restored under v3 key schema
      ValueTimestampHeaders<GenericRecord> r3 = store2.get(key3);
      assertNotNull(r3, "sensor-3 should be restored");
      assertEquals(40.0, r3.value().get("temperature"));
      assertEquals(70.0, r3.value().get("humidity"));
      assertEquals(1020.0, r3.value().get("pressure"));
      assertSchemaIdHeaders(r3.headers(), "sensor#" + key3);

      assertEquals(3, countStoreEntries(store2),
          "Store should hold exactly the 3 records restored from the changelog");

      // Verify changelog topic still has the same 3 records after restore
      changelogRecords = consumeRawChangelog(
          changelogTopic, "restore-changelog-assert-" + System.currentTimeMillis(), 3);
      assertEquals(3, changelogRecords.size(),
          "Changelog should hold 3 records before restore");
      for (int i = 0; i < changelogRecords.size(); i++) {
        assertSchemaIdHeaders(changelogRecords.get(i).headers(), "changelog#" + i);
      }
    } finally {
      if (streams1 != null) {
        streams1.close(Duration.ofSeconds(10));
      }
      if (streams2 != null) {
        streams2.close(Duration.ofSeconds(10));
      }
      deleteRecursively(stateDir);
    }
  }

  /**
   * Test complex schema evolution with enum and map fields.
   */
  @Test
  public void shouldReadComplexTypeEvolution() throws Exception {
    String inputTopic = "complex-types-input";
    String appId = "complex-types-test-" + System.currentTimeMillis();

    KafkaStreams streams = null;
    try {
      streams = startTableApp(inputTopic, appId);

      GenericRecord key1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-1").build();
      GenericRecord key2 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-2").build();

      Schema v1Color = VALUE_SCHEMA_ENUM_MAP_V1.getField("color").schema();
      Schema v2Color = VALUE_SCHEMA_ENUM_MAP_V2.getField("color").schema();

      Map<String, String> tags = new HashMap<>();
      tags.put("env", "prod");
      tags.put("region", "us-east");

      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        producer.send(new ProducerRecord<>(inputTopic, key1,
            new GenericRecordBuilder(VALUE_SCHEMA_ENUM_MAP_V1)
                .set("temperature", 35.5)
                .set("color", new GenericData.EnumSymbol(v1Color, "GREEN")).build())).get();
        restApp.restClient.registerSchema(
            VALUE_SCHEMA_ENUM_MAP_V2.toString(), inputTopic + "-value");
        producer.send(new ProducerRecord<>(inputTopic, key2,
            new GenericRecordBuilder(VALUE_SCHEMA_ENUM_MAP_V2)
                .set("temperature", 40.0)
                .set("color", new GenericData.EnumSymbol(v2Color, "BLUE"))
                .set("tags", tags).build())).get();
        producer.flush();
      }
      waitForStoreToContainKeys(streams, 2);

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      // sensor-1 written under v1 → reader sees v1 shape only.
      GenericRecord v1Read = store.get(key1).value();
      assertEquals(35.5, v1Read.get("temperature"));
      assertEquals("GREEN", v1Read.get("color").toString());
      assertThrows(AvroRuntimeException.class, () -> v1Read.get("tags"),
          "v1-written record should not expose tags added in v2");
      assertSchemaIdHeaders(store.get(key1).headers(), "sensor#" + key1);

      // sensor-2 written under v2 → sees both the new enum symbol and the map.
      GenericRecord v2Read = store.get(key2).value();
      assertEquals(40.0, v2Read.get("temperature"));
      assertEquals("BLUE", v2Read.get("color").toString());
      assertSchemaIdHeaders(store.get(key2).headers(), "sensor#" + key2);

      @SuppressWarnings("unchecked")
      Map<Object, Object> readTags = (Map<Object, Object>) v2Read.get("tags");
      Map<String, String> normalizedTags = readTags.entrySet().stream()
          .collect(Collectors.toMap(
              e -> e.getKey().toString(), e -> e.getValue().toString()));
      assertEquals(2, normalizedTags.size());
      assertEquals("prod", normalizedTags.get("env"));
      assertEquals("us-east", normalizedTags.get("region"));
    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  private StreamsBuilder buildTableTopology(String inputTopic) {
    StreamsBuilder builder = new StreamsBuilder();
    builder.table(
        inputTopic,
        Consumed.with(createKeySerde(), createValueSerde()),
        Materialized.<GenericRecord, GenericRecord>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME))
            .withKeySerde(createKeySerde())
            .withValueSerde(createValueSerde()));
    return builder;
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Test tombstone with a key that should delete the corresponding entry that shares the same bytes representation.
   */
  @Test
  public void shouldSwitchToDefaultValueOnNullField() throws Exception {
    String inputTopic = "null-value-field-evolution-input";
    String appId = "null-value-field-evolution-test-" + System.currentTimeMillis();

    KafkaStreams streams = null;
    try {
      streams = startTableApp(inputTopic, appId);

      GenericRecord keyV1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-1").build();
      GenericRecord key2V1 = new GenericRecordBuilder(KEY_SCHEMA_V1).set("sensorId", "sensor-2").build();
      GenericRecord keyV2 = new GenericRecordBuilder(KEY_SCHEMA_V2)
          .set("sensorId", "sensor-1").set("region", null).build();
      assertEquals(null, keyV2.get("region"));
      keyV2 = new GenericRecordBuilder(KEY_SCHEMA_V2).set("sensorId", "sensor-2").build();
      assertEquals("us-east", keyV2.get("region").toString());

      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        // send a normal record with v1 key schema
        producer.send(new ProducerRecord<>(inputTopic, keyV1,
            new GenericRecordBuilder(VALUE_SCHEMA_V1).set("temperature", 35.5).set("timestamp", 1000L).build())).get();
        // send a v2 key schema record with null region field
        producer.send(new ProducerRecord<>(inputTopic, keyV2,
            new GenericRecordBuilder(VALUE_SCHEMA_V1).set("temperature", 40.0).set("timestamp", 2000L).build())).get();
        producer.flush();
      }
      waitForStoreToContainKeys(streams, 2);

      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      assertEquals(2, countStoreEntries(store));
      ValueTimestampHeaders<GenericRecord> initKeyV1 = store.get(keyV1);
      assertNotNull(initKeyV1);
      assertSchemaIdHeaders(initKeyV1.headers(), "initKeyV1");

      ValueTimestampHeaders<GenericRecord> initKeyV2 = store.get(keyV2);
      assertEquals(40.0, initKeyV2.value().get("temperature"));
      assertSchemaIdHeaders(initKeyV2.headers(), "initKeyV2");

      // send a record with null for a non-nullable field, it should fail Avro serialization.
      GenericRecord badVal = new GenericData.Record(VALUE_SCHEMA_V1);
      badVal.put("temperature", null);
      badVal.put("timestamp", 1500L);
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        assertThrows(SerializationException.class, () ->
                producer.send(new ProducerRecord<>(inputTopic, key2V1, badVal)),
            "producing null for a non-nullable double field should fail Avro serialization");
      }

      // overwrite with a record that omits `pressure` and `humidity`, using a schema where
      // those two fields have no default value
      GenericRecord badVal2 = new GenericData.Record(VALUE_SCHEMA_V3_NO_DEFAULT);
      badVal2.put("temperature", 30.0);
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        assertThrows(SerializationException.class, () ->
                producer.send(new ProducerRecord<>(inputTopic, key2V1, badVal2)),
            "not specifying value for a non-nullable double field should fail Avro serialization");
      }

      // send a record with null for a non-nullable field but with default value.
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        // send a v4 value schema record with no pressure field, with a default humidity field.
        producer.send(new ProducerRecord<>(inputTopic, keyV2,
            new GenericRecordBuilder(VALUE_SCHEMA_V4_NO_HUMIDITY).set("temperature", 45.0).set("timestamp", 2500L).build())).get();
        producer.flush();
      }

      store = streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));
      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline) {
        ValueTimestampHeaders<GenericRecord> r = store.get(keyV2);
        if (r != null && r.value().getSchema().getField("pressure") != null
            && 2500L == (long) r.value().get("timestamp")) {
          break;
        }
        Thread.sleep(200);
      }
      ValueTimestampHeaders<GenericRecord> afterNullField = store.get(keyV2);
      assertEquals(45.0, afterNullField.value().get("temperature"));
      assertEquals(1013.0, afterNullField.value().get("pressure"),
          "After sending a record with null for a non-nullable field but with default value, the store should use the default value");
      assertThrows(AvroRuntimeException.class,
          () -> afterNullField.value().get("humidity"), "humidity should not be present");
      assertSchemaIdHeaders(afterNullField.headers(), "afterNullField");


    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }


  /**
   * Creates {@code inputTopic}, builds a KTable materialized into a header-aware state store
   * named {@link #STORE_NAME}, and starts the Streams app under {@code appId}.
   */
  private KafkaStreams startTableApp(String inputTopic, String appId) throws Exception {
    createTopics(inputTopic);
    StreamsBuilder builder = new StreamsBuilder();
    builder.table(
        inputTopic,
        Consumed.with(createKeySerde(), createValueSerde()),
        Materialized.<GenericRecord, GenericRecord>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME))
            .withKeySerde(createKeySerde())
            .withValueSerde(createValueSerde()));
    return startStreams(builder, appId);
  }

  private void createTopics(String... topics) throws Exception {
    Properties adminProps = new Properties();
    adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient admin = AdminClient.create(adminProps)) {
      admin
          .createTopics(
              Arrays.stream(topics)
                  .map(t -> new NewTopic(t, 1, (short) 1))
                  .collect(Collectors.toList()))
          .all()
          .get(30, TimeUnit.SECONDS);
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

  private KafkaProducer<GenericRecord, GenericRecord> createHeaderProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    props.put(
        AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    props.put(
        AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private KafkaStreams startStreams(StreamsBuilder builder, String appId) throws Exception {
    return startStreams(builder, appId, null);
  }

  private KafkaStreams startStreams(StreamsBuilder builder, String appId, Path stateDir)
      throws Exception {
    Properties streamsProps = new Properties();
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    streamsProps.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    if (stateDir != null) {
      streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    }

    CountDownLatch startedLatch = new CountDownLatch(1);
    KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
    streams.setStateListener(
        (newState, oldState) -> {
          if (newState == KafkaStreams.State.RUNNING) {
            startedLatch.countDown();
          }
        });
    streams.start();
    assertTrue(
        startedLatch.await(60, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");
    return streams;
  }

  private static void waitForStoreEntryCount(
      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store,
      int expectedCount) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      if (countStoreEntries(store) == expectedCount) {
        return;
      }
      Thread.sleep(200);
    }
    throw new AssertionError(
        "Store did not reach " + expectedCount + " entries within timeout; had "
            + countStoreEntries(store));
  }

  private static int countStoreEntries(
      ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store) {
    int count = 0;
    try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
      while (iter.hasNext()) {
        iter.next();
        count++;
      }
    }
    return count;
  }

  private void waitForStoreToContainKeys(KafkaStreams streams, int expectedCount)
      throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      try {
        ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
            streams.store(StoreQueryParameters.fromNameAndType(
                STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));
        int count = 0;
        try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter = store.all()) {
          while (iter.hasNext()) {
            iter.next();
            count++;
          }
        }
        if (count >= expectedCount) {
          return;
        }
      } catch (Exception e) {
        // Store may not be ready yet
      }
      Thread.sleep(200);
    }
    throw new AssertionError(
        "Store did not contain " + expectedCount + " entries within timeout");
  }

  private List<ConsumerRecord<byte[], byte[]>> consumeRawChangelog(
      String topic, String group, int count) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    List<ConsumerRecord<byte[], byte[]>> results = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
      consumer.subscribe(Collections.singletonList(topic));
      long end = System.currentTimeMillis() + 15000;
      while (results.size() < count && System.currentTimeMillis() < end) {
        for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) {
          results.add(r);
        }
      }
    }
    return results;
  }

  private void assertSchemaIdHeaders(Headers headers, String context) {
    Header keySchemaIdHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
    assertNotNull(keySchemaIdHeader, context + ": should have __key_schema_id header");
    byte[] keyHeaderBytes = keySchemaIdHeader.value();
    assertEquals(17, keyHeaderBytes.length, context + ": Key GUID header should be 17 bytes");
    assertEquals(SchemaId.MAGIC_BYTE_V1, keyHeaderBytes[0],
        context + ": Key header should have V1 magic byte");

    Header valueSchemaIdHeader = headers.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
    assertNotNull(valueSchemaIdHeader, context + ": should have __value_schema_id header");
    byte[] valueHeaderBytes = valueSchemaIdHeader.value();
    assertEquals(17, valueHeaderBytes.length, context + ": Value GUID header should be 17 bytes");
    assertEquals(SchemaId.MAGIC_BYTE_V1, valueHeaderBytes[0],
        context + ": Value header should have V1 magic byte");
  }

  private void assertKeySchemaIdHeader(Headers headers, String context) {
    Header h = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
    assertNotNull(h, context + ": Missing key schema ID");
    assertEquals(17, h.value().length);
  }

  private static class TimestampedKeyValueStoreWithHeadersType<K, V>
      implements QueryableStoreType<ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>>> {

    @Override
    public boolean accepts(final StateStore stateStore) {
      return stateStore instanceof TimestampedKeyValueStoreWithHeaders
          && stateStore instanceof ReadOnlyKeyValueStore;
    }

    @Override
    public ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> create(
        final StateStoreProvider storeProvider, final String storeName) {
      return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
    }
  }
}
