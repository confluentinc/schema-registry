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

import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
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
public class StatelessHeaderIntegrationTest extends HeadersIQv2IntegrationTestBase {

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

  /**
   * Verifies that header-based schema IDs survive every stateless operator that re-serializes
   * records: filter, mapValues, map, selectKey, flatMapValues, and branch. Each operator's output
   * is written to its own topic via {@code .to(...)} so re-serialization is forced; we then assert
   * each output record's schema-ID headers are byte-for-byte the GUIDs the producer wrote to the
   * input topic.
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

    createTopics(INPUT_TOPIC, filterOutput, mapValuesOutput, mapOutput, selectKeyOutput,
        flatMapValuesOutput, branchHotOutput);

    GenericAvroSerde keySerde = createKeySerde();
    GenericAvroSerde valueSerde = createValueSerde();

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

    KafkaStreams streams =
        startStreamsAndAwaitRunning(builder.build(), "stateless-operators-integration-test");
    try {
      GenericRecord key = new GenericData.Record(keySchema);
      key.put("sensorId", "sensor-1");
      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("temperature", 42.0);
      value.put("timestamp", System.currentTimeMillis());
      CapturedSchemaIds produced = produce(INPUT_TOPIC, key, value, System.currentTimeMillis());

      for (String outputTopic : Arrays.asList(
          filterOutput, mapValuesOutput, mapOutput, selectKeyOutput, flatMapValuesOutput,
          branchHotOutput)) {
        ConsumerRecord<GenericRecord, GenericRecord> result =
            consumeRecords(outputTopic, "stateless-" + outputTopic + "-consumer", 1).get(0);
        // Every operator here is identity, so the re-serialized output must carry the exact GUID
        // bytes the producer wrote: a schema GUID is a content hash, so re-registering the same
        // schema under each output topic's own subject yields the same GUID.
        assertSchemaIdHeaders(result.headers(), produced, outputTopic);
      }
      closeStreams(streams);
    } finally {
      closeStreamsQuietly(streams);
    }
  }
}