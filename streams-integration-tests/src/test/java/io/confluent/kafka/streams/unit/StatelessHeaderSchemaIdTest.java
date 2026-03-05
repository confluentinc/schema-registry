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

package io.confluent.kafka.streams.unit;

import static io.confluent.kafka.streams.unit.Utils.assertSchemaIdHeadersOnRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test that verifies a Kafka Streams stateless filter topology works correctly with
 * {@link GenericAvroSerde} configured to use {@link HeaderSchemaIdSerializer} for header-based
 * schema ID transport. Both keys and values use Avro with header-based schema IDs.
 */
public class StatelessHeaderSchemaIdTest {
    private static final String APPLICATION_ID = "header-schema-id-unit-test";
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

    private TopologyTestDriver testDriver;
    private TestInputTopic<GenericRecord, GenericRecord> inputTopic;
    private TestOutputTopic<GenericRecord, GenericRecord> outputTopic;
    private SchemaRegistryClient mockSchemaRegistry;
    private Schema keySchema;
    private Schema valueSchema;

    @BeforeEach
    void setup() {
        mockSchemaRegistry = new MockSchemaRegistryClient();

        keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
        valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

        // Create serdes with mock SR and header-based schema ID
        GenericAvroSerde keySerde = createSerde(true);
        GenericAvroSerde valueSerde = createSerde(false);

        // Build stateless filter topology
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .filter((key, value) ->
                key.get("sensorId") != null && (double) value.get("temperature") > 30.0)
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        // Configure test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:9092");

        testDriver = new TopologyTestDriver(builder.build(), props);

        // Create test topics
        inputTopic = testDriver.createInputTopic(
            INPUT_TOPIC, keySerde.serializer(), valueSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
            OUTPUT_TOPIC, keySerde.deserializer(), valueSerde.deserializer());
    }

    @AfterEach
    void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void shouldFilterRecordsUsingHeaderBasedSchemaId() {
        // Create hot reading (temperature > 30, should pass filter)
        GenericRecord hotKey = new GenericData.Record(keySchema);
        hotKey.put("sensorId", "sensor-1");

        GenericRecord hotReading = new GenericData.Record(valueSchema);
        hotReading.put("temperature", 35.5);
        hotReading.put("timestamp", 1650000000000L);

        // Create cold reading (temperature <= 30, should be filtered out)
        GenericRecord coldKey = new GenericData.Record(keySchema);
        coldKey.put("sensorId", "sensor-2");

        GenericRecord coldReading = new GenericData.Record(valueSchema);
        coldReading.put("temperature", 20.0);
        coldReading.put("timestamp", 1650000000100L);

        // When - send both records
        inputTopic.pipeInput(hotKey, hotReading);
        inputTopic.pipeInput(coldKey, coldReading);

        // Only hot reading should pass the filter
        assertEquals(1, outputTopic.getQueueSize(),
            "Only one record should pass the temperature > 30 filter");

        TestRecord<GenericRecord, GenericRecord> outputRecord = outputTopic.readRecord();
        assertNotNull(outputRecord);

        assertEquals("sensor-1", outputRecord.key().get("sensorId").toString());
        assertEquals(35.5, outputRecord.value().get("temperature"));
        assertSchemaIdHeadersOnRecord(outputRecord, "Output record should have correct schema ID headers");
    }

    private GenericAvroSerde createSerde(boolean isKey) {
        GenericAvroSerde serde = new GenericAvroSerde(mockSchemaRegistry);
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test");
        config.put(
            isKey ? AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER
                : AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, isKey);
        return serde;
    }
}
