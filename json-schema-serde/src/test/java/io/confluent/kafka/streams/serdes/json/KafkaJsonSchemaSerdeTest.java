/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.streams.serdes.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class KafkaJsonSchemaSerdeTest {

  private static final String ANY_TOPIC = "any-topic";

  private static ObjectMapper objectMapper = new ObjectMapper();

  private static final String recordSchemaString = "{\"properties\": {\n"
      + "     \"null\": {\"type\": \"null\"},\n"
      + "     \"boolean\": {\"type\": \"boolean\"},\n"
      + "     \"number\": {\"type\": \"number\"},\n"
      + "     \"string\": {\"type\": \"string\"}\n"
      + "  },\n"
      + "  \"additionalProperties\": false\n"
      + "}";

  private static final JsonSchema recordSchema = new JsonSchema(recordSchemaString);

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class SomeTestRecord {
    String string;
    Integer number;
    private SomeTestRecord() {}

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SomeTestRecord that = (SomeTestRecord) o;
      return Objects.equals(string, that.string) &&
              Objects.equals(number, that.number);
    }
  }

  private Object createJsonRecord() throws IOException {
    String json = "{\n"
        + "    \"null\": null,\n"
        + "    \"boolean\": true,\n"
        + "    \"number\": 12,\n"
        + "    \"string\": \"string\"\n"
        + "}";

    return objectMapper.readValue(json, Object.class);
  }

  private SomeTestRecord createJsonRecordWithClass() throws IOException {
    String json = "{\n"
            + "    \"null\": null,\n"
            + "    \"boolean\": true,\n"
            + "    \"number\": 12,\n"
            + "    \"string\": \"string\"\n"
            + "}";

    return objectMapper.readValue(json, SomeTestRecord.class);
  }

  private static KafkaJsonSchemaSerde<Object> createConfiguredSerdeForRecordValues() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    KafkaJsonSchemaSerde<Object> serde = new KafkaJsonSchemaSerde<>(schemaRegistryClient);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }

  private static KafkaJsonSchemaSerde<SomeTestRecord> createConfiguredSerdeForRecordValuesWithClass() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    KafkaJsonSchemaSerde<SomeTestRecord> serde = new KafkaJsonSchemaSerde<>(schemaRegistryClient, SomeTestRecord.class);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }


  @Test
  public void shouldRoundTripRecords() throws Exception {
    // Given
    KafkaJsonSchemaSerde<Object> serde = createConfiguredSerdeForRecordValues();
    Object record = createJsonRecord();

    // When
    Object roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC, serde.serializer().serialize(ANY_TOPIC, record));

    // Then
    assertThat(roundtrippedRecord, equalTo(record));

    // Cleanup
    serde.close();
  }

  @Test
  public void shouldRoundTripNullRecordsToNull() {
    // Given
    KafkaJsonSchemaSerde<Object> serde = createConfiguredSerdeForRecordValues();

    // When
    Object roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC, serde.serializer().serialize(ANY_TOPIC, null));
    // Then
    assertThat(roundtrippedRecord, nullValue());

    // Cleanup
    serde.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenInstantiatedWithNullSchemaRegistryClient() {
    new KafkaJsonSchemaSerde<>((SchemaRegistryClient) null);
  }

  @Test
  public void shouldLetTheAbilityToDeserializeToASpecificClass() throws IOException {
    // Given
    KafkaJsonSchemaSerde<SomeTestRecord> serde = createConfiguredSerdeForRecordValuesWithClass();
    SomeTestRecord record = createJsonRecordWithClass();

    // When
    Object roundtrippedRecord = serde.deserializer().deserialize(
            ANY_TOPIC, serde.serializer().serialize(ANY_TOPIC, record));

    // Then
    assertThat(roundtrippedRecord, equalTo(record));

    // Cleanup
    serde.close();
  }

}
