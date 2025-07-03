/*
 * Copyright 2017-2020 Confluent Inc.
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

package io.confluent.kafka.streams.serdes.avro;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class GenericAvroSerdeTest {

  private static final String ANY_TOPIC = "any-topic";

  private static GenericAvroSerde createConfiguredSerdeForRecordValues() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    GenericAvroSerde serde = new GenericAvroSerde(schemaRegistryClient);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }

  @Test
  public void shouldRoundTripRecords() {
    // Given
    GenericAvroSerde serde = createConfiguredSerdeForRecordValues();
    GenericRecord record = AvroUtils.createGenericRecord();

    // When
    GenericRecord roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC, serde.serializer().serialize(ANY_TOPIC, record));

    // Then
    assertThat(roundtrippedRecord, equalTo(record));

    // Cleanup
    serde.close();
  }

  @Test
  public void shouldRoundTripNullRecordsToNull() {
    // Given
    GenericAvroSerde serde = createConfiguredSerdeForRecordValues();

    // When
    GenericRecord roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC, serde.serializer().serialize(ANY_TOPIC, null));
    // Then
    assertThat(roundtrippedRecord, nullValue());

    // Cleanup
    serde.close();
  }


  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenInstantiatedWithNullSchemaRegistryClient() {
    new GenericAvroSerde(null);
  }

}