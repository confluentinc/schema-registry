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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.example.User;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class SpecificAvroSerdeTest {

  private static final String ANY_TOPIC = "any-topic";

  private static <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T>
  createConfiguredSerdeForRecordValues() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    SpecificAvroSerde<T> serde = new SpecificAvroSerde<>(schemaRegistryClient);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }

  @Test
  public void shouldRoundTripRecords() {
    // Given
    SpecificAvroSerde<User> serde = createConfiguredSerdeForRecordValues();
    User record = User.newBuilder().setName("alice").build();

    // When
    User roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC,
        serde.serializer().serialize(ANY_TOPIC, record));

    // Then
    assertThat(roundtrippedRecord, equalTo(record));

    // Cleanup
    serde.close();
  }

  @Test
  public void shouldRoundTripNullRecordsToNull() {
    // Given
    SpecificAvroSerde<User> serde = createConfiguredSerdeForRecordValues();

    // When
    User roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC,
        serde.serializer().serialize(ANY_TOPIC, null));

    // Then
    assertThat(roundtrippedRecord, nullValue());

    // Cleanup
    serde.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenInstantiatedWithNullSchemaRegistryClient() {
    new SpecificAvroSerde<>(null);
  }

  /**
   * Verifies that, even if the user explicitly wants to disable specific Avro for this serde, we
   * don't allow this and, behind the scenes, override the setting so that specific Avro is enabled
   * again.
   */
  @Test
  public void shouldRoundTripRecordsEvenWhenConfiguredToDisableSpecificAvro() {
    // Given
    SpecificAvroSerde<User> serde = createConfiguredSerdeForRecordValues();
    User record = User.newBuilder().setName("alice").build();
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "fake-to-satisfy-checks");
    serdeConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

    // When configured to be a serde for record keys
    serde.configure(serdeConfig, true);
    User roundtrippedRecordForKeySerde = serde.deserializer().deserialize(
        ANY_TOPIC,
        serde.serializer().serialize(ANY_TOPIC, record));
    // Then
    assertThat(roundtrippedRecordForKeySerde, equalTo(record));

    // When configured to be a serde for record values
    serde.configure(serdeConfig, true);
    User roundtrippedRecordForValueSerde = serde.deserializer().deserialize(
        ANY_TOPIC,
        serde.serializer().serialize(ANY_TOPIC, record));
    // Then
    assertThat(roundtrippedRecordForValueSerde, equalTo(record));

    // Cleanup
    serde.close();
  }

}