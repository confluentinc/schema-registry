/*
 * Copyright 2017-2025 Confluent Inc.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import io.confluent.kafka.example.Widget;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ReflectionAvroSerdeGenericTest {

  private static final String ANY_TOPIC = "any-topic";

  private static <T> ReflectionAvroSerde<T> createConfiguredSerde() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    ReflectionAvroSerde<T> serde = new ReflectionAvroSerde<>(schemaRegistryClient);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }

  @Test
  public void shouldRoundTripRecords() {
    // Given
    ReflectionAvroSerde<Widget> serde = createConfiguredSerde();
    Widget record = new Widget("alice");

    // When
    Widget roundtrippedRecord = serde.deserializer().deserialize(
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
    ReflectionAvroSerde<Widget> serde = createConfiguredSerde();

    // When
    Widget roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC,
        serde.serializer().serialize(ANY_TOPIC, null));

    // Then
    assertThat(roundtrippedRecord, nullValue());

    // Cleanup
    serde.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenInstantiatedWithNullSchemaRegistryClient() {
    new ReflectionAvroSerde<>((SchemaRegistryClient)null);
  }

}