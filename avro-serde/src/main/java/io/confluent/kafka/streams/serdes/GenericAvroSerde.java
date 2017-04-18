/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.kafka.streams.serdes;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * A serde (serializer/deserializer) for Apache Kafka's Streams API that can be used for for reading
 * and/or writing data in "generic Avro" format.
 *
 * <p>Usage example for configuring this serde as a Kafka Streams application's default serde for
 * both record keys and record values:
 *
 * <p><pre>{@code
 * Properties streamsConfiguration = new Properties();
 * streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
 * streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
 * streamsConfiguration.put(
 *     AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *     "http://confluent-schema-registry-server:8081/");
 * }</pre>
 *
 * <p>Usage example for explicitly overriding the application's default serdes (whatever they were
 * configured to) so that only specific operations such as {@code KStream#to()} use this serde:
 *
 * <p><pre>{@code
 * Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
 * boolean isKeySerde = false;
 * genericAvroSerde.configure(
 *     Collections.singletonMap(
 *         AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *         "http://confluent-schema-registry-server:8081/"),
 *     isKeySerde);
 * KStream<String, GenericRecord> stream = ...;
 * stream.to(Serdes.String(), genericAvroSerde, "my-output-topic");
 * }</pre>
 */
public class GenericAvroSerde implements Serde<GenericRecord> {

  private final Serde<GenericRecord> inner;

  /**
   * Implementation detail: This constructor is used by Kafka's Streams API.
   */
  public GenericAvroSerde() {
    inner = Serdes.serdeFrom(new GenericAvroSerializer(), new GenericAvroDeserializer());
  }

  public GenericAvroSerde(final SchemaRegistryClient client) {
    this(client, Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap());
  }

  public GenericAvroSerde(final SchemaRegistryClient client,
                          final Map<String, ?> serializerConfig,
                          final Map<String, ?> deserializerConfig) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    inner = Serdes.serdeFrom(
        new GenericAvroSerializer(client, serializerConfig),
        new GenericAvroDeserializer(client, deserializerConfig));
  }

  @Override
  public Serializer<GenericRecord> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<GenericRecord> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
    inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
    inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

}