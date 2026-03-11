/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Map;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A schema-registry aware serde (serializer/deserializer) for Apache Kafka's Streams API that can
 * be used for reading and writing data in "reflection Avro" format.  This serde's "generic Avro"
 * counterpart is {@link GenericAvroSerde}.
 *
 * <p>This serde reads and writes data according to the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format. It
 * requires access to a Confluent Schema Registry endpoint, which you must {@link
 * GenericAvroDeserializer#configure(Map, boolean)} via the parameter "schema.registry.url".</p>
 *
 * <p><strong>Usage</strong></p>
 *
 * <p>Example for configuring this serde as a Kafka Streams application's default serde for both
 * record keys and record values:</p>
 *
 * <pre>{@code
 * Properties streamsConfiguration = new Properties();
 * streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, ReflectionAvroSerde.class);
 * streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, ReflectionAvroSerde.class);
 * streamsConfiguration.put(
 *     AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *     "http://confluent-schema-registry-server:8081/");
 * }</pre>
 *
 * <p>Example for explicitly overriding the application's default serdes (whatever they were
 * configured to) so that only specific operations such as {@code KStream#to()} use this serde:</p>
 *
 * <pre>{@code
 * Serde<MyJavaClassGeneratedFromAvroSchema> reflectionAvroSerde = new ReflectionAvroSerde<>();
 * boolean isKeySerde = false;
 * reflectionAvroSerde.configure(
 *     Collections.singletonMap(
 *         AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *         "http://confluent-schema-registry-server:8081/"),
 *     isKeySerde);
 * KStream<String, MyJavaClassGeneratedFromAvroSchema> stream = ...;
 * stream.to(Serdes.String(), reflectionAvroSerde, "my-output-topic");
 * }</pre>
 */
@InterfaceStability.Unstable
public class ReflectionAvroSerde<T> implements Serde<T> {

  private final Serde<T> inner;

  public ReflectionAvroSerde() {
    inner = Serdes
        .serdeFrom(new ReflectionAvroSerializer<>(), new ReflectionAvroDeserializer<>());
  }

  public ReflectionAvroSerde(Class<T> type) {
    inner = Serdes
        .serdeFrom(new ReflectionAvroSerializer<>(), new ReflectionAvroDeserializer<>(type));
  }

  /**
   * For testing purposes only.
   */
  public ReflectionAvroSerde(final SchemaRegistryClient client) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    inner = Serdes.serdeFrom(
        new ReflectionAvroSerializer<>(client),
        new ReflectionAvroDeserializer<>(client));
  }

  /**
   * For testing purposes only.
   */
  public ReflectionAvroSerde(final SchemaRegistryClient client, Class<T> type) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    inner = Serdes.serdeFrom(
        new ReflectionAvroSerializer<>(client),
        new ReflectionAvroDeserializer<>(client, type));
  }

  @Override
  public Serializer<T> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<T> deserializer() {
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
