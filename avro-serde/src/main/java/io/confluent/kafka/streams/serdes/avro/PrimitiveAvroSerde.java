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


package io.confluent.kafka.streams.serdes.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * <p>
 * A schema-registry aware serde (serializer/deserializer) for Apache Kafka's Streams API that can
 * be used for reading and writing data of <strong>Avro primitive</strong> types.
 * </p>
 * <p>
 * The Avro primitive types (cf https://avro.apache.org/docs/current/spec.html#schema_primitive) are
 * null, boolean, int, long, float, double, bytes, and string. Any other types aren't supported by
 * this Serde.
 * </p>
 * <p>
 * This serde's "specific Avro" counterpart is {@link SpecificAvroSerde} and "generic Avro"
 * counterpart is {@link GenericAvroSerde}.</p>
 *
 * <p>This serde reads and writes data according to the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format. It
 * requires access to a Confluent Schema Registry endpoint, which you must {@link
 * PrimitiveAvroSerde#configure(Map, boolean)} via the parameter "schema.registry.url".</p>
 *
 * <p><strong>Usage</strong></p>
 *
 * <p>Example for configuring this serde as a Kafka Streams application's default serde for both
 * record keys and record values:</p>
 *
 * <pre>{@code
 * Properties streamsConfiguration = new Properties();
 * streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, PrimitiveAvroSerde.class);
 * streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, PrimitiveAvroSerde.class);
 * streamsConfiguration.put(
 *     AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *     "http://confluent-schema-registry-server:8081/");
 * }</pre>
 *
 * <p>
 * In practice, it's expected that the {@link PrimitiveAvroSerde} primary use case will be keys.
 * </p>
 *
 * <p>Example for explicitly overriding the application's default serdes (whatever they were
 * configured to) so that only specific operations such as {@code KStream#to()} use this serde:</p>
 *
 * <pre>{@code
 * Serde<Long> longAvroSerde = new PrimitiveAvroSerde<Long>();
 * boolean isKeySerde = true;
 * longAvroSerde.configure(
 *     Collections.singletonMap(
 *         AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *         "http://confluent-schema-registry-server:8081/"),
 *     isKeySerde);
 *
 * Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
 * isKeySerde = false;
 * genericAvroSerde.configure(
 * Collections.singletonMap(
 * AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *  "http://confluent-schema-registry-server:8081/"),
 *  isKeySerde);
 *
 * KStream<Long, GenericRecord> stream = builder.stream("my-input-topic",
 * Consumed.with(longAvroSerde, genericAvroSerde));
 *
 * }</pre>
 */

// need to have this as KafkaAvro(De)Serializer uses type Object
@SuppressWarnings("unchecked")
public class PrimitiveAvroSerde<T> implements Serde<T> {

  private final Serde<T> inner;

  public PrimitiveAvroSerde() {
    inner = (Serde<T>) Serdes.serdeFrom(new KafkaAvroSerializer(), new KafkaAvroDeserializer());
  }

  /**
   * For testing purposes only.
   */
  public PrimitiveAvroSerde(final SchemaRegistryClient client) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    inner = (Serde<T>) Serdes.serdeFrom(
        new KafkaAvroSerializer(client),
        new KafkaAvroDeserializer(client)
    );

  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

  @Override
  public Serializer<T> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<T> deserializer() {
    return inner.deserializer();
  }
}
