/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package io.confluent.kafka.streams.serdes.json;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;

/**
 * A schema-registry aware serde (serializer/deserializer) for Apache Kafka's Streams API that can
 * be used for reading and writing data in JSON format.
 */
public class KafkaJsonSchemaSerde<T> implements Serde<T> {

  private Class<T> specificClass;
  private final Serde<T> inner;

  public KafkaJsonSchemaSerde() {
    inner = Serdes.serdeFrom(new KafkaJsonSchemaSerializer<>(),
        new KafkaJsonSchemaDeserializer<>());
  }

  public KafkaJsonSchemaSerde(Class<T> specificClass) {
    this.specificClass = specificClass;
    inner = Serdes.serdeFrom(new KafkaJsonSchemaSerializer<>(),
        new KafkaJsonSchemaDeserializer<>());
  }

  /**
   * For testing purposes only.
   */
  public KafkaJsonSchemaSerde(final SchemaRegistryClient client) {
    this(client, null);
  }

  /**
   * For testing purposes only.
   */
  public KafkaJsonSchemaSerde(final SchemaRegistryClient client, final Class<T> specificClass) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    this.specificClass = specificClass;
    inner = Serdes.serdeFrom(new KafkaJsonSchemaSerializer<>(client),
            new KafkaJsonSchemaDeserializer<>(client));
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
    inner.deserializer().configure(withSpecificClass(serdeConfig, isSerdeForRecordKeys),
        isSerdeForRecordKeys);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

  private Map<String, Object> withSpecificClass(final Map<String, ?> config, boolean isKey) {
    if (specificClass == null) {
      return (Map<String, Object>) config;
    }
    Map<String, Object> newConfig =
        config == null ? new HashMap<String, Object>() : new HashMap<>(config);
    if (isKey) {
      newConfig.put(
          KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE,
          specificClass
      );
    } else {
      newConfig.put(
          KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,
          specificClass
      );
    }
    return newConfig;
  }

}
