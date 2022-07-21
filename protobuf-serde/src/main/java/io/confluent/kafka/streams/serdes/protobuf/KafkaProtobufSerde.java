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

package io.confluent.kafka.streams.serdes.protobuf;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

/**
 * A schema-registry aware serde (serializer/deserializer) for Apache Kafka's Streams API that can
 * be used for reading and writing data in Protocol Buffers format.
 */
public class KafkaProtobufSerde<T extends Message> implements Serde<T> {

  private Class<T> specificProtobufClass;
  private final Serde<T> inner;

  public KafkaProtobufSerde() {
    inner = Serdes.serdeFrom(new KafkaProtobufSerializer<>(),
        new KafkaProtobufDeserializer<>());
  }

  public KafkaProtobufSerde(Class<T> specificProtobufClass) {
    this.specificProtobufClass = specificProtobufClass;
    inner = Serdes.serdeFrom(new KafkaProtobufSerializer<>(),
        new KafkaProtobufDeserializer<>());
  }

  /**
   * For testing purposes only.
   */
  public KafkaProtobufSerde(final SchemaRegistryClient client) {
    this(client, null);
  }

  /**
   * For testing purposes only.
   */
  public KafkaProtobufSerde(final SchemaRegistryClient client, final Class<T> specificClass) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    this.specificProtobufClass = specificClass;
    inner = Serdes.serdeFrom(new KafkaProtobufSerializer<>(client),
            new KafkaProtobufDeserializer<>(client));
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
    inner.deserializer().configure(
        withSpecificClass(serdeConfig, isSerdeForRecordKeys),
        isSerdeForRecordKeys
    );
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

  private Map<String, Object> withSpecificClass(final Map<String, ?> config, boolean isKey) {
    if (specificProtobufClass == null) {
      return (Map<String, Object>) config;
    }
    Map<String, Object> newConfig =
        config == null ? new HashMap<String, Object>() : new HashMap<>(config);
    if (isKey) {
      newConfig.put(
          KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_KEY_TYPE,
          specificProtobufClass
      );
    } else {
      newConfig.put(
          KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
          specificProtobufClass
      );
    }
    return newConfig;
  }

}
