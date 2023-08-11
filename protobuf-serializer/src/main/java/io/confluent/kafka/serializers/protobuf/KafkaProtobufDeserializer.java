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

package io.confluent.kafka.serializers.protobuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class KafkaProtobufDeserializer<T extends Message>
    extends AbstractKafkaProtobufDeserializer<T> implements Deserializer<T> {

  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaProtobufDeserializer() {

  }

  public KafkaProtobufDeserializer(SchemaRegistryClient client) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public KafkaProtobufDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    this(client, props, null);
  }

  @VisibleForTesting
  public KafkaProtobufDeserializer(SchemaRegistryClient client,
                                   Map<String, ?> props,
                                   Class<T> type) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    configure(deserializerConfig(props), type);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(new KafkaProtobufDeserializerConfig(configs), isKey);
  }

  @SuppressWarnings("unchecked")
  protected void configure(KafkaProtobufDeserializerConfig config, boolean isKey) {
    this.isKey = isKey;
    if (isKey) {
      configure(
          config,
          (Class<T>) config.getClass(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_KEY_TYPE)
      );
    } else {
      configure(
          config,
          (Class<T>) config.getClass(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE)
      );
    }
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    return deserialize(topic, null, bytes);
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] bytes) {
    return (T) deserialize(false, topic, isKey, headers, bytes);
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (IOException e) {
      throw new RuntimeException("Exception while closing deserializer", e);
    }
  }
}
