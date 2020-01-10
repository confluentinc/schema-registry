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
import com.google.protobuf.MessageLite;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class KafkaProtobufDeserializer<T extends MessageLite>
    extends AbstractKafkaProtobufDeserializer<T> implements Deserializer<T> {

  private boolean isKey;

  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaProtobufDeserializer() {

  }

  public KafkaProtobufDeserializer(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @VisibleForTesting
  public KafkaProtobufDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    schemaRegistry = client;
    configure(deserializerConfig(props));
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaProtobufDeserializerConfig(configs));
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    return deserialize(bytes);
  }

  @Override
  public void close() {

  }
}
