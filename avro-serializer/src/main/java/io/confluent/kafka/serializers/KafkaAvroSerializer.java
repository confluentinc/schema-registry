/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.serializers;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class KafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<Object> {

  private boolean isKey;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaAvroSerializer() {

  }

  public KafkaAvroSerializer(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  public KafkaAvroSerializer(SchemaRegistryClient client, Map<String, ?> props) {
    schemaRegistry = client;
    configure(serializerConfig(props));
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaAvroSerializerConfig(configs));
  }

  @Override
  public byte[] serialize(String topic, Object record) {
    return serializeImpl(getSubjectName(topic, isKey), record);
  }

  @Override
  public void close() {

  }
}
