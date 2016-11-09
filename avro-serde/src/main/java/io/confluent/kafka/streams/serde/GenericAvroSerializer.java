/**
 * Copyright 2016 Confluent Inc.
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
package io.confluent.kafka.streams.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GenericAvroSerializer implements Serializer<GenericRecord> {

  KafkaAvroSerializer inner;

  /**
   * Constructor used by Kafka Streams.
   */
  public GenericAvroSerializer() {
    inner = new KafkaAvroSerializer();
  }

  public GenericAvroSerializer(SchemaRegistryClient client) {
    inner = new KafkaAvroSerializer(client);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, GenericRecord record) {
    return inner.serialize(topic, record);
  }

  @Override
  public void close() {
    inner.close();
  }
}