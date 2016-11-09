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
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;

public class SpecificAvroSerializer<T extends org.apache.avro.specific.SpecificRecord> implements Serializer<T> {

  KafkaAvroSerializer inner;

  /**
   * Constructor used by Kafka Streams.
   */
  public SpecificAvroSerializer() {
    inner = new KafkaAvroSerializer();
  }

  public SpecificAvroSerializer(SchemaRegistryClient client) {
    inner = new KafkaAvroSerializer(client);
  }

  public SpecificAvroSerializer(SchemaRegistryClient client, Map<String, ?> props) {
    inner = new KafkaAvroSerializer(client, props);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs, boolean isKey) {
    Map<String, Object> effectiveConfigs = new HashMap<>(configs);
    effectiveConfigs.put(SPECIFIC_AVRO_READER_CONFIG, true);
    inner.configure(effectiveConfigs, isKey);
  }

  @Override
  public byte[] serialize(String topic, T record) {
    return inner.serialize(topic, record);
  }

  @Override
  public void close() {
    inner.close();
  }
}