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
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;

public class SpecificAvroDeserializer<T extends org.apache.avro.specific.SpecificRecord> implements Deserializer<T> {

  KafkaAvroDeserializer inner;

  /**
   * Constructor used by Kafka Streams.
   */
  public SpecificAvroDeserializer() {
    inner = new KafkaAvroDeserializer();
  }

  public SpecificAvroDeserializer(SchemaRegistryClient client) {
    inner = new KafkaAvroDeserializer(client);
  }

  public SpecificAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    inner = new KafkaAvroDeserializer(client, props);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs, boolean isKey) {
    Map<String, Object> effectiveConfigs = new HashMap<>(configs);
    effectiveConfigs.put(SPECIFIC_AVRO_READER_CONFIG, true);
    inner.configure(effectiveConfigs, isKey);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T deserialize(String s, byte[] bytes) {
    return (T) inner.deserialize(s, bytes);
  }

  @Override
  public void close() {
    inner.close();
  }
}