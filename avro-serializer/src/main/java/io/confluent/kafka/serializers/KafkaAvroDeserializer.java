/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.serializers;

import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class KafkaAvroDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<Object> {

  private boolean isKey;

  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaAvroDeserializer() {

  }

  public KafkaAvroDeserializer(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  public KafkaAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    schemaRegistry = client;
    configure(deserializerConfig(props));
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaAvroDeserializerConfig(configs));
  }

  @Override
  public Object deserialize(String s, byte[] bytes) {
    return deserialize(bytes);
  }

  /**
   * Pass a reader schema to get an Avro projection
   */
  public Object deserialize(String s, byte[] bytes, Schema readerSchema) {
    return deserialize(bytes, readerSchema);
  }

  @Override
  public void close() {

  }
}
