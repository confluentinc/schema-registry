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
 */


package io.confluent.kafka.serializers.json;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;

public class KafkaJsonSchemaSerializer<T> extends AbstractKafkaJsonSchemaSerializer<T>
    implements Serializer<T> {

  private boolean isKey;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaJsonSchemaSerializer() {
  }

  public KafkaJsonSchemaSerializer(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  public KafkaJsonSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props) {
    schemaRegistry = client;
    configure(serializerConfig(props));
  }

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaJsonSchemaSerializerConfig(config));
  }

  @Override
  public byte[] serialize(String topic, T record) {
    if (record == null) {
      return null;
    }
    JsonSchema schema = JsonSchemaUtils.getSchema(record);
    Object value = JsonSchemaUtils.getValue(record);
    return serializeImpl(getSubjectName(topic, isKey, value, schema), (T) value, schema);
  }

  @Override
  public void close() {
  }
}
