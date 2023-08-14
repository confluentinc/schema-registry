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

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * Generic JSON deserializer.
 */
public class KafkaJsonSchemaDeserializer<T> extends AbstractKafkaJsonSchemaDeserializer<T>
    implements Deserializer<T> {

  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaJsonSchemaDeserializer() {
  }

  public KafkaJsonSchemaDeserializer(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  public KafkaJsonSchemaDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    this(client, props, null);
  }

  @VisibleForTesting
  public KafkaJsonSchemaDeserializer(
      SchemaRegistryClient client,
      Map<String, ?> props,
      Class<T> type
  ) {
    schemaRegistry = client;
    configure(deserializerConfig(props), type);
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    configure(new KafkaJsonSchemaDeserializerConfig(props), isKey);
  }

  @SuppressWarnings("unchecked")
  protected void configure(KafkaJsonSchemaDeserializerConfig config, boolean isKey) {
    this.isKey = isKey;
    if (isKey) {
      configure(
          config,
          (Class<T>) config.getClass(KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE)
      );
    } else {
      configure(
          config,
          (Class<T>) config.getClass(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE)
      );
    }
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    return (T) deserialize(false, topic, isKey, bytes);
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
