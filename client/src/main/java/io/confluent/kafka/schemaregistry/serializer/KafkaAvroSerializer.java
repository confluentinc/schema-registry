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
package io.confluent.kafka.schemaregistry.serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.SchemaRegistryClient;

public class KafkaAvroSerializer extends AbstracKafkaAvroSerializer implements Serializer<Object> {

  @Override
  public void configure(Map<String,?> configs, boolean isKey) {
    Object url = configs.get(propertyName);
    if (url == null ) {
      throw new IllegalArgumentException("Missing Schema registry url!");
    }
    schemaRegistry = new SchemaRegistryClient((String) url);
  }


  @Override
  public byte[] serialize(String topic, Object record) {
    return serializeImpl(topic, record);
  }

  @Override
  public void close() {

  }
}
