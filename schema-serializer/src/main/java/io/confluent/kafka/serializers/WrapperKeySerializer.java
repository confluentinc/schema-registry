/*
 * Copyright 2023 Confluent Inc.
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

import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public class WrapperKeySerializer<T> implements Serializer<T> {

  private Serializer<T> inner;

  /**
   * Constructor used by Kafka producer.
   */
  public WrapperKeySerializer() {
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(new WrapperKeySerializerConfig(configs), isKey);
  }

  @SuppressWarnings("unchecked")
  protected void configure(WrapperKeySerializerConfig config, boolean isKey) {
    if (!isKey) {
      throw new IllegalArgumentException("WrapperKeySerializer is only for keys");
    }
    this.inner = config.getConfiguredInstance(
        WrapperKeySerializerConfig.WRAPPED_KEY_SERIALIZER, Serializer.class);
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return serialize(topic, null, data);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T data) {
    try {
      return inner.serialize(topic, headers, data);
    } finally {
      AbstractKafkaSchemaSerDe.setKey(data);
    }
  }

  @Override
  public void close() {
    inner.close();
  }
}
