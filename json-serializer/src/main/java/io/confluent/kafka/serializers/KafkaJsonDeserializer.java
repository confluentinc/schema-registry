/**
 * Copyright 2015 Confluent Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafka.serializers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Generic JSON deserializer.
 */
public class KafkaJsonDeserializer<T> implements Deserializer<T> {

  private ObjectMapper objectMapper;
  private Class<T> type;

  /**
   * Default constructor needed by Kafka
   */
  public KafkaJsonDeserializer() {

  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    configure(new KafkaJsonDeserializerConfig(props), isKey);
  }

  protected void configure(KafkaJsonDecoderConfig config, Class<T> type) {
    this.objectMapper = new ObjectMapper();
    this.type = type;

    boolean
        failUnknownProperties =
        config.getBoolean(KafkaJsonDeserializerConfig.FAIL_UNKNOWN_PROPERTIES);
    this.objectMapper
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, failUnknownProperties);
  }

  @SuppressWarnings("unchecked")
  private void configure(KafkaJsonDeserializerConfig config, boolean isKey) {
    if (isKey) {
      configure(config, (Class<T>) config.getClass(KafkaJsonDeserializerConfig.JSON_KEY_TYPE));
    } else {
      configure(config, (Class<T>) config.getClass(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE));
    }

  }

  @Override
  public T deserialize(String ignored, byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }

    try {
      return objectMapper.readValue(bytes, type);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  protected Class<T> getType() {
    return type;
  }

  @Override
  public void close() {

  }
}
