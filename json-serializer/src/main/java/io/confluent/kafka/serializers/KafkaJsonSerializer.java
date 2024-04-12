/*
 * Copyright 2018 Confluent Inc.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.confluent.kafka.serializers.jackson.Jackson;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serialize objects to UTF-8 JSON. This works with any object which is serializable with Jackson.
 */
public class KafkaJsonSerializer<T> implements Serializer<T> {

  private ObjectMapper objectMapper;

  /**
   * Default constructor needed by Kafka
   */
  public KafkaJsonSerializer() {

  }

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    configure(new KafkaJsonSerializerConfig(config));
  }

  protected void configure(KafkaJsonSerializerConfig config) {
    this.objectMapper = Jackson.newObjectMapper();
    boolean prettyPrint = config.getBoolean(KafkaJsonSerializerConfig.JSON_INDENT_OUTPUT);
    this.objectMapper.configure(SerializationFeature.INDENT_OUTPUT, prettyPrint);
    boolean writeDatesAsIso8601 = config.getBoolean(
        KafkaJsonSerializerConfig.WRITE_DATES_AS_ISO8601);
    this.objectMapper.configure(
        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, !writeDatesAsIso8601);
  }

  public ObjectMapper objectMapper() {
    return objectMapper;
  }

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null) {
      return null;
    }

    try {
      return objectMapper.writeValueAsBytes(data);
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  @Override
  public void close() {
  }

}
