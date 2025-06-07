/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dpregistry.storage.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.dpregistry.storage.DataProductKey;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class DataProductKeySerializer implements Serializer<DataProductKey> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, DataProductKey key) {
    try {
      return objectMapper.writeValueAsBytes(key);
    } catch (IOException e) {
      throw new SerializationException("Error while serializing key info"
          + key.toString(), e);
    }
  }
}
