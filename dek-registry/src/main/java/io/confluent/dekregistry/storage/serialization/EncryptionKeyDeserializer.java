/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.storage.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.dekregistry.storage.EncryptionKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class EncryptionKeyDeserializer implements Deserializer<EncryptionKey> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public EncryptionKey deserialize(String topic, byte[] key) throws SerializationException {
    try {
      return objectMapper.readValue(key, EncryptionKey.class);
    } catch (IOException e) {
      throw new SerializationException("Error while deserializing key"
          + new String(key, StandardCharsets.UTF_8), e);
    }
  }
}
