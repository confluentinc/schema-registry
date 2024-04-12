/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.storage.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.dekregistry.storage.EncryptionKeyId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class EncryptionKeyIdDeserializer implements Deserializer<EncryptionKeyId> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public EncryptionKeyId deserialize(String topic, byte[] key) throws SerializationException {
    try {
      return objectMapper.readValue(key, EncryptionKeyId.class);
    } catch (IOException e) {
      throw new SerializationException("Error while deserializing key"
          + new String(key, StandardCharsets.UTF_8), e);
    }
  }
}
