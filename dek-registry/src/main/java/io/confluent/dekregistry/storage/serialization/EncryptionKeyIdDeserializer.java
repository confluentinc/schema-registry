/*
 * Copyright 2023 Confluent Inc.
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
