/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage.encoder;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class KeysetWrapperDeserializer implements Deserializer<KeysetWrapper> {

  private Aead aead;
  private Aead oldAead;

  public KeysetWrapperDeserializer(SchemaRegistryConfig config) {
    try {
      String secret = MetadataEncoderService.encoderSecret(config);
      aead = MetadataEncoderService.getPrimitive(secret);
      String oldSecret = MetadataEncoderService.encoderOldSecret(config);
      if (oldSecret != null) {
        oldAead = MetadataEncoderService.getPrimitive(oldSecret);
      }
    } catch (GeneralSecurityException e) {
      throw new ConfigException("Error while configuring KeysetWrapperDeserializer", e);
    }
  }

  @Override
  public KeysetWrapper deserialize(String topic, byte[] key) throws SerializationException {
    try {
      KeysetHandle keysetHandle = KeysetHandle.read(JsonKeysetReader.withBytes(key), aead);
      return new KeysetWrapper(keysetHandle, false);
    } catch (GeneralSecurityException | IOException e) {
      if (oldAead != null) {
        // ignore, next try old secret
      } else {
        throw new SerializationException("Error while deserializing KeysetWrapper", e);
      }
    }
    try {
      KeysetHandle keysetHandle = KeysetHandle.read(JsonKeysetReader.withBytes(key), oldAead);
      return new KeysetWrapper(keysetHandle, true);
    } catch (GeneralSecurityException | IOException e) {
      throw new SerializationException("Error while deserializing KeysetWrapper", e);
    }
  }
}
