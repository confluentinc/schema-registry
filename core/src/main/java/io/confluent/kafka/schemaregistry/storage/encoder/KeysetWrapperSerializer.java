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
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class KeysetWrapperSerializer implements Serializer<KeysetWrapper> {

  private Aead aead;

  public KeysetWrapperSerializer(SchemaRegistryConfig config) {
    try {
      String secret = MetadataEncoderService.encoderSecret(config);
      aead = MetadataEncoderService.getPrimitive(secret);
    } catch (GeneralSecurityException e) {
      throw new ConfigException("Error while configuring KeysetWrapperSerializer", e);
    }
  }

  @Override
  public byte[] serialize(String topic, KeysetWrapper key) {
    try {
      KeysetHandle keysetHandle = key.getKeysetHandle();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      keysetHandle.write(JsonKeysetWriter.withOutputStream(baos), aead);
      return baos.toByteArray();
    } catch (GeneralSecurityException | IOException e) {
      throw new SerializationException("Error while serializing KeysetWrapper", e);
    }
  }
}
