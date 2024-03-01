/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.tink;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.DeterministicAead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.Registry;
import com.google.crypto.tink.TinkProtoParametersFormat;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.daead.DeterministicAeadConfig;
import com.google.crypto.tink.proto.KeyTemplate;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.BufferUnderflowException;
import java.security.GeneralSecurityException;

public class Cryptor {

  static {
    try {
      AeadConfig.register();
      DeterministicAeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private final DekFormat dekFormat;
  private final KeyTemplate dekTemplate;

  public Cryptor(DekFormat dekFormat) throws GeneralSecurityException {
    KeyTemplate dekTemplate;
    try {
      dekTemplate = KeyTemplate.parseFrom(
          TinkProtoParametersFormat.serialize(dekFormat.getParameters()),
          ExtensionRegistryLite.getEmptyRegistry());
    } catch (GeneralSecurityException e) {
      // Use deprecated Tink APIs to support older versions of Tink
      com.google.crypto.tink.KeyTemplate keyTemplate = KeyTemplates.get(dekFormat.name());
      dekTemplate = com.google.crypto.tink.proto.KeyTemplate.newBuilder()
          .setTypeUrl(keyTemplate.getTypeUrl())
          .setValue(ByteString.copyFrom(keyTemplate.getValue()))
          .build();

    } catch (InvalidProtocolBufferException e) {
      throw new GeneralSecurityException(e);
    }
    this.dekFormat = dekFormat;
    this.dekTemplate = dekTemplate;
  }

  public DekFormat getDekFormat() {
    return dekFormat;
  }

  public byte[] generateKey() throws GeneralSecurityException {
    return Registry.newKeyData(dekTemplate).getValue().toByteArray();
  }

  public byte[] encrypt(byte[] dek, byte[] plaintext, byte[] associatedData)
      throws GeneralSecurityException {
    // Use DEK to encrypt plaintext.
    byte[] ciphertext;
    if (dekFormat.isDeterministic()) {
      DeterministicAead aead = Registry.getPrimitive(
          dekTemplate.getTypeUrl(), dek, DeterministicAead.class);
      ciphertext = aead.encryptDeterministically(plaintext, associatedData);
    } else {
      Aead aead = Registry.getPrimitive(dekTemplate.getTypeUrl(), dek, Aead.class);
      ciphertext = aead.encrypt(plaintext, associatedData);
    }
    return ciphertext;
  }

  public byte[] decrypt(byte[] dek, byte[] ciphertext, byte[] associatedData)
      throws GeneralSecurityException {
    try {
      // Use DEK to decrypt ciphertext.
      if (dekFormat.isDeterministic()) {
        DeterministicAead aead = Registry.getPrimitive(
            dekTemplate.getTypeUrl(), dek, DeterministicAead.class);
        return aead.decryptDeterministically(ciphertext, associatedData);
      } else {
        Aead aead = Registry.getPrimitive(dekTemplate.getTypeUrl(), dek, Aead.class);
        return aead.decrypt(ciphertext, associatedData);
      }
    } catch (IndexOutOfBoundsException
             | BufferUnderflowException
             | NegativeArraySizeException e) {
      throw new GeneralSecurityException("invalid ciphertext", e);
    }
  }
}
