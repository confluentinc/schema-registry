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

package io.confluent.kafka.schemaregistry.encryption;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.DeterministicAead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.Registry;
import com.google.crypto.tink.proto.KeyTemplate;
import com.google.protobuf.ByteString;
import java.nio.BufferUnderflowException;
import java.security.GeneralSecurityException;

public class Cryptor {

  public static final String RANDOM_KEY_FORMAT = "AES128_GCM";
  public static final String DETERMINISTIC_KEY_FORMAT = "AES256_SIV";

  private final DekFormat dekFormat;
  private final KeyTemplate dekTemplate;

  public Cryptor(DekFormat dekFormat) throws GeneralSecurityException {
    this.dekFormat = dekFormat;
    com.google.crypto.tink.KeyTemplate keyTemplate = KeyTemplates.get(dekFormat.name());
    this.dekTemplate = com.google.crypto.tink.proto.KeyTemplate.newBuilder()
        .setTypeUrl(keyTemplate.getTypeUrl())
        .setValue(ByteString.copyFrom(keyTemplate.getValue()))
        .build();
  }

  public DekFormat getDekFormat() {
    return dekFormat;
  }

  public byte[] generateKey() throws GeneralSecurityException {
    return Registry.newKey(dekTemplate).toByteArray();
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

  public enum DekFormat {
    AES128_GCM(false),
    AES256_GCM(false),
    AES256_SIV(true);

    private boolean isDeterministic;

    DekFormat(boolean isDeterministic) {
      this.isDeterministic = isDeterministic;
    }

    public boolean isDeterministic() {
      return isDeterministic;
    }
  }
}
