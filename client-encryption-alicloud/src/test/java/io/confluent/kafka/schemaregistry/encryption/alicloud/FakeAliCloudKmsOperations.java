/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.alicloud;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A partial, fake implementation of Alibaba Cloud KMS operations for tests.
 *
 * <p>It creates one AEAD for every valid key ID. Encrypt uses the requested key, while decrypt
 * tries all registered keys and reports the key that successfully decrypts the ciphertext.
 */
final class FakeAliCloudKmsOperations implements AliCloudKmsOperations {

  private final Map<String, Aead> aeads = new HashMap<>();

  FakeAliCloudKmsOperations(List<String> validKeyIds) throws GeneralSecurityException {
    for (String keyId : validKeyIds) {
      Aead aead = KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM")).getPrimitive(Aead.class);
      aeads.put(keyId, aead);
    }
  }

  @Override
  public String encrypt(String keyId, String plaintextBase64, Map<String, ?> encryptionContext)
      throws GeneralSecurityException {
    if (!aeads.containsKey(keyId)) {
      throw new GeneralSecurityException(
          "Unknown key ID : " + keyId + " is not in " + aeads.keySet());
    }
    byte[] plaintext = Base64.getDecoder().decode(plaintextBase64);
    byte[] ciphertext = aeads.get(keyId).encrypt(plaintext, serializeContext(encryptionContext));
    return Base64.getEncoder().encodeToString(ciphertext);
  }

  @Override
  public DecryptResult decrypt(String ciphertextBlob, Map<String, ?> encryptionContext)
      throws GeneralSecurityException {
    byte[] ciphertext = Base64.getDecoder().decode(ciphertextBlob);
    byte[] context = serializeContext(encryptionContext);
    for (Map.Entry<String, Aead> entry : aeads.entrySet()) {
      try {
        byte[] plaintext = entry.getValue().decrypt(ciphertext, context);
        return new DecryptResult(Base64.getEncoder().encodeToString(plaintext), entry.getKey());
      } catch (GeneralSecurityException e) {
        // Try the next key.
      }
    }
    throw new GeneralSecurityException("unable to decrypt");
  }

  private static byte[] serializeContext(Map<String, ?> encryptionContext) {
    if (encryptionContext == null || encryptionContext.isEmpty()) {
      return new byte[0];
    }
    TreeMap<String, String> ordered = new TreeMap<>();
    encryptionContext.forEach((key, value) -> ordered.put(key, String.valueOf(value)));
    return ordered.toString().getBytes(StandardCharsets.UTF_8);
  }
}
