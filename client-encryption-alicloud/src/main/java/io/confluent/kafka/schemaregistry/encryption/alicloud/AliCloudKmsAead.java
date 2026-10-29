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
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

public final class AliCloudKmsAead implements Aead {

  static final String ASSOCIATED_DATA_CONTEXT_KEY = "csfle.associated_data";

  private final AliCloudKmsKeyUri keyUri;
  private final AliCloudKmsOperations operations;

  AliCloudKmsAead(AliCloudKmsKeyUri keyUri, AliCloudKmsOperations operations) {
    this.keyUri = Objects.requireNonNull(keyUri, "keyUri");
    this.operations = Objects.requireNonNull(operations, "operations");
  }

  @Override
  public byte[] encrypt(byte[] plaintext, byte[] associatedData) throws GeneralSecurityException {
    Objects.requireNonNull(plaintext, "plaintext");
    try {
      String plaintextBase64 = Base64.getEncoder().encodeToString(plaintext);
      String ciphertext = operations.encrypt(
          keyUri.keyId(),
          plaintextBase64,
          encryptionContext(associatedData));
      if (ciphertext == null || ciphertext.isBlank()) {
        throw new GeneralSecurityException("Alibaba Cloud KMS returned an empty ciphertext blob");
      }
      return ciphertext.getBytes(StandardCharsets.UTF_8);
    } catch (GeneralSecurityException e) {
      throw e;
    } catch (Exception e) {
      throw new GeneralSecurityException("Alibaba Cloud KMS encryption failed", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] ciphertext, byte[] associatedData) throws GeneralSecurityException {
    Objects.requireNonNull(ciphertext, "ciphertext");
    try {
      String ciphertextBlob = new String(ciphertext, StandardCharsets.UTF_8);
      AliCloudKmsOperations.DecryptResult result = operations.decrypt(
          ciphertextBlob,
          encryptionContext(associatedData));
      if (result == null) {
        throw new GeneralSecurityException("Alibaba Cloud KMS returned an empty plaintext");
      }
      checkKeyId(result.keyId());
      String plaintextBase64 = result.plaintextBase64();
      if (plaintextBase64 == null || plaintextBase64.isBlank()) {
        throw new GeneralSecurityException("Alibaba Cloud KMS returned an empty plaintext");
      }
      return Base64.getDecoder().decode(plaintextBase64);
    } catch (IllegalArgumentException e) {
      throw new GeneralSecurityException("Alibaba Cloud KMS returned invalid base64 plaintext", e);
    } catch (GeneralSecurityException e) {
      throw e;
    } catch (Exception e) {
      throw new GeneralSecurityException("Alibaba Cloud KMS decryption failed", e);
    }
  }

  private static Map<String, String> encryptionContext(byte[] associatedData) {
    if (associatedData == null || associatedData.length == 0) {
      return null;
    }
    return Map.of(
        ASSOCIATED_DATA_CONTEXT_KEY,
        Base64.getEncoder().encodeToString(associatedData));
  }

  private void checkKeyId(String returnedKeyId) throws GeneralSecurityException {
    String expectedKeyId = expectedReturnedKeyId(keyUri.keyId());
    if (expectedKeyId == null) {
      return;
    }
    if (!expectedKeyId.equals(returnedKeyId)) {
      throw new GeneralSecurityException("Alibaba Cloud KMS decryption failed: wrong key id");
    }
  }

  private static String expectedReturnedKeyId(String keyId) {
    if (keyId == null || keyId.startsWith("alias/") || keyId.contains(":alias/")) {
      return null;
    }
    int arnKeyIndex = keyId.lastIndexOf(":key/");
    return arnKeyIndex >= 0 ? keyId.substring(arnKeyIndex + ":key/".length()) : keyId;
  }
}
