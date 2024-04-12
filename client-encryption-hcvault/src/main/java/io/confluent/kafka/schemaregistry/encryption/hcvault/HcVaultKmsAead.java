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

package io.confluent.kafka.schemaregistry.encryption.hcvault;

import io.github.jopenlibs.vault.api.Logical;
import io.github.jopenlibs.vault.response.LogicalResponse;
import io.github.jopenlibs.vault.VaultException;
import com.google.crypto.tink.Aead;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Map;

/**
 * A {@link Aead} that forwards encryption/decryption requests to a key in <a
 * href="https://www.vaultproject.io/docs/secrets/transit">Vault Transit Secrets Engine</a>.
 */
public class HcVaultKmsAead implements Aead {

  private final Logical vault;
  private final String encryptPath;
  private final String decryptPath;


  public HcVaultKmsAead(Logical vault, String keyUri) throws GeneralSecurityException {
    this.vault = vault;
    this.encryptPath = getOperationPath(keyUri, "encrypt");
    this.decryptPath = getOperationPath(keyUri, "decrypt");
  }

  private String getOperationPath(String keyUri, String op) throws GeneralSecurityException {
    try {
      URI uri = new URI(keyUri);
      String key = uri.getPath().substring(1);
      String[] parts = key.split("/");
      parts[1] = op;
      return String.join("/", parts);
    } catch (URISyntaxException e) {
      throw new GeneralSecurityException("could not process uri " + keyUri, e);
    }
  }

  @Override
  public byte[] encrypt(byte[] plaintext, byte[] associatedData) throws GeneralSecurityException {
    Map<String, Object> request = ImmutableMap.of(
        "plaintext", Base64.getEncoder().encodeToString(plaintext),
        "context", associatedData == null ? "" : Base64.getEncoder().encodeToString(associatedData)
    );

    try {
      LogicalResponse response = vault.write(this.encryptPath, request);
      Map<String, String> data = response.getData();
      String error = data.get("errors");
      if (error != null) {
        throw new GeneralSecurityException("failed to encrypt");
      }

      String ciphertext = data.get("ciphertext");
      if (ciphertext == null) {
        String err = new String(response.getRestResponse().getBody(), StandardCharsets.UTF_8);
        throw new GeneralSecurityException("encryption failed: " + err);
      }
      return ciphertext.getBytes(StandardCharsets.UTF_8);
    } catch (VaultException e) {
      throw new GeneralSecurityException("encryption failed", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] ciphertext, byte[] associatedData) throws GeneralSecurityException {
    Map<String, Object> request = ImmutableMap.of(
        "ciphertext", new String(ciphertext, StandardCharsets.UTF_8),
        "context", associatedData == null ? "" : Base64.getEncoder().encodeToString(associatedData)
    );

    try {
      LogicalResponse response = vault.write(this.decryptPath, request);
      Map<String, String> data = response.getData();
      String error = data.get("errors");
      if (error != null) {
        throw new GeneralSecurityException("failed to decrypt");
      }

      String plaintext64 = response.getData().get("plaintext");
      if (plaintext64 == null) {
        throw new GeneralSecurityException("decryption failed");
      }
      return Base64.getDecoder().decode(plaintext64);

    } catch (VaultException e) {
      throw new GeneralSecurityException("decryption failed", e);
    }
  }
}