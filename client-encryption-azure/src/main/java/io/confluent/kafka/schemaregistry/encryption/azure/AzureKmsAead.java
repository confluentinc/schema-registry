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

package io.confluent.kafka.schemaregistry.encryption.azure;

import com.azure.core.exception.AzureException;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import com.google.crypto.tink.Aead;
import java.security.GeneralSecurityException;

/**
 * A {@link Aead} that forwards encryption/decryption requests to a key in <a
 * href="https://azure.microsoft.com/en-us/services/key-vault/">AZURE KMS</a>.
 */
public final class AzureKmsAead implements Aead {

  /**
   * Azure crypto client
   */
  private final CryptographyClient kmsClient;

  /**
   * Encryption algorithm
   */
  private final EncryptionAlgorithm algorithm;

  /**
   * Constructor
   *
   * @param kmsClient -  kms client
   * @param algorithm - algorithm
   */
  public AzureKmsAead(CryptographyClient kmsClient, EncryptionAlgorithm algorithm) {
    this.kmsClient = kmsClient;
    this.algorithm = algorithm;
  }

  @Override
  public byte[] encrypt(final byte[] plaintext, final byte[] associatedData)
      throws GeneralSecurityException {
    try {
      return kmsClient.encrypt(this.algorithm, plaintext).getCipherText();
    } catch (AzureException e) {
      throw new GeneralSecurityException("encryption failed", e);
    }
  }

  @Override
  public byte[] decrypt(final byte[] ciphertext, final byte[] associatedData)
      throws GeneralSecurityException {
    try {
      return kmsClient.decrypt(this.algorithm, ciphertext).getPlainText();
    } catch (AzureException e) {
      throw new GeneralSecurityException("decryption failed", e);
    }
  }
}
