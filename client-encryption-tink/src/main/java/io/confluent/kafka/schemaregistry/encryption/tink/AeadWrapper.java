/*
 * Copyright 2025 Confluent Inc.
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
import com.google.crypto.tink.KmsClient;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AeadWrapper implements Aead {

  private static final Logger log = LoggerFactory.getLogger(AeadWrapper.class);

  public static final String ENCRYPT_ALTERNATE_KMS_KEY_IDS = "encrypt.alternate.kms.key.ids";

  private final Map<String, ?> configs;
  private final String kmsType;
  private final String kmsKeyId;
  private final List<String> kmsKeyIds;

  public AeadWrapper(Map<String, ?> configs, String kmsType, String kmsKeyId) {
    this.configs = configs;
    this.kmsType = kmsType;
    this.kmsKeyId = kmsKeyId;
    this.kmsKeyIds = getKmsKeyIds();
  }

  @Override
  public byte[] encrypt(byte[] plaintext, byte[] associatedData)
      throws GeneralSecurityException {
    Exception lastException = null;
    Exception accessDeniedException = null;
    for (int i = 0; i < kmsKeyIds.size(); i++) {
      try {
        Aead aead = getAead(configs, kmsType, kmsKeyIds.get(i));
        return aead.encrypt(plaintext, associatedData);
      } catch (Exception e) {
        log.warn("Failed to encrypt with kms key id {}: {}",
            kmsKeyIds.get(i), e.getMessage());
        lastException = e;
        if (accessDeniedException == null && isAccessDenied(e, kmsKeyIds.get(i))) {
          accessDeniedException = e;
        }
      }
    }
    if (lastException == null) {
      throw new GeneralSecurityException("No KMS key IDs available for encryption");
    }
    throw toGeneralSecurityException(
        accessDeniedException, lastException, "Failed to encrypt with all KEKs");
  }

  @Override
  public byte[] decrypt(byte[] ciphertext, byte[] associatedData)
      throws GeneralSecurityException {
    Exception lastException = null;
    Exception accessDeniedException = null;
    for (int i = 0; i < kmsKeyIds.size(); i++) {
      try {
        Aead aead = getAead(configs, kmsType, kmsKeyIds.get(i));
        return aead.decrypt(ciphertext, associatedData);
      } catch (Exception e) {
        log.warn("Failed to decrypt with kms key id {}: {}",
            kmsKeyIds.get(i), e.getMessage());
        lastException = e;
        if (accessDeniedException == null && isAccessDenied(e, kmsKeyIds.get(i))) {
          accessDeniedException = e;
        }
      }
    }
    if (lastException == null) {
      throw new GeneralSecurityException("No KMS key IDs available for decryption");
    }
    throw toGeneralSecurityException(
        accessDeniedException, lastException, "Failed to decrypt with all KEKs");
  }

  /**
   * Builds the exception to throw after every KMS key id has failed. Access-denied is aggregated
   * across all keys, not just the last one: if any key failed with an access-denied error
   * (401/403) it is preferred and surfaced as a {@link KmsAccessDeniedException} so callers can map
   * it to a 4xx — even when a later key failed for an unrelated reason. Otherwise the last failure
   * is propagated as-is.
   */
  private GeneralSecurityException toGeneralSecurityException(
      Exception accessDeniedException, Exception lastException, String fallbackMessage) {
    if (accessDeniedException != null) {
      String message = accessDeniedException.getMessage() != null
          ? accessDeniedException.getMessage() : fallbackMessage;
      return new KmsAccessDeniedException(message, accessDeniedException);
    }
    if (lastException != null) {
      return lastException instanceof GeneralSecurityException
          ? (GeneralSecurityException) lastException
          : new GeneralSecurityException(fallbackMessage, lastException);
    }
    return new GeneralSecurityException(fallbackMessage);
  }

  private boolean isAccessDenied(Throwable t, String kmsKeyId) {
    String kekUrl = kmsType + KmsDriver.KMS_TYPE_SUFFIX + kmsKeyId;
    try {
      return KmsDriverManager.getDriver(kekUrl).isAccessDenied(t);
    } catch (GeneralSecurityException e) {
      // The driver already resolved during the failed crypto attempt, so this is unexpected; log it
      // because it means a potential access-denied (4xx) will fall through to a 5xx.
      log.warn("Could not resolve KMS driver for {} to classify failure; "
          + "treating as non-access-denied", kekUrl, e);
      return false;
    }
  }

  private List<String> getKmsKeyIds() {
    List<String> kmsKeyIds = new ArrayList<>();
    kmsKeyIds.add(kmsKeyId);
    String alternateKmsKeyIds = (String) configs.get(ENCRYPT_ALTERNATE_KMS_KEY_IDS);
    if (alternateKmsKeyIds != null && !alternateKmsKeyIds.isEmpty()) {
      String[] ids = alternateKmsKeyIds.split("\\s*,\\s*");
      for (String id : ids) {
        if (!id.isEmpty()) {
          kmsKeyIds.add(id);
        }
      }
    }
    return kmsKeyIds;
  }

  private static Aead getAead(Map<String, ?> configs, String kmsType, String kmsKeyId)
      throws GeneralSecurityException {
    String kekUrl = kmsType + KmsDriver.KMS_TYPE_SUFFIX + kmsKeyId;
    KmsClient kmsClient = getKmsClient(configs, kekUrl);
    if (kmsClient == null) {
      throw new GeneralSecurityException("No kms client found for " + kekUrl);
    }
    return kmsClient.getAead(kekUrl);
  }

  private static KmsClient getKmsClient(Map<String, ?> configs, String kekUrl)
      throws GeneralSecurityException {
    try {
      return KmsDriverManager.getDriver(kekUrl).getKmsClient(kekUrl);
    } catch (GeneralSecurityException e) {
      return KmsDriverManager.getDriver(kekUrl).registerKmsClient(configs, Optional.of(kekUrl));
    }
  }
}

