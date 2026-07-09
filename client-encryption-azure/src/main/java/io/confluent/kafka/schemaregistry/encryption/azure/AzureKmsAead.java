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
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link Aead} that forwards encryption/decryption requests to a key in <a
 * href="https://azure.microsoft.com/en-us/services/key-vault/">AZURE KMS</a>.
 *
 * <p>Unlike AWS KMS and GCP KMS, Azure Key Vault addresses wrap/unwrap by an explicit key version
 * and does not embed that version in the ciphertext it returns. When {@code encryptTarget} is set
 * (see {@link AzureKmsDriver#ENCRYPT_AZURE_KEY_VERSION_SAVE}), {@link #encrypt} makes its output
 * self-describing by prepending the exact version that produced it: {@code
 * azure:v1:<32-character key version>:<raw ciphertext bytes>}.
 *
 * <p>{@link #decrypt} always checks for this prefix regardless of the current {@code
 * encryptTarget}, since a DEK wrapped while the toggle was on must remain decryptable even after
 * it is turned back off.
 */
public final class AzureKmsAead implements Aead {

  private static final byte[] PREFIX =
      "azure:v1:".getBytes(StandardCharsets.US_ASCII);
  private static final int VERSION_LENGTH = 32;
  private static final int HEADER_LENGTH = PREFIX.length + VERSION_LENGTH + 1; // +1 for ':'

  private final CryptographyClient defaultClient;
  private final EncryptionAlgorithm algorithm;
  private final Supplier<Map.Entry<CryptographyClient, String>> encryptTarget;
  private final Function<String, CryptographyClient> clientFactory;

  /**
   * Constructor
   *
   * @param defaultClient -  kms client
   * @param algorithm - algorithm
   */
  public AzureKmsAead(CryptographyClient defaultClient, EncryptionAlgorithm algorithm) {
    this(defaultClient, algorithm, null, null);
  }

  /**
   * @param defaultClient used when encrypting with {@code encryptTarget} unset, and when
   *     decrypting ciphertext with no embedded version prefix
   * @param algorithm algorithm
   * @param encryptTarget if non-null, resolved lazily (not until {@link #encrypt} is actually
   *     called) to determine the client and version (the entry's key and value, respectively) to
   *     prefix new output with. May throw {@link AzureKmsClient.RuntimeAzureKmsException} to
   *     surface a checked {@link GeneralSecurityException} from resolution.
   * @param clientFactory builds a {@link CryptographyClient} for an arbitrary version, used by
   *     {@link #decrypt} to target whichever version is embedded in already-wrapped ciphertext,
   *     which may differ from whatever {@code encryptTarget} currently resolves to. Consulted by
   *     {@code decrypt} regardless of whether {@code encryptTarget} is set, so may be {@code null}
   *     only if {@code encryptTarget} is also {@code null} and no already-prefixed ciphertext
   *     will ever be presented to this Aead.
   */
  public AzureKmsAead(CryptographyClient defaultClient, EncryptionAlgorithm algorithm,
      Supplier<Map.Entry<CryptographyClient, String>> encryptTarget,
      Function<String, CryptographyClient> clientFactory) {
    this.defaultClient = defaultClient;
    this.algorithm = algorithm;
    this.encryptTarget = encryptTarget;
    this.clientFactory = clientFactory;
  }

  @Override
  public byte[] encrypt(final byte[] plaintext, final byte[] associatedData)
      throws GeneralSecurityException {
    if (encryptTarget == null) {
      try {
        return defaultClient.encrypt(this.algorithm, plaintext).getCipherText();
      } catch (AzureException e) {
        throw new GeneralSecurityException("encryption failed", e);
      }
    }
    Map.Entry<CryptographyClient, String> resolved;
    try {
      resolved = encryptTarget.get();
    } catch (AzureKmsClient.RuntimeAzureKmsException e) {
      throw e.getCause();
    } catch (RuntimeException e) {
      throw new GeneralSecurityException("failed to resolve kms client for encryption", e);
    }
    String version = resolved.getValue();
    if (!isValidVersion(version)) {
      // Mirrors decrypt()'s own validation: a DEK this method wraps must always be one this same
      // class can later unwrap.
      throw new GeneralSecurityException(
          "kms key version '" + version + "' must be a " + VERSION_LENGTH
              + "-character hex string; cannot be embedded in a fixed-width azure:v1: prefix");
    }
    try {
      byte[] ciphertext = resolved.getKey().encrypt(this.algorithm, plaintext).getCipherText();
      byte[] versionBytes = version.getBytes(StandardCharsets.US_ASCII);
      ByteArrayOutputStream out = new ByteArrayOutputStream(
          PREFIX.length + versionBytes.length + 1 + ciphertext.length);
      // The 3-arg write(byte[], int, int) overload is used throughout (even for whole arrays)
      // because it is the one ByteArrayOutputStream documents as never throwing IOException;
      // the inherited write(byte[]) convenience overload still declares the checked exception.
      out.write(PREFIX, 0, PREFIX.length);
      out.write(versionBytes, 0, versionBytes.length);
      out.write(':');
      out.write(ciphertext, 0, ciphertext.length);
      return out.toByteArray();
    } catch (AzureException e) {
      throw new GeneralSecurityException("encryption failed", e);
    }
  }

  @Override
  public byte[] decrypt(final byte[] ciphertext, final byte[] associatedData)
      throws GeneralSecurityException {
    CryptographyClient client = defaultClient;
    byte[] wrapped = ciphertext;
    String version = extractVersion(ciphertext);
    if (version != null) {
      if (!isValidVersion(version)) {
        // encryptedKeyMaterial is unauthenticated at this layer, so a corrupted or tampered
        // value could otherwise smuggle arbitrary characters (e.g. '/') into the key identifier
        // URL built from it below.
        throw new GeneralSecurityException(
            "ciphertext carries an invalid azure:v1: key version: '" + version + "'");
      }
      if (clientFactory == null) {
        throw new GeneralSecurityException(
            "ciphertext carries a kms key version prefix, but this Aead has no client factory "
                + "to resolve it");
      }
      try {
        client = clientFactory.apply(version);
      } catch (AzureKmsClient.RuntimeAzureKmsException e) {
        throw e.getCause();
      } catch (RuntimeException e) {
        // decrypt()'s contract is to only ever throw GeneralSecurityException; do not let an
        // unexpected RuntimeException from the client factory (or the Azure SDK calls it makes)
        // leak out uncaught.
        throw new GeneralSecurityException(
            "failed to resolve kms client for embedded key version '" + version + "'", e);
      }
      wrapped = Arrays.copyOfRange(ciphertext, HEADER_LENGTH, ciphertext.length);
    }
    try {
      return client.decrypt(this.algorithm, wrapped).getPlainText();
    } catch (AzureException e) {
      throw new GeneralSecurityException("decryption failed", e);
    }
  }

  /**
   * Returns true if {@code value} is exactly {@value #VERSION_LENGTH} hex characters, the only
   * shape that can be embedded in (and later parsed back out of) the fixed-width azure:v1:
   * prefix. Used to validate both a freshly resolved version (in {@link #encrypt}) and one
   * extracted from ciphertext (in {@link #decrypt}), since encryptedKeyMaterial is unauthenticated
   * at this layer and could be corrupted or tampered with.
   */
  private static boolean isValidVersion(String value) {
    if (value == null || value.length() != VERSION_LENGTH) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      boolean isHexDigit = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
      if (!isHexDigit) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the embedded version if {@code ciphertext} carries the {@code azure:v1:} prefix (see
   * class javadoc), or {@code null} if it does not (e.g. a legacy DEK wrapped before
   * ENCRYPT_AZURE_KEY_VERSION_SAVE was enabled on its KEK, or the toggle is not set). Returning
   * null rather than throwing is deliberate: the toggle can be flipped on/off over a KEK's
   * lifetime, and old, un-prefixed ciphertext must remain decryptable.
   */
  private static String extractVersion(byte[] ciphertext) {
    // Arrays.mismatch compares the prefix range in place, avoiding an array copy on every call
    // (this runs for every decrypt(), including the common case of legacy, un-prefixed
    // ciphertext).
    if (ciphertext.length < HEADER_LENGTH
        || Arrays.mismatch(ciphertext, 0, PREFIX.length, PREFIX, 0, PREFIX.length) != -1
        || ciphertext[HEADER_LENGTH - 1] != ':') {
      return null;
    }
    return new String(
        ciphertext, PREFIX.length, VERSION_LENGTH, StandardCharsets.US_ASCII);
  }
}
