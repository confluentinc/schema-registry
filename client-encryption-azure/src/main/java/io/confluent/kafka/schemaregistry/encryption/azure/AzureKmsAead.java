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
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.function.Function;

/**
 * A {@link Aead} that forwards encryption/decryption requests to a key in <a
 * href="https://azure.microsoft.com/en-us/services/key-vault/">AZURE KMS</a>.
 *
 * <p>Unlike AWS KMS, GCP KMS, and HashiCorp Vault, Azure Key Vault addresses wrap/unwrap by an
 * explicit key version and does not embed that version in the ciphertext it returns. When {@code
 * encryptTarget} is set (see {@link AzureKmsDriver#ENCRYPT_AZURE_KEY_VERSION_SAVE}), {@link
 * #encrypt} makes its output self-describing by prepending the exact version that produced it,
 * loosely mirroring HashiCorp Vault's own {@code vault:v1:<base64>} ciphertext convention: {@code
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
  private final EncryptTarget encryptTarget;
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
   *     called) to determine the client and version to prefix new output with
   * @param clientFactory builds a {@link CryptographyClient} for an arbitrary version, used by
   *     {@link #decrypt} to target whichever version is embedded in already-wrapped ciphertext,
   *     which may differ from whatever {@code encryptTarget} currently resolves to. Consulted by
   *     {@code decrypt} regardless of whether {@code encryptTarget} is set, so may be {@code null}
   *     only if {@code encryptTarget} is also {@code null} and no already-prefixed ciphertext
   *     will ever be presented to this Aead.
   */
  public AzureKmsAead(CryptographyClient defaultClient, EncryptionAlgorithm algorithm,
      EncryptTarget encryptTarget, Function<String, CryptographyClient> clientFactory) {
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
    EncryptTarget.Resolved resolved = encryptTarget.resolve();
    try {
      byte[] ciphertext = resolved.client.encrypt(this.algorithm, plaintext).getCipherText();
      byte[] versionBytes = resolved.version.getBytes(StandardCharsets.US_ASCII);
      byte[] combined = new byte[PREFIX.length + versionBytes.length + 1 + ciphertext.length];
      int pos = 0;
      System.arraycopy(PREFIX, 0, combined, pos, PREFIX.length);
      pos += PREFIX.length;
      System.arraycopy(versionBytes, 0, combined, pos, versionBytes.length);
      pos += versionBytes.length;
      combined[pos++] = ':';
      System.arraycopy(ciphertext, 0, combined, pos, ciphertext.length);
      return combined;
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
      if (clientFactory == null) {
        throw new GeneralSecurityException(
            "ciphertext carries a kms key version prefix, but this Aead has no client factory "
                + "to resolve it");
      }
      try {
        client = clientFactory.apply(version);
      } catch (AzureKmsClient.RuntimeAzureKmsException e) {
        throw e.getCause();
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
   * Returns the embedded version if {@code ciphertext} carries the {@code azure:v1:} prefix (see
   * class javadoc), or {@code null} if it does not (e.g. a legacy DEK wrapped before
   * ENCRYPT_AZURE_KEY_VERSION_SAVE was enabled on its KEK, or the toggle is not set). Returning
   * null rather than throwing is deliberate: the toggle can be flipped on/off over a KEK's
   * lifetime, and old, un-prefixed ciphertext must remain decryptable.
   */
  private static String extractVersion(byte[] ciphertext) {
    if (ciphertext.length < HEADER_LENGTH
        || !Arrays.equals(Arrays.copyOfRange(ciphertext, 0, PREFIX.length), PREFIX)
        || ciphertext[HEADER_LENGTH - 1] != ':') {
      return null;
    }
    return new String(
        ciphertext, PREFIX.length, VERSION_LENGTH, StandardCharsets.US_ASCII);
  }

  /**
   * Lazily resolved on {@link #encrypt}, not at {@code Aead} construction time, so that building
   * this {@code Aead} for a decrypt-only call site never triggers a wasted version-resolution
   * round trip.
   */
  @FunctionalInterface
  public interface EncryptTarget {
    Resolved resolve() throws GeneralSecurityException;

    final class Resolved {
      final CryptographyClient client;
      final String version;

      public Resolved(CryptographyClient client, String version) {
        this.client = client;
        this.version = version;
      }
    }
  }
}
