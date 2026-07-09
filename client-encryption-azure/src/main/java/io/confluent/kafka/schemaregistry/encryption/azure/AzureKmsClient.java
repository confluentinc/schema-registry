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

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.policy.ExponentialBackoff;
import com.azure.core.http.policy.RetryPolicy;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import com.azure.security.keyvault.keys.implementation.KeyVaultCredentialPolicy;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.subtle.Validators;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link KmsClient} for <a
 * href="https://azure.microsoft.com/en-us/services/key-vault/">Azure KMS</a>.
 */
public final class AzureKmsClient implements KmsClient {

  private static final Logger log = LoggerFactory.getLogger(AzureKmsClient.class);

  public static final String PREFIX = "azure-kms://";

  private CryptographyClient cryptographyClient;
  private String keyUri;
  private TokenCredential provider;
  private Map<String, ?> configs;
  private static final EncryptionAlgorithm DEFAULT_ENCRYPTION_ALGORITHM =
      EncryptionAlgorithm.RSA_OAEP_256;
  private EncryptionAlgorithm algorithm = DEFAULT_ENCRYPTION_ALGORITHM;

  public AzureKmsClient() {
  }

  /**
   * Constructs a specific AzureKmsClient that is bound to a single key identified by {@code uri}.
   */

  public AzureKmsClient(String uri) {
    this(uri, DEFAULT_ENCRYPTION_ALGORITHM);
  }

  /**
   * Constructs a specific AzureKmsClient that is bound to a single key identified by {@code uri}
   * and specified {@code EncryptionAlgorithm}.
   *
   * @param uri uri
   * @param algorithm algorithm
   */
  public AzureKmsClient(String uri, EncryptionAlgorithm algorithm) {
    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("key URI must not be blank");
    }
    if (algorithm == null) {
      throw new IllegalArgumentException("algorithm must not be null");
    }
    if (!uri.toLowerCase(Locale.US).startsWith(PREFIX)) {
      throw new IllegalArgumentException("key URI must starts with " + PREFIX);
    }
    this.keyUri = uri;
    this.algorithm = algorithm;
  }

  /**
   * @return @return true either if this client is a generic one and uri starts with
   *     {@link AzureKmsClient#PREFIX}, or the client is a specific one that is bound to the key
   *     identified by {@code uri}.
   */
  @Override
  public boolean doesSupport(String uri) {
    if (this.keyUri != null && this.keyUri.equals(uri)) {
      return true;
    }
    return this.keyUri == null && uri.toLowerCase(Locale.US).startsWith(PREFIX);
  }

  /**
   * Loads Azure credentials from a properties file. Not supported yet.
   */

  @Override
  public KmsClient withCredentials(String credentialPath) throws GeneralSecurityException {
    throw new UnsupportedOperationException("Not supported yet");
  }

  /**
   * Loads credentials using {@code DefaultAzureCredentialBuilder} Creates default
   * DefaultAzureCredential instance. Uses AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, and AZURE_TENANT_ID
   * environment variables to create a ClientSecretCredential. If these environment variables are
   * not available, then this will use the Shared MSAL token cache.
   *
   * @return KmsClient object
   * @throws GeneralSecurityException security exception
   */
  @Override
  public KmsClient withDefaultCredentials() throws GeneralSecurityException {
    return withCredentialsProvider(new DefaultAzureCredentialBuilder().build());
  }

  /**
   * loads credentials using provided {@code TokenCredential}
   *
   * @return KmsClient object
   * @throws GeneralSecurityException security exception
   */
  public KmsClient withCredentialsProvider(TokenCredential provider)
      throws GeneralSecurityException {
    this.provider = provider;
    return this;
  }

  /**
   * Specifies the {@link CryptographyClient} object to be used. Only used for testing.
   */
  public KmsClient withCryptographyClient(CryptographyClient cryptographyClient) {
    this.cryptographyClient = cryptographyClient;
    return this;
  }

  /**
   * Associates this client with the configs it was created with, so that {@link #getAead} can
   * consult {@link AzureKmsDriver#ENCRYPT_AZURE_KEY_VERSION_SAVE} and resolve a versionless key id
   * before building a {@link CryptographyClient}.
   */
  public KmsClient withConfigs(Map<String, ?> configs) {
    this.configs = configs;
    return this;
  }

  /**
   * Returns {@code AzureKmsAead} for the url provided.
   *
   * @param uri - azure keyvault key uri
   * @return Aead
   * @throws GeneralSecurityException security exception
   */
  @Override
  public Aead getAead(String uri) throws GeneralSecurityException {
    if (this.keyUri != null && !this.keyUri.equals(uri)) {
      throw new GeneralSecurityException(
          String.format(
              "this client is bound to %s, cannot load keys bound to %s", this.keyUri, uri));
    }
    String keyUri = Validators.validateKmsKeyUriAndRemovePrefix(PREFIX, uri);
    Object saveVersionValue = configs == null
        ? null
        : configs.get(AzureKmsDriver.ENCRYPT_AZURE_KEY_VERSION_SAVE);
    boolean saveVersion = saveVersionValue != null
        && Boolean.parseBoolean(saveVersionValue.toString());
    if (!saveVersion) {
      try {
        if (AzureKmsDriver.isVersionless(keyUri)) {
          log.warn("Azure Key Vault key '{}' is versionless and {} is not enabled; DEKs wrapped "
                  + "with it may become undecryptable after the key is rotated.",
              keyUri, AzureKmsDriver.ENCRYPT_AZURE_KEY_VERSION_SAVE);
        }
      } catch (GeneralSecurityException e) {
        // Malformed key id; surfaced properly when it is actually used below.
      }
    }
    // retry policy defined as per guidelines from MS
    // https://docs.microsoft.com/en-us/azure/key-vault/general/overview-throttling#recommended-client-side-throttling-method
    HttpPipeline pipeline = new HttpPipelineBuilder()
        .policies(new KeyVaultCredentialPolicy(provider == null
                ? new DefaultAzureCredentialBuilder().build() : provider, false),
            new RetryPolicy(
                new ExponentialBackoff(5, Duration.ofSeconds(1), Duration.ofSeconds(16))))
        .build();
    // Built from the raw (possibly versionless) keyUri, exactly as before this feature existed:
    // used directly whenever saveVersion is off, and as decrypt()'s fallback for legacy
    // ciphertext with no embedded version. Cheap to build eagerly: buildClient() does not itself
    // make a network call (Azure's SDK resolves lazily on first actual encrypt/decrypt/wrapKey).
    CryptographyClient testOverride = this.cryptographyClient;
    CryptographyClient defaultClient = testOverride != null
        ? testOverride
        : new CryptographyClientBuilder().pipeline(pipeline).keyIdentifier(keyUri).buildClient();
    // Always built, regardless of the current toggle value: a DEK wrapped while the toggle was on
    // may still need to be decrypted after it has been turned back off, so decrypt() must always
    // be able to resolve whatever version is embedded in an already-prefixed ciphertext. Building
    // this closure is cheap (no network call) regardless of whether decrypt() ever invokes it.
    Function<String, CryptographyClient> clientFactory = version -> {
      if (testOverride != null) {
        return testOverride;
      }
      try {
        String versionedKeyUri = AzureKmsDriver.withVersion(keyUri, version);
        return new CryptographyClientBuilder()
            .pipeline(pipeline)
            .keyIdentifier(versionedKeyUri)
            .buildClient();
      } catch (GeneralSecurityException e) {
        throw new RuntimeAzureKmsException(e);
      }
    };
    if (!saveVersion) {
      return new AzureKmsAead(defaultClient, this.algorithm, null, clientFactory);
    }
    Map<String, ?> clientConfigs = configs;
    // Deferred until encrypt() actually runs (not built here), so that constructing this Aead for
    // a decrypt-only call site never triggers a wasted version-resolution round trip: getAead() is
    // called for both encrypt and decrypt, and decrypt's own resolution (if needed at all) comes
    // from whatever version is embedded in the ciphertext, not from re-resolving "current" here.
    AzureKmsAead.EncryptTarget encryptTarget = () -> {
      String resolvedKeyUri = AzureKmsDriver.getVersionedKeyId(clientConfigs, keyUri);
      CryptographyClient client = testOverride != null
          ? testOverride
          : new CryptographyClientBuilder()
              .pipeline(pipeline)
              .keyIdentifier(resolvedKeyUri)
              .buildClient();
      String version = resolvedKeyUri.substring(resolvedKeyUri.lastIndexOf('/') + 1);
      return new AzureKmsAead.EncryptTarget.Resolved(client, version);
    };
    return new AzureKmsAead(defaultClient, this.algorithm, encryptTarget, clientFactory);
  }

  /**
   * Wraps a {@link GeneralSecurityException} so it can cross a {@link Function} boundary (which
   * cannot declare checked exceptions); unwrapped by {@link AzureKmsAead#decrypt}.
   */
  static final class RuntimeAzureKmsException extends RuntimeException {
    RuntimeAzureKmsException(GeneralSecurityException cause) {
      super(cause);
    }

    @Override
    public synchronized GeneralSecurityException getCause() {
      return (GeneralSecurityException) super.getCause();
    }
  }
}
