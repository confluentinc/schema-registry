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

/**
 * An implementation of {@link KmsClient} for <a
 * href="https://azure.microsoft.com/en-us/services/key-vault/">Azure KMS</a>.
 */
public final class AzureKmsClient implements KmsClient {

  public static final String PREFIX = "azure-kms://";

  private CryptographyClient cryptographyClient;
  private String keyUri;
  private TokenCredential provider;
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
    // retry policy defined as per guidelines from MS
    // https://docs.microsoft.com/en-us/azure/key-vault/general/overview-throttling#recommended-client-side-throttling-method
    HttpPipeline pipeline = new HttpPipelineBuilder()
        .policies(new KeyVaultCredentialPolicy(provider == null
                ? new DefaultAzureCredentialBuilder().build() : provider, false),
            new RetryPolicy(
                new ExponentialBackoff(5, Duration.ofSeconds(1), Duration.ofSeconds(16))))
        .build();
    CryptographyClient client = this.cryptographyClient;
    if (client == null) {
      client = new CryptographyClientBuilder()
          .pipeline(pipeline)
          .keyIdentifier(keyUri)
          .buildClient();
    }
    return new AzureKmsAead(client, this.algorithm);
  }
}
