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
import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.policy.ExponentialBackoff;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.http.policy.RetryPolicy;
import com.azure.core.util.HttpClientOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import com.azure.security.keyvault.keys.implementation.KeyVaultCredentialPolicy;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.subtle.Validators;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link KmsClient} for <a
 * href="https://azure.microsoft.com/en-us/services/key-vault/">Azure KMS</a>.
 */
public final class AzureKmsClient implements KmsClient {

  private static final Logger log = LoggerFactory.getLogger(AzureKmsClient.class);

  public static final String PREFIX = "azure-kms://";

  // Retry policy as per guidelines from MS:
  // https://docs.microsoft.com/en-us/azure/key-vault/general/overview-throttling#recommended-client-side-throttling-method
  static final int DEFAULT_MAX_RETRIES = 5;
  static final Duration RETRY_BASE_DELAY = Duration.ofSeconds(1);
  static final Duration RETRY_MAX_DELAY = Duration.ofSeconds(16);

  private CryptographyClient cryptographyClient;
  private String keyUri;
  private TokenCredential provider;
  private Map<String, ?> configs;
  // Null means azure-core's default HTTP client. Built once in withTimeouts rather than per
  // getAead, which runs on every DEK wrap/unwrap.
  private HttpClient httpClient;
  // Stateless, so shared by every pipeline this client builds.
  private HttpPipelinePolicy retryPolicy = retryPolicy(KmsTimeouts.NONE);
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
   * Bounds the connect and per-response time of the HTTP client behind the Key Vault pipeline,
   * and, when an operation timeout is given, puts a deadline on the retries so that the whole
   * operation fits in it (see {@link #retryPolicy}). With {@link KmsTimeouts#NONE} (the default)
   * the pipeline uses azure-core's default HTTP client, timeouts and retry policy unchanged.
   */
  public KmsClient withTimeouts(KmsTimeouts timeouts) {
    KmsTimeouts effective = timeouts == null ? KmsTimeouts.NONE : timeouts;
    this.httpClient = buildHttpClient(effective);
    this.retryPolicy = retryPolicy(effective);
    return this;
  }

  /**
   * The retry policy for the Key Vault pipeline: azure-core's own {@link RetryPolicy} with the
   * Microsoft-recommended backoff when no operation timeout is set, otherwise a
   * {@link DeadlineRetryPolicy} with the same backoff that grants each retry only while it still
   * fits in the operation timeout.
   *
   * <p>azure-core has no deadline that spans retries for the synchronous client (a
   * {@code TimeoutPolicy} runs the downstream policies on the calling thread, so its reactor
   * timeout cannot fire), so the deadline is enforced at each retry decision instead: a retry is
   * granted only while the time already spent, the backoff and one more worst-case attempt still
   * fit in the budget. A vault that fails fast (429, 5xx) gets every retry the cap allows; a vault
   * that hangs gets none.
   *
   * <p>An attempt is costed at connect + request: after a response timeout reactor-netty drops the
   * connection, so every retry pays the connect timeout again, and the request timeout is applied
   * as the response timeout (until headers arrive), which is what a slow or hung vault hits. The
   * same value also serves as the read and write timeout between individual socket operations;
   * Key Vault payloads are a few hundred bytes, so those only add time if the vault trickles
   * bytes, and that pathological case is not budgeted. A {@code Retry-After} header from a
   * throttled Key Vault is honoured when it fits in the budget and ends the operation when it does
   * not. With {@link AzureKmsDriver#ENCRYPT_AZURE_KEY_VERSION_SAVE} on and a versionless key,
   * encrypt first resolves the current version on the same pipeline, which is a second round trip
   * with its own budget, so that case can take up to twice the operation timeout. The deadline is
   * therefore a tight bound on the realistic worst case, not a hard wall clock.
   */
  static HttpPipelinePolicy retryPolicy(KmsTimeouts timeouts) {
    if (!timeouts.operationTimeout().isPresent()) {
      return new RetryPolicy(
          new ExponentialBackoff(DEFAULT_MAX_RETRIES, RETRY_BASE_DELAY, RETRY_MAX_DELAY));
    }
    // fromConfigs guarantees a request timeout accompanies an operation timeout.
    Duration attempt = timeouts.requestTimeout().get()
        .plus(timeouts.connectTimeout().orElse(Duration.ZERO));
    return new DeadlineRetryPolicy(DEFAULT_MAX_RETRIES, RETRY_BASE_DELAY, RETRY_MAX_DELAY,
        attempt, timeouts.operationTimeout().get());
  }

  /**
   * Returns the HTTP client for the Key Vault pipeline, or null to keep azure-core's default
   * (exactly what {@link HttpPipelineBuilder} does when no client is given).
   */
  static HttpClient buildHttpClient(KmsTimeouts timeouts) {
    if (timeouts.isEmpty()) {
      return null;
    }
    HttpClientOptions options = new HttpClientOptions();
    timeouts.connectTimeout().ifPresent(options::setConnectTimeout);
    timeouts.requestTimeout().ifPresent(timeout -> {
      // Response timeout: until the response headers arrive. Read timeout: between body reads.
      options.setResponseTimeout(timeout);
      options.setReadTimeout(timeout);
      options.setWriteTimeout(timeout);
    });
    return HttpClient.createDefault(options);
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
    // azure-core's RetryPolicy unless withTimeouts gave the operation a deadline.
    HttpPipeline pipeline = new HttpPipelineBuilder()
        .httpClient(httpClient)
        .policies(new KeyVaultCredentialPolicy(provider == null
                ? new DefaultAzureCredentialBuilder().build() : provider, false),
            retryPolicy)
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
    // Resolved on the same pipeline as the wrap itself, so the lookup gets the configured
    // timeouts and retry policy rather than azure-core's defaults. It is a round trip of its own,
    // so for a versionless key with version-saving on, one encrypt can take up to twice the
    // operation budget (resolve + wrap); decrypt never resolves and stays within one budget.
    Supplier<Map.Entry<CryptographyClient, String>> encryptTarget = () -> {
      try {
        String resolvedKeyUri = AzureKmsDriver.getVersionedKeyId(clientConfigs, keyUri, pipeline);
        String version = resolvedKeyUri.substring(resolvedKeyUri.lastIndexOf('/') + 1);
        // Reuses clientFactory rather than building its own CryptographyClient, so there is only
        // one place (clientFactory) that knows how to turn a version into a client.
        return new SimpleEntry<>(clientFactory.apply(version), version);
      } catch (GeneralSecurityException e) {
        throw new RuntimeAzureKmsException(e);
      }
    };
    return new AzureKmsAead(defaultClient, this.algorithm, encryptTarget, clientFactory);
  }

  /**
   * Wraps a {@link GeneralSecurityException} so it can cross a {@link Function} or
   * {@link java.util.function.Supplier} boundary (neither can declare checked exceptions);
   * unwrapped by {@link AzureKmsAead#decrypt} and {@link AzureKmsAead#encrypt}, respectively.
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
