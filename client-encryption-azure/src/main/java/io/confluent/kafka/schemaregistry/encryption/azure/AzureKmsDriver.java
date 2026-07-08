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
import com.azure.core.exception.HttpResponseException;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class AzureKmsDriver implements KmsDriver {

  public static final String TENANT_ID = "tenant.id";
  public static final String CLIENT_ID = "client.id";
  public static final String CLIENT_SECRET = "client.secret";

  // Only used for testing.
  public static final String TEST_KEY_CLIENT = "test.key.client";

  public AzureKmsDriver() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return AzureKmsClient.PREFIX;
  }

  @Override
  public boolean isAccessDeniedException(Throwable t) {
    if (!(t instanceof HttpResponseException)) {
      return false;
    }
    HttpResponseException e = (HttpResponseException) t;
    return e.getResponse() != null && isAccessDeniedStatus(e.getResponse().getStatusCode());
  }

  /**
   * Resolves a possibly-versionless Azure Key Vault key identifier (e.g.
   * {@code https://vault.vault.azure.net/keys/name}) into the concrete, currently-enabled version
   * (e.g. {@code https://vault.vault.azure.net/keys/name/<version>}). If {@code kmsKeyId} already
   * includes a version segment, it is returned unchanged and no call is made. This exists because,
   * unlike AWS KMS, GCP KMS, and HashiCorp Vault, Azure Key Vault's wrap/unwrap operations address
   * an explicit key version and do not embed that version in the returned ciphertext, so a caller
   * that only ever uses a versionless reference has no way to know which version encrypted a given
   * DEK once the key has been rotated.
   */
  @Override
  public String getVersionedKeyId(Map<String, ?> configs, String kmsKeyId)
      throws GeneralSecurityException {
    URI uri;
    try {
      uri = new URI(kmsKeyId);
    } catch (URISyntaxException e) {
      throw new GeneralSecurityException("Invalid Azure Key Vault key id: " + kmsKeyId, e);
    }
    String[] segments = Arrays.stream(uri.getPath().split("/"))
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);
    if (segments.length < 2 || segments.length > 3 || !"keys".equals(segments[0])) {
      throw new GeneralSecurityException("Invalid Azure Key Vault key id: " + kmsKeyId);
    }
    if (segments.length == 3) {
      // Already versioned; respect the explicitly pinned config as-is.
      return kmsKeyId;
    }
    String name = segments[1];
    // A Function<String, KeyVaultKey> (rather than a raw KeyClient) is used as the test seam
    // because KeyClient is a final Azure SDK class that plain Mockito cannot mock; KeyClient::getKey
    // satisfies this functional interface for the real (non-test) path.
    @SuppressWarnings("unchecked")
    Function<String, KeyVaultKey> testKeyResolver =
        (Function<String, KeyVaultKey>) configs.get(TEST_KEY_CLIENT);
    Function<String, KeyVaultKey> keyResolver = testKeyResolver != null
        ? testKeyResolver
        : new KeyClientBuilder()
            .vaultUrl(uri.getScheme() + "://" + uri.getAuthority())
            .credential(getCredentials(configs))
            .buildClient()::getKey;
    KeyVaultKey key = keyResolver.apply(name);
    return key.getId();
  }

  private TokenCredential getCredentials(Map<String, ?> configs) {
    String tenantId = (String) configs.get(TENANT_ID);
    String clientId = (String) configs.get(CLIENT_ID);
    String clientSecret = (String) configs.get(CLIENT_SECRET);
    if (tenantId != null && clientId != null && clientSecret != null) {
      return new ClientSecretCredentialBuilder()
          .tenantId(tenantId)
          .clientId(clientId)
          .clientSecret(clientSecret)
          .build();
    } else {
      return new DefaultAzureCredentialBuilder().build();
    }
  }

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    CryptographyClient testClient = (CryptographyClient) getTestClient(configs);
    Optional<TokenCredential> creds = testClient != null
        ? Optional.empty()
        : Optional.of(getCredentials(configs));
    return newKmsClientWithAzureKms(kekUrl, creds, testClient);
  }

  protected static KmsClient newKmsClientWithAzureKms(
      Optional<String> keyUri, Optional<TokenCredential> credentials,
      CryptographyClient cryptographyClient)
      throws GeneralSecurityException {
    AzureKmsClient client;
    if (keyUri.isPresent()) {
      client = new AzureKmsClient(keyUri.get());
    } else {
      client = new AzureKmsClient();
    }
    if (credentials.isPresent()) {
      client.withCredentialsProvider(credentials.get());
    } else {
      client.withDefaultCredentials();
    }
    if (cryptographyClient != null) {
      client.withCryptographyClient(cryptographyClient);
    }
    return client;
  }
}

