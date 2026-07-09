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

  /**
   * Enables making a DEK's encryptedKeyMaterial self-describing with respect to which exact
   * Azure Key Vault key version wrapped it (see {@link AzureKmsAead}), matching the same
   * self-description property AWS KMS and GCP KMS ciphertext already provide natively. Set as a
   * kek kmsProps entry.
   */
  public static final String ENCRYPT_AZURE_KEY_VERSION_SAVE = "encrypt.azure.key.version.save";

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
   * unlike AWS KMS and GCP KMS, Azure Key Vault's wrap/unwrap operations address an explicit key
   * version and do not embed that version in the returned ciphertext, so a caller
   * that only ever uses a versionless reference has no way to know which version encrypted a given
   * DEK once the key has been rotated.
   */
  public static String getVersionedKeyId(Map<String, ?> configs, String kmsKeyId)
      throws GeneralSecurityException {
    KeyVaultId parsed = parse(kmsKeyId);
    if (parsed.version != null) {
      // Already versioned; respect the explicitly pinned config as-is.
      return kmsKeyId;
    }
    // A Function<String, KeyVaultKey> (rather than a raw KeyClient) is used as the test seam
    // since KeyClient is a final Azure SDK class that plain Mockito cannot mock; KeyClient::getKey
    // satisfies this functional interface for the real (non-test) path.
    @SuppressWarnings("unchecked")
    Function<String, KeyVaultKey> testKeyResolver =
        (Function<String, KeyVaultKey>) configs.get(TEST_KEY_CLIENT);
    Function<String, KeyVaultKey> keyResolver = testKeyResolver != null
        ? testKeyResolver
        : new KeyClientBuilder()
            .vaultUrl(parsed.vaultUrl)
            .credential(getCredentials(configs))
            .buildClient()::getKey;
    KeyVaultKey key = keyResolver.apply(parsed.name);
    if (key == null || key.getId() == null) {
      throw new GeneralSecurityException(
          "Failed to resolve Azure Key Vault key id for key name '" + parsed.name + "' in vault "
              + parsed.vaultUrl);
    }
    String resolvedId = key.getId();
    if (parse(resolvedId).version == null) {
      throw new GeneralSecurityException(
          "Resolved Azure Key Vault key id is missing a version segment: " + resolvedId);
    }
    return resolvedId;
  }

  /**
   * Combines {@code kmsKeyId} (versionless or versioned; only the vault and key name are used)
   * with an explicit {@code version}, returning the full versioned key identifier. Used to
   * reconstruct a target for a version extracted from an already-wrapped DEK, which may differ
   * from whatever {@link #getVersionedKeyId} currently resolves to (e.g. after a rotation).
   */
  public static String withVersion(String kmsKeyId, String version)
      throws GeneralSecurityException {
    KeyVaultId parsed = parse(kmsKeyId);
    return parsed.vaultUrl + "/keys/" + parsed.name + "/" + version;
  }

  /**
   * Returns true if {@code kmsKeyId} has no explicit version segment. Used to warn when
   * ENCRYPT_AZURE_KEY_VERSION_SAVE is not enabled for a versionless key, without performing any
   * actual resolution (no {@code KeyClient} call).
   */
  public static boolean isVersionless(String kmsKeyId) throws GeneralSecurityException {
    return parse(kmsKeyId).version == null;
  }

  private static KeyVaultId parse(String kmsKeyId) throws GeneralSecurityException {
    URI uri;
    try {
      uri = new URI(kmsKeyId);
    } catch (URISyntaxException e) {
      throw new GeneralSecurityException("Invalid Azure Key Vault key id: " + kmsKeyId, e);
    }
    String path = uri.getPath();
    boolean hasSchemeAndAuthority = uri.getScheme() != null && uri.getAuthority() != null;
    if (path == null || !hasSchemeAndAuthority) {
      // path is null for opaque URIs (e.g. "keys:key1"), which have no authority either, but the
      // path check must come first since it feeds the split() below.
      throw new GeneralSecurityException("Invalid Azure Key Vault key id: " + kmsKeyId);
    }
    String[] segments = Arrays.stream(path.split("/"))
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);
    boolean hasValidSegments = segments.length >= 2 && segments.length <= 3
        && "keys".equals(segments[0]);
    if (!hasValidSegments) {
      throw new GeneralSecurityException("Invalid Azure Key Vault key id: " + kmsKeyId);
    }
    String vaultUrl = uri.getScheme() + "://" + uri.getAuthority();
    return new KeyVaultId(vaultUrl, segments[1], segments.length == 3 ? segments[2] : null);
  }

  private static final class KeyVaultId {
    private final String vaultUrl;
    private final String name;
    private final String version;

    private KeyVaultId(String vaultUrl, String name, String version) {
      this.vaultUrl = vaultUrl;
      this.name = name;
      this.version = version;
    }
  }

  private static TokenCredential getCredentials(Map<String, ?> configs) {
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
    return newKmsClientWithAzureKms(configs, kekUrl, creds, testClient);
  }

  protected static KmsClient newKmsClientWithAzureKms(
      Map<String, ?> configs, Optional<String> keyUri,
      Optional<TokenCredential> credentials, CryptographyClient cryptographyClient)
      throws GeneralSecurityException {
    AzureKmsClient client;
    if (keyUri.isPresent()) {
      client = new AzureKmsClient(keyUri.get());
    } else {
      client = new AzureKmsClient();
    }
    client.withConfigs(configs);
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
