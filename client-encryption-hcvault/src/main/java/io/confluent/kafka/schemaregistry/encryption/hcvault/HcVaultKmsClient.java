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

import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_NAMESPACE;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_SSL_KEYSTORE_LOCATION;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_SSL_KEYSTORE_PASSWORD;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_SSL_TRUSTSTORE_LOCATION;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.getSslConfig;

import io.github.jopenlibs.vault.EnvironmentLoader;
import io.github.jopenlibs.vault.SslConfig;
import io.github.jopenlibs.vault.Vault;
import io.github.jopenlibs.vault.VaultConfig;
import io.github.jopenlibs.vault.VaultException;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.subtle.Validators;

import io.github.jopenlibs.vault.api.Logical;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.Optional;

/**
 * An implementation of {@link KmsClient} for <a
 * href="https://www.vaultproject.io/docs/secrets/transit">Vault Transit Secrets Engine</a>..
 */
public class HcVaultKmsClient implements KmsClient {

  public static final String PREFIX = "hcvault://";

  private String keyUri;
  private Logical vault;

  public HcVaultKmsClient() {
  }

  /**
   * Constructs a specific HcVaultKmsClient that is bound to a single key identified by
   * {@code uri}.
   */
  public HcVaultKmsClient(String uri) {
    if (!uri.toLowerCase(Locale.US).startsWith(PREFIX)) {
      throw new IllegalArgumentException("key URI must starts with " + PREFIX);
    }
    this.keyUri = uri;
  }

  /**
   * @return @return true either if this client is a generic one and uri starts with
   *     {@link HcVaultKmsClient#PREFIX}, or the client is a specific one that is bound to the key
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
   * Loads Vault config with the provided {@code token}.
   *
   * <p>If {@code token} is null, loads token from "VAULT_TOKEN" environment variables.</p>
   *
   * <p>All other configuration elements will also be read from environment variables.
   */
  @Override
  public KmsClient withCredentials(String token) throws GeneralSecurityException {
    return withCredentials(token, Optional.empty());
  }

  public KmsClient withCredentials(String token, Optional<String> namespace)
      throws GeneralSecurityException {
    return withCredentials(null, token, namespace);
  }

  public KmsClient withCredentials(SslConfig sslConfig, String token, Optional<String> namespace)
      throws GeneralSecurityException {
    try {
      URI uri = new URI(toHcVaultUri(this.keyUri));
      String address = "";
      if (uri.getScheme() != null) {
        address += uri.getScheme() + "://";
      }
      address += uri.getHost();
      if (uri.getPort() != -1) {
        address += ":" + uri.getPort();
      }
      VaultConfig config = new VaultConfig()
          .address(address)
          .token(token)
          .engineVersion(1);

      if (namespace.isPresent()) {
        config = config.nameSpace(namespace.get());
      }
      if (sslConfig != null) {
        config = config.sslConfig(sslConfig);
      }

      config = config.build();

      this.vault = new Vault(config).logical();
      return this;
    } catch (URISyntaxException | VaultException e) {
      throw new GeneralSecurityException("invalid path provided", e);
    }
  }

  /**
   * Loads default Vault config.
   *
   * <p>Token and timeouts can be loaded from environment variables.</p>
   *
   * <ul>
   *     <li>Vault Token read from "VAULT_TOKEN" environment variable</li>
   *     <li>Open Timeout read from "VAULT_OPEN_TIMEOUT" environment variable</li>
   *     <li>Read Timeout read from "VAULT_READ_TIMEOUT" environment variable</li>
   * </ul>
   */
  @Override
  public KmsClient withDefaultCredentials() throws GeneralSecurityException {
    try {
      URI uri = new URI(toHcVaultUri(this.keyUri));
      String address = "";
      if (uri.getScheme() != null) {
        address += uri.getScheme() + "://";
      }
      address += uri.getHost();
      if (uri.getPort() != -1) {
        address += ":" + uri.getPort();
      }
      VaultConfig config = new VaultConfig()
          .address(address)
          .engineVersion(1);

      EnvironmentLoader envLoader = new EnvironmentLoader();
      String namespace = envLoader.loadVariable(VAULT_NAMESPACE);
      if (namespace != null && !namespace.isEmpty()) {
        config = config.nameSpace(namespace);
      }

      String keystore = envLoader.loadVariable(VAULT_SSL_KEYSTORE_LOCATION);
      String keystorePassword = envLoader.loadVariable(VAULT_SSL_KEYSTORE_PASSWORD);
      String truststore = envLoader.loadVariable(VAULT_SSL_TRUSTSTORE_LOCATION);
      SslConfig sslConfig = getSslConfig(keystore, keystorePassword, truststore);
      if (sslConfig != null) {
        config = config.sslConfig(sslConfig);
      }

      config = config.build();

      this.vault = new Vault(config).logical();
    } catch (URISyntaxException | VaultException e) {
      throw new GeneralSecurityException("unable to create config", e);
    }
    return this;
  }

  /**
   * Loads Vault credentials from a config.
   */
  public KmsClient withConfig(VaultConfig config)
      throws GeneralSecurityException {
    this.vault = new Vault(config).logical();
    return this;
  }

  /**
   * Specifies the {@link Logical} object to be used. Only used for testing.
   */
  public KmsClient withVault(Logical vault) {
    this.vault = vault;
    return this;
  }

  @Override
  public Aead getAead(String uri) throws GeneralSecurityException {
    if (this.keyUri != null && !this.keyUri.equals(uri)) {
      throw new GeneralSecurityException(
          String.format(
              "this client is bound to %s, cannot load keys bound to %s", this.keyUri, uri));
    }

    return new HcVaultKmsAead(this.vault, toHcVaultUri(uri));
  }

  private static String toHcVaultUri(String uri) {
    String uriStr = Validators.validateKmsKeyUriAndRemovePrefix(PREFIX, uri);
    if (!uriStr.startsWith("http")) {
      uriStr = "https://" + uriStr;
    }
    return uriStr;
  }
}
