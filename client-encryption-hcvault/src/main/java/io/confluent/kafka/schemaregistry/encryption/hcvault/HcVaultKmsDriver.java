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

import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import io.github.jopenlibs.vault.EnvironmentLoader;
import io.github.jopenlibs.vault.SslConfig;
import io.github.jopenlibs.vault.VaultConfig;
import io.github.jopenlibs.vault.VaultException;
import io.github.jopenlibs.vault.api.Auth;
import io.github.jopenlibs.vault.api.Logical;
import java.io.File;
import java.security.GeneralSecurityException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Optional;

public class HcVaultKmsDriver implements KmsDriver {

  public static final String TOKEN_ID = "token.id";
  public static final String NAMESPACE = "namespace";
  public static final String APPROLE_ROLE_ID = "approle.role.id";
  public static final String APPROLE_SECRET_ID = "approle.secret.id";
  public static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
  public static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
  public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";

  public static final String VAULT_NAMESPACE = "VAULT_NAMESPACE";
  public static final String VAULT_SSL_KEYSTORE_LOCATION = "VAULT_SSL_KEYSTORE_LOCATION";
  public static final String VAULT_SSL_KEYSTORE_PASSWORD = "VAULT_SSL_KEYSTORE_PASSWORD";
  public static final String VAULT_SSL_TRUSTSTORE_LOCATION = "VAULT_SSL_TRUSTSTORE_LOCATION";
  public static final String VAULT_APPROLE_ROLE_ID = "VAULT_APPROLE_ROLE_ID";
  public static final String VAULT_APPROLE_SECRET_ID = "VAULT_APPROLE_SECRET_ID";

  public HcVaultKmsDriver() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return HcVaultKmsClient.PREFIX;
  }

  private SslConfig getSslConfig(Map<String, ?> configs) throws GeneralSecurityException {
    String keystore = (String) configs.get(SSL_KEYSTORE_LOCATION);
    String keystorePassword = (String) configs.get(SSL_KEYSTORE_PASSWORD);
    String truststore = (String) configs.get(SSL_TRUSTSTORE_LOCATION);
    return getSslConfig(keystore, keystorePassword, truststore);
  }

  protected static SslConfig getSslConfig(
      String keystore, String keystorePassword, String truststore) throws GeneralSecurityException {
    try {
      boolean hasKeyStore = keystore != null && !keystore.isEmpty();
      boolean hasTrustStore = truststore != null && !truststore.isEmpty();
      if (hasKeyStore || hasTrustStore) {
        SslConfig sslConfig = new SslConfig();
        if (hasKeyStore) {
          sslConfig = sslConfig.keyStoreFile(new File(keystore), keystorePassword);
        }
        if (hasTrustStore) {
          sslConfig = sslConfig.trustStoreFile(new File(truststore));
        }
        return sslConfig.build();
      }
      return null;
    } catch (VaultException e) {
      throw new GeneralSecurityException("unable to create ssl config", e);
    }
  }

  private String getToken(Map<String, ?> configs) {
    return (String) configs.get(TOKEN_ID);
  }

  private Map.Entry<String, String> getRoleId(Map<String, ?> configs) {
    String roleId = (String) configs.get(APPROLE_ROLE_ID);
    String secretId = (String) configs.get(APPROLE_SECRET_ID);
    if (roleId != null && secretId != null) {
      return new SimpleEntry<>(roleId, secretId);
    }
    EnvironmentLoader envLoader = new EnvironmentLoader();
    roleId = envLoader.loadVariable(VAULT_APPROLE_ROLE_ID);
    secretId = envLoader.loadVariable(VAULT_APPROLE_SECRET_ID);
    if (roleId != null && secretId != null) {
      return new SimpleEntry<>(roleId, secretId);
    }
    return null;
  }

  private String getNamespace(Map<String, ?> configs) {
    return (String) configs.get(NAMESPACE);
  }

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    try {
      Logical testClient = (Logical) getTestClient(configs);
      Optional<String> creds = testClient != null
          ? Optional.empty()
          : Optional.ofNullable(getToken(configs));
      SslConfig sslConfig = getSslConfig(configs);
      Optional<String> namespace = Optional.ofNullable(getNamespace(configs));
      KmsClient client = newKmsClientWithHcVaultKms(kekUrl, sslConfig, creds, namespace,
          testClient);
      Map.Entry<String, String> roleId = getRoleId(configs);
      if (testClient == null && roleId != null) {
        VaultConfig config = ((HcVaultKmsClient) client).getVaultConfig();
        Auth auth = new Auth(config);
        String token = auth.loginByAppRole(roleId.getKey(), roleId.getValue()).getAuthClientToken();
        client = newKmsClientWithHcVaultKms(kekUrl, sslConfig, Optional.of(token), namespace, null);
      }
      return client;
    } catch (VaultException e) {
      throw new GeneralSecurityException("unable to create vault client", e);
    }
  }

  protected static KmsClient newKmsClientWithHcVaultKms(
      Optional<String> keyUri, SslConfig sslConfig, Optional<String> credentials,
      Optional<String> namespace, Logical vault)
      throws GeneralSecurityException {
    HcVaultKmsClient client;
    if (keyUri.isPresent()) {
      client = new HcVaultKmsClient(keyUri.get());
    } else {
      client = new HcVaultKmsClient();
    }
    if (credentials.isPresent()) {
      client.withCredentials(sslConfig, credentials.get(), namespace);
    } else {
      client.withDefaultCredentials();
    }
    if (vault != null) {
      client.withVault(vault);
    }
    return client;
  }
}

