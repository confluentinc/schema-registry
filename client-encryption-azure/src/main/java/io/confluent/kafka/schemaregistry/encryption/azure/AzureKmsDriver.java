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
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public class AzureKmsDriver implements KmsDriver {

  public static final String TENANT_ID = "tenant.id";
  public static final String CLIENT_ID = "client.id";
  public static final String CLIENT_SECRET = "client.secret";

  public AzureKmsDriver() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return AzureKmsClient.PREFIX;
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

