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
import com.google.crypto.tink.KmsClients;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public class AzureFieldEncryptionExecutor extends FieldEncryptionExecutor {

  public static final String TENANT_ID = "tenant.id";
  public static final String CLIENT_ID = "client.id";
  public static final String CLIENT_SECRET = "client.secret";

  public AzureFieldEncryptionExecutor() {
  }

  public void configure(Map<String, ?> configs) {
    try {
      super.configure(configs);
      String keyId = (String) configs.get(DEFAULT_KMS_KEY_ID);
      // Key id is not mandatory for decryption
      String keyUri = keyId != null ? AzureKmsClient.PREFIX + keyId : null;
      String tenantId = (String) configs.get(TENANT_ID);
      String clientId = (String) configs.get(CLIENT_ID);
      String clientSecret = (String) configs.get(CLIENT_SECRET);
      TokenCredential credentials;
      if (tenantId != null && clientId != null && clientSecret != null) {
        credentials = new ClientSecretCredentialBuilder()
            .tenantId(tenantId)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .build();
      } else {
        credentials = new DefaultAzureCredentialBuilder().build();
      }
      registerWithAzureKms(Optional.empty(), Optional.of(credentials),
          (CryptographyClient) getTestClient());

      setDefaultKekId(keyUri);
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static void registerWithAzureKms(
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
    KmsClients.add(client);
  }
}

