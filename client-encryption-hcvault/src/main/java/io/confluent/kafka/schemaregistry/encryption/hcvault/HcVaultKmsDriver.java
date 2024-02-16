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
import io.github.jopenlibs.vault.api.Logical;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public class HcVaultKmsDriver implements KmsDriver {

  public static final String TOKEN_ID = "token.id";
  public static final String NAMESPACE = "namespace";

  public HcVaultKmsDriver() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return HcVaultKmsClient.PREFIX;
  }

  private String getToken(Map<String, ?> configs) {
    return (String) configs.get(TOKEN_ID);
  }

  private String getNamespace(Map<String, ?> configs) {
    return (String) configs.get(NAMESPACE);
  }

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    Logical testClient = (Logical) getTestClient(configs);
    Optional<String> creds = testClient != null
        ? Optional.empty()
        : Optional.ofNullable(getToken(configs));
    Optional<String> namespace = Optional.ofNullable(getNamespace(configs));
    return newKmsClientWithHcVaultKms(kekUrl, creds, namespace, testClient);
  }

  protected static KmsClient newKmsClientWithHcVaultKms(
      Optional<String> keyUri, Optional<String> credentials,
      Optional<String> namespace, Logical vault)
      throws GeneralSecurityException {
    HcVaultKmsClient client;
    if (keyUri.isPresent()) {
      client = new HcVaultKmsClient(keyUri.get());
    } else {
      client = new HcVaultKmsClient();
    }
    if (credentials.isPresent()) {
      client.withCredentials(credentials.get());
    } else {
      client.withDefaultCredentials();
    }
    if (vault != null) {
      client.withVault(vault);
    }
    return client;
  }
}

