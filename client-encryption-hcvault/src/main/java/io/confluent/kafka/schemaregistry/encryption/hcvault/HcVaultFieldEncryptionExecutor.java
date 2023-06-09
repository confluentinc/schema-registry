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

import com.bettercloud.vault.Vault;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public class HcVaultFieldEncryptionExecutor extends FieldEncryptionExecutor {

  public static final String TOKEN_ID = "token.id";

  private String tokenId;

  public HcVaultFieldEncryptionExecutor() {
  }

  @Override
  public String getKeyUrlPrefix(RuleContext ctx) {
    return HcVaultKmsClient.PREFIX;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    this.tokenId = (String) configs.get(TOKEN_ID);
  }

  @Override
  public KmsClient registerKmsClient(Optional<String> kekId) throws GeneralSecurityException {
    return registerWithHcVaultKms(kekId, Optional.ofNullable(tokenId), (Vault) getTestClient());
  }

  public static KmsClient registerWithHcVaultKms(
      Optional<String> keyUri, Optional<String> credentials, Vault vault)
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
    KmsClients.add(client);
    return client;
  }
}

