/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.kafka.schemaregistry.encryption.hcvault;

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.EMPTY_AAD;
import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.TEST_CLIENT;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultFieldEncryptionExecutor.TOKEN_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.api.Logical;
import com.bettercloud.vault.response.LogicalResponse;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HcVaultFieldEncryptionProperties implements FieldEncryptionProperties {

  @Override
  public String getKeyId() {
    return "http://127.0.0.1:8200/transit/keys/my-key";
  }

  @Override
  public Map<String, Object> getClientPropertiesWithoutKey(List<String> ruleNames) throws Exception {
    Vault testClient = mockClient(getKeyId());
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_TTL, "60");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, String.join(",", ruleNames));
    for (String ruleName : ruleNames) {
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName + ".class",
          HcVaultFieldEncryptionExecutor.class.getName());
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + TOKEN_ID,
          "dev-only-token");
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + TEST_CLIENT,
          testClient);
    }
    return props;
  }

  static Vault mockClient(String keyId) throws Exception {
    Aead aead = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead.class);
    Map<String, String> response = new HashMap<>();
    LogicalResponse logicalResponse = mock(LogicalResponse.class);
    when(logicalResponse.getData()).thenReturn(response);
    Logical logical = mock(Logical.class);
    Vault client = mock(Vault.class);
    when(client.logical()).thenReturn(logical);
    when(logical.write(any(String.class), any(Map.class)))
        .thenAnswer(invocationOnMock -> {
          String path = invocationOnMock.getArgument(0);
          Map<String, Object> request = invocationOnMock.getArgument(1);
          if (request.containsKey("plaintext")) {
            byte[] plaintext = Base64.getDecoder().decode(((String) request.get("plaintext")));
            byte[] ciphertext = aead.encrypt(plaintext, EMPTY_AAD);
            response.put("ciphertext", Base64.getEncoder().encodeToString(ciphertext));
          } else {
            byte[] ciphertext = Base64.getDecoder().decode(((String) request.get("ciphertext")));
            byte[] plaintext = aead.decrypt(ciphertext, EMPTY_AAD);
            response.put("plaintext", Base64.getEncoder().encodeToString(plaintext));
          }
          return logicalResponse;
        });
    return client;
  }
}

