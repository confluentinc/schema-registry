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
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.TOKEN_ID;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.TEST_CLIENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.jopenlibs.vault.api.Logical;
import io.github.jopenlibs.vault.response.LogicalResponse;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HcVaultFieldEncryptionProperties extends FieldEncryptionProperties {

  public HcVaultFieldEncryptionProperties(List<String> ruleNames) {
    super(ruleNames);
  }

  public HcVaultFieldEncryptionProperties(List<String> ruleNames, Class<?> ruleExecutor) {
    super(ruleNames, ruleExecutor);
  }

  @Override
  public String getKmsType() {
    return "hcvault";
  }

  @Override
  public String getKmsKeyId() {
    return "http://127.0.0.1:8200/transit/keys/my-key";
  }

  @Override
  public Map<String, Object> getClientProperties(String baseUrls) throws Exception {
    List<String> ruleNames = getRuleNames();
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, baseUrls);
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_TTL, "60");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, String.join(",", ruleNames));
    for (String ruleName : ruleNames) {
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName + ".class",
          getRuleExecutor().getName());
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + TOKEN_ID,
          "dev-only-token");
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + TEST_CLIENT,
          getTestClient());
    }
    return props;
  }

  @Override
  public Object getTestClient() throws Exception {
    return mockClient(getKmsKeyId());
  }

  static Logical mockClient(String keyId) throws Exception {
    Aead aead = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead.class);
    Map<String, String> response = new HashMap<>();
    LogicalResponse logicalResponse = mock(LogicalResponse.class);
    when(logicalResponse.getData()).thenReturn(response);
    Logical logical = mock(Logical.class);
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
    return logical;
  }
}

