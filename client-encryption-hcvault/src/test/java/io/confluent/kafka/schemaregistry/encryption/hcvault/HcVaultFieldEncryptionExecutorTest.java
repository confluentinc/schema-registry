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

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.EMPTY_AAD;
import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.TEST_CLIENT;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultFieldEncryptionExecutor.DEFAULT_KMS_KEY_ID;
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
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class HcVaultFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  public HcVaultFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  protected Map<String, Object> getClientProperties() throws Exception {
    String keyId = "http://127.0.0.1:8200/transit/keys/my-key";
    Vault testClient = mockClient(keyId);
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, "hcvault");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".hcvault.class",
        HcVaultFieldEncryptionExecutor.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".hcvault.param." + DEFAULT_KMS_KEY_ID,
        keyId);
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".hcvault.param." + TOKEN_ID,
        "dev-only-token");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".hcvault.param." + TEST_CLIENT,
        testClient);
    return props;
  }

  private static Vault mockClient(String keyId) throws Exception {
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

