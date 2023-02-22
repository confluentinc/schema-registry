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
package io.confluent.kafka.schemaregistry.encryption.azure;

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.EMPTY_AAD;
import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.TEST_CLIENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.DecryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureFieldEncryptionProperties implements FieldEncryptionProperties {

  @Override
  public String getKeyId() {
    return "https://yokota1.vault.azure.net/keys/key1/1234567890";
  }

  @Override
  public Map<String, Object> getClientPropertiesWithoutKey(List<String> ruleNames)
      throws Exception {
    CryptographyClient testClient = mockClient(getKeyId());
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_TTL, "60");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, String.join(",", ruleNames));
    for (String ruleName : ruleNames) {
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName + ".class",
          AzureFieldEncryptionExecutor.class.getName());
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + TEST_CLIENT,
          testClient);
    }
    return props;
  }

  static CryptographyClient mockClient(String keyId) throws Exception {
    Aead aead = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead.class);
    CryptographyClient client = mock(CryptographyClient.class);
    when(client.encrypt(any(EncryptionAlgorithm.class), any(byte[].class)))
        .thenAnswer(invocationOnMock -> {
          EncryptionAlgorithm algo = invocationOnMock.getArgument(0);
          byte[] plainText = invocationOnMock.getArgument(1);
          byte[] ciphertext = aead.encrypt(plainText, EMPTY_AAD);
          return new EncryptResult(ciphertext, algo, keyId);
        });
    when(client.decrypt(any(EncryptionAlgorithm.class), any(byte[].class)))
        .thenAnswer(invocationOnMock -> {
          EncryptionAlgorithm algo = invocationOnMock.getArgument(0);
          byte[] ciphertext = invocationOnMock.getArgument(1);
          byte[] plaintext = aead.decrypt(ciphertext, EMPTY_AAD);
          return new DecryptResult(plaintext, algo, keyId);
        });
    return client;
  }
}

