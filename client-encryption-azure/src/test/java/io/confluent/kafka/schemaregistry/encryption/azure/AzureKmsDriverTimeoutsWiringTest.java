/*
 * Copyright 2026 Confluent Inc.
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

import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_CONNECT_TIMEOUT_MS;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_OPERATION_TIMEOUT_MS;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_REQUEST_TIMEOUT_MS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.azure.core.http.policy.RetryPolicy;
import com.google.crypto.tink.KmsClient;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class AzureKmsDriverTimeoutsWiringTest {

  private static final String KEK_URL =
      "azure-kms://https://vault.example.vault.azure.net/keys/key1/0123456789abcdef";

  private static Map<String, Object> configs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AzureKmsDriver.TENANT_ID, "tenant");
    configs.put(AzureKmsDriver.CLIENT_ID, "client");
    configs.put(AzureKmsDriver.CLIENT_SECRET, "secret");
    return configs;
  }

  private static Object fieldOf(KmsClient client, String name) throws Exception {
    Field field = AzureKmsClient.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(client);
  }

  @Test
  public void configuredTimeoutsReachTheClient() throws Exception {
    Map<String, Object> configs = configs();
    configs.put(KMS_CONNECT_TIMEOUT_MS, "5000");
    configs.put(KMS_REQUEST_TIMEOUT_MS, "10000");
    configs.put(KMS_OPERATION_TIMEOUT_MS, "30000");

    KmsClient client = new AzureKmsDriver().newKmsClient(configs, Optional.of(KEK_URL));
    assertNotNull(fieldOf(client, "httpClient"));
    assertTrue(fieldOf(client, "retryPolicy") instanceof DeadlineRetryPolicy);
  }

  @Test
  public void connectAndRequestTimeoutsAloneKeepAzureCoreRetries() throws Exception {
    Map<String, Object> configs = configs();
    configs.put(KMS_CONNECT_TIMEOUT_MS, "5000");
    configs.put(KMS_REQUEST_TIMEOUT_MS, "10000");

    KmsClient client = new AzureKmsDriver().newKmsClient(configs, Optional.of(KEK_URL));
    assertNotNull(fieldOf(client, "httpClient"));
    assertTrue(fieldOf(client, "retryPolicy") instanceof RetryPolicy);
  }

  @Test
  public void noKeysLeavesAzureCoreDefaults() throws Exception {
    KmsClient client = new AzureKmsDriver().newKmsClient(configs(), Optional.of(KEK_URL));
    assertNull(fieldOf(client, "httpClient"));
    assertTrue(fieldOf(client, "retryPolicy") instanceof RetryPolicy);
  }

  @Test
  public void invalidTimeoutsAreRejectedByTheDriver() {
    Map<String, Object> configs = configs();
    configs.put(KMS_OPERATION_TIMEOUT_MS, "30000");
    assertThrows(IllegalArgumentException.class,
        () -> new AzureKmsDriver().newKmsClient(configs, Optional.of(KEK_URL)));
  }
}
