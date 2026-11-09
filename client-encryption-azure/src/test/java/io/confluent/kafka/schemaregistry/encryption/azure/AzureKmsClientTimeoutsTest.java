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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class AzureKmsClientTimeoutsTest {

  private static KmsTimeouts timeouts(String requestMs, String operationMs) {
    return timeouts(null, requestMs, operationMs);
  }

  private static KmsTimeouts timeouts(String connectMs, String requestMs, String operationMs) {
    Map<String, Object> configs = new HashMap<>();
    if (connectMs != null) {
      configs.put(KMS_CONNECT_TIMEOUT_MS, connectMs);
    }
    configs.put(KMS_REQUEST_TIMEOUT_MS, requestMs);
    configs.put(KMS_OPERATION_TIMEOUT_MS, operationMs);
    return KmsTimeouts.fromConfigs(configs);
  }

  @Test
  public void noTimeoutsKeepsAzureCoreDefaultClient() {
    // Null tells HttpPipelineBuilder to use HttpClient.createDefault(), as before.
    assertNull(AzureKmsClient.buildHttpClient(KmsTimeouts.NONE));
  }

  @Test
  public void timeoutsBuildAConfiguredClient() {
    assertNotNull(AzureKmsClient.buildHttpClient(KmsTimeouts.fromConfigs(
        Collections.singletonMap(KMS_REQUEST_TIMEOUT_MS, "4000"))));
  }

  @Test
  public void noOperationTimeoutKeepsDefaultRetries() {
    assertEquals(5, AzureKmsClient.DEFAULT_MAX_RETRIES);
    assertEquals(5, AzureKmsClient.maxRetries(KmsTimeouts.NONE));
    assertEquals(5, AzureKmsClient.maxRetries(KmsTimeouts.fromConfigs(
        Collections.singletonMap(KMS_REQUEST_TIMEOUT_MS, "4000"))));
  }

  @Test
  public void operationTimeoutLimitsRetriesToWhatFits() {
    // 10s attempts in a 30s budget: 10 + (1.2 + 10) = 21.2 fits, + (2.4 + 10) = 33.6 does not.
    assertEquals(1, AzureKmsClient.maxRetries(timeouts("10000", "30000")));
    // Budget equal to one attempt: no retries.
    assertEquals(0, AzureKmsClient.maxRetries(timeouts("10000", "10000")));
    // 1s attempts in a 60s budget: 1 + 2.2 + 3.4 + 5.8 + 10.6 + 20.2 = 43.2 fits all five retries;
    // never more than the default count.
    assertEquals(5, AzureKmsClient.maxRetries(timeouts("1000", "60000")));
    // 1s attempts in a 5s budget: 1 + 2.2 = 3.2 fits, + 3.4 = 6.6 does not.
    assertEquals(1, AzureKmsClient.maxRetries(timeouts("1000", "5000")));
  }

  @Test
  public void connectTimeoutIsPartOfEveryAttempt() {
    // Each retry reconnects, so an attempt costs connect + request. 5s + 10s in a 30s budget:
    // 15 + (1.2 + 15) = 31.2 does not fit, so no retries.
    assertEquals(0, AzureKmsClient.maxRetries(timeouts("5000", "10000", "30000")));
    // Shrinking connect to 3s makes one retry fit: 13 + 1.2 + 13 = 27.2.
    assertEquals(1, AzureKmsClient.maxRetries(timeouts("3000", "10000", "30000")));
    // Same budget without a connect timeout: one retry, as in the test above.
    assertEquals(1, AzureKmsClient.maxRetries(timeouts("10000", "30000")));
  }
}
