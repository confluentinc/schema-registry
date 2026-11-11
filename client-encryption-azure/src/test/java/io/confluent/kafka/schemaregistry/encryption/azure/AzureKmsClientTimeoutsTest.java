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
import static org.junit.Assert.assertTrue;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.RetryPolicy;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class AzureKmsClientTimeoutsTest {

  private static KmsTimeouts timeouts(String connectMs, String requestMs, String operationMs) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KMS_CONNECT_TIMEOUT_MS, connectMs);
    configs.put(KMS_REQUEST_TIMEOUT_MS, requestMs);
    configs.put(KMS_OPERATION_TIMEOUT_MS, operationMs);
    return KmsTimeouts.fromConfigs(configs);
  }

  @Test
  public void noTimeoutsKeepsAzureCoreDefaultClient() {
    assertNull(AzureKmsClient.buildHttpClient(KmsTimeouts.NONE));
  }

  @Test
  public void timeoutsBuildAConfiguredClient() {
    assertNotNull(AzureKmsClient.buildHttpClient(KmsTimeouts.fromConfigs(
        Collections.singletonMap(KMS_REQUEST_TIMEOUT_MS, "4000"))));
  }

  @Test
  public void noOperationTimeoutKeepsAzureCoreRetryPolicy() {
    assertEquals(5, AzureKmsClient.DEFAULT_MAX_RETRIES);
    assertTrue(AzureKmsClient.retryPolicy(KmsTimeouts.NONE) instanceof RetryPolicy);
    assertTrue(AzureKmsClient.retryPolicy(KmsTimeouts.fromConfigs(
        Collections.singletonMap(KMS_REQUEST_TIMEOUT_MS, "4000"))) instanceof RetryPolicy);
  }

  @Test
  public void operationTimeoutPutsADeadlineOnRetries() {
    assertTrue(AzureKmsClient.retryPolicy(timeouts("5000", "10000", "30000"))
        instanceof DeadlineRetryPolicy);
  }

  @Test
  public void deadlinePolicyCostsAnAttemptAtConnectPlusRequest() {
    // Attempt = 5s + 10s, so after 15s elapsed another does not fit in 30s.
    DeadlineRetryPolicy policy =
        (DeadlineRetryPolicy) AzureKmsClient.retryPolicy(timeouts("5000", "10000", "30000"));
    HttpRequest request = new HttpRequest(HttpMethod.POST, "https://vault.example/keys/k/wrap");
    HttpResponse throttled = new ThrottledResponse(request);
    long start = System.nanoTime();
    assertNotNull(policy.retryDelay(throttled, null, 0, start));
    assertNull(policy.retryDelay(throttled, null, 0, start - Duration.ofSeconds(15).toNanos()));
    // Attempt = 2s + 10s: fits after 15s elapsed, not after 18s.
    DeadlineRetryPolicy shortConnect =
        (DeadlineRetryPolicy) AzureKmsClient.retryPolicy(timeouts("2000", "10000", "30000"));
    assertNotNull(shortConnect.retryDelay(throttled, null, 0,
        start - Duration.ofSeconds(15).toNanos()));
    assertNull(shortConnect.retryDelay(throttled, null, 0,
        start - Duration.ofSeconds(18).toNanos()));
  }

  private static final class ThrottledResponse extends HttpResponse {
    ThrottledResponse(HttpRequest request) {
      super(request);
    }

    @Override
    public int getStatusCode() {
      return 429;
    }

    @Override
    public String getHeaderValue(String name) {
      return null;
    }

    @Override
    public HttpHeaders getHeaders() {
      return new HttpHeaders();
    }

    @Override
    public Flux<ByteBuffer> getBody() {
      return Flux.empty();
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
      return Mono.just(new byte[0]);
    }

    @Override
    public Mono<String> getBodyAsString() {
      return Mono.just("");
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
      return Mono.just("");
    }
  }
}
