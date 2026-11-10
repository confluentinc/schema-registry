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
  public void noOperationTimeoutKeepsAzureCoreRetryPolicy() {
    assertEquals(5, AzureKmsClient.DEFAULT_MAX_RETRIES);
    // Connect and request timeouts alone leave the retry policy exactly as before: azure-core's
    // own, with the Microsoft-recommended five retries.
    assertTrue(AzureKmsClient.retryPolicy(KmsTimeouts.NONE) instanceof RetryPolicy);
    assertTrue(AzureKmsClient.retryPolicy(KmsTimeouts.fromConfigs(
        Collections.singletonMap(KMS_REQUEST_TIMEOUT_MS, "4000"))) instanceof RetryPolicy);
  }

  @Test
  public void operationTimeoutPutsADeadlineOnRetries() {
    assertTrue(AzureKmsClient.retryPolicy(timeouts("10000", "30000"))
        instanceof DeadlineRetryPolicy);
    assertTrue(AzureKmsClient.retryPolicy(timeouts("5000", "10000", "30000"))
        instanceof DeadlineRetryPolicy);
  }

  @Test
  public void deadlinePolicyCostsAnAttemptAtConnectPlusRequest() {
    // Each retry reconnects, so one worst-case attempt is connect + request = 15s. After a hung
    // first attempt nothing more fits in 30s; after a fast one, a retry does.
    DeadlineRetryPolicy policy =
        (DeadlineRetryPolicy) AzureKmsClient.retryPolicy(timeouts("5000", "10000", "30000"));
    HttpRequest request = new HttpRequest(HttpMethod.POST, "https://vault.example/keys/k/wrap");
    HttpResponse throttled = new ThrottledResponse(request);
    long start = System.nanoTime();
    assertNotNull(policy.retryDelay(throttled, null, 0, start));
    assertNull(policy.retryDelay(throttled, null, 0, start - Duration.ofSeconds(15).toNanos()));
    // Without a connect timeout an attempt is just the request: 10 + 10 fits in 30 with room for
    // the backoff, 15 elapsed + ~1s backoff + 10 still fits, 21 elapsed does not.
    DeadlineRetryPolicy requestOnly =
        (DeadlineRetryPolicy) AzureKmsClient.retryPolicy(timeouts("10000", "30000"));
    assertNotNull(requestOnly.retryDelay(throttled, null, 0,
        start - Duration.ofSeconds(15).toNanos()));
    assertNull(requestOnly.retryDelay(throttled, null, 0,
        start - Duration.ofSeconds(21).toNanos()));
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
