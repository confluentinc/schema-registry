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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.RetryStrategy;
import com.azure.core.util.Context;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DeadlineRetryPolicyTest {

  private static final Duration ATTEMPT = Duration.ofSeconds(15);
  private static final Duration BUDGET = Duration.ofSeconds(30);

  /** Five retries with no backoff, so the tests exercise the deadline and not the clock. */
  private static final RetryStrategy NO_BACKOFF = new RetryStrategy() {
    @Override
    public int getMaxRetries() {
      return 5;
    }

    @Override
    public Duration calculateRetryDelay(int retryAttempts) {
      return Duration.ZERO;
    }
  };

  /** A vault whose reply to attempt {@code n} is scripted, and whose clock the test controls. */
  private static final class FakeVault {
    final AtomicLong nanos = new AtomicLong();
    final List<HttpRequest> requests = Collections.synchronizedList(new ArrayList<>());
    final HttpClient client;

    FakeVault(Duration costPerAttempt, IntFunction<Object> outcomes) {
      this.client = request -> {
        int attempt = requests.size();
        requests.add(request);
        nanos.addAndGet(costPerAttempt.toNanos());
        Object outcome = outcomes.apply(attempt);
        if (outcome instanceof Throwable) {
          return Mono.error((Throwable) outcome);
        }
        return Mono.just(new FakeResponse(request, (Integer) outcome, new HttpHeaders()));
      };
    }

    HttpPipeline pipeline(Duration budget) {
      return new HttpPipelineBuilder()
          .httpClient(client)
          .policies(new DeadlineRetryPolicy(NO_BACKOFF, ATTEMPT, budget, nanos::get))
          .build();
    }
  }

  private static HttpRequest request() {
    HttpRequest request = new HttpRequest(HttpMethod.POST, "https://vault.example/keys/k/wrap");
    request.setBody("{\"alg\":\"RSA-OAEP-256\"}");
    return request;
  }

  @Test
  public void fastFailuresGetEveryRetryTheCapAllows() {
    // Each 429 comes back in 10ms, so five retries fit easily in the 30s budget even though a
    // worst-case attempt is 15s: this is the case the static pre-computation could not grant.
    FakeVault vault = new FakeVault(Duration.ofMillis(10), attempt -> 429);
    HttpResponse response = vault.pipeline(BUDGET).sendSync(request(), Context.NONE);
    assertEquals(429, response.getStatusCode());
    assertEquals(6, vault.requests.size());
  }

  @Test
  public void asyncPathAppliesTheSameRule() {
    FakeVault vault = new FakeVault(Duration.ofMillis(10), attempt -> 503);
    HttpResponse response = vault.pipeline(BUDGET).send(request()).block();
    assertEquals(503, response.getStatusCode());
    assertEquals(6, vault.requests.size());
  }

  @Test
  public void hungAttemptEndsTheOperation() {
    // The first attempt used (a little over) its whole worst-case cost, so one more cannot fit.
    // With the real backoff ladder the 1s delay alone pushes an exact 15 + 15 past 30s; this
    // strategy has no backoff, so the test adds the margin to the attempt instead.
    FakeVault vault = new FakeVault(ATTEMPT.plusMillis(1), attempt -> 503);
    HttpResponse response = vault.pipeline(BUDGET).sendSync(request(), Context.NONE);
    assertEquals(503, response.getStatusCode());
    assertEquals(1, vault.requests.size());
  }

  @Test
  public void retryIsGrantedOnlyWhileAnotherWorstCaseAttemptFits() {
    // 8s per attempt: after one, 8 + 15 = 23 fits; after two, 16 + 15 = 31 does not.
    FakeVault vault = new FakeVault(Duration.ofSeconds(8), attempt -> 503);
    vault.pipeline(BUDGET).sendSync(request(), Context.NONE);
    assertEquals(2, vault.requests.size());
  }

  @Test
  public void successAndNonRetriableStatusesReturnImmediately() {
    FakeVault ok = new FakeVault(Duration.ofMillis(10), attempt -> 200);
    assertEquals(200, ok.pipeline(BUDGET).sendSync(request(), Context.NONE).getStatusCode());
    assertEquals(1, ok.requests.size());

    FakeVault badRequest = new FakeVault(Duration.ofMillis(10), attempt -> 400);
    assertEquals(400,
        badRequest.pipeline(BUDGET).sendSync(request(), Context.NONE).getStatusCode());
    assertEquals(1, badRequest.requests.size());
  }

  @Test
  public void retriableExceptionsAreRetriedThenTheResponseIsReturned() {
    FakeVault vault = new FakeVault(Duration.ofMillis(10),
        attempt -> attempt < 2 ? new TimeoutException("response timed out") : 200);
    HttpResponse response = vault.pipeline(BUDGET).sendSync(request(), Context.NONE);
    assertEquals(200, response.getStatusCode());
    assertEquals(3, vault.requests.size());
  }

  @Test
  public void exhaustedRetriesRethrowTheLastException() {
    FakeVault vault = new FakeVault(Duration.ofMillis(10),
        attempt -> new TimeoutException("response timed out"));
    assertThrows(RuntimeException.class,
        () -> vault.pipeline(BUDGET).sendSync(request(), Context.NONE));
    assertEquals(6, vault.requests.size());
  }

  @Test
  public void withoutABudgetRetriesAreCountedOnly() {
    // No deadline: the same five retries as azure-core's RetryPolicy, however long attempts take.
    FakeVault vault = new FakeVault(Duration.ofHours(1), attempt -> 503);
    vault.pipeline(null).sendSync(request(), Context.NONE);
    assertEquals(6, vault.requests.size());
  }

  @Test
  public void everyRetryResendsTheOriginalBody() {
    FakeVault vault = new FakeVault(Duration.ofMillis(10), attempt -> 503);
    vault.pipeline(BUDGET).sendSync(request(), Context.NONE);
    for (HttpRequest sent : vault.requests) {
      assertEquals("{\"alg\":\"RSA-OAEP-256\"}", sent.getBodyAsBinaryData().toString());
    }
  }

  @Test
  public void retryAfterIsHonouredWhenItFitsAndEndsTheOperationWhenItDoesNot() {
    AtomicLong nanos = new AtomicLong();
    DeadlineRetryPolicy policy = new DeadlineRetryPolicy(NO_BACKOFF, ATTEMPT, BUDGET, nanos::get);
    HttpRequest request = request();

    HttpHeaders fits = new HttpHeaders().set(HttpHeaderName.RETRY_AFTER_MS, "2500");
    assertEquals(Duration.ofMillis(2500),
        policy.retryDelay(new FakeResponse(request, 429, fits), null, 0, 0L));

    // 20s of Retry-After plus a 15s attempt does not fit in 30s, so stop rather than overrun.
    HttpHeaders tooLong = new HttpHeaders().set(HttpHeaderName.RETRY_AFTER, "20");
    assertNull(policy.retryDelay(new FakeResponse(request, 429, tooLong), null, 0, 0L));

    // Without a budget the same header is simply waited out.
    DeadlineRetryPolicy unbounded = new DeadlineRetryPolicy(NO_BACKOFF, null, null, nanos::get);
    assertEquals(Duration.ofSeconds(20),
        unbounded.retryDelay(new FakeResponse(request, 429, tooLong), null, 0, 0L));
  }

  @Test
  public void retryAfterHeaderParsing() {
    assertEquals(Duration.ofMillis(750), DeadlineRetryPolicy.retryAfter(
        new HttpHeaders().set(HttpHeaderName.X_MS_RETRY_AFTER_MS, "750")));
    assertEquals(Duration.ofSeconds(3), DeadlineRetryPolicy.retryAfter(
        new HttpHeaders().set(HttpHeaderName.RETRY_AFTER, "3")));
    // An HTTP date in the past means "now".
    assertEquals(Duration.ZERO, DeadlineRetryPolicy.retryAfter(
        new HttpHeaders().set(HttpHeaderName.RETRY_AFTER, "Wed, 21 Oct 2015 07:28:00 GMT")));
    assertNull(DeadlineRetryPolicy.retryAfter(
        new HttpHeaders().set(HttpHeaderName.RETRY_AFTER, "soon")));
    assertNull(DeadlineRetryPolicy.retryAfter(new HttpHeaders()));
    assertNull(DeadlineRetryPolicy.retryAfter(null));
  }

  private static final class FakeResponse extends HttpResponse {
    private final int status;
    private final HttpHeaders headers;

    FakeResponse(HttpRequest request, int status, HttpHeaders headers) {
      super(request);
      this.status = status;
      this.headers = headers;
    }

    @Override
    public int getStatusCode() {
      return status;
    }

    @Override
    public String getHeaderValue(String name) {
      return headers.getValue(HttpHeaderName.fromString(name));
    }

    @Override
    public HttpHeaders getHeaders() {
      return headers;
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
      return getBodyAsString(StandardCharsets.UTF_8);
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
      return Mono.just("");
    }
  }
}
