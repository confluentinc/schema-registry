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

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.ExponentialBackoff;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.http.policy.RetryStrategy;
import com.azure.core.util.BinaryData;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * azure-core's exponential-backoff retry, with a deadline on the whole operation.
 *
 * <p>azure-core's own {@link com.azure.core.http.policy.RetryPolicy} decides retries purely by
 * count, and the synchronous client has no deadline that spans retries (a {@code TimeoutPolicy}
 * runs the downstream policies on the calling thread, so its reactor timeout cannot fire). The
 * number of retries that fit in a budget therefore had to be computed up front, costing every
 * attempt at its worst case. That is right for a hung vault and far too pessimistic for the
 * realistic blip, a 429 or 5xx that comes back in milliseconds: at a 15s worst-case attempt a 30s
 * budget allowed no retry at all, even though five fast failures plus the whole backoff ladder
 * take about 31s of sleep and almost no request time.
 *
 * <p>This policy keeps the same ladder and cap but decides each retry at runtime: a retry is
 * granted only while {@code elapsed + delay + one worst-case attempt} still fits in the budget.
 * Fast failures get every retry the cap allows; a hung vault gets none and exits after one
 * attempt, exactly as the static computation did. A {@code Retry-After} header is honoured when
 * it fits and ends the operation when it does not, so a throttled vault is never hammered and the
 * deadline is never silently extended. Without a budget it behaves like
 * {@code new RetryPolicy(new ExponentialBackoff(maxRetries, baseDelay, maxDelay))}.
 *
 * <p>The retry decision itself (which statuses and exceptions are retriable, the jittered delay)
 * is delegated to {@link ExponentialBackoff}, so the two policies disagree only on the deadline.
 */
final class DeadlineRetryPolicy implements HttpPipelinePolicy {

  private static final Logger log = LoggerFactory.getLogger(DeadlineRetryPolicy.class);

  private final RetryStrategy strategy;
  /** Worst-case cost of one attempt (connect + request timeouts); only used with a budget. */
  private final Duration attempt;
  /** Deadline for the whole operation, or null for count-only retries. */
  private final Duration budget;
  private final LongSupplier nanoClock;

  /**
   * @param maxRetries cap on the number of retries, as for {@link ExponentialBackoff}
   * @param baseDelay first backoff delay, as for {@link ExponentialBackoff}
   * @param maxDelay cap on the backoff delay, as for {@link ExponentialBackoff}
   * @param attempt worst-case cost of one attempt; ignored when {@code budget} is null
   * @param budget deadline for the whole operation including retries, or null for none
   */
  DeadlineRetryPolicy(int maxRetries, Duration baseDelay, Duration maxDelay,
      Duration attempt, Duration budget) {
    this(new ExponentialBackoff(maxRetries, baseDelay, maxDelay), attempt, budget,
        System::nanoTime);
  }

  // Visible for testing: the strategy supplies the delays and the clock supplies the time.
  DeadlineRetryPolicy(RetryStrategy strategy, Duration attempt, Duration budget,
      LongSupplier nanoClock) {
    if (budget != null && attempt == null) {
      throw new IllegalArgumentException("an operation budget needs the cost of one attempt");
    }
    this.strategy = strategy;
    this.attempt = attempt;
    this.budget = budget;
    this.nanoClock = nanoClock;
  }

  @Override
  public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
    HttpRequest original = context.getHttpRequest();
    long start = nanoClock.getAsLong();
    BinaryData body = original.getBodyAsBinaryData();
    if (body == null || body.isReplayable()) {
      return attemptAsync(context, next, original, 0, start);
    }
    // A streamed body can only be sent once; buffer it so every retry can resend it.
    return body.toReplayableBinaryDataAsync()
        .flatMap(replayable -> attemptAsync(context, next, original.setBody(replayable), 0, start));
  }

  @Override
  public HttpResponse processSync(HttpPipelineCallContext context,
      HttpPipelineNextSyncPolicy next) {
    HttpRequest original = context.getHttpRequest();
    long start = nanoClock.getAsLong();
    BinaryData body = original.getBodyAsBinaryData();
    if (body != null && !body.isReplayable()) {
      original.setBody(body.toReplayableBinaryData());
    }
    int tryCount = 0;
    while (true) {
      // Each attempt sends a copy so a retry never resends headers mutated downstream.
      context.setHttpRequest(original.copy());
      HttpResponse response;
      try {
        response = next.clone().processSync();
      } catch (RuntimeException e) {
        Duration delay = retryDelay(null, e, tryCount, start);
        if (delay == null) {
          throw e;
        }
        sleep(delay);
        tryCount++;
        continue;
      }
      Duration delay = retryDelay(response, null, tryCount, start);
      if (delay == null) {
        return response;
      }
      response.close();
      sleep(delay);
      tryCount++;
    }
  }

  private Mono<HttpResponse> attemptAsync(HttpPipelineCallContext context,
      HttpPipelineNextPolicy next, HttpRequest original, int tryCount, long start) {
    context.setHttpRequest(original.copy());
    return next.clone().process()
        .flatMap(response -> {
          Duration delay = retryDelay(response, null, tryCount, start);
          if (delay == null) {
            return Mono.just(response);
          }
          response.close();
          return Mono.delay(delay)
              .then(Mono.defer(() -> attemptAsync(context, next, original, tryCount + 1, start)));
        })
        .onErrorResume(error -> {
          Duration delay = retryDelay(null, error, tryCount, start);
          if (delay == null) {
            return Mono.error(error);
          }
          return Mono.delay(delay)
              .then(Mono.defer(() -> attemptAsync(context, next, original, tryCount + 1, start)));
        });
  }

  /**
   * Decides whether attempt {@code tryCount} (zero-based) is retried, and after how long.
   * Exactly one of {@code response} and {@code error} is non-null.
   *
   * @return the delay before the next attempt, or null to stop and return this outcome
   */
  // Visible for testing.
  Duration retryDelay(HttpResponse response, Throwable error, int tryCount, long startNanos) {
    if (tryCount >= strategy.getMaxRetries()) {
      return null;
    }
    boolean retriable = response != null
        ? strategy.shouldRetry(response)
        : strategy.shouldRetryException(error);
    if (!retriable) {
      return null;
    }
    Duration delay = strategy.calculateRetryDelay(tryCount);
    if (response != null) {
      Duration retryAfter = retryAfter(response.getHeaders());
      if (retryAfter != null && retryAfter.compareTo(delay) > 0) {
        delay = retryAfter;
      }
    }
    if (budget != null) {
      Duration elapsed = Duration.ofNanos(nanoClock.getAsLong() - startNanos);
      Duration needed = elapsed.plus(delay).plus(attempt);
      if (needed.compareTo(budget) > 0) {
        log.debug("Not retrying Key Vault request after attempt {}: {} elapsed, next attempt "
            + "would need up to {} of a {} budget", tryCount + 1, elapsed, needed, budget);
        return null;
      }
    }
    return delay;
  }

  /**
   * The server's requested delay, from the same headers azure-core's RetryPolicy consults:
   * {@code retry-after-ms} and {@code x-ms-retry-after-ms} in milliseconds, then
   * {@code Retry-After} in seconds or as an HTTP date. Null when absent or unparseable.
   */
  static Duration retryAfter(HttpHeaders headers) {
    if (headers == null) {
      return null;
    }
    Duration millis = millisHeader(headers, HttpHeaderName.RETRY_AFTER_MS);
    if (millis == null) {
      millis = millisHeader(headers, HttpHeaderName.X_MS_RETRY_AFTER_MS);
    }
    if (millis != null) {
      return millis;
    }
    String value = headers.getValue(HttpHeaderName.RETRY_AFTER);
    if (value == null || value.isEmpty()) {
      return null;
    }
    try {
      return Duration.ofSeconds(Long.parseLong(value.trim()));
    } catch (NumberFormatException notSeconds) {
      try {
        OffsetDateTime at =
            OffsetDateTime.parse(value.trim(), DateTimeFormatter.RFC_1123_DATE_TIME);
        Duration until = Duration.between(OffsetDateTime.now(), at);
        return until.isNegative() ? Duration.ZERO : until;
      } catch (DateTimeParseException notDate) {
        return null;
      }
    }
  }

  private static Duration millisHeader(HttpHeaders headers, HttpHeaderName name) {
    String value = headers.getValue(name);
    if (value == null || value.isEmpty()) {
      return null;
    }
    try {
      return Duration.ofMillis(Long.parseLong(value.trim()));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static void sleep(Duration delay) {
    try {
      Thread.sleep(delay.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting to retry a Key Vault request", e);
    }
  }
}
