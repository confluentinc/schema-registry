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
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Exponential-backoff retry that grants a retry only while
 * {@code elapsed + delay + one worst-case attempt} fits in the operation budget. azure-core's
 * RetryPolicy only counts retries, and a TimeoutPolicy cannot fire for the synchronous client.
 */
final class DeadlineRetryPolicy implements HttpPipelinePolicy {

  private static final Logger log = LoggerFactory.getLogger(DeadlineRetryPolicy.class);

  private final RetryStrategy strategy;
  private final Duration attempt;
  // Null means no deadline.
  private final Duration budget;
  private final LongSupplier nanoClock;

  DeadlineRetryPolicy(int maxRetries, Duration baseDelay, Duration maxDelay,
      Duration attempt, Duration budget) {
    this(new ExponentialBackoff(maxRetries, baseDelay, maxDelay), attempt, budget,
        System::nanoTime);
  }

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
    // A streamed body can only be sent once; buffer it for the retries.
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
    // Decide on the materialized outcome: an error handler here would also catch the retries'
    // final error and retry it again.
    return next.clone().process()
        .materialize()
        .flatMap(outcome -> retryOrFinish(outcome.get(), outcome.getThrowable(),
            () -> attemptAsync(context, next, original, tryCount + 1, start), tryCount, start));
  }

  private Mono<HttpResponse> retryOrFinish(HttpResponse response, Throwable error,
      Supplier<Mono<HttpResponse>> nextAttempt, int tryCount, long start) {
    if (response == null && error == null) {
      return Mono.empty();
    }
    Duration delay = retryDelay(response, error, tryCount, start);
    if (delay == null) {
      return error != null ? Mono.error(error) : Mono.just(response);
    }
    if (response != null) {
      response.close();
    }
    return Mono.delay(delay).then(Mono.defer(nextAttempt));
  }

  /** Returns the delay before the next attempt, or null to stop. */
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
        // WARN: otherwise a failure the deadline cut short looks like exhausted retries.
        log.warn("Not retrying Key Vault request after attempt {} ({}): {} elapsed, next attempt "
            + "would need up to {} of the {} operation timeout",
            tryCount + 1, describe(response, error), elapsed, needed, budget);
        return null;
      }
    }
    log.debug("Retrying Key Vault request in {} after attempt {} ({})",
        delay, tryCount + 1, describe(response, error));
    return delay;
  }

  private static String describe(HttpResponse response, Throwable error) {
    return response != null ? "HTTP " + response.getStatusCode() : String.valueOf(error);
  }

  /** Same headers azure-core's RetryPolicy consults; null when absent or unparseable. */
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
