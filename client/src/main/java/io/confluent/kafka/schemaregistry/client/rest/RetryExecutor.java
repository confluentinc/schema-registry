/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryExecutor {
  private static final Logger log = LoggerFactory.getLogger(RetryExecutor.class);

  /*
   * Max permitted retry times. To prevent exponentialDelay from overflow, there must be
   * 2 ^ retriesAttempted &lt;= 2 ^ 31 - 1, which means retriesAttempted &lt;= 30, so that
   * is the ceil for retriesAttempted.
   */
  static int RETRIES_ATTEMPTED_CEILING =
      (int) Math.floor(Math.log(Integer.MAX_VALUE) / Math.log(2));

  private final int maxRetries;
  private final Duration initialWaitMs;
  private final Duration maxWaitMs;
  private final Random random;
  private final Predicate<RestClientException> isRetriablePredicate;

  public RetryExecutor(int maxRetries, int initialWaitMs, int maxWaitMs) {
    this(maxRetries, initialWaitMs, maxWaitMs, new Random(),
        RestService::isRestClientExceptionRetriable);
  }

  public RetryExecutor(int maxRetries, int initialWaitMs, int maxWaitMs, Random random) {
    this(maxRetries, initialWaitMs, maxWaitMs, random, RestService::isRestClientExceptionRetriable);
  }

  /**
   * Deliberately the only constructor accepting a predicate: a four-arg overload taking just a
   * predicate would be ambiguous with the {@link Random} one for a {@code null} literal, which
   * fails to compile rather than reporting a bad argument. Requiring the caller to supply the
   * {@link Random} keeps every four-arg call unambiguous.
   */
  public RetryExecutor(int maxRetries, int initialWaitMs, int maxWaitMs, Random random,
      Predicate<RestClientException> isRetriablePredicate) {
    this.maxRetries = maxRetries;
    this.initialWaitMs = Duration.ofMillis(initialWaitMs);
    this.maxWaitMs = Duration.ofMillis(maxWaitMs);
    this.random = Objects.requireNonNull(random, "random cannot be null");
    this.isRetriablePredicate =
        Objects.requireNonNull(isRetriablePredicate, "isRetriablePredicate cannot be null");
  }

  /**
   * Whether this executor treats the given exception as retriable. Exposed so that callers
   * sharing this executor's retry policy -- notably {@code RestService}'s URL failover -- make
   * the same decision as {@link #retry(Callable)} rather than re-deriving it from the default.
   */
  public boolean isRetriable(RestClientException e) {
    return isRetriablePredicate.test(e);
  }

  public <T> T retry(Callable<T> callable) throws RestClientException, IOException {
    for (int i = 0; i < maxRetries + 1; i++) {
      try {
        return callable.call();
      } catch (RestClientException e) {
        if (!isRetriablePredicate.test(e)) {
          throw e;
        }
        if (i >= maxRetries) {
          // Only meaningful when retries are enabled; with maxRetries == 0 nothing was retried.
          if (maxRetries > 0) {
            log.warn("Retries exhausted after {} attempts, status {}: {}",
                maxRetries + 1, e.getStatus(), e.getMessage());
          }
          throw e;
        }
        log.debug("Retriable error on attempt {}/{}, status {}: {}. Retrying...",
            i + 1, maxRetries + 1, e.getStatus(), e.getMessage());
      } catch (IOException e) {
        if (i >= maxRetries) {
          throw e;
        }
      } catch (Exception e) {
        throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
      }
      sleepBeforeRetry(i);
    }
    return null;
  }

  private void sleepBeforeRetry(int attempt) {
    long delayMs = computeDelayBeforeNextRetry(attempt).toMillis();
    if (delayMs > 0) {
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException ignored) {
        // ignore
      }
    }
  }

  protected Duration computeDelayBeforeNextRetry(int retriesAttempted) {
    int ceil = calculateExponentialDelay(retriesAttempted);
    // Use full jitter strategy for computing the next backoff delay.
    // see https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    return ceil == 0 ? Duration.ofMillis(0L) : Duration.ofMillis(random.nextInt(ceil) + 1L);
  }

  protected int calculateExponentialDelay(int retriesAttempted) {
    int cappedRetries = Math.min(retriesAttempted, RETRIES_ATTEMPTED_CEILING);
    return (int) Math.min(
        initialWaitMs.multipliedBy(1L << cappedRetries).toMillis(), maxWaitMs.toMillis());
  }
}
