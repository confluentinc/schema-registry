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
import java.util.Random;
import java.util.concurrent.Callable;

public class RetryExecutor {
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

  public RetryExecutor(int maxRetries, int initialWaitMs, int maxWaitMs) {
    this(maxRetries, initialWaitMs, maxWaitMs, new Random());
  }

  public RetryExecutor(int maxRetries, int initialWaitMs, int maxWaitMs, Random random) {
    this.maxRetries = maxRetries;
    this.initialWaitMs = Duration.ofMillis(initialWaitMs);
    this.maxWaitMs = Duration.ofMillis(maxWaitMs);
    this.random = random;
  }

  public <T> T retry(Callable<T> callable) throws RestClientException, IOException {
    for (int i = 0; i < maxRetries + 1; i++) {
      try {
        return callable.call();
      } catch (RestClientException e) {
        if (i >= maxRetries || !RestService.isRestClientExceptionRetriable(e)) {
          throw e;
        }
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
