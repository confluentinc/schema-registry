/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.rest.client;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.concurrent.Callable;

public class RetryExecutor {
  private static final int HTTP_TOO_MANY_REQUESTS = 429;
  private final int maxRetries;
  private final int retriesWaitMs;

  public RetryExecutor(int maxRetries, int retriesWaitMs) {
    this.maxRetries = maxRetries;
    this.retriesWaitMs = retriesWaitMs;
  }

  public <T> T retry(Callable<T> callable) throws RestClientException, IOException {
    T result = null;
    for (int i = 0; i < maxRetries + 1; i++) {
      try {
        result = callable.call();
      } catch (RestClientException e) {
        if (i >= maxRetries || e.getStatus() != HTTP_TOO_MANY_REQUESTS) {
          throw e;
        }
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
      }
      if (result != null) {
        break;
      }
      if (retriesWaitMs > 0) {
        try {
          Thread.sleep(retriesWaitMs);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }
    return result;
  }

}
