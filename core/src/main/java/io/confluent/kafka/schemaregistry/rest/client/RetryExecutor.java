/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
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
