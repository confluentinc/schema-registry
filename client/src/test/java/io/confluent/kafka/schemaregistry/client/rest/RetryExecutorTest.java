/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Test;

public class RetryExecutorTest {

  @Test
  public void testRetryExecutor() throws IOException, RestClientException {
    RetryExecutor retryExecutor = new RetryExecutor(3, 0);
    TestCallable testCallable = new TestCallable();
    int result = retryExecutor.retry(testCallable);
    Assert.assertEquals(3, result);
  }

  class TestCallable implements Callable<Integer> {
    private int count = 0;
    @Override
    public Integer call() throws RestClientException {
      if (count < 3) {
        count++;
        throw new RestClientException("test", 500, 50001);
      }
      return count;
    }
  }
}
