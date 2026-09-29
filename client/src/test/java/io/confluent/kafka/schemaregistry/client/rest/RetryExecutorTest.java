/*
 * Copyright 2024 Confluent Inc.
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
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import org.junit.Assert;
import org.junit.Test;

public class RetryExecutorTest {

  @Test
  public void testRetryExecutorRestClientException() throws IOException, RestClientException {
    RetryExecutor retryExecutor = new RetryExecutor(3, 0, 0);
    TestCallable testCallable = new TestCallable();
    int result = retryExecutor.retry(testCallable);
    Assert.assertEquals(3, result);
  }

  @Test
  public void testRetryExecutorIOException() throws IOException, RestClientException {
    RetryExecutor retryExecutor = new RetryExecutor(3, 0, 0);
    TestCallableIO testCallable = new TestCallableIO();
    int result = retryExecutor.retry(testCallable);
    Assert.assertEquals(3, result);
  }

  @Test
  public void testRetryExecutorConnectTimeoutRetried() throws IOException, RestClientException {
    // Mirrors the leader-forwarding case: a transient "Connect timed out" is retried once.
    RetryExecutor retryExecutor = new RetryExecutor(1, 0, 0);
    TestCallableConnectTimeout testCallable = new TestCallableConnectTimeout(1);
    int result = retryExecutor.retry(testCallable);
    Assert.assertEquals(2, result);
  }

  @Test
  public void testRetryExecutorConnectTimeoutNotRetriedWhenDisabled() {
    // With retries disabled (maxRetries == 0) the connect failure propagates immediately.
    RetryExecutor retryExecutor = new RetryExecutor(0, 0, 0);
    TestCallableConnectTimeout testCallable = new TestCallableConnectTimeout(1);
    Assert.assertThrows(SocketTimeoutException.class, () -> retryExecutor.retry(testCallable));
    Assert.assertEquals(1, testCallable.count);
  }

  @Test
  public void testRetryExecutorWithVoid() throws IOException, RestClientException {
    RetryExecutor retryExecutor = new RetryExecutor(3, 0, 0);
    TestVoidCallable testCallable = new TestVoidCallable();
    retryExecutor.retry(testCallable);
    Assert.assertEquals(1, testCallable.count);
  }

  @Test
  public void testRetryExecutorWithNonRetryable() {
    RetryExecutor retryExecutor = new RetryExecutor(3, 0, 0);
    TestCallableNotFound testCallable = new TestCallableNotFound();
    Assert.assertThrows(RestClientException.class, () -> retryExecutor.retry(testCallable));
  }

  @Test
  public void testRetryExecutorTooManyRetries() {
    RetryExecutor retryExecutor = new RetryExecutor(2, 0, 0);
    TestCallable testCallable = new TestCallable();
    Assert.assertThrows(RestClientException.class, () -> retryExecutor.retry(testCallable));
  }

  @Test
  public void testCustomPredicateRetriesStatusDefaultWouldNot()
      throws IOException, RestClientException {
    // 409 is not retriable by default; the custom predicate opts into it.
    RetryExecutor retryExecutor =
        new RetryExecutor(3, 0, 0, new Random(), e -> e.getStatus() == 409);
    TestCallableWithStatus testCallable = new TestCallableWithStatus(409, 3);
    int result = retryExecutor.retry(testCallable);
    Assert.assertEquals(3, result);
  }

  @Test
  public void testDefaultPredicateDoesNotRetryConflict() {
    // Guards the flip side of the above: without a custom predicate, 409 must still fail fast.
    RetryExecutor retryExecutor = new RetryExecutor(3, 0, 0);
    TestCallableWithStatus testCallable = new TestCallableWithStatus(409, 3);
    Assert.assertThrows(RestClientException.class, () -> retryExecutor.retry(testCallable));
    Assert.assertEquals(1, testCallable.count);
  }

  @Test
  public void testCustomPredicateDoesNotRetryStatusDefaultWould() {
    // The predicate replaces the default rather than widening it: 500 is retriable by default
    // but excluded here, so the call must fail on its first attempt.
    RetryExecutor retryExecutor =
        new RetryExecutor(3, 0, 0, new Random(), e -> e.getStatus() == 409);
    TestCallableWithStatus testCallable = new TestCallableWithStatus(500, 3);
    Assert.assertThrows(RestClientException.class, () -> retryExecutor.retry(testCallable));
    Assert.assertEquals(1, testCallable.count);
  }

  @Test
  public void testNullFourthArgBindsToRandomAndIsRejected() {
    // Locks in why there is no four-arg predicate overload: with Random as the only four-arg
    // parameter, a bare null resolves unambiguously and is reported as a bad argument at
    // construction, instead of failing to compile with an ambiguous-reference error.
    Assert.assertThrows(NullPointerException.class, () -> new RetryExecutor(3, 0, 0, null));
  }

  @Test
  public void testCustomPredicateComposedWithDefault() throws IOException, RestClientException {
    // The expected real-world usage: the default statuses plus an extra one.
    Predicate<RestClientException> predicate =
        ((Predicate<RestClientException>) RestService::isRestClientExceptionRetriable)
            .or(e -> e.getStatus() == 409);
    RetryExecutor retryExecutor = new RetryExecutor(3, 0, 0, new Random(), predicate);
    Assert.assertEquals(3, (int) retryExecutor.retry(new TestCallableWithStatus(409, 3)));
    Assert.assertEquals(3, (int) retryExecutor.retry(new TestCallableWithStatus(503, 3)));
  }

  @Test
  public void testRetriesExhaustedWithCustomPredicate() {
    RetryExecutor retryExecutor =
        new RetryExecutor(2, 0, 0, new Random(), e -> e.getStatus() == 409);
    TestCallableWithStatus testCallable = new TestCallableWithStatus(409, 5);
    Assert.assertThrows(RestClientException.class, () -> retryExecutor.retry(testCallable));
    Assert.assertEquals(3, testCallable.count);
  }

  @Test
  public void testNullPredicateRejected() {
    Assert.assertThrows(NullPointerException.class,
        () -> new RetryExecutor(3, 0, 0, new Random(), null));
  }

  @Test
  public void testNullRandomRejected() {
    // Deferred otherwise: a null Random only fails at the first backoff, and only when
    // initialWaitMs > 0, so it would survive every test that uses a zero wait.
    Assert.assertThrows(NullPointerException.class,
        () -> new RetryExecutor(3, 1000, 20000, null, e -> e.getStatus() == 409));
  }

  @Test
  public void testIsRetriableReflectsPredicate() {
    RetryExecutor custom =
        new RetryExecutor(3, 0, 0, new Random(), e -> e.getStatus() == 409);
    Assert.assertTrue(custom.isRetriable(new RestClientException("test", 409, 40901)));
    Assert.assertFalse(custom.isRetriable(new RestClientException("test", 503, 50301)));

    RetryExecutor defaultExecutor = new RetryExecutor(3, 0, 0);
    Assert.assertFalse(defaultExecutor.isRetriable(new RestClientException("test", 409, 40901)));
    Assert.assertTrue(defaultExecutor.isRetriable(new RestClientException("test", 503, 50301)));
  }

  static class TestCallable implements Callable<Integer> {
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

  static class TestCallableIO implements Callable<Integer> {
    private int count = 0;
    @Override
    public Integer call() throws IOException {
      if (count < 3) {
        count++;
        throw new SocketException("test");
      }
      return count;
    }
  }

  /** Throws a connect timeout until {@code failures} attempts have been made. */
  static class TestCallableConnectTimeout implements Callable<Integer> {
    private final int failures;
    private int count = 0;

    TestCallableConnectTimeout(int failures) {
      this.failures = failures;
    }

    @Override
    public Integer call() throws IOException {
      if (count < failures) {
        count++;
        throw new SocketTimeoutException("Connect timed out");
      }
      return count + 1;
    }
  }

  static class TestVoidCallable implements Callable<Void> {
    protected int count = 0;
    @Override
    public Void call() throws RestClientException {
      count++;
      return null;
    }
  }

  /** Fails with the given status until {@code failures} attempts have been made. */
  static class TestCallableWithStatus implements Callable<Integer> {
    private final int status;
    private final int failures;
    private int count = 0;

    TestCallableWithStatus(int status, int failures) {
      this.status = status;
      this.failures = failures;
    }

    @Override
    public Integer call() throws RestClientException {
      if (count < failures) {
        count++;
        throw new RestClientException("test", status, status * 100 + 1);
      }
      return count;
    }
  }

  static class TestCallableNotFound implements Callable<Integer> {
    private int count = 0;
    @Override
    public Integer call() throws RestClientException {
      if (count < 3) {
        count++;
        throw new RestClientException("test", 404, 40401);
      }
      return count;
    }
  }
}
