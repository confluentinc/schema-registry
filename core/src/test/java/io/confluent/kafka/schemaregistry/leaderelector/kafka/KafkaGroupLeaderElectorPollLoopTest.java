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

package io.confluent.kafka.schemaregistry.leaderelector.kafka;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that the leader-election poll loop survives transient exceptions
 * thrown from {@link SchemaRegistryCoordinator#poll(long)}.
 *
 * <p>Prior to this change the loop's try/catch was placed around the
 * surrounding {@code while}, so any unhandled exception terminated the elector
 * thread permanently. The pod kept serving reads from cache while writes
 * failed with {@code Leader not known} / {@code 50002} until a JVM restart.
 * See upstream issues #1696, #2492, #3135, #3910.
 */
public class KafkaGroupLeaderElectorPollLoopTest {

  private static final Logger log =
      LoggerFactory.getLogger(KafkaGroupLeaderElectorPollLoopTest.class);

  @Test(timeout = 10_000)
  public void survivesTransientException() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    CountDownLatch firstPoll = new CountDownLatch(1);
    CountDownLatch fivePollsCompleted = new CountDownLatch(5);

    Runnable pollOnce = () -> {
      int n = calls.incrementAndGet();
      firstPoll.countDown();
      fivePollsCompleted.countDown();
      // Throw exactly once, on the second invocation, to mimic the rebalance
      // edge case (e.g. IllegalStateException from onAssigned on DUPLICATE_URLS,
      // or a transient KafkaException) that previously killed the thread.
      if (n == 2) {
        throw new IllegalStateException("simulated rebalance error");
      }
      try {
        Thread.sleep(5);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    };

    AtomicBoolean stopped = new AtomicBoolean(false);
    Thread t = new Thread(() -> KafkaGroupLeaderElector.runPollLoop(
        pollOnce, stopped, /* retryBackoffMs */ 20L, log), "test-poll-loop");
    t.setDaemon(true);
    t.start();

    assertTrue("first poll should run", firstPoll.await(2, TimeUnit.SECONDS));
    assertTrue("loop should survive the throw and reach 5 polls",
        fivePollsCompleted.await(5, TimeUnit.SECONDS));

    stopped.set(true);
    t.join(2_000);
    assertFalse("thread should exit cleanly after stopped flag is set", t.isAlive());
  }

  @Test(timeout = 10_000)
  public void backsOffWhenExceptionIsPersistent() throws Exception {
    AtomicInteger calls = new AtomicInteger();

    Runnable alwaysThrows = () -> {
      calls.incrementAndGet();
      throw new IllegalStateException("simulated persistent failure");
    };

    AtomicBoolean stopped = new AtomicBoolean(false);
    long backoffMs = 50L;
    Thread t = new Thread(() -> KafkaGroupLeaderElector.runPollLoop(
        alwaysThrows, stopped, backoffMs, log), "test-poll-loop-persistent");
    t.setDaemon(true);
    t.start();

    // Let it spin for ~500 ms.
    Thread.sleep(500);
    stopped.set(true);
    t.join(2_000);
    assertFalse(t.isAlive());

    int observed = calls.get();
    // With backoffMs=50 and runtime ~500ms we expect roughly 10 retries.
    // Assert a generous range so the test is not timing-flaky, while still
    // catching a regression where the backoff is dropped (which would yield
    // hundreds or thousands of retries in 500 ms).
    assertTrue("expected backoff-paced retries, got " + observed + " in 500ms",
        observed >= 3 && observed <= 30);
  }

  @Test(timeout = 10_000)
  public void wakeupExceptionExitsLoop() throws Exception {
    AtomicInteger calls = new AtomicInteger();

    Runnable wakeup = () -> {
      calls.incrementAndGet();
      throw new WakeupException();
    };

    AtomicBoolean stopped = new AtomicBoolean(false);
    Thread t = new Thread(() -> KafkaGroupLeaderElector.runPollLoop(
        wakeup, stopped, /* retryBackoffMs */ 100L, log), "test-poll-loop-wakeup");
    t.setDaemon(true);
    t.start();

    t.join(2_000);
    assertFalse("WakeupException must exit the loop (close path)", t.isAlive());
    assertTrue("poll should have been invoked exactly once before the wakeup",
        calls.get() == 1);
  }

  @Test(timeout = 10_000)
  public void stoppedFlagExitsLoopCleanly() throws Exception {
    AtomicInteger calls = new AtomicInteger();

    Runnable normal = () -> {
      calls.incrementAndGet();
      try {
        Thread.sleep(5);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    };

    AtomicBoolean stopped = new AtomicBoolean(false);
    Thread t = new Thread(() -> KafkaGroupLeaderElector.runPollLoop(
        normal, stopped, /* retryBackoffMs */ 100L, log), "test-poll-loop-normal");
    t.setDaemon(true);
    t.start();

    Thread.sleep(100);
    stopped.set(true);
    t.join(2_000);
    assertFalse(t.isAlive());
    assertTrue("loop should have made progress before being stopped", calls.get() > 0);
  }
}
