/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

public class LockChainedExecutorTest {

  @Test
  public void testSameKeyRunsInOrderWithoutOverlap() throws Exception {
    LockChainedExecutor executor = new LockChainedExecutor(4, "test");
    Lock lock = new ReentrantLock();
    List<Integer> order = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger running = new AtomicInteger();
    AtomicInteger maxRunning = new AtomicInteger();
    int tasks = 50;
    CountDownLatch done = new CountDownLatch(tasks);

    for (int i = 0; i < tasks; i++) {
      int n = i;
      assertTrue(executor.submit(lock, () -> {
        maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        order.add(n);
        running.decrementAndGet();
        done.countDown();
      }));
    }

    assertTrue(done.await(30, TimeUnit.SECONDS));
    assertEquals(1, maxRunning.get());
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < tasks; i++) {
      expected.add(i);
    }
    assertEquals(expected, order);
    assertTrue(executor.close(10, TimeUnit.SECONDS));
  }

  @Test
  public void testDifferentKeysRunConcurrently() throws Exception {
    LockChainedExecutor executor = new LockChainedExecutor(2, "test");
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch done = new CountDownLatch(2);
    Runnable task = () -> {
      bothStarted.countDown();
      try {
        // Only returns true if the other key's task is running at the same time
        if (bothStarted.await(10, TimeUnit.SECONDS)) {
          done.countDown();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    assertTrue(executor.submit(new ReentrantLock(), task));
    assertTrue(executor.submit(new ReentrantLock(), task));

    assertTrue(done.await(30, TimeUnit.SECONDS));
    assertTrue(executor.close(10, TimeUnit.SECONDS));
  }

  @Test
  public void testFailedTaskDoesNotBreakChain() throws Exception {
    LockChainedExecutor executor = new LockChainedExecutor(1, "test");
    Object key = new Object();
    CountDownLatch done = new CountDownLatch(1);

    assertTrue(executor.submit(key, () -> {
      throw new RuntimeException("boom");
    }));
    assertTrue(executor.submit(key, done::countDown));

    assertTrue(done.await(30, TimeUnit.SECONDS));
    assertTrue(executor.close(10, TimeUnit.SECONDS));
  }

  @Test
  public void testCloseWaitsForQueuedTasksAndRejectsNewOnes() throws Exception {
    LockChainedExecutor executor = new LockChainedExecutor(1, "test");
    Object key = new Object();
    AtomicInteger completed = new AtomicInteger();
    for (int i = 0; i < 3; i++) {
      assertTrue(executor.submit(key, () -> {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        completed.incrementAndGet();
      }));
    }

    assertTrue(executor.close(10, TimeUnit.SECONDS));
    assertEquals(3, completed.get());
    assertFalse(executor.submit(key, () -> { }));
  }

  @Test
  public void testCloseTimesOut() throws Exception {
    LockChainedExecutor executor = new LockChainedExecutor(1, "test");
    CountDownLatch release = new CountDownLatch(1);
    assertTrue(executor.submit(new Object(), () -> {
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }));

    assertFalse(executor.close(100, TimeUnit.MILLISECONDS));
    release.countDown();
  }

  @Test
  public void testTaskQueuedBehindTimedOutTaskNeverRuns() throws Exception {
    LockChainedExecutor executor = new LockChainedExecutor(1, "test");
    Object key = new Object();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger secondRan = new AtomicInteger();
    assertTrue(executor.submit(key, () -> {
      started.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }));
    assertTrue(executor.submit(key, secondRan::incrementAndGet));
    assertTrue(started.await(10, TimeUnit.SECONDS));

    // close() interrupts the first task after the timeout; the pool is shut down by then, so
    // the second task is rejected instead of running, and submit() never threw for it
    assertFalse(executor.close(100, TimeUnit.MILLISECONDS));
    Thread.sleep(200);
    assertEquals(0, secondRan.get());
  }
}
