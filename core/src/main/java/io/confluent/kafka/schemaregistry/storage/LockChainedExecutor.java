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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs tasks on a shared thread pool, chained per key. Tasks submitted with the same key run
 * one at a time, in submission order. Tasks with different keys run in parallel. A task only
 * starts once the previous task for its key has finished, so a pool thread never waits on
 * another task's key.
 */
class LockChainedExecutor {

  private final ExecutorService executor;
  private final ConcurrentHashMap<Object, CompletableFuture<Void>> tails =
      new ConcurrentHashMap<>();
  private volatile boolean closed = false;

  LockChainedExecutor(int threads, String threadNamePrefix) {
    AtomicInteger threadCount = new AtomicInteger();
    this.executor = Executors.newFixedThreadPool(threads, r -> {
      Thread t = new Thread(r, threadNamePrefix + "-" + threadCount.incrementAndGet());
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Queues a task after the last task queued for the same key.
   *
   * @return false if the executor is closed and the task was not queued
   */
  boolean submit(Object key, Runnable task) {
    if (closed) {
      return false;
    }
    CompletableFuture<Void> next;
    try {
      next = tails.compute(key, (k, tail) ->
          (tail == null ? CompletableFuture.<Void>completedFuture(null)
              : tail.handle((r, t) -> (Void) null))
              .thenRunAsync(task, executor));
    } catch (RejectedExecutionException e) {
      return false;
    }
    // Drop the entry once this task is the last one for its key and has finished
    next.whenComplete((r, t) -> tails.remove(key, next));
    return true;
  }

  /**
   * Stops accepting new tasks, waits up to the timeout for queued and running tasks, then
   * stops the pool.
   *
   * @return true if every queued task finished before the timeout
   */
  boolean close(long timeout, TimeUnit unit) {
    closed = true;
    boolean drained = true;
    try {
      CompletableFuture.allOf(tails.values().toArray(new CompletableFuture<?>[0]))
          .handle((r, t) -> (Void) null)
          .get(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      drained = false;
    } catch (ExecutionException | TimeoutException e) {
      drained = false;
    }
    executor.shutdownNow();
    return drained;
  }
}
