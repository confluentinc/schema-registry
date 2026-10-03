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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Runs tasks on a shared thread pool, chained per key. Tasks submitted with the same key run
 * one at a time, in submission order. Tasks with different keys run in parallel. A task only
 * starts once the previous task for its key has finished, so a pool thread never waits on
 * another task's key.
 */
class LockChainedExecutor {

  static final long TERMINATION_WAIT_MS = 5_000;

  private final ExecutorService executor;
  private final ConcurrentHashMap<Object, CompletableFuture<Void>> tails =
      new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  private boolean closed = false;

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
    CompletableFuture<Void> next;
    // Hold the read lock so close() cannot snapshot the chains between the closed check and
    // adding this task. The pool never rejects a task before close(): a task rejected after
    // close() completes its future exceptionally without running, rather than throwing here.
    stateLock.readLock().lock();
    try {
      if (closed) {
        return false;
      }
      next = tails.compute(key, (k, tail) ->
          (tail == null ? CompletableFuture.<Void>completedFuture(null)
              : tail.handle((r, t) -> (Void) null))
              .thenRunAsync(task, executor));
    } finally {
      stateLock.readLock().unlock();
    }
    // Drop the entry once this task is the last one for its key and has finished
    next.whenComplete((r, t) -> tails.remove(key, next));
    return true;
  }

  /**
   * Stops accepting new tasks, waits up to the timeout for queued and running tasks, then
   * stops the pool. Tasks still queued after the timeout never run. Tasks still running are
   * interrupted, and close() waits a further {@link #TERMINATION_WAIT_MS} for them to exit, so
   * by the time it returns they have finished their cleanup; {@link #isTerminated()} reports
   * whether they did.
   *
   * @return true if every queued task finished before the timeout
   */
  boolean close(long timeout, TimeUnit unit) {
    CompletableFuture<?>[] queued;
    stateLock.writeLock().lock();
    try {
      closed = true;
      queued = tails.values().toArray(new CompletableFuture<?>[0]);
    } finally {
      stateLock.writeLock().unlock();
    }
    boolean drained = true;
    // Restore the interrupt flag only after the waits below, since it would make them throw
    boolean interrupted = false;
    try {
      CompletableFuture.allOf(queued)
          .handle((r, t) -> (Void) null)
          .get(timeout, unit);
    } catch (InterruptedException e) {
      interrupted = true;
      drained = false;
    } catch (ExecutionException | TimeoutException e) {
      drained = false;
    }
    executor.shutdownNow();
    try {
      executor.awaitTermination(TERMINATION_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      interrupted = true;
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return drained;
  }

  boolean isTerminated() {
    return executor.isTerminated();
  }
}
