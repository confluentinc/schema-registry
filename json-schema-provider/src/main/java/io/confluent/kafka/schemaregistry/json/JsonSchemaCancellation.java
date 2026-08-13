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

package io.confluent.kafka.schemaregistry.json;

import java.util.concurrent.CancellationException;

/**
 * Cooperative cancellation for the potentially expensive JSON Schema operations (compatibility
 * diff, normalization, recursive traversals, and the lead-in to parsing/$ref resolution).
 *
 * <p>The rest-utils blanket request timeout (request.timeout.ms with
 * request.timeout.interrupt.enable=true) interrupts the Jetty worker thread once a request
 * exceeds its wall-clock budget. That timeout is the universal backstop: it abandons the request
 * with an HTTP 504 regardless of where it is stuck. This helper is the optimization layered on
 * top — it lets the code paths we own notice the interrupt promptly, abort, and return the pooled
 * thread to service instead of burning CPU to completion.
 *
 * <p>IMPORTANT: this is cooperative. It only fires in code that calls
 * {@link #throwIfInterrupted()}.
 * The third-party schema loaders ({@code org.everit.json.schema} and
 * {@code com.github.erosb.jsonsKema}) do their own (potentially exponential) $ref resolution and do
 * not poll the interrupt flag, so a parse already in flight inside those libraries is reclaimed
 * only by the request timeout, not by this check. Calling {@link #throwIfInterrupted()} at our
 * entry points (before a parse is kicked off) is the closest cooperative guard we can place around
 * that path.
 */
public final class JsonSchemaCancellation {

  private JsonSchemaCancellation() {
  }

  /**
   * Aborts the current operation if the worker thread has been interrupted (e.g. by the request
   * timeout). {@link Thread#interrupted()} also clears the interrupt flag so the pooled thread
   * returns clean to the executor.
   *
   * @throws CancellationException if the current thread's interrupt flag was set
   */
  public static void throwIfInterrupted() {
    if (Thread.interrupted()) {
      throw new CancellationException(
          "JSON schema operation was interrupted (likely a request timeout)");
    }
  }
}
