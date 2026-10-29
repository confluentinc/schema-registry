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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.concurrent.CancellationException;
import org.junit.After;
import org.junit.Test;

public class JsonSchemaCancellationTest {

  @After
  public void clearInterrupt() {
    // Make sure an interrupt set by a test never leaks to other tests on this pooled thread.
    Thread.interrupted();
  }

  @Test
  public void noopWhenNotInterrupted() {
    // No exception expected.
    JsonSchemaCancellation.throwIfInterrupted();
  }

  @Test
  public void throwsAndClearsFlagWhenInterrupted() {
    Thread.currentThread().interrupt();
    try {
      JsonSchemaCancellation.throwIfInterrupted();
      fail("expected CancellationException when the thread is interrupted");
    } catch (CancellationException expected) {
      // expected
    }
    // The flag is cleared as it throws, so the pooled thread returns clean and a subsequent
    // call is a no-op.
    assertFalse(Thread.currentThread().isInterrupted());
    JsonSchemaCancellation.throwIfInterrupted();
  }
}
