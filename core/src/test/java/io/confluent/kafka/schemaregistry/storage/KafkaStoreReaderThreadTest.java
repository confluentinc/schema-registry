/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaStoreReaderThreadTest extends ClusterTestHarness {

  public KafkaStoreReaderThreadTest() {
    super(1, true);
  }

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreReaderThreadTest.class);

  @Before
  public void setup() {
  }

  @After
  public void teardown() {
    log.debug("Shutting down");
  }


  @Test
  public void testWaitUntilOffset() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    int id1 = restApp.restClient.registerSchema(schema, "subject1");

    KafkaSchemaRegistry sr = (KafkaSchemaRegistry) restApp.schemaRegistry();
    KafkaStoreReaderThread readerThread = sr.getKafkaStore().getKafkaStoreReaderThread();
    try {
      readerThread.waitUntilOffset(50L, 500L, TimeUnit.MILLISECONDS);
      fail("Should have timed out waiting to reach non-existent offset.");
    } catch (StoreTimeoutException e) {
      // This is expected
    }
    
    try {
      readerThread.waitUntilOffset(0L, 5000L, TimeUnit.MILLISECONDS);
    } catch (StoreTimeoutException e) {
      fail("5 seconds should be more than enough time to reach offset 0 in the log.");
    }
  }

  @Test
  public void testDeadReaderMakesNodeUnhealthyAndRejectsWrites() throws Exception {
    KafkaSchemaRegistry sr = (KafkaSchemaRegistry) restApp.schemaRegistry();
    KafkaStore<?, ?> store = sr.getKafkaStore();
    KafkaStoreReaderThread<?, ?> readerThread = store.getKafkaStoreReaderThread();
    assertTrue(store.isReaderAlive());
    assertTrue(sr.healthy());

    // Swap in a consumer whose poll throws a fatal error, then wake the real one so the
    // reader loop picks up the replacement and dies.
    Field field = KafkaStoreReaderThread.class.getDeclaredField("consumer");
    field.setAccessible(true);
    Consumer<byte[], byte[]> realConsumer = (Consumer<byte[], byte[]>) field.get(readerThread);
    MockConsumer<byte[], byte[]> failing = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    failing.schedulePollTask(() -> {
      throw new IllegalStateException("simulated fatal reader failure");
    });
    field.set(readerThread, failing);
    realConsumer.wakeup();

    long deadline = System.currentTimeMillis() + 10_000L;
    while (store.isReaderAlive() && System.currentTimeMillis() < deadline) {
      Thread.sleep(10L);
    }
    assertFalse("Reader should be reported dead", store.isReaderAlive());
    assertFalse("Node with a dead reader must be unhealthy", sr.healthy());
    try {
      restApp.restClient.registerSchema(
          TestUtils.getRandomCanonicalAvroString(1).get(0), "subject-dead-reader");
      fail("Writes must be rejected when the reader is dead");
    } catch (RestClientException e) {
      // expected
    }
    realConsumer.close();
  }
}
