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
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import static org.junit.Assert.assertEquals;
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
    assertEquals(1.0, gauge(sr, MetricsContainer.METRIC_NAME_READER_ALIVE), 0.0);

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
    assertEquals(0.0, gauge(sr, MetricsContainer.METRIC_NAME_READER_ALIVE), 0.0);
    try {
      restApp.restClient.registerSchema(
          TestUtils.getRandomCanonicalAvroString(1).get(0), "subject-dead-reader");
      fail("Writes must be rejected when the reader is dead");
    } catch (RestClientException e) {
      assertEquals(50001, e.getErrorCode());
    }
    realConsumer.close();
  }

  @Test
  public void testFailingPollDoesNotFailInitStyleWaitButRejectsWrites() throws Exception {
    KafkaSchemaRegistry sr = (KafkaSchemaRegistry) restApp.schemaRegistry();
    KafkaStore<?, ?> store = sr.getKafkaStore();
    KafkaStoreReaderThread<?, ?> readerThread = store.getKafkaStoreReaderThread();

    // Swap in a consumer whose poll keeps failing with an authentication error
    Field field = KafkaStoreReaderThread.class.getDeclaredField("consumer");
    field.setAccessible(true);
    Consumer<byte[], byte[]> realConsumer = (Consumer<byte[], byte[]>) field.get(readerThread);
    MockConsumer<byte[], byte[]> failing =
        new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
          @Override
          public synchronized ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            throw new SaslAuthenticationException("Authentication failed");
          }
        };
    field.set(readerThread, failing);
    realConsumer.wakeup();

    long deadline = System.currentTimeMillis() + 10_000L;
    while (readerThread.consecutivePollFailures() == 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(10L);
    }
    assertTrue("Polls should be failing", readerThread.consecutivePollFailures() > 0);
    assertTrue("Reader is stuck, not dead", store.isReaderAlive());
    assertTrue("Health limit is off by default", sr.healthy());
    assertEquals(1.0, gauge(sr, MetricsContainer.METRIC_NAME_READER_ALIVE), 0.0);
    assertTrue(gauge(sr, MetricsContainer.METRIC_NAME_READER_POLL_FAILURES) > 0.0);

    // The wait used by init() and leader transition must not fail fast just because polls are
    // failing: a reader that recovers within the timeout is fine. It may return immediately (the
    // last written offset is already known) or time out, but must not throw the "failing to
    // poll" StoreException that the write paths use.
    try {
      store.waitUntilKafkaReaderReachesLastOffset(500);
    } catch (StoreTimeoutException e) {
      // acceptable: waited for the timeout instead of failing fast
    }

    // Writes must be rejected while the store may be stale
    try {
      restApp.restClient.registerSchema(
          TestUtils.getRandomCanonicalAvroString(1).get(0), "subject-failing-poll");
      fail("Writes must be rejected while polls are failing");
    } catch (RestClientException e) {
      assertEquals(50001, e.getErrorCode());
    }
    realConsumer.close();
  }

  private static double gauge(KafkaSchemaRegistry sr, String name) {
    for (Map.Entry<MetricName, KafkaMetric> e
        : sr.getMetricsContainer().getMetrics().metrics().entrySet()) {
      if (e.getKey().name().equals(name)) {
        return ((Number) e.getValue().metricValue()).doubleValue();
      }
    }
    throw new AssertionError("Metric not registered: " + name);
  }
}
