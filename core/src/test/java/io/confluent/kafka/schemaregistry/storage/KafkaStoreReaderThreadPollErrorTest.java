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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaStoreReaderThreadPollErrorTest {

  private static final String TOPIC = "_schemas";
  private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

  private MockConsumer<byte[], byte[]> consumer;
  private Store<String, String> store;
  private KafkaStoreReaderThread<String, String> reader;

  @Before
  public void setUp() {
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.assign(Collections.singletonList(TP));
    consumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));
    store = new InMemoryCache<>(StringSerializer.INSTANCE);
    reader = newReader(consumer, new StringMessageHandler(), new MockTime(), 0L);
  }

  private KafkaStoreReaderThread<String, String> newReader(
      Consumer<byte[], byte[]> consumer,
      StoreUpdateHandler<String, String> handler,
      Time time,
      long unhealthyAfterPollFailureMs) {
    return new KafkaStoreReaderThread<>(
        TOPIC,
        "test-group",
        handler,
        StringSerializer.INSTANCE,
        store,
        new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()),
        "noop",
        new AtomicBoolean(true),
        consumer,
        new KafkaStoreReaderThread.Tuning(time, 1L, 10L, unhealthyAfterPollFailureMs));
  }

  @After
  public void tearDown() throws Exception {
    if (reader.isAlive()) {
      reader.shutdown();
    }
  }

  private static void awaitTrue(java.util.function.BooleanSupplier condition) throws Exception {
    long deadline = System.currentTimeMillis() + 10_000L;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Timed out waiting for condition");
      }
      Thread.sleep(10L);
    }
  }

  @Test
  public void readerSurvivesTransientAuthenticationException() throws Exception {
    consumer.setPollException(new SaslAuthenticationException("Authentication failed"));
    consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(), "v".getBytes()));

    reader.start();

    awaitTrue(() -> "v".equals(getQuietly("k")));
    assertTrue(reader.isReaderAlive());
    assertTrue(reader.isAlive());
    assertEquals(0L, reader.consecutivePollFailures());
    assertEquals(0L, reader.millisInPollFailure());
  }

  @Test
  public void readerKeepsRetryingWhilePollKeepsFailing() throws Exception {
    // poll fails on every scheduled task, then recovers
    for (int i = 0; i < 5; i++) {
      consumer.schedulePollTask(() ->
          consumer.setPollException(new SaslAuthenticationException("Authentication failed")));
    }
    consumer.schedulePollTask(() ->
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(), "v".getBytes())));

    reader.start();

    awaitTrue(() -> "v".equals(getQuietly("k")));
    assertTrue(reader.isReaderAlive());
  }

  @Test
  public void deadReaderIsReportedNotAlive() throws Exception {
    consumer.schedulePollTask(() -> {
      throw new IllegalStateException("unexpected non-Kafka failure");
    });

    reader.start();

    awaitTrue(() -> !reader.isReaderAlive());
    assertFalse(reader.isReaderAlive());
  }

  @Test
  public void cleanShutdownIsNotReportedAsDead() throws Exception {
    reader.start();
    reader.shutdown();
    assertTrue(reader.isReaderAlive());
  }

  @Test
  public void kafkaExceptionAfterPollIsNotRetriedAsPollFailure() throws Exception {
    // A KafkaException from the update handler happens after poll() returned, so the consumer is
    // already past the batch. It must keep its original behaviour (the thread dies) instead of
    // being counted as a poll failure and silently dropping the batch.
    StoreUpdateHandler<String, String> failingHandler = new StringMessageHandler() {
      @Override
      public void startBatch(int batchSize) {
        throw new KafkaException("handler failure");
      }
    };
    reader = newReader(consumer, failingHandler, new MockTime(), 0L);
    consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(), "v".getBytes()));

    reader.start();

    awaitTrue(() -> !reader.isReaderAlive());
    assertEquals(0L, reader.consecutivePollFailures());
    assertEquals(0L, reader.millisInPollFailure());
  }

  @Test
  public void recordTooLargeFromPollIsFatal() throws Exception {
    consumer.setPollException(new RecordTooLargeException("too large"));

    reader.start();

    awaitTrue(() -> !reader.isReaderAlive());
    assertEquals(0L, reader.consecutivePollFailures());
  }

  @Test
  public void configExceptionFromPollIsFatal() throws Exception {
    // A configuration error cannot clear by retrying, so it must not be retried forever
    consumer.setPollException(new ConfigException("bad consumer config"));

    reader.start();

    awaitTrue(() -> !reader.isReaderAlive());
    assertEquals(0L, reader.consecutivePollFailures());
  }

  private MockConsumer<byte[], byte[]> flakyConsumer(AtomicBoolean failing) {
    MockConsumer<byte[], byte[]> flaky = new MockConsumer<byte[], byte[]>(
        OffsetResetStrategy.EARLIEST) {
      @Override
      public synchronized ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        if (failing.get()) {
          throw new SaslAuthenticationException("Authentication failed");
        }
        return super.poll(timeout);
      }
    };
    flaky.assign(Collections.singletonList(TP));
    flaky.updateBeginningOffsets(Collections.singletonMap(TP, 0L));
    return flaky;
  }

  @Test
  public void stuckReaderBecomesUnhealthyAfterThresholdAndRecovers() throws Exception {
    AtomicBoolean failing = new AtomicBoolean(true);
    MockTime time = new MockTime();
    reader = newReader(flakyConsumer(failing), new StringMessageHandler(), time, 1000L);

    reader.start();

    awaitTrue(() -> reader.consecutivePollFailures() > 0);
    assertTrue("alive and within the limit", reader.isReaderHealthy());
    assertFalse("store may be stale while polls fail", reader.isStoreTrustworthy());

    time.sleep(1500L);
    assertTrue(reader.isReaderAlive());
    assertTrue(reader.millisInPollFailure() >= 1500L);
    assertFalse("polls failing past the limit", reader.isReaderHealthy());

    failing.set(false);
    awaitTrue(() -> reader.consecutivePollFailures() == 0);
    assertEquals(0L, reader.millisInPollFailure());
    assertTrue(reader.isReaderHealthy());
    assertTrue(reader.isStoreTrustworthy());
  }

  @Test
  public void failureDurationIsCorrectWhenClockStartsAtZero() throws Exception {
    // A clock reading of 0 must not be mistaken for "not failing"
    MockTime time = new MockTime(0L, 0L, 0L);
    reader = newReader(flakyConsumer(new AtomicBoolean(true)), new StringMessageHandler(), time,
        1000L);

    reader.start();

    awaitTrue(() -> reader.consecutivePollFailures() > 0);
    time.sleep(1500L);
    assertEquals(1500L, reader.millisInPollFailure());
    assertFalse(reader.isReaderHealthy());
  }

  @Test
  public void thresholdOfZeroDisablesPollFailureHealthCheck() throws Exception {
    MockTime time = new MockTime();
    reader = newReader(
        flakyConsumer(new AtomicBoolean(true)), new StringMessageHandler(), time, 0L);

    reader.start();

    awaitTrue(() -> reader.consecutivePollFailures() > 0);
    time.sleep(10_000_000L);
    assertTrue(reader.millisInPollFailure() > 0);
    assertTrue(reader.isReaderHealthy());
  }

  private String getQuietly(String key) {
    try {
      return store.get(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
