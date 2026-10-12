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

import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
    reader = new KafkaStoreReaderThread<>(
        TOPIC,
        new StringMessageHandler(),
        StringSerializer.INSTANCE,
        store,
        new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()),
        "noop",
        consumer,
        1L,
        10L);
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

  private String getQuietly(String key) {
    try {
      return store.get(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
