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
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaStoreReaderThreadTest extends ClusterTestHarness {

  public KafkaStoreReaderThreadTest() {
    super(1, true);
  }

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreReaderThreadTest.class);

  @Test
  public void testWaitUntilOffset() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    int id1 = restApp.restClient.registerSchema(schema, "subject1");

    SchemaRegistry sr = restApp.schemaRegistry();
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
  public void testPollFailureIsRetried() throws Exception {
    Store<String, String> localStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    MockConsumer<byte[], byte[]> consumer = mockConsumer();
    KafkaStoreReaderThread<String, String> readerThread =
        readerThread(localStore, new StringMessageHandler(), consumer);
    try {
      // Used to rethrow and end the reader thread for good
      consumer.setPollException(new AuthenticationException("auth outage"));
      readerThread.doWork();

      consumer.addRecord(new ConsumerRecord<>(KAFKASTORE_TOPIC, 0, 0L,
          StringSerializer.INSTANCE.serializeKey("key"),
          StringSerializer.INSTANCE.serializeValue("value")));
      readerThread.doWork();
      assertEquals("value", localStore.get("key"));
    } finally {
      readerThread.shutdown();
    }
  }

  // A reader thread that dies fails a wait for an offset at once, with what it died of, rather
  // than leave the wait to time out.
  @Test
  public void testWaitUntilOffsetFailsOnceTheReaderDies() throws Exception {
    StoreUpdateHandler<String, String> handler = new StringMessageHandler() {
      @Override
      public void startBatch(int count) {
        throw new IllegalStateException("handler failed");
      }
    };
    KafkaStoreReaderThread<String, String> readerThread =
        readerThread(new InMemoryCache<>(StringSerializer.INSTANCE), handler, mockConsumer());
    try {
      readerThread.start();
      long start = System.nanoTime();
      StoreException e = assertThrows(StoreException.class,
          () -> readerThread.waitUntilOffset(0L, 30_000L, TimeUnit.MILLISECONDS));
      assertFalse(e instanceof StoreTimeoutException, "Failed as a timeout: " + e);
      assertTrue(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 10,
          "Waited for the timeout");
      assertTrue(e.getMessage().contains("has died"), e.getMessage());
      Throwable root = e;
      while (root.getCause() != null) {
        root = root.getCause();
      }
      assertEquals("handler failed", root.getMessage());
    } finally {
      readerThread.shutdown();
    }
  }

  // Shutting down the reader closes the local store only once the thread has finished the
  // batch it is applying, rather than close it under the thread.
  @Test
  public void testShutdownClosesTheStoreOnceTheReaderHasStopped() throws Exception {
    HeldReader held = new HeldReader();
    Thread closer = new Thread(held.readerThread::shutdown);
    closer.setDaemon(true);
    try {
      held.startAndAwaitBatch();
      closer.start();
      awaitWaitingInShutdown(closer);
      held.release.countDown();
      closer.join(TimeUnit.SECONDS.toMillis(30));
      assertFalse(closer.isAlive());
      assertFalse(held.closedDuringBatch.get(), "The store was closed under the reader's batch");
      assertTrue(held.closed.get());
    } finally {
      held.stop(closer);
    }
  }

  // An interrupt does not cut a shutdown's wait for the reader thread short, so the store is
  // still not closed under the reader's batch, and is closed once it is done; the interrupt is
  // restored afterward.
  @Test
  public void testAnInterruptedShutdownStillWaitsForTheReader() throws Exception {
    HeldReader held = new HeldReader();
    AtomicBoolean interruptRestored = new AtomicBoolean();
    Thread closer = new Thread(() -> {
      held.readerThread.shutdown();
      interruptRestored.set(Thread.currentThread().isInterrupted());
    });
    closer.setDaemon(true);
    try {
      held.startAndAwaitBatch();
      closer.start();
      awaitWaitingInShutdown(closer);
      closer.interrupt();
      // Waits again, rather than go on to close the store under the batch.
      awaitWaitingInShutdown(closer);
      assertFalse(held.closed.get(), "The store was closed under the reader's batch");
      held.release.countDown();
      // The reader too, so that its batch has recorded whether the store was closed under it,
      // even if the closer did not wait for it.
      held.readerThread.join(TimeUnit.SECONDS.toMillis(30));
      closer.join(TimeUnit.SECONDS.toMillis(30));
      assertFalse(closer.isAlive());
      assertFalse(held.closedDuringBatch.get(), "The store was closed under the reader's batch");
      assertTrue(held.closed.get(), "The store was not closed");
      assertTrue(interruptRestored.get(), "The interrupt was not restored");
    } finally {
      held.stop(closer);
    }
  }

  /** A reader thread held inside the batch for its first record until released. */
  private class HeldReader {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean closedDuringBatch = new AtomicBoolean();
    final AtomicBoolean closed = new AtomicBoolean();
    final KafkaStoreReaderThread<String, String> readerThread;

    HeldReader() throws Exception {
      Store<String, String> localStore = new InMemoryCache<String, String>(
          StringSerializer.INSTANCE) {
        @Override
        public void close() throws StoreException {
          closed.set(true);
          super.close();
        }
      };
      StoreUpdateHandler<String, String> handler = new StringMessageHandler() {
        @Override
        public void handleUpdate(String key, String value, String oldValue,
                                 TopicPartition tp, long offset, long timestamp) {
          entered.countDown();
          try {
            release.await(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          closedDuringBatch.set(closed.get());
        }
      };
      MockConsumer<byte[], byte[]> consumer = mockConsumer();
      readerThread = readerThread(localStore, handler, consumer);
      consumer.addRecord(new ConsumerRecord<>(KAFKASTORE_TOPIC, 0, 0L,
          StringSerializer.INSTANCE.serializeKey("key"),
          StringSerializer.INSTANCE.serializeValue("value")));
    }

    void startAndAwaitBatch() throws InterruptedException {
      readerThread.start();
      assertTrue(entered.await(30, TimeUnit.SECONDS));
    }

    // Releases the batch and shuts the reader down, unless the closer has done or is doing so.
    void stop(Thread closer) {
      release.countDown();
      if (closer.getState() == Thread.State.NEW) {
        readerThread.shutdown();
      }
    }
  }

  // Until the thread waits in awaitShutdown() for the reader, having done whatever shutdown()
  // does before that.
  private static void awaitWaitingInShutdown(Thread closer) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (closer.getState() != Thread.State.WAITING
        || Arrays.stream(closer.getStackTrace())
            .noneMatch(frame -> frame.getMethodName().equals("awaitShutdown"))) {
      assertTrue(closer.isAlive() && System.nanoTime() < deadline,
          "The shutdown did not wait for the reader thread");
      Thread.sleep(10);
    }
  }

  private static MockConsumer<byte[], byte[]> mockConsumer() {
    TopicPartition tp = new TopicPartition(KAFKASTORE_TOPIC, 0);
    MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.assign(Collections.singletonList(tp));
    consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
    return consumer;
  }

  private KafkaStoreReaderThread<String, String> readerThread(
      Store<String, String> localStore, StoreUpdateHandler<String, String> handler,
      MockConsumer<byte[], byte[]> consumer) throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaStoreReaderThread<String, String> readerThread = new KafkaStoreReaderThread<>(
        brokerList, KAFKASTORE_TOPIC, "test-group", handler,
        StringSerializer.INSTANCE, localStore, null, "noop", new AtomicBoolean(true), config);
    readerThread.setConsumer(consumer);
    return readerThread;
  }
}
