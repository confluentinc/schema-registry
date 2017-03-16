/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import kafka.utils.ShutdownableThread;

/**
 * Thread that reads schema registry state from the Kafka compacted topic and modifies
 * the local store to be consistent.
 *
 * <p>On startup, this thread will always read from the beginning of the topic. We assume
 * the topic will always be small, hence the startup time to read the topic won't take
 * too long. Because the topic is always read from the beginning, the consumer never
 * commits offsets.
 */
public class KafkaStoreReaderThread<K, V> extends ShutdownableThread {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreReaderThread.class);

  private final String topic;
  private final TopicPartition topicPartition;
  private final String groupId;
  private final StoreUpdateHandler<K, V> storeUpdateHandler;
  private final Serializer<K, V> serializer;
  private final Store<K, V> localStore;
  private final ReentrantLock offsetUpdateLock;
  private final Condition offsetReachedThreshold;
  private Consumer<byte[], byte[]> consumer;
  private long offsetInSchemasTopic = -1L;
  // Noop key is only used to help reliably determine last offset; reader thread ignores 
  // messages with this key
  private final K noopKey;

  private Properties consumerProps = new Properties();

  public KafkaStoreReaderThread(String bootstrapBrokers,
                                String topic,
                                String groupId,
                                StoreUpdateHandler<K, V> storeUpdateHandler,
                                Serializer<K, V> serializer,
                                Store<K, V> localStore,
                                K noopKey,
                                SchemaRegistryConfig config) {
    super("kafka-store-reader-thread-" + topic, false);  // this thread is not interruptible
    offsetUpdateLock = new ReentrantLock();
    offsetReachedThreshold = offsetUpdateLock.newCondition();
    this.topic = topic;
    this.groupId = groupId;
    this.storeUpdateHandler = storeUpdateHandler;
    this.serializer = serializer;
    this.localStore = localStore;
    this.noopKey = noopKey;

    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaStore-reader-" + this.topic);

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

    consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                      config.getString(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG));
    KafkaStore.addSecurityConfigsToClientProperties(config, consumerProps);

    this.consumer = new KafkaConsumer<>(consumerProps);

    // Include a few retries since topic creation may take some time to propagate and schema
    // registry is often started immediately after creating the schemas topic.
    int retries = 0;
    List<PartitionInfo> partitions = null;
    while (retries++ < 10) {
      partitions = this.consumer.partitionsFor(this.topic);
      if (partitions != null && partitions.size() >= 1) {
        break;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore
      }
    }

    if (partitions == null || partitions.size() < 1) {
      throw new IllegalArgumentException("Unable to subscribe to the Kafka topic "
                                         + topic
                                         + " backing this data store. Topic may not exist.");
    } else if (partitions.size() > 1) {
      throw new IllegalStateException("Unexpected number of partitions in the "
                                      + topic
                                      + " topic. Expected 1 and instead got " + partitions.size());
    }

    this.topicPartition = new TopicPartition(topic, 0);
    this.consumer.assign(Arrays.asList(this.topicPartition));
    this.consumer.seekToBeginning(Arrays.asList(this.topicPartition));

    log.info("Initialized last consumed offset to " + offsetInSchemasTopic);

    log.debug("Kafka store reader thread started with consumer properties "
              + consumerProps.toString());
  }

  @Override
  public void doWork() {
    try {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Long.MAX_VALUE);
      for (ConsumerRecord<byte[], byte[]> record : records) {
        K messageKey = null;
        try {
          messageKey = this.serializer.deserializeKey(record.key());
        } catch (SerializationException e) {
          log.error("Failed to deserialize the schema or config key", e);
          continue;
        }

        if (messageKey.equals(noopKey)) {
          // If it's a noop, update local offset counter and do nothing else
          try {
            offsetUpdateLock.lock();
            offsetInSchemasTopic = record.offset();
            offsetReachedThreshold.signalAll();
          } finally {
            offsetUpdateLock.unlock();
          }
        } else {
          V message = null;
          try {
            message =
                record.value() == null ? null
                                       : serializer.deserializeValue(messageKey, record.value());
          } catch (SerializationException e) {
            log.error("Failed to deserialize a schema or config update", e);
            continue;
          }
          try {
            log.trace("Applying update ("
                      + messageKey
                      + ","
                      + message
                      + ") to the local store");
            if (message == null) {
              localStore.delete(messageKey);
            } else {
              localStore.put(messageKey, message);
            }
            this.storeUpdateHandler.handleUpdate(messageKey, message);
            try {
              offsetUpdateLock.lock();
              offsetInSchemasTopic = record.offset();
              offsetReachedThreshold.signalAll();
            } finally {
              offsetUpdateLock.unlock();
            }
          } catch (StoreException se) {
            log.error("Failed to add record from the Kafka topic"
                      + topic
                      + " the local store");
          }
        }
      }
    } catch (WakeupException we) {
      // do nothing because the thread is closing -- see shutdown()
    } catch (RecordTooLargeException rtle) {
      throw new IllegalStateException(
          "Consumer threw RecordTooLargeException. A schema has been written that "
          + "exceeds the default maximum fetch size.", rtle);
    } catch (RuntimeException e) {
      log.error("KafkaStoreReader thread has died for an unknown reason.");
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    log.debug("Starting shutdown of KafkaStoreReaderThread.");

    super.initiateShutdown();
    if (consumer != null) {
      consumer.wakeup();
    }
    if (localStore != null) {
      localStore.close();
    }
    super.awaitShutdown();
    consumer.close();
    log.info("KafkaStoreReaderThread shutdown complete.");
  }

  public void waitUntilOffset(long offset, long timeout, TimeUnit timeUnit) throws StoreException {
    if (offset < 0) {
      throw new StoreException("KafkaStoreReaderThread can't wait for a negative offset.");
    }

    log.trace("Waiting to read offset {}. Currently at offset {}", offset, offsetInSchemasTopic);

    try {
      offsetUpdateLock.lock();
      long timeoutNs = TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
      while ((offsetInSchemasTopic < offset) && (timeoutNs > 0)) {
        try {
          timeoutNs = offsetReachedThreshold.awaitNanos(timeoutNs);
        } catch (InterruptedException e) {
          log.debug("Interrupted while waiting for the background store reader thread to reach"
                    + " the specified offset: " + offset, e);
        }
      }
    } finally {
      offsetUpdateLock.unlock();
    }

    if (offsetInSchemasTopic < offset) {
      throw new StoreTimeoutException(
          "KafkaStoreReaderThread failed to reach target offset within the timeout interval. "
          + "targetOffset: " + offset + ", offsetReached: " + offsetInSchemasTopic
          + ", timeout(ms): " + TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
    }
  }

  /* for testing purposes */
  public String getConsumerProperty(String key) {
    return this.consumerProps.getProperty(key);
  }
}
