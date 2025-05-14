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

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import io.confluent.kafka.schemaregistry.storage.StoreUpdateHandler.ValidationStatus;
import io.confluent.kafka.schemaregistry.storage.encoder.KeysetWrapper;
import io.confluent.kafka.schemaregistry.storage.encoder.KeysetWrapperSerde;
import io.confluent.kafka.schemaregistry.storage.encoder.ResourceManagerStoreUpdateHandler;
import io.confluent.kafka.schemaregistry.utils.ShutdownableThread;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.kcache.CacheUpdateHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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

import java.nio.charset.StandardCharsets;

/**
 * Thread that reads keyset wrapper state from the Kafka compacted topic and modifies
 * RM to be consistent.
 *
 * <p>On startup, this thread will always read from the beginning of the topic. We assume
 * the topic will always be small, hence the startup time to read the topic won't take
 * too long. Because the topic is always read from the beginning, the consumer never
 * commits offsets.
 */
public class MetadataEncoderStoreReaderThread<K, V> extends ShutdownableThread {

  private static final Logger log = LoggerFactory.getLogger(MetadataEncoderStoreReaderThread.class);

  private final String topic;
  private final TopicPartition topicPartition;
  private final String groupId;
  private final StoreUpdateHandler<K, V> storeUpdateHandler;
  private final ResourceManagerStoreUpdateHandler<String, KeysetWrapper> resourceManagerStoreUpdateHandler;
  private final KeysetWrapperSerde keysetWrapperSerde;
  private final ReentrantLock offsetUpdateLock;
  private final Condition offsetReachedThreshold;
  private Consumer<byte[], byte[]> consumer;
  private final Producer<byte[], byte[]> producer;
  private long offsetInMetadataEncoderTopic = -1L;
  private OffsetCheckpoint checkpointFile;
  private Map<TopicPartition, Long> checkpointFileCache = new HashMap<>();
  // Noop key is only used to help reliably determine last offset; reader thread ignores
  // messages with this key
  private final String noopKey;
  private final AtomicBoolean initialized;

  private Properties consumerProps = new Properties();

  public MetadataEncoderStoreReaderThread(String bootstrapBrokers,
                                          String topic,
                                          String groupId,
                                          StoreUpdateHandler<K, V> storeUpdateHandler,
                                          KeysetWrapperSerde keysetWrapperSerde,
                                          Producer<byte[], byte[]> producer,
                                          String noopKey,
                                          AtomicBoolean initialized,
                                          SchemaRegistryConfig config) {
    super("metadata-encoder-store-reader-thread-" + topic, false);  // this thread is not interruptible
    offsetUpdateLock = new ReentrantLock();
    offsetReachedThreshold = offsetUpdateLock.newCondition();
    this.topic = topic;
    this.groupId = groupId;
    this.storeUpdateHandler = storeUpdateHandler;
    this.keysetWrapperSerde = keysetWrapperSerde;
    this.producer = producer;
    this.noopKey = noopKey;
    this.initialized = initialized;
    this.resourceManagerStoreUpdateHandler = new ResourceManagerStoreUpdateHandler<>(config);

    // always attempt to read checkpoints
    try {
      String checkpointDir =
              config.getString(SchemaRegistryConfig.KAFKASTORE_CHECKPOINT_DIR_CONFIG); // change this (dont need to)
      int checkpointVersion =
              config.getInt(SchemaRegistryConfig.KAFKASTORE_CHECKPOINT_VERSION_CONFIG); // change this (dont need to)
      checkpointFile = new OffsetCheckpoint(checkpointDir, checkpointVersion, topic);
      checkpointFileCache.putAll(checkpointFile.read());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read checkpoints", e);
    }
    log.info("Successfully read checkpoints");

    addSchemaRegistryConfigsToClientProperties(config, consumerProps); // changed
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "MetadataEncoderStore-reader-" + this.topic);

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

    log.info("Metadata Encoder store reader thread starting consumer");
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
    List<TopicPartition> topicPartitions = Arrays.asList(this.topicPartition);
    this.consumer.assign(topicPartitions);

    // always seek checkpoint for all partitions
    for (final TopicPartition topicPartition : topicPartitions) {
      final Long checkpoint = checkpointFileCache.get(topicPartition);
      if (checkpoint != null) {
        log.info("Seeking to checkpoint {} for {}", checkpoint, topicPartition);
        consumer.seek(topicPartition, checkpoint);
      } else {
        log.info("Seeking to beginning for {}", topicPartition);
        consumer.seekToBeginning(Collections.singletonList(topicPartition));
      }
    }

    log.info("Initialized last consumed offset to {}", offsetInMetadataEncoderTopic);

    log.debug("Kafka store reader thread started");
  }

  public Map<TopicPartition, Long> checkpoints() {
    return checkpointFileCache;
  }

  public static void addSchemaRegistryConfigsToClientProperties(SchemaRegistryConfig config,
                                                                Properties props) {
    props.putAll(config.originalsWithPrefix("metadata.encoder."));
  }

  @Override
  public void doWork() {
    try {
      log.info("Doing work");
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
      resourceManagerStoreUpdateHandler.startBatch(records.count());
      for (ConsumerRecord<byte[], byte[]> record : records) {
        String messageKey = null;
        try {
          messageKey = new String(record.key(), StandardCharsets.UTF_8);  // Keys are just strings
        } catch (Exception e) {
          log.error("Failed to deserialize the key at offset " + record.offset(), e);
          continue;
        }

        if (messageKey.equals(noopKey)) {
          // If it's a noop, update local offset counter and do nothing else
          try {
            offsetUpdateLock.lock();
            offsetInMetadataEncoderTopic = record.offset();
            offsetReachedThreshold.signalAll();
          } finally {
            offsetUpdateLock.unlock();
          }
        } else {
          KeysetWrapper message = null;
          try {
            message = record.value() == null ? null
                    : keysetWrapperSerde.deserializer().deserialize(null, record.value());
          } catch (Exception e) {
            log.error("Failed to deserialize an encoder update at offset "
                    + record.offset(), e);
            continue;
          }
          try {
            log.trace("Applying update ("
                    + messageKey
                    + ","
                    + message
                    + ") to RM");
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            long offset = record.offset();
            long timestamp = record.timestamp();

            // Validate the update first
            CacheUpdateHandler.ValidationStatus status = this.resourceManagerStoreUpdateHandler.validateUpdate(
                    messageKey, message, tp, offset, timestamp);

            switch (status) {
              case SUCCESS:
                // If validation succeeds, process the update
                this.resourceManagerStoreUpdateHandler.handleUpdate(
                        messageKey,
                        message,
                        null,
                        tp,
                        offset,
                        timestamp);
                break;
              case ROLLBACK_FAILURE:
                // If validation fails and rollback is needed, write the old value back
                try {
                  ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                          topic,
                          record.key(),
                          null  // Since we don't have a local store, we can't rollback to old value
                  );
                  producer.send(producerRecord);
                  log.warn("Rollback invalid update to key {}", messageKey);
                } catch (KafkaException ke) {
                  log.error("Failed to recover from invalid update to key {}", messageKey, ke);
                }
                break;
              case IGNORE_FAILURE:
              default:
                log.warn("Ignore invalid update to key {}", messageKey);
                break;
            }

            // Keep track of the offset we've processed
            try {
              offsetUpdateLock.lock();
              offsetInMetadataEncoderTopic = record.offset();
              offsetReachedThreshold.signalAll();
            } finally {
              offsetUpdateLock.unlock();
            }
          } catch (Exception se) {
            log.error("Failed to add record from the Kafka topic"
                    + topic
                    + " to RM", se);
          }
        }
      }

      if (initialized.get()) {
        Map<TopicPartition, Long> offsets = Collections.singletonMap(
                new TopicPartition(topic, 0),
                offsetInMetadataEncoderTopic + 1
        );
        checkpointOffsets(offsets);
      }
      resourceManagerStoreUpdateHandler.endBatch(records.count());
    } catch (WakeupException we) {
      // do nothing because the thread is closing -- see shutdown()
    } catch (RecordTooLargeException rtle) {
      throw new IllegalStateException(
              "Consumer threw RecordTooLargeException. A schema has been written that "
                      + "exceeds the default maximum fetch size.", rtle);
    } catch (RuntimeException e) {
      log.error("KafkaStoreReader thread has died for an unknown reason.", e);
      throw new RuntimeException(e);
    }
  }

  private void checkpointOffsets(Map<TopicPartition, Long> offsets) {
    Map<TopicPartition, Long> newOffsets = offsets != null
            ? offsets
            : Collections.singletonMap(new TopicPartition(topic, 0), offsetInMetadataEncoderTopic + 1);
    checkpointFileCache.putAll(newOffsets);
    try {
      checkpointFile.write(checkpointFileCache);
    } catch (final IOException e) {
      log.warn("Failed to write offset checkpoint file to {}: {}", checkpointFile, e);
    }
  }

  @Override
  public void shutdown() {
    try {
      log.debug("Starting shutdown of KafkaStoreReaderThread.");

      super.initiateShutdown();
      if (consumer != null) {
        consumer.wakeup();
      }
      if (checkpointFile != null) {
        checkpointFile.close();
      }
      super.awaitShutdown();
      if (consumer != null) {
        consumer.close();
      }
      log.info("KafkaStoreReaderThread shutdown complete.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void waitUntilOffset(long offset, long timeout, TimeUnit timeUnit) throws StoreException {
    if (offset < 0) {
      throw new StoreException("KafkaStoreReaderThread can't wait for a negative offset.");
    }

    log.trace("Waiting to read offset {}. Currently at offset {}", offset, offsetInMetadataEncoderTopic);

    try {
      offsetUpdateLock.lock();
      long timeoutNs = TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
      while ((offsetInMetadataEncoderTopic < offset) && (timeoutNs > 0)) {
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

    if (offsetInMetadataEncoderTopic < offset) {
      throw new StoreTimeoutException(
              "MetadataEncoderStoreReaderThread failed to reach target offset within the timeout interval. "
                      + "targetOffset: " + offset + ", offsetReached: " + offsetInMetadataEncoderTopic
                      + ", timeout(ms): " + TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
    }
  }

  /* for testing purposes */
  public String getConsumerProperty(String key) {
    return this.consumerProps.getProperty(key);
  }
}
