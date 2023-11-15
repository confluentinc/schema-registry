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

import io.confluent.kafka.schemaregistry.storage.exceptions.EntryTooLargeException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;

public class KafkaStore<K, V> implements Store<K, V> {

  private static final Logger log = LoggerFactory.getLogger(KafkaStore.class);

  private final String topic;
  private final int desiredReplicationFactor;
  private final String groupId;
  private final StoreUpdateHandler<K, V> storeUpdateHandler;
  private final Serializer<K, V> serializer;
  private final Store<K, V> localStore;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final CountDownLatch initLatch = new CountDownLatch(1);
  private final int initTimeout;
  private final int timeout;
  private final String bootstrapBrokers;
  private final boolean skipSchemaTopicValidation;
  private KafkaProducer<byte[], byte[]> producer;
  private KafkaStoreReaderThread<K, V> kafkaTopicReader;
  // Noop key is only used to help reliably determine last offset; reader thread ignores
  // messages with this key
  private final K noopKey;
  private volatile long lastWrittenOffset = -1L;
  private final SchemaRegistryConfig config;
  private final Lock leaderLock = new ReentrantLock();
  private final Lock lock = new ReentrantLock();

  public KafkaStore(SchemaRegistryConfig config,
                    StoreUpdateHandler<K, V> storeUpdateHandler,
                    Serializer<K, V> serializer,
                    Store<K, V> localStore,
                    K noopKey) throws SchemaRegistryException {
    this.topic = config.getString(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG);
    this.desiredReplicationFactor =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG);
    this.config = config;
    int port = KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(),
        config.interInstanceListenerName(),
        config.interInstanceProtocol()).getUri().getPort();
    this.groupId = config.getString(SchemaRegistryConfig.KAFKASTORE_GROUP_ID_CONFIG).isEmpty()
                   ? String.format("schema-registry-%s-%d",
                        config.getString(SchemaRegistryConfig.HOST_NAME_CONFIG), port)
                   : config.getString(SchemaRegistryConfig.KAFKASTORE_GROUP_ID_CONFIG);
    initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
    timeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
    this.storeUpdateHandler = storeUpdateHandler;
    this.serializer = serializer;
    this.localStore = localStore;
    this.noopKey = noopKey;
    this.bootstrapBrokers = config.bootstrapBrokers();
    this.skipSchemaTopicValidation =
        config.getBoolean(SchemaRegistryConfig.KAFKASTORE_TOPIC_SKIP_VALIDATION_CONFIG);

    log.info("Initializing KafkaStore with broker endpoints: {}", this.bootstrapBrokers);
  }

  @Override
  public void init() throws StoreInitializationException {
    if (initialized.get()) {
      throw new StoreInitializationException(
          "Illegal state while initializing store. Store was already initialized");
    }
    localStore.init();

    createOrVerifySchemaTopic();

    // set the producer properties and initialize a Kafka producer client
    Properties props = new Properties();
    addSchemaRegistryConfigsToClientProperties(this.config, props);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(ProducerConfig.ACKS_CONFIG, "-1");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
    props.put(ProducerConfig.RETRIES_CONFIG, 0); // Producer should not retry
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

    producer = new KafkaProducer<byte[], byte[]>(props);

    // start the background thread that subscribes to the Kafka topic and applies updates.
    // the thread must be created after the schema topic has been created.
    this.kafkaTopicReader =
        new KafkaStoreReaderThread<>(this.bootstrapBrokers, topic, groupId,
                                     this.storeUpdateHandler, serializer, this.localStore,
                                     this.producer, this.noopKey, this.initialized, this.config);
    this.kafkaTopicReader.start();

    try {
      waitUntilKafkaReaderReachesLastOffset(initTimeout);
    } catch (StoreException e) {
      throw new StoreInitializationException(e);
    }

    boolean isInitialized = initialized.compareAndSet(false, true);
    if (!isInitialized) {
      throw new StoreInitializationException("Illegal state while initializing store. Store "
                                             + "was already initialized");
    }
    this.storeUpdateHandler.cacheInitialized(new HashMap<>(kafkaTopicReader.checkpoints()));
    initLatch.countDown();
  }

  public static void addSchemaRegistryConfigsToClientProperties(SchemaRegistryConfig config,
                                                                Properties props) {
    props.putAll(config.originalsWithPrefix("kafkastore."));
  }



  private void createOrVerifySchemaTopic() throws StoreInitializationException {
    if (this.skipSchemaTopicValidation) {
      log.info("Skipping auto topic creation and verification");
      return;
    }

    Properties props = new Properties();
    addSchemaRegistryConfigsToClientProperties(this.config, props);
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

    try (AdminClient admin = AdminClient.create(props)) {
      //
      Set<String> allTopics = admin.listTopics().names().get(initTimeout, TimeUnit.MILLISECONDS);
      if (allTopics.contains(topic)) {
        verifySchemaTopic(admin);
      } else {
        createSchemaTopic(admin);
      }
    } catch (TimeoutException e) {
      throw new StoreInitializationException(
          "Timed out trying to create or validate schema topic configuration",
          e
      );
    } catch (InterruptedException | ExecutionException e) {
      throw new StoreInitializationException(
          "Failed trying to create or validate schema topic configuration",
          e
      );
    }
  }

  private void createSchemaTopic(AdminClient admin) throws StoreInitializationException,
                                                           InterruptedException,
                                                           ExecutionException,
                                                           TimeoutException {
    log.info("Creating schemas topic {}", topic);

    int numLiveBrokers = admin.describeCluster().nodes()
        .get(initTimeout, TimeUnit.MILLISECONDS).size();
    if (numLiveBrokers <= 0) {
      throw new StoreInitializationException("No live Kafka brokers");
    }

    int schemaTopicReplicationFactor = Math.min(numLiveBrokers, desiredReplicationFactor);
    if (schemaTopicReplicationFactor < desiredReplicationFactor) {
      log.warn("Creating the schema topic "
               + topic
               + " using a replication factor of "
               + schemaTopicReplicationFactor
               + ", which is less than the desired one of "
               + desiredReplicationFactor + ". If this is a production environment, it's "
               + "crucial to add more brokers and increase the replication factor of the topic.");
    }

    NewTopic schemaTopicRequest = new NewTopic(topic, 1, (short) schemaTopicReplicationFactor);
    Map topicConfigs = new HashMap(config.originalsWithPrefix("kafkastore.topic.config."));
    topicConfigs.put(
        TopicConfig.CLEANUP_POLICY_CONFIG,
        TopicConfig.CLEANUP_POLICY_COMPACT
    );
    schemaTopicRequest.configs(topicConfigs);
    try {
      admin.createTopics(Collections.singleton(schemaTopicRequest)).all()
          .get(initTimeout, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        // If topic already exists, ensure that it is configured correctly.
        verifySchemaTopic(admin);
      } else {
        throw e;
      }
    }
  }

  private void verifySchemaTopic(AdminClient admin) throws StoreInitializationException,
                                                           InterruptedException,
                                                           ExecutionException,
                                                           TimeoutException {
    log.info("Validating schemas topic {}", topic);

    Set<String> topics = Collections.singleton(topic);
    Map<String, TopicDescription> topicDescription = admin.describeTopics(topics)
        .all().get(initTimeout, TimeUnit.MILLISECONDS);

    TopicDescription description = topicDescription.get(topic);
    final int numPartitions = description.partitions().size();
    if (numPartitions != 1) {
      throw new StoreInitializationException("The schema topic " + topic + " should have only 1 "
                                             + "partition but has " + numPartitions);
    }

    if (description.partitions().get(0).replicas().size() < desiredReplicationFactor) {
      log.warn("The replication factor of the schema topic "
               + topic
               + " is less than the desired one of "
               + desiredReplicationFactor
               + ". If this is a production environment, it's crucial to add more brokers and "
               + "increase the replication factor of the topic.");
    }

    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

    Map<ConfigResource, Config> configs =
        admin.describeConfigs(Collections.singleton(topicResource)).all()
            .get(initTimeout, TimeUnit.MILLISECONDS);
    Config topicConfigs = configs.get(topicResource);
    String retentionPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
    if (retentionPolicy == null || !TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
      log.error("The retention policy of the schema topic {} is incorrect. "
                + "You must configure the topic to 'compact' cleanup policy to avoid Kafka "
                + "deleting your schemas after a week. "
                + "Refer to Kafka documentation for more details on cleanup policies", topic);

      throw new StoreInitializationException("The retention policy of the schema topic " + topic
                                             + " is incorrect. Expected cleanup.policy to be "
                                             + "'compact' but it is " + retentionPolicy);

    }
  }



  /**
   * Wait until the KafkaStore catches up to the last message in the Kafka topic.
   */
  public void waitUntilKafkaReaderReachesLastOffset(int timeoutMs) throws StoreException {
    long offsetOfLastMessage = getLatestOffset(timeoutMs);
    waitUntilKafkaReaderReachesOffset(offsetOfLastMessage, timeoutMs);
  }

  /**
   * Wait until the KafkaStore catches up to the last message for the given subject.
   */
  public void waitUntilKafkaReaderReachesLastOffset(String subject, int timeoutMs)
      throws StoreException {
    long lastOffset = lastOffset(subject);
    if (lastOffset == -1) {
      lastOffset = getLatestOffset(timeoutMs);
    }
    waitUntilKafkaReaderReachesOffset(lastOffset, timeoutMs);
  }

  /**
   * Wait until the KafkaStore catches up to the given offset in the Kafka topic.
   */
  private void waitUntilKafkaReaderReachesOffset(long offset, int timeoutMs) throws StoreException {
    log.info("Wait to catch up until the offset at {}", offset);
    kafkaTopicReader.waitUntilOffset(offset, timeoutMs, TimeUnit.MILLISECONDS);
    log.info("Reached offset at {}", offset);
  }

  public void markLastWrittenOffsetInvalid() {
    lastWrittenOffset = -1L;
  }

  @Override
  public V get(K key) throws StoreException {
    // Allow reads during intialization, such as referenced schemas
    return localStore.get(key);
  }

  @Override
  public V put(K key, V value) throws StoreTimeoutException, StoreException {
    assertInitialized();
    if (key == null) {
      throw new StoreException("Key should not be null");
    }
    V oldValue = get(key);

    // write to the Kafka topic
    ProducerRecord<byte[], byte[]> producerRecord = null;
    try {
      producerRecord =
          new ProducerRecord<byte[], byte[]>(topic, 0, this.serializer.serializeKey(key),
                                             value == null ? null : this.serializer.serializeValue(
                                                 value));
    } catch (SerializationException e) {
      throw new StoreException("Error serializing schema while creating the Kafka produce "
                               + "record", e);
    }

    boolean knownSuccessfulWrite = false;
    try {
      log.trace("Sending record to KafkaStore topic: {}", producerRecord);
      Future<RecordMetadata> ack = producer.send(producerRecord);
      RecordMetadata recordMetadata = ack.get(timeout, TimeUnit.MILLISECONDS);

      log.trace("Waiting for the local store to catch up to offset {}", recordMetadata.offset());
      this.lastWrittenOffset = recordMetadata.offset();
      if (key instanceof SubjectKey) {
        setLastOffset(((SubjectKey) key).getSubject(), recordMetadata.offset());
      }
      waitUntilKafkaReaderReachesOffset(recordMetadata.offset(), timeout);
      knownSuccessfulWrite = true;
    } catch (InterruptedException e) {
      throw new StoreException("Put operation interrupted while waiting for an ack from Kafka", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RecordTooLargeException) {
        throw new EntryTooLargeException("Put operation failed because entry is too large");
      } else {
        throw new StoreException("Put operation failed while waiting for an ack from Kafka", e);
      }
    } catch (TimeoutException e) {
      throw new StoreTimeoutException(
          "Put operation timed out while waiting for an ack from Kafka", e);
    } catch (KafkaException ke) {
      throw new StoreException("Put operation to Kafka failed", ke);
    } finally {
      if (!knownSuccessfulWrite) {
        markLastWrittenOffsetInvalid();
      }
    }
    return oldValue;
  }

  @Override
  public CloseableIterator<V> getAll(K key1, K key2) throws StoreException {
    assertInitialized();
    return localStore.getAll(key1, key2);
  }

  @Override
  public void putAll(Map<K, V> entries) throws StoreException {
    assertInitialized();
    // TODO: write to the Kafka topic as a batch
    for (Map.Entry<K, V> entry : entries.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public V delete(K key) throws StoreException {
    assertInitialized();
    // deleteSchemaVersion from the Kafka topic by writing a null value for the key
    return put(key, null);
  }

  @Override
  public CloseableIterator<K> getAllKeys() throws StoreException {
    assertInitialized();
    return localStore.getAllKeys();
  }

  @Override
  public void flush() throws StoreException {
    localStore.flush();
  }

  @Override
  public void close() {
    try {
      if (kafkaTopicReader != null) {
        kafkaTopicReader.shutdown();
      }
      if (producer != null) {
        producer.close();
        log.info("Kafka store producer shut down");
      }
      localStore.close();
      if (storeUpdateHandler != null) {
        storeUpdateHandler.close();
      }
      log.info("Kafka store shut down complete");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void waitForInit() throws InterruptedException {
    if (initLatch.getCount() > 0) {
      initLatch.await();
    }
  }

  public boolean initialized() {
    return initialized.get();
  }

  /**
   * For testing.
   */
  KafkaStoreReaderThread<K, V> getKafkaStoreReaderThread() {
    return this.kafkaTopicReader;
  }

  private void assertInitialized() throws StoreException {
    if (!initialized.get()) {
      throw new StoreException("Illegal state. Store not initialized yet");
    }
  }

  /**
   * Return the latest offset of the store topic.
   *
   * <p>The most reliable way to do so in face of potential Kafka broker failure is to produce
   * successfully to the Kafka topic and get the offset of the returned metadata.
   *
   * <p>If the most recent write to Kafka was successful (signaled by lastWrittenOffset >= 0),
   * immediately return that offset. Otherwise write a "Noop key" to Kafka in order to find the
   * latest offset.
   */
  private long getLatestOffset(int timeoutMs) throws StoreException {
    ProducerRecord<byte[], byte[]> producerRecord = null;

    if (this.lastWrittenOffset >= 0) {
      return this.lastWrittenOffset;
    }

    try {
      producerRecord =
          new ProducerRecord<byte[], byte[]>(topic, 0, this.serializer.serializeKey(noopKey), null);
    } catch (SerializationException e) {
      throw new StoreException("Failed to serialize noop key.", e);
    }

    try {
      log.trace("Sending Noop record to KafkaStore to find last offset.");
      Future<RecordMetadata> ack = producer.send(producerRecord);
      RecordMetadata metadata = ack.get(timeoutMs, TimeUnit.MILLISECONDS);
      this.lastWrittenOffset = metadata.offset();
      log.trace("Noop record's offset is {}", this.lastWrittenOffset);
      return this.lastWrittenOffset;
    } catch (Exception e) {
      throw new StoreException("Failed to write Noop record to kafka store.", e);
    }
  }

  public long lastOffset(String subject) {
    return lastWrittenOffset;
  }

  public void setLastOffset(String subject, long lastOffset) {
    this.lastWrittenOffset = lastOffset;
  }

  public Lock leaderLock() {
    return leaderLock;
  }

  public Lock lockFor(String subject) {
    return lock;
  }
}
