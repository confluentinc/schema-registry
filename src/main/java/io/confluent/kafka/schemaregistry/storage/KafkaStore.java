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

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import kafka.client.ClientUtils;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataResponse;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaStore<K, V> implements Store<K, V> {

  private static final Logger log = LoggerFactory.getLogger(KafkaStore.class);
  private static final String CLIENT_ID = "schema-registry";
  private static final int SOCKET_BUFFER_SIZE = 4096;
  private static final long LATEST_OFFSET = -1;
  private static final int CONSUMER_ID = -1;


  private final String kafkaClusterZkUrl;
  private final String topic;
  private final String groupId;
  private final StoreUpdateHandler<K, V> storeUpdateHandler;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final Store<K, V> localStore;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final int timeout;
  private final Seq<Broker> brokerSeq;
  private KafkaProducer producer;
  private KafkaStoreReaderThread<K, V> kafkaTopicReader;

  public KafkaStore(SchemaRegistryConfig config,
                    StoreUpdateHandler<K, V> storeUpdateHandler,
                    Serializer<K> keySerializer,
                    Serializer<V> valueSerializer,
                    Store<K, V> localStore,
                    ZkClient zkClient) {
    this.kafkaClusterZkUrl =
        config.getString(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG);
    this.topic = config.getString(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG);
    this.groupId = String.format("schema-registry-%s-%d",
                                 config.getString(SchemaRegistryConfig.ADVERTISED_HOST_CONFIG),
                                 config.getInt(SchemaRegistryConfig.PORT_CONFIG));
    timeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
    this.storeUpdateHandler = storeUpdateHandler;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.localStore = localStore;
    // TODO: Do not use the commit interval until the decision on the embedded store is done
    int commitInterval = config.getInt(SchemaRegistryConfig.KAFKASTORE_COMMIT_INTERVAL_MS_CONFIG);
    this.kafkaTopicReader =
        new KafkaStoreReaderThread<K, V>(zkClient, kafkaClusterZkUrl, topic, groupId,
                                         Integer.MIN_VALUE, this.storeUpdateHandler, keySerializer,
                                         valueSerializer, this.localStore);
    this.brokerSeq = ZkUtils.getAllBrokersInCluster(zkClient);

  }

  @Override
  public void init() throws StoreInitializationException {
    if (initialized.get()) {
      throw new StoreInitializationException("Illegal state while initializing store. Store "
                                             + "was already initialized");
    }

    // set the producer properties
    List<Broker> brokers = JavaConversions.seqAsJavaList(brokerSeq);
    String bootstrapBrokers = "";
    for (int i = 0; i < brokers.size(); i++) {
      bootstrapBrokers += brokers.get(i).connectionString();
      if (i != (brokers.size() - 1)) {
        bootstrapBrokers += ",";
      }
    }
    // initialize a Kafka producer client
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(ProducerConfig.ACKS_CONFIG, "-1");
    producer = new KafkaProducer(props);

    // start the background thread that subscribes to the Kafka topic and applies updates
    kafkaTopicReader.start();

    try {
      waitUntilBootstrapCompletes();
    } catch (StoreException e) {
      throw new StoreInitializationException(e);
    }

    boolean isInitialized = initialized.compareAndSet(false, true);
    if (!isInitialized) {
      throw new StoreInitializationException("Illegal state while initializing store. Store "
                                             + "was already initialized");
    }
  }

  /**
   * Wait until the KafkaStore catches up to the last message in the Kafka topic.
   */
  public void waitUntilBootstrapCompletes() throws StoreException {
    long offsetOfLastMessage = getLatestOffsetOfKafkaTopic(timeout) - 1;
    log.info("Wait to catch up until the offset of the last message at " + offsetOfLastMessage);
    kafkaTopicReader.waitUntilOffset(offsetOfLastMessage, timeout, TimeUnit.MILLISECONDS);
    log.debug("Reached offset at " + offsetOfLastMessage);
  }

  @Override
  public V get(K key) throws StoreException {
    assertInitialized();
    return localStore.get(key);
  }

  @Override
  public void put(K key, V value) throws StoreException {
    assertInitialized();
    if (key == null) {
      throw new StoreException("Key should not be null");
    }
    // write to the Kafka topic
    ProducerRecord producerRecord = null;
    try {
      producerRecord = new ProducerRecord(topic, 0, keySerializer.toBytes(key),
                                          value == null ? null : valueSerializer.toBytes(value));
    } catch (SerializationException e) {
      throw new StoreException("Error serializing schema while creating the Kafka produce "
                               + "record", e);
    }
    Future<RecordMetadata> ack = producer.send(producerRecord);
    try {
      RecordMetadata recordMetadata = ack.get(timeout, TimeUnit.MILLISECONDS);
      log.trace("Waiting for the local store to catch up to offset " + recordMetadata.offset());
      kafkaTopicReader.waitUntilOffset(recordMetadata.offset(), timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new StoreException("Put operation interrupted while waiting for an ack from Kafka",
                               e);
    } catch (ExecutionException e) {
      throw new StoreException("Put operation failed while waiting for an ack from Kafka",
                               e);
    } catch (TimeoutException e) {
      throw new StoreException("Put operation timed out while waiting for an ack from Kafka",
                               e);
    }
  }

  @Override
  public Iterator<V> getAll(K key1, K key2) throws StoreException {
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
  public void delete(K key) throws StoreException {
    assertInitialized();
    // delete from the Kafka topic by writing a null value for the key
    put(key, null);
  }

  @Override
  public Iterator<K> getAllKeys() throws StoreException {
    return localStore.getAllKeys();
  }

  @Override
  public void close() {
    kafkaTopicReader.shutdown();
    log.debug("Kafka store reader thread shut down");
    producer.close();
    log.debug("Kafka store producer shut down");
    localStore.close();
    log.debug("Kafka store shut down complete");
  }

  private void assertInitialized() throws StoreException {
    if (!initialized.get()) {
      throw new StoreException("Illegal state. Store not initialized yet");
    }
  }

  /**
   * Get the latest offset of the Kafka topic that has a single partition.
   */
  private long getLatestOffsetOfKafkaTopic(int timeoutMs) throws StoreException {
    long start = System.currentTimeMillis();
    do {
      Set<String> topics = new HashSet<String>();
      scala.collection.mutable.Set<String> topicsScalaSet = JavaConversions.asScalaSet(topics);
      TopicMetadataResponse topicMetadataResponse =
          new kafka.javaapi.TopicMetadataResponse(
              ClientUtils.fetchTopicMetadata(topicsScalaSet, brokerSeq, CLIENT_ID, timeoutMs, 0));
      if (topicMetadataResponse.topicsMetadata().size() <= 0) {
        // topic doesn't exist yet
        return 0L;
      }

      TopicMetadata topicMetadata = topicMetadataResponse.topicsMetadata().get(0);
      Broker leader = topicMetadata.partitionsMetadata().get(0).leader();
      if (leader != null) {
        try {
          SimpleConsumer simpleConsumer = new SimpleConsumer(
              leader.host(), leader.port(), timeoutMs, SOCKET_BUFFER_SIZE, CLIENT_ID);
          TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);
          return simpleConsumer.earliestOrLatestOffset(
              topicAndPartition, LATEST_OFFSET, CONSUMER_ID);
        } catch (Exception e) {
          log.warn("Exception while fetch the latest offset", e);
        }
      }

      try {
        // backoff a bit
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // let it go
      }
    } while (System.currentTimeMillis() - start <= timeoutMs);

    throw new StoreException(
        String.format("Can't fetch latest offset of Kafka topic %s after %d ms", topic, timeoutMs));
  }
}
