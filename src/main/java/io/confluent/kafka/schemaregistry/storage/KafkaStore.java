package io.confluent.kafka.schemaregistry.storage;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.schemaregistry.storage.serialization.ZkStringSerializer;
import kafka.cluster.Broker;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.ZookeeperConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

public class KafkaStore<K, V> implements Store<K, V> {

  private static final Logger log = LoggerFactory.getLogger(KafkaStore.class);

  private final String kafkaClusterZkUrl;
    private final int zkSessionTimeoutMs;
    private final String topic;
    private final String groupId;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Store<K, V> localStore;
    private final Random random = new Random(System.currentTimeMillis());
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final int timeout;
    private KafkaProducer producer;
    private ConsumerConnector consumer;
    private KafkaStoreReaderThread<K, V> kafkaTopicReader;

    public KafkaStore(KafkaStoreConfig storeConfig, Serializer<K> keySerializer,
        Serializer<V> valueSerializer, Store<K, V> localStore) {
        this.kafkaClusterZkUrl =
            storeConfig.getString(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG);
        this.zkSessionTimeoutMs =
            storeConfig.getInt(KafkaStoreConfig.KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);
        this.topic = storeConfig.getString(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG);
        this.groupId = "KafkaStore-" + topic + "-bootstrap-" + random.nextInt(1000000);
        timeout = storeConfig.getInt(KafkaStoreConfig.KAFKASTORE_TIMEOUT_CONFIG);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.localStore = localStore;
        this.kafkaTopicReader =
            new KafkaStoreReaderThread<K, V>(kafkaClusterZkUrl, topic, groupId, keySerializer,
                valueSerializer, this.localStore);
    }

    @Override public void init() throws StoreInitializationException {
        if (initialized.get()) {
            throw new StoreInitializationException("Illegal state while initializing store. Store "
                + "was already initialized");
        }
        ZkClient zkClient = new ZkClient(kafkaClusterZkUrl, zkSessionTimeoutMs, zkSessionTimeoutMs,
            new ZkStringSerializer());
        // set the producer properties
        List<Broker> brokers =
            JavaConversions.seqAsJavaList(ZkUtils.getAllBrokersInCluster(zkClient));
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
        // bootstrap the embedded local store by consuming data from the backing Kafka topic
        // since time 0
        Properties consumerProps = new Properties();
        consumerProps.put("group.id", groupId);
        consumerProps.put("auto.offset.reset", "smallest");
        consumerProps.put("zookeeper.connect", kafkaClusterZkUrl);
        consumerProps.put("consumer.timeout.ms", "2000");
        consumer = new ZookeeperConsumerConnector(new ConsumerConfig(consumerProps));
        Map<String, Integer> kafkaStreamConfig = new HashMap<String, Integer>();
        kafkaStreamConfig.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams =
            consumer.createMessageStreams(kafkaStreamConfig);
        List<KafkaStream<byte[], byte[]>> streamsForTheLogTopic = streams.get(topic);
        // there should be only one kafka partition and hence only one stream
        if (streamsForTheLogTopic != null && streamsForTheLogTopic.size() != 1) {
            throw new IllegalArgumentException("Unable to subscribe to the Kafka topic " + topic +
                " backing this data store. Topic may not exist.");
        }
        KafkaStream<byte[], byte[]> stream = streamsForTheLogTopic.get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        try {
            while (iterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
                byte[] messageBytes = messageAndMetadata.message();
              V message = null;
              try {
                message = messageBytes == null ? null : valueSerializer.fromBytes(messageBytes);
              } catch (SerializationException e) {
                throw new StoreInitializationException("Failed to deserialize the schema", e);
              }
              K messageKey = null;
              try {
                messageKey = keySerializer.fromBytes(messageAndMetadata.key());
              } catch (SerializationException e) {
                throw new StoreInitializationException("Failed to deserialize the schema key", e);
              }
              try {
                  log.trace("Applying update (" + messageKey + "," + message + ") to the " +
                            "local store during bootstrap");
                    if (message == null) {
                        localStore.delete(messageKey);
                    } else {
                        localStore.put(messageKey, message);
                    }
                } catch (StoreException se) {
                    throw new StoreInitializationException("Failed to add record from the Kafka topic" +
                        topic + " the local store");
                }
            }
        } catch (ConsumerTimeoutException cte) {
            // do nothing
        } finally {
            // the consumer will checkpoint it's offset in zookeeper, so the background thread will
            // continue from where the bootstrap consumer left off
            consumer.shutdown();
          log.info("Kafka store bootstrap from the log " + this.topic + " is complete. Now " +
                   "switching to live update");
        }
        // start the background thread that subscribes to the Kafka topic and applies updates
        kafkaTopicReader.start();
        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new StoreInitializationException("Illegal state while initializing store. Store "
                + "was already initialized");
        }
    }

    @Override public V get(K key) throws StoreException {
        assertInitialized();
        return localStore.get(key);
    }

    @Override public void put(K key, V value) throws StoreException {
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
            ack.get(timeout, TimeUnit.MILLISECONDS);
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

    @Override public Iterator<V> getAll(K key1, K key2) throws StoreException {
        assertInitialized();
        return localStore.getAll(key1, key2);
    }

    @Override public void putAll(Map<K, V> entries) throws StoreException {
        assertInitialized();
        // TODO: write to the Kafka topic as a batch
        for (Map.Entry<K, V> entry : entries.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override public void delete(K key) throws StoreException {
        assertInitialized();
        // delete from the Kafka topic by writing a null value for the key
        put(key, null);
    }

    @Override public void close() {
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
}
