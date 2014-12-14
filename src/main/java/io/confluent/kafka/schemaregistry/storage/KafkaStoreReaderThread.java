package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.ZookeeperConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.ShutdownableThread;

import java.util.*;

public class KafkaStoreReaderThread<K, V> extends ShutdownableThread {
    private final String kafkaClusterZkUrl;
    private final String topic;
    private final String groupId;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Random random = new Random(System.currentTimeMillis());
    private final Store<K, V> localStore;
    private final long commitInterval;
    private ConsumerIterator<byte[], byte[]> consumerIterator;
    private ConsumerConnector consumer;

    public KafkaStoreReaderThread(String kafkaClusterZkUrl,
                                  String topic,
                                  String groupId,
                                  Serializer<K> keySerializer,
                                  Serializer<V> valueSerializer,
                                  Store<K, V> localStore) {
        super("kafka-store-reader-thread-" + topic, false);  // this thread is not interruptible
        this.kafkaClusterZkUrl = kafkaClusterZkUrl;
        this.topic = topic;
        this.groupId = groupId;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.localStore = localStore;
        this.commitInterval = 5000;
    }

    @Override
    public void start() {
        super.start();
        Properties consumerProps = new Properties();
        consumerProps.put("group.id", this.groupId);
        consumerProps.put("client.id", "KafkaStore-reader-" + this.topic);
        consumerProps.put("zookeeper.connect", kafkaClusterZkUrl);
        consumerProps.put("auto.offset.reset", "smallest");
        consumerProps.put("auto.commit.enable", "false");
        consumerProps.put("auto.commit.interval.ms", String.valueOf(commitInterval));
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
        consumerIterator = stream.iterator();
        System.out.println("Thread started with consumer properties " + consumerProps.toString());
    }

    @Override
    public void doWork() {
        long lastCommitTime = Long.MIN_VALUE;
        while (consumerIterator != null && consumerIterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = consumerIterator.next();
            byte[] messageBytes = messageAndMetadata.message();
            V message = messageBytes == null ? null : valueSerializer.fromBytes(messageBytes);
            K messageKey = keySerializer.fromBytes(messageAndMetadata.key());
            try {
                System.out.println("Applying update (" + messageKey + "," + message + ") to the " +
                    "local store");
                if (message == null) {
                    localStore.delete(messageKey);
                } else {
                    localStore.put(messageKey, message);
                }
                if (System.currentTimeMillis() - lastCommitTime > commitInterval) {
                    consumer.commitOffsets(true);
                }
            } catch (StoreException se) {
                /**
                 * TODO: maybe retry for a configurable amount before logging a failure?
                 * TODO: maybe fail all subsequent operations of the store if retries fail
                 * Only 2 operations make sense if this happens -
                 * 1. Restart the store hoping that it works subsequently
                 * 2. Look into the issue manually
                 */
                System.err.println("Failed to add record from the Kafka topic" +
                    topic + " the local store");
            }
        }
    }

    @Override
    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }
        if (localStore != null) {
            localStore.close();
        }
        super.shutdown();
    }
}
