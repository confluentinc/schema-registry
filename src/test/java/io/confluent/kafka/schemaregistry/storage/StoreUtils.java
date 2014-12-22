package io.confluent.kafka.schemaregistry.storage;

import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.StringSerializer;

import static org.junit.Assert.fail;

public class StoreUtils {

  /**
   * Get a new instance of KafkaStore and initialize it.
   * @return
   */
  public static KafkaStore<String, String> createAndInitKafkaStoreInstance(String zkConnect,
                                                                           ZkClient zkClient) {
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    return createAndInitKafkaStoreInstance(zkConnect, zkClient, inMemoryStore);
  }

  /**
   * Get a new instance of KafkaStore and initialize it.
   * @return
   */
  public static KafkaStore<String, String> createAndInitKafkaStoreInstance(String zkConnect,
                                                                           ZkClient zkClient,
                                                                           Store<String, String> inMemoryStore) {
    Properties props = new Properties();
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
    KafkaStoreConfig storeConfig = new KafkaStoreConfig(props);
    KafkaStore<String, String> kafkaStore = new KafkaStore<String, String>(storeConfig,
                                                                           StringSerializer.INSTANCE,
                                                                           StringSerializer.INSTANCE,
                                                                           inMemoryStore,
                                                                           zkClient);
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    return kafkaStore;
  }

}
