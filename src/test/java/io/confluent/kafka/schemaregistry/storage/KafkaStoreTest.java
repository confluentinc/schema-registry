package io.confluent.kafka.schemaregistry.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.StringSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class KafkaStoreTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreTest.class);

  private String topic = "_schemas";

  @Before
  public void setup() {
    log.debug("Zk conn url = " + zkConnect);
  }

  @After
  public void teardown() {
    log.debug("Shutting down");
  }

  @Test
  public void testInitialization() {
    Properties props = new Properties();
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, topic);
    KafkaStoreConfig storeConfig = new KafkaStoreConfig(props);
    StringSerializer stringSerializer = new StringSerializer();
    KafkaStore<String, String> kafkaStore = new KafkaStore<String, String>(storeConfig,
                                                                           stringSerializer,
                                                                           stringSerializer,
                                                                           new InMemoryStore<String, String>());
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
  }

  @Test
  public void testIncorrectInitialization() {
    Properties props = new Properties();
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, topic);
    KafkaStoreConfig storeConfig = new KafkaStoreConfig(props);
    StringSerializer stringSerializer = new StringSerializer();
    KafkaStore<String, String> kafkaStore = new KafkaStore<String, String>(storeConfig,
                                                                           stringSerializer,
                                                                           stringSerializer,
                                                                           new InMemoryStore<String, String>());
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    try {
      kafkaStore.init();
      fail("Kafka store repeated initialization should fail");
    } catch (StoreInitializationException e) {
      // this is expected
    }
  }

  @Test
  public void testSimplePut() throws InterruptedException {
    Properties props = new Properties();
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, topic);
    KafkaStoreConfig storeConfig = new KafkaStoreConfig(props);
    StringSerializer stringSerializer = new StringSerializer();
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    KafkaStore<String, String> kafkaStore = new KafkaStore<String, String>(storeConfig,
                                                                           stringSerializer,
                                                                           stringSerializer,
                                                                           inMemoryStore);
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    String key = "Kafka";
    String value = "Rocks";
    try {
      kafkaStore.put(key, value);
    } catch (StoreException e) {
      fail("Kafka store put(Kafka, Rocks) operation failed");
    }
    String retrievedValue = null;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertEquals("Retrieved value should match entered value", value, retrievedValue);
    kafkaStore.close();
  }

  @Test
  public void testSimpleGetAfterFailure() throws InterruptedException {
    Properties props = new Properties();
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, topic);
    KafkaStoreConfig storeConfig = new KafkaStoreConfig(props);
    StringSerializer stringSerializer = new StringSerializer();
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    KafkaStore<String, String> kafkaStore = new KafkaStore<String, String>(storeConfig,
                                                                           stringSerializer,
                                                                           stringSerializer,
                                                                           inMemoryStore);
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    String key = "Kafka";
    String value = "Rocks";
    try {
      kafkaStore.put(key, value);
    } catch (StoreException e) {
      fail("Kafka store put(Kafka, Rocks) operation failed");
    }
    String retrievedValue = null;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertEquals("Retrieved value should match entered value", value, retrievedValue);
    kafkaStore.close();
    inMemoryStore.close();
    // recreate kafka store
    kafkaStore = new KafkaStore<String, String>(storeConfig, stringSerializer, stringSerializer,
                                                inMemoryStore);
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    retrievedValue = null;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertEquals("Retrieved value should match entered value", value, retrievedValue);
    kafkaStore.close();
    inMemoryStore.close();
  }

  @Test
  public void testSimpleDelete() throws InterruptedException {
    Properties props = new Properties();
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, topic);
    KafkaStoreConfig storeConfig = new KafkaStoreConfig(props);
    StringSerializer stringSerializer = new StringSerializer();
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    KafkaStore<String, String> kafkaStore = new KafkaStore<String, String>(storeConfig,
                                                                           stringSerializer,
                                                                           stringSerializer,
                                                                           inMemoryStore);
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    String key = "Kafka";
    String value = "Rocks";
    try {
      kafkaStore.put(key, value);
    } catch (StoreException e) {
      fail("Kafka store put(Kafka, Rocks) operation failed");
    }
    String retrievedValue = null;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertEquals("Retrieved value should match entered value", value, retrievedValue);
    try {
      kafkaStore.delete(key);
    } catch (StoreException e) {
      fail("Kafka store delete(Kafka) operation failed");
    }
    // verify that value is deleted
    retrievedValue = value;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertNull("Value should have been deleted", retrievedValue);
    kafkaStore.close();
  }

  @Test
  public void testDeleteAfterRestart() throws InterruptedException {
    Properties props = new Properties();
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, topic);
    KafkaStoreConfig storeConfig = new KafkaStoreConfig(props);
    StringSerializer stringSerializer = new StringSerializer();
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    KafkaStore<String, String> kafkaStore = new KafkaStore<String, String>(storeConfig,
                                                                           stringSerializer,
                                                                           stringSerializer,
                                                                           inMemoryStore);
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    String key = "Kafka";
    String value = "Rocks";
    try {
      kafkaStore.put(key, value);
    } catch (StoreException e) {
      fail("Kafka store put(Kafka, Rocks) operation failed");
    }
    String retrievedValue = null;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertEquals("Retrieved value should match entered value", value, retrievedValue);
    // delete the key
    try {
      kafkaStore.delete(key);
    } catch (StoreException e) {
      fail("Kafka store delete(Kafka) operation failed");
    }
    // verify that key is deleted
    retrievedValue = value;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertNull("Value should have been deleted", retrievedValue);
    kafkaStore.close();
    inMemoryStore.close();
    // recreate kafka store
    kafkaStore = new KafkaStore<String, String>(storeConfig, stringSerializer, stringSerializer,
                                                inMemoryStore);
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    // verify that key still doesn't exist in the store
    retrievedValue = value;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertNull("Value should have been deleted", retrievedValue);
    kafkaStore.close();
    inMemoryStore.close();
  }
}
