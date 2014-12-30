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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class KafkaStoreTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreTest.class);

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
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient);
    kafkaStore.close();
  }

  @Test
  public void testIncorrectInitialization() {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient);
    try {
      kafkaStore.init();
      fail("Kafka store repeated initialization should fail");
    } catch (StoreInitializationException e) {
      // this is expected
    }
    kafkaStore.close();
  }

  @Test
  public void testSimplePut() throws InterruptedException {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient);
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
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient,
                                                                                       inMemoryStore);
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

    // recreate kafka store
    kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect, zkClient, inMemoryStore);
    retrievedValue = null;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertEquals("Retrieved value should match entered value", value, retrievedValue);
    kafkaStore.close();
  }

  @Test
  public void testSimpleDelete() throws InterruptedException {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient);
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
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    Map<Long, String> idIndex = new HashMap<Long, String>();
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient,
                                                                                       inMemoryStore);
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
    // recreate kafka store
    kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect, zkClient, inMemoryStore);
    // verify that key still doesn't exist in the store
    retrievedValue = value;
    try {
      retrievedValue = kafkaStore.get(key);
    } catch (StoreException e) {
      fail("Kafka store get(Kafka) operation failed");
    }
    assertNull("Value should have been deleted", retrievedValue);
    kafkaStore.close();
  }
}
