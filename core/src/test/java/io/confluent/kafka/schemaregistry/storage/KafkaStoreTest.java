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

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.TestUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
  public void testDoubleInitialization() {
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
      try {
        kafkaStore.put(key, value);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
      }
      String retrievedValue = null;
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
    } finally {
      kafkaStore.close();
    }
  }

  // TODO: This requires fix for https://issues.apache.org/jira/browse/KAFKA-1788
//  @Test
//  public void testPutRetries() throws InterruptedException {
//    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
//                                                                                       zkClient);
//    String key = "Kafka";
//    String value = "Rocks";
//    try {
//      kafkaStore.put(key, value);
//    } catch (StoreException e) {
//      fail("Kafka store put(Kafka, Rocks) operation failed");
//    }
//    String retrievedValue = null;
//    try {
//      retrievedValue = kafkaStore.get(key);
//    } catch (StoreException e) {
//      fail("Kafka store get(Kafka) operation failed");
//    }
//    assertEquals("Retrieved value should match entered value", value, retrievedValue);
//    // stop the Kafka servers
//    for (KafkaServer server : servers) {
//      server.shutdown();
//    }
//    try {
//      kafkaStore.put(key, value);
//      fail("Kafka store put(Kafka, Rocks) operation should fail");
//    } catch (StoreException e) {
//      // expected since the Kafka producer will run out of retries
//    }
//    kafkaStore.close();
//  }

  @Test
  public void testSimpleGetAfterFailure() throws InterruptedException {
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient,
                                                                                       inMemoryStore);
    String key = "Kafka";
    String value = "Rocks";
    String retrievedValue = null;
    try {
      try {
        kafkaStore.put(key, value);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
      }
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
    } finally {
      kafkaStore.close();
    }

    // recreate kafka store
    kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect, zkClient, inMemoryStore);
    try {
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
    } finally {
      kafkaStore.close();
    }
  }

  @Test
  public void testSimpleDelete() throws InterruptedException {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient);
    String key = "Kafka";
    String value = "Rocks";
    try {
      try {
        kafkaStore.put(key, value);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
      }
      String retrievedValue = null;
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
      try {
        kafkaStore.delete(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store delete(Kafka) operation failed", e);
      }
      // verify that value is deleted
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertNull("Value should have been deleted", retrievedValue);
    } finally {
      kafkaStore.close();
    }
  }

  @Test
  public void testDeleteAfterRestart() throws InterruptedException {
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
                                                                                       zkClient,
                                                                                       inMemoryStore);
    String key = "Kafka";
    String value = "Rocks";
    try {
      try {
        kafkaStore.put(key, value);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
      }
      String retrievedValue = null;
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
      // delete the key
      try {
        kafkaStore.delete(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store delete(Kafka) operation failed", e);
      }
      // verify that key is deleted
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
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
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertNull("Value should have been deleted", retrievedValue);
    } finally {
      kafkaStore.close();
    }
  }

  @Test
  public void testGetBrokerEndpointsSinglePlaintext() {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
            zkClient);
    Seq<Broker> brokersSeq = zkUtils.getAllBrokersInCluster();
    List<Broker> brokersList = JavaConversions.seqAsJavaList(brokersSeq);

    Iterator<EndPoint> endpoints =
            JavaConversions.asJavaCollection(brokersList.get(0).endPoints().values()).iterator();
    String expectedEndpoint = endpoints.next().connectionString();

    assertEquals("Expected one PLAINTEXT endpoint for localhost", expectedEndpoint,
            KafkaStore.getBrokerEndpoints(brokersList));
  }

  @Test(expected = ConfigException.class)
  public void testGetBrokerEndpointsEmpty() {
    KafkaStore.getBrokerEndpoints(new ArrayList<Broker>());
  }

  /**
   * This test creates brokers with different security protocols. This scenario
   * where different brokers in the same cluster support different security endpoints wouldn't exist.
   * However, this setup creates the needed test scenario for getBrokerEndpoints().
   */
  @Test
  public void testGetBrokerEndpointsMixed() throws IOException {
    List<Broker> brokersList = new ArrayList<Broker>(3);
    brokersList.add(new Broker(0, "localhost", TestUtils.RandomPort(), SecurityProtocol.PLAINTEXT));
    brokersList.add(new Broker(1, "localhost1", TestUtils.RandomPort(), SecurityProtocol.PLAINTEXT));
    brokersList.add(new Broker(2, "localhost2", TestUtils.RandomPort(), SecurityProtocol.SASL_PLAINTEXT));
    brokersList.add(new Broker(3, "localhost3", TestUtils.RandomPort(), SecurityProtocol.SSL));

    String endpointsString = KafkaStore.getBrokerEndpoints(brokersList);
    String[] endpoints = endpointsString.split(",");
    assertEquals("Expected a different number of endpoints.", brokersList.size() - 1, endpoints.length);
    for (String endpoint : endpoints) {
      if (endpoint.contains("localhost3")) {
        assertTrue("Endpoint must be a SSL endpoint.", endpoint.contains("SSL://"));
      } else {
        assertTrue("Endpoint must be a PLAINTEXT endpoint.", endpoint.contains("PLAINTEXT://"));
      }
    }
  }
}
