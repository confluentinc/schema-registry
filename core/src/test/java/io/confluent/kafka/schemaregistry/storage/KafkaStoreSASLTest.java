/**
 * Copyright 2016 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.SASLClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// tests SASL with ZooKeeper and Kafka.
public class KafkaStoreSASLTest extends SASLClusterTestHarness {
  @Test
  public void testInitialization() {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitSASLStoreInstance(zkConnect,
            zkClient);
    kafkaStore.close();
  }

  @Test
  public void testDoubleInitialization() {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitSASLStoreInstance(zkConnect,
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
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitSASLStoreInstance(zkConnect,
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
}
