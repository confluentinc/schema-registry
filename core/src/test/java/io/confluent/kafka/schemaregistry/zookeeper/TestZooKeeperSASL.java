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

package io.confluent.kafka.schemaregistry.zookeeper;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.ZkSASLClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.KafkaStore;
import io.confluent.kafka.schemaregistry.storage.StoreUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestZooKeeperSASL extends ZkSASLClusterTestHarness {
  @Test
  public void testKafkaStoreInitialization() {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitZKSASLKafkaStoreInstance(zkConnect,
            zkClient, false);
    kafkaStore.close();
  }

  @Test
  // borrowed from MasterElectorTest.java.
  public void testRestAppAndKafkaStore() throws Exception {
    RestApp restApp = new RestApp(choosePort(), zkConnect, KAFKASTORE_TOPIC);
    restApp.start();

    int zkIdCounter = MasterElectorTest.getZkIdCounter(zkClient);
    assertEquals("Initial value of ZooKeeper id counter is incorrect.", MasterElectorTest.ID_BATCH_SIZE, zkIdCounter);

    restApp.stop();
  }

  // IMPORTANT: ideally we could write tests that test SchemaRegistryConfig.ZOOKEEPER_SET_ACL_CONFIG. However,
  // this is impossible because the client ZooKeeper Jaas section is the same for the embedded Kafka cluster
  // and the schema registry. Meaning, the schema registry and kafka use the same principal. As such, a scenario
  // where the schema registry does not have the right authorization is impossible. See `ZkSASLClusterTestHarness.java`
  // for an explanation for why kafka and the schema registry share the same principal.
}
