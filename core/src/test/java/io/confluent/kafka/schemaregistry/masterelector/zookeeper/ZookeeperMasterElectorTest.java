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
package io.confluent.kafka.schemaregistry.masterelector.zookeeper;

import io.confluent.common.utils.zookeeper.ZkUtils;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.id.ZookeeperIdGenerator;
import io.confluent.kafka.schemaregistry.storage.serialization.ZkStringSerializer;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// Tests that are specific to the ZookeeperMasterElector. See MasterElectorTest for general tests
// covering all MasterElectors
public class ZookeeperMasterElectorTest extends ClusterTestHarness {
  private static final int ID_BATCH_SIZE =
      ZookeeperIdGenerator.ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
  private static final String ZK_ID_COUNTER_PATH =
      "/schema_registry" + ZookeeperIdGenerator.ZOOKEEPER_SCHEMA_ID_COUNTER;

  @Test
  /**
   * If the zk id counter used to help hand out unique ids is lower than the lowest id in the
   * kafka store, KafkaSchemaRegistry should still do the right thing and continue to hand out
   * increasing ids when new schemas are registered.
   */
  public void testIncreasingIdZkResetLow() throws Exception {
    // create schema registry instance 1
    final RestApp restApp1 = new RestApp(choosePort(),
                                         zkConnect, KAFKASTORE_TOPIC);
    restApp1.start();
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(ID_BATCH_SIZE);
    String subject = "testSubject";

    Set<Integer> ids = new HashSet<Integer>();
    int maxId = -1;
    for (int i = 0; i < ID_BATCH_SIZE / 2; i++) {
      int newId = restApp1.restClient.registerSchema(schemas.get(i), subject);
      ids.add(newId);

      // Sanity check - ids should be increasing
      assertTrue(newId > maxId);
      maxId = newId;
    }

    // Overwrite zk id counter to 0
    final ZkClient zkClient = new ZkClient(zkConnect, 10000, 10000, new ZkStringSerializer());
    int zkIdCounter = getZkIdCounter(zkClient);
    assertEquals(ID_BATCH_SIZE, zkIdCounter); // sanity check
    ZkUtils.updatePersistentPath(zkClient, ZK_ID_COUNTER_PATH, "0");

    // Make sure ids are still increasing
    String anotherSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    int newId = restApp1.restClient.registerSchema(anotherSchema, subject);
    assertTrue("Next assigned id should be greater than all previous.", newId > maxId);
    maxId = newId;

    // Add another schema registry and trigger reelection
    final RestApp restApp2 = new RestApp(choosePort(),
                                         zkConnect, KAFKASTORE_TOPIC);
    restApp2.start();
    restApp1.stop();
    Callable<Boolean> electionComplete = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return restApp2.isMaster();
      }
    };
    TestUtils.waitUntilTrue(electionComplete, 15000,
                            "Schema registry instance 2 should become the master");
    // Reelection should have triggered zk id to update to the next batch
    assertEquals("Zk counter is not the expected value.",
                 2 * ID_BATCH_SIZE, getZkIdCounter(zkClient));

    // Overwrite zk id counter again, then register another batch to trigger id batch update
    // (meanwhile verifying that ids continue to increase)
    ZkUtils.updatePersistentPath(zkClient, ZK_ID_COUNTER_PATH, "0");
    schemas = TestUtils.getRandomCanonicalAvroString(ID_BATCH_SIZE);
    for (int i = 0; i < ID_BATCH_SIZE; i++) {
      newId = restApp2.restClient.registerSchema(schemas.get(i), subject);
      ids.add(newId);

      // Sanity check - ids should be increasing
      assertTrue("new id " + newId + " should be greater than previous max " + maxId,
                 newId > maxId);
      maxId = newId;
    }

    // We just wrote another batch worth of schemas, so zk counter should jump
    assertEquals("Zk counter is not the expected value.",
                 3 * ID_BATCH_SIZE, getZkIdCounter(zkClient));
  }

  @Test
  /**
   * If there is no schema data in the kafka, but there is id data in zookeeper when a SchemaRegistry
   * instance is booted up, newly assigned ids should be greater than whatever is in zookeeper.
   *
   * Strange preexisting values in zk id counter path should be dealt with gracefully.
   * I.e. regardless of initial value, after zk id counter is updated
   * it should be a multiple of if ID_BATCH_SIZE.
   */
  public void testIdBehaviorWithZkWithoutKafka() throws Exception {
    // Overwrite the value in zk
    final ZkClient zkClient = new ZkClient(zkConnect, 10000, 10000, new ZkStringSerializer());
    int weirdInitialCounterValue = ID_BATCH_SIZE - 1;
    ZkUtils.createPersistentPath(zkClient, ZK_ID_COUNTER_PATH, "" + weirdInitialCounterValue);

    // Check that zookeeper id counter is updated sensibly during SchemaRegistry bootstrap process
    final RestApp restApp = new RestApp(choosePort(),
                                         zkConnect, KAFKASTORE_TOPIC);
    restApp.start();
    assertEquals("", 2 * ID_BATCH_SIZE, getZkIdCounter(zkClient));
  }

  @Test
  /**
   * Verify correct id allocation when a SchemaRegistry instance is initialized, and there is
   * preexisting data in the kafkastore, but no zookeeper id counter node.
   */
  public void testIdBehaviorWithoutZkWithKafka() throws Exception {

    // Pre-populate kafkastore with a few schemas
    int numSchemas = 2;
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(numSchemas);
    String subject = "testSubject";
    Set<Integer> ids = new HashSet<Integer>();
    RestApp restApp = new RestApp(choosePort(), zkConnect, KAFKASTORE_TOPIC);
    restApp.start();
    for (String schema: schemas) {
      int id = restApp.restClient.registerSchema(schema, subject);
      ids.add(id);
      waitUntilIdExists(restApp.restClient, id, "Expected id to be available.");
    }
    restApp.stop();

    // Sanity check id counter then remove it
    int zkIdCounter = getZkIdCounter(zkClient);
    assertEquals("Incorrect ZK id counter.", ID_BATCH_SIZE, zkIdCounter);
    zkClient.delete(ZK_ID_COUNTER_PATH);

    // start up another app instance and verify zk id node
    restApp = new RestApp(choosePort(), zkConnect, KAFKASTORE_TOPIC);
    restApp.start();
    zkIdCounter = getZkIdCounter(zkClient);
    assertEquals("ZK id counter was incorrectly initialized.", 2 * ID_BATCH_SIZE, zkIdCounter);
    restApp.stop();
  }

  @Test
  /** Verify expected value of zk schema id counter when schema registry starts up. */
  public void testZkCounterOnStartup() throws Exception {
    RestApp restApp = new RestApp(choosePort(), zkConnect, KAFKASTORE_TOPIC);
    restApp.start();

    int zkIdCounter = getZkIdCounter(zkClient);
    assertEquals("Initial value of ZooKeeper id counter is incorrect.", ID_BATCH_SIZE, zkIdCounter);

    restApp.stop();
  }

  private static int getZkIdCounter(ZkClient zkClient) {
    return Integer.valueOf(ZkUtils.readData(
        zkClient, ZK_ID_COUNTER_PATH).getData());
  }

  private void waitUntilIdExists(final RestService restService, final int expectedId, String errorMsg) {
    Callable<Boolean> canGetSchemaById = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          restService.getId(expectedId);
          return true;
        } catch (RestClientException e) {
          return false;
        }
      }
    };
    TestUtils.waitUntilTrue(canGetSchemaById, 15000, errorMsg);
  }
}
