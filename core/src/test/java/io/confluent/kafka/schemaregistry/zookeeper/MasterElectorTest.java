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
package io.confluent.kafka.schemaregistry.zookeeper;

import io.confluent.common.utils.zookeeper.ZkUtils;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.serialization.ZkStringSerializer;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel.FORWARD;
import static io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel.NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MasterElectorTest extends ClusterTestHarness {
  static final int ID_BATCH_SIZE =
      KafkaSchemaRegistry.ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
  private static final String ZK_ID_COUNTER_PATH =
      "/schema_registry" + KafkaSchemaRegistry.ZOOKEEPER_SCHEMA_ID_COUNTER;

  @Test
  public void testAutoFailover() throws Exception {
    final String subject = "testTopic";
    final String configSubject = "configTopic";
    List<String> avroSchemas = TestUtils.getRandomCanonicalAvroString(4);

    // create schema registry instance 1
    final RestApp restApp1 = new RestApp(choosePort(),
                                         zkConnect, KAFKASTORE_TOPIC);
    restApp1.start();

    // create schema registry instance 2
    final RestApp restApp2 = new RestApp(choosePort(),
                                         zkConnect, KAFKASTORE_TOPIC);
    restApp2.start();
    assertTrue("Schema registry instance 1 should be the master", restApp1.isMaster());
    assertFalse("Schema registry instance 2 shouldn't be the master", restApp2.isMaster());
    assertEquals("Instance 2's master should be instance 1",
                 restApp1.myIdentity(), restApp2.masterIdentity());

    // test registering a schema to the master and finding it on the expected version
    final String firstSchema = avroSchemas.get(0);
    final int firstSchemaExpectedId = 1;
    TestUtils.registerAndVerifySchema(restApp1.restClient, firstSchema, firstSchemaExpectedId,
                                      subject);
    // the newly registered schema should be eventually readable on the non-master
    verifyIdAndSchema(restApp2.restClient, firstSchemaExpectedId, firstSchema,
                      "Registered schema should be found on the non-master");

    // test registering a schema to the non-master and finding it on the expected version
    final String secondSchema = avroSchemas.get(1);
    final int secondSchemaExpectedId = 2;
    final int secondSchemaExpectedVersion = 2;
    assertEquals("Registering a new schema to the non-master should succeed",
            secondSchemaExpectedId,
            restApp2.restClient.registerSchema(secondSchema, subject));

    // the newly registered schema should be immediately readable on the master using the id
    assertEquals("Registered schema should be found on the master",
            secondSchema,
            restApp1.restClient.getId(secondSchemaExpectedId).getSchemaString());

    // the newly registered schema should be immediately readable on the master using the version
    assertEquals("Registered schema should be found on the master",
            secondSchema,
            restApp1.restClient.getVersion(subject,
                    secondSchemaExpectedVersion).getSchema());

    // the newly registered schema should be eventually readable on the non-master
    verifyIdAndSchema(restApp2.restClient, secondSchemaExpectedId, secondSchema,
                      "Registered schema should be found on the non-master");

    // test registering an existing schema to the master
    assertEquals("Registering an existing schema to the master should return its id",
                 secondSchemaExpectedId,
                 restApp1.restClient.registerSchema(secondSchema, subject));

    // test registering an existing schema to the non-master
    assertEquals("Registering an existing schema to the non-master should return its id",
            secondSchemaExpectedId,
            restApp2.restClient.registerSchema(secondSchema, subject));

    // update config to master
    restApp1.restClient
        .updateCompatibility(AvroCompatibilityLevel.FORWARD.name, configSubject);
    assertEquals("New compatibility level should be FORWARD on the master",
                 FORWARD.name,
                 restApp1.restClient.getConfig(configSubject).getCompatibilityLevel());

    // the new config should be eventually readable on the non-master
    waitUntilCompatibilityLevelSet(restApp2.restClient, configSubject,
                                   AvroCompatibilityLevel.FORWARD.name,
                                   "New compatibility level should be FORWARD on the non-master");

    // update config to non-master
    restApp2.restClient
        .updateCompatibility(AvroCompatibilityLevel.NONE.name, configSubject);
    assertEquals("New compatibility level should be NONE on the master",
                 NONE.name,
                 restApp1.restClient.getConfig(configSubject).getCompatibilityLevel());

    // the new config should be eventually readable on the non-master
    waitUntilCompatibilityLevelSet(restApp2.restClient, configSubject,
                                   AvroCompatibilityLevel.NONE.name,
                                   "New compatibility level should be NONE on the non-master");

    // fake an incorrect master and registration should fail
    restApp1.setMaster(null);
    int statusCodeFromRestApp1 = 0;
    final String failedSchema = "{\"type\":\"string\"}";;
    try {
      restApp1.restClient.registerSchema(failedSchema, subject);
      fail("Registration should fail on the master");
    } catch (RestClientException e) {
      // this is expected.
      statusCodeFromRestApp1 = e.getStatus();
    }

    int statusCodeFromRestApp2 = 0;
    try {
      restApp2.restClient.registerSchema(failedSchema, subject);
      fail("Registration should fail on the non-master");
    } catch (RestClientException e) {
      // this is expected.
      statusCodeFromRestApp2 = e.getStatus();
    }

    assertEquals("Status code from a non-master rest app for register schema should be 500",
                 500, statusCodeFromRestApp1);
    assertEquals("Error code from the master and the non-master should be the same",
                 statusCodeFromRestApp1, statusCodeFromRestApp2);

    // update config should fail if master is not available
    int updateConfigStatusCodeFromRestApp1 = 0;
    try {
      restApp1.restClient.updateCompatibility(AvroCompatibilityLevel.FORWARD.name,
              configSubject);
      fail("Update config should fail on the master");
    } catch (RestClientException e) {
      // this is expected.
      updateConfigStatusCodeFromRestApp1 = e.getStatus();
    }

    int updateConfigStatusCodeFromRestApp2 = 0;
    try {
      restApp2.restClient.updateCompatibility(AvroCompatibilityLevel.FORWARD.name,
              configSubject);
      fail("Update config should fail on the non-master");
    } catch (RestClientException e) {
      // this is expected.
      updateConfigStatusCodeFromRestApp2 = e.getStatus();
    }

    assertEquals("Status code from a non-master rest app for update config should be 500",
                 500, updateConfigStatusCodeFromRestApp1);
    assertEquals("Error code from the master and the non-master should be the same",
                 updateConfigStatusCodeFromRestApp1, updateConfigStatusCodeFromRestApp2);

    // test registering an existing schema to the non-master when the master is not available
    assertEquals("Registering an existing schema to the non-master should return its id",
            secondSchemaExpectedId,
            restApp2.restClient.registerSchema(secondSchema, subject));

    // set the correct master identity back
    restApp1.setMaster(restApp1.myIdentity());

    // registering a schema to the master
    final String thirdSchema = avroSchemas.get(2);
    final int thirdSchemaExpectedVersion = 3;
    final int thirdSchemaExpectedId = ID_BATCH_SIZE + 1;
    assertEquals("Registering a new schema to the master should succeed",
            thirdSchemaExpectedId,
            restApp1.restClient.registerSchema(thirdSchema, subject));

    // stop schema registry instance 1; instance 2 should become the new master
    restApp1.stop();
    Callable<Boolean> condition = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return restApp2.isMaster();
      }
    };
    TestUtils.waitUntilTrue(condition, 5000,
                            "Schema registry instance 2 should become the master");

    // the latest version should be immediately available on the new master using the id
    assertEquals("Latest version should be found on the new master",
            thirdSchema,
            restApp2.restClient.getId(thirdSchemaExpectedId).getSchemaString());

    // the latest version should be immediately available on the new master using the version
    assertEquals("Latest version should be found on the new master",
            thirdSchema,
            restApp2.restClient.getVersion(subject,
                    thirdSchemaExpectedVersion).getSchema());

    // register a schema to the new master
    final String fourthSchema = avroSchemas.get(3);
    final int fourthSchemaExpectedId = 2 * ID_BATCH_SIZE + 1;
    TestUtils.registerAndVerifySchema(restApp2.restClient, fourthSchema,
                                      fourthSchemaExpectedId,
                                      subject);

    restApp2.stop();
  }


  @Test
  /**
   * Trigger reelection with both a master cluster and slave cluster present.
   * Ensure that nodes in slave cluster are never elected master.
   */
  public void testSlaveIsNeverMaster() throws Exception {
    int numSlaves = 2;
    int numMasters = 30;

    Set<RestApp> slaveApps = new HashSet<RestApp>();
    RestApp aSlave = null;
    for (int i = 0; i < numSlaves; i++) {
      RestApp slave = new RestApp(choosePort(),
                                  zkConnect, KAFKASTORE_TOPIC,
                                  AvroCompatibilityLevel.NONE.name, false);
      slaveApps.add(slave);
      slave.start();
      aSlave = slave;
    }
    // Sanity check
    assertNotNull(aSlave);

    // Check that nothing in the slave cluster points to a master
    for (RestApp slave: slaveApps) {
      assertFalse("No slave should be master.", slave.isMaster());
      assertNull("No master should be present in a slave cluster.", slave.masterIdentity());
    }

    // It should not be possible to set a slave node as master
    try {
      aSlave.setMaster(aSlave.myIdentity());
    } catch (IllegalStateException e) {
      // This is expected
    }
    assertFalse("Should not be able to set a slave to be master.", aSlave.isMaster());
    assertNull("There should be no master present.", aSlave.masterIdentity());

    // Make a master-eligible 'cluster'
    final Set<RestApp> masterApps = new HashSet<RestApp>();
    for (int i = 0; i < numMasters; i++) {
      RestApp master = new RestApp(choosePort(),
                                   zkConnect, KAFKASTORE_TOPIC,
                                   AvroCompatibilityLevel.NONE.name, true);
      masterApps.add(master);
      master.start();
      waitUntilMasterElectionCompletes(masterApps);
    }

    // Kill the current master and wait for reelection until no masters are left
    while (masterApps.size() > 0) {
      RestApp reportedMaster = checkOneMaster(masterApps);
      masterApps.remove(reportedMaster);

      checkMasterIdentity(slaveApps, reportedMaster.myIdentity());
      checkMasterIdentity(masterApps, reportedMaster.myIdentity());
      checkNoneIsMaster(slaveApps);

      reportedMaster.stop();
      waitUntilMasterElectionCompletes(masterApps);
    }

    // All masters are now dead
    checkNoneIsMaster(slaveApps);
    checkNoneIsMaster(masterApps);

    for (RestApp slave: slaveApps) {
      slave.stop();
    }
  }

  @Test
  /**
   * Test registration of schemas and fetching by id when a 'master cluster' and 'slave cluster' is
   * present. (Slave cluster == all nodes have masterEligibility false)
   *
   * If only slaves are alive, registration should fail. If both slave and master cluster are
   * alive, registration should succeed.
   *
   * Fetching by id should succeed in all configurations.
   */
  public void testRegistrationOnMasterSlaveClusters() throws Exception {
    int numSlaves = 4;
    int numMasters = 4;
    int numSchemas = 5;
    String subject = "testSubject";
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(numSchemas);
    List<Integer> ids = new ArrayList<Integer>();

    Set<RestApp> slaveApps = new HashSet<RestApp>();
    RestApp aSlave = null;
    for (int i = 0; i < numSlaves; i++) {
      RestApp slave = new RestApp(choosePort(),
                                  zkConnect, KAFKASTORE_TOPIC,
                                  AvroCompatibilityLevel.NONE.name, false);
      slaveApps.add(slave);
      slave.start();
      aSlave = slave;
    }
    // Sanity check
    assertNotNull(aSlave);

    // Try to register schemas to a slave - should fail
    boolean successfullyRegistered = false;
    try {
      aSlave.restClient.registerSchema(schemas.get(0), subject);
      successfullyRegistered = true;
    } catch (RestClientException e) {
      // registration should fail
    }
    assertFalse("Should not be possible to register with no masters present.",
                successfullyRegistered);

    // Make a master-eligible 'cluster'
    final Set<RestApp> masterApps = new HashSet<RestApp>();
    RestApp aMaster = null;
    for (int i = 0; i < numMasters; i++) {
      RestApp master = new RestApp(choosePort(),
                                   zkConnect, KAFKASTORE_TOPIC,
                                   AvroCompatibilityLevel.NONE.name, true);
      masterApps.add(master);
      master.start();
      aMaster = master;
    }
    assertNotNull(aMaster);

    // Try to register to a master cluster node - should succeed
    try {
      for (String schema : schemas) {
        ids.add(aMaster.restClient.registerSchema(schema, subject));
      }
    } catch (RestClientException e) {
      fail("It should be possible to register schemas when a master cluster is present.");
    }

    // Try to register to a slave cluster node - should succeed
    String anotherSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    try {
      ids.add(aSlave.restClient.registerSchema(anotherSchema, subject));
    } catch (RestClientException e) {
      fail("Should be possible register a schema through slave cluster.");
    }

    // Verify all ids can be fetched
    try {
      for (int id: ids) {
        waitUntilIdExists(aSlave.restClient, id,
                          String.format("Should be possible to fetch id %d from this slave.", id));
        waitUntilIdExists(aMaster.restClient, id,
                          String.format("Should be possible to fetch id %d from this master.", id));

        SchemaString slaveResponse = aSlave.restClient.getId(id);
        SchemaString masterResponse = aMaster.restClient.getId(id);
        assertEquals(
            "Master and slave responded with different schemas when queried with the same id.",
            slaveResponse.getSchemaString(), masterResponse.getSchemaString());
      }
    } catch (RestClientException e) {
      fail("Expected ids were not found in the schema registry.");
    }

    // Stop everything in the master cluster
    while (masterApps.size() > 0) {
      RestApp master = findMaster(masterApps);
      masterApps.remove(master);
      master.stop();
      waitUntilMasterElectionCompletes(masterApps);
    }

    // Try to register a new schema - should fail
    anotherSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    successfullyRegistered = false;
    try {
      aSlave.restClient.registerSchema(anotherSchema, subject);
      successfullyRegistered = true;
    } catch (RestClientException e) {
      // should fail
    }
    assertFalse("Should not be possible to register with no masters present.",
                successfullyRegistered);

    // Try fetching preregistered ids from slaves - should succeed
    try {

      for (int id: ids) {
        SchemaString schemaString = aSlave.restClient.getId(id);
      }
      List<Integer> versions = aSlave.restClient.getAllVersions(subject);
      assertEquals("Number of ids should match number of versions.", ids.size(), versions.size());
    } catch (RestClientException e) {
      fail("Should be possible to fetch registered schemas even with no masters present.");
    }

    for (RestApp slave: slaveApps) {
      slave.stop();
    }
  }

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
    TestUtils.waitUntilTrue(electionComplete, 5000,
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

  /** Return the first node which reports itself as master, or null if none does. */
  private static RestApp findMaster(Collection<RestApp> cluster) {
    for (RestApp restApp: cluster) {
      if (restApp.isMaster()) {
        return restApp;
      }
    }

    return null;
  }

  /** Verify that no node in cluster reports itself as master. */
  private static void checkNoneIsMaster(Collection<RestApp> cluster) {
    assertNull("Expected none of the nodes in this cluster to report itself as master.", findMaster(cluster));
  }

  /** Verify that all nodes agree on the expected master identity. */
  private static void checkMasterIdentity(Collection<RestApp> cluster,
                                          SchemaRegistryIdentity expectedMasterIdentity) {
    for (RestApp restApp: cluster) {
      assertEquals("Each master identity should be " + expectedMasterIdentity,
                   expectedMasterIdentity, restApp.masterIdentity());
    }
  }

  static int getZkIdCounter(ZkClient zkClient) {
    return Integer.valueOf(ZkUtils.readData(
        zkClient, ZK_ID_COUNTER_PATH).getData());
  }

  /**
   * Return set of identities of all nodes reported as master. Expect this to be a set of
   * size 1 unless there is some pathological behavior.
   */
  private static Set<SchemaRegistryIdentity> getMasterIdentities(Collection<RestApp> cluster) {
    Set<SchemaRegistryIdentity> masterIdentities = new HashSet<SchemaRegistryIdentity>();
    for (RestApp app: cluster) {
      if (app != null && app.masterIdentity() != null) {
        masterIdentities.add(app.masterIdentity());
      }
    }

    return masterIdentities;
  }

  /**
   * Check that exactly one RestApp in the cluster reports itself as master.
   */
  private static RestApp checkOneMaster(Collection<RestApp> cluster) {
    int masterCount = 0;
    RestApp master = null;
    for (RestApp restApp: cluster) {
      if (restApp.isMaster()) {
        masterCount++;
        master = restApp;
      }
    }

    assertEquals("Expected one master but found " + masterCount, 1, masterCount);
    return master;
  }

  private void waitUntilMasterElectionCompletes(final Collection<RestApp> cluster) {
    if (cluster == null || cluster.size() == 0) {
      return;
    }

    Callable<Boolean> newMasterElected = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        boolean hasMaster = findMaster(cluster) != null;
        // Check that new master identity has propagated to all nodes
        boolean oneReportedMaster = getMasterIdentities(cluster).size() == 1;

        return hasMaster && oneReportedMaster;
      }
    };
    TestUtils.waitUntilTrue(
        newMasterElected, 5000, "A node should have been elected master by now.");
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
    TestUtils.waitUntilTrue(canGetSchemaById, 5000, errorMsg);
  }

  private void waitUntilCompatibilityLevelSet(final RestService restService, final String subject,
                                              final String expectedCompatibilityLevel,
                                              String errorMsg) {
    Callable<Boolean> canGetSchemaById = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          String actualCompatibilityLevel = restService.getConfig(subject).getCompatibilityLevel();
          return expectedCompatibilityLevel.compareTo(actualCompatibilityLevel) == 0;
        } catch (RestClientException e) {
          return false;
        }
      }
    };
    TestUtils.waitUntilTrue(canGetSchemaById, 5000, errorMsg);
  }

  private void verifyIdAndSchema(final RestService restService, final int expectedId,
                                 final String expectedSchemaString, String errMsg) {
    waitUntilIdExists(restService, expectedId, errMsg);
    String schemaString = null;
    try {
      schemaString = restService.getId(expectedId)
          .getSchemaString();
    } catch (IOException e) {
      fail(errMsg);
    } catch (RestClientException e) {
      fail(errMsg);
    }

    assertEquals(errMsg, expectedSchemaString, schemaString);
  }
}
