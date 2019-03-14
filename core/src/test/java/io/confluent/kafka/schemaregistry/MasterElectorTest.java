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
package io.confluent.kafka.schemaregistry;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.Mode;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import io.confluent.kafka.schemaregistry.id.ZookeeperIdGenerator;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;

import static io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel.FORWARD;
import static io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel.NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class MasterElectorTest extends ClusterTestHarness {

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {
            "kafka",
            0, // reservation size, i.e. how many IDs are reserved and potentially discarded
        },
        {
            "zookeeper",
            ZookeeperIdGenerator.ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE
        }
    });
  }

  @Parameter(0)
  public String electorType;
  @Parameter(1)
  public int reservationBatchSize;

  private String zkConnect() {
    switch (electorType) {
      case "zookeeper":
        return zkConnect;
      case "kafka":
        return null;
      default:
        throw new IllegalArgumentException();
    }
  }

  private String bootstrapServers() {
    switch (electorType) {
      case "zookeeper":
        return null;
      case "kafka":
        return bootstrapServers;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Test
  public void testAutoFailover() throws Exception {
    final String subject = "testTopic";
    final String configSubject = "configTopic";
    List<String> avroSchemas = TestUtils.getRandomCanonicalAvroString(4);

    // Since master selection depends on the lexicographic ordering of members, we need to make
    // sure the first instance gets a lower port.
    int port1 = choosePort();
    int port2 = choosePort();
    if (port2 < port1) {
      int tmp = port2;
      port2 = port1;
      port1 = tmp;
    }

    // create schema registry instance 1
    final RestApp restApp1 = new RestApp(port1,
                                         zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                         AvroCompatibilityLevel.NONE.name, true, null);
    restApp1.start();

    // create schema registry instance 2
    final RestApp restApp2 = new RestApp(port2,
                                         zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                         AvroCompatibilityLevel.NONE.name, true, null);
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
    final int thirdSchemaExpectedId;
    if (electorType == "zookeeper") {
      thirdSchemaExpectedId = reservationBatchSize + 1;
    } else {
      thirdSchemaExpectedId = secondSchemaExpectedId + 1;
    }
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
    TestUtils.waitUntilTrue(condition, 15000,
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
    final int fourthSchemaExpectedId;
    if (electorType == "zookeeper") {
      fourthSchemaExpectedId = 2 * reservationBatchSize + 1;
    } else {
      fourthSchemaExpectedId = thirdSchemaExpectedId + 1;
    }
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
                                  zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                  AvroCompatibilityLevel.NONE.name, false, null);
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
                                   zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                   AvroCompatibilityLevel.NONE.name, true, null);
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
                                  zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                  AvroCompatibilityLevel.NONE.name, false, null);
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
                                   zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                   AvroCompatibilityLevel.NONE.name, true, null);
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
   * Test import mode and registration of schemas with version and id when a 'master cluster' and
   * 'slave cluster' is present. (Slave cluster == all nodes have masterEligibility false)
   *
   * If only slaves are alive, registration should fail. If both slave and master cluster are
   * alive, registration should succeed.
   *
   * Fetching by id should succeed in all configurations.
   */
  public void testImportOnMasterSlaveClusters() throws Exception {
    int numSlaves = 4;
    int numMasters = 4;
    int numSchemas = 5;
    String subject = "testSubject";
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(numSchemas);
    List<Integer> ids = new ArrayList<Integer>();
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.MODE_MUTABILITY, "true");
    int newId = 1;
    int newVersion = 1;

    Set<RestApp> slaveApps = new HashSet<RestApp>();
    RestApp aSlave = null;
    for (int i = 0; i < numSlaves; i++) {
      RestApp slave = new RestApp(choosePort(),
          zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
          AvroCompatibilityLevel.NONE.name, false, props);
      slaveApps.add(slave);
      slave.start();
      aSlave = slave;
    }
    // Sanity check
    assertNotNull(aSlave);

    // Try to register schemas to a slave - should fail
    boolean successfullyRegistered = false;
    try {
      aSlave.restClient.registerSchema(schemas.get(0), subject, newVersion++, newId++);
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
          zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
          AvroCompatibilityLevel.NONE.name, true, props);
      masterApps.add(master);
      master.start();
      aMaster = master;
    }
    assertNotNull(aMaster);

    // Enter import mode
    try {
      aMaster.restClient.setMode(Mode.IMPORT.toString());
    } catch (RestClientException e) {
      fail("It should be possible to set mode when a master cluster is present.");
    }

    // Try to register to a master cluster node - should succeed
    try {
      for (String schema : schemas) {
        ids.add(aMaster.restClient.registerSchema(schema, subject, newVersion++, newId++));
      }
    } catch (RestClientException e) {
      fail("It should be possible to register schemas when a master cluster is present.");
    }

    // Try to register to a slave cluster node - should succeed
    String anotherSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    try {
      ids.add(aSlave.restClient.registerSchema(anotherSchema, subject, newVersion++, newId++));
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
      aSlave.restClient.registerSchema(anotherSchema, subject, newVersion++, newId++);
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
      for (int i = 0; i < 3; i++) {
        SchemaRegistryIdentity masterIdentity = restApp.masterIdentity();
        // There can be some latency in all the nodes picking up the new master from ZK so we need
        // to allow for some retries here.
        if (masterIdentity == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // ignore
          }
          continue;
        }
        assertEquals("Each master identity should be " + expectedMasterIdentity,
                     expectedMasterIdentity, masterIdentity);
      }
    }
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
        newMasterElected, 15000, "A node should have been elected master by now.");
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
    TestUtils.waitUntilTrue(canGetSchemaById, 15000, errorMsg);
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
