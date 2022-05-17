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

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.Mode;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;

import static io.confluent.kafka.schemaregistry.CompatibilityLevel.FORWARD;
import static io.confluent.kafka.schemaregistry.CompatibilityLevel.NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LeaderElectorTest extends ClusterTestHarness {

  private String zkConnect() {
    return null;
  }

  private String bootstrapServers() {
    return bootstrapServers;
  }

  @Test
  public void testAutoFailover() throws Exception {
    final String subject = "testTopic";
    final String configSubject = "configTopic";
    List<String> avroSchemas = TestUtils.getRandomCanonicalAvroString(4);

    // Since leader selection depends on the lexicographic ordering of members, we need to make
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
                                         CompatibilityLevel.NONE.name, true, null);
    restApp1.start();

    // create schema registry instance 2
    final RestApp restApp2 = new RestApp(port2,
                                         zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                         CompatibilityLevel.NONE.name, true, null);
    restApp2.start();
    assertTrue("Schema registry instance 1 should be the leader", restApp1.isLeader());
    assertFalse("Schema registry instance 2 shouldn't be the leader", restApp2.isLeader());
    assertEquals("Instance 2's leader should be instance 1",
                 restApp1.myIdentity(), restApp2.leaderIdentity());

    // test registering a schema to the leader and finding it on the expected version
    final String firstSchema = avroSchemas.get(0);
    final int firstSchemaExpectedId = 1;
    TestUtils.registerAndVerifySchema(restApp1.restClient, firstSchema, firstSchemaExpectedId,
                                      subject);
    // the newly registered schema should be eventually readable on the non-leader
    verifyIdAndSchema(restApp2.restClient, firstSchemaExpectedId, firstSchema,
                      "Registered schema should be found on the non-leader");

    // test registering a schema to the non-leader and finding it on the expected version
    final String secondSchema = avroSchemas.get(1);
    final int secondSchemaExpectedId = 2;
    final int secondSchemaExpectedVersion = 2;
    assertEquals("Registering a new schema to the non-leader should succeed",
            secondSchemaExpectedId,
            restApp2.restClient.registerSchema(secondSchema, subject));

    // the newly registered schema should be immediately readable on the leader using the id
    assertEquals("Registered schema should be found on the leader",
            secondSchema,
            restApp1.restClient.getId(secondSchemaExpectedId).getSchemaString());

    // the newly registered schema should be immediately readable on the leader using the version
    assertEquals("Registered schema should be found on the leader",
            secondSchema,
            restApp1.restClient.getVersion(subject,
                    secondSchemaExpectedVersion).getSchema());

    // the newly registered schema should be eventually readable on the non-leader
    verifyIdAndSchema(restApp2.restClient, secondSchemaExpectedId, secondSchema,
                      "Registered schema should be found on the non-leader");

    // test registering an existing schema to the leader
    assertEquals("Registering an existing schema to the leader should return its id",
                 secondSchemaExpectedId,
                 restApp1.restClient.registerSchema(secondSchema, subject));

    // test registering an existing schema to the non-leader
    assertEquals("Registering an existing schema to the non-leader should return its id",
            secondSchemaExpectedId,
            restApp2.restClient.registerSchema(secondSchema, subject));

    // update config to leader
    restApp1.restClient
        .updateCompatibility(CompatibilityLevel.FORWARD.name, configSubject);
    assertEquals("New compatibility level should be FORWARD on the leader",
                 FORWARD.name,
                 restApp1.restClient.getConfig(configSubject).getCompatibilityLevel());

    // the new config should be eventually readable on the non-leader
    waitUntilCompatibilityLevelSet(restApp2.restClient, configSubject,
                                   CompatibilityLevel.FORWARD.name,
                                   "New compatibility level should be FORWARD on the non-leader");

    // update config to non-leader
    restApp2.restClient
        .updateCompatibility(CompatibilityLevel.NONE.name, configSubject);
    assertEquals("New compatibility level should be NONE on the leader",
                 NONE.name,
                 restApp1.restClient.getConfig(configSubject).getCompatibilityLevel());

    // the new config should be eventually readable on the non-leader
    waitUntilCompatibilityLevelSet(restApp2.restClient, configSubject,
                                   CompatibilityLevel.NONE.name,
                                   "New compatibility level should be NONE on the non-leader");

    // fake an incorrect leader and registration should fail
    restApp1.setLeader(null);
    int statusCodeFromRestApp1 = 0;
    final String failedSchema = "{\"type\":\"string\"}";;
    try {
      restApp1.restClient.registerSchema(failedSchema, subject);
      fail("Registration should fail on the leader");
    } catch (RestClientException e) {
      // this is expected.
      statusCodeFromRestApp1 = e.getStatus();
    }

    int statusCodeFromRestApp2 = 0;
    try {
      restApp2.restClient.registerSchema(failedSchema, subject);
      fail("Registration should fail on the non-leader");
    } catch (RestClientException e) {
      // this is expected.
      statusCodeFromRestApp2 = e.getStatus();
    }

    assertEquals("Status code from a non-leader rest app for register schema should be 500",
                 500, statusCodeFromRestApp1);
    assertEquals("Error code from the leader and the non-leader should be the same",
                 statusCodeFromRestApp1, statusCodeFromRestApp2);

    // update config should fail if leader is not available
    int updateConfigStatusCodeFromRestApp1 = 0;
    try {
      restApp1.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name,
              configSubject);
      fail("Update config should fail on the leader");
    } catch (RestClientException e) {
      // this is expected.
      updateConfigStatusCodeFromRestApp1 = e.getStatus();
    }

    int updateConfigStatusCodeFromRestApp2 = 0;
    try {
      restApp2.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name,
              configSubject);
      fail("Update config should fail on the non-leader");
    } catch (RestClientException e) {
      // this is expected.
      updateConfigStatusCodeFromRestApp2 = e.getStatus();
    }

    assertEquals("Status code from a non-leader rest app for update config should be 500",
                 500, updateConfigStatusCodeFromRestApp1);
    assertEquals("Error code from the leader and the non-leader should be the same",
                 updateConfigStatusCodeFromRestApp1, updateConfigStatusCodeFromRestApp2);

    // test registering an existing schema to the non-leader when the leader is not available
    assertEquals("Registering an existing schema to the non-leader should return its id",
            secondSchemaExpectedId,
            restApp2.restClient.registerSchema(secondSchema, subject));

    // set the correct leader identity back
    restApp1.setLeader(restApp1.myIdentity());

    // registering a schema to the leader
    final String thirdSchema = avroSchemas.get(2);
    final int thirdSchemaExpectedVersion = 3;
    final int thirdSchemaExpectedId = secondSchemaExpectedId + 1;
    assertEquals("Registering a new schema to the leader should succeed",
            thirdSchemaExpectedId,
            restApp1.restClient.registerSchema(thirdSchema, subject));

    // stop schema registry instance 1; instance 2 should become the new leader
    restApp1.stop();
    Callable<Boolean> condition = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return restApp2.isLeader();
      }
    };
    TestUtils.waitUntilTrue(condition, 15000,
                            "Schema registry instance 2 should become the leader");

    // the latest version should be immediately available on the new leader using the id
    assertEquals("Latest version should be found on the new leader",
            thirdSchema,
            restApp2.restClient.getId(thirdSchemaExpectedId).getSchemaString());

    // the latest version should be immediately available on the new leader using the version
    assertEquals("Latest version should be found on the new leader",
            thirdSchema,
            restApp2.restClient.getVersion(subject,
                    thirdSchemaExpectedVersion).getSchema());

    // register a schema to the new leader
    final String fourthSchema = avroSchemas.get(3);
    final int fourthSchemaExpectedId = thirdSchemaExpectedId + 1;
    TestUtils.registerAndVerifySchema(restApp2.restClient, fourthSchema,
                                      fourthSchemaExpectedId,
                                      subject);

    restApp2.stop();
  }


  @Test
  /**
   * Trigger reelection with both a leader cluster and follower cluster present.
   * Ensure that nodes in follower cluster are never elected leader.
   */
  public void testFollowerIsNeverLeader() throws Exception {
    int numFollowers = 2;
    int numLeaders = 30;

    Set<RestApp> followerApps = new HashSet<RestApp>();
    RestApp aFollower = null;
    for (int i = 0; i < numFollowers; i++) {
      RestApp follower = new RestApp(choosePort(),
                                  zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                  CompatibilityLevel.NONE.name, false, null);
      followerApps.add(follower);
      follower.start();
      aFollower = follower;
    }
    // Sanity check
    assertNotNull(aFollower);

    // Check that nothing in the follower cluster points to a leader
    for (RestApp follower : followerApps) {
      assertFalse("No follower should be leader.", follower.isLeader());
      assertNull("No follower should be present in a follower cluster.", follower.leaderIdentity());
    }

    // It should not be possible to set a follower node as leader
    try {
      aFollower.setLeader(aFollower.myIdentity());
    } catch (IllegalStateException e) {
      // This is expected
    }
    assertFalse("Should not be able to set a follower to be leader.", aFollower.isLeader());
    assertNull("There should be no leader present.", aFollower.leaderIdentity());

    // Make a leader-eligible 'cluster'
    final Set<RestApp> leaderApps = new HashSet<RestApp>();
    for (int i = 0; i < numLeaders; i++) {
      RestApp leader = new RestApp(choosePort(),
                                   zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                   CompatibilityLevel.NONE.name, true, null);
      leaderApps.add(leader);
      leader.start();
      waitUntilLeaderElectionCompletes(leaderApps);
    }

    // Kill the current leader and wait for reelection until no leaders are left
    while (leaderApps.size() > 0) {
      RestApp reportedLeader = checkOneLeader(leaderApps);
      leaderApps.remove(reportedLeader);

      checkLeaderIdentity(followerApps, reportedLeader.myIdentity());
      checkLeaderIdentity(leaderApps, reportedLeader.myIdentity());
      checkNoneIsLeader(followerApps);

      reportedLeader.stop();
      waitUntilLeaderElectionCompletes(leaderApps);
    }

    // All leaders are now dead
    checkNoneIsLeader(followerApps);
    checkNoneIsLeader(leaderApps);

    for (RestApp follower : followerApps) {
      follower.stop();
    }
  }

  @Test
  /**
   * Test registration of schemas and fetching by id when a 'leader cluster' and 'follower cluster'
   * is present. (follwer cluster == all nodes have leaderEligibility false)
   *
   * If only followers are alive, registration should fail. If both follower and leader cluster are
   * alive, registration should succeed.
   *
   * Fetching by id should succeed in all configurations.
   */
  public void testRegistrationOnLeaderFollowerClusters() throws Exception {
    int numFollowers = 4;
    int numLeaders = 4;
    int numSchemas = 5;
    String subject = "testSubject";
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(numSchemas);
    List<Integer> ids = new ArrayList<Integer>();

    Set<RestApp> followerApps = new HashSet<RestApp>();
    RestApp aFollower = null;
    for (int i = 0; i < numFollowers; i++) {
      RestApp follower = new RestApp(choosePort(),
                                  zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                  CompatibilityLevel.NONE.name, false, null);
      followerApps.add(follower);
      follower.start();
      aFollower = follower;
    }
    // Sanity check
    assertNotNull(aFollower);

    // Try to register schemas to a follower - should fail
    boolean successfullyRegistered = false;
    try {
      aFollower.restClient.registerSchema(schemas.get(0), subject);
      successfullyRegistered = true;
    } catch (RestClientException e) {
      // registration should fail
    }
    assertFalse("Should not be possible to register with no leaders present.",
                successfullyRegistered);

    // Make a leader-eligible 'cluster'
    final Set<RestApp> leaderApps = new HashSet<RestApp>();
    RestApp aLeader = null;
    for (int i = 0; i < numLeaders; i++) {
      RestApp leader = new RestApp(choosePort(),
                                   zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
                                   CompatibilityLevel.NONE.name, true, null);
      leaderApps.add(leader);
      leader.start();
      aLeader = leader;
    }
    assertNotNull(aLeader);

    // Try to register to a leader cluster node - should succeed
    try {
      for (String schema : schemas) {
        ids.add(aLeader.restClient.registerSchema(schema, subject));
      }
    } catch (RestClientException e) {
      fail("It should be possible to register schemas when a leader cluster is present.");
    }

    // Try to register to a follower cluster node - should succeed
    String anotherSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    try {
      ids.add(aFollower.restClient.registerSchema(anotherSchema, subject));
    } catch (RestClientException e) {
      fail("Should be possible register a schema through follower cluster.");
    }

    // Verify all ids can be fetched
    try {
      for (int id: ids) {
        waitUntilIdExists(aFollower.restClient, id,
                          String.format("Should be possible to fetch id %d from this follower.", id));
        waitUntilIdExists(aLeader.restClient, id,
                          String.format("Should be possible to fetch id %d from this leader.", id));

        SchemaString followerResponse = aFollower.restClient.getId(id);
        SchemaString leaderResponse = aLeader.restClient.getId(id);
        assertEquals(
            "Leader and follower responded with different schemas when queried with the same id.",
            followerResponse.getSchemaString(), leaderResponse.getSchemaString());
      }
    } catch (RestClientException e) {
      fail("Expected ids were not found in the schema registry.");
    }

    // Stop everything in the leader cluster
    while (leaderApps.size() > 0) {
      RestApp leader = findLeader(leaderApps);
      leaderApps.remove(leader);
      leader.stop();
      waitUntilLeaderElectionCompletes(leaderApps);
    }

    // Try to register a new schema - should fail
    anotherSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    successfullyRegistered = false;
    try {
      aFollower.restClient.registerSchema(anotherSchema, subject);
      successfullyRegistered = true;
    } catch (RestClientException e) {
      // should fail
    }
    assertFalse("Should not be possible to register with no leaders present.",
                successfullyRegistered);

    // Try fetching preregistered ids from followers - should succeed
    try {

      for (int id: ids) {
        SchemaString schemaString = aFollower.restClient.getId(id);
      }
      List<Integer> versions = aFollower.restClient.getAllVersions(subject);
      assertEquals("Number of ids should match number of versions.", ids.size(), versions.size());
    } catch (RestClientException e) {
      fail("Should be possible to fetch registered schemas even with no leaders present.");
    }

    for (RestApp follower : followerApps) {
      follower.stop();
    }
  }

  @Test
  /**
   * Test import mode and registration of schemas with version and id when a 'leader cluster' and
   * 'follower cluster' is present. (Follower cluster == all nodes have leaderEligibility false)
   *
   * If only followers are alive, registration should fail. If both follower and leader cluster are
   * alive, registration should succeed.
   *
   * Fetching by id should succeed in all configurations.
   */
  public void testImportOnLeaderFollowerClusters() throws Exception {
    int numFollowers = 4;
    int numLeaders = 4;
    int numSchemas = 5;
    String subject = "testSubject";
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(numSchemas);
    List<Integer> ids = new ArrayList<Integer>();
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.MODE_MUTABILITY, "true");
    int newId = 100000;
    int newVersion = 100;

    Set<RestApp> followerApps = new HashSet<RestApp>();
    RestApp aFollower = null;
    for (int i = 0; i < numFollowers; i++) {
      RestApp follower = new RestApp(choosePort(),
          zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
          CompatibilityLevel.NONE.name, false, props);
      followerApps.add(follower);
      follower.start();
      aFollower = follower;
    }
    // Sanity check
    assertNotNull(aFollower);

    // Try to register schemas to a follower - should fail
    boolean successfullyRegistered = false;
    try {
      aFollower.restClient.registerSchema(schemas.get(0), subject, newVersion++, newId++);
      successfullyRegistered = true;
    } catch (RestClientException e) {
      // registration should fail
    }
    assertFalse("Should not be possible to register with no leaders present.",
        successfullyRegistered);

    // Make a leader-eligible 'cluster'
    final Set<RestApp> leaderApps = new HashSet<RestApp>();
    RestApp aLeader = null;
    for (int i = 0; i < numLeaders; i++) {
      RestApp leader = new RestApp(choosePort(),
          zkConnect(), bootstrapServers(), KAFKASTORE_TOPIC,
          CompatibilityLevel.NONE.name, true, props);
      leaderApps.add(leader);
      leader.start();
      aLeader = leader;
    }
    assertNotNull(aLeader);

    // Enter import mode
    try {
      aLeader.restClient.setMode(Mode.IMPORT.toString());
    } catch (RestClientException e) {
      fail("It should be possible to set mode when a leader cluster is present.");
    }

    // Try to register to a leader cluster node - should succeed
    try {
      for (String schema : schemas) {
        ids.add(aLeader.restClient.registerSchema(schema, subject, newVersion++, newId++));
      }
    } catch (RestClientException e) {
      fail("It should be possible to register schemas when a leader cluster is present. "
          + "Error: " + e.getMessage());
    }

    // Try to register to a follower cluster node - should succeed
    String anotherSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    try {
      ids.add(aFollower.restClient.registerSchema(anotherSchema, subject, newVersion++, newId++));
    } catch (RestClientException e) {
      fail("Should be possible register a schema through follower cluster.");
    }

    // Verify all ids can be fetched
    try {
      for (int id: ids) {
        waitUntilIdExists(aFollower.restClient, id,
            String.format("Should be possible to fetch id %d from this follower.", id));
        waitUntilIdExists(aLeader.restClient, id,
            String.format("Should be possible to fetch id %d from this leader.", id));

        SchemaString followerResponse = aFollower.restClient.getId(id);
        SchemaString leaderResponse = aLeader.restClient.getId(id);
        assertEquals(
            "Leader and follower responded with different schemas when queried with the same id.",
            followerResponse.getSchemaString(), leaderResponse.getSchemaString());
      }
    } catch (RestClientException e) {
      fail("Expected ids were not found in the schema registry.");
    }

    // Stop everything in the leader cluster
    while (leaderApps.size() > 0) {
      RestApp leader = findLeader(leaderApps);
      leaderApps.remove(leader);
      leader.stop();
      waitUntilLeaderElectionCompletes(leaderApps);
    }

    // Try to register a new schema - should fail
    anotherSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    successfullyRegistered = false;
    try {
      aFollower.restClient.registerSchema(anotherSchema, subject, newVersion++, newId++);
      successfullyRegistered = true;
    } catch (RestClientException e) {
      // should fail
    }
    assertFalse("Should not be possible to register with no leaders present.",
        successfullyRegistered);

    // Try fetching preregistered ids from followers - should succeed
    try {

      for (int id: ids) {
        SchemaString schemaString = aFollower.restClient.getId(id);
      }
      List<Integer> versions = aFollower.restClient.getAllVersions(subject);
      assertEquals("Number of ids should match number of versions.", ids.size(), versions.size());
    } catch (RestClientException e) {
      fail("Should be possible to fetch registered schemas even with no leaders present.");
    }

    for (RestApp follower : followerApps) {
      follower.stop();
    }
  }

  /** Return the first node which reports itself as leader, or null if none does. */
  private static RestApp findLeader(Collection<RestApp> cluster) {
    for (RestApp restApp: cluster) {
      if (restApp.isLeader()) {
        return restApp;
      }
    }

    return null;
  }

  /** Verify that no node in cluster reports itself as leader. */
  private static void checkNoneIsLeader(Collection<RestApp> cluster) {
    assertNull("Expected none of the nodes in this cluster to report itself as leader.", findLeader(cluster));
  }

  /** Verify that all nodes agree on the expected leader identity. */
  private static void checkLeaderIdentity(Collection<RestApp> cluster,
                                          SchemaRegistryIdentity expectedLeaderIdentity) {
    for (RestApp restApp: cluster) {
      for (int i = 0; i < 3; i++) {
        SchemaRegistryIdentity leaderIdentity = restApp.leaderIdentity();
        // There can be some latency in all the nodes picking up the new leader so we need
        // to allow for some retries here.
        if (leaderIdentity == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // ignore
          }
          continue;
        }
        assertEquals("Each leader identity should be " + expectedLeaderIdentity,
            expectedLeaderIdentity, leaderIdentity);
      }
    }
  }

  /**
   * Return set of identities of all nodes reported as leader. Expect this to be a set of
   * size 1 unless there is some pathological behavior.
   */
  private static Set<SchemaRegistryIdentity> getLeaderIdentities(Collection<RestApp> cluster) {
    Set<SchemaRegistryIdentity> leaderIdentities = new HashSet<SchemaRegistryIdentity>();
    for (RestApp app: cluster) {
      if (app != null && app.leaderIdentity() != null) {
        leaderIdentities.add(app.leaderIdentity());
      }
    }

    return leaderIdentities;
  }

  /**
   * Check that exactly one RestApp in the cluster reports itself as leader.
   */
  private static RestApp checkOneLeader(Collection<RestApp> cluster) {
    int leaderCount = 0;
    RestApp leader = null;
    for (RestApp restApp: cluster) {
      if (restApp.isLeader()) {
        leaderCount++;
        leader = restApp;
      }
    }

    assertEquals("Expected one leader but found " + leaderCount, 1, leaderCount);
    return leader;
  }

  private void waitUntilLeaderElectionCompletes(final Collection<RestApp> cluster) {
    if (cluster == null || cluster.size() == 0) {
      return;
    }

    Callable<Boolean> newLeaderElected = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        boolean hasLeader = findLeader(cluster) != null;
        // Check that new leader identity has propagated to all nodes
        boolean oneReportedLeader = getLeaderIdentities(cluster).size() == 1;

        return hasLeader && oneReportedLeader;
      }
    };
    TestUtils.waitUntilTrue(
        newLeaderElected, 15000, "A node should have been elected leader by now.");
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
