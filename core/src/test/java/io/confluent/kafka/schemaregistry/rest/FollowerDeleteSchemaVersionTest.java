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

package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests for follower behavior when deleting schema versions.
 * Tests the fix for stale cache issues where followers prematurely decide
 * a schema is deleted based on stale local cache.
 */
public class FollowerDeleteSchemaVersionTest extends ClusterTestHarness {

  private RestApp leader;
  private RestApp follower;

  public FollowerDeleteSchemaVersionTest() {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterEach
  public void tearDownApps() throws Exception {
    if (follower != null) {
      follower.stop();
    }
    if (leader != null) {
      leader.stop();
    }
  }

  /**
   * Test that follower forwards delete requests to leader without performing
   * validation that could be based on stale cache.
   */
  @Test
  public void testFollowerForwardsDeleteWithoutValidation() throws Exception {
    // Setup: Create leader and follower instances
    int port1 = choosePort();
    int port2 = choosePort();

    // Ensure port1 < port2 for deterministic leader election
    if (port2 < port1) {
      int tmp = port2;
      port2 = port1;
      port1 = tmp;
    }

    leader = new RestApp(port1, null, brokerList, KAFKASTORE_TOPIC,
                         CompatibilityLevel.NONE.name, true, null);
    leader.start();

    follower = new RestApp(port2, null, brokerList, KAFKASTORE_TOPIC,
                           CompatibilityLevel.NONE.name, true, null);
    follower.start();

    // Verify leadership
    assertTrue(leader.isLeader(), "First instance should be the leader");
    assertFalse(follower.isLeader(), "Second instance should be the follower");

    // Register a schema through the leader
    String subject = "test-subject";
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    int schemaId = leader.restClient.registerSchema(schema, subject);
    assertEquals(1, schemaId, "First schema should have ID 1");

    // Wait for follower to replicate the schema
    waitUntilSchemaReplicatedToFollower(follower, schemaId, schema, 10000);

    // Verify follower can read the schema
    assertEquals(schema,
                 follower.restClient.getId(schemaId).getSchemaString(),
                 "Follower should be able to read replicated schema");

    // Delete schema version through follower
    // This tests that follower forwards the delete request to leader
    // without performing local validation that could use stale cache
    int deletedVersion = follower.restClient.deleteSchemaVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        subject,
        "1"
    );
    assertEquals(1, deletedVersion, "Delete should return version 1");

    // Verify the schema is marked as deleted on the leader
    waitUntilSoftDeleted(leader, subject, 1, 10000);

    // Verify follower eventually sees the schema as deleted
    waitUntilSoftDeleted(follower, subject, 1, 10000);
  }

  /**
   * Test that follower can handle "latest" version deletion without
   * requiring local resolution of what "latest" means.
   */
  @Test
  public void testFollowerForwardsDeleteLatestVersion() throws Exception {
    int port1 = choosePort();
    int port2 = choosePort();

    if (port2 < port1) {
      int tmp = port2;
      port2 = port1;
      port1 = tmp;
    }

    leader = new RestApp(port1, null, brokerList, KAFKASTORE_TOPIC,
                         CompatibilityLevel.NONE.name, true, null);
    leader.start();

    follower = new RestApp(port2, null, brokerList, KAFKASTORE_TOPIC,
                           CompatibilityLevel.NONE.name, true, null);
    follower.start();

    assertTrue(leader.isLeader(), "First instance should be the leader");
    assertFalse(follower.isLeader(), "Second instance should be the follower");

    // Register multiple schema versions
    String subject = "test-subject-latest";
    String schema1 = TestUtils.getRandomCanonicalAvroString(1).get(0);
    String schema2 = TestUtils.getRandomCanonicalAvroString(2).get(1);

    int schemaId1 = leader.restClient.registerSchema(schema1, subject);
    int schemaId2 = leader.restClient.registerSchema(schema2, subject);

    // Wait for follower to catch up
    waitUntilSchemaReplicatedToFollower(follower, schemaId2, schema2, 10000);

    // Delete "latest" version through follower
    // Follower should forward without trying to resolve what "latest" means locally
    int deletedVersion = follower.restClient.deleteSchemaVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        subject,
        "latest"
    );

    // Should return the actual version number (2), not -1
    assertEquals(2, deletedVersion, "Delete latest should return actual version 2");

    // Verify version 2 is deleted
    waitUntilSoftDeleted(leader, subject, 2, 10000);
  }

  /**
   * Test permanent delete through follower.
   */
  @Test
  public void testFollowerForwardsPermanentDelete() throws Exception {
    int port1 = choosePort();
    int port2 = choosePort();

    if (port2 < port1) {
      int tmp = port2;
      port2 = port1;
      port1 = tmp;
    }

    leader = new RestApp(port1, null, brokerList, KAFKASTORE_TOPIC,
                         CompatibilityLevel.NONE.name, true, null);
    leader.start();

    follower = new RestApp(port2, null, brokerList, KAFKASTORE_TOPIC,
                           CompatibilityLevel.NONE.name, true, null);
    follower.start();

    assertTrue(leader.isLeader(), "First instance should be the leader");
    assertFalse(follower.isLeader(), "Second instance should be the follower");

    String subject = "test-subject-permanent";
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);

    int schemaId = leader.restClient.registerSchema(schema, subject);
    waitUntilSchemaReplicatedToFollower(follower, schemaId, schema, 10000);

    // Soft delete first
    int softDeletedVersion = follower.restClient.deleteSchemaVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        subject,
        "1"
    );
    assertEquals(1, softDeletedVersion);

    // Wait for soft delete to replicate
    waitUntilSoftDeleted(leader, subject, 1, 10000);

    // Permanent delete through follower
    int permanentDeletedVersion = follower.restClient.deleteSchemaVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        subject,
        "1",
        true  // permanent delete
    );
    assertEquals(1, permanentDeletedVersion);

    // Verify permanent deletion - schema should not be retrievable even with deleted flag
    waitUntilHardDeleted(leader, subject, 1, 10000);
  }

  /**
   * Test that follower correctly handles error when trying to delete non-existent version.
   * The error should come from the leader, not from stale follower cache.
   *
   * Test scenario:
   * 1. Register a schema on the leader
   * 2. Wait for follower to replicate
   * 3. Disconnect follower from Kafka (simulating stale cache)
   * 4. Leader deletes the subject
   * 5. Follower tries to delete - should forward to leader and get "not found" error
   */
  @Test
  public void testFollowerForwardsDeleteNonExistentVersion() throws Exception {
    int port1 = choosePort();
    int port2 = choosePort();

    if (port2 < port1) {
      int tmp = port2;
      port2 = port1;
      port1 = tmp;
    }

    leader = new RestApp(port1, null, brokerList, KAFKASTORE_TOPIC,
                         CompatibilityLevel.NONE.name, true, null);
    leader.start();

    follower = new RestApp(port2, null, brokerList, KAFKASTORE_TOPIC,
                           CompatibilityLevel.NONE.name, true, null);
    follower.start();

    assertTrue(leader.isLeader(), "First instance should be the leader");
    assertFalse(follower.isLeader(), "Second instance should be the follower");

    String subject = "test-subject-nonexistent";
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Register a schema on the leader
    int schemaId = leader.restClient.registerSchema(schema, subject);
    assertEquals(1, schemaId, "First schema should have ID 1");

    // Wait for follower to replicate the schema
    waitUntilSchemaReplicatedToFollower(follower, schemaId, schema, 10000);

    // Disconnect follower from Kafka to create stale cache
    ((io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry) follower.schemaRegistry())
        .getKafkaStore().close();

    // Leader deletes the subject while follower is disconnected
    leader.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject);

    // Follower tries to delete a version - should forward to leader despite stale cache
    // and get error from leader indicating subject not found
    try {
      follower.restClient.deleteSchemaVersion(
          RestService.DEFAULT_REQUEST_PROPERTIES,
          subject,
          "1"
      );
      fail("Should throw exception for non-existent subject");
    } catch (RestClientException e) {
      // Expected - should get error from leader indicating subject not found
      // This proves the follower forwarded to the leader rather than using stale cache
      assertEquals(40406, e.getErrorCode(),
                   "Should get subject not found error code from leader");
    }
  }

  // Helper methods

  private void waitUntilSchemaReplicatedToFollower(
      RestApp follower,
      int schemaId,
      String expectedSchema,
      int timeoutMs) throws Exception {

    Callable<Boolean> condition = () -> {
      try {
        String actualSchema = follower.restClient.getId(schemaId).getSchemaString();
        return expectedSchema.equals(actualSchema);
      } catch (Exception e) {
        return false;
      }
    };

    TestUtils.waitUntilTrue(condition, timeoutMs,
                           "Schema should be replicated to follower");
  }

  private void waitUntilSoftDeleted(
      RestApp app,
      String subject,
      int version,
      int timeoutMs) throws Exception {

    Callable<Boolean> condition = () -> {
      try {
        // Try to get the version without deleted flag - should fail
        return app.restClient.getVersion(subject, version, true).getDeleted();
      } catch (Exception e) {
        return false;
      }
    };

    TestUtils.waitUntilTrue(condition, timeoutMs,
                           "Schema version should be marked as deleted");
  }

  private void waitUntilHardDeleted(
      RestApp app,
      String subject,
      int version,
      int timeoutMs) throws Exception {

    Callable<Boolean> condition = () -> {
      try {
        // Try to get the version even with deleted flag - should fail after permanent delete
        app.restClient.getVersion(subject, version, true);
        return false;
      } catch (RestClientException e) {
        // Should get "subject not found" error
        return e.getErrorCode() == 40401;
      } catch (Exception e) {
        return false;
      }
    };

    TestUtils.waitUntilTrue(condition, timeoutMs,
                           "Schema version should be permanently deleted");
  }
}
