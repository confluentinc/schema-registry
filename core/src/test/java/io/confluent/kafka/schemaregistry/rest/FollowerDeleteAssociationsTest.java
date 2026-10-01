/*
 * Copyright 2026 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for deleting associations through a follower.
 */
public class FollowerDeleteAssociationsTest extends ClusterTestHarness {

  private RestApp leader;
  private RestApp follower;

  @AfterEach
  public void tearDownApps() throws Exception {
    if (follower != null) {
      follower.stop();
    }
    if (leader != null) {
      leader.stop();
    }
  }

  @Test
  public void testFollowerRelaysAsyncCascadeDelete() throws Exception {
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

    assertTrue(leader.isLeader(), "First instance should be the leader");
    assertFalse(follower.isLeader(), "Second instance should be the follower");

    String resourceId = "follower-async-123";
    String subject = ":.default:follower-topic-key";
    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(TestUtils.getRandomCanonicalAvroString(1).get(0));
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "follower-topic",
        "default",
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                null, "key", LifecyclePolicy.STRONG, true, schemaRequest, null)
        )
    );
    leader.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // The follower looks up the association locally before forwarding
    TestUtils.waitUntilTrue(() -> !follower.restClient.getAssociationsByResourceId(
            RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
            Collections.singletonList("key"), null, 0, -1).isEmpty(),
        10000, "Association should be replicated to follower");

    assertEquals(202, rawDelete(follower, "/associations/resources/" + resourceId
        + "?resourceType=topic&associationType=key&cascadeLifecycle=true&async=true"));

    TestUtils.waitUntilTrue(() -> {
      try {
        leader.restClient.getAllVersions(
            RestService.DEFAULT_REQUEST_PROPERTIES, subject, true, false);
        return false;
      } catch (RestClientException e) {
        return e.getErrorCode() == 40401;
      }
    }, 30000, "Subject should be hard-deleted on the leader");
  }

  private int rawDelete(RestApp app, String path) throws Exception {
    URL url = new URL(app.restConnect + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("DELETE");
    conn.setConnectTimeout(10_000);
    conn.setReadTimeout(10_000);
    try {
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }
}
