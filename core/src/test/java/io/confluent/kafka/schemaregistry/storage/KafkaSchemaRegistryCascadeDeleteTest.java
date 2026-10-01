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

package io.confluent.kafka.schemaregistry.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;

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
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests for the background task behind async deleteAssociations. Calls runCascadeDelete
 * directly so each skip and failure branch can be checked.
 */
public class KafkaSchemaRegistryCascadeDeleteTest extends ClusterTestHarness {

  private static final String SUBJECT = ":.default:cascade-topic-key";
  private static final String RESOURCE_ID = "cascade-123";

  private RestApp follower;

  public KafkaSchemaRegistryCascadeDeleteTest() {
    super(1, true);
  }

  @AfterEach
  public void tearDownFollower() throws Exception {
    if (follower != null) {
      follower.stop();
    }
  }

  @Test
  public void testDeletesSubject() throws Exception {
    createStrongKeyAssociation();
    deleteAssociationOnly();
    double failures = cascadeFailureCount(registry());

    registry().runCascadeDelete(SUBJECT, RESOURCE_ID);

    assertTrue(isHardDeleted(restApp));
    assertEquals(failures, cascadeFailureCount(registry()));
  }

  @Test
  public void testSkipsReassociatedSubject() throws Exception {
    createStrongKeyAssociation();
    double failures = cascadeFailureCount(registry());

    // The association still exists, so deleteSubject refuses to delete the subject
    registry().runCascadeDelete(SUBJECT, RESOURCE_ID);

    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(SUBJECT));
    assertEquals(failures, cascadeFailureCount(registry()));
  }

  @Test
  public void testSkipsSubjectInImportMode() throws Exception {
    createStrongKeyAssociation();
    deleteAssociationOnly();
    restApp.restClient.setMode("IMPORT", SUBJECT, true);
    double failures = cascadeFailureCount(registry());

    registry().runCascadeDelete(SUBJECT, RESOURCE_ID);

    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(SUBJECT));
    assertEquals(failures, cascadeFailureCount(registry()));
  }

  @Test
  public void testCountsFailedDelete() throws Exception {
    createStrongKeyAssociation();
    deleteAssociationOnly();
    // deleteSubject rejects read-only subjects
    restApp.restClient.setMode("READONLY", SUBJECT, true);
    double failures = cascadeFailureCount(registry());

    registry().runCascadeDelete(SUBJECT, RESOURCE_ID);

    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(SUBJECT));
    assertEquals(failures + 1, cascadeFailureCount(registry()));
  }

  @Test
  public void testSkipsSubjectThatNoLongerExists() throws Exception {
    double failures = cascadeFailureCount(registry());

    registry().runCascadeDelete(":.default:never-registered-key", RESOURCE_ID);

    assertEquals(failures, cascadeFailureCount(registry()));
  }

  @Test
  public void testCountsDeleteSkippedOnFollower() throws Exception {
    createStrongKeyAssociation();
    deleteAssociationOnly();
    // Not eligible for leadership, so it stays a follower regardless of port order
    follower = new RestApp(choosePort(), null, brokerList, KAFKASTORE_TOPIC,
        CompatibilityLevel.NONE.name, false, null);
    follower.start();
    KafkaSchemaRegistry followerRegistry = (KafkaSchemaRegistry) follower.schemaRegistry();
    assertFalse(follower.isLeader(), "Second instance should be the follower");
    double failures = cascadeFailureCount(followerRegistry);

    followerRegistry.runCascadeDelete(SUBJECT, RESOURCE_ID);

    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(SUBJECT));
    assertEquals(failures + 1, cascadeFailureCount(followerRegistry));
  }

  @Test
  public void testDeletesVersionsRegisteredAfterRequest() throws Exception {
    createStrongKeyAssociation();
    deleteAssociationOnly();
    // A producer registers a new schema before the queued delete runs
    restApp.restClient.updateCompatibility(CompatibilityLevel.NONE.name, SUBJECT);
    restApp.restClient.registerSchema(TestUtils.getRandomCanonicalAvroString(1).get(0), SUBJECT);
    assertEquals(2, restApp.restClient.getAllVersions(SUBJECT).size());
    double failures = cascadeFailureCount(registry());

    registry().runCascadeDelete(SUBJECT, RESOURCE_ID);

    // Documented behavior: the queued delete removes the new version too
    assertTrue(isHardDeleted(restApp));
    assertEquals(failures, cascadeFailureCount(registry()));
  }

  @Test
  public void testInterruptedDeleteCountedOnce() throws Exception {
    createStrongKeyAssociation();
    deleteAssociationOnly();
    double failures = cascadeFailureCount(registry());

    // Simulates shutdownNow() interrupting the task
    Thread.currentThread().interrupt();
    try {
      registry().runCascadeDelete(SUBJECT, RESOURCE_ID);
    } finally {
      Thread.interrupted();
    }

    assertEquals(failures + 1, cascadeFailureCount(registry()));
    assertFalse(isHardDeleted(restApp));
  }

  @Test
  public void testWithRequestContextRestoresAndClearsTenant() {
    KafkaSchemaRegistry spyRegistry = spy(registry());
    AtomicBoolean ran = new AtomicBoolean();

    spyRegistry.withRequestContext("tenant1", () -> ran.set(true));

    assertTrue(ran.get());
    InOrder order = inOrder(spyRegistry);
    order.verify(spyRegistry).setTenant("tenant1");
    order.verify(spyRegistry).setTenant(null);
  }

  private KafkaSchemaRegistry registry() {
    return (KafkaSchemaRegistry) restApp.schemaRegistry();
  }

  private void createStrongKeyAssociation() throws Exception {
    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(TestUtils.getRandomCanonicalAvroString(1).get(0));
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "cascade-topic", "default", RESOURCE_ID, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "key", LifecyclePolicy.STRONG, true, schemaRequest, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
  }

  // Deletes the association entry but not the subject, as the async request does before
  // queuing the background task
  private void deleteAssociationOnly() throws Exception {
    KafkaSchemaRegistry registry = registry();
    registry.deleteAssociationEntries(registry.getAssociationsByResourceId(
        RESOURCE_ID, "topic", Collections.singletonList("key"), null));
  }

  private static boolean isHardDeleted(RestApp app) throws Exception {
    try {
      app.restClient.getAllVersions(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, true, false);
      return false;
    } catch (RestClientException e) {
      return e.getErrorCode() == 40401;
    }
  }

  private static double cascadeFailureCount(KafkaSchemaRegistry registry) {
    return registry.getMetricsContainer().getMetrics().metrics().entrySet().stream()
        .filter(e -> e.getKey().name().equals(
            MetricsContainer.METRIC_NAME_ASSOCIATION_DELETE_ASYNC_CASCADE_FAILURE_COUNT))
        .mapToDouble(e -> ((Number) e.getValue().metricValue()).doubleValue())
        .sum();
  }
}
