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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationDeleteOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOpRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpsertOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import java.util.Arrays;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class RestApiAssociationBatchLimitsTest extends ClusterTestHarness {

  public RestApiAssociationBatchLimitsTest() {
    super(1, true);
  }

  @Override
  public Properties getSchemaRegistryProperties() throws Exception {
    Properties props = super.getSchemaRegistryProperties();
    props.put(SchemaRegistryConfig.ASSOCIATION_BATCH_MUTATE_LIMITS_ENABLED_CONFIG, "true");
    return props;
  }

  private static RegisterSchemaRequest schema(String schemaString) {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema(schemaString);
    return schema;
  }

  private static AssociationOp create(String associationType, String recordName) {
    return new AssociationCreateOp(null, associationType, null, null,
        schema("{\"type\":\"record\",\"name\":\"" + recordName + "\",\"fields\":[]}"), null);
  }

  private static AssociationOp delete(String associationType) {
    return new AssociationDeleteOp(associationType, true);
  }

  private static AssociationOpRequest topic(String name, AssociationOp... ops) {
    return new AssociationOpRequest(name, "default", name + "-id", "topic", Arrays.asList(ops));
  }

  private AssociationBatchResponse mutate(boolean dryRun, AssociationOpRequest... topics)
      throws Exception {
    return restApp.restClient.mutateAssociations(RestService.DEFAULT_REQUEST_PROPERTIES, null,
        dryRun, new AssociationBatchRequest(Arrays.asList(topics)));
  }

  private static void assertAllSucceeded(AssociationBatchResponse response, int expected) {
    assertEquals(expected, response.getResults().size());
    for (AssociationResult result : response.getResults()) {
      assertNull(result.getError());
    }
  }

  private static void assertRejected(Executable call, String expectedMessage) {
    RestClientException e = assertThrows(RestClientException.class, call);
    assertEquals(422, e.getStatus());
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
    assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
  }

  private static String padded(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append('x');
    }
    return sb.toString();
  }

  @Test
  public void testOneTopicWithKeyAndValueCreatesSucceeds() throws Exception {
    assertAllSucceeded(mutate(false,
        topic("key-and-value", create("key", "KeyKv"), create("value", "ValueKv"))), 1);
  }

  @Test
  public void testTopicCountIsNotLimited() throws Exception {
    assertAllSucceeded(mutate(false,
        topic("two-topics-a", create("value", "TwoA")),
        topic("two-topics-b", create("value", "TwoB"))), 2);
  }

  @Test
  public void testThreeCreatesAcrossTopicsRejected() {
    assertRejected(() -> mutate(false,
        topic("three-creates-a", create("value", "ThreeA")),
        topic("three-creates-b", create("value", "ThreeB")),
        topic("three-creates-c", create("value", "ThreeC"))),
        "has 3 CREATE operations");
  }

  @Test
  public void testThreeUpsertsWithoutSchemasRejected() {
    // Rejected before anything runs, so the referenced subjects don't need to exist.
    assertRejected(() -> mutate(false,
        topic("upserts-a",
            new AssociationUpsertOp("shared-key", "key", null, null, null, null),
            new AssociationUpsertOp("shared-value", "value", null, null, null, null)),
        topic("upserts-b",
            new AssociationUpsertOp("shared-value", "value", null, null, null, null))),
        "has 3 UPSERT operations");
  }

  @Test
  public void testDryRunIsLimited() {
    assertRejected(() -> mutate(true,
        topic("dry-run-a", create("value", "DryA")),
        topic("dry-run-b", create("value", "DryB")),
        topic("dry-run-c", create("value", "DryC"))),
        "has 3 CREATE operations");
  }

  @Test
  public void testCreatesAndDeletesInTheSameRequestSucceed() throws Exception {
    assertAllSucceeded(mutate(false,
        topic("mixed-existing", create("key", "MixedKey"), create("value", "MixedValue"))), 1);

    assertAllSucceeded(mutate(false,
        topic("mixed-new", create("key", "NewKey"), create("value", "NewValue")),
        topic("mixed-existing", delete("key"), delete("value"))), 2);
  }

  @Test
  public void testThreeDeletesOfExistingAssociationsRejected() throws Exception {
    assertAllSucceeded(mutate(false,
        topic("deletes-a", create("key", "DelKeyA"), create("value", "DelValueA"))), 1);
    assertAllSucceeded(mutate(false, topic("deletes-b", create("value", "DelValueB"))), 1);

    assertRejected(() -> mutate(false,
        topic("deletes-a", delete("key"), delete("value")),
        topic("deletes-b", delete("value"))),
        "at least 3 DELETE operations");
  }

  @Test
  public void testDeletesOfMissingAssociationsAreNotCounted() throws Exception {
    AssociationBatchResponse response = mutate(false,
        topic("missing-a", delete("key"), delete("value")),
        topic("missing-b", delete("key"), delete("value")),
        topic("missing-c", delete("value")));
    assertEquals(3, response.getResults().size());
  }

  @Test
  public void testInvalidCreatesFailOnTheirOwnInsteadOfCounting() throws Exception {
    AssociationOpRequest badResourceType = new AssociationOpRequest("invalid-create-c", "default",
        "invalid-create-c-id", "table", Arrays.asList(create("value", "InvalidC")));

    AssociationBatchResponse response = mutate(false,
        topic("invalid-create-a", create("value", "InvalidA")),
        badResourceType,
        topic("invalid-create-b", create("value", "InvalidB")));
    assertEquals(3, response.getResults().size());
    assertNull(response.getResults().get(0).getError());
    assertEquals(Errors.INVALID_ASSOCIATION_ERROR_CODE,
        response.getResults().get(1).getError().getErrorCode());
    assertNull(response.getResults().get(2).getError());
  }

  @Test
  public void testInvalidDeletesFailOnTheirOwnInsteadOfCounting() throws Exception {
    assertAllSucceeded(mutate(false,
        topic("invalid-delete-a", create("key", "InvDelKeyA"), create("value", "InvDelValueA"))),
        1);
    assertAllSucceeded(mutate(false,
        topic("invalid-delete-b", create("value", "InvDelValueB"))), 1);
    AssociationOpRequest badResourceType = new AssociationOpRequest("invalid-delete-b", "default",
        "invalid-delete-b-id", "table", Arrays.asList(delete("value")));

    AssociationBatchResponse response = mutate(false,
        topic("invalid-delete-a", delete("key"), delete("value")),
        badResourceType);
    assertEquals(2, response.getResults().size());
    assertNull(response.getResults().get(0).getError());
    assertEquals(Errors.INVALID_ASSOCIATION_ERROR_CODE,
        response.getResults().get(1).getError().getErrorCode());
  }

  @Test
  public void testPayloadLimitAppliesToASingleCreate() {
    assertRejected(() -> mutate(false, topic("oversized",
        new AssociationCreateOp(null, "value", null, null,
            schema("\"" + padded(1_100_000) + "\""), null))),
        "bytes per batch");
  }
}
