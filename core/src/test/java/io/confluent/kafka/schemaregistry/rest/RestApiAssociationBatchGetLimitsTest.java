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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOpRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Associations batchGet size limit
 * ({@link SchemaRegistryConfig#ASSOCIATION_BATCH_GET_LIMITS_ENABLED_CONFIG},
 * {@link SchemaRegistryConfig#MAX_ASSOCIATION_NUM_PER_GET_BATCH_CONFIG}), with enforcement
 * explicitly enabled at the shipped default threshold (10). A resourceId/resourceName can have
 * at most 2 stored associations (one key, one value), so a batch of N queries can never
 * retrieve more than 2N schemas -- when 2N is already within the configured max, the check
 * takes a cheap path and never touches the store; batches of more than 5 queries (2 * 5 = 10 is
 * the default max) fall through to resolving actual matches to get an exact count. The tests
 * below exercise both paths.
 */
public class RestApiAssociationBatchGetLimitsTest extends ClusterTestHarness {

  public RestApiAssociationBatchGetLimitsTest() {
    super(1, true);
  }

  @Override
  public Properties getSchemaRegistryProperties() throws Exception {
    Properties props = super.getSchemaRegistryProperties();
    props.put(SchemaRegistryConfig.ASSOCIATION_BATCH_GET_LIMITS_ENABLED_CONFIG, "true");
    return props;
  }

  // Creates one resource with a single "value" association carrying a schema. A single
  // association is exempt from the (disabled, in this test class) batchMutate limits, so this
  // always succeeds regardless of that unrelated feature.
  private static void createResource(RestApp restApp, String resourceName, int index)
      throws Exception {
    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema("{\"type\":\"record\",\"name\":\"R" + index + "\",\"fields\":[]}");
    AssociationCreateOp createOp = new AssociationCreateOp(
        null, "value", null, null, schemaRequest, null);
    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, "default", resourceName + "-id", "topic",
        Collections.singletonList(createOp));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));
    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertNull(response.getResults().get(0).getError());
  }

  private static AssociationGetRequest getRequestFor(String resourceName) {
    return new AssociationGetRequest(
        resourceName + "-id", "topic", Collections.emptyList(), null);
  }

  @Test
  public void testIncludeSchemasFalseExemptFromLimit() throws Exception {
    // 11 resources (1 association each) exceeds the default limit of 10, but includeSchemas is
    // false, so no schemas are retrieved and the limit is never enforced.
    int numResources = 11;
    List<AssociationGetRequest> queries = new ArrayList<>();
    for (int i = 0; i < numResources; i++) {
      String resourceName = "get-limit-exempt-" + i;
      createResource(restApp, resourceName, i);
      queries.add(getRequestFor(resourceName));
    }
    AssociationBatchGetRequest getRequest = new AssociationBatchGetRequest(queries);

    AssociationBatchResponse response = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, false, getRequest);
    assertEquals(numResources, response.getResults().size());
    for (AssociationResult result : response.getResults()) {
      assertNull(result.getError());
    }
  }

  @Test
  public void testSmallBatchTakesCheapPathRegardlessOfActualCount() throws Exception {
    // 5 queries (1 association each): worst case is 5 * 2 = 10 schemas, which already fits the
    // default max of 10, so the cheap request-size-only bound applies and the request is
    // accepted without the check ever resolving any associations from the store.
    int numResources = 5;
    List<AssociationGetRequest> queries = new ArrayList<>();
    for (int i = 0; i < numResources; i++) {
      String resourceName = "get-limit-cheap-path-" + i;
      createResource(restApp, resourceName, i);
      queries.add(getRequestFor(resourceName));
    }
    AssociationBatchGetRequest getRequest = new AssociationBatchGetRequest(queries);

    AssociationBatchResponse response = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, true, getRequest);
    assertEquals(numResources, response.getResults().size());
    for (AssociationResult result : response.getResults()) {
      assertNull(result.getError());
      assertNotNull(result.getResult().getAssociations().get(0).getSchema());
    }
  }

  @Test
  public void testIncludeSchemasTrueWithinLimitSucceeds() throws Exception {
    // 10 queries: worst case (20) exceeds the default max (10), so this falls through to the
    // exact, store-resolving count -- which finds exactly 10 (at, not over, the limit).
    int numResources = 10;
    List<AssociationGetRequest> queries = new ArrayList<>();
    for (int i = 0; i < numResources; i++) {
      String resourceName = "get-limit-within-" + i;
      createResource(restApp, resourceName, i);
      queries.add(getRequestFor(resourceName));
    }
    AssociationBatchGetRequest getRequest = new AssociationBatchGetRequest(queries);

    AssociationBatchResponse response = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, true, getRequest);
    assertEquals(numResources, response.getResults().size());
    for (AssociationResult result : response.getResults()) {
      assertNull(result.getError());
      assertNotNull(result.getResult().getAssociations().get(0).getSchema());
    }
  }

  @Test
  public void testIncludeSchemasTrueExceedsMaxAssociationNumPerGetBatch() throws Exception {
    // 11 queries: worst case (22) exceeds the default max (10), so this falls through to the
    // exact count -- which finds 11, over the limit, and rejects the whole request.
    int numResources = 11;
    List<AssociationGetRequest> queries = new ArrayList<>();
    for (int i = 0; i < numResources; i++) {
      String resourceName = "get-limit-exceeded-" + i;
      createResource(restApp, resourceName, i);
      queries.add(getRequestFor(resourceName));
    }
    AssociationBatchGetRequest getRequest = new AssociationBatchGetRequest(queries);

    RestClientException e = assertThrows(RestClientException.class, () ->
        restApp.restClient.batchGetAssociations(
            RestService.DEFAULT_REQUEST_PROPERTIES, true, getRequest));
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
  }
}
