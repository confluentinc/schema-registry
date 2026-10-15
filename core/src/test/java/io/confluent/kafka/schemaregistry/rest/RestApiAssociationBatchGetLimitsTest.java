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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  private static AssociationGetRequest getRequestFor(String resourceId) {
    return new AssociationGetRequest(resourceId, "topic", Collections.emptyList(), null);
  }

  @Test
  public void testIncludeSchemasFalseExemptFromLimit() throws Exception {
    int numItems = 11;
    List<AssociationGetRequest> queries = new ArrayList<>();
    for (int i = 0; i < numItems; i++) {
      queries.add(getRequestFor("get-limit-exempt-" + i + "-id"));
    }
    AssociationBatchGetRequest getRequest = new AssociationBatchGetRequest(queries);

    AssociationBatchResponse response = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, false, getRequest);
    assertEquals(numItems, response.getResults().size());
  }

  @Test
  public void testIncludeSchemasTrueWithinLimitSucceeds() throws Exception {
    int numItems = 1;
    List<AssociationGetRequest> queries = new ArrayList<>();
    for (int i = 0; i < numItems; i++) {
      String resourceName = "get-limit-within-" + i;
      createResource(restApp, resourceName, i);
      queries.add(getRequestFor(resourceName + "-id"));
    }
    AssociationBatchGetRequest getRequest = new AssociationBatchGetRequest(queries);

    AssociationBatchResponse response = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, true, getRequest);
    assertEquals(numItems, response.getResults().size());
    for (AssociationResult result : response.getResults()) {
      assertNull(result.getError());
      assertNotNull(result.getResult().getAssociations().get(0).getSchema());
    }
  }

  @Test
  public void testIncludeSchemasTrueExceedsMaxAssociationNumPerGetBatch() throws Exception {
    int numItems = 2;
    List<AssociationGetRequest> queries = new ArrayList<>();
    for (int i = 0; i < numItems; i++) {
      queries.add(getRequestFor("get-limit-exceeded-" + i + "-id"));
    }
    AssociationBatchGetRequest getRequest = new AssociationBatchGetRequest(queries);

    RestClientException e = assertThrows(RestClientException.class, () ->
        restApp.restClient.batchGetAssociations(
            RestService.DEFAULT_REQUEST_PROPERTIES, true, getRequest));
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
    assertTrue(e.getMessage().contains("2 topics"));
    assertTrue(e.getMessage().contains("maximum of 1"));
  }
}
