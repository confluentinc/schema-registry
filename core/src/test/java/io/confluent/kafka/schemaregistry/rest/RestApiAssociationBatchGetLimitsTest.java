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
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOpRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

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

  private void createTopic(String name, String... associationTypes) throws Exception {
    List<AssociationOp> ops = new ArrayList<>();
    for (String associationType : associationTypes) {
      RegisterSchemaRequest schema = new RegisterSchemaRequest();
      schema.setSchema("{\"type\":\"record\",\"name\":\"R" + name.replace("-", "")
          + associationType + "\",\"fields\":[]}");
      ops.add(new AssociationCreateOp(null, associationType, null, null, schema, null));
    }
    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationBatchRequest(Collections.singletonList(
            new AssociationOpRequest(name, "default", name + "-id", "topic", ops))));
    assertNull(response.getResults().get(0).getError());
  }

  private static AssociationGetRequest byId(String name) {
    return new AssociationGetRequest(name + "-id", "topic", null, null);
  }

  private static AssociationGetRequest byName(String name) {
    return new AssociationGetRequest(name, "default", "topic", null, null);
  }

  private AssociationBatchResponse batchGet(boolean includeSchemas,
      AssociationGetRequest... queries) throws Exception {
    return restApp.restClient.batchGetAssociations(RestService.DEFAULT_REQUEST_PROPERTIES,
        includeSchemas, new AssociationBatchGetRequest(Arrays.asList(queries)));
  }

  private static void assertRejected(Executable call, String expectedMessage) {
    RestClientException e = assertThrows(RestClientException.class, call);
    assertEquals(422, e.getStatus());
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
    assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
  }

  @Test
  public void testRetrievingTwoAssociationsSucceeds() throws Exception {
    createTopic("get-two", "key", "value");

    AssociationBatchResponse response = batchGet(true, byId("get-two"), byId("get-missing"));
    assertEquals(2, response.getResults().size());
    AssociationResult result = response.getResults().get(0);
    assertNull(result.getError());
    assertEquals(2, result.getResult().getAssociations().size());
    for (AssociationInfo info : result.getResult().getAssociations()) {
      assertNotNull(info.getSchema());
    }
  }

  @Test
  public void testRetrievingThreeAssociationsRejectedWithoutSchemas() throws Exception {
    createTopic("get-three-a", "key", "value");
    createTopic("get-three-b", "value");

    assertRejected(() -> batchGet(false, byId("get-three-a"), byId("get-three-b")),
        "at least 3 associations");
  }

  @Test
  public void testEntriesWithoutAssociationsAreNotCounted() throws Exception {
    List<AssociationGetRequest> queries = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      queries.add(byId("get-none-" + i));
    }

    AssociationBatchResponse response = batchGet(true,
        queries.toArray(new AssociationGetRequest[0]));
    assertEquals(11, response.getResults().size());
  }

  @Test
  public void testEntriesByNameAreCounted() throws Exception {
    createTopic("get-by-name-a", "key", "value");
    createTopic("get-by-name-b", "value");

    assertRejected(() -> batchGet(false, byName("get-by-name-a"), byName("get-by-name-b")),
        "at least 3 associations");
  }

  @Test
  public void testPayloadLimit() {
    StringBuilder longName = new StringBuilder();
    for (int i = 0; i < 1_100_000; i++) {
      longName.append('x');
    }

    assertRejected(() -> batchGet(false, byName(longName.toString())), "bytes per batch");
  }
}
