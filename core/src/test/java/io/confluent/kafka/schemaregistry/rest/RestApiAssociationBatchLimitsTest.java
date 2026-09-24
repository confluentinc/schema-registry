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
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationDeleteOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOpRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

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

  private static AssociationOpRequest createOpRequest(String resourceName, String schema) {
    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(schema);
    AssociationCreateOp createOp = new AssociationCreateOp(
        null, "value", null, null, schemaRequest, null);
    return new AssociationOpRequest(
        resourceName, "default", resourceName + "-id", "topic",
        java.util.Collections.singletonList(createOp));
  }

  private static String padded(int bytes) {
    StringBuilder sb = new StringBuilder(bytes);
    for (int i = 0; i < bytes; i++) {
      sb.append('x');
    }
    return sb.toString();
  }

  @Test
  public void testSingleAssociationExemptFromAllLimits() throws Exception {
    String hugeSchema = "{\"type\":\"record\",\"name\":\"Huge\",\"fields\":["
        + "{\"name\":\"f\",\"type\":\"string\",\"default\":\"" + padded(1200) + "\"}]}";
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(createOpRequest("single-assoc-exempt", hugeSchema));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(requests);

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertEquals(1, response.getResults().size());
    assertNull(response.getResults().get(0).getError());
  }

  @Test
  public void testNoInlineSchemaExemptFromAllLimits() throws Exception {
    int numResources = 12;
    List<String> subjects = TestUtils.getRandomCanonicalAvroString(numResources);
    List<AssociationOpRequest> requests = new ArrayList<>();
    for (int i = 0; i < numResources; i++) {
      String subject = "no-schema-exempt-subject-" + i;
      restApp.restClient.registerSchema(subjects.get(i), subject);
      AssociationCreateOp createOp = new AssociationCreateOp(
          subject, "value", LifecyclePolicy.WEAK, false, null, null);
      requests.add(new AssociationOpRequest(
          "no-schema-exempt-" + i, "default", "no-schema-exempt-" + i + "-id", "topic",
          java.util.Collections.singletonList(createOp)));
    }
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(requests);

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertEquals(numResources, response.getResults().size());
    for (AssociationResult result : response.getResults()) {
      assertNull(result.getError());
    }
  }

  @Test
  public void testRealisticSingleTopicWithKeyAndValueSucceedsUnderDefaultLimits() throws Exception {
    // A single topic (1 resourceId) may carry both a key and a value association; that is a
    // batchSize of 1 topic, not 2, so it succeeds even though the max is 1 topic per batch.
    String valueSchema = "{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":["
        + "{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"customerId\",\"type\":\"string\"},"
        + "{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}";
    String keySchema = "{\"type\":\"record\",\"name\":\"OrderKey\",\"fields\":["
        + "{\"name\":\"orderId\",\"type\":\"string\"}]}";
    RegisterSchemaRequest valueSchemaRequest = new RegisterSchemaRequest();
    valueSchemaRequest.setSchema(valueSchema);
    RegisterSchemaRequest keySchemaRequest = new RegisterSchemaRequest();
    keySchemaRequest.setSchema(keySchema);

    AssociationOpRequest opRequest = new AssociationOpRequest(
        "orders-topic", "default", "orders-topic-id", "topic",
        java.util.Arrays.asList(
            new AssociationCreateOp(null, "value", null, null, valueSchemaRequest, null),
            new AssociationCreateOp(null, "key", null, null, keySchemaRequest, null)));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertEquals(1, response.getResults().size());
    assertNull(response.getResults().get(0).getError());
  }

  @Test
  public void testExceedsMaxAssociationNumPerBatch() throws Exception {
    List<AssociationOpRequest> requests = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      requests.add(createOpRequest("num-limit-" + i, "{}"));
    }
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(requests);

    RestClientException e = assertThrows(RestClientException.class, () ->
        restApp.restClient.mutateAssociations(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest));
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
    assertTrue(e.getMessage().contains("2 topics"));
    assertTrue(e.getMessage().contains("maximum of 1 topics"));
  }

  @Test
  public void testExceedsMaxAssociationEntryPayloadBytes() throws Exception {
    // A single topic (1 resourceId, so it doesn't trip the topic-count limit) whose combined
    // key+value payload exceeds the per-entry byte limit.
    RegisterSchemaRequest valueSchemaRequest = new RegisterSchemaRequest();
    valueSchemaRequest.setSchema("\"" + padded(1500) + "\"");
    RegisterSchemaRequest keySchemaRequest = new RegisterSchemaRequest();
    keySchemaRequest.setSchema("{}");
    AssociationOpRequest opRequest = new AssociationOpRequest(
        "entry-limit-big", "default", "entry-limit-big-id", "topic",
        java.util.Arrays.asList(
            new AssociationCreateOp(null, "value", null, null, valueSchemaRequest, null),
            new AssociationCreateOp(null, "key", null, null, keySchemaRequest, null)));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    RestClientException e = assertThrows(RestClientException.class, () ->
        restApp.restClient.mutateAssociations(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest));
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
    // The violating entry's resourceId is called out explicitly in the message.
    assertTrue(e.getMessage().contains("resourceId 'entry-limit-big-id'"));
    assertTrue(e.getMessage().contains("per resource entry"));
  }

  @Test
  public void testExceedsMaxAssociationBatchPayloadBytes() throws Exception {
    // A single topic whose combined key+value payload exceeds the cumulative batch byte limit
    // (checked ahead of the per-entry limit, so this fires the batch-level violation).
    RegisterSchemaRequest valueSchemaRequest = new RegisterSchemaRequest();
    valueSchemaRequest.setSchema("\"" + padded(6000) + "\"");
    RegisterSchemaRequest keySchemaRequest = new RegisterSchemaRequest();
    keySchemaRequest.setSchema("\"" + padded(6000) + "\"");
    AssociationOpRequest opRequest = new AssociationOpRequest(
        "batch-limit-big", "default", "batch-limit-big-id", "topic",
        java.util.Arrays.asList(
            new AssociationCreateOp(null, "value", null, null, valueSchemaRequest, null),
            new AssociationCreateOp(null, "key", null, null, keySchemaRequest, null)));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    RestClientException e = assertThrows(RestClientException.class, () ->
        restApp.restClient.mutateAssociations(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest));
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
    assertTrue(e.getMessage().contains("bytes per batch"));
  }

  @Test
  public void testDeleteOnlyBatchExemptFromAllLimits() throws Exception {
    int numResources = 5;
    List<String> subjects = TestUtils.getRandomCanonicalAvroString(numResources);
    List<AssociationOpRequest> deleteRequests = new ArrayList<>();
    for (int i = 0; i < numResources; i++) {
      String subject = "delete-only-subject-" + i;
      restApp.restClient.registerSchema(subjects.get(i), subject);
      AssociationCreateOp createOp = new AssociationCreateOp(
          subject, "value", LifecyclePolicy.WEAK, false, null, null);
      AssociationOpRequest createRequest = new AssociationOpRequest(
          "delete-only-" + i, "default", "delete-only-" + i + "-id", "topic",
          Collections.singletonList(createOp));
      AssociationBatchResponse createResponse = restApp.restClient.mutateAssociations(
          RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
          new AssociationBatchRequest(Collections.singletonList(createRequest)));
      assertNull(createResponse.getResults().get(0).getError());

      deleteRequests.add(new AssociationOpRequest(
          "delete-only-" + i, "default", "delete-only-" + i + "-id", "topic",
          Collections.singletonList(new AssociationDeleteOp("value"))));
    }

    // A batch of only delete ops has no inline schemas, so it is exempt from the batch size
    // limit even though it well exceeds the configured maximum of 1 topic per batch.
    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationBatchRequest(deleteRequests));
    assertEquals(numResources, response.getResults().size());
    for (AssociationResult result : response.getResults()) {
      assertNull(result.getError());
    }
  }
}
