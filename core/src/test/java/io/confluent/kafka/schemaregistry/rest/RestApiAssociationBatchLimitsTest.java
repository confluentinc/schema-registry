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
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOpRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Associations batchMutate size/count limits
 * ({@link SchemaRegistryConfig#ASSOCIATION_BATCH_MUTATE_LIMITS_ENABLED_CONFIG},
 * {@link SchemaRegistryConfig#MAX_ASSOCIATION_NUM_PER_MUTATE_BATCH_CONFIG},
 * {@link SchemaRegistryConfig#MAX_ASSOCIATION_MUTATE_ENTRY_PAYLOAD_BYTES_CONFIG}, and
 * {@link SchemaRegistryConfig#MAX_ASSOCIATION_MUTATE_BATCH_PAYLOAD_BYTES_CONFIG}), with enforcement
 * explicitly enabled at the shipped default thresholds (10 / 1000 bytes / 10000 bytes). The
 * byte limits are measured as the full JSON serialized size of the request/entry, so they must
 * stay well above the fixed JSON structural overhead (resourceName/resourceNamespace/resourceId/
 * resourceType/opType/associationType/etc.) of even a minimal entry -- otherwise every non-exempt
 * batch would be rejected regardless of actual content size.
 */
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
    // A single association is exempt regardless of size (protects Flink-style calls).
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
    // No association in the batch carries an inline schema, so the whole batch is exempt
    // regardless of count (protects Kafka Cluster Linking-style calls).
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
  public void testRealisticBatchAtMaxCountSucceedsUnderDefaultLimits() throws Exception {
    // Regression guard: the byte limits are measured as the full JSON serialized size of the
    // request/entry (resourceName/resourceNamespace/resourceId/resourceType/opType/
    // associationType/etc., not just the raw subject+schema text), so a "realistic" default
    // must stay comfortably above that fixed structural overhead. A batch of 10 (the default
    // count limit) entries, each with a modest, real-world-sized schema, must not be rejected --
    // if either byte default regresses to a value too close to (or below) that per-entry JSON
    // overhead, this fails.
    String realisticSchema = "{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":["
        + "{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"customerId\",\"type\":\"string\"},"
        + "{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}";
    int numResources = 10;
    List<AssociationOpRequest> requests = new ArrayList<>();
    for (int i = 0; i < numResources; i++) {
      requests.add(createOpRequest("orders-topic-" + i, realisticSchema));
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
  public void testExceedsMaxAssociationNumPerBatch() throws Exception {
    // 11 associations with inline schemas, each individually tiny, exceeds the default
    // MAX_ASSOCIATION_NUM_PER_MUTATE_BATCH of 10.
    List<AssociationOpRequest> requests = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      requests.add(createOpRequest("num-limit-" + i, "{}"));
    }
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(requests);

    RestClientException e = assertThrows(RestClientException.class, () ->
        restApp.restClient.mutateAssociations(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest));
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
  }

  @Test
  public void testExceedsMaxAssociationEntryPayloadBytes() throws Exception {
    // One resource entry has an association payload well over the default
    // MAX_ASSOCIATION_MUTATE_ENTRY_PAYLOAD_BYTES of 1000 bytes, while the total count (2) and
    // the cumulative payload stay well under the other two limits.
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(createOpRequest("entry-limit-big", "\"" + padded(1500) + "\""));
    requests.add(createOpRequest("entry-limit-small", "{}"));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(requests);

    RestClientException e = assertThrows(RestClientException.class, () ->
        restApp.restClient.mutateAssociations(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest));
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
  }

  @Test
  public void testExceedsMaxAssociationBatchPayloadBytes() throws Exception {
    // 9 resource entries, each under the default association count limit (10), whose
    // cumulative payload exceeds the default MAX_ASSOCIATION_MUTATE_BATCH_PAYLOAD_BYTES of
    // 10000 bytes.
    List<AssociationOpRequest> requests = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      requests.add(createOpRequest("batch-limit-" + i, "\"" + padded(1200) + "\""));
    }
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(requests);

    RestClientException e = assertThrows(RestClientException.class, () ->
        restApp.restClient.mutateAssociations(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest));
    assertEquals(Errors.ASSOCIATION_BATCH_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
  }
}
