/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.metrics;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationDeleteOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOpRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpsertOp;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_API_FAILURE_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_API_SUCCESS_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_GET_BATCH_SIZE;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_GET_FAILURE_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_GET_SUCCESS_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_BATCH_SIZE;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_MULTI_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_SINGLE_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_MUTATE_DELETE_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_MUTATE_FAILURE_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_MUTATE_SUCCESS_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_ASSOCIATION_BATCH_MUTATE_UPSERT_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_AVRO_SCHEMAS_CREATED;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_AVRO_SCHEMAS_DELETED;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_DELETED_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_MASTER_SLAVE_ROLE;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_REGISTERED_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class MetricsTest extends ClusterTestHarness {

  public MetricsTest() { super(1, true); }

  @Test
  public void testLeaderFollowerMetric() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName leaderFollowerMetricName =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_MASTER_SLAVE_ROLE);
    assertEquals(1.0,
            mBeanServer.getAttribute(leaderFollowerMetricName, METRIC_NAME_MASTER_SLAVE_ROLE));
  }

  @Test
  public void testSchemaCreatedCount() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName schemasCreated =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_REGISTERED_COUNT);
    ObjectName avroCreated =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_AVRO_SCHEMAS_CREATED);
    ObjectName schemasDeleted =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_DELETED_COUNT);
    ObjectName avroDeleted =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_AVRO_SCHEMAS_DELETED);

    RestService service = restApp.restClient;
    String subject = "testTopic1";
    int schemaCount = 3;
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(schemaCount);

    // test registering and verifying new schemas in subject1
    int schemaIdCounter = 1;
    for (int i = 0; i < schemaCount; i++) {
      String schema = schemas.get(i);
      TestUtils.registerAndVerifySchema(service, schema, schemaIdCounter++, subject);
    }

    // Re-registering schemas should not increase metrics.
    for (int i = 0; i < schemaCount; i++) {
      String schemaString = schemas.get(i);
      service.registerSchema(schemaString, subject);
    }

    // Deleting schemas should not modify create count.
    for (Integer i = 1; i < schemaIdCounter; i++) {
      assertEquals(i, service.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
                                                  subject, i.toString()));
    }

    assertEquals((double) schemaCount, mBeanServer.getAttribute(schemasCreated, METRIC_NAME_REGISTERED_COUNT));
    assertEquals((double) schemaCount, mBeanServer.getAttribute(avroCreated, METRIC_NAME_AVRO_SCHEMAS_CREATED));
    assertEquals((double) schemaCount, mBeanServer.getAttribute(schemasDeleted, METRIC_NAME_DELETED_COUNT));
    assertEquals((double) schemaCount, mBeanServer.getAttribute(avroDeleted, METRIC_NAME_AVRO_SCHEMAS_DELETED));
  }

  @Test
  public void testApiCallMetrics() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName apiSuccess =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_API_SUCCESS_COUNT);
    ObjectName apiFailure =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_API_FAILURE_COUNT);

    String subject = "testTopic1";
    int schemaCount = 3;
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(schemaCount);

    // test registering and verifying new schemas in subject
    int schemaIdCounter = 1;
    for (int i = 0; i < schemaCount; i++) {
      String schema = schemas.get(i);
      TestUtils.registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter++, subject);
    }

    // We perform two operations (register & get) for each schema
    assertEquals((double) schemaCount * 2, mBeanServer.getAttribute(apiSuccess, METRIC_NAME_API_SUCCESS_COUNT));
    assertEquals(0.0, mBeanServer.getAttribute(apiFailure, METRIC_NAME_API_FAILURE_COUNT));

    try {
      restApp.restClient.getId(100);
      fail("Schema lookup with missing ID expected to fail");
    } catch (RestClientException rce) {
      assertEquals(1.0, mBeanServer.getAttribute(apiFailure, METRIC_NAME_API_FAILURE_COUNT));
    }
  }

  @Test
  public void testAssociationBatchGetMetrics() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName batchGetSuccess = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_GET_SUCCESS_COUNT);
    ObjectName batchGetFailure = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_GET_FAILURE_COUNT);
    ObjectName batchGetBatchSize = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_GET_BATCH_SIZE);

    // batchGet has no per-item validation failure path today (an unknown resource ID
    // still returns a successful, empty result) -- so both a known and an unknown
    // resource ID count as association-level successes.
    String subject = "metricsBatchGetSubject";
    String resourceName = "metricsBatchGetTopic";
    String resourceNamespace = "default";
    String resourceId = "metrics-batch-get-id";
    restApp.restClient.registerSchema(
        TestUtils.getRandomCanonicalAvroString(1).get(0), subject);
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.WEAK, false, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    AssociationBatchGetRequest batchGetRequest = new AssociationBatchGetRequest(
        ImmutableList.of(
            new AssociationGetRequest(resourceId, "topic", null, null),
            new AssociationGetRequest("unknown-metrics-id", "topic", null, null)
        ));
    restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, false, batchGetRequest);

    assertEquals(2.0, mBeanServer.getAttribute(batchGetSuccess,
        METRIC_NAME_ASSOCIATION_BATCH_GET_SUCCESS_COUNT));
    assertEquals(0.0, mBeanServer.getAttribute(batchGetFailure,
        METRIC_NAME_ASSOCIATION_BATCH_GET_FAILURE_COUNT));
    assertEquals(2.0, mBeanServer.getAttribute(batchGetBatchSize,
        METRIC_NAME_ASSOCIATION_BATCH_GET_BATCH_SIZE));
  }

  @Test
  public void testAssociationBatchMutateMetrics() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName batchMutateSuccess = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_MUTATE_SUCCESS_COUNT);
    ObjectName batchMutateFailure = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_MUTATE_FAILURE_COUNT);

    String subject1 = "metricsMutateSubject1";
    String subject2 = "metricsMutateSubject2";
    String resourceName1 = "metricsMutateTopic1";
    String resourceName2 = "metricsMutateTopic2";
    String resourceNamespace = "default";
    String resourceId1 = "metrics-mutate-id-1";
    String resourceId2 = "metrics-mutate-id-2";
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    restApp.restClient.registerSchema(schemas.get(0), subject1);
    restApp.restClient.registerSchema(schemas.get(1), subject2);

    // Pre-create an association for resourceId1 so a second create for the same
    // resource/association-type conflicts, forcing a per-item failure in the batch below.
    AssociationCreateOrUpdateRequest existingRequest = new AssociationCreateOrUpdateRequest(
        resourceName1, resourceNamespace, resourceId1, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject1, "key", LifecyclePolicy.WEAK, false, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, existingRequest);

    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(new AssociationOpRequest(
        resourceName1, resourceNamespace, resourceId1, "topic",
        ImmutableList.of(new AssociationCreateOp(
            subject1, "key", LifecyclePolicy.STRONG, false, null, null))));
    requests.add(new AssociationOpRequest(
        resourceName2, resourceNamespace, resourceId2, "topic",
        ImmutableList.of(new AssociationCreateOp(
            subject2, "value", LifecyclePolicy.WEAK, false, null, null))));

    restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationBatchRequest(requests));

    assertEquals(1.0, mBeanServer.getAttribute(batchMutateSuccess,
        METRIC_NAME_ASSOCIATION_BATCH_MUTATE_SUCCESS_COUNT));
    assertEquals(1.0, mBeanServer.getAttribute(batchMutateFailure,
        METRIC_NAME_ASSOCIATION_BATCH_MUTATE_FAILURE_COUNT));
  }

  @Test
  public void testAssociationBatchMutateCompositionMetrics() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName createSingle = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_SINGLE_COUNT);
    ObjectName createMulti = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_MULTI_COUNT);
    ObjectName createBatchSize = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_BATCH_SIZE);
    ObjectName upsertCount = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_MUTATE_UPSERT_COUNT);
    ObjectName deleteCount = new ObjectName(
        "kafka.schema.registry:type=" + METRIC_NAME_ASSOCIATION_BATCH_MUTATE_DELETE_COUNT);

    String resourceNamespace = "default";
    String singleResourceId = "metrics-composition-single";
    String multiResourceId = "metrics-composition-multi";
    String upsertResourceId = "metrics-composition-upsert";
    String deleteResourceId = "metrics-composition-delete";

    String singleSubject = "metricsCompositionSingle";
    String multiKeySubject = "metricsCompositionMultiKey";
    String multiValueSubject = "metricsCompositionMultiValue";
    String upsertSubject = "metricsCompositionUpsert";
    String deleteSubject = "metricsCompositionDelete";
    restApp.restClient.registerSchema(
        TestUtils.getRandomCanonicalAvroString(1).get(0), singleSubject);
    restApp.restClient.registerSchema(
        TestUtils.getRandomCanonicalAvroString(1).get(0), multiKeySubject);
    restApp.restClient.registerSchema(
        TestUtils.getRandomCanonicalAvroString(1).get(0), multiValueSubject);
    restApp.restClient.registerSchema(
        TestUtils.getRandomCanonicalAvroString(1).get(0), upsertSubject);
    restApp.restClient.registerSchema(
        TestUtils.getRandomCanonicalAvroString(1).get(0), deleteSubject);

    // Pre-create an association so the batch below can delete it.
    AssociationCreateOrUpdateRequest existingRequest = new AssociationCreateOrUpdateRequest(
        "metricsCompositionDeleteTopic", resourceNamespace, deleteResourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            deleteSubject, "value", LifecyclePolicy.WEAK, false, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, existingRequest);

    List<AssociationOpRequest> requests = new ArrayList<>();
    // A single-item create run -> batch of 1.
    requests.add(new AssociationOpRequest(
        "metricsCompositionSingleTopic", resourceNamespace, singleResourceId, "topic",
        ImmutableList.of(new AssociationCreateOp(
            singleSubject, "value", LifecyclePolicy.WEAK, false, null, null))));
    // Two adjacent create ops for the same resource -> one batch-of-many run, size 2.
    requests.add(new AssociationOpRequest(
        "metricsCompositionMultiTopic", resourceNamespace, multiResourceId, "topic",
        ImmutableList.of(
            new AssociationCreateOp(
                multiKeySubject, "key", LifecyclePolicy.WEAK, false, null, null),
            new AssociationCreateOp(
                multiValueSubject, "value", LifecyclePolicy.WEAK, false, null, null))));
    // An upsert op.
    requests.add(new AssociationOpRequest(
        "metricsCompositionUpsertTopic", resourceNamespace, upsertResourceId, "topic",
        ImmutableList.of(new AssociationUpsertOp(
            upsertSubject, "value", LifecyclePolicy.WEAK, false, null, null))));
    // A delete op for the association pre-created above.
    requests.add(new AssociationOpRequest(
        "metricsCompositionDeleteTopic", resourceNamespace, deleteResourceId, "topic",
        ImmutableList.of(new AssociationDeleteOp("value"))));

    restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationBatchRequest(requests));

    assertEquals(1.0, mBeanServer.getAttribute(createSingle,
        METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_SINGLE_COUNT));
    assertEquals(1.0, mBeanServer.getAttribute(createMulti,
        METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_MULTI_COUNT));
    // Value stat holds the most recently recorded run size; the multi-item run (size 2)
    // is processed after the single-item run (size 1), so 2.0 is the last value recorded.
    assertEquals(2.0, mBeanServer.getAttribute(createBatchSize,
        METRIC_NAME_ASSOCIATION_BATCH_MUTATE_CREATE_BATCH_SIZE));
    assertEquals(1.0, mBeanServer.getAttribute(upsertCount,
        METRIC_NAME_ASSOCIATION_BATCH_MUTATE_UPSERT_COUNT));
    assertEquals(1.0, mBeanServer.getAttribute(deleteCount,
        METRIC_NAME_ASSOCIATION_BATCH_MUTATE_DELETE_COUNT));
  }
}
