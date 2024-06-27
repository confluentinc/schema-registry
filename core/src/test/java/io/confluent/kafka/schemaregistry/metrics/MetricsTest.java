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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;

import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_API_FAILURE_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_API_SUCCESS_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_AVRO_SCHEMAS_CREATED;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_AVRO_SCHEMAS_DELETED;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_DELETED_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_MASTER_SLAVE_ROLE;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_REGISTERED_COUNT;
import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_TOMBSTONED_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MetricsTest extends ClusterTestHarness {

  public MetricsTest() { super(1, true); }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

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
    ObjectName schemasTombstoned =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_TOMBSTONED_COUNT);
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
    assertEquals((double) schemaCount, mBeanServer.getAttribute(schemasTombstoned, METRIC_NAME_TOMBSTONED_COUNT));
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
}
