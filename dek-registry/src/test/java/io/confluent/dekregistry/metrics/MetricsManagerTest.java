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

package io.confluent.dekregistry.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.dekregistry.client.rest.entities.KeyType;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsManagerTest {

  private Metrics metrics;
  private MetricsManager metricsManager;

  @BeforeEach
  public void setUp() {
    metrics = new Metrics();
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    MetricsContainer metricsContainer = mock(MetricsContainer.class);
    when(schemaRegistry.getMetricsContainer()).thenReturn(metricsContainer);
    when(metricsContainer.getMetrics()).thenReturn(metrics);
    when(schemaRegistry.properties()).thenReturn(new HashMap<>());
    metricsManager = new MetricsManager(schemaRegistry);
  }

  @AfterEach
  public void tearDown() {
    metrics.close();
  }

  private MetricName tenantMetric(String metricName, String tenant) {
    Map<String, String> tags = new HashMap<>();
    tags.put(MetricsManager.TENANT_TAG, tenant);
    String description;
    switch (metricName) {
      case MetricsManager.NUM_KEKS:
        description = "Number of keks";
        break;
      case MetricsManager.NUM_KEKS_SHARED:
        description = "Number of keks shared";
        break;
      case MetricsManager.NUM_DEKS:
        description = "Number of deks";
        break;
      default:
        throw new IllegalArgumentException(metricName);
    }
    return new MetricName(metricName, MetricsManager.METRIC_GROUP, description, tags);
  }

  private String sensorName(String metricName, String tenant) {
    return metricName + "." + tenant;
  }

  @Test
  public void getTenants_emptyByDefault() {
    assertTrue(metricsManager.getTenants().isEmpty());
  }

  @Test
  public void getTenants_returnsTenantsAfterIncrements() {
    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);
    metricsManager.incrementSharedKeyCount("tenant-b");

    assertEquals(Set.of("tenant-a", "tenant-b"), metricsManager.getTenants());
  }

  @Test
  public void getTenants_isSnapshot() {
    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);
    Set<String> snapshot = metricsManager.getTenants();

    metricsManager.incrementKeyCount("tenant-b", KeyType.KEK);

    assertEquals(Set.of("tenant-a"), snapshot);
    assertThrows(UnsupportedOperationException.class, () -> snapshot.add("tenant-c"));
  }

  @Test
  public void removeTenant_dropsAllSensorsForTenant() {
    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);
    metricsManager.incrementSharedKeyCount("tenant-a");
    metricsManager.incrementKeyCount("tenant-a", KeyType.DEK);

    assertNotNull(metrics.getSensor(sensorName(MetricsManager.NUM_KEKS, "tenant-a")));
    assertNotNull(metrics.getSensor(sensorName(MetricsManager.NUM_KEKS_SHARED, "tenant-a")));
    assertNotNull(metrics.getSensor(sensorName(MetricsManager.NUM_DEKS, "tenant-a")));
    assertNotNull(metrics.metric(tenantMetric(MetricsManager.NUM_KEKS, "tenant-a")));
    assertNotNull(metrics.metric(tenantMetric(MetricsManager.NUM_KEKS_SHARED, "tenant-a")));
    assertNotNull(metrics.metric(tenantMetric(MetricsManager.NUM_DEKS, "tenant-a")));

    metricsManager.removeTenant("tenant-a");

    assertFalse(metricsManager.getTenants().contains("tenant-a"));
    assertNull(metrics.getSensor(sensorName(MetricsManager.NUM_KEKS, "tenant-a")));
    assertNull(metrics.getSensor(sensorName(MetricsManager.NUM_KEKS_SHARED, "tenant-a")));
    assertNull(metrics.getSensor(sensorName(MetricsManager.NUM_DEKS, "tenant-a")));
    assertNull(metrics.metric(tenantMetric(MetricsManager.NUM_KEKS, "tenant-a")));
    assertNull(metrics.metric(tenantMetric(MetricsManager.NUM_KEKS_SHARED, "tenant-a")));
    assertNull(metrics.metric(tenantMetric(MetricsManager.NUM_DEKS, "tenant-a")));
  }

  @Test
  public void removeTenant_doesNotAffectOtherTenants() {
    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);
    metricsManager.incrementKeyCount("tenant-b", KeyType.KEK);
    metricsManager.incrementKeyCount("tenant-b", KeyType.KEK);

    metricsManager.removeTenant("tenant-a");

    assertEquals(Set.of("tenant-b"), metricsManager.getTenants());
    assertNotNull(metrics.getSensor(sensorName(MetricsManager.NUM_KEKS, "tenant-b")));
    assertNotNull(metrics.metric(tenantMetric(MetricsManager.NUM_KEKS, "tenant-b")));
    assertEquals(2L, metricsManager.getKeyCount("tenant-b", KeyType.KEK));
  }

  @Test
  public void removeTenant_unknownTenantIsNoOp() {
    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);

    assertDoesNotThrow(() -> metricsManager.removeTenant("never-registered"));

    assertEquals(Set.of("tenant-a"), metricsManager.getTenants());
  }

  @Test
  public void removeTenant_isIdempotent() {
    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);
    metricsManager.removeTenant("tenant-a");

    assertDoesNotThrow(() -> metricsManager.removeTenant("tenant-a"));
    assertTrue(metricsManager.getTenants().isEmpty());
  }

  @Test
  public void removeTenant_thenIncrementReregisters() {
    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);
    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);
    metricsManager.removeTenant("tenant-a");

    metricsManager.incrementKeyCount("tenant-a", KeyType.KEK);

    assertTrue(metricsManager.getTenants().contains("tenant-a"));
    assertEquals(1L, metricsManager.getKeyCount("tenant-a", KeyType.KEK));
    assertNotNull(metrics.getSensor(sensorName(MetricsManager.NUM_KEKS, "tenant-a")));
    assertNotNull(metrics.metric(tenantMetric(MetricsManager.NUM_KEKS, "tenant-a")));
  }
}
