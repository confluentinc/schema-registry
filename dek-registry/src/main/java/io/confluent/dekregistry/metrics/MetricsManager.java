/*
 * Copyright 2023 Confluent Inc.
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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class MetricsManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);

  public static final String KEY = "metricsManager";

  static final String METRIC_GROUP = "dek_registry";

  static final String TENANT_TAG = "tenant";

  static final String NUM_KEKS = "num_keks";

  static final String NUM_KEKS_SHARED = "num_keks_shared";

  static final String NUM_DEKS = "num_deks";

  private final Map<String, TenantMetrics> tenantMetrics = new ConcurrentHashMap<>();

  private final Metrics metrics;

  @Inject
  public MetricsManager(SchemaRegistry schemaRegistry) {
    this.metrics = ((KafkaSchemaRegistry) schemaRegistry).getMetricsContainer().getMetrics();
    // for testing
    schemaRegistry.properties().put(KEY, this);
  }

  public long getKeyCount(String tenant, KeyType keyType) {
    TenantMetrics tenantMetrics = getOrCreateTenantMetrics(tenant);
    return tenantMetrics.getSensor(getMetricDescriptor(keyType), null, null).get();
  }

  public void incrementKeyCount(String tenant, KeyType keyType) {
    TenantMetrics tenantMetrics = getOrCreateTenantMetrics(tenant);
    tenantMetrics.getSensor(getMetricDescriptor(keyType), null, null).add(1);
  }

  public void decrementKeyCount(String tenant, KeyType keyType) {
    TenantMetrics tenantMetrics = getOrCreateTenantMetrics(tenant);
    tenantMetrics.getSensor(getMetricDescriptor(keyType), null, null).add(-1);
  }

  public void incrementSharedKeyCount(String tenant) {
    TenantMetrics tenantMetrics = getOrCreateTenantMetrics(tenant);
    tenantMetrics.getSensor(MetricDescriptor.NUM_KEKS_SHARED_MD, null, null).add(1);
  }

  public void decrementSharedKeyCount(String tenant) {
    TenantMetrics tenantMetrics = getOrCreateTenantMetrics(tenant);
    tenantMetrics.getSensor(MetricDescriptor.NUM_KEKS_SHARED_MD, null, null).add(-1);
  }

  private TenantMetrics getOrCreateTenantMetrics(String tenant) {
    return tenantMetrics.computeIfAbsent(tenant, TenantMetrics::new);
  }

  private MetricDescriptor getMetricDescriptor(KeyType keyType) {
    switch (keyType) {
      case KEK:
        return MetricDescriptor.NUM_KEKS_MD;
      case DEK:
        return MetricDescriptor.NUM_DEKS_MD;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public void close() {
    this.metrics.close();
  }

  private class TenantMetrics {
    private final String tenant;
    private final Map<String, MetricSensor> sensors = new ConcurrentHashMap<>();

    public TenantMetrics(String tenant) {
      this.tenant = tenant;
    }

    private MetricSensor getSensor(MetricDescriptor md, String tagKey, String tagValue) {
      final String sensorName = tagKey == null
          ? md.metricName + "." + tenant
          : md.metricName + "." + tenant + "." + tagKey;
      return sensors.computeIfAbsent(sensorName, k ->
          new MetricSensor(tenant, md, tagKey, tagValue));
    }
  }

  private class MetricSensor {
    private final AtomicLong count = new AtomicLong(0);
    private final Sensor sensor;

    public MetricSensor(String tenant, MetricDescriptor md,
        String tagKey, String tagValue) {
      final String sensorName = tagKey == null
          ? md.metricName + "." + tenant
          : md.metricName + "." + tenant + "." + tagKey + "." + tagValue;
      this.sensor = metrics.sensor(sensorName);
      Map<String, String> tags = new LinkedHashMap<>();
      tags.put(TENANT_TAG, tenant);
      if (tagKey != null) {
        tags.put(tagKey, tagValue);
      }
      MetricName metricNameCount = new MetricName(md.metricName, md.group, md.description, tags);
      sensor.add(metricNameCount, new Value());
    }

    public long get() {
      return count.get();
    }

    public void add(long delta) {
      sensor.record(count.addAndGet(delta));
    }

    public void set(long number) {
      count.set(number);
      sensor.record(number);
    }

    public void reset() {
      count.set(0);
      sensor.record(0);
    }
  }

  private enum MetricDescriptor {
    NUM_KEKS_MD(NUM_KEKS, METRIC_GROUP,
        "Number of keks"),
    NUM_KEKS_SHARED_MD(NUM_KEKS_SHARED, METRIC_GROUP,
        "Number of keks shared"),
    NUM_DEKS_MD(NUM_DEKS, METRIC_GROUP,
        "Number of deks");

    public final String metricName;
    public final String group;
    public final String description;

    MetricDescriptor(String metricName, String group, String description) {
      this.metricName = metricName;
      this.group = group;
      this.description = description;
    }
  }
}