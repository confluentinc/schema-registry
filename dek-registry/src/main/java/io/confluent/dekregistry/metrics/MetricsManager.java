/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.metrics;

import com.google.inject.Inject;
import com.google.inject.Singleton;
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

  static final String METRIC_GROUP = "exporter";

  static final String TENANT_TAG = "tenant";

  static final String NUM_EXPORTERS = "num_exporters";

  private final Map<String, TenantMetrics> tenantMetrics = new ConcurrentHashMap<>();

  private final Metrics metrics;

  @Inject
  public MetricsManager(SchemaRegistry schemaRegistry) {
    this.metrics = ((KafkaSchemaRegistry) schemaRegistry).getMetricsContainer().getMetrics();
    // for testing
    schemaRegistry.properties().put(KEY, this);
  }

  public long getExporterCount(String tenant) {
    TenantMetrics tenantMetrics = getOrCreateTenantMetrics(tenant);
    return tenantMetrics.getSensor(MetricDescriptor.NUM_EXPORTERS_MD, null, null).get();
  }

  public void incrementExporterCount(String tenant) {
    TenantMetrics tenantMetrics = getOrCreateTenantMetrics(tenant);
    tenantMetrics.getSensor(MetricDescriptor.NUM_EXPORTERS_MD, null, null).add(1);
  }

  public void decrementExporterCount(String tenant) {
    TenantMetrics tenantMetrics = getOrCreateTenantMetrics(tenant);
    tenantMetrics.getSensor(MetricDescriptor.NUM_EXPORTERS_MD, null, null).add(-1);
  }

  private TenantMetrics getOrCreateTenantMetrics(String tenant) {
    return tenantMetrics.computeIfAbsent(tenant, TenantMetrics::new);
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
    NUM_EXPORTERS_MD(NUM_EXPORTERS, METRIC_GROUP,
        "Number of exporters");

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