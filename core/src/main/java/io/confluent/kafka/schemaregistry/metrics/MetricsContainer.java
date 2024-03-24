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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.utils.AppInfoParser;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.SystemTime;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsContainer {

  public static final String JMX_PREFIX = "kafka.schema.registry";

  public static final String RESOURCE_LABEL_PREFIX = "resource.";

  public static final String RESOURCE_LABEL_KAFKA_CLUSTER_ID =
          RESOURCE_LABEL_PREFIX + "kafka.cluster.id";

  public static final String METRIC_NAME_MASTER_SLAVE_ROLE = "master-slave-role";
  public static final String METRIC_NAME_NODE_COUNT = "node-count";
  public static final String METRIC_NAME_CUSTOM_SCHEMA_PROVIDER = "custom-schema-provider-count";
  public static final String METRIC_NAME_API_SUCCESS_COUNT = "api-success-count";
  public static final String METRIC_NAME_API_FAILURE_COUNT = "api-failure-count";
  public static final String METRIC_NAME_REGISTERED_COUNT = "registered-count";
  public static final String METRIC_NAME_DELETED_COUNT = "deleted-count";
  public static final String METRIC_NAME_AVRO_SCHEMAS_CREATED = "avro-schemas-created";
  public static final String METRIC_NAME_AVRO_SCHEMAS_DELETED = "avro-schemas-deleted";
  public static final String METRIC_NAME_JSON_SCHEMAS_CREATED = "json-schemas-created";
  public static final String METRIC_NAME_JSON_SCHEMAS_DELETED = "json-schemas-deleted";
  public static final String METRIC_NAME_PB_SCHEMAS_CREATED = "protobuf-schemas-created";
  public static final String METRIC_NAME_PB_SCHEMAS_DELETED = "protobuf-schemas-deleted";
  public static final String METRIC_LEADER_INITIALIZATION_LATENCY = "leader-initialization-latency";
  public static final String METRIC_CERTIFICATE_EXPIRATION = "certificate-expiration";

  private final Metrics metrics;
  private final Map<String, String> configuredTags;

  private final SchemaRegistryMetric isLeaderNode;
  private final SchemaRegistryMetric nodeCount;

  private final SchemaRegistryMetric schemasCreated;
  private final SchemaRegistryMetric schemasDeleted;
  private final SchemaRegistryMetric customSchemaProviders;
  private final SchemaRegistryMetric apiCallsSuccess;
  private final SchemaRegistryMetric apiCallsFailure;

  private final SchemaRegistryMetric avroSchemasCreated;
  private final SchemaRegistryMetric jsonSchemasCreated;
  private final SchemaRegistryMetric protobufSchemasCreated;

  private final SchemaRegistryMetric avroSchemasDeleted;
  private final SchemaRegistryMetric jsonSchemasDeleted;
  private final SchemaRegistryMetric protobufSchemasDeleted;
  private final SchemaRegistryMetric leaderInitializationLatency;

  private final MetricsContext metricsContext;

  public MetricsContainer(SchemaRegistryConfig config, String kafkaClusterId) {
    this.configuredTags = Application.parseListToMap(config.getList(RestConfig.METRICS_TAGS_CONFIG));

    List<MetricsReporter> reporters = config.getConfiguredInstances(
        config.getList(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG),
        MetricsReporter.class,
        Collections.singletonMap(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG,
                                 config.getString(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG)));

    reporters.add(getJmxReporter(config));

    metricsContext = buildMetricsContext(config, kafkaClusterId);

    MetricConfig metricConfig =
            new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                            TimeUnit.MILLISECONDS);
    this.metrics = new Metrics(metricConfig, reporters, new SystemTime(), metricsContext);

    this.isLeaderNode = createMetric(METRIC_NAME_MASTER_SLAVE_ROLE,
            "1.0 indicates the node is the active leader in the cluster and is the"
            + " node where all register schema and config update requests are "
            + "served.", new Value());
    this.nodeCount = createMetric(METRIC_NAME_NODE_COUNT,
            "Number of Schema Registry nodes in the cluster", new Value());

    this.apiCallsSuccess = createMetric(METRIC_NAME_API_SUCCESS_COUNT,
            "Number of successful API calls", new CumulativeCount());
    this.apiCallsFailure = createMetric(METRIC_NAME_API_FAILURE_COUNT,
            "Number of failed API calls", new CumulativeCount());

    this.customSchemaProviders = createMetric(METRIC_NAME_CUSTOM_SCHEMA_PROVIDER,
            "Number of custom schema providers", new Value());

    this.schemasCreated = createMetric(METRIC_NAME_REGISTERED_COUNT, "Number of registered schemas",
            new CumulativeCount());
    this.schemasDeleted = createMetric(METRIC_NAME_DELETED_COUNT, "Number of deleted schemas",
            new CumulativeCount());

    this.avroSchemasCreated = createMetric(METRIC_NAME_AVRO_SCHEMAS_CREATED,
            "Number of registered Avro schemas", new CumulativeCount());

    this.avroSchemasDeleted = createMetric(METRIC_NAME_AVRO_SCHEMAS_DELETED,
            "Number of deleted Avro schemas", new CumulativeCount());

    this.jsonSchemasCreated = createMetric(METRIC_NAME_JSON_SCHEMAS_CREATED,
            "Number of registered JSON schemas", new CumulativeCount());

    this.jsonSchemasDeleted = createMetric(METRIC_NAME_JSON_SCHEMAS_DELETED,
            "Number of deleted JSON schemas", new CumulativeCount());

    this.protobufSchemasCreated = createMetric(METRIC_NAME_PB_SCHEMAS_CREATED,
            "Number of registered Protobuf schemas", new CumulativeCount());

    this.protobufSchemasDeleted = createMetric(METRIC_NAME_PB_SCHEMAS_DELETED,
            "Number of deleted Protobuf schemas", new CumulativeCount());

    this.leaderInitializationLatency = createMetric(METRIC_LEADER_INITIALIZATION_LATENCY,
            "Time spent initializing the leader's kafka store", new Value());
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public Map<String, String> getMetricsTags() {
    return configuredTags;
  }

  public MetricsContext getMetricsContext() {
    return metricsContext;
  }

  private static MetricsReporter getJmxReporter(SchemaRegistryConfig config) {
    MetricsReporter reporter = new JmxReporter();
    reporter.configure(config.originals());
    return reporter;
  }

  private SchemaRegistryMetric createMetric(String name, String metricDescription,
                                            MeasurableStat stat) {
    return createMetric(name, name, name, metricDescription, stat);
  }

  private SchemaRegistryMetric createMetric(String name, String metricDescription,
                                            MeasurableStat stat, Map<String, String> tags) {
    tags.putAll(configuredTags);
    MetricName mn = new MetricName(name, name, metricDescription, configuredTags);
    return new SchemaRegistryMetric(metrics, name, mn, stat);
  }

  private SchemaRegistryMetric createMetric(String sensorName, String metricName,
                                            String metricGroup, String metricDescription,
                                            MeasurableStat stat) {
    MetricName mn = new MetricName(metricName, metricGroup, metricDescription, configuredTags);
    return new SchemaRegistryMetric(metrics, sensorName, mn, stat);
  }

  public SchemaRegistryMetric getNodeCountMetric() {
    return nodeCount;
  }

  public SchemaRegistryMetric getLeaderNode() {
    return isLeaderNode;
  }

  public SchemaRegistryMetric getApiCallsSuccess() {
    return apiCallsSuccess;
  }

  public SchemaRegistryMetric getApiCallsFailure() {
    return apiCallsFailure;
  }

  public SchemaRegistryMetric getCustomSchemaProviderCount() {
    return customSchemaProviders;
  }

  public SchemaRegistryMetric getSchemasCreated() {
    return schemasCreated;
  }

  public SchemaRegistryMetric getSchemasCreated(String type) {
    return getSchemaTypeMetric(type, true);
  }

  public SchemaRegistryMetric getSchemasDeleted() {
    return schemasDeleted;
  }

  public SchemaRegistryMetric getSchemasDeleted(String type) {
    return getSchemaTypeMetric(type, false);
  }

  private SchemaRegistryMetric getSchemaTypeMetric(String type, boolean isRegister) {
    switch (type) {
      case AvroSchema.TYPE:
        return isRegister ? avroSchemasCreated : avroSchemasDeleted;
      case JsonSchema.TYPE:
        return isRegister ? jsonSchemasCreated : jsonSchemasDeleted;
      case ProtobufSchema.TYPE:
        return isRegister ? protobufSchemasCreated : protobufSchemasDeleted;
      default:
        return null;
    }
  }

  public SchemaRegistryMetric getLeaderInitializationLatencyMetric() {
    return leaderInitializationLatency;
  }

  public SchemaRegistryMetric getCertificateExpirationMetric(Map<String, String> tags) {
    return createMetric(METRIC_CERTIFICATE_EXPIRATION,
            "Epoch timestamp when the certificate expires", new Value(), tags);
  }

  public void emitCertificateExpirationMetric(KeyStore keystore, String type) {
    try {
      Enumeration<String> aliases = keystore.aliases();
      while (aliases.hasMoreElements()) {
        String alias = aliases.nextElement();

        if (keystore.isCertificateEntry(alias)) {
          Certificate certificate = keystore.getCertificate(alias);

          if (certificate instanceof X509Certificate) {
            X509Certificate crt = (X509Certificate) certificate;
            Map<String, String> tags = new HashMap<>();
            tags.put(SchemaRegistryConfig.RESOURCE_CERT_LABEL_TYPE, type);
            tags.put(SchemaRegistryConfig.RESOURCE_CERT_LABEL_NAME, alias);
            getCertificateExpirationMetric(tags).record(crt.getNotAfter().getTime());
          }
        }
      }
    } catch (KeyStoreException e) {
      throw new RuntimeException(e);
    }
  }

  private static MetricsContext buildMetricsContext(SchemaRegistryConfig config, String kafkaClusterId) {

    String srGroupId = config.getString(SchemaRegistryConfig.SCHEMAREGISTRY_GROUP_ID_CONFIG);

    Map<String, Object> metadata =
            config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX);

    metadata.put(RESOURCE_LABEL_KAFKA_CLUSTER_ID, kafkaClusterId);
    metadata.put(SchemaRegistryConfig.RESOURCE_LABEL_CLUSTER_ID, srGroupId);
    metadata.put(SchemaRegistryConfig.RESOURCE_LABEL_GROUP_ID, srGroupId);
    metadata.put(SchemaRegistryConfig.RESOURCE_LABEL_TYPE,  "schema_registry");
    metadata.put(SchemaRegistryConfig.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
    metadata.put(SchemaRegistryConfig.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());

    return new KafkaMetricsContext(JMX_PREFIX, metadata);
  }
}
