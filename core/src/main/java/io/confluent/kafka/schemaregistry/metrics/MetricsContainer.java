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
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsContainer {

  public static final String JMX_PREFIX = "kafka.schema.registry";

  public static final String RESOURCE_LABEL_PREFIX = "resource.";
  public static final String RESOURCE_LABEL_KAFKA_CLUSTER_ID =
          RESOURCE_LABEL_PREFIX + "kafka.cluster.id";
  public static final String RESOURCE_LABEL_CLUSTER_ID = RESOURCE_LABEL_PREFIX + "cluster.id";
  public static final String RESOURCE_LABEL_GROUP_ID = RESOURCE_LABEL_PREFIX + "group.id";
  public static final String RESOURCE_LABEL_TYPE = RESOURCE_LABEL_PREFIX + "type";
  public static final String RESOURCE_LABEL_VERSION = RESOURCE_LABEL_PREFIX + "version";
  public static final String RESOURCE_LABEL_COMMIT_ID = RESOURCE_LABEL_PREFIX + "commit.id";

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

  private final MetricsContext metricsContext;

  public MetricsContainer(SchemaRegistryConfig config, String kafkaClusterId) {
    this.configuredTags =
            Application.parseListToMap(config.getList(RestConfig.METRICS_TAGS_CONFIG));

    List<MetricsReporter> reporters = config.getConfiguredInstances(
        config.getList(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG),
        MetricsReporter.class,
        Collections.singletonMap(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG,
                                 config.getString(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG)));

    reporters.add(getJmxReporter(config));

    metricsContext = getMetricsContext(config, kafkaClusterId);

    MetricConfig metricConfig =
            new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                            TimeUnit.MILLISECONDS);
    this.metrics = new Metrics(metricConfig, reporters, new SystemTime(), metricsContext);

    this.isLeaderNode = createMetric("master-slave-role",
            "1.0 indicates the node is the active leader in the cluster and is the"
            + " node where all register schema and config update requests are "
            + "served.");
    this.nodeCount = createMetric("node-count", "Number of Schema Registry nodes in the cluster");

    this.apiCallsSuccess = createMetric("api-success-count", "Number of successful API calls");
    this.apiCallsFailure = createMetric("api-failure-count", "Number of failed API calls");

    this.customSchemaProviders = createMetric("custom-schema-provider-count",
            "Number of custom schema providers");

    this.schemasCreated = createMetric("registered-count", "Number of registered schemas");
    this.schemasDeleted = createMetric("deleted-count", "Number of deleted schemas");

    this.avroSchemasCreated = createMetric("avro-schemas-created",
            "Number of registered Avro schemas");

    this.avroSchemasDeleted = createMetric("avro-schemas-deleted",
            "Number of deleted Avro schemas");

    this.jsonSchemasCreated = createMetric("json-schemas-created",
            "Number of registered JSON schemas");

    this.jsonSchemasDeleted = createMetric("json-schemas-deleted",
            "Number of deleted JSON schemas");

    this.protobufSchemasCreated = createMetric("protobuf-schemas-created",
            "Number of registered Protobuf schemas");

    this.protobufSchemasDeleted = createMetric("protobuf-schemas-deleted",
            "Number of deleted Protobuf schemas");
  }

  private static MetricsReporter getJmxReporter(SchemaRegistryConfig config) {
    MetricsReporter reporter = new JmxReporter();
    reporter.configure(config.originals());
    return reporter;
  }

  private SchemaRegistryMetric createMetric(String name, String metricDescription) {
    return createMetric(name, name, name, metricDescription);
  }

  private SchemaRegistryMetric createMetric(String sensorName, String metricName,
                                            String metricGroup, String metricDescription) {
    MetricName mn = new MetricName(metricName, metricGroup, metricDescription, configuredTags);
    return new SchemaRegistryMetric(metrics, sensorName, mn);
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

  private static MetricsContext getMetricsContext(SchemaRegistryConfig config,
                                                  String kafkaClusterId) {
    String srGroupId = config.getString(SchemaRegistryConfig.SCHEMAREGISTRY_GROUP_ID_CONFIG);

    Map<String, Object> metadata =
            config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX);

    metadata.put(RESOURCE_LABEL_KAFKA_CLUSTER_ID, kafkaClusterId);
    metadata.put(RESOURCE_LABEL_CLUSTER_ID, srGroupId);
    metadata.put(RESOURCE_LABEL_GROUP_ID, srGroupId);
    metadata.put(RESOURCE_LABEL_TYPE,  "schema_registry");
    metadata.put(RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
    metadata.put(RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());

    return new KafkaMetricsContext(JMX_PREFIX, metadata);
  }
}
