/*
 * Copyright 2024 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import org.apache.kafka.common.metrics.MetricsContext;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricsContainerTest {

  private SchemaRegistryConfig createMinimalConfig() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "_schemas");
    return new SchemaRegistryConfig(props);
  }

  @Test
  public void testBuildMetricsContextWithNullKafkaClusterId() throws Exception {
    SchemaRegistryConfig config = createMinimalConfig();

    MetricsContext context = assertDoesNotThrow(
        () -> MetricsContainer.buildMetricsContext(config, null)
    );

    assertNotNull(context);
    // Verify the context labels do not contain null values
    assertFalse(
        context.contextLabels().containsKey(MetricsContainer.RESOURCE_LABEL_KAFKA_CLUSTER_ID),
        "Metadata should not contain kafka.cluster.id key when kafkaClusterId is null"
    );
  }

  @Test
  public void testBuildMetricsContextWithKafkaClusterId() throws Exception {
    SchemaRegistryConfig config = createMinimalConfig();

    MetricsContext context = MetricsContainer.buildMetricsContext(config, "test-cluster-id");

    assertNotNull(context);
    assertTrue(
        context.contextLabels().containsKey(MetricsContainer.RESOURCE_LABEL_KAFKA_CLUSTER_ID),
        "Metadata should contain kafka.cluster.id key when kafkaClusterId is provided"
    );
  }
}
