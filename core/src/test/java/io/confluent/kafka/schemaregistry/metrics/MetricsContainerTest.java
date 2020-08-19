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
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
s
public class MetricsContainerTest extends ClusterTestHarness {

    public MetricsContainerTest() { super(1, true); }

    @Override
    protected Properties getSchemaRegistryProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                          CustomMetricReporter.class.getName());
        return props;
    }

    @Test
    public void testCustomSchemaProviderMetricCount() {
        MetricsContainer container = restApp.restApp.schemaRegistry().getMetricsContainer();
        assert container != null;
    }

    public static class CustomMetricReporter implements MetricsReporter {

        @Override
        public void init(List<KafkaMetric> metrics) { }

        @Override
        public void metricChange(KafkaMetric metric) { }

        @Override
        public void metricRemoval(KafkaMetric metric) { }

        @Override
        public void close() { }

        @Override
        public void configure(Map<String, ?> configs) {
            assert configs.containsKey(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG);
        }
    }
}
