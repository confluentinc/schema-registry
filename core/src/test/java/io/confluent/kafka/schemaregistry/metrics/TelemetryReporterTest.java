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
import io.confluent.kafka.schemaregistry.utils.AppInfoParser;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import io.confluent.shaded.io.opencensus.proto.metrics.v1.Metric;
import io.confluent.shaded.io.opencensus.proto.resource.v1.Resource;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.provider.SchemaRegistryProvider;
import io.confluent.telemetry.serde.OpencensusMetricsProto;
import junit.framework.TestCase;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class TelemetryReporterTest extends ClusterTestHarness {

  public TelemetryReporterTest() {
    super(1, true);
  }

  private static final Logger log = LoggerFactory.getLogger(TelemetryReporterTest.class);

  protected KafkaConsumer<byte[], byte[]> consumer;
  protected Serde<Metric> serde = new OpencensusMetricsProto();

  @Before
  public void setUp() throws Exception {
    super.setUp();
    consumer = createNewConsumer(brokerList);
    consumer.subscribe(Collections.singleton(KafkaExporterConfig.DEFAULT_TOPIC_NAME));
  }

  @After
  public void tearDown() throws Exception {
    consumer.close();
    super.tearDown();
  }

  public static KafkaConsumer<byte[], byte[]> createNewConsumer(String brokerList) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "telemetry-metric-reporter-consumer");
    // The metric topic may not be there initially. So, we need to refresh metadata more frequently
    // to pick it up once created.
    properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "400");
    return new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
        new ByteArrayDeserializer());
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();

    props.putAll(getExporterConfig());

    props.setProperty("bootstrap.servers", brokerList);
    props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
    props.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
    props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

    props.setProperty(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "500");

    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX +
                      MetricsContainer.RESOURCE_LABEL_PREFIX + "region", "test");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX +
                      MetricsContainer.RESOURCE_LABEL_PREFIX + "pkc", "pkc-bar");

    return props;
  }

  private Map<String, String> getExporterConfig() {
    Map<String, String> config = new HashMap<>();

    String exporterName = "_test";
    String prefix = ConfluentTelemetryConfig.PREFIX_EXPORTER + exporterName + '.';

    config.put(prefix + ExporterConfig.ENABLED_CONFIG, "true");
    config.put(prefix + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());
    config.put(prefix + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    config.put(prefix + KafkaExporterConfig.PREFIX_PRODUCER + CommonClientConfigs.CLIENT_ID_CONFIG,
               "confluent-telemetry-reporter-local-producer-test");
    config.put(prefix + KafkaExporterConfig.PREFIX_PRODUCER
            + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");

    // Need to disable the default local exporter.
    config.put(ConfluentTelemetryConfig.PREFIX_EXPORTER
            + ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME + "." + ExporterConfig.ENABLED_CONFIG,
            "false");
    return config;
  }

  @Test(timeout = 20000)
  public void testMetricsReporter() throws Exception {
    TestUtils.registerAndVerifySchema(restApp.restClient,
            TestUtils.getRandomCanonicalAvroString(1).get(0), 1, "testTopic");
    boolean srMetricsPresent = false;
    while (!srMetricsPresent) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], byte[]> record : records) {
        Metric metric = serde.deserializer().deserialize(record.topic(), record.headers(),
                                                         record.value());

        // Check the resource labels are present
        Resource resource = metric.getResource();
        assertTrue("schemaregistry".equals(resource.getType()));

        Map<String, String> resourceLabels = resource.getLabelsMap();

        // Check that the labels from the config are present.
        TestCase.assertEquals("test", resourceLabels.get("schemaregistry.region"));
        TestCase.assertEquals("pkc-bar", resourceLabels.get("schemaregistry.pkc"));
        TestCase.assertEquals("_schemas", resourceLabels.get("schemaregistry.topic"));
        TestCase.assertEquals("schemaregistry", resourceLabels.get("schemaregistry.type"));
        TestCase.assertEquals(AppInfoParser.getCommitId(),
                              resourceLabels.get("schemaregistry.commit.id"));
        TestCase.assertEquals(AppInfoParser.getVersion(),
                              resourceLabels.get("schemaregistry.version"));

        if (metric.getMetricDescriptor().getName().startsWith(SchemaRegistryProvider.DOMAIN)) {
          srMetricsPresent = true;
        }
      }
    }
  }

}
