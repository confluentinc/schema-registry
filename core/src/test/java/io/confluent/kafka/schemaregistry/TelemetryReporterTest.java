package io.confluent.kafka.schemaregistry;

import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.shaded.io.opencensus.proto.metrics.v1.Metric;
import io.confluent.shaded.io.opencensus.proto.resource.v1.Resource;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.provider.SchemaRegistryProvider;
import io.confluent.telemetry.serde.OpencensusMetricsProto;
import io.confluent.telemetry.serde.ProtoToFlatJson;
import junit.framework.TestCase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
public class TelemetryReporterTest extends ClusterTestHarness {

  public TelemetryReporterTest() {
    super(1, true);
  }

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
    // The metric topic may not be there initially. So, we need to refresh metadata more frequently to pick it up once created.
    properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "400");
    return new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
        new ByteArrayDeserializer());
  }

  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
        "io.confluent.telemetry.reporter.TelemetryReporter");
    props.setProperty(KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");
    props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
    props.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
    props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

    props.setProperty(KafkaMetricsContext.METRICS_CONTEXT_PREFIX +
                      KafkaSchemaRegistry.RESOURCE_LABEL_CLUSTER_ID, "foobar");

    props.setProperty(KafkaMetricsContext.METRICS_CONTEXT_PREFIX +
                      KafkaSchemaRegistry.RESOURCE_LABEL_PREFIX + "region", "test");
    props.setProperty(KafkaMetricsContext.METRICS_CONTEXT_PREFIX +
                      KafkaSchemaRegistry.RESOURCE_LABEL_PREFIX + "pkc", "pkc-bar");
    return props;
  }

  @Test
  public void testMetricsReporter() {
    long startMs = System.currentTimeMillis();
    boolean srMetricsPresent = false;
    while (System.currentTimeMillis() - startMs < 20000) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], byte[]> record : records) {

        // Verify that the message de-serializes successfully
        Metric m = null;
        try {
          m = this.serde.deserializer().deserialize(record.topic(), record.headers(), record.value());
        } catch (SerializationException e) {
          fail("failed to deserialize message " + e.getMessage());
        }

        System.out.println(new ProtoToFlatJson().deserialize("topic", m.toByteArray()));
        // Verify labels

        // Check the resource labels are present
        Resource resource = m.getResource();
        TestCase.assertEquals("schemaregistry", resource.getType());

        Map<String, String> resourceLabels = resource.getLabelsMap();

        // Check that the labels from the config are present.
        TestCase.assertEquals(
            resourceLabels.get("schemaregistry.region"), "test");
        TestCase.assertEquals(
            resourceLabels.get("schemaregistry.pkc"), "pkc-bar");

        if (m.getMetricDescriptor().getName().startsWith(SchemaRegistryProvider.DOMAIN)) {
          srMetricsPresent = true;
        }
      }
    }
    assertTrue(srMetricsPresent);
  }

}
