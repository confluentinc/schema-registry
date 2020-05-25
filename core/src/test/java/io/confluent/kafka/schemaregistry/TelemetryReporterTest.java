package io.confluent.kafka.schemaregistry;

import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import io.confluent.shaded.io.opencensus.proto.metrics.v1.Metric;
import io.confluent.shaded.io.opencensus.proto.resource.v1.Resource;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.provider.SchemaRegistryProvider;
import io.confluent.telemetry.serde.OpencensusMetricsProto;
import io.confluent.telemetry.serde.ProtoToFlatJson;
import junit.framework.TestCase;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
public class TelemetryReporterTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(TelemetryReporterTest.class);

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
    props.setProperty("bootstrap.servers", brokerList);
    props.setProperty(KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");
    props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
    props.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
    props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

    props.setProperty(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "500");

    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + MetricsContainer.RESOURCE_LABEL_CLUSTER_ID, "foobar1");
    props.setProperty(MetricsContainer.RESOURCE_LABEL_CLUSTER_ID, "foobar2");

    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX +
                      MetricsContainer.RESOURCE_LABEL_PREFIX + "region", "test");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX +
                      MetricsContainer.RESOURCE_LABEL_PREFIX + "pkc", "pkc-bar");

    props.setProperty("producer.retries", "3");

    props.setProperty("schema.registry.service.id", "yolo");
    return props;
  }

  @Test
  public void testMetricsReporter() throws Exception {
    long startMs = System.currentTimeMillis();
    boolean srMetricsPresent = false;
    TestUtils.registerAndVerifySchema(restApp.restClient,
            TestUtils.getRandomCanonicalAvroString(1).get(0), 1, "testTopic");
    log.error("************ Broker list: {} *************", brokerList);
    while (!srMetricsPresent && System.currentTimeMillis() - startMs < 90000) {
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
        // TestCase.assertEquals("kafka.schema.registry", resource.getType());

        Map<String, String> resourceLabels = resource.getLabelsMap();

        // Check that the labels from the config are present.
        TestCase.assertEquals("test", resourceLabels.get("schemaregistry.region"));
        TestCase.assertEquals("pkc-bar", resourceLabels.get("schemaregistry.pkc"));

        if (m.getMetricDescriptor().getName().startsWith(SchemaRegistryProvider.DOMAIN)) {
          srMetricsPresent = true;
        }
      }
    }
    assertTrue(srMetricsPresent);
  }

}
