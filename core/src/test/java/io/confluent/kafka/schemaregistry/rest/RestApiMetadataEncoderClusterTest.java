/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class RestApiMetadataEncoderClusterTest extends RestApiMetadataEncoderTest {

  private static final Logger log = LoggerFactory.getLogger(RestApiMetadataEncoderClusterTest.class);

  protected ClusterTestHarness harness;

  private static final String encodersTopic = SchemaRegistryConfig.METADATA_ENCODER_TOPIC_DEFAULT;

  public RestApiMetadataEncoderClusterTest() {
    this.harness = new ClusterTestHarness(1, true, CompatibilityLevel.BACKWARD.name);
    this.harness.injectSchemaRegistryProperties(getSchemaRegistryProperties());
  }

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    harness.setUpTest(testInfo);
    setRestApp(harness.getRestApp());
  }

  @AfterEach
  public void tearDown() throws Exception {
    harness.tearDown();
  }

  public Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, INITIAL_SECRET);
    return props;
  }

  @Override
  protected void removeEncoder(String tenant) throws Exception {
    RecordMetadata producerRecord = removeEncoderFromKafka(
        harness.getBrokerList(), encodersTopic, tenant);
    log.info("Produced record to KafkaStore topic: {}", producerRecord);
  }

  @Override
  protected RestApp createRotatedRestApp(String newSecret, String oldSecret) throws Exception {
    Properties rotatedProps = new Properties();
    rotatedProps.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, newSecret);
    rotatedProps.setProperty(SchemaRegistryConfig.METADATA_ENCODER_OLD_SECRET_CONFIG, oldSecret);
    rotatedProps.put(SchemaRegistryConfig.LISTENERS_CONFIG,
        harness.getSchemaRegistryProtocol() + "://0.0.0.0:" + harness.getSchemaRegistryPort());
    rotatedProps.put(SchemaRegistryConfig.MODE_MUTABILITY, true);

    RestApp rotatedRestApp = new RestApp(
        harness.choosePort(),
        null,
        harness.getBrokerList(),
        ClusterTestHarness.KAFKASTORE_TOPIC,
        CompatibilityLevel.BACKWARD.name,
        true,
        rotatedProps);
    rotatedRestApp.start();
    return rotatedRestApp;
  }

  // Create a kafka producer and produce message with key:<tenant>
  // and value:null to the encoders topic
  public static RecordMetadata removeEncoderFromKafka(
      String brokerList, String encodersTopic, String tenant) throws Exception {
    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", brokerList);
    producerProps.put("key.serializer", StringSerializer.class.getName());
    producerProps.put("value.serializer", ByteArraySerializer.class.getName());
    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(encodersTopic, tenant, null);
    RecordMetadata r = producer.send(producerRecord).get();
    producer.close();
    return r;
  }
}

