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
 *
 */

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.Customer;
import io.confluent.kafka.schemaregistry.rules.ResourceLoader;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.RuleSetHandler;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.WrapperKeyDeserializer;
import io.confluent.kafka.serializers.WrapperKeySerializer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CelExecutorIntegrationTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(CelExecutorIntegrationTest.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

  private static final String TOPIC = "customer";

  private static final String DLQ_TOPIC1 = "DLQ1";
  private static final String DLQ_TOPIC2 = "DLQ2";
  private static final String DLQ_TOPIC3 = "DLQ3";

  public CelExecutorIntegrationTest() {
    super(1, true);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    ((KafkaSchemaRegistry) restApp.schemaRegistry()).setRuleSetHandler(new RuleSetHandler() {
      public void handle(String subject, ConfigUpdateRequest request) {
      }

      public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
      }

      public io.confluent.kafka.schemaregistry.storage.RuleSet transform(RuleSet ruleSet) {
        return ruleSet != null
            ? new io.confluent.kafka.schemaregistry.storage.RuleSet(ruleSet)
            : null;
      }
    });
  }

  private static void registerSchema(String schemaRegistryUrl) throws Exception {
    ResourceLoader resourceLoader = new ResourceLoader("/");
    String schemaString = resourceLoader.toString("test_cel.json");
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema schema = mapper.readValue(
        schemaString, io.confluent.kafka.schemaregistry.client.rest.entities.Schema.class);

    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
        new RestService(schemaRegistryUrl),
        10,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    Optional<ParsedSchema> parsedSchema = schemaRegistry.parseSchema(schema);
    schemaRegistry.register(TOPIC + "-value", parsedSchema.get());
  }

  private static Properties createConsumerProps(String brokerList, String schemaRegistryUrl) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", "avroGroup");
    props.put("session.timeout.ms", "6000"); // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, WrapperKeyDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    props.put("wrapped.key.deserializer", StringDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
    return props;
  }

  private static Properties createDlqConsumerProps(
      String brokerList, String schemaRegistryUrl, Class<?> valueDeserializer) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", "avroGroup");
    props.put("session.timeout.ms", "6000"); // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, WrapperKeyDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    props.put("wrapped.key.deserializer", StringDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
    return props;
  }

  private static Consumer<String, Object> createConsumer(Properties props) {
    return new KafkaConsumer<>(props);
  }

  private static List<Map.Entry<String, Object>> consume(Consumer<String, Object> consumer, String topic,
      int numMessages) {
    List<Map.Entry<String, Object>> recordList = new ArrayList<>();

    consumer.subscribe(Arrays.asList(topic));

    int i = 0;
    do {
      ConsumerRecords<String, Object> records = consumer.poll(1000);
      for (ConsumerRecord<String, Object> record : records) {
        recordList.add(new SimpleEntry<>(record.key(), record.value()));
        i++;
      }
    } while (i < numMessages);

    return recordList;
  }

  private static Properties createProducerProps(String brokerList, String schemaRegistryUrl) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, WrapperKeySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put("wrapped.key.serializer", StringSerializer.class);
    props.put("auto.register.schemas", "false");
    props.put("use.latest.version", "true");
    props.put("latest.compatibility.strict", "false");
    return props;
  }

  private static Producer createProducer(Properties props) {
    return new KafkaProducer(props);
  }

  private static void produce(Producer producer, String topic, String key, Object object) throws Exception {
    ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, object);
    producer.send(record).get();
  }

  private static Object createPayload(boolean isActive, String mode) {
    Customer value = new Customer();
    value.setSsn("123456789"); // rule: size = 9 digits
    value.setAddress("#10 abc, CA 94402"); // rule: matches regex
    value.setMail("david@confluent.io"); // rule: contains @
    value.setUserId("uid_23434"); // rule: starts with uid_
    value.setAge(45); // rule: > 18
    value.setIBAN("GB33BUKB20201555555555"); // rule: matches regex
    value.setActive(isActive); // rule: is true
    value.setBalance(new Float(10.0).floatValue()); // rule: >= 0.0
    value.setMode(mode);
    return value;
  }

  @Test
  public void testAvroProducerFailWrite() throws Exception {
    registerSchema(restApp.restConnect);
    Object payload = createPayload(true, "fail_write");
    Properties producerProps = createProducerProps(brokerList, restApp.restConnect);
    try (Producer producer = createProducer(producerProps)) {
      produce(producer, TOPIC, "key", payload);
    } catch (Exception e) {
      // ignore
    }

    Properties consumerProps = createDlqConsumerProps(
        brokerList, restApp.restConnect, StringDeserializer.class);
    try (Consumer<String, Object> consumer = createConsumer(consumerProps)) {
      List<Map.Entry<String, Object>> recordList = consume(consumer, DLQ_TOPIC2, 1);
      Map.Entry<String, Object> entry = recordList.get(0);
      String record = (String) entry.getValue();
      Customer avroRecord = (Customer) AvroSchemaUtils.toObject(
          record, new AvroSchema(Customer.SCHEMA$), new SpecificDatumReader<>(Customer.SCHEMA$));
      assertEquals("key", entry.getKey());
      assertEquals(payload, avroRecord);
    }
  }

  @Test
  public void testAvroProducerFailRead() throws Exception {
    registerSchema(restApp.restConnect);
    Object payload = createPayload(true, "fail_read");
    Properties producerProps = createProducerProps(brokerList, restApp.restConnect);
    try (Producer producer = createProducer(producerProps)) {
      produce(producer, TOPIC, "key", payload);
    } catch (Exception e) {
      // ignore
    }

    Properties consumerProps = createConsumerProps(brokerList, restApp.restConnect);
    try (Consumer<String, Object> consumer = createConsumer(consumerProps)) {
      try {
        consume(consumer, TOPIC, 0);
      } catch (Exception e) {
        // ignore
      }
    }

    Properties dlqConsumerProps = createDlqConsumerProps(
        brokerList, restApp.restConnect, KafkaAvroDeserializer.class);
    try (Consumer<String, Object> consumer = createConsumer(dlqConsumerProps)) {
      List<Map.Entry<String, Object>> recordList = consume(consumer, DLQ_TOPIC3, 1);
      Map.Entry<String, Object> entry = recordList.get(0);
      Object record = entry.getValue();
      assertEquals("key", entry.getKey());
      assertEquals(payload, record);
    }
  }

  @Test
  public void testAvroProducerSuccess() throws Exception {
    registerSchema(restApp.restConnect);
    Object payload = createPayload(true, "success");
    Properties producerProps = createProducerProps(brokerList, restApp.restConnect);
    try (Producer producer = createProducer(producerProps)) {
      produce(producer, TOPIC, "key", payload);
    } catch (Exception e) {
      // ignore
    }

    Properties consumerProps = createConsumerProps(brokerList, restApp.restConnect);
    try (Consumer<String, Object> consumer = createConsumer(consumerProps)) {
      List<Map.Entry<String, Object>> recordList = consume(consumer, TOPIC, 1);
      Map.Entry<String, Object> entry = recordList.get(0);
      Object record = entry.getValue();
      assertEquals("key", entry.getKey());
      assertEquals(payload, record);
    }
  }

  @Test
  public void testAvroProducerDoubleDlq() throws Exception {
    registerSchema(restApp.restConnect);
    Object payload1 = createPayload(false, "success");
    Object payload2 = createPayload(true, "fail_write");
    Properties producerProps = createProducerProps(brokerList, restApp.restConnect);
    try (Producer producer = createProducer(producerProps)) {
      try {
        produce(producer, TOPIC, "key1", payload1);
      } catch (Exception e) {
        // ignore
      }
      try {
        produce(producer, TOPIC, "key2", payload2);
      } catch (Exception e) {
        // ignore
      }
    }

    Properties consumerProps = createDlqConsumerProps(
        brokerList, restApp.restConnect, StringDeserializer.class);
    try (Consumer<String, Object> consumer = createConsumer(consumerProps)) {
      List<Map.Entry<String, Object>> recordList = consume(consumer, DLQ_TOPIC1, 1);
      Map.Entry<String, Object> entry = recordList.get(0);
      String record = (String) entry.getValue();
      Customer avroRecord = (Customer) AvroSchemaUtils.toObject(
          record, new AvroSchema(Customer.SCHEMA$), new SpecificDatumReader<>(Customer.SCHEMA$));
      assertEquals("key1", entry.getKey());
      assertEquals(payload1, avroRecord);
    }
    try (Consumer<String, Object> consumer = createConsumer(consumerProps)) {
      List<Map.Entry<String, Object>> recordList = consume(consumer, DLQ_TOPIC2, 1);
      Map.Entry<String, Object> entry = recordList.get(0);
      String record = (String) entry.getValue();
      Customer avroRecord = (Customer) AvroSchemaUtils.toObject(
          record, new AvroSchema(Customer.SCHEMA$), new SpecificDatumReader<>(Customer.SCHEMA$));
      assertEquals("key2", entry.getKey());
      assertEquals(payload2, avroRecord);
    }
  }

  // For manual testing
  /*
  public static void main(final String[] args) throws Exception {
    registerSchema("http://localhost:8081");
    Properties props = createProducerProps("localhost:9092", "http://localhost:8081");
    try (Producer producer = createProducer(props)) {
      produce(producer, TOPIC, "key", createPayload(true, "fail_write"));
    }
  }
  */
}

