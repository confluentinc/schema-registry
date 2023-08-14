/*
 * Copyright 2018 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.client;

import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;
import java.util.ArrayList;
import java.util.Arrays;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY;
import static org.junit.Assert.assertArrayEquals;

public class CachedSchemaRegistryClientTest extends ClusterTestHarness {

  private final String SCHEMA_REGISTRY_URL = "schema.registry.url";

  private String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                        "\"name\": \"User\"," +
                        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";

  public CachedSchemaRegistryClientTest() {
    super(1, true);
  }

  private IndexedRecord createAvroRecord() {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  private Properties createConsumerProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", "avroGroup");
    props.put("session.timeout.ms", "6000"); // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
    props.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
    return props;
  }

  private Consumer<String, Object> createConsumer(Properties props) {
    return new KafkaConsumer<>(props);
  }

  private ArrayList<Object> consume(Consumer<String, Object> consumer, String topic, int numMessages) {
    ArrayList<Object> recordList = new ArrayList<Object>();

    consumer.subscribe(Arrays.asList(topic));

    int i = 0;
    while (i < numMessages) {
      ConsumerRecords<String, Object> records = consumer.poll(1000);
      for (ConsumerRecord<String, Object> record : records) {
        recordList.add(record.value());
        i++;
      }
    }

    consumer.close();
    return recordList;
  }

  private Properties createNewProducerProps() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    return props;
  }

  private KafkaProducer createNewProducer(Properties props) {
    return new KafkaProducer(props);
  }

  private void newProduce(KafkaProducer producer, String topic, Object[] objects) {
    ProducerRecord<String, Object> record;
    for (Object object : objects) {
      record = new ProducerRecord<String, Object>(topic, object);
      producer.send(record);
    }
  }

  private Properties createProducerProps() {
    Properties props = new Properties();
    props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
    props.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put("bootstrap.servers", brokerList);
    props.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
    return props;
  }

  private Producer<String, Object> createProducer(Properties props) {
    return new KafkaProducer<>(props);
  }

  private void produce(Producer<String, Object> producer, String topic, Object[] objects) {
    ProducerRecord<String, Object> message;
    for (Object object : objects) {
      message = new ProducerRecord<>(topic, object);
      producer.send(message);
    }
  }

  @Test
  public void testAvroProducer() {
    String topic = "testAvro";
    IndexedRecord avroRecord = createAvroRecord();
    Object[] objects = new Object[]{avroRecord};
    Properties producerProps = createProducerProps();
    Producer<String, Object> producer = createProducer(producerProps);
    produce(producer, topic, objects);

    Properties consumerProps = createConsumerProps();
    Consumer<String, Object> consumer = createConsumer(consumerProps);
    ArrayList<Object> recordList = consume(consumer, topic, objects.length);
    assertArrayEquals(objects, recordList.toArray());
  }

  @Test
  public void testAvroNewProducer() {
    String topic = "testAvro";
    IndexedRecord avroRecord = createAvroRecord();
    Object[] objects = new Object[]
        {avroRecord, true, 130, 345L, 1.23f, 2.34d, "abc", "def".getBytes()};
    Properties producerProps = createNewProducerProps();
    KafkaProducer producer = createNewProducer(producerProps);
    newProduce(producer, topic, objects);

    Properties consumerProps = createConsumerProps();
    Consumer<String, Object> consumer = createConsumer(consumerProps);
    ArrayList<Object> recordList = consume(consumer, topic, objects.length);
    assertArrayEquals(objects, recordList.toArray());
  }

  @Test
  public void testAvroNewProducerUsingContextUrl() {
    String topic = "testAvro";
    IndexedRecord avroRecord = createAvroRecord();
    Object[] objects = new Object[]
        {avroRecord, true, 130, 345L, 1.23f, 2.34d, "abc", "def".getBytes()};
    Properties producerProps = createNewProducerProps();
    producerProps.put(SCHEMA_REGISTRY_URL, restApp.restConnect + "/contexts/.ctx1" );
    KafkaProducer producer = createNewProducer(producerProps);
    newProduce(producer, topic, objects);

    Properties consumerProps = createConsumerProps();
    consumerProps.put(SCHEMA_REGISTRY_URL, restApp.restConnect + "/contexts/.ctx1" );
    Consumer<String, Object> consumer = createConsumer(consumerProps);
    ArrayList<Object> recordList = consume(consumer, topic, objects.length);
    assertArrayEquals(objects, recordList.toArray());
  }

  @Test
  public void testAvroNewProducerWithContext() throws Exception {
    String topic = "testAvro";
    restApp.restClient.registerSchema(userSchema, ":.context02:" + topic + "-value");

    IndexedRecord avroRecord = createAvroRecord();
    Object[] objects = new Object[]{avroRecord};
    Properties producerProps = createNewProducerProps();
    producerProps.put("auto.register.schemas", false);
    producerProps.put("use.latest.version", true);
    KafkaProducer producer = createNewProducer(producerProps);
    newProduce(producer, topic, objects);

    Properties consumerProps = createConsumerProps();
    Consumer<String, Object> consumer = createConsumer(consumerProps);
    ArrayList<Object> recordList = consume(consumer, topic, objects.length);
    assertArrayEquals(objects, recordList.toArray());
  }

  @Test
  public void testAvroNewProducerUsingContextStrategy() {
    String topic = "testAvro";
    IndexedRecord avroRecord = createAvroRecord();
    Object[] objects = new Object[]
        {avroRecord, true, 130, 345L, 1.23f, 2.34d, "abc", "def".getBytes()};
    Properties producerProps = createNewProducerProps();
    producerProps.put(CONTEXT_NAME_STRATEGY, CustomContextNameStrategy.class.getName());
    KafkaProducer producer = createNewProducer(producerProps);
    newProduce(producer, topic, objects);

    Properties consumerProps = createConsumerProps();
    consumerProps.put(CONTEXT_NAME_STRATEGY, CustomContextNameStrategy.class.getName());
    Consumer<String, Object> consumer = createConsumer(consumerProps);
    ArrayList<Object> recordList = consume(consumer, topic, objects.length);
    assertArrayEquals(objects, recordList.toArray());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAvroNewProducerUsingInvalidContextStrategy() {
    String topic = "testAvro";
    IndexedRecord avroRecord = createAvroRecord();
    Object[] objects = new Object[]
        {avroRecord, true, 130, 345L, 1.23f, 2.34d, "abc", "def".getBytes()};
    Properties producerProps = createNewProducerProps();
    producerProps.put(CONTEXT_NAME_STRATEGY, InvalidContextNameStrategy.class.getName());
    KafkaProducer producer = createNewProducer(producerProps);
    newProduce(producer, topic, objects);

    Properties consumerProps = createConsumerProps();
    consumerProps.put(CONTEXT_NAME_STRATEGY, InvalidContextNameStrategy.class.getName());
    Consumer<String, Object> consumer = createConsumer(consumerProps);
    ArrayList<Object> recordList = consume(consumer, topic, objects.length);
    assertArrayEquals(objects, recordList.toArray());
  }

  public static class CustomContextNameStrategy implements ContextNameStrategy {
    public void configure(Map<String, ?> configs) {
    }
    public String contextName(String topic) {
      return ".ctx1";
    }
  }

  public static class InvalidContextNameStrategy implements ContextNameStrategy {
    public void configure(Map<String, ?> configs) {
    }
    public String contextName(String topic) {
      return "foo:bar";
    }
  }
}

