/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.schemaregistry.client;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import static org.junit.Assert.assertArrayEquals;

public class CachedSchemaRegistryClientTest extends ClusterTestHarness {

  private final String SCHEMA_REGISTRY_URL = "schema.registry.url";

  public CachedSchemaRegistryClientTest() {
    super(1, true);
  }

  private IndexedRecord createAvroRecord() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                        "\"name\": \"User\"," +
                        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  private Properties createConsumerProps() {
    Properties props = new Properties();
    props.put("zookeeper.connect", zkConnect);
    props.put("group.id", "avroGroup");
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "smallest");
    props.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
    return props;
  }

  private ConsumerConnector createConsumer(Properties props) {
    return kafka.consumer.Consumer.createJavaConsumerConnector(
        new ConsumerConfig(props));
  }

  private ArrayList<Object> consume(ConsumerConnector consumer, String topic, int numMessages) {
    ArrayList<Object> recordList = new ArrayList<Object>();
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));

    Properties props = createConsumerProps();
    VerifiableProperties vProps = new VerifiableProperties(props);
    KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);
    StringDecoder keyDecoder = new StringDecoder(vProps);

    Map<String, List<KafkaStream<String, Object>>> consumerMap = consumer.createMessageStreams(
        topicCountMap, keyDecoder, valueDecoder);
    KafkaStream<String, Object> stream = consumerMap.get(topic).get(0);
    ConsumerIterator<String, Object> it = stream.iterator();
    int i = 0;
    while (i < numMessages) {
      i++;
      recordList.add(it.next().message());
    }
    consumer.shutdown();
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
    props.put(KafkaAvroSerializerConfig.ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG, true);
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
    props.put(KafkaAvroSerializerConfig.ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG, true);
    props.put("serializer.class",
              "io.confluent.kafka.serializers.KafkaAvroEncoder");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", brokerList);
    props.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
    return props;
  }

  private Producer<String, Object> createProducer(Properties props) {
    return new Producer<String, Object>(
        new kafka.producer.ProducerConfig(props));
  }

  private void produce(Producer<String, Object> producer, String topic, Object[] objects) {
    KeyedMessage<String, Object> message;
    for (Object object : objects) {
      message = new KeyedMessage<String, Object>(topic, object);
      producer.send(message);
    }
  }

  @Ignore
  @Test
  public void testAvroProducer() {
    String topic = "testAvro";
    IndexedRecord avroRecord = createAvroRecord();
    Object[] objects = new Object[]{avroRecord};
    Properties producerProps = createProducerProps();
    Producer<String, Object> producer = createProducer(producerProps);
    produce(producer, topic, objects);

    Properties consumerProps = createConsumerProps();
    ConsumerConnector consumer = createConsumer(consumerProps);
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
    ConsumerConnector consumer = createConsumer(consumerProps);
    ArrayList<Object> recordList = consume(consumer, topic, objects.length);
    assertArrayEquals(objects, recordList.toArray());
  }
}

