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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistryclient.serializer.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchemaRegistryClientTest extends ClusterTestHarness{

  private final String SCHEMA_REGISTRY_URL = "schema.registry.url";
  private final String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                      "\"name\": \"User\"," +
                      "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
  private final Schema.Parser parser = new Schema.Parser();
  private final String topic = "testAvro";
  private final String groupId = "avroGroup";

  private final String avroRecordString = "{\"name\": \"testUser\"}";
  private final ArrayList<Object> recordList = new ArrayList<Object>();

  public SchemaRegistryClientTest() {
    super(1, true);
  }

  private class AvroConsumer implements Runnable {
    private final ConsumerConnector consumer;
    private final String topic;
    private final String groupId;
    private Properties props = new Properties();

    public AvroConsumer(String topic, String groupId) {
      this.groupId = groupId;
      consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
          createConsumerConfig());
      this.topic = topic;
    }

    private ConsumerConfig createConsumerConfig() {
      props.put("zookeeper.connect", zkConnect);
      props.put("group.id", groupId);
      props.put("zookeeper.session.timeout.ms", "400");
      props.put("zookeeper.sync.time.ms", "200");
      props.put("auto.commit.interval.ms", "1000");
      props.put("auto.offset.reset", "smallest");
      props.put("schema.registry.url", restApp.restConnect);
      return new ConsumerConfig(props);
    }

    @Override
    public void run() {
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      topicCountMap.put(topic, new Integer(1));

      VerifiableProperties vProps = new VerifiableProperties(props);
      KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);
      StringDecoder keyDecoder = new StringDecoder(vProps);

      Map<String, List<KafkaStream<String, Object>>> consumerMap = consumer.createMessageStreams(
          topicCountMap, keyDecoder, valueDecoder);
      KafkaStream<String, Object> stream =  consumerMap.get(topic).get(0);
      ConsumerIterator<String, Object> it = stream.iterator();
      while(it.hasNext()) {
        recordList.add(it.next().message());
      }
      consumer.shutdown();
    }
  }

  private class AvroNewProducer implements Runnable {
    private final KafkaProducer producer;
    private final String topic;

    public AvroNewProducer(String topic, String brokers) {
      this.topic = topic;
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      props.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.schemaregistryclient.serializer.KafkaAvroSerializer.class);
      producer = new KafkaProducer(props);
    }

    private void produceIndexedRecord(int num) {
      Schema schema = parser.parse(userSchema);
      GenericRecord avroRecord = new GenericData.Record(schema);
      avroRecord.put("name", "testUser");
      ProducerRecord<String, IndexedRecord> record = new ProducerRecord<String, IndexedRecord>(topic, avroRecord);
      for(int i = 0; i < num; ++i) {
        producer.send(record);
      }
    }

    private void producePrimitives(int num) {
      ProducerRecord<String, Object> nullRecord = new ProducerRecord<String, Object>(topic, null);
      ProducerRecord<String, Boolean> booleanRecord = new ProducerRecord<String, Boolean>(topic, true);
      ProducerRecord<String, Integer> intRecord = new ProducerRecord<String, Integer>(topic, 130);
      ProducerRecord<String, Long> longRecord = new ProducerRecord<String, Long>(topic, 345l);
      ProducerRecord<String, Float> floatRecord = new ProducerRecord<String, Float>(topic, 1.23f);
      ProducerRecord<String, Double> doubleRecord = new ProducerRecord<String, Double>(topic, 3.45);
      ProducerRecord<String, String> stringRecord = new ProducerRecord<String, String>(topic, "abc");
      ProducerRecord<String, byte[]> bytesRecord = new ProducerRecord<String, byte[]>(topic, "abc".getBytes());
      for(int i = 0; i < num; ++i) {
        // producer.send(nullRecord);
        producer.send(booleanRecord);
        producer.send(intRecord);
        producer.send(longRecord);
        producer.send(floatRecord);
        producer.send(doubleRecord);
        producer.send(stringRecord);
        producer.send(bytesRecord);
      }
    }

    @Override
    public void run() {
      produceIndexedRecord(1);
      producePrimitives(1);
      producer.close();
    }
  }

  private class AvroProducer implements Runnable {
    private final kafka.javaapi.producer.Producer<String, Object> producer;
    private final String topic;
    private final Properties props = new Properties();

    public AvroProducer(String topic) {
      props.put("serializer.class", "io.confluent.kafka.schemaregistryclient.serializer.KafkaAvroEncoder");
      props.put("key.serializer.class", "kafka.serializer.StringEncoder");
      props.put("metadata.broker.list", brokerList);
      props.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
      producer = new kafka.javaapi.producer.Producer<String, Object>(new kafka.producer.ProducerConfig(props));
      this.topic = topic;
    }

    private void produceIndexedRecord(int num) {
      Schema schema = parser.parse(userSchema);
      GenericRecord avroRecord = new GenericData.Record(schema);
      avroRecord.put("name", "testUser");
      KeyedMessage<String, Object> record = new KeyedMessage<String, Object>(topic, avroRecord);
      for(int i = 0; i < num; ++i) {
        producer.send(record);
      }
    }

    @Override
    public void run() {
      produceIndexedRecord(1);
      producer.close();
    }
  }


  @Test
  public void testAvroOldProducer() {
    recordList.clear();

    AvroProducer producer = new AvroProducer(topic);
    Thread producerThread = new Thread(producer);
    producerThread.start();

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {

    }
    AvroConsumer avroConsumer = new AvroConsumer(topic, groupId);
    Thread consumerThread = new Thread(avroConsumer);
    consumerThread.start();

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {

    }
    assertTrue("record list should have elements", recordList.size() != 0);
    for (Object object: recordList) {
      if (object != null) {
        if (object instanceof Boolean) {
          boolean value = (Boolean) object;
          assertEquals("Should match", value, true);
        } else if (object instanceof Integer) {
          int value = (Integer) object;
          assertEquals("Should match", value, 130);
        } else if (object instanceof Long) {
          long value = (Long) object;
          assertEquals("Should match", value, 345l);
        } else if (object instanceof Float) {
          float value = (Float) object;
          assertEquals(value, 1.23f, 0.0);
        } else if (object instanceof Double) {
          double value = (Double) object;
          assertEquals(value, 3.45, 0.0);
        } else if (object instanceof String) {
          String value = (String) object;
          assertEquals("Should match", value, "abc");
        } else if (object instanceof byte[]) {
          byte[] value = (byte[]) object;
          assertEquals("Should match", value, "abc".getBytes());
        } else if (object instanceof IndexedRecord) {
          IndexedRecord record = (IndexedRecord) object;
          assertEquals("Should match", record.toString(), avroRecordString);
        }
      }
    }
  }

  @Test
  public void testAvroNewProducer() {
    recordList.clear();

    AvroNewProducer newProducer = new AvroNewProducer(topic, brokerList);
    Thread producerThread = new Thread(newProducer);
    producerThread.start();

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {

    }
    AvroConsumer avroConsumer = new AvroConsumer(topic, groupId);
    Thread consumerThread = new Thread(avroConsumer);
    consumerThread.start();

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {

    }

    assertTrue("record list should have elements", recordList.size() != 0);

    for (Object object: recordList) {
      if (object != null) {
        if (object instanceof Boolean) {
          boolean value = (Boolean) object;
          assertEquals("Should match", value, true);
        } else if (object instanceof Integer) {
          int value = (Integer) object;
          assertEquals("Should match", value, 130);
        } else if (object instanceof Long) {
          long value = (Long) object;
          assertEquals("Should match", value, 345l);
        } else if (object instanceof Float) {
          float value = (Float) object;
          assertEquals(value, 1.23f, 0.0);
        } else if (object instanceof Double) {
          double value = (Double) object;
          assertEquals(value, 3.45, 0.0);
        } else if (object instanceof String) {
          String value = (String) object;
          assertEquals("Should match", value, "abc");
        } else if (object instanceof byte[]) {
          byte[] value = (byte[]) object;
          assertEquals("Should match", value, "abc".getBytes());
        } else if (object instanceof IndexedRecord) {
          IndexedRecord record = (IndexedRecord) object;
          assertEquals("Should match", record.toString(), avroRecordString);
        }
      }
    }
  }

}
