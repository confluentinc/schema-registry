/*
 * Copyright 2018 Confluent Inc.
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
package io.confluent.kafka.formatter;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import java.util.Optional;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaAvroFormatterTest {

  private static final String RECORD_SCHEMA_STRING = "{\"namespace\": \"example.avro\"," +
          "\"type\": \"record\"," +
          "\"name\": \"User\"," +
          "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
  private static final String RECORD_KEY_SCHEMA_STRING = "{\"namespace\": \"example.avro\"," +
      "\"type\": \"record\"," +
      "\"name\": \"keyRecord\"," +
      "\"fields\": [{\"name\": \"key_field\", \"type\": \"string\"}]}";
  private static final String RECORD_VALUE_SCHEMA_STRING = "{\"namespace\": \"example.avro\"," +
      "\"type\": \"record\"," +
      "\"name\": \"valueRecord\"," +
      "\"fields\": [{\"name\": \"value_field\", \"type\": \"string\"}]}";
  private Properties props;
  private AvroMessageFormatter formatter;
  private Schema recordSchema = null;
  private Schema intSchema = null;
  private String url = "mock://test";
  private SchemaRegistryClient schemaRegistry = null;

  @Before
  public void setUp() {
    props = new Properties();
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, url);

    Schema.Parser parser = new Schema.Parser();
    recordSchema = parser.parse(RECORD_SCHEMA_STRING);
    intSchema = parser.parse("{\"type\" : \"int\"}");
    schemaRegistry = MockSchemaRegistry.getClientForScope("test");
    formatter = new AvroMessageFormatter(url, null);
  }

  @After
  public void tearDown() {
    MockSchemaRegistry.dropScope("test");
  }

  @Test
  public void testKafkaAvroValueFormatter() {
    formatter.init(props);

    String inputJson = "{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(url, null, recordSchema, "topic1", false, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();

    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, 0, serializedValue.length,
        null, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();

    assertEquals("Input value json should match output value json", inputJson, outputJson);
  }

  @Test
  public void testKafkaAvroKeyValueFormatter() {
    props.put("print.key", "true");
    formatter.init(props);

    String inputJson = "10\t{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(url, intSchema, recordSchema, "topic1", true, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();

    byte[] serializedKey = message.key();
    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, serializedKey.length,
        serializedValue.length, serializedKey, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();

    assertEquals("Input key/value json should match output key/value json", inputJson, outputJson);
  }

  @Test
  public void testKafkaAvroValueWithTimestampFormatter() {
    props.put("print.timestamp", "true");
    formatter.init(props);

    long timestamp = 1000;
    TimestampType timestampType = TimestampType.LOG_APPEND_TIME;

    String inputJson = "{\"name\":\"myname\"}\n";
    String expectedJson = String.format("%s:%d\t%s",
            timestampType.name, timestamp, inputJson);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(url, null, recordSchema, "topic1", false, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();

    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        "topic1", 0, 200, timestamp, timestampType, 0, 0, serializedValue.length,
        null, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();

    assertEquals("Input value json should match output value json", expectedJson, outputJson);
  }

  @Test
  public void testInvalidFormat() {
    String inputJson = "{\"invalid-field-name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(url, null, recordSchema, "topic1", false, reader,
            false, true, false);
    try {
      avroReader.readMessage();
      fail("Registering an invalid schema should fail");
    } catch (SerializationException e) {
      assertTrue("The cause of the exception should be avro",
                 e.getCause() instanceof AvroRuntimeException);
    }
  }

  @Test
  public void testStringKey() {
    props.put("print.key", "true");
    formatter = new AvroMessageFormatter(url, new StringDeserializer());
    formatter.init(props);

    String inputJson = "{\"name\":\"myname\"}\n";
    String expectedJson = "TestKey\t"+inputJson;
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(url, null, recordSchema, "topic1", false, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();

    byte[] serializedKey = "TestKey".getBytes();
    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, serializedKey.length,
        serializedValue.length, serializedKey, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();

    assertEquals("Input key/value json should match output key/value json", expectedJson, outputJson);
  }

  @Test
  public void testStringKeyWithTimestamp() {
    props.put("print.key", "true");
    props.put("print.timestamp", "true");
    formatter = new AvroMessageFormatter(url, new StringDeserializer());
    formatter.init(props);

    long timestamp = 1000;
    TimestampType timestampType = TimestampType.LOG_APPEND_TIME;

    String inputJson = "{\"name\":\"myname\"}\n";
    String expectedJson = String.format("%s:%d\tTestKey\t%s",
        timestampType.name, timestamp, inputJson);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(url, null, recordSchema, "topic1", false, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();

    byte[] serializedKey = "TestKey".getBytes();
    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
            "topic1", 0, 200, timestamp, timestampType, 0, serializedKey.length,
            serializedValue.length, serializedKey, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();

    assertEquals("Input key/value json should match output key/value json", expectedJson, outputJson);
  }

  @Test
  public void testKafkaAvroValueUsingLatestVersion() throws Exception {
    formatter.init(props);

    schemaRegistry.register("topic1-value", new AvroSchema(recordSchema));

    String inputJson = "{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(url, null, recordSchema, "topic1", false, reader,
            false, false, true);
    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();

    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, 0, serializedValue.length,
        null, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();

    assertEquals("Input value json should match output value json", inputJson, outputJson);
  }

  @Test
  public void testUsingTopicRecordNameStrategy() throws Exception {
    final Map<String, String> propertyMap = new HashMap<>();
    final String topicName = "mytopic";
    propertyMap.put("topic", topicName);
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://foo");
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    propertyMap.put(SchemaMessageReader.VALUE_SCHEMA, RECORD_SCHEMA_STRING);

    final AvroMessageFormatter avroMessageFormatter = new AvroMessageFormatter();
    avroMessageFormatter.configure(propertyMap);

    final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("foo");

    schemaRegistryClient.register(topicName + "-value", new AvroSchema(recordSchema));

    String inputJson = "{\"name\":\"myname\"}\n";
    final InputStream is = new ByteArrayInputStream(inputJson.getBytes());
    AvroMessageReader avroReader = new AvroMessageReader();
    // Initialize AvroMessageReader using the same approach that ConsoleProducer uses so we exercise that code
    final Properties properties = new Properties();
    properties.putAll(propertyMap);
    avroReader.init(is, properties);

    try {
      ProducerRecord<byte[], byte[]> message = avroReader.readMessage();
      fail("Expected exception was not thrown. Exception should have been thrown due to schema not present in the " +
              "mock schema registry with the TopicRecordNameStrategy, and auto-register disabled.");
    } catch (SerializationException e) {
      assertTrue(e.getMessage().contains("Error retrieving Avro schema"));
    }

    // Now register the schema with the proper name and try again
    schemaRegistryClient.register(topicName + "-" + recordSchema.getFullName(), new AvroSchema(recordSchema));

    is.reset();
    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();

    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
            "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, 0, serializedValue.length,
            null, serializedValue);

    avroMessageFormatter.writeTo(crecord, ps);

    String outputJson = baos.toString();
    assertEquals("Input value json should match output value json", inputJson, outputJson);
  }

  @Test
  public void testUsingSubjectNameStrategy() throws Exception {
    final Map<String, String> propertyMap = new HashMap<>();
    final String topicName = "mytopic";
    propertyMap.put("topic", topicName);
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://foo");
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    propertyMap.put(SchemaMessageReader.VALUE_SCHEMA, RECORD_VALUE_SCHEMA_STRING);
    propertyMap.put(SchemaMessageReader.KEY_SCHEMA, RECORD_KEY_SCHEMA_STRING);
    propertyMap.put("parse.key", "true");
    propertyMap.put("print.key", "true");

    final AvroMessageFormatter avroMessageFormatter = new AvroMessageFormatter();
    avroMessageFormatter.configure(propertyMap);

    final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("foo");

    Schema.Parser parser = new Schema.Parser();
    Schema keySchema = parser.parse(RECORD_KEY_SCHEMA_STRING);
    Schema valueSchema = parser.parse(RECORD_VALUE_SCHEMA_STRING);
    schemaRegistryClient.register(topicName + "-key", new AvroSchema(keySchema));
    schemaRegistryClient.register(topicName + "-value", new AvroSchema(valueSchema));

    String inputJson = "{\"key_field\":\"1\"}\t{\"value_field\":\"1\"}\n";
    final InputStream is = new ByteArrayInputStream(inputJson.getBytes());
    AvroMessageReader avroReader = new AvroMessageReader();
    // Initialize AvroMessageReader using the same approach that ConsoleProducer uses so we exercise that code
    final Properties properties = new Properties();
    properties.putAll(propertyMap);
    avroReader.init(is, properties);

    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();
    byte[] serializedKey = message.key();
    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        topicName, 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, serializedKey.length, serializedValue.length,
        serializedKey, serializedValue);

    avroMessageFormatter.writeTo(crecord, ps);
    String outputJson = baos.toString();
    assertEquals("Input value json should match output value json", inputJson, outputJson);
  }

  @Test
  public void testUsingHeaders() throws Exception {
    final Map<String, String> propertyMap = new HashMap<>();
    final String topicName = "mytopic";
    propertyMap.put("topic", topicName);
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://foo");
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    propertyMap.put(SchemaMessageReader.VALUE_SCHEMA, RECORD_VALUE_SCHEMA_STRING);
    propertyMap.put(SchemaMessageReader.KEY_SCHEMA, RECORD_KEY_SCHEMA_STRING);
    propertyMap.put("parse.key", "true");
    propertyMap.put("parse.headers", "true");
    propertyMap.put("print.key", "true");
    propertyMap.put("print.headers", "true");
    propertyMap.put("headers.deserializer", StringDeserializer.class.getName());

    final AvroMessageFormatter avroMessageFormatter = new AvroMessageFormatter();
    avroMessageFormatter.configure(propertyMap);

    final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("foo");

    Schema.Parser parser = new Schema.Parser();
    Schema keySchema = parser.parse(RECORD_KEY_SCHEMA_STRING);
    Schema valueSchema = parser.parse(RECORD_VALUE_SCHEMA_STRING);
    schemaRegistryClient.register(topicName + "-key", new AvroSchema(keySchema));
    schemaRegistryClient.register(topicName + "-value", new AvroSchema(valueSchema));

    String input = "headerKey0:headerValue0,headerKey1:headerValue\t{\"key_field\":\"1\"}\t{\"value_field\":\"1\"}\n";
    final InputStream is = new ByteArrayInputStream(input.getBytes());
    AvroMessageReader avroReader = new AvroMessageReader();
    // Initialize AvroMessageReader using the same approach that ConsoleProducer uses so we exercise that code
    final Properties properties = new Properties();
    properties.putAll(propertyMap);
    avroReader.init(is, properties);

    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();
    byte[] serializedKey = message.key();
    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        topicName, 0, 200, 1000, TimestampType.LOG_APPEND_TIME, serializedKey.length, serializedValue.length,
        serializedKey, serializedValue, message.headers(), Optional.empty());

    avroMessageFormatter.writeTo(crecord, ps);
    String output = baos.toString();
    assertEquals("Input value should match output value", input, output);
  }

  @Test
  public void testUsingNull() throws Exception {
    final Map<String, String> propertyMap = new HashMap<>();
    final String topicName = "mytopic";
    propertyMap.put("topic", topicName);
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://foo");
    propertyMap.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    propertyMap.put(SchemaMessageReader.VALUE_SCHEMA, RECORD_VALUE_SCHEMA_STRING);
    propertyMap.put(SchemaMessageReader.KEY_SCHEMA, RECORD_KEY_SCHEMA_STRING);
    propertyMap.put("parse.key", "true");
    propertyMap.put("parse.headers", "true");
    propertyMap.put("null.marker", "<NULL>");
    propertyMap.put("print.key", "true");
    propertyMap.put("print.headers", "true");
    propertyMap.put("null.literal", "<NULL>");
    propertyMap.put("headers.deserializer", StringDeserializer.class.getName());

    final AvroMessageFormatter avroMessageFormatter = new AvroMessageFormatter();
    avroMessageFormatter.configure(propertyMap);

    final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("foo");

    Schema.Parser parser = new Schema.Parser();
    Schema keySchema = parser.parse(RECORD_KEY_SCHEMA_STRING);
    Schema valueSchema = parser.parse(RECORD_VALUE_SCHEMA_STRING);
    schemaRegistryClient.register(topicName + "-key", new AvroSchema(keySchema));
    schemaRegistryClient.register(topicName + "-value", new AvroSchema(valueSchema));

    String input = "headerKey0:<NULL>,headerKey1:<NULL>\t<NULL>\t<NULL>\n";
    final InputStream is = new ByteArrayInputStream(input.getBytes());
    AvroMessageReader avroReader = new AvroMessageReader();
    // Initialize AvroMessageReader using the same approach that ConsoleProducer uses so we exercise that code
    final Properties properties = new Properties();
    properties.putAll(propertyMap);
    avroReader.init(is, properties);

    ProducerRecord<byte[], byte[]> message = avroReader.readMessage();
    byte[] serializedKey = message.key();
    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        topicName, 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, 0,
        serializedKey, serializedValue, message.headers(), Optional.empty());

    avroMessageFormatter.writeTo(crecord, ps);
    String output = baos.toString();
    assertEquals("Input value should match output value", input, output);
  }
}
