/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.kafka.formatter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaAvroFormatterTest {

  private Properties props;
  private AvroMessageFormatter formatter;
  private Schema recordSchema = null;
  private Schema intSchema = null;
  private SchemaRegistryClient schemaRegistry = null;

  @Before
  public void setUp() {
    props = new Properties();
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");

    String userSchema = "{\"namespace\": \"example.avro\"," +
                        "\"type\": \"record\"," +
                        "\"name\": \"User\"," +
                        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    recordSchema = parser.parse(userSchema);
    intSchema = parser.parse("{\"type\" : \"int\"}");
    schemaRegistry = new MockSchemaRegistryClient();
    formatter = new AvroMessageFormatter(schemaRegistry, null);
  }

  @Test
  public void testKafkaAvroValueFormatter() {
    formatter.init(props);

    String inputJson = "{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader, true);
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
        new AvroMessageReader(schemaRegistry, intSchema, recordSchema, "topic1", true, reader, true);
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
            new AvroMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader, true);
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
        new AvroMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader, true);
    try {
      avroReader.readMessage();
    } catch (SerializationException e) {
      assertTrue("The cause of the exception should be avro",
                 e.getCause() instanceof AvroRuntimeException);
    }
  }

  @Test
  public void testStringKey() {
    props.put("print.key", "true");
    formatter = new AvroMessageFormatter(schemaRegistry, new StringDeserializer());
    formatter.init(props);

    String inputJson = "{\"name\":\"myname\"}\n";
    String expectedJson = "TestKey\t"+inputJson;
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader,
            true);
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
    formatter = new AvroMessageFormatter(schemaRegistry, new StringDeserializer());
    formatter.init(props);

    long timestamp = 1000;
    TimestampType timestampType = TimestampType.LOG_APPEND_TIME;

    String inputJson = "{\"name\":\"myname\"}\n";
    String expectedJson = String.format("%s:%d\tTestKey\t%s",
            timestampType.name, timestamp, inputJson);
    BufferedReader reader =
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
            new AvroMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader,
                    true);
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
}
