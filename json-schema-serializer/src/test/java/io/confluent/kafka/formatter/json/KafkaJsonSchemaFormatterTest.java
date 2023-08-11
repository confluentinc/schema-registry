/*
 * Copyright 2020 Confluent Inc.
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
package io.confluent.kafka.formatter.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaJsonSchemaFormatterTest {

  private Properties props;
  private JsonSchemaMessageFormatter formatter;
  private JsonSchema recordSchema = null;
  private JsonSchema keySchema = null;
  private String url = "mock://test";
  private static ObjectMapper objectMapper = new ObjectMapper();

  @Before
  public void setUp() {
    props = new Properties();
    props.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, url);

    String userSchema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}, "
        + "\"additionalProperties\": false }";
    recordSchema = new JsonSchema(userSchema);
    String keySchema = "{\"type\":\"integer\"}";
    this.keySchema = new JsonSchema(keySchema);
    formatter = new JsonSchemaMessageFormatter(url, null);
  }

  @After
  public void tearDown() {
    MockSchemaRegistry.dropScope("test");
  }

  @Test
  public void testKafkaJsonSchemaValueFormatter() throws Exception {
    formatter.init(props);

    String inputJson = "{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    JsonSchemaMessageReader jsonSchemaReader =
        new JsonSchemaMessageReader(url, null, recordSchema, "topic1", false, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = jsonSchemaReader.readMessage();

    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, 0, serializedValue.length,
        null, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();

    assertEquals("Input value json should match output value json",
        objectMapper.readTree(inputJson),
        objectMapper.readTree(outputJson));
  }

  @Test
  public void testKafkaJsonSchemaKeyValueFormatter() throws Exception {
    props.put("print.key", "true");
    formatter.init(props);

    String inputJson = "10\t{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    JsonSchemaMessageReader jsonSchemaReader =
        new JsonSchemaMessageReader(url, keySchema, recordSchema, "topic1", true, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = jsonSchemaReader.readMessage();

    byte[] serializedKey = message.key();
    byte[] serializedValue = message.value();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, serializedKey.length,
        serializedValue.length, serializedKey, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();

    assertEquals("Input key/value json should match output key/value json",
        objectMapper.readTree(inputJson),
        objectMapper.readTree(outputJson));
  }

  @Test
  public void testInvalidFormat() {
    String inputJson = "{\"invalid-field-name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    JsonSchemaMessageReader jsonSchemaReader =
        new JsonSchemaMessageReader(url, null, recordSchema, "topic1", false, reader,
            false, true, false);
    try {
      jsonSchemaReader.readMessage();
      fail("Registering an invalid schema should fail");
    } catch (SerializationException e) {
      assertTrue("The cause of the exception should be json schema",
                 e.getCause() instanceof SerializationException
      );
    }
  }
}
