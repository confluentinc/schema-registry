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
package io.confluent.kafka.formatter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.LocalSchemaRegistryClient;
import kafka.producer.KeyedMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaAvroFormatterTest {

  private Schema recordSchema = null;
  private Schema intSchema = null;
  private SchemaRegistryClient schemaRegistry = null;

  @Before
  public void setUp() {
    String userSchema = "{\"namespace\": \"example.avro\"," +
                        "\"type\": \"record\"," +
                        "\"name\": \"User\"," +
                        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    recordSchema = parser.parse(userSchema);
    intSchema = parser.parse("{\"type\" : \"int\"}");
    schemaRegistry = new LocalSchemaRegistryClient();
  }

  @Test
  public void testKafkaAvroValueFormatter() {
    String inputJson = "{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader);
    KeyedMessage keyedMessage = avroReader.readMessage();

    byte[] serializedValue = (byte[]) keyedMessage.message();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    AvroMessageFormatter formatter = new AvroMessageFormatter(schemaRegistry, false);
    formatter.writeTo(null, serializedValue, ps);
    String outputJson = baos.toString();

    assertEquals("Input value json should match output value json", inputJson, outputJson);
  }

  @Test
  public void testKafkaAvroKeyValueFormatter() {
    String inputJson = "10\t{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(schemaRegistry, intSchema, recordSchema, "topic1", true, reader);
    KeyedMessage keyedMessage = avroReader.readMessage();

    byte[] serializedKey = (byte[]) keyedMessage.key();
    byte[] serializedKeyValue = (byte[]) keyedMessage.message();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    AvroMessageFormatter formatter = new AvroMessageFormatter(schemaRegistry, true);
    formatter.writeTo(serializedKey, serializedKeyValue, ps);
    String outputJson = baos.toString();

    assertEquals("Input key/value json should match output key/value json", inputJson, outputJson);
  }

  @Test
  public void testInvalidFormat() {
    String inputJson = "{\"invalid-field-name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    AvroMessageReader avroReader =
        new AvroMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader);
    try {
      avroReader.readMessage();
    } catch (SerializationException e) {
      assertTrue("The cause of the exception should be avro",
                 e.getCause() instanceof AvroRuntimeException);
    }
  }
}