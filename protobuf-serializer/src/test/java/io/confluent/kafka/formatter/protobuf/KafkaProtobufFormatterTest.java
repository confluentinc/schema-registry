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
package io.confluent.kafka.formatter.protobuf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaProtobufFormatterTest {

  private Properties props;
  private ProtobufMessageFormatter formatter;
  private ProtobufSchema recordSchema = null;
  private ProtobufSchema enumSchema = null;
  private ProtobufSchema keySchema = null;
  private ProtobufSchema snakeCaseSchema = null;
  private SchemaRegistryClient schemaRegistry = null;
  private static ObjectMapper objectMapper = new ObjectMapper();

  @Before
  public void setUp() {
    props = new Properties();
    props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put("preserve.json.field.name", "true");

    String userSchema = "syntax = \"proto3\"; message User { string name = 1; } "
        + "message User2 { string full_name = 1; }";
    recordSchema = new ProtobufSchema(userSchema);
    String enumSchema = "syntax = \"proto3\"; message ConfluentDefault1 {enum Suit {SPADES = 0; "
      + "HEARTS = 1; DIAMONDS = 2; CLUBS = 4;} Suit c1 = 1;}";
    this.enumSchema = new ProtobufSchema(enumSchema);
    String keySchema = "syntax = \"proto3\"; message Key { int32 key = 1; }";
    this.keySchema = new ProtobufSchema(keySchema);
    String snakeCaseSchema = "syntax = \"proto3\"; message Foo { string first_field = 1;"
            + "string second_field = 2; }";
    this.snakeCaseSchema = new ProtobufSchema(snakeCaseSchema);
    schemaRegistry = new MockSchemaRegistryClient();
    formatter = new ProtobufMessageFormatter(schemaRegistry, null);
  }

  @Test
  public void testKafkaProtobufValueFormatter() throws Exception {
    formatter.init(props);

    String inputJson = "{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    ProtobufMessageReader protobufReader =
        new ProtobufMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = protobufReader.readMessage();

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
  public void testKafkaProtobufEnumValueFormatter() throws Exception {
    formatter.init(props);

    String inputJson = "{\"c1\":\"SPADES\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    ProtobufMessageReader protobufReader =
        new ProtobufMessageReader(schemaRegistry, null, enumSchema, "topic1", false, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = protobufReader.readMessage();

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
  public void testKafkaProtobufSnakeCaseFormatter() throws Exception {
    formatter.init(props);

    String inputJson = "{\"first_field\":\"first\",\"second_field\":\"second\"}\n";
    BufferedReader reader =
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    ProtobufMessageReader protobufReader =
            new ProtobufMessageReader(schemaRegistry, null, snakeCaseSchema, "topic1", false, reader,
                    false, true, false);
    ProducerRecord<byte[], byte[]> message = protobufReader.readMessage();

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
  public void testKafkaProtobufKeyValueFormatter() throws Exception {
    props.put("print.key", "true");
    formatter.init(props);

    String inputJson = "{\"key\":10}\t{\"name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    ProtobufMessageReader protobufReader =
        new ProtobufMessageReader(schemaRegistry, keySchema, recordSchema, "topic1", true, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = protobufReader.readMessage();

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
  public void testKafkaProtobufKeyValueFormatterNullMessage() throws Exception {
    formatter.init(props);

    byte[] serializedValue = null;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
        "topic1", 0, 200,  null, serializedValue);
    formatter.writeTo(crecord, ps);
    String outputJson = baos.toString();
    assertEquals("Null message should print \"null\"",
        "null\n",
        outputJson);
  }

  @Test
  public void testKafkaProtobufValueFormatterSecondMessage() throws Exception {
    formatter.init(props);

    String inputJson = "{\"full_name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    ProtobufMessageReader protobufReader =
        new ProtobufMessageReader(schemaRegistry, null, recordSchema.copy("User2"), "topic1", false, reader,
            false, true, false);
    ProducerRecord<byte[], byte[]> message = protobufReader.readMessage();

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
  public void testInvalidFormat() {
    String inputJson = "{\"invalid-field-name\":\"myname\"}\n";
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputJson.getBytes())));
    ProtobufMessageReader protobufReader =
        new ProtobufMessageReader(schemaRegistry, null, recordSchema, "topic1", false, reader,
            false, true, false);
    try {
      protobufReader.readMessage();
      fail("Registering an invalid schema should fail");
    } catch (SerializationException e) {
      assertTrue("The cause of the exception should be protobuf",
                 e.getCause() instanceof InvalidProtocolBufferException
      );
    }
  }
}
