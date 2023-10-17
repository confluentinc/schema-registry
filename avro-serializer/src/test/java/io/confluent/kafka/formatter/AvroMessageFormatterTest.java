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

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AvroMessageFormatterTest {

  private static final byte MAGIC_BYTE = 0x0;
  private static final String KEY_STRING = "These are other bytes.";
  private static final String A_STRING = "These are bytes.";
  private static final byte[] KEY_BYTES = KEY_STRING.getBytes();
  private static final byte[] SOME_BYTES = A_STRING.getBytes();
  private static final int KEY_SCHEMA_ID = 2;
  private static final int VALUE_SCHEMA_ID = 1;
  private static final byte[] KEY_SCHEMA_ID_BYTES = ByteBuffer.allocate(4).putInt(KEY_SCHEMA_ID).array();
  private static final byte[] SCHEMA_ID_BYTES = ByteBuffer.allocate(4).putInt(VALUE_SCHEMA_ID).array();

  private AvroMessageFormatter formatter;
  private Properties props;
  private String url = "mock://test";
  private SchemaRegistryClient schemaRegistry;
  private ConsumerRecord<byte[], byte[]> recordWithValue;
  private ConsumerRecord<byte[], byte[]> recordWithKeyAndValue;

  @Before
  public void setup() throws IOException, RestClientException {
    props = new Properties();
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, url);

    schemaRegistry = MockSchemaRegistry.getClientForScope("test");
    recordWithValue = createConsumerRecord(false);
    recordWithKeyAndValue = createConsumerRecord(true);

    Schema schema1 = Schema.create(Type.BYTES);
    Schema schema2 = Schema.create(Type.BYTES);
    schema2.addProp("foo", "bar"); // must be different than schema1
    schemaRegistry.register("topicname", new AvroSchema(schema1));
    schemaRegistry.register("othertopic", new AvroSchema(schema2));

    formatter = new AvroMessageFormatter(url, null);
  }

  @After
  public void tearDown() {
    MockSchemaRegistry.dropScope("test");
  }

  @Test
  public void testDeserializeBytesIssue506() throws IOException, RestClientException {
		formatter.init(props);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    formatter.writeTo(recordWithValue, ps);
    assertEquals("\"" + A_STRING + "\"\n", baos.toString());
  }

  @Test
  public void testDeserializeRecordWithKeyAndValue() throws IOException, RestClientException {
    formatter.init(props);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    formatter.writeTo(recordWithKeyAndValue, ps);
    assertEquals("\"" + A_STRING + "\"\n", baos.toString());
  }

  @Test
  public void testDeserializeRecordWithKeyAndValueAndPrintingKey() throws IOException, RestClientException {
    props.put("print.key", "true");
    formatter.init(props);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    formatter.writeTo(recordWithKeyAndValue, ps);
    assertEquals("\"" + KEY_STRING + "\"\t\"" + A_STRING + "\"\n", baos.toString());
  }

  @Test
  public void testDeserializeRecordWithKeyAndValueAndPrintingKeyWithoutSchemaId()
      throws IOException, RestClientException {
    props.put("print.key", "true");
    props.put("print.schema.ids", "false");
    formatter.init(props);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    formatter.writeTo(recordWithKeyAndValue, ps);
    assertEquals("\"" + KEY_STRING + "\"\t\"" + A_STRING + "\"\n", baos.toString());
  }

  @Test
  public void testDeserializeRecordWithKeyAndValueAndPrintingKeyAndSchemaIds()
      throws IOException, RestClientException {
    props.put("print.key", "true");
    props.put("print.schema.ids", "true");
    formatter.init(props);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    formatter.writeTo(recordWithKeyAndValue, ps);
    assertEquals("\"" + KEY_STRING + "\"\t2\t\"" + A_STRING + "\"\t1\n", baos.toString());

    baos = new ByteArrayOutputStream();
    ps = new PrintStream(baos);
    formatter.writeTo(recordWithValue, ps);
    assertEquals("null\tnull\t\"" + A_STRING + "\"\t1\n", baos.toString());
  }

  @Test
  public void testDeserializeRecordWithKeyAndValueAndPrintingSchemaIds()
      throws IOException, RestClientException {
    props.put("print.key", "false");
    props.put("print.schema.ids", "true");
    formatter.init(props);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    formatter.writeTo(recordWithKeyAndValue, ps);
    assertEquals("\"" + A_STRING + "\"\t1\n", baos.toString());
  }

  @Test
  public void testDeserializeRecordWithKeyAndValueAndPrintingSchemaIdsWithDelimiter()
      throws IOException, RestClientException {
    props.put("print.key", "false");
    props.put("print.schema.ids", "true");
    props.put("schema.id.separator", "___");
    formatter.init(props);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    formatter.writeTo(recordWithKeyAndValue, ps);
    assertEquals("\"" + A_STRING + "\"___1\n", baos.toString());
  }

  @Test
  public void testDeserializeRecordWithKeyAndValueAndPrintingKeysAndSchemaIdsWithDelimiter()
      throws IOException, RestClientException {
    props.put("print.key", "true");
    props.put("print.schema.ids", "true");
    props.put("schema.id.separator", "___");
    formatter.init(props);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    formatter.writeTo(recordWithKeyAndValue, ps);
    assertEquals("\"" + KEY_STRING + "\"___2\t\"" + A_STRING + "\"___1\n", baos.toString());
  }
  protected ConsumerRecord<byte[], byte[]> createConsumerRecord(boolean includeKey) {
    byte[] key = new byte[1 + KEY_SCHEMA_ID_BYTES.length + KEY_BYTES.length];
    key[0] = MAGIC_BYTE;
    System.arraycopy(KEY_SCHEMA_ID_BYTES, 0, key, 1, KEY_SCHEMA_ID_BYTES.length);
    System.arraycopy(KEY_BYTES, 0, key, 1 + KEY_SCHEMA_ID_BYTES.length, KEY_BYTES.length);

    byte[] value = new byte[1 + SCHEMA_ID_BYTES.length + SOME_BYTES.length];
    value[0] = MAGIC_BYTE;
    System.arraycopy(SCHEMA_ID_BYTES, 0, value, 1, SCHEMA_ID_BYTES.length);
    System.arraycopy(SOME_BYTES, 0, value, 1 + SCHEMA_ID_BYTES.length, SOME_BYTES.length);

    return new ConsumerRecord<>(
        "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0,
        0, value.length,
        includeKey ? key : null, value);
  }
}
