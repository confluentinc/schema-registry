/**
 * Copyright 2017 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

public class AvroMessageFormatterTest {

  private static final byte MAGIC_BYTE = 0x0;
  private static final String A_STRING = "These are bytes.";
  private static final byte[] SOME_BYTES = A_STRING.getBytes();
  private static final byte[] SCHEMA_ID = ByteBuffer.allocate(4).putInt(1).array();
  private static final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();

  @Test
  public void testDeserializeBytesIssue506() throws IOException, RestClientException {
    Map<String,String> properties = new HashMap<String,String>();
    properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    AvroMessageFormatter formatter = new AvroMessageFormatter(schemaRegistry, false, null);
		
    byte[] message = new byte[1 + SCHEMA_ID.length + SOME_BYTES.length];
    message[0] = MAGIC_BYTE;
    System.arraycopy(SCHEMA_ID, 0, message, 1, SCHEMA_ID.length);
    System.arraycopy(SOME_BYTES, 0, message, 1 + SCHEMA_ID.length, SOME_BYTES.length);

    Schema schema = Schema.create(Type.BYTES);
    schemaRegistry.register("topicname", schema);
		
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>(
      "topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, 
	  0, message.length,
      null, message);
    formatter.writeTo(crecord, ps);
    assertEquals("\"" + A_STRING + "\"\n", baos.toString());
  }

}
