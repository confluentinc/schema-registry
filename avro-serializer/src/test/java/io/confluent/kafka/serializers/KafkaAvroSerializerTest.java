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
package io.confluent.kafka.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class KafkaAvroSerializerTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroEncoder avroEncoder;
  private final KafkaAvroDecoder avroDecoder;
  private final String topic;

  public KafkaAvroSerializerTest() {
    schemaRegistry = new LocalSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry);
    avroEncoder = new KafkaAvroEncoder(schemaRegistry);
    avroDecoder = new KafkaAvroDecoder(schemaRegistry);
    topic = "test";
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

  @Test
  public void testKafkaAvroSerializer() {
    byte[] bytes;
    Object obj;
    IndexedRecord avroRecord = createAvroRecord();
    bytes= avroSerializer.serialize(topic, avroRecord);
    obj =  avroDecoder.fromBytes(bytes);
    assertEquals(avroRecord, obj);

    bytes = avroSerializer.serialize(topic, null);
    obj = avroDecoder.fromBytes(bytes);
    assertEquals(null, obj);

    bytes = avroSerializer.serialize(topic, true);
    obj = avroDecoder.fromBytes(bytes);
    assertEquals(true, obj);

    bytes = avroSerializer.serialize(topic, 123);
    obj = avroDecoder.fromBytes(bytes);
    assertEquals(123, obj);

    bytes = avroSerializer.serialize(topic, 345L);
    obj = avroDecoder.fromBytes(bytes);
    assertEquals(345l, obj);

    bytes = avroSerializer.serialize(topic, 1.23f);
    obj = avroDecoder.fromBytes(bytes);
    assertEquals(1.23f, obj);

    bytes = avroSerializer.serialize(topic, 2.34d);
    obj = avroDecoder.fromBytes(bytes);
    assertEquals(2.34, obj);

    bytes = avroSerializer.serialize(topic, "abc");
    obj = avroDecoder.fromBytes(bytes);
    assertEquals("abc", obj);

    bytes = avroSerializer.serialize(topic, "abc".getBytes());
    obj = avroDecoder.fromBytes(bytes);
    assertArrayEquals("abc".getBytes(), (byte[]) obj);
  }

  @Test
  public void testKafkaAvroEncoder() {
    byte[] bytes;
    Object obj;
    IndexedRecord avroRecord = createAvroRecord();
    bytes= avroEncoder.toBytes(avroRecord);
    obj =  avroDecoder.fromBytes(bytes);
    assertEquals(avroRecord, obj);
  }
}
