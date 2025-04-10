/*
 * Copyright 2025 Confluent Inc.
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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.schema.id.ConfigSchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class KafkaAvroConfigDeserializerTest {

  private final Properties defaultConfig;
  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroDecoder avroDecoder;
  private final String topic;

  public KafkaAvroConfigDeserializerTest() {
    defaultConfig = createSerializerConfig();
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    Properties deserializerConfig = createDeserializerConfig();
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, new HashMap(deserializerConfig));
    avroDecoder = new KafkaAvroDecoder(schemaRegistry, new VerifiableProperties(deserializerConfig));
    topic = "test";
  }

  protected Properties createSerializerConfig() {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializerConfig.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
    serializerConfig.put(KafkaAvroSerializerConfig.USE_SCHEMA_ID, 1);
    serializerConfig.put(KafkaAvroSerializerConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    return serializerConfig;
  }

  protected Properties createDeserializerConfig() {
    Properties deserializerConfig = new Properties();
    deserializerConfig.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    deserializerConfig.put(KafkaAvroSerializerConfig.USE_SCHEMA_ID, 1);
    deserializerConfig.put(KafkaAvroDeserializerConfig.VALUE_SCHEMA_ID_DESERIALIZER,
        ConfigSchemaIdDeserializer.class.getName());
    return deserializerConfig;
  }

  private Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"User\"," +
        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private IndexedRecord createUserRecord() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  private IndexedRecord createAnnotatedUserRecord() {
    return io.confluent.kafka.example.annotated.User.newBuilder().setName("testUser").build();
  }

  @Test
  public void testKafkaAvroSerializerWithPreRegisteredUseSchemaId()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_SCHEMA_ID,
        1,
        KafkaAvroSerializerConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName()
    );
    avroSerializer.configure(configs, false);
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));
  }
}
