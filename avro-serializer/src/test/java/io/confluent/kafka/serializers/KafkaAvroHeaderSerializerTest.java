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
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class KafkaAvroHeaderSerializerTest extends KafkaAvroSerializerTest {

  public KafkaAvroHeaderSerializerTest() {
  }

  @Override
  protected Properties createSerializerConfig() {
    Properties props = super.createSerializerConfig();
    props.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    return props;
  }

  @Test
  public void testKafkaAvroSerializerWithPreRegisteredUseSchemaGuid()
      throws IOException, RestClientException {
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    String guid = schemaRegistry.getLatestSchemaMetadata(topic + "-value").getGuid();
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_SCHEMA_GUID,
        guid,
        KafkaAvroSerializerConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName()
    );
    avroSerializer.configure(configs, false);
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerWithPreRegisteredUseSchemaGuidIncompatibleError()
      throws IOException, RestClientException {
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    schemaRegistry.register(topic + "-value", new AvroSchema(createAccountSchema()));
    String guid = schemaRegistry.getLatestSchemaMetadata(topic + "-value").getGuid();
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_SCHEMA_GUID,
        guid,
        KafkaAvroSerializerConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName()
    );
    avroSerializer.configure(configs, false);
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    RecordHeaders headers = new RecordHeaders();
    avroSerializer.serialize(topic, headers, annotatedUserRecord);
  }

  @Test
  public void testKafkaAvroSerializerWithPreRegisteredUseSchemaGuidIncompatibleNoError()
      throws IOException, RestClientException {
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    schemaRegistry.register(topic + "-value", new AvroSchema(createAccountSchema()));
    String guid = schemaRegistry.getLatestSchemaMetadata(topic + "-value").getGuid();
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_SCHEMA_GUID,
        guid,
        KafkaAvroSerializerConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName(),
        KafkaAvroSerializerConfig.ID_COMPATIBILITY_STRICT,
        false
    );
    avroSerializer.configure(configs, false);
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    // User gets deserialized as an account!
    IndexedRecord badRecord = createAccountRecord("testUser");
    assertEquals(badRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(badRecord, avroDecoder.fromBytes(headers, bytes));
  }
}
