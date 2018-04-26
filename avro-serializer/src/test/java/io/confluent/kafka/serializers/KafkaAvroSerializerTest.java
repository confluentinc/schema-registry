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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.kafka.example.ExtendedUser;
import io.confluent.kafka.example.User;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

public class KafkaAvroSerializerTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroEncoder avroEncoder;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroDecoder avroDecoder;
  private final String topic;
  private final KafkaAvroDeserializer specificAvroDeserializer;
  private final KafkaAvroDecoder specificAvroDecoder;

  public KafkaAvroSerializerTest() {
    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroEncoder = new KafkaAvroEncoder(schemaRegistry, new VerifiableProperties(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
    avroDecoder = new KafkaAvroDecoder(schemaRegistry, new VerifiableProperties(defaultConfig));
    topic = "test";
    specificAvroDeserializer = buildSpecificDeserializer(schemaRegistry,false);
    specificAvroDecoder = buildSpecificDecoder(schemaRegistry, false);
  }

  private static KafkaAvroDeserializer buildSpecificDeserializer(
      SchemaRegistryClient schemaRegistry,
      Boolean unknownAsGeneric) {
    HashMap<String, String> specificDeserializerProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    specificDeserializerProps.put(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
        "true");
    specificDeserializerProps.put(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_UNKNOWN_AS_GENERIC_CONFIG,
        unknownAsGeneric.toString());
    return new KafkaAvroDeserializer(schemaRegistry, specificDeserializerProps);
  }

  private KafkaAvroDecoder buildSpecificDecoder(
      SchemaRegistryClient schemaRegistry,
      Boolean unknownAsGeneric) {
    Properties specificDecoderProps = new Properties();
    specificDecoderProps.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    specificDecoderProps.setProperty(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    specificDecoderProps.put(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_UNKNOWN_AS_GENERIC_CONFIG,
        unknownAsGeneric.toString());
    return new KafkaAvroDecoder(
        schemaRegistry, new VerifiableProperties(specificDecoderProps));
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

  private IndexedRecord createAccountRecord() {
    String accountSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                           "\"name\": \"Account\"," +
                           "\"fields\": [{\"name\": \"accountNumber\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(accountSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("accountNumber", "0123456789");
    return avroRecord;
  }

  private IndexedRecord createSpecificAvroRecord() {
    return User.newBuilder().setName("testUser").build();
  }

  private IndexedRecord createExtendedSpecificAvroRecord() {
    return ExtendedUser.newBuilder()
        .setName("testUser")
        .setAge(99)
        .build();
  }

  private IndexedRecord createInvalidAvroRecord() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                        "\"name\": \"User\"," +
                        "\"fields\": [{\"name\": \"f1\", \"type\": \"string\"}," +
                        "{\"name\": \"f2\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("f1", "value1");
    avroRecord.put("f1", 12);
    // intentionally miss setting a required field f2
    return avroRecord;
  }

  @Test
  public void testKafkaAvroSerializer() {
    byte[] bytes;
    IndexedRecord avroRecord = createAvroRecord();
    bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, null);
    assertEquals(null, avroDeserializer.deserialize(topic, bytes));
    assertEquals(null, avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, true);
    assertEquals(true, avroDeserializer.deserialize(topic, bytes));
    assertEquals(true, avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, 123);
    assertEquals(123, avroDeserializer.deserialize(topic, bytes));
    assertEquals(123, avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, 345L);
    assertEquals(345l, avroDeserializer.deserialize(topic, bytes));
    assertEquals(345l, avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, 1.23f);
    assertEquals(1.23f, avroDeserializer.deserialize(topic, bytes));
    assertEquals(1.23f, avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, 2.34d);
    assertEquals(2.34, avroDeserializer.deserialize(topic, bytes));
    assertEquals(2.34, avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, "abc");
    assertEquals("abc", avroDeserializer.deserialize(topic, bytes));
    assertEquals("abc", avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, "abc".getBytes());
    assertArrayEquals("abc".getBytes(), (byte[]) avroDeserializer.deserialize(topic, bytes));
    assertArrayEquals("abc".getBytes(), (byte[]) avroDecoder.fromBytes(bytes));

    bytes = avroSerializer.serialize(topic, new Utf8("abc"));
    assertEquals("abc", avroDeserializer.deserialize(topic, bytes));
    assertEquals("abc", avroDecoder.fromBytes(bytes));
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerWithoutAutoRegister() {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false
    );
    avroSerializer.configure(configs, false);
    IndexedRecord avroRecord = createAvroRecord();
    avroSerializer.serialize(topic, avroRecord);
  }

  @Test
  public void testKafkaAvroSerializerWithPreRegistered() throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false
    );
    avroSerializer.configure(configs, false);
    IndexedRecord avroRecord = createAvroRecord();
    schemaRegistry.register(topic + "-value", avroRecord.getSchema());
    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(bytes));
  }

  @Test
  public void testKafkaAvroSerializerWithMultiType() throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        TopicRecordNameStrategy.class.getName()
    );
    avroSerializer.configure(configs, false);
    IndexedRecord record1 = createAvroRecord();
    IndexedRecord record2 = createAccountRecord();
    byte[] bytes1 = avroSerializer.serialize(topic, record1);
    byte[] bytes2 = avroSerializer.serialize(topic, record2);
    assertNotNull(schemaRegistry.getLatestSchemaMetadata(topic + "-example.avro.User"));
    assertNotNull(schemaRegistry.getLatestSchemaMetadata(topic + "-example.avro.Account"));
    assertEquals(record1, avroDeserializer.deserialize(topic, bytes1));
    assertEquals(record2, avroDeserializer.deserialize(topic, bytes2));
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerWithMultiTypeError() {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        TopicRecordNameStrategy.class.getName()
    );
    avroSerializer.configure(configs, false);
    avroSerializer.serialize(topic, "a string should not be allowed");
  }

  @Test
  public void testKafkaAvroSerializerWithProjection() {
    byte[] bytes;
    Object obj;
    IndexedRecord avroRecord = createExtendedSpecificAvroRecord();
    bytes = avroSerializer.serialize(topic, avroRecord);

    obj = avroDecoder.fromBytes(bytes);
    GenericData.Record extendedUser = (GenericData.Record) obj;
    assertTrue(
        "Returned object should be a GenericData Record",
        GenericData.Record.class.isInstance(obj)
    );
    //Age field is visible
    assertNotNull(extendedUser.get("age"));

    obj = avroDecoder.fromBytes(bytes, User.getClassSchema());
    assertTrue(
        "Returned object should be a GenericData Record",
        GenericData.Record.class.isInstance(obj)
    );
    GenericData.Record decoderProjection = (GenericData.Record) obj;
    assertEquals("testUser", decoderProjection.get("name").toString());
    //Age field was hidden by projection
    assertNull(decoderProjection.get("age"));

    obj = avroDeserializer.deserialize(topic, bytes, User.getClassSchema());
    assertTrue(
        "Returned object should be a GenericData Record",
        GenericData.Record.class.isInstance(obj)
    );
    GenericData.Record deserializeProjection = (GenericData.Record) obj;
    assertEquals("testUser", deserializeProjection.get("name").toString());
    //Age field was hidden by projection
    assertNull(deserializeProjection.get("age"));
  }

  @Test
  public void testKafkaAvroSerializerSpecificRecord() {
    byte[] bytes;
    Object obj;

    IndexedRecord avroRecord = createSpecificAvroRecord();
    bytes = avroSerializer.serialize(topic, avroRecord);

    obj = avroDecoder.fromBytes(bytes);
    assertTrue(
        "Returned object should be a GenericData Record",
        GenericData.Record.class.isInstance(obj)
    );

    obj = specificAvroDecoder.fromBytes(bytes);
    assertTrue(
        "Returned object should be a io.confluent.kafka.example.User",
        User.class.isInstance(obj)
    );
    assertEquals(avroRecord, obj);

    obj = specificAvroDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a io.confluent.kafka.example.User",
        User.class.isInstance(obj)
    );
    assertEquals(avroRecord, obj);
  }

  @Test
  public void testKafkaAvroSerializerSpecificRecordWithProjection() {
    byte[] bytes;
    Object obj;

    IndexedRecord avroRecord = createExtendedSpecificAvroRecord();
    bytes = avroSerializer.serialize(topic, avroRecord);

    obj = specificAvroDecoder.fromBytes(bytes);
    assertTrue(
        "Full object should be a io.confluent.kafka.example.ExtendedUser",
        ExtendedUser.class.isInstance(obj)
    );
    assertEquals(avroRecord, obj);

    obj = specificAvroDecoder.fromBytes(bytes, User.getClassSchema());
    assertTrue(
        "Projection object should be a io.confluent.kafka.example.User",
        User.class.isInstance(obj)
    );
    assertEquals("testUser", ((User) obj).getName().toString());

    obj = specificAvroDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Full object should be a io.confluent.kafka.example.ExtendedUser",
        ExtendedUser.class.isInstance(obj)
    );
    assertEquals(avroRecord, obj);

    obj = specificAvroDeserializer.deserialize(topic, bytes, User.getClassSchema());
    assertTrue(
        "Projection object should be a io.confluent.kafka.example.User",
        User.class.isInstance(obj)
    );
    assertEquals("testUser", ((User) obj).getName().toString());
  }

  @Test
  public void testKafkaAvroSerializerNonexistantSpecificRecord() {
    byte[] bytes;

    IndexedRecord avroRecord = createAvroRecord();
    bytes = avroSerializer.serialize(topic, avroRecord);

    try {
      specificAvroDecoder.fromBytes(bytes);
      fail("Did not throw an exception when class for specific avro record does not exist.");
    } catch (SerializationException e) {
      // this is expected
    } catch (Exception e) {
      fail("Threw the incorrect exception when class for specific avro record does not exist.");
    }

    try {
      specificAvroDeserializer.deserialize(topic, bytes);
      fail("Did not throw an exception when class for specific avro record does not exist.");
    } catch (SerializationException e) {
      // this is expected
    } catch (Exception e) {
      fail("Threw the incorrect exception when class for specific avro record does not exist.");
    }
  }

  @Test
  public void testKafkaAvroSerializerUnknownAsGeneric_whenNonexistantSpecificRecord() {
    byte[] bytes;
    Object obj;

    KafkaAvroDeserializer specificAvroDeserializer = buildSpecificDeserializer(
        schemaRegistry, true);
    KafkaAvroDecoder specificAvroDecoder = buildSpecificDecoder(schemaRegistry,
        true);
    IndexedRecord avroRecord = createAvroRecord();

    bytes = avroSerializer.serialize(topic, avroRecord);

    try {
      obj = specificAvroDecoder.fromBytes(bytes);
      assertTrue(
          "Returned object should be a GenericData Record",
          GenericData.Record.class.isInstance(obj)
      );
    } catch (Exception e) {
      fail("Threw an exception when class for specific avro record does not exist.");
    }

    try {
      obj = specificAvroDeserializer.deserialize(topic, bytes);
      assertTrue(
          "Returned object should be a GenericData Record",
          GenericData.Record.class.isInstance(obj)
      );
    } catch (Exception e) {
      fail("Threw an exception when class for specific avro record does not exist.");
    }
  }

  @Test
  public void testNull() {
    SchemaRegistryClient nullSchemaRegistryClient = null;
    KafkaAvroSerializer nullAvroSerializer = new KafkaAvroSerializer(nullSchemaRegistryClient);

    // null doesn't require schema registration. So serialization should succeed with a null
    // schema registry client.
    assertEquals(null, nullAvroSerializer.serialize("test", null));
  }

  @Test
  public void testKafkaAvroEncoder() {
    byte[] bytes;
    Object obj;
    IndexedRecord avroRecord = createAvroRecord();
    bytes = avroEncoder.toBytes(avroRecord);
    obj = avroDecoder.fromBytes(bytes);
    assertEquals(avroRecord, obj);
  }

  @Test
  public void testInvalidInput() {
    IndexedRecord invalidRecord = createInvalidAvroRecord();
    try {
      avroSerializer.serialize(topic, invalidRecord);
      fail("Sending invalid record should fail serializer");
    } catch (SerializationException e) {
      // this is expected
    }

    try {
      avroEncoder.toBytes(invalidRecord);
      fail("Sending invalid record should fail encoder");
    } catch (SerializationException e) {
      // this is expected
    }

    try {
      avroEncoder.toBytes("abc");
      fail("Sending data of unsupported type should fail encoder");
    } catch (SerializationException e) {
      // this is expected
    }
  }

  @Test
  public void test_schemas_per_subject() {
    HashMap<String, String> props = new HashMap<>();
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    props.put(AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG, "5");
    avroSerializer.configure(props, false);
  }

  @Test
  public void testKafkaAvroSerializerSpecificRecordWithPrimitives() {
    byte[] bytes;
    Object obj;

    String message = "testKafkaAvroSerializerSpecificRecordWithPrimitives";
    bytes = avroSerializer.serialize(topic, message);

    obj = avroDecoder.fromBytes(bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));

    obj = specificAvroDecoder.fromBytes(bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);

    obj = specificAvroDeserializer.deserialize(topic, bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);
  }

}
