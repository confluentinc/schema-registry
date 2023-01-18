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
package io.confluent.kafka.serializers;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.example.ExtendedWidget;
import io.confluent.kafka.example.Widget;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.avro.AvroSchema.Format;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.example.ExtendedUser;
import io.confluent.kafka.example.User;
import io.confluent.kafka.example.Grant;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import kafka.utils.VerifiableProperties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaAvroSerializerTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroDecoder avroDecoder;
  private final String topic;
  private final KafkaAvroDeserializer specificAvroDeserializer;
  private final KafkaAvroDecoder specificAvroDecoder;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final KafkaAvroDecoder reflectionAvroDecoder;

  public KafkaAvroSerializerTest() {
    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
    avroDecoder = new KafkaAvroDecoder(schemaRegistry, new VerifiableProperties(defaultConfig));
    topic = "test";

    HashMap<String, String> specificDeserializerProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    specificAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, specificDeserializerProps);

    Properties specificDecoderProps = new Properties();
    specificDecoderProps.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    specificDecoderProps.setProperty(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    specificAvroDecoder = new KafkaAvroDecoder(
        schemaRegistry, new VerifiableProperties(specificDecoderProps));

    HashMap<String, String> reflectionProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionAvroSerializer = new KafkaAvroSerializer(schemaRegistry, reflectionProps);
    reflectionAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, reflectionProps);

    Properties reflectionDecoderProps = new Properties();
    reflectionDecoderProps.setProperty(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    reflectionDecoderProps.setProperty(
            KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionAvroDecoder = new KafkaAvroDecoder(
            schemaRegistry, new VerifiableProperties(reflectionDecoderProps));
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

  private Schema createExtendUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"User\"," +
        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}, " +
        "{\"name\": \"age\", \"type\": [\"null\", \"int\"]}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private IndexedRecord createExtendUserRecordWithNullField() {
    Schema schema = createExtendUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  private IndexedRecord createExtendUserRecord() {
    Schema schema = createExtendUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("age", 30);
    return avroRecord;
  }

  private IndexedRecord createUserRecordUtf8() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", new Utf8("testUser"));
    return avroRecord;
  }

  private Schema createAccountSchema() {
    String accountSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"Account\"," +
        "\"fields\": [{\"name\": \"accountNumber\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(accountSchema);
    return schema;
  }

  private IndexedRecord createAccountRecord() {
    return createAccountRecord("0123456789");
  }

  private IndexedRecord createAccountRecord(String accountNumber) {
    Schema schema = createAccountSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("accountNumber", accountNumber);
    return avroRecord;
  }

  private Schema createBalanceSchema() {
    String balanceSchema = "{\n" +
            "\t\"namespace\": \"example.avro\", \"type\": \"record\",\n" +
            "    \"name\": \"Account\",\n" +
            "    \"fields\": [\n" +
            "    \t{\"name\": \"accountNumber\", \"type\": \"string\"},\n" +
            "        {\"name\": \"balance\", \"type\": {\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}},\n" +
            "        {\"name\": \"date\", \"type\": {\"type\":\"int\",\"logicalType\":\"date\"}},\n" +
            "        {\"name\": \"timeMs\", \"type\": {\"type\":\"int\",\"logicalType\":\"time-millis\"}},\n" +
            "        {\"name\": \"timeMicros\", \"type\": {\"type\":\"long\",\"logicalType\":\"time-micros\"}},\n" +
            "        {\"name\": \"tsMs\", \"type\": {\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},\n" +
            "        {\"name\": \"tsMicros\", \"type\": {\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},\n" +
            "        {\"name\": \"localTsMs\", \"type\": {\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}},\n" +
            "        {\"name\": \"localTsMicros\", \"type\": {\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}}\n" +
            "    ]\n" +
            "}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(balanceSchema);
    return schema;
  }

  private IndexedRecord createBalanceRecord() {
    Schema schema = createBalanceSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("accountNumber", "0123456789");
    avroRecord.put("balance", new BigDecimal("10.00"));
    avroRecord.put("date", LocalDate.of(2021,1, 1));
    avroRecord.put("timeMs", LocalTime.of(1, 1, 1, 1000000));
    avroRecord.put("timeMicros", LocalTime.of(1, 1, 1, 1001000));
    avroRecord.put("tsMs", Instant.ofEpochMilli(1613646696368L));
    avroRecord.put("tsMicros", Instant.ofEpochMilli(1613646696368009L));
    avroRecord.put("localTsMs", LocalDateTime.of(2021,1, 1, 1, 1, 1, 1000000));
    avroRecord.put("localTsMicros", LocalDateTime.of(2021,1, 1, 1, 1, 1, 1001000));
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

  private IndexedRecord createAnnotatedUserRecord() {
    return io.confluent.kafka.example.annotated.User.newBuilder().setName("testUser").build();
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

  private static final Schema arraySchema = new Schema.Parser().parse(
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"array\",\n"
          + " \"name\": \"test\",\n"
          + " \"items\": {\n"
          + "\"type\": \"record\",\n"
          + "\"namespace\": \"example.avro\",\n"
          + "\"name\": \"User\",\n"
          + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}}");

  private static final Schema mapSchema = new Schema.Parser().parse(
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"map\",\n"
          + " \"name\": \"test\",\n"
          + " \"values\": {\n"
          + "\"type\": \"record\",\n"
          + "\"namespace\": \"example.avro\",\n"
          + "\"name\": \"User\",\n"
          + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}}");

  @Test
  public void testKafkaAvroSerializer() {
    byte[] bytes;
    IndexedRecord avroRecord = createUserRecord();
    bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(bytes));

    IndexedRecord avroRecordWithAllField = createExtendUserRecord();
    bytes = avroSerializer.serialize(topic, avroRecordWithAllField);
    assertEquals(avroRecordWithAllField, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecordWithAllField, avroDecoder.fromBytes(bytes));

    IndexedRecord avroRecordWithoutOptional = createExtendUserRecordWithNullField();
    bytes = avroSerializer.serialize(topic, avroRecordWithoutOptional);
    assertEquals(avroRecordWithoutOptional, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecordWithoutOptional, avroDecoder.fromBytes(bytes));

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
    IndexedRecord avroRecord = createUserRecord();
    avroSerializer.serialize(topic, avroRecord);
  }

  @Test(expected = InvalidConfigurationException.class)
  public void testKafkaAvroSerializerWithoutConfigure() {
    KafkaAvroSerializer unconfiguredSerializer = new KafkaAvroSerializer();
    IndexedRecord avroRecord = createUserRecord();
    unconfiguredSerializer.serialize(topic, avroRecord);
  }

  @Test(expected = InvalidConfigurationException.class)
  public void testKafkaAvroDeserializerWithoutConfigure() {
    KafkaAvroDeserializer unconfiguredSerializer = new KafkaAvroDeserializer();
    byte[] randomBytes = "foo".getBytes();
    unconfiguredSerializer.deserialize(topic, randomBytes);
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
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(bytes));
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
        1
    );
    avroSerializer.configure(configs, false);
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    byte[] bytes = avroSerializer.serialize(topic, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(bytes));
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerWithPreRegisteredUseSchemaIdIncompatibleError()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_SCHEMA_ID,
        2
    );
    avroSerializer.configure(configs, false);
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    schemaRegistry.register(topic + "-value", new AvroSchema(createAccountSchema()));
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    byte[] bytes = avroSerializer.serialize(topic, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(bytes));
  }

  @Test
  public void testKafkaAvroSerializerWithPreRegisteredUseSchemaIdIncompatibleNoError()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_SCHEMA_ID,
        2,
        KafkaAvroSerializerConfig.ID_COMPATIBILITY_STRICT,
        false
    );
    avroSerializer.configure(configs, false);
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    schemaRegistry.register(topic + "-value", new AvroSchema(createAccountSchema()));
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    byte[] bytes = avroSerializer.serialize(topic, annotatedUserRecord);
    // User gets deserialized as an account!
    IndexedRecord badRecord = createAccountRecord("testUser");
    assertEquals(badRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(badRecord, avroDecoder.fromBytes(bytes));
  }

  @Test
  public void testKafkaAvroSerializerWithPreRegisteredUseLatest()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
        true
    );
    avroSerializer.configure(configs, false);
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    byte[] bytes = avroSerializer.serialize(topic, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(bytes));
  }

  @Test
  public void testKafkaAvroSerializerWithPreRegisteredUseLatestAndNormalize()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
        true,
        KafkaAvroSerializerConfig.NORMALIZE_SCHEMAS,
        true
    );
    avroSerializer.configure(configs, false);
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register(topic + "-value", new AvroSchema(avroRecord.getSchema()));
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    byte[] bytes = avroSerializer.serialize(topic, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(bytes));
  }

  @Test
  public void testKafkaAvroSerializerWithPreRegisteredRemoveJavaProperties()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.AVRO_REMOVE_JAVA_PROPS_CONFIG,
        true
    );
    avroSerializer.configure(configs, false);
    String schema = "{\n"
        + "  \"namespace\": \"io.confluent.kafka.example.annotated\",\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"User\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"name\",\n"
        + "      \"type\": \"string\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    schemaRegistry.register(topic + "-value", new AvroSchema(schema));
    IndexedRecord annotatedUserRecord = createAnnotatedUserRecord();
    byte[] bytes = avroSerializer.serialize(topic, annotatedUserRecord);
    assertEquals(annotatedUserRecord, specificAvroDeserializer.deserialize(topic, bytes));
    assertEquals(annotatedUserRecord, specificAvroDecoder.fromBytes(bytes));
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
    IndexedRecord record1 = createUserRecord();
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
  public void testKafkaAvroSerializerWithMultiTypeUnion() throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
        true
    );
    schemaRegistry.register("user", new AvroSchema(createUserSchema()));
    schemaRegistry.register("account", new AvroSchema(createAccountSchema()));
    schemaRegistry.register(topic + "-value",
        new AvroSchema("[ \"example.avro.User\", \"example.avro.Account\" ]",
            ImmutableList.of(
                new SchemaReference("example.avro.User", "user", 1),
                new SchemaReference("example.avro.Account", "account", 1)
            ),
            ImmutableMap.of(
                "example.avro.User",
                createUserSchema().toString(),
                "example.avro.Account",
                createAccountSchema().toString()
            ),
            null
        ));
    avroSerializer.configure(configs, false);
    IndexedRecord record1 = createUserRecord();
    IndexedRecord record2 = createAccountRecord();
    byte[] bytes1 = avroSerializer.serialize(topic, record1);
    byte[] bytes2 = avroSerializer.serialize(topic, record2);
    assertEquals(record1, avroDeserializer.deserialize(topic, bytes1));
    assertEquals(record2, avroDeserializer.deserialize(topic, bytes2));
  }

  @Test
  public void testKafkaAvroSerializerWithMultiTypeUnionSpecific() throws IOException, RestClientException {
    Map serializerConfigs = ImmutableMap.of(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus",
            KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
            false,
            KafkaAvroSerializerConfig.USE_LATEST_VERSION,
            true
    );
    Map deserializerConfigs = ImmutableMap.of(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus",
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
            true
    );
    IndexedRecord record = createSpecificAvroRecord();
    AvroSchema schema = new AvroSchema(record.getSchema());
    schemaRegistry.register("user", schema);
    schemaRegistry.register("account", new AvroSchema(createAccountSchema()));
    schemaRegistry.register(topic + "-value",
            new AvroSchema("[ \"io.confluent.kafka.example.User\", \"example.avro.Account\" ]",
                    ImmutableList.of(
                            new SchemaReference("io.confluent.kafka.example.User", "user", 1),
                            new SchemaReference("example.avro.Account", "account", 1)
                    ),
                    ImmutableMap.of(
                            "io.confluent.kafka.example.User",
                            schema.toString(),
                            "example.avro.Account",
                            createAccountSchema().toString()
                    ),
                    null
            ));
    avroSerializer.configure(serializerConfigs, false);
    avroDeserializer.configure(deserializerConfigs, false);
    byte[] bytes1 = avroSerializer.serialize(topic, record);
    assertEquals(record, avroDeserializer.deserialize(topic, bytes1));
  }

  /**
   * Verify the capability to use specific record deserializer in a scenario where
   * consumer is bound to multiple topics with different union type subjects
   */
  @Test
  public void testKafkaAvroSerializerWithMultiTypeUnionSpecificAndMultipleTopics() throws IOException, RestClientException {
    Map serializerConfigs = ImmutableMap.of(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus",
            KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
            false,
            KafkaAvroSerializerConfig.USE_LATEST_VERSION,
            true
    );
    Map deserializerConfigs = ImmutableMap.of(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus",
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
            true
    );

    String differentTopic = "another_topic";
    IndexedRecord record = createSpecificAvroRecord();
    AvroSchema schema = new AvroSchema(record.getSchema());

    Grant grantRecord = Grant.newBuilder()
            .setGrant("p234")
            .build();
    AvroSchema grantSchema = new AvroSchema(grantRecord.getSchema());

    schemaRegistry.register("user", schema);
    schemaRegistry.register("account", new AvroSchema(createAccountSchema()));
    // Given TOPIC A subject is union type [A, B]
    schemaRegistry.register(topic + "-value",
            new AvroSchema("[ \"io.confluent.kafka.example.User\", \"example.avro.Account\" ]",
                    ImmutableList.of(
                            new SchemaReference("io.confluent.kafka.example.User", "user", 1),
                            new SchemaReference("example.avro.Account", "account", 1)
                    ),
                    ImmutableMap.of(
                            "io.confluent.kafka.example.User",
                            schema.toString(),
                            "example.avro.Account",
                            createAccountSchema().toString()
                    ),
                    null
            ));
    schemaRegistry.register("grant", grantSchema);
    // Given TOPIC B subject is union type [C]
    schemaRegistry.register(differentTopic + "-value",
            new AvroSchema("[ \"io.confluent.kafka.example.Grant\" ]",
                    ImmutableList.of(
                            new SchemaReference("io.confluent.kafka.example.Grant", "grant", 1)
                    ),
                    ImmutableMap.of(
                            "io.confluent.kafka.example.User",
                            schema.toString(),
                            "example.avro.Account",
                            createAccountSchema().toString(),
                            "io.confluent.kafka.example.Grant",
                            grantSchema.toString()
                    ),
                    null
            ));

    avroSerializer.configure(serializerConfigs, false);
    avroDeserializer.configure(deserializerConfigs, false);

    byte[] bytes1 = avroSerializer.serialize(topic, record);
    byte[] bytesGrant = avroSerializer.serialize(differentTopic, grantRecord);

    // Assert that deserialize is capable to deserializing from topic A using A-subject
    assertEquals(record, avroDeserializer.deserialize(topic, bytes1));
    // Assert that deserialize is capable to deserializing from topic B using B-subject
    assertEquals(grantRecord, avroDeserializer.deserialize(differentTopic, bytesGrant));
  }

  @Test
  public void testKafkaAvroSerializerWithCyclicReference() throws IOException, RestClientException {
    IndexedRecord record = createSpecificAvroRecord();
    AvroSchema schema = new AvroSchema(record.getSchema());
    schemaRegistry.register("user", schema);
    schemaRegistry.register("account",
        new AvroSchema(createAccountSchema().toString(),
            ImmutableList.of(
                new SchemaReference("io.confluent.kafka.example.User", "user", -1)
                ),
            ImmutableMap.of(
                "io.confluent.kafka.example.User",
                schema.toString()
            ),
            null
        ));
    schemaRegistry.register("user",
        new AvroSchema(schema.toString(),
            ImmutableList.of(
                new SchemaReference("example.avro.Account", "account", -1)
            ),
            ImmutableMap.of(
                "example.avro.Account",
                createAccountSchema().toString()
            ),
            null
        ));
    assertNotNull(schemaRegistry.parseSchema(
        AvroSchema.TYPE,
        createAccountSchema().toString(),
        ImmutableList.of(
            new SchemaReference("io.confluent.kafka.example.User", "user", -1)
        )));
  }
  @Test
  public void testKafkaAvroSerializerWithArraySpecific() throws IOException, RestClientException {
    Map serializerConfigs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
        true
    );
    Map deserializerConfigs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
        true
    );
    IndexedRecord record1 = createUserRecordUtf8();
    GenericData.Array<IndexedRecord> data = new GenericData.Array(arraySchema,
        Arrays.asList(record1)
    );
    schemaRegistry.register(topic + "-value", new AvroSchema(arraySchema));
    avroSerializer.configure(serializerConfigs, false);
    avroDeserializer.configure(deserializerConfigs, false);
    byte[] bytes1 = avroSerializer.serialize(topic, data);
    Object result = avroDeserializer.deserialize(topic, bytes1);
    assertEquals(data, result);
  }

  @Test
  public void testKafkaAvroSerializerWithMapSpecific() throws IOException, RestClientException {
    Map serializerConfigs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
        true
    );
    Map deserializerConfigs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
        true
    );
    Map<Utf8, IndexedRecord> data = new HashMap<>();
    data.put(new Utf8("one"), createUserRecordUtf8());
    schemaRegistry.register(topic + "-value", new AvroSchema(mapSchema));
    avroSerializer.configure(serializerConfigs, false);
    avroDeserializer.configure(deserializerConfigs, false);
    byte[] bytes1 = avroSerializer.serialize(topic, data);
    Object result = avroDeserializer.deserialize(topic, bytes1);
    assertEquals(data, result);
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
    try {
      decoderProjection.get("age");
      fail("Getting invalid schema field should fail");
    } catch (AvroRuntimeException e){
      //this is expected
    }

    obj = avroDeserializer.deserialize(topic, bytes, User.getClassSchema());
    assertTrue(
        "Returned object should be a GenericData Record",
        GenericData.Record.class.isInstance(obj)
    );
    GenericData.Record deserializeProjection = (GenericData.Record) obj;
    assertEquals("testUser", deserializeProjection.get("name").toString());
    //Age field was hidden by projection
    try {
      deserializeProjection.get("age");
      fail("Getting invalid schema field should fail");
    } catch (AvroRuntimeException e){
      //this is expected
    }
  }

  @Test
  public void testKafkaAvroSerializerSupportsSchemaEvolution() throws IOException, RestClientException {
    final String fieldToDelete = "fieldToDelete";
    final String newOptionalField = "newOptionalField";

    Schema schemaV1 = SchemaBuilder
            .record("SchemaEvolution")
            .namespace("example.avro")
            .fields()
            .requiredString(fieldToDelete)
            .endRecord();
    Schema schemaV2 = SchemaBuilder
            .record("SchemaEvolution")
            .namespace("example.avro")
            .fields()
            .nullableString(newOptionalField, "optional")
            .endRecord();

    AvroSchema avroSchemaV1 = new AvroSchema(schemaV1);
    AvroSchema avroSchemaV2 = new AvroSchema(schemaV2);
    assertTrue("Schema V2 should be backwards compatible", avroSchemaV2.isBackwardCompatible(avroSchemaV1).isEmpty());

    GenericRecord recordV1 = new GenericData.Record(avroSchemaV1.rawSchema());
    recordV1.put(fieldToDelete, "present");

    byte[] bytes = avroSerializer.serialize(topic, recordV1);
    GenericRecord genericRecordV2 = (GenericRecord) avroDeserializer.deserialize(topic, bytes, avroSchemaV2.rawSchema());

    // In version 2 of the schema, newOptionalField field has a non-null default value
    assertNotNull("Optional field should have a non-null default value", genericRecordV2.get(newOptionalField));

    // In version 2 of the schema, the fieldToDelete field is gone
    try {
      genericRecordV2.get(fieldToDelete);
      fail("Getting invalid schema field should fail");
    } catch (AvroRuntimeException e){
      //this is expected
    }
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
  public void testKafkaAvroSerializerReflectionRecord() {
    byte[] bytes;
    Object obj;

    Widget widget = new Widget("alice");
    Schema schema = ReflectData.get().getSchema(widget.getClass());

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDecoder.fromBytes(bytes, schema);
    assertTrue(
            "Returned object should be a io.confluent.kafka.example.Widget",
            Widget.class.isInstance(obj)
    );
    assertEquals(widget, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
            "Returned object should be a io.confluent.kafka.example.Widget",
            Widget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
  }

  @Test
  public void testKafkaAvroSerializerReflectionRecordWithNullField() {
    byte[] bytes;
    Object obj;

    ExtendedWidget widget = new ExtendedWidget();
    // intentionally miss setting Age field
    widget.setName("alice");
    Schema schema = ReflectData.AllowNull.get().getSchema(widget.getClass());

    try {
      reflectionAvroSerializer.serialize(topic, widget);
      fail("Sending instance with null field should fail reflection serializer");
    } catch (SerializationException e){
      //this is expected
    }

    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus",
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, true,
        KafkaAvroSerializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG, true
    );
    reflectionAvroDeserializer.configure(configs, false);
    reflectionAvroSerializer.configure(configs, false);

    bytes = reflectionAvroSerializer.serialize(topic, widget);
    obj = reflectionAvroDecoder.fromBytes(bytes, schema);
    assertTrue(
        "Returned object should be a io.confluent.kafka.example.ExtendedWidget",
        ExtendedWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a io.confluent.kafka.example.ExtendedWidget",
        ExtendedWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
  }


  @Test
  public void testKafkaAvroSerializerReflectionRecordWithProjection() {
    byte[] bytes;
    Object obj;

    ExtendedWidget widget = new ExtendedWidget("alice", 20);
    Schema extendedWidgetSchema = ReflectData.get().getSchema(ExtendedWidget.class);
    Schema widgetSchema = ReflectData.get().getSchema(Widget.class);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDecoder.fromBytes(bytes, extendedWidgetSchema);
    assertTrue(
            "Full object should be a io.confluent.kafka.example.ExtendedWidget",
            ExtendedWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);

    obj = reflectionAvroDecoder.fromBytes(bytes, widgetSchema);
    assertTrue(
            "Projection object should be a io.confluent.kafka.example.Widget",
            Widget.class.isInstance(obj)
    );
    assertEquals("alice", ((Widget) obj).getName());

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, extendedWidgetSchema);
    assertTrue(
            "Full object should be a io.confluent.kafka.example.ExtendedWidget",
            ExtendedWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, widgetSchema);
    assertTrue(
            "Projection object should be a io.confluent.kafka.example.Widget",
            Widget.class.isInstance(obj)
    );
    assertEquals("alice", ((Widget) obj).getName());
  }

  @Test
  public void testKafkaAvroSerializerNonexistantReflectionRecord() {
    byte[] bytes;

    IndexedRecord avroRecord = createUserRecord();
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
  public void testNull() {
    SchemaRegistryClient nullSchemaRegistryClient = null;
    KafkaAvroSerializer nullAvroSerializer = new KafkaAvroSerializer(nullSchemaRegistryClient);

    // null doesn't require schema registration. So serialization should succeed with a null
    // schema registry client.
    assertEquals(null, nullAvroSerializer.serialize("test", null));
  }

  @Test
  public void testAvroSerializerInvalidInput() {
    IndexedRecord invalidRecord = createInvalidAvroRecord();
    try {
      avroSerializer.serialize(topic, invalidRecord);
      fail("Sending invalid record should fail serializer");
    } catch (SerializationException e) {
      // this is expected
    }
  }

  @Test
  public void test_schemas_per_subject() {
    HashMap<String, String> props = new HashMap<>();
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG, "5");
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

  @Test
  public void testKafkaAvroSerializerReflectionRecordWithPrimitives() {
    byte[] bytes;
    Object obj;

    String message = "testKafkaAvroSerializerReflectionRecordWithPrimitives";
    Schema schema = AvroSchemaUtils.getSchema(message);
    bytes = avroSerializer.serialize(topic, message);

    obj = avroDecoder.fromBytes(bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);

    obj = avroDeserializer.deserialize(topic, bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);

    obj = reflectionAvroDecoder.fromBytes(bytes, schema);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);
  }

  @Test
  public void testKafkaAvroSerializerGenericRecordWithConverters() {
    Map configs = ImmutableMap.of(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus",
            KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
            TopicRecordNameStrategy.class.getName(),
            KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG,
            true
    );

    avroSerializer.configure(configs, false);
    avroDeserializer.configure(configs, false);

    IndexedRecord record1 = createBalanceRecord();
    byte[] bytes1 = avroSerializer.serialize(topic, record1);
    assertEquals(record1, avroDeserializer.deserialize(topic, bytes1));
  }

  @Test
  public void testResolvedFormat() throws IOException, RestClientException {
    schemaRegistry.register("user", new AvroSchema(createUserSchema()));
    schemaRegistry.register("account", new AvroSchema(createAccountSchema()));
    int id = schemaRegistry.register(topic + "-value",
        new AvroSchema("[ \"example.avro.User\", \"example.avro.Account\" ]",
            ImmutableList.of(
                new SchemaReference("example.avro.User", "user", 1),
                new SchemaReference("example.avro.Account", "account", 1)
            ),
            ImmutableMap.of(
                "example.avro.User",
                createUserSchema().toString(),
                "example.avro.Account",
                createAccountSchema().toString()
            ),
            null
        ));
    AvroSchema schema = (AvroSchema) schemaRegistry.getSchemaById(id);
    String expectedCanonical = "[\"example.avro.User\",\"example.avro.Account\"]";
    assertEquals(expectedCanonical, schema.canonicalString());
    String expectedResolved = "[{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\","
        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}"
        + ",{\"type\":\"record\",\"name\":\"Account\",\"namespace\":\"example.avro\","
        + "\"fields\":[{\"name\":\"accountNumber\",\"type\":\"string\"}]}]";
    assertEquals(expectedResolved, schema.formattedString(Format.RESOLVED.symbol()));
  }
}
