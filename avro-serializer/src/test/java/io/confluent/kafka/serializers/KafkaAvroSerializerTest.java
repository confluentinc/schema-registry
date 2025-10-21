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

import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaAvroSerializerTest {

  private final Properties defaultConfig;
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
    defaultConfig = createSerializerConfig();
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    Properties deserializerConfig = createDeserializerConfig();
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, new HashMap(deserializerConfig));
    avroDecoder = new KafkaAvroDecoder(schemaRegistry, new VerifiableProperties(deserializerConfig));
    topic = "test";

    HashMap<String, String> specificDeserializerProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    specificAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, specificDeserializerProps);

    Properties specificDecoderProps = createDeserializerConfig();
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

    Properties reflectionDecoderProps = createDeserializerConfig();
    reflectionDecoderProps.setProperty(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    reflectionDecoderProps.setProperty(
            KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionDecoderProps.setProperty(
            KafkaAvroDeserializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG, "true");
    reflectionDecoderProps.setProperty(
            KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, "true");
    reflectionAvroDecoder = new KafkaAvroDecoder(
            schemaRegistry, new VerifiableProperties(reflectionDecoderProps));
  }

  protected Properties createSerializerConfig() {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    return serializerConfig;
  }

  protected Properties createDeserializerConfig() {
    Properties deserializerConfig = new Properties();
    deserializerConfig.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
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
        .setUpdatedAt(Instant.ofEpochMilli(1613646696368L))
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
          + "\"namespace\": \"io.confluent.kafka.example\",\n"
          + "\"name\": \"User\",\n"
          + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}}");

  private static final Schema mapSchema = new Schema.Parser().parse(
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"map\",\n"
          + " \"name\": \"test\",\n"
          + " \"values\": {\n"
          + "\"type\": \"record\",\n"
          + "\"namespace\": \"io.confluent.kafka.example\",\n"
          + "\"name\": \"User\",\n"
          + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}}");

  @Test
  public void testKafkaAvroSerializer() {
    byte[] bytes;
    IndexedRecord avroRecord = createUserRecord();
    RecordHeaders headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));

    IndexedRecord avroRecordWithAllField = createExtendUserRecord();
    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, avroRecordWithAllField);
    assertEquals(avroRecordWithAllField, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecordWithAllField, avroDecoder.fromBytes(headers, bytes));

    IndexedRecord avroRecordWithoutOptional = createExtendUserRecordWithNullField();
    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, avroRecordWithoutOptional);
    assertEquals(avroRecordWithoutOptional, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecordWithoutOptional, avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, null);
    assertEquals(null, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(null, avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, true);
    assertEquals(true, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(true, avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, 123);
    assertEquals(123, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(123, avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, 345L);
    assertEquals(345l, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(345l, avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, 1.23f);
    assertEquals(1.23f, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(1.23f, avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, 2.34d);
    assertEquals(2.34, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(2.34, avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, "abc");
    assertEquals("abc", avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals("abc", avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, "abc".getBytes());
    assertArrayEquals("abc".getBytes(), (byte[]) avroDeserializer.deserialize(topic, headers, bytes));
    assertArrayEquals("abc".getBytes(), (byte[]) avroDecoder.fromBytes(headers, bytes));

    headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, new Utf8("abc"));
    assertEquals("abc", avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals("abc", avroDecoder.fromBytes(headers, bytes));
  }

  @Test
  public void testKafkaAvroSerializerPrimitiveArrays() {
    final Map<String, List<?>> arrays = ImmutableMap.of(
        "{\"type\": \"array\", \"items\": \"boolean\"}", ImmutableList.of(true, false),
        "{\"type\": \"array\", \"items\": \"int\"}", ImmutableList.of(1, 2),
        "{\"type\": \"array\", \"items\": \"long\"}", ImmutableList.of(1L, 2L),
        "{\"type\": \"array\", \"items\": \"double\"}", ImmutableList.of(1.1, 2.2),
        "{\"type\": \"array\", \"items\": \"string\"}", ImmutableList.of("string", "elements!")
    );

    int index = 0;
    for (Map.Entry<String, List<?>> entry : arrays.entrySet()) {
      String schema = entry.getKey();
      List<?> input = entry.getValue();
      final List<String> expected = input.stream()
          .map(Object::toString)
          .collect(Collectors.toList());
      AvroSchema avroSchema = new AvroSchema(schema);
      GenericData.Array<?> array = new GenericData.Array<>(avroSchema.rawSchema(), input);
      RecordHeaders headers = new RecordHeaders();
      byte[] bytes = avroSerializer.serialize(topic + "_" + index, headers, array);
      Object object = avroDeserializer.deserialize(topic + "_" + index, headers, bytes);
      List<String> result = ((List<?>) object).stream()
          .map(Object::toString)
          .collect(Collectors.toList());
      assertEquals(expected, result);
    }
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
    RecordHeaders headers = new RecordHeaders();
    avroSerializer.serialize(topic, headers, avroRecord);
  }

  @Test(expected = InvalidConfigurationException.class)
  public void testKafkaAvroSerializerWithoutConfigure() {
    KafkaAvroSerializer unconfiguredSerializer = new KafkaAvroSerializer();
    IndexedRecord avroRecord = createUserRecord();
    RecordHeaders headers = new RecordHeaders();
    unconfiguredSerializer.serialize(topic, headers, avroRecord);
  }

  @Test(expected = InvalidConfigurationException.class)
  public void testKafkaAvroDeserializerWithoutConfigure() {
    KafkaAvroDeserializer unconfiguredSerializer = new KafkaAvroDeserializer();
    byte[] randomBytes = "foo".getBytes();
    RecordHeaders headers = new RecordHeaders();
    unconfiguredSerializer.deserialize(topic, headers, randomBytes);
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    // User gets deserialized as an account!
    IndexedRecord badRecord = createAccountRecord("testUser");
    assertEquals(badRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(badRecord, avroDecoder.fromBytes(headers, bytes));
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, annotatedUserRecord);
    assertEquals(annotatedUserRecord, specificAvroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(annotatedUserRecord, specificAvroDecoder.fromBytes(headers, bytes));
  }

  @Test
  public void testKafkaAvroDeserializerWithPreRegisteredUseLatestRecordNameStrategy()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
        true,
        KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        RecordNameStrategy.class.getName()
    );
    avroSerializer.configure(configs, false);
    avroDeserializer.configure(configs, false);
    IndexedRecord avroRecord = createUserRecord();
    schemaRegistry.register("example.avro.User", new AvroSchema(avroRecord.getSchema()));
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, headers, bytes));
    assertEquals(avroRecord, avroDecoder.fromBytes(headers, bytes));

    // restore configs
    avroDeserializer.configure(new HashMap(defaultConfig), false);
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes1 = avroSerializer.serialize(topic, headers, record1);
    assertEquals(record1, avroDeserializer.deserialize(topic, headers, bytes1));
    headers = new RecordHeaders();
    byte[] bytes2 = avroSerializer.serialize(topic, headers, record2);
    assertEquals(record2, avroDeserializer.deserialize(topic, headers, bytes2));
    assertNotNull(schemaRegistry.getLatestSchemaMetadata(topic + "-example.avro.User"));
    assertNotNull(schemaRegistry.getLatestSchemaMetadata(topic + "-example.avro.Account"));
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
    RecordHeaders headers = new RecordHeaders();
    avroSerializer.serialize(topic, headers, "a string should not be allowed");
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes1 = avroSerializer.serialize(topic, headers, record1);
    assertEquals(record1, avroDeserializer.deserialize(topic, headers, bytes1));
    headers = new RecordHeaders();
    byte[] bytes2 = avroSerializer.serialize(topic, headers, record2);
    assertEquals(record2, avroDeserializer.deserialize(topic, headers, bytes2));
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes1 = avroSerializer.serialize(topic, headers, record);
    assertEquals(record, avroDeserializer.deserialize(topic, headers, bytes1));
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

    RecordHeaders headers = new RecordHeaders();
    byte[] bytes1 = avroSerializer.serialize(topic, headers, record);
    // Assert that deserialize is capable to deserializing from topic A using A-subject
    assertEquals(record, avroDeserializer.deserialize(topic, headers, bytes1));

    headers = new RecordHeaders();
    byte[] bytesGrant = avroSerializer.serialize(differentTopic, headers, grantRecord);
    // Assert that deserialize is capable to deserializing from topic B using B-subject
    assertEquals(grantRecord, avroDeserializer.deserialize(differentTopic, headers, bytesGrant));
  }

  @Test
  public void testKafkaAvroSerializerWithMultiTypeUnionSpecificLogical() throws IOException, RestClientException {
    Map serializerConfigs = ImmutableMap.of(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus",
            KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
            false,
            KafkaAvroSerializerConfig.USE_LATEST_VERSION,
            true,
            KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG,
            true
    );
    Map deserializerConfigs = ImmutableMap.of(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus",
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
            true,
            KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG,
            true
    );

    String differentTopic = "another_topic";
    IndexedRecord record = createExtendedSpecificAvroRecord();
    AvroSchema schema = new AvroSchema(record.getSchema());

    Grant grantRecord = Grant.newBuilder()
            .setGrant("p234")
            .build();
    AvroSchema grantSchema = new AvroSchema(grantRecord.getSchema());

    schemaRegistry.register("user", schema);
    schemaRegistry.register("account", new AvroSchema(createAccountSchema()));
    // Given TOPIC A subject is union type [A, B]
    schemaRegistry.register(topic + "-value",
            new AvroSchema("[ \"io.confluent.kafka.example.ExtendedUser\", \"example.avro.Account\" ]",
                    ImmutableList.of(
                            new SchemaReference("io.confluent.kafka.example.ExtendedUser", "user", 1),
                            new SchemaReference("example.avro.Account", "account", 1)
                    ),
                    ImmutableMap.of(
                            "io.confluent.kafka.example.ExtendedUser",
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
                            "io.confluent.kafka.example.ExtendedUser",
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

    RecordHeaders headers = new RecordHeaders();
    byte[] bytes1 = avroSerializer.serialize(topic, headers, record);
    // Assert that deserialize is capable to deserializing from topic A using A-subject
    assertEquals(record, avroDeserializer.deserialize(topic, headers, bytes1));

    headers = new RecordHeaders();
    byte[] bytesGrant = avroSerializer.serialize(differentTopic, headers, grantRecord);
    // Assert that deserialize is capable to deserializing from topic B using B-subject
    assertEquals(grantRecord, avroDeserializer.deserialize(differentTopic, headers, bytesGrant));
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes1 = avroSerializer.serialize(topic, headers, data);
    Object result = avroDeserializer.deserialize(topic, headers, bytes1);
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
    data.put(new Utf8("one"), createSpecificAvroRecord());
    schemaRegistry.register(topic + "-value", new AvroSchema(mapSchema));
    avroSerializer.configure(serializerConfigs, false);
    avroDeserializer.configure(deserializerConfigs, false);
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes1 = avroSerializer.serialize(topic, headers, data);
    Object result = avroDeserializer.deserialize(topic, headers, bytes1);
    assertEquals(data, result);
  }

  @Test
  public void testKafkaAvroSerializerWithProjection() {
    byte[] bytes;
    Object obj;
    IndexedRecord avroRecord = createExtendedSpecificAvroRecord();
    RecordHeaders headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, avroRecord);

    obj = avroDecoder.fromBytes(headers, bytes);
    GenericData.Record extendedUser = (GenericData.Record) obj;
    assertTrue(
        "Returned object should be a GenericData Record",
        GenericData.Record.class.isInstance(obj)
    );
    //Age field is visible
    assertNotNull(extendedUser.get("age"));

    obj = avroDecoder.fromBytes(headers, bytes, User.getClassSchema());
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

    obj = avroDeserializer.deserialize(topic, headers, bytes, User.getClassSchema());
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

    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, recordV1);
    GenericRecord genericRecordV2 = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes, avroSchemaV2.rawSchema());

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
    RecordHeaders headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, avroRecord);

    obj = avroDecoder.fromBytes(headers, bytes);
    assertTrue(
        "Returned object should be a GenericData Record",
        GenericData.Record.class.isInstance(obj)
    );

    obj = specificAvroDecoder.fromBytes(headers, bytes);
    assertTrue(
        "Returned object should be a io.confluent.kafka.example.User",
        User.class.isInstance(obj)
    );
    assertEquals(avroRecord, obj);

    obj = specificAvroDeserializer.deserialize(topic, headers, bytes);
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
    RecordHeaders headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, avroRecord);

    obj = specificAvroDecoder.fromBytes(headers, bytes);
    assertTrue(
        "Full object should be a io.confluent.kafka.example.ExtendedUser",
        ExtendedUser.class.isInstance(obj)
    );
    assertEquals(avroRecord, obj);

    obj = specificAvroDecoder.fromBytes(headers, bytes, User.getClassSchema());
    assertTrue(
        "Projection object should be a io.confluent.kafka.example.User",
        User.class.isInstance(obj)
    );
    assertEquals("testUser", ((User) obj).getName().toString());

    obj = specificAvroDeserializer.deserialize(topic, headers, bytes);
    assertTrue(
        "Full object should be a io.confluent.kafka.example.ExtendedUser",
        ExtendedUser.class.isInstance(obj)
    );
    assertEquals(avroRecord, obj);

    obj = specificAvroDeserializer.deserialize(topic, headers, bytes, User.getClassSchema());
    assertTrue(
        "Projection object should be a io.confluent.kafka.example.User",
        User.class.isInstance(obj)
    );
    assertEquals("testUser", ((User) obj).getName().toString());
  }

  @Test
  public void testKafkaAvroSerializerSpecificRecordWithValueTypeConfig() {
    HashMap<String, String> specificDeserializerProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG,
        User.class.getName()
    );

    final KafkaAvroDeserializer specificAvroDeserializerWithReaderSchema = new KafkaAvroDeserializer(
      schemaRegistry, specificDeserializerProps
    );

    IndexedRecord avroRecord = createExtendedSpecificAvroRecord();
    RecordHeaders headers = new RecordHeaders();
    final byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);

    final Object obj = specificAvroDeserializerWithReaderSchema.deserialize(topic, headers, bytes);
    assertTrue(
        "Full object should be a io.confluent.kafka.example.User",
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

    RecordHeaders headers = new RecordHeaders();
    bytes = reflectionAvroSerializer.serialize(topic, headers, widget);

    obj = reflectionAvroDecoder.fromBytes(headers, bytes, schema);
    assertTrue(
            "Returned object should be a io.confluent.kafka.example.Widget",
            Widget.class.isInstance(obj)
    );
    assertEquals(widget, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes, schema);
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
      RecordHeaders headers = new RecordHeaders();
      reflectionAvroSerializer.serialize(topic, headers, widget);
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

    RecordHeaders headers = new RecordHeaders();
    bytes = reflectionAvroSerializer.serialize(topic, headers, widget);
    obj = reflectionAvroDecoder.fromBytes(headers, bytes, schema);
    assertTrue(
        "Returned object should be a io.confluent.kafka.example.ExtendedWidget",
        ExtendedWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes);
    assertTrue(
        "Returned object should be a io.confluent.kafka.example.ExtendedWidget",
        ExtendedWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
  }

  @Test
  public void testKafkaAvroSerializerReflectionRecordWithLogicalType() {
    byte[] bytes;
    Object obj;

    RecordWithUUID record = new RecordWithUUID();
    record.uuid = UUID.randomUUID();

    Schema schema = AvroSchemaUtils.getReflectData().getSchema(record.getClass());

    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus",
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, true,
        KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true
    );
    reflectionAvroDeserializer.configure(configs, false);
    reflectionAvroSerializer.configure(configs, false);

    RecordHeaders headers = new RecordHeaders();
    bytes = reflectionAvroSerializer.serialize(topic, headers, record);
    obj = reflectionAvroDecoder.fromBytes(headers, bytes, schema);
    assertTrue(
        "Returned object should be a RecordWithUUID",
        RecordWithUUID.class.isInstance(obj)
    );
    assertEquals(record, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes);
    assertTrue(
        "Returned object should be a RecordWithUUID",
        RecordWithUUID.class.isInstance(obj)
    );
    assertEquals(record, obj);
  }

  @Test
  public void testKafkaAvroSerializerReflectionRecordWithLogicalTypeNullField() {
    byte[] bytes;
    Object obj;

    RecordWithUUID record = new RecordWithUUID();

    Schema schema = AvroSchemaUtils.getReflectDataAllowNull().getSchema(record.getClass());

    Map configs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus",
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, true,
        KafkaAvroSerializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG, true,
        KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true
    );
    reflectionAvroDeserializer.configure(configs, false);
    reflectionAvroSerializer.configure(configs, false);

    RecordHeaders headers = new RecordHeaders();
    bytes = reflectionAvroSerializer.serialize(topic, headers, record);
    obj = reflectionAvroDecoder.fromBytes(headers, bytes, schema);
    assertTrue(
        "Returned object should be a RecordWithUUID",
        RecordWithUUID.class.isInstance(obj)
    );
    assertEquals(record, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes);
    assertTrue(
        "Returned object should be a RecordWithUUID",
        RecordWithUUID.class.isInstance(obj)
    );
    assertEquals(record, obj);
  }

  @Test
  public void testKafkaAvroSerializerReflectionRecordWithProjection() {
    byte[] bytes;
    Object obj;

    ExtendedWidget widget = new ExtendedWidget("alice", 20);
    Schema extendedWidgetSchema = ReflectData.get().getSchema(ExtendedWidget.class);
    Schema widgetSchema = ReflectData.get().getSchema(Widget.class);

    RecordHeaders headers = new RecordHeaders();
    bytes = reflectionAvroSerializer.serialize(topic, headers, widget);

    obj = reflectionAvroDecoder.fromBytes(headers, bytes, extendedWidgetSchema);
    assertTrue(
            "Full object should be a io.confluent.kafka.example.ExtendedWidget",
            ExtendedWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);

    obj = reflectionAvroDecoder.fromBytes(headers, bytes, widgetSchema);
    assertTrue(
            "Projection object should be a io.confluent.kafka.example.Widget",
            Widget.class.isInstance(obj)
    );
    assertEquals("alice", ((Widget) obj).getName());

    obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes, extendedWidgetSchema);
    assertTrue(
            "Full object should be a io.confluent.kafka.example.ExtendedWidget",
            ExtendedWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes, widgetSchema);
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
    RecordHeaders headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, avroRecord);

    try {
      specificAvroDecoder.fromBytes(headers, bytes);
      fail("Did not throw an exception when class for specific avro record does not exist.");
    } catch (SerializationException e) {
      // this is expected
    } catch (Exception e) {
      fail("Threw the incorrect exception when class for specific avro record does not exist.");
    }

    try {
      specificAvroDeserializer.deserialize(topic, headers, bytes);
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
    RecordHeaders headers = new RecordHeaders();
    assertEquals(null, nullAvroSerializer.serialize("test", headers, null));
  }

  @Test
  public void testAvroSerializerInvalidInput() {
    IndexedRecord invalidRecord = createInvalidAvroRecord();
    try {
      RecordHeaders headers = new RecordHeaders();
      avroSerializer.serialize(topic, headers, invalidRecord);
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
    RecordHeaders headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, message);

    obj = avroDecoder.fromBytes(headers, bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));

    obj = specificAvroDecoder.fromBytes(headers, bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);

    obj = specificAvroDeserializer.deserialize(topic, headers, bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);
  }

  @Test
  public void testKafkaAvroSerializerReflectionRecordWithPrimitives() {
    byte[] bytes;
    Object obj;

    String message = "testKafkaAvroSerializerReflectionRecordWithPrimitives";
    Schema schema = AvroSchemaUtils.getSchema(message);
    RecordHeaders headers = new RecordHeaders();
    bytes = avroSerializer.serialize(topic, headers, message);

    obj = avroDecoder.fromBytes(headers, bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);

    obj = avroDeserializer.deserialize(topic, headers, bytes);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);

    obj = reflectionAvroDecoder.fromBytes(headers, bytes, schema);
    assertTrue("Returned object should be a String", String.class.isInstance(obj));
    assertEquals(message, obj);

    obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes, schema);
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
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes1 = avroSerializer.serialize(topic, headers, record1);
    assertEquals(record1, avroDeserializer.deserialize(topic, headers, bytes1));
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

  static class RecordWithUUID {
    UUID uuid;

    @Override
    public int hashCode() {
      return uuid.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof RecordWithUUID)) {
        return false;
      }
      RecordWithUUID that = (RecordWithUUID) obj;
      return Objects.equals(this.uuid, that.uuid);
    }
  }
}
