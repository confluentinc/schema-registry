/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption;

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.CLOCK;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.TEST_CLIENT;
import static io.confluent.kafka.schemaregistry.rules.RuleBase.DEFAULT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClientFactory;
import io.confluent.dekregistry.client.MockDekRegistryClientFactory;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.RuleBase;
import io.confluent.kafka.schemaregistry.rules.WidgetBytesProto.PiiBytes;
import io.confluent.kafka.schemaregistry.rules.WidgetBytesProto.WidgetBytes;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Pii;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Widget;
import io.confluent.kafka.schemaregistry.testutil.FakeClock;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.After;
import org.junit.Test;

public abstract class FieldEncryptionExecutorTest {

  static {
    try {
      AeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  protected final FieldEncryptionProperties fieldEncryptionProps;
  protected final SchemaRegistryClient schemaRegistry;
  protected final DekRegistryClient dekRegistry;
  protected final KafkaAvroSerializer avroSerializer;
  protected final KafkaAvroDeserializer avroDeserializer;
  protected final KafkaAvroSerializer avroKeySerializer;
  protected final KafkaAvroDeserializer avroKeyDeserializer;
  protected final KafkaAvroSerializer avroValueSerializer;
  protected final KafkaAvroDeserializer avroValueDeserializer;
  protected final KafkaAvroSerializer avroSerializerWithoutKey;
  protected final KafkaAvroDeserializer avroDeserializerWithoutKey;
  protected final KafkaAvroSerializer reflectionAvroSerializer;
  protected final KafkaAvroDeserializer reflectionAvroDeserializer;
  protected final KafkaJsonSchemaSerializer<OldWidget> jsonSchemaSerializer;
  protected final KafkaJsonSchemaSerializer<AnnotatedOldWidget> jsonSchemaSerializer2;
  protected final KafkaJsonSchemaSerializer<Employee> jsonSchemaSerializer3;
  protected final KafkaJsonSchemaSerializer<SampleEvent> jsonSchemaSerializer4;
  protected final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;
  protected final KafkaProtobufSerializer<Widget> protobufSerializer;
  protected final KafkaProtobufSerializer<WidgetBytes> protobufSerializerBytes;
  protected final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  protected final KafkaAvroSerializer badSerializer;
  protected final KafkaAvroDeserializer badDeserializer;
  protected final KafkaAvroSerializer goodDekSerializer;
  protected final KafkaAvroSerializer badDekSerializer;
  protected final String topic;
  protected final FakeClock fakeClock = new FakeClock();

  public FieldEncryptionExecutorTest() throws Exception {
    topic = "test";
    List<String> ruleNames = ImmutableList.of("rule1", "rule2");
    fieldEncryptionProps = getFieldEncryptionProperties(ruleNames, FieldEncryptionExecutor.class);
    Map<String, Object> clientProps = fieldEncryptionProps.getClientProperties("mock://");
    clientProps.put(CLOCK, fakeClock);
    schemaRegistry = SchemaRegistryClientFactory.newClient(Collections.singletonList(
        "mock://"),
        1000,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        null,
        null
    );
    dekRegistry = DekRegistryClientFactory.newClient(Collections.singletonList(
        "mock://"),
        1000,
        100000,
        Collections.singletonMap(TEST_CLIENT, fieldEncryptionProps.getTestClient()),
        null
    );

    avroSerializer = new KafkaAvroSerializer(schemaRegistry, clientProps);
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, clientProps);

    List<String> qualifiedRuleNames = ImmutableList.of("test-key:rule1", "test-value:rule1");
    FieldEncryptionProperties qualifiedFieldEncryptionProps =
        getFieldEncryptionProperties(qualifiedRuleNames, FieldEncryptionExecutor.class);
    Map<String, Object> qualifiedClientProps = qualifiedFieldEncryptionProps.getClientProperties("mock://");
    avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(qualifiedClientProps, true);
    avroKeyDeserializer = new KafkaAvroDeserializer();
    avroKeyDeserializer.configure(qualifiedClientProps, true);
    avroValueSerializer = new KafkaAvroSerializer();
    avroValueSerializer.configure(qualifiedClientProps, false);
    avroValueDeserializer = new KafkaAvroDeserializer();
    avroValueDeserializer.configure(qualifiedClientProps, false);

    Map<String, Object> clientPropsWithoutKey = fieldEncryptionProps.getClientProperties("mock://");
    avroSerializerWithoutKey = new KafkaAvroSerializer(schemaRegistry, clientPropsWithoutKey);
    avroDeserializerWithoutKey = new KafkaAvroDeserializer(schemaRegistry, clientPropsWithoutKey);

    Map<String, Object> reflectionClientProps = new HashMap<>(clientProps);
    reflectionClientProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionAvroSerializer = new KafkaAvroSerializer(schemaRegistry, reflectionClientProps);
    reflectionAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, reflectionClientProps);

    clientProps.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, false);
    jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, clientProps);
    jsonSchemaSerializer2 = new KafkaJsonSchemaSerializer<>(schemaRegistry, clientProps);
    jsonSchemaSerializer3 = new KafkaJsonSchemaSerializer<>(schemaRegistry, clientProps);
    jsonSchemaSerializer4 = new KafkaJsonSchemaSerializer<>(schemaRegistry, clientProps);
    jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry, clientProps);

    protobufSerializer = new KafkaProtobufSerializer<>(schemaRegistry, clientProps);
    protobufSerializerBytes = new KafkaProtobufSerializer<>(schemaRegistry, clientProps);
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, clientProps);

    Map<String, Object> badClientProps = new HashMap<>(clientProps);
    badClientProps.remove(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS);
    badClientProps.put(AbstractKafkaSchemaSerDeConfig.RULE_SERVICE_LOADER_ENABLE, false);
    badSerializer = new KafkaAvroSerializer(schemaRegistry, badClientProps);
    badDeserializer = new KafkaAvroDeserializer(schemaRegistry, badClientProps);

    FieldEncryptionProperties goodDekProps = getFieldEncryptionProperties(ruleNames, GoodDekGenerator.class);
    Map<String, Object> goodDekClientProps = goodDekProps.getClientProperties("mock://");
    goodDekSerializer = new KafkaAvroSerializer(schemaRegistry, goodDekClientProps);

    FieldEncryptionProperties badDekProps = getFieldEncryptionProperties(ruleNames, BadDekGenerator.class);
    Map<String, Object> badDekClientProps = badDekProps.getClientProperties("mock://");
    badDekSerializer = new KafkaAvroSerializer(schemaRegistry, badDekClientProps);
  }

  protected abstract FieldEncryptionProperties getFieldEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor);

  protected Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde) throws Exception {
    return addSpyToCryptor(serde, DekFormat.AES256_GCM);
  }

  protected Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde, DekFormat dekFormat) throws Exception {
    Map<String, Map<String, RuleBase>> executors = serde.getRuleExecutors();
    Map<String, RuleBase> executorsByType = executors.get(FieldEncryptionExecutor.TYPE);
    FieldEncryptionExecutor executor = null;
    if (executorsByType != null && !executorsByType.isEmpty()) {
      executor = (FieldEncryptionExecutor) executorsByType.entrySet().iterator().next().getValue();
    }
    if (executor != null) {
      Map<DekFormat, Cryptor> cryptors = executor.getCryptors();
      Cryptor spy = spy(new Cryptor(dekFormat));
      cryptors.put(dekFormat, spy);
      return spy;
    }
    return null;
  }

  protected Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde, String name) throws Exception {
    return addSpyToCryptor(serde, name, DekFormat.AES256_GCM);
  }

  protected Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde, String name, DekFormat dekFormat)
      throws Exception {
    Map<String, Map<String, RuleBase>> executors = serde.getRuleExecutors();
    Map<String, RuleBase> executorsByType = executors.get(FieldEncryptionExecutor.TYPE);
    FieldEncryptionExecutor executor = null;
    if (executorsByType != null && !executorsByType.isEmpty()) {
      executor = (FieldEncryptionExecutor) executors.get(FieldEncryptionExecutor.TYPE).get(name);
    }
    if (executor == null) {
      FieldEncryptionExecutor encryptor = (FieldEncryptionExecutor)
          executors.get(FieldEncryptionExecutor.TYPE).get("_ENCRYPT_");
      executor = encryptor;
    }
    if (executor == null) {
      FieldEncryptionExecutor encryptor = (FieldEncryptionExecutor)
          executors.get(FieldEncryptionExecutor.TYPE).get(DEFAULT_NAME);
      executor = encryptor;
    }
    if (executor != null) {
      // Check for existing cryptor
      Map<DekFormat, Cryptor> cryptors = executor.getCryptors();
      Cryptor cryptor = cryptors.get(dekFormat);
      if (cryptor != null) {
        return cryptor;
      }
      Cryptor spy = spy(new Cryptor(dekFormat));
      cryptors.put(dekFormat, spy);
      return spy;
    }
    return null;
  }

  protected Cryptor addBadSpyToCryptor(AbstractKafkaSchemaSerDe serde) throws Exception {
    return addBadSpyToCryptor(serde, DekFormat.AES256_GCM);
  }

  protected Cryptor addBadSpyToCryptor(AbstractKafkaSchemaSerDe serde, DekFormat dekFormat) throws Exception {
    Map<String, Map<String, RuleBase>> executors = serde.getRuleExecutors();
    FieldEncryptionExecutor executor =
        (FieldEncryptionExecutor) executors.get(FieldEncryptionExecutor.TYPE).entrySet()
            .iterator().next().getValue();
    if (executor != null) {
      Map<DekFormat, Cryptor> cryptors = executor.getCryptors();
      Cryptor spy = spy(new Cryptor(dekFormat));
      doThrow(new GeneralSecurityException()).when(spy).encrypt(any(), any(), any());
      doThrow(new GeneralSecurityException()).when(spy).decrypt(any(), any(), any());
      cryptors.put(dekFormat, spy);
      return spy;
    }
    return null;
  }

  protected Schema createF1Schema() {
    String userSchema = "{\"type\": \"record\", "
        + "\"name\": \"myrecord\","
        + "\"fields\": ["
        + "{\"name\": \"f1\", \"type\": \"string\", \"confluent:tags\": [\"PII\"]}"
        + "]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  protected GenericRecord createF1Record() {
    Schema schema = createF1Schema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("f1", "hello world");
    return avroRecord;
  }

  protected Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": ["
        + "{\"name\": \"name\", \"type\": [\"null\", \"string\"], \"confluent:tags\": [\"PII\", \"PII3\"]},"
        + "{\"name\": \"name2\", \"type\": [\"null\", \"string\"], \"confluent:tags\": [\"PII2\"]},"
        + "{\"name\": \"age\", \"type\": [\"null\", \"int\"]}"
        + "]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  protected GenericRecord createUserRecord() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("name2", "testUser2");
    avroRecord.put("age", 18);
    return avroRecord;
  }

  protected GenericRecord createUserRecordWithNull() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", null);
    avroRecord.put("name2", "testUser2");
    avroRecord.put("age", 18);
    return avroRecord;
  }

  protected Schema createUserSchemaWithTaggedInt() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": ["
        + "{\"name\": \"name\", \"type\": [\"null\", \"string\"], \"confluent:tags\": [\"PII\", \"PII3\"]},"
        + "{\"name\": \"name2\", \"type\": [\"null\", \"string\"], \"confluent:tags\": [\"PII2\"]},"
        + "{\"name\": \"age\", \"type\": [\"null\", \"int\"], \"confluent:tags\": [\"PII\"]}"
        + "]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  protected IndexedRecord createUserRecordWithTaggedInt() {
    Schema schema = createUserSchemaWithTaggedInt();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("name2", "testUser2");
    avroRecord.put("age", 18);
    return avroRecord;
  }

  protected Schema createUserBytesSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": ["
        + "{\"name\": \"name\", \"type\": [\"null\", \"bytes\"], \"confluent:tags\": [\"PII\", \"PII3\"]},"
        + "{\"name\": \"name2\", \"type\": [\"null\", \"bytes\"], \"confluent:tags\": [\"PII2\"]}"
        + "]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  protected IndexedRecord createUserBytesRecord() {
    Schema schema = createUserBytesSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", ByteBuffer.wrap("testUser".getBytes(StandardCharsets.UTF_8)));
    avroRecord.put("name2", ByteBuffer.wrap("testUser2".getBytes(StandardCharsets.UTF_8)));
    return avroRecord;
  }

  protected Schema createWidgetSchema() {
    String userSchema = "{\"type\":\"record\",\"name\":\"OldWidget\",\"namespace\":\"io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest\",\"fields\":\n"
        + "[{\"name\": \"name\", \"type\": \"string\",\"confluent:tags\": [\"PII\"]},\n"
        + "{\"name\": \"ssn\", \"type\": { \"type\": \"array\", \"items\": \"string\"},\"confluent:tags\": [\"PII\"]},\n"
        + "{\"name\": \"piiArray\", \"type\": { \"type\": \"array\", \"items\": { \"type\": \"record\", \"name\":\"OldPii\", \"fields\":\n"
        + "[{\"name\": \"pii\", \"type\": \"string\",\"confluent:tags\": [\"PII\"]}]}}},\n"
        + "{\"name\": \"piiMap\", \"type\": { \"type\": \"map\", \"values\": \"OldPii\"},\n"
        + "\"confluent:tags\": [\"PII\"]},\n"
        + "{\"name\": \"size\", \"type\": \"int\"},{\"name\": \"version\", \"type\": \"int\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  @After
  public void tearDown() {
    MockSchemaRegistry.clear();
    MockDekRegistryClientFactory.clear();
  }

  @Test
  public void testKafkaAvroSerializer() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));
  }

  @Test
  public void testKafkaAvroSerializerF1() throws Exception {
    IndexedRecord avroRecord = createF1Record();
    AvroSchema avroSchema = new AvroSchema(createF1Schema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("hello world", record.get("f1"));

  }

  @Test
  public void testKafkaAvr1SerializerDeterministicF1() throws Exception {
    IndexedRecord avroRecord = createF1Record();
    AvroSchema avroSchema = new AvroSchema(createF1Schema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1", DekFormat.AES256_SIV);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer, DekFormat.AES256_SIV);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer, DekFormat.AES256_SIV);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("hello world", record.get("f1"));
  }

  @Test
  public void testKafkaAvroSerializerPreserveSource() throws Exception {
    GenericRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("preserve.source.fields", "true"), null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));
    // Old value is preserved
    assertEquals("testUser", avroRecord.get("name"));
  }

  @Test
  public void testKafkaAvroDekRotation() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("encrypt.dek.expiry.days", "1", "preserve.source.fields", "true"),
        null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));

    Dek dek = dekRegistry.getDekLatestVersion("kek1", topic + "-value", null, false);
    assertEquals(1, dek.getVersion());

    // Advance 2 days
    fakeClock.advance(2, ChronoUnit.DAYS);

    cryptor = addSpyToCryptor(avroSerializer);
    bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));

    dek = dekRegistry.getDekLatestVersion("kek1", topic + "-value", null, false);
    assertEquals(2, dek.getVersion());

    // Advance 2 days
    fakeClock.advance(2, ChronoUnit.DAYS);

    cryptor = addSpyToCryptor(avroSerializer);
    bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));

    dek = dekRegistry.getDekLatestVersion("kek1", topic + "-value", null, false);
    assertEquals(3, dek.getVersion());
  }

  @Test
  public void testKafkaAvroDekRotationF1() throws Exception {
    IndexedRecord avroRecord = createF1Record();
    AvroSchema avroSchema = new AvroSchema(createF1Schema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("encrypt.dek.expiry.days", "1", "preserve.source.fields", "true"),
        null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("hello world", record.get("f1"));
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroDekRotationInvalidExpiry() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("encrypt.dek.expiry.days", "-1", "preserve.source.fields", "true"),
        null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    avroSerializer.serialize(topic, headers, avroRecord);
  }

  @Test
  public void testKafkaAvroSerializerWithAlgorithm() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("encrypt.dek.algorithm", "AES128_GCM"), null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer, DekFormat.AES128_GCM);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer, DekFormat.AES128_GCM);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));
  }

  @Test
  public void testKafkaAvroSerializerBytes() throws Exception {
    IndexedRecord avroRecord = createUserBytesRecord();
    AvroSchema avroSchema = new AvroSchema(createUserBytesSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals(ByteBuffer.wrap("testUser".getBytes(StandardCharsets.UTF_8)), record.get("name"));
  }

  @Test
  public void testKafkaAvroSerializerWithNull() throws Exception {
    IndexedRecord avroRecord = createUserRecordWithNull();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 0;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertNull(record.get("name"));
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerInt() throws Exception {
    IndexedRecord avroRecord = createUserRecordWithTaggedInt();
    AvroSchema avroSchema = new AvroSchema(createUserSchemaWithTaggedInt());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    avroSerializer.serialize(topic, headers, avroRecord);
  }

  @Test
  public void testKafkaAvroSerializerReflection() throws Exception {
    OldWidget widget = new OldWidget("alice");
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 7;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(reflectionAvroSerializer);
    byte[] bytes = reflectionAvroSerializer.serialize(topic, headers, widget);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(reflectionAvroDeserializer);
    Object obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be a Widget",
        OldWidget.class.isInstance(obj)
    );
    assertEquals("alice", ((OldWidget)obj).getName());
    assertEquals("123", ((OldWidget)obj).getSsn().get(0));
    assertEquals("456", ((OldWidget)obj).getSsn().get(1));
    assertEquals("789", ((OldWidget)obj).getPiiArray().get(0).getPii());
    assertEquals("012", ((OldWidget)obj).getPiiArray().get(1).getPii());
    assertEquals("345", ((OldWidget)obj).getPiiMap().get("key1").getPii());
    assertEquals("678", ((OldWidget)obj).getPiiMap().get("key2").getPii());
  }

  @Test
  public void testKafkaAvroSerializerReflectionPreserveSource() throws Exception {
    OldWidget widget = new OldWidget("alice");
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("preserve.source.fields", "true"), null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 7;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(reflectionAvroSerializer);
    byte[] bytes = reflectionAvroSerializer.serialize(topic, headers, widget);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(reflectionAvroDeserializer);
    Object obj = reflectionAvroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be a Widget",
        OldWidget.class.isInstance(obj)
    );
    assertEquals("alice", ((OldWidget)obj).getName());
    // Old value is preserved
    assertEquals("alice", widget.getName());
  }

  @Test
  public void testKafkaAvroSchemaSerializerUnionWithRefs() throws Exception {
    String ccStr = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"CreditCardInfo\",\n"
        + "  \"namespace\": \"com.confluent.dto\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"cardNumber\",\n"
        + "      \"type\": \"string\",\n"
        + "      \"default\": \"\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"cardPin\",\n"
        + "      \"type\": \"string\",\n"
        + "      \"default\": \"\",\n"
        + "      \"confluent:tags\": [\"PII\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";
    String bankingStr = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"BankingInfo\",\n"
        + "  \"namespace\": \"com.confluent.dto\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"accountId\",\n"
        + "      \"type\": \"string\",\n"
        + "      \"default\": \"\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"accountPassword\",\n"
        + "      \"type\": \"string\",\n"
        + "      \"default\": \"\",\n"
        + "      \"confluent:tags\": [\"PII\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";
    String schemaStr = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Employee\",\n"
        + "  \"namespace\": \"com.confluent.dto\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"id\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"paymentDetails\",\n"
        + "      \"type\": [\"null\", \"BankingInfo\", \"CreditCardInfo\"],\n"
        + "      \"default\": null\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    Schema.Parser parser = new Schema.Parser();
    Schema ccSchema = parser.parse(ccStr);

    Schema bankingSchema = parser.parse(bankingStr);
    GenericRecord banking = new GenericData.Record(bankingSchema);
    banking.put("accountId", "123");
    banking.put("accountPassword", "456");

    Schema employeeSchema = parser.parse(schemaStr);
    GenericRecord employee = new GenericData.Record(employeeSchema);
    employee.put("id", "789");
    employee.put("paymentDetails", banking);

    schemaRegistry.register("CreditCardInfoJson", new AvroSchema(ccSchema));
    schemaRegistry.register("BankingInfoJson", new AvroSchema(bankingSchema));

    AvroSchema avroSchema = new AvroSchema(schemaStr,
        ImmutableList.of(new SchemaReference("CreditCardInfo", "CreditCardInfoJson", 1),
            new SchemaReference("BankingInfo", "BankingInfoJson", 1)),
        ImmutableMap.of("CreditCardInfo", ccStr, "BankingInfo", bankingStr),
        null);
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, employee);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    Object obj = avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be an employee ",
        GenericRecord.class.isInstance(obj)
    );
    assertEquals(
        "Returned value should be the password",
        "456",
        ((GenericRecord) ((GenericRecord) obj).get("paymentDetails")).get("accountPassword")
    );
  }

  @Test
  public void testKafkaAvroSerializerMultipleRules() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    Rule rule2 = new Rule("rule2", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII2"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule, rule2));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer, "rule1");
    Cryptor cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));
    assertEquals("testUser2", record.get("name2"));
  }

  @Test
  public void testKafkaAvroSerializerMultipleRulesIncludingDekRotation() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("preserve.source.fields", "true"),
        null, null, null, false);
    Rule rule2 = new Rule("rule2", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII2"),
        ImmutableMap.of("encrypt.dek.expiry.days", "1", "preserve.source.fields", "true"),
        null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule, rule2));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer, "rule1");
    Cryptor cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));
    assertEquals("testUser2", record.get("name2"));

    // Advance 2 days
    fakeClock.advance(2, ChronoUnit.DAYS);

    expectedEncryptions += 1;
    cryptor = addSpyToCryptor(avroSerializer, "rule1");
    cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));
    assertEquals("testUser2", record.get("name2"));

    Dek dek = dekRegistry.getDekLatestVersion("kek1", topic + "-value", null, false);
    assertEquals(2, dek.getVersion());
  }

  @Test
  public void testKafkaAvroSerializerDoubleEncryption() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    Rule rule2 = new Rule("rule2", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII3"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule, rule2));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer, "rule1");
    Cryptor cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));
  }

  @Test
  public void testKafkaAvroSerializerDoubleEncryptionWithDekRotation() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("preserve.source.fields", "true"),
        null, null, null, false);
    Rule rule2 = new Rule("rule2", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII3"),
        ImmutableMap.of("encrypt.dek.expiry.days", "1", "preserve.source.fields", "true"),
        null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule, rule2));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer, "rule1");
    Cryptor cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));

    // Advance 2 days
    fakeClock.advance(2, ChronoUnit.DAYS);

    expectedEncryptions += 1;
    headers = new RecordHeaders();
    cryptor = addSpyToCryptor(avroSerializer, "rule1");
    cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));

    Dek dek = dekRegistry.getDekLatestVersion("kek1", topic + "-value", null, false);
    assertEquals(2, dek.getVersion());
  }

  @Test
  public void testKafkaAvroSerializerDoubleEncryptionAllDekRotation() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("encrypt.dek.expiry.days", "2", "preserve.source.fields", "true"),
        null, null, null, false);
    Rule rule2 = new Rule("rule2", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII3"),
        ImmutableMap.of("encrypt.dek.expiry.days", "1", "preserve.source.fields", "true"),
        null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule, rule2));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer, "rule1");
    Cryptor cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));

    // Advance 2 days
    fakeClock.advance(3, ChronoUnit.DAYS);

    expectedEncryptions += 1;
    headers = new RecordHeaders();
    cryptor = addSpyToCryptor(avroSerializer, "rule1");
    cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptions * 2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));

    Dek dek = dekRegistry.getDekLatestVersion("kek1", topic + "-value", null, false);
    assertEquals(3, dek.getVersion());
  }

  @Test
  public void testKafkaAvroSerializerRuleWithSameTag() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    Rule rule2 = new Rule("rule2", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule, rule2));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptionsRule1 = 1;
    int expectedEncryptionsRule2 = 0;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer, "rule1");
    Cryptor cryptor2 = addSpyToCryptor(avroSerializer, "rule2");
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptionsRule1 + expectedEncryptionsRule2)).encrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptionsRule1)).encrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptionsRule2)).encrypt(any(), any(), any());
    }
    cryptor = addSpyToCryptor(avroDeserializer, "rule1");
    cryptor2 = addSpyToCryptor(avroDeserializer, "rule2");
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    if (cryptor == cryptor2) {
      verify(cryptor, times(expectedEncryptionsRule1 + expectedEncryptionsRule2)).decrypt(any(), any(), any());
    } else {
      verify(cryptor, times(expectedEncryptionsRule1)).decrypt(any(), any(), any());
      verify(cryptor2, times(expectedEncryptionsRule2)).decrypt(any(), any(), any());
    }
    assertEquals("testUser", record.get("name"));
  }

  @Test
  public void testKafkaAvroSerializerQualifiedRuleNames() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    // Register to key subject
    schemaRegistry.register(topic + "-key", avroSchema);

    rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII2"), null, null, null, null, false);
    ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    avroSchema = avroSchema.copy(metadata, ruleSet);
    // Register to value subject
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroKeySerializer, "test-key:rule1");
    byte[] bytes = avroKeySerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    Cryptor cryptor2 = addSpyToCryptor(avroValueSerializer, "test-value:rule1");
    RecordHeaders headers2 = new RecordHeaders();
    avroRecord = createUserRecord();
    byte[] bytes2 = avroValueSerializer.serialize(topic, headers2, avroRecord);
    verify(cryptor2, times(expectedEncryptions)).encrypt(any(), any(), any());

    cryptor = addSpyToCryptor(avroKeyDeserializer, "test-key:rule1");
    GenericRecord record = (GenericRecord) avroKeyDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name").toString());
    assertEquals("testUser2", record.get("name2").toString());
    cryptor2 = addSpyToCryptor(avroValueDeserializer, "test-value:rule1");
    GenericRecord record2 = (GenericRecord) avroValueDeserializer.deserialize(topic, headers2, bytes2);
    verify(cryptor2, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record2.get("name").toString());
    assertEquals("testUser2", record2.get("name2").toString());
  }

  @Test
  public void testKafkaAvroSerializerExistingKek() throws Exception {
    // Create shared kek
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(),
        fieldEncryptionProps.getKmsKeyId(), fieldEncryptionProps.getKmsProps(), null, false);

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, "kek1");
    Metadata metadata = getMetadata(properties);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));
  }

  @Test
  public void testKafkaAvroSerializerExistingSharedKek() throws Exception {
    // Create shared kek
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(),
        fieldEncryptionProps.getKmsKeyId(), fieldEncryptionProps.getKmsProps(), null, true);

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));
  }

  @Test
  public void testKafkaAvroSerializerBadKekName() throws Exception {
    // Create shared kek
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(),
        fieldEncryptionProps.getKmsKeyId(), fieldEncryptionProps.getKmsProps(), null, false);

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, "$kek");
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_TYPE, "wrong");
    Metadata metadata = getMetadata(properties);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    try {
      avroSerializer.serialize(topic, headers, avroRecord);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerializationException);
    }
  }

  @Test
  public void testKafkaAvroSerializerWrongKmsType() throws Exception {
    // Create shared kek
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(),
        fieldEncryptionProps.getKmsKeyId(), fieldEncryptionProps.getKmsProps(), null, false);

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, "kek1");
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_TYPE, "wrong");
    Metadata metadata = getMetadata(properties);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    try {
      avroSerializer.serialize(topic, headers, avroRecord);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerializationException);
    }
  }

  @Test
  public void testKafkaAvroSerializerWrongKmsKeyId() throws Exception {
    // Create shared kek
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(),
        fieldEncryptionProps.getKmsKeyId(), fieldEncryptionProps.getKmsProps(), null, false);

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, "kek1");
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_KEY_ID, "wrong");
    Metadata metadata = getMetadata(properties);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    try {
      avroSerializer.serialize(topic, headers, avroRecord);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerializationException);
    }
  }

  @Test
  public void testKafkaJsonSchemaSerializer() throws Exception {
    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"ssn\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"type\":\"string\"}}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"piiArray\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/OldPii\"}}]},"
        + "\"piiMap\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"object\",\"additionalProperties\":{\"$ref\":\"#/definitions/OldPii\"}}]},"
        + "\"size\":{\"type\":\"integer\"},"
        + "\"version\":{\"type\":\"integer\"}},"
        + "\"required\":[\"size\",\"version\"],"
        + "\"definitions\":{\"OldPii\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{"
        + "\"pii\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]}}}}}";
    JsonSchema jsonSchema = new JsonSchema(schemaStr);
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    int expectedEncryptions = 5;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(jsonSchemaSerializer);
    byte[] bytes = jsonSchemaSerializer.serialize(topic, headers, widget);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(jsonSchemaDeserializer);
    Object obj = jsonSchemaDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be a Widget",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "alice",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "123",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "456",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "789",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "012",
        ((JsonNode)obj).get("piiArray").get(1).get("pii").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerPreserveSource() throws Exception {
    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"ssn\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"type\":\"string\"}}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"piiArray\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/OldPii\"}}]},"
        + "\"piiMap\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"object\",\"additionalProperties\":{\"$ref\":\"#/definitions/OldPii\"}}]},"
        + "\"size\":{\"type\":\"integer\"},"
        + "\"version\":{\"type\":\"integer\"}},"
        + "\"required\":[\"size\",\"version\"],"
        + "\"definitions\":{\"OldPii\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{"
        + "\"pii\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]}}}}}";
    JsonSchema jsonSchema = new JsonSchema(schemaStr);
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("preserve.source.fields", "true"), null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    int expectedEncryptions = 5;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(jsonSchemaSerializer);
    byte[] bytes = jsonSchemaSerializer.serialize(topic, headers, widget);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(jsonSchemaDeserializer);
    Object obj = jsonSchemaDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertEquals("alice", ((JsonNode)obj).get("name").textValue());
    // Old value is preserved
    assertEquals("alice", widget.getName());
  }

  @Test
  public void testKafkaJsonSchemaSerializerAnnotated() throws Exception {
    AnnotatedOldWidget widget = new AnnotatedOldWidget("alice");
    widget.setSize(123);
    widget.setAnnotatedSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new AnnotatedOldPii("789"), new AnnotatedOldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"ssn\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"type\":\"string\"}}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"piiArray\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/OldPii\"}}]},"
        + "\"piiMap\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"object\",\"additionalProperties\":{\"$ref\":\"#/definitions/OldPii\"}}]},"
        + "\"size\":{\"type\":\"integer\"},"
        + "\"version\":{\"type\":\"integer\"}},"
        + "\"required\":[\"size\",\"version\"],"
        + "\"definitions\":{\"OldPii\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{"
        + "\"pii\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]}}}}}";
    JsonSchema jsonSchema = new JsonSchema(schemaStr);
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    int expectedEncryptions = 5;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(jsonSchemaSerializer2);
    byte[] bytes = jsonSchemaSerializer2.serialize(topic, headers, widget);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(jsonSchemaDeserializer);
    Object obj = jsonSchemaDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be a Widget",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "alice",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "123",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "456",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "789",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "012",
        ((JsonNode)obj).get("piiArray").get(1).get("pii").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerOneOfWithRefs() throws Exception {
    BankingInfo bankingInfo = new BankingInfo();
    bankingInfo.setAccountId("123");
    bankingInfo.setAccountPassword("456");
    Employee employee = new Employee();
    employee.setId("789");
    employee.setPaymentDetails(bankingInfo);

    String ccStr = "{\n" +
        "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
        "  \"additionalProperties\": false,\n" +
        "  \"properties\": {\n" +
        "    \"paymentType\": {\"type\":\"string\",\"enum\":[\"CC\"],\"default\":\"CC\"},\n" +
        "    \"cardNumber\": {\n" +
        "      \"default\": \"\",\n" +
        "      \"type\": \"string\"\n" +
        "    },\n" +
        "    \"cardPin\": {\n" +
        "      \"confluent:tags\": [\n" +
        "        \"PII\"\n" +
        "      ],\n" +
        "      \"default\": \"\",\n" +
        "      \"type\": \"string\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"title\": \"CreditCardInfo\",\n" +
        "  \"type\": \"object\"\n" +
        "}";
    String bankingStr = "{\n" +
        "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
        "  \"additionalProperties\": false,\n" +
        "  \"properties\": {\n" +
        "    \"paymentType\": {\"type\":\"string\",\"enum\":[\"BANK\"],\"default\":\"BANK\"},\n" +
        "    \"accountId\": {\n" +
        "      \"default\": \"\",\n" +
        "      \"type\": \"string\"\n" +
        "    },\n" +
        "    \"accountPassword\": {\n" +
        "      \"confluent:tags\": [\n" +
        "        \"PII\"\n" +
        "      ],\n" +
        "      \"default\": \"\",\n" +
        "      \"type\": \"string\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"title\": \"BankingInfo\",\n" +
        "  \"type\": \"object\"\n" +
        "}";
    String schemaStr = "{\n" +
        "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
        "  \"additionalProperties\": false,\n" +
        "  \"properties\": {\n" +
        "    \"id\": {\n" +
        "      \"type\": \"string\"\n" +
        "    },\n" +
        "    \"paymentDetails\": {\n" +
        "      \"anyOf\": [\n" +
        "        {\n" +
        "          \"$ref\": \"CreditCardInfo\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"$ref\": \"BankingInfo\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"null\"\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  },\n" +
        "  \"title\": \"Employee\",\n" +
        "  \"type\": \"object\"\n" +
        "}";

    JsonSchema ccSchema = new JsonSchema(ccStr);
    schemaRegistry.register("CreditCardInfoJson", ccSchema);
    JsonSchema bankingSchema = new JsonSchema(bankingStr);
    schemaRegistry.register("BankingInfoJson", bankingSchema);

    JsonSchema jsonSchema = new JsonSchema(schemaStr,
        ImmutableList.of(new SchemaReference("CreditCardInfo", "CreditCardInfoJson", 1),
            new SchemaReference("BankingInfo", "BankingInfoJson", 1)),
        ImmutableMap.of("CreditCardInfo", ccStr, "BankingInfo", bankingStr),
        null);
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(jsonSchemaSerializer3);
    byte[] bytes = jsonSchemaSerializer3.serialize(topic, headers, employee);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(jsonSchemaDeserializer);
    Object obj = jsonSchemaDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be an employee ",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned value should be the password",
        "456",
        ((JsonNode)obj).get("paymentDetails").get("accountPassword").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerMissingProperty() throws Exception {
    Address address = new Address();
    address.doornumber = 1234;
    address.doorpin = "1234";
    address.street = "la rue";

    SampleEvent test = new SampleEvent();
    test.firstname = "bob";
    test.lastname = "cool";
    test.nas = "2012-12-12";
    test.address = address;

    String schemaStr = "{\n"
        + "    \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
        + "    \"additionalProperties\": false,\n"
        + "    \"definitions\": {\n"
        + "        \"Address\": {\n"
        + "            \"additionalProperties\": false,\n"
        + "            \"properties\": {\n"
        + "                \"doornumber\": {\n"
        + "                    \"type\": \"integer\"\n"
        + "                },\n"
        + "                \"doorpin\": {\n"
        + "                    \"confluent:tags\": [\n"
        + "                        \"PII\"\n"
        + "                    ],\n"
        + "                    \"type\": \"string\"\n"
        + "                },\n"
        + "                \"state\": {\n"
        + "                    \"type\": \"string\"\n"
        + "                },\n"
        + "                \"street\": {\n"
        + "                    \"type\": \"string\"\n"
        + "                }\n"
        + "            },\n"
        + "            \"required\": [\n"
        + "                \"doornumber\",\n"
        + "                \"street\",\n"
        + "                \"doorpin\"\n"
        + "            ],\n"
        + "            \"type\": \"object\"\n"
        + "        }\n"
        + "    },\n"
        + "    \"properties\": {\n"
        + "        \"address\": {\n"
        + "            \"$ref\": \"#/definitions/Address\"\n"
        + "        },\n"
        + "        \"firstname\": {\n"
        + "            \"type\": \"string\"\n"
        + "        },\n"
        + "        \"lastname\": {\n"
        + "            \"type\": \"string\"\n"
        + "        },\n"
        + "        \"nas\": {\n"
        + "            \"confluent:tags\": [\n"
        + "                \"PII\"\n"
        + "            ],\n"
        + "            \"type\": \"string\"\n"
        + "        }\n"
        + "    },\n"
        + "    \"required\": [\n"
        + "        \"firstname\",\n"
        + "        \"lastname\",\n"
        + "        \"nas\",\n"
        + "        \"address\"\n"
        + "    ],\n"
        + "    \"title\": \"Sample Event\",\n"
        + "    \"type\": \"object\"\n"
        + "}";

    JsonSchema jsonSchema = new JsonSchema(schemaStr);
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    int expectedEncryptions = 2;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(jsonSchemaSerializer4);
    byte[] bytes = jsonSchemaSerializer4.serialize(topic, headers, test);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(jsonSchemaDeserializer);
    Object obj = jsonSchemaDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be an an event ",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned value should be the password",
        "1234",
        ((JsonNode)obj).get("address").get("doorpin").textValue()
    );
  }

  @Test
  public void testKafkaProtobufSerializer() throws Exception {
    Widget widget = Widget.newBuilder()
        .setName("alice")
        .addSsn("123")
        .addSsn("456")
        .addPiiArray(Pii.newBuilder().setPii("789").build())
        .addPiiArray(Pii.newBuilder().setPii("012").build())
        .putPiiMap("key1", Pii.newBuilder().setPii("345").build())
        .putPiiMap("key2", Pii.newBuilder().setPii("678").build())
        .setSize(123)
        .build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    protobufSchema = protobufSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    int expectedEncryptions = 7;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(protobufSerializer);
    byte[] bytes = protobufSerializer.serialize(topic, headers, widget);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(protobufDeserializer);
    Object obj = protobufDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be a Widget",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object should be a NewWidget",
        "alice",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
    assertEquals(
        "Returned object should be a NewWidget",
        ImmutableList.of("123", "456"),
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("ssn"))
    );
    List<String> ssnArrayValues = ((List<?>)((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("pii_array")))
        .stream()
        .map(o -> {
          DynamicMessage msg = (DynamicMessage) o;
          return msg.getField(msg.getDescriptorForType().findFieldByName("pii")).toString();
        })
        .collect(Collectors.toList());
    assertEquals(
        "Returned object should be a NewWidget",
        ImmutableList.of("789", "012"),
        ssnArrayValues
    );
    List<String> ssnMapValues = ((List<?>)((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("pii_map")))
        .stream()
        .map(o -> {
          DynamicMessage msg = (DynamicMessage) o;
          DynamicMessage msg2 = (DynamicMessage) msg.getField(msg.getDescriptorForType().findFieldByName("value"));
          return msg2.getField(msg2.getDescriptorForType().findFieldByName("pii")).toString();
        })
        .collect(Collectors.toList());
    assertEquals(
        "Returned object should be a NewWidget",
        ImmutableList.of("345", "678"),
        ssnMapValues
    );
  }

  @Test
  public void testKafkaProtobufSerializerBytes() throws Exception {
    WidgetBytes widget = WidgetBytes.newBuilder()
        .setName(ByteString.copyFromUtf8("alice"))
        .addSsn(ByteString.copyFromUtf8("123"))
        .addSsn(ByteString.copyFromUtf8("456"))
        .addPiiArray(PiiBytes.newBuilder().setPii(ByteString.copyFromUtf8("789")).build())
        .addPiiArray(PiiBytes.newBuilder().setPii(ByteString.copyFromUtf8("012")).build())
        .setSize(123)
        .build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    protobufSchema = protobufSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    int expectedEncryptions = 5;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(protobufSerializerBytes);
    byte[] bytes = protobufSerializerBytes.serialize(topic, headers, widget);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(protobufDeserializer);
    Object obj = protobufDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());

    assertTrue(
        "Returned object should be a Widget",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object should be a NewWidget",
        ByteString.copyFromUtf8("alice"),
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
    assertEquals(
        "Returned object should be a NewWidget",
        ImmutableList.of(ByteString.copyFromUtf8("123"), ByteString.copyFromUtf8("456")),
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("ssn"))
    );
    List<ByteString> ssnArrayValues = ((List<?>)((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("pii_array")))
        .stream()
        .map(o -> {
          DynamicMessage msg = (DynamicMessage) o;
          return (ByteString) msg.getField(msg.getDescriptorForType().findFieldByName("pii"));
        })
        .collect(Collectors.toList());
    assertEquals(
        "Returned object should be a NewWidget",
        ImmutableList.of(ByteString.copyFromUtf8("789"), ByteString.copyFromUtf8("012")),
        ssnArrayValues
    );
  }

  @Test
  public void testNoEncryptionsDueToData() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    // Tag in rule differs from data
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("NOT_PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 0;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());

    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name").toString());
  }

  @Test
  public void testBadCryptor() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    addBadSpyToCryptor(avroSerializer);
    try {
      avroSerializer.serialize(topic, headers, avroRecord);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerializationException);
    }

    // Run good serializer to get bytes
    int expectedEncryptions = 1;
    headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());

    addBadSpyToCryptor(avroDeserializer);
    try {
      avroDeserializer.deserialize(topic, headers, bytes);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerializationException);
    }
  }

  @Test
  public void testBadCryptorIgnoreFailure() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    // NONE,NONE ignores errors during WRITE,READ
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, "NONE,NONE", false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addBadSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());

    // Run good serializer to get bytes
    headers = new RecordHeaders();
    cryptor = addSpyToCryptor(avroSerializer);
    bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());

    cryptor = addBadSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertNotEquals("testUser", record.get("name").toString()); // still encrypted
  }

  @Test
  public void testBadSerializerWithMissingRuleExecutors() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    try {
      badSerializer.serialize(topic, headers, avroRecord);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerializationException);
    }

    // Run good serializer to get bytes
    int expectedEncryptions = 1;
    headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());

    try {
      badDeserializer.deserialize(topic, headers, bytes);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerializationException);
    }
  }

  @Test
  public void testBadSerializerWithMissingRuleExecutorsButIgnoreFailure() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    // NONE,NONE ignores errors during WRITE,READ
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, "NONE,NONE", false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 0;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(badSerializer);
    assertNull(cryptor);
    byte[] oldBytes = badSerializer.serialize(topic, headers, avroRecord);

    // Run good serializer to get bytes
    expectedEncryptions = 1;
    headers = new RecordHeaders();
    cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    assertFalse(Arrays.equals(oldBytes, bytes));

    cryptor = addSpyToCryptor(badDeserializer);
    assertNull(cryptor);
    GenericRecord record = (GenericRecord) badDeserializer.deserialize(topic, headers, bytes);
    assertNotEquals("testUser", record.get("name").toString()); // still encrypted
  }

  @Test
  public void testGoodDekGenerator() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);


    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(goodDekSerializer);
    byte[] bytes = goodDekSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));
  }

  @Test
  public void testBadDekGenerator() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);


    RecordHeaders headers = new RecordHeaders();
    try {
      badDekSerializer.serialize(topic, headers, avroRecord);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerializationException);
    }
  }

  protected Metadata getMetadata(String kekName) {
    return getMetadata(kekName, null);
  }

  protected Metadata getMetadata(String kekName, DekFormat algorithm) {
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, kekName);
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_TYPE, fieldEncryptionProps.getKmsType());
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_KEY_ID, fieldEncryptionProps.getKmsKeyId());
    if (algorithm != null) {
      properties.put(FieldEncryptionExecutor.ENCRYPT_DEK_ALGORITHM, algorithm.name());
    }
    return getMetadata(properties);
  }

  protected Metadata getMetadata(Map<String, String> properties) {
    return new Metadata(Collections.emptyMap(), properties, Collections.emptySet());
  }

  public static class OldWidget {
    private String name;
    private List<String> ssn = new ArrayList<>();
    private List<OldPii> piiArray = new ArrayList<>();
    private Map<String, OldPii> piiMap = new HashMap<>();
    private int size;
    private int version;

    public OldWidget() {}
    public OldWidget(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public List<String> getSsn() {
      return ssn;
    }

    public void setSsn(List<String> ssn) {
      this.ssn = ssn;
    }

    public List<OldPii> getPiiArray() {
      return piiArray;
    }

    public void setPiiArray(List<OldPii> pii) {
      this.piiArray = pii;
    }

    public Map<String, OldPii> getPiiMap() {
      return piiMap;
    }

    public void setPiiMap(Map<String, OldPii> pii) {
      this.piiMap = pii;
    }

    public int getSize() {
      return size;
    }

    public void setSize(int size) {
      this.size = size;
    }

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OldWidget widget = (OldWidget) o;
      return name.equals(widget.name)
          && Objects.equals(ssn, widget.ssn)
          && Objects.equals(piiArray, widget.piiArray)
          && Objects.equals(piiMap, widget.piiMap)
          && size == widget.size
          && version == widget.version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, ssn, piiArray, piiMap, size, version);
    }
  }

  public static class OldPii {
    private String pii;

    public OldPii() {}
    public OldPii(String pii) {
      this.pii = pii;
    }

    public String getPii() {
      return pii;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OldPii pii1 = (OldPii) o;
      return Objects.equals(pii, pii1.pii);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pii);
    }
  }

  public static class AnnotatedOldWidget {
    private String annotatedName;
    private List<String> annotatedSsn = new ArrayList<>();
    private List<AnnotatedOldPii> piiArray = new ArrayList<>();
    private Map<String, AnnotatedOldPii> piiMap = new HashMap<>();
    private int size;
    private int version;

    public AnnotatedOldWidget() {}
    public AnnotatedOldWidget(String annotatedName) {
      this.annotatedName = annotatedName;
    }

    @JsonProperty("name")
    public String getAnnotatedName() {
      return annotatedName;
    }

    @JsonProperty("name")
    public void setAnnotatedName(String name) {
      this.annotatedName = name;
    }

    @JsonProperty("ssn")
    public List<String> getAnnotatedSsn() {
      return annotatedSsn;
    }

    @JsonProperty("ssn")
    public void setAnnotatedSsn(List<String> ssn) {
      this.annotatedSsn = ssn;
    }

    public List<AnnotatedOldPii> getPiiArray() {
      return piiArray;
    }

    public void setPiiArray(List<AnnotatedOldPii> pii) {
      this.piiArray = pii;
    }

    public Map<String, AnnotatedOldPii> getPiiMap() {
      return piiMap;
    }

    public void setPiiMap(Map<String, AnnotatedOldPii> pii) {
      this.piiMap = pii;
    }

    public int getSize() {
      return size;
    }

    public void setSize(int size) {
      this.size = size;
    }

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OldWidget widget = (OldWidget) o;
      return annotatedName.equals(widget.name)
          && Objects.equals(annotatedSsn, widget.ssn)
          && Objects.equals(piiArray, widget.piiArray)
          && Objects.equals(piiMap, widget.piiMap)
          && size == widget.size
          && version == widget.version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(annotatedName, annotatedSsn, piiArray, piiMap, size, version);
    }
  }

  public static class AnnotatedOldPii {
    @JsonProperty("pii")
    private String annotatedPii;

    public AnnotatedOldPii() {}
    public AnnotatedOldPii(String annotatedPii) {
      this.annotatedPii = annotatedPii;
    }

    public String getAnnotatedPii() {
      return annotatedPii;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AnnotatedOldPii pii1 = (AnnotatedOldPii) o;
      return Objects.equals(annotatedPii, pii1.annotatedPii);
    }

    @Override
    public int hashCode() {
      return Objects.hash(annotatedPii);
    }
  }

  public static class GoodDekGenerator extends FieldEncryptionExecutor {

    @Override
    protected byte[] generateDek(DekFormat dekFormat) throws GeneralSecurityException {
      // generate a valid dek
      return new byte[32];
    }
  }

  public static class BadDekGenerator extends FieldEncryptionExecutor {

    @Override
    protected byte[] generateDek(DekFormat dekFormat) throws GeneralSecurityException {
      // generate an invalid dek
      return new byte[15];
    }
  }

  public class Employee {
    @JsonProperty(required = true)
    private String id;
    @JsonProperty(required = true)
    private PaymentDetails paymentDetails;

    // Getters and Setters
    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public PaymentDetails getPaymentDetails() {
      return paymentDetails;
    }

    public void setPaymentDetails(PaymentDetails paymentDetails) {
      this.paymentDetails = paymentDetails;
    }
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = As.EXISTING_PROPERTY,
      property = "paymentType"
  )
  @JsonSubTypes({
      @JsonSubTypes.Type(value = BankingInfo.class, name = "BANK"),
      @JsonSubTypes.Type(value = CreditCardInfo.class, name = "CC")
  })
  public static class PaymentDetails {
    private String paymentType;

    public PaymentDetails(String paymentType) {
      this.paymentType = paymentType;
    }

    // Getters and Setters
    public String getPaymentType() {
      return paymentType;
    }
  }

  public static class BankingInfo extends PaymentDetails {
    private String accountId;
    private String accountPassword;

    public BankingInfo() {
      super("BANK");
    }

    // Getters and Setters
    public String getAccountId() {
      return accountId;
    }

    public void setAccountId(String accountId) {
      this.accountId = accountId;
    }

    public String getAccountPassword() {
      return accountPassword;
    }

    public void setAccountPassword(String accountPassword) {
      this.accountPassword = accountPassword;
    }
  }

  public static class CreditCardInfo extends PaymentDetails {
    private String cardNumber;
    private String cardPin;

    public CreditCardInfo() {
      super("CC");
    }

    // Getters and Setters
    public String getCardNumber() {
      return cardNumber;
    }

    public void setCardNumber(String cardNumber) {
      this.cardNumber = cardNumber;
    }

    public String getCardPin() {
      return cardPin;
    }

    public void setCardPin(String cardPin) {
      this.cardPin = cardPin;
    }
  }

  public class SampleEvent {
    @JsonProperty(required = true)
    public String firstname;

    @JsonProperty(required = true)
    public String lastname;

    @JsonProperty(required = true)
    public String nas;

    @JsonProperty(required = true)
    public Address address;

    @Override
    public String toString() {
      return "SampleEvent{" +
          "firstname='" + firstname + '\'' +
          ", lastname='" + lastname + '\'' +
          ", nas='" + nas + '\'' +
          ", Address='" + address.toString() + '\'' +
          '}';
    }
  }

  class Address {
    @JsonProperty(required = true)
    public Integer doornumber;
    @JsonProperty(required = true)
    public String street;

    //@JsonProperty(required = true)
    //public String state;

    @JsonProperty(required = true)
    public String doorpin;

    @Override
    public String toString() {
      return "Address{" +
          "doornumber='" + doornumber + '\'' +
          ", street='" + street + '\'' +
          // ", state='" + state + '\'' +
          ", doorpin='" + doorpin + '\'' +
          '}';
    }
  }
}

