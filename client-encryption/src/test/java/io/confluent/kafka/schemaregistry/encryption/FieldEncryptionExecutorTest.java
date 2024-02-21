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

  private final FieldEncryptionProperties fieldEncryptionProps;
  private final SchemaRegistryClient schemaRegistry;
  private final DekRegistryClient dekRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer avroKeySerializer;
  private final KafkaAvroDeserializer avroKeyDeserializer;
  private final KafkaAvroSerializer avroValueSerializer;
  private final KafkaAvroDeserializer avroValueDeserializer;
  private final KafkaAvroSerializer avroSerializerWithoutKey;
  private final KafkaAvroDeserializer avroDeserializerWithoutKey;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final KafkaJsonSchemaSerializer<OldWidget> jsonSchemaSerializer;
  private final KafkaJsonSchemaSerializer<AnnotatedOldWidget> jsonSchemaSerializer2;
  private final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;
  private final KafkaProtobufSerializer<Widget> protobufSerializer;
  private final KafkaProtobufSerializer<WidgetBytes> protobufSerializerBytes;
  private final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  private final KafkaAvroSerializer badSerializer;
  private final KafkaAvroDeserializer badDeserializer;
  private final KafkaAvroSerializer goodDekSerializer;
  private final KafkaAvroSerializer badDekSerializer;
  private final String topic;
  private final FakeClock fakeClock = new FakeClock();

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

    jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, clientProps);
    jsonSchemaSerializer2 = new KafkaJsonSchemaSerializer<>(schemaRegistry, clientProps);
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

  private Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde) throws Exception {
    return addSpyToCryptor(serde, DekFormat.AES256_GCM);
  }

  private Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde, DekFormat dekFormat) throws Exception {
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

  private Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde, String name) throws Exception {
    return addSpyToCryptor(serde, name, DekFormat.AES256_GCM);
  }

  private Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde, String name, DekFormat dekFormat)
      throws Exception {
    Map<String, Map<String, RuleBase>> executors = serde.getRuleExecutors();
    Map<String, RuleBase> executorsByType = executors.get(FieldEncryptionExecutor.TYPE);
    FieldEncryptionExecutor executor = null;
    if (executorsByType != null && !executorsByType.isEmpty()) {
      executor = (FieldEncryptionExecutor) executors.get(FieldEncryptionExecutor.TYPE).get(name);
    }
    if (executor == null) {
      FieldEncryptionExecutor encryptor = (FieldEncryptionExecutor) executors.get(FieldEncryptionExecutor.TYPE).get(DEFAULT_NAME);
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

  private Cryptor addBadSpyToCryptor(AbstractKafkaSchemaSerDe serde) throws Exception {
    return addBadSpyToCryptor(serde, DekFormat.AES256_GCM);
  }

  private Cryptor addBadSpyToCryptor(AbstractKafkaSchemaSerDe serde, DekFormat dekFormat) throws Exception {
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

  private Schema createUserSchema() {
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

  private GenericRecord createUserRecord() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("name2", "testUser2");
    avroRecord.put("age", 18);
    return avroRecord;
  }

  private Schema createUserSchemaWithTaggedInt() {
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

  private IndexedRecord createUserRecordWithTaggedInt() {
    Schema schema = createUserSchemaWithTaggedInt();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("name2", "testUser2");
    avroRecord.put("age", 18);
    return avroRecord;
  }

  private Schema createUserBytesSchema() {
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

  private IndexedRecord createUserBytesRecord() {
    Schema schema = createUserBytesSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", ByteBuffer.wrap("testUser".getBytes(StandardCharsets.UTF_8)));
    avroRecord.put("name2", ByteBuffer.wrap("testUser2".getBytes(StandardCharsets.UTF_8)));
    return avroRecord;
  }

  private Schema createWidgetSchema() {
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
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, kekName);
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_TYPE, fieldEncryptionProps.getKmsType());
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_KEY_ID, fieldEncryptionProps.getKmsKeyId());
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
}

