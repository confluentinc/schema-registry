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

import static io.confluent.kafka.schemaregistry.encryption.EncryptionExecutor.CLOCK;
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
import com.google.crypto.tink.aead.AeadConfig;
import com.google.protobuf.DynamicMessage;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClientFactory;
import io.confluent.dekregistry.client.MockDekRegistryClientFactory;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.RuleBase;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.schemaregistry.rules.WidgetBytesProto.WidgetBytes;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.After;
import org.junit.Test;

public abstract class EncryptionExecutorTest {

  static {
    try {
      AeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  protected final EncryptionProperties fieldEncryptionProps;
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
  protected final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;
  protected final KafkaProtobufSerializer<Widget> protobufSerializer;
  protected final KafkaProtobufSerializer<WidgetBytes> protobufSerializerBytes;
  protected final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  protected final KafkaAvroSerializer badSerializer;
  protected final KafkaAvroDeserializer badDeserializer;
  protected final String topic;
  protected final FakeClock fakeClock = new FakeClock();

  public EncryptionExecutorTest() throws Exception {
    topic = "test";
    List<String> ruleNames = ImmutableList.of("rule1", "rule2");
    fieldEncryptionProps = getEncryptionProperties(ruleNames, EncryptionExecutor.class);
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
    EncryptionProperties qualifiedEncryptionProps =
        getEncryptionProperties(qualifiedRuleNames, EncryptionExecutor.class);
    Map<String, Object> qualifiedClientProps = qualifiedEncryptionProps.getClientProperties("mock://");
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
    jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry, clientProps);

    protobufSerializer = new KafkaProtobufSerializer<>(schemaRegistry, clientProps);
    protobufSerializerBytes = new KafkaProtobufSerializer<>(schemaRegistry, clientProps);
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, clientProps);

    Map<String, Object> badClientProps = new HashMap<>(clientProps);
    badClientProps.remove(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS);
    badClientProps.put(AbstractKafkaSchemaSerDeConfig.RULE_SERVICE_LOADER_ENABLE, false);
    badSerializer = new KafkaAvroSerializer(schemaRegistry, badClientProps);
    badDeserializer = new KafkaAvroDeserializer(schemaRegistry, badClientProps);
  }

  protected abstract EncryptionProperties getEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor);

  protected Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde) throws Exception {
    return addSpyToCryptor(serde, DekFormat.AES256_GCM);
  }

  protected Cryptor addSpyToCryptor(AbstractKafkaSchemaSerDe serde, DekFormat dekFormat) throws Exception {
    Map<String, Map<String, RuleBase>> executors = serde.getRuleExecutors();
    Map<String, RuleBase> executorsByType = executors.get(EncryptionExecutor.TYPE);
    EncryptionExecutor executor = null;
    if (executorsByType != null && !executorsByType.isEmpty()) {
      executor = (EncryptionExecutor) executorsByType.entrySet().iterator().next().getValue();
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
    Map<String, RuleBase> executorsByType = executors.get(EncryptionExecutor.TYPE);
    EncryptionExecutor executor = null;
    if (executorsByType != null && !executorsByType.isEmpty()) {
      executor = (EncryptionExecutor) executors.get(EncryptionExecutor.TYPE).get(name);
    }
    if (executor == null) {
      EncryptionExecutor encryptor = (EncryptionExecutor)
          executors.get(EncryptionExecutor.TYPE).get("_ENCRYPT_");
      executor = encryptor;
    }
    if (executor == null) {
      EncryptionExecutor encryptor = (EncryptionExecutor)
          executors.get(EncryptionExecutor.TYPE).get(DEFAULT_NAME);
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
    EncryptionExecutor executor =
        (EncryptionExecutor) executors.get(EncryptionExecutor.TYPE).entrySet()
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
    Parser parser = new Parser();
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
    Parser parser = new Parser();
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
    Parser parser = new Parser();
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
    Parser parser = new Parser();
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
    String userSchema = "{\"type\":\"record\",\"name\":\"OldWidget\",\"namespace\":\"io.confluent.kafka.schemaregistry.encryption.EncryptionExecutorTest\",\"fields\":\n"
        + "[{\"name\": \"name\", \"type\": \"string\",\"confluent:tags\": [\"PII\"]},\n"
        + "{\"name\": \"ssn\", \"type\": { \"type\": \"array\", \"items\": \"string\"},\"confluent:tags\": [\"PII\"]},\n"
        + "{\"name\": \"piiArray\", \"type\": { \"type\": \"array\", \"items\": { \"type\": \"record\", \"name\":\"OldPii\", \"fields\":\n"
        + "[{\"name\": \"pii\", \"type\": \"string\",\"confluent:tags\": [\"PII\"]}]}}},\n"
        + "{\"name\": \"piiMap\", \"type\": { \"type\": \"map\", \"values\": \"OldPii\"},\n"
        + "\"confluent:tags\": [\"PII\"]},\n"
        + "{\"name\": \"size\", \"type\": \"int\"},{\"name\": \"version\", \"type\": \"int\"}]}";
    Parser parser = new Parser();
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
        EncryptionExecutor.TYPE, null, null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.emptyList(), ImmutableList.of(rule));
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
    assertEquals("testUser", record.get("name").toString());
  }

  protected Metadata getMetadata(String kekName) {
    return getMetadata(kekName, null);
  }

  protected Metadata getMetadata(String kekName, DekFormat algorithm) {
    Map<String, String> properties = new HashMap<>();
    properties.put(EncryptionExecutor.ENCRYPT_KEK_NAME, kekName);
    properties.put(EncryptionExecutor.ENCRYPT_KMS_TYPE, fieldEncryptionProps.getKmsType());
    properties.put(EncryptionExecutor.ENCRYPT_KMS_KEY_ID, fieldEncryptionProps.getKmsKeyId());
    if (algorithm != null) {
      properties.put(EncryptionExecutor.ENCRYPT_DEK_ALGORITHM, algorithm.name());
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
}

