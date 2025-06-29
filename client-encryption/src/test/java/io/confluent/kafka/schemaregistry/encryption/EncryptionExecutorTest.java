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

package io.confluent.kafka.schemaregistry.encryption;

import static io.confluent.kafka.schemaregistry.encryption.EncryptionExecutor.CLOCK;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.TEST_CLIENT;
import static io.confluent.kafka.schemaregistry.rules.RuleBase.DEFAULT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.protobuf.Descriptors.Descriptor;
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
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.RuleBase;
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
import java.security.GeneralSecurityException;
import java.util.ArrayList;
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

  protected final EncryptionProperties encryptionProps;
  protected final SchemaRegistryClient schemaRegistry;
  protected final DekRegistryClient dekRegistry;
  protected final KafkaAvroSerializer avroSerializer;
  protected final KafkaAvroDeserializer avroDeserializer;
  protected final KafkaJsonSchemaSerializer<OldWidget> jsonSchemaSerializer;
  protected final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;
  protected final KafkaProtobufSerializer<Widget> protobufSerializer;
  protected final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  protected final String topic;
  protected final FakeClock fakeClock = new FakeClock();

  public EncryptionExecutorTest() throws Exception {
    topic = "test";
    List<String> ruleNames = ImmutableList.of("rule1", "rule2");
    encryptionProps = getEncryptionProperties(ruleNames, EncryptionExecutor.class);
    Map<String, Object> clientProps = encryptionProps.getClientProperties("mock://");
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
        Collections.singletonMap(TEST_CLIENT, encryptionProps.getTestClient()),
        null
    );

    avroSerializer = new KafkaAvroSerializer(schemaRegistry, clientProps);
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, clientProps);

    clientProps.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, false);
    jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, clientProps);
    jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry, clientProps);

    protobufSerializer = new KafkaProtobufSerializer<>(schemaRegistry, clientProps);
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, clientProps);
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
        EncryptionExecutor.TYPE, null, null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    int expectedEncryptions = 1;
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
        EncryptionExecutor.TYPE, null, null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = getMetadata("kek1");
    protobufSchema = protobufSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    int expectedEncryptions = 1;
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

  protected Metadata getMetadata(String kekName) {
    return getMetadata(kekName, null);
  }

  protected Metadata getMetadata(String kekName, DekFormat algorithm) {
    Map<String, String> properties = new HashMap<>();
    properties.put(EncryptionExecutor.ENCRYPT_KEK_NAME, kekName);
    properties.put(EncryptionExecutor.ENCRYPT_KMS_TYPE, encryptionProps.getKmsType());
    properties.put(EncryptionExecutor.ENCRYPT_KMS_KEY_ID, encryptionProps.getKmsKeyId());
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
}

