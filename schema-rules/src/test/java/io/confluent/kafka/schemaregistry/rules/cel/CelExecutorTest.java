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

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.DlqAction;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Pii;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Widget;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

public class CelExecutorTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final KafkaProtobufSerializer<Widget> protobufSerializer;
  private final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  private final KafkaJsonSchemaSerializer<OldWidget> jsonSchemaSerializer;
  private final KafkaJsonSchemaSerializer<TaggedOldWidget> jsonSchemaSerializer2;
  private final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;
  private final String topic;
  private final KafkaProducer<byte[], byte[]> producer;

  public CelExecutorTest() {
    topic = "test";
    schemaRegistry = new MockSchemaRegistryClient(ImmutableList.of(
        new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()));
    producer = mock(KafkaProducer.class);
    when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(
        CompletableFuture.completedFuture(null));

    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "true");
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS, "cel,cel-field");
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".cel.class",
        CelExecutor.class.getName());
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".cel-field.class",
        CelFieldExecutor.class.getName());
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_ACTIONS, "dlq");
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_ACTIONS + ".dlq.class",
        DlqAction.class.getName());
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_ACTIONS + ".dlq.param." + DlqAction.TOPIC,
        "dlq-topic");
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_ACTIONS + ".dlq.param." + DlqAction.PRODUCER,
        producer);
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, new HashMap(defaultConfig));

    HashMap<String, String> reflectionProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    reflectionProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "true");
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS, "cel,cel-field");
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".cel.class",
        CelExecutor.class.getName());
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".cel-field.class",
        CelFieldExecutor.class.getName());
    reflectionAvroSerializer = new KafkaAvroSerializer(schemaRegistry, reflectionProps);
    reflectionAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, reflectionProps);

    protobufSerializer = new KafkaProtobufSerializer<>(schemaRegistry, new HashMap(defaultConfig));
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, new HashMap(defaultConfig));

    jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, new HashMap(defaultConfig));
    jsonSchemaSerializer2 = new KafkaJsonSchemaSerializer<>(schemaRegistry, new HashMap(defaultConfig));
    jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry, new HashMap(defaultConfig));
  }

  private Schema createEnumSchema() {
    String enumSchema = "{\"name\": \"Kind\",\n"
        + "   \"type\": \"enum\",\n"
        + "  \"symbols\" : [\"ONE\", \"TWO\", \"THREE\"]\n"
        + "}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(enumSchema);
    return schema;
  }
  private Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}, "
        + "{\"name\": \"kind\",\n"
        + "  \"type\": {\n"
        + "    \"name\": \"Kind\",\n"
        + "    \"type\": \"enum\",\n"
        + "    \"symbols\" : [\"ONE\", \"TWO\", \"THREE\"]\n"
        + "  }\n"
        + "}"
        + "]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private IndexedRecord createUserRecord() {
    Schema enumSchema = createEnumSchema();
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("kind", new GenericData.EnumSymbol(enumSchema, "ONE"));
    return avroRecord;
  }

  private Schema createWidgetSchema() {
    String userSchema = "{\"type\":\"record\",\"name\":\"OldWidget\",\"namespace\":\"io.confluent.kafka.schemaregistry.rules.cel.CelExecutorTest\",\"fields\":\n"
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

  @Test
  public void testKafkaAvroSerializer() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", RuleKind.CONSTRAINT, RuleMode.READ,
        CelExecutor.TYPE, null, "message.name == \"testUser\" && message.kind == \"ONE\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerConstraintException() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", RuleKind.CONSTRAINT, RuleMode.READ,
        CelExecutor.TYPE, null, "message.name != \"testUser\" || message.kind != \"ONE\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    avroDeserializer.deserialize(topic, bytes);
  }

  @Test
  public void testKafkaAvroSerializerConstraintIgnore() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", RuleKind.CONSTRAINT, RuleMode.READ,
        CelExecutor.TYPE, null, "message.name != \"testUser\" || message.kind != \"ONE\"",
        null, "NONE", false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    avroDeserializer.deserialize(topic, bytes);
  }

  @Test
  public void testKafkaAvroSerializerConstraintDlq() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", RuleKind.CONSTRAINT, RuleMode.READ,
        CelExecutor.TYPE, null, "message.name != \"testUser\" || message.kind != \"ONE\"",
        null, "DLQ", false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    try {
      byte[] bytes = avroSerializer.serialize(topic, avroRecord);
      avroDeserializer.deserialize(topic, bytes);
      fail("Should send to DLQ and throw exception");
    } catch (SerializationException e) {
      // expected
    }

    verify(producer).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testKafkaAvroSerializerReflection() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    Schema schema = ReflectData.get().getSchema(widget.getClass());
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", RuleKind.CONSTRAINT, RuleMode.READ,
        CelExecutor.TYPE, null, "message.name == \"alice\"", null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);


    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object should be a Widget",
        OldWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
  }

  @Test
  public void testKafkaAvroSerializerReflectionFieldTransform() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), "value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object should be a Widget",
        OldWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
    assertEquals("alice-suffix", ((OldWidget)obj).getName());
    assertEquals("123-suffix", ((OldWidget)obj).getSsn().get(0));
    assertEquals("456-suffix", ((OldWidget)obj).getSsn().get(1));
    assertEquals("789-suffix", ((OldWidget)obj).getPiiArray().get(0).getPii());
    assertEquals("012-suffix", ((OldWidget)obj).getPiiArray().get(1).getPii());
    assertEquals("345-suffix", ((OldWidget)obj).getPiiMap().get("key1").getPii());
    assertEquals("678-suffix", ((OldWidget)obj).getPiiMap().get("key2").getPii());
  }

  @Test
  public void testKafkaProtobufSerializerFieldTransform() throws Exception {
    byte[] bytes;
    Object obj;

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
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), "value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    protobufSchema = protobufSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobufSerializer.serialize(topic, widget);

    obj = protobufDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a Widget",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object should be a NewWidget",
        "alice-suffix",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
    assertEquals(
        "Returned object should be a NewWidget",
        ImmutableList.of("123-suffix", "456-suffix"),
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
        ImmutableList.of("789-suffix", "012-suffix"),
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
        ImmutableList.of("345-suffix", "678-suffix"),
        ssnMapValues
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerFieldTransform() throws Exception {
    byte[] bytes;
    Object obj;

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
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), "value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    jsonSchema = jsonSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer.serialize(topic, widget);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a Widget",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "alice-suffix",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "123-suffix",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "456-suffix",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "789-suffix",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "012-suffix",
        ((JsonNode)obj).get("piiArray").get(1).get("pii").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerTaggedFieldTransform() throws Exception {
    byte[] bytes;
    Object obj;

    TaggedOldWidget widget = new TaggedOldWidget("alice");
    widget.setSize(123);
    widget.withTaggedSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new TaggedOldPii("789"), new TaggedOldPii("012")));
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
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), "value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    jsonSchema = jsonSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer2.serialize(topic, widget);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a Widget",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "alice-suffix",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "123-suffix",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "456-suffix",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "789-suffix",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object should be a NewWidget",
        "012-suffix",
        ((JsonNode)obj).get("piiArray").get(1).get("pii").textValue()
    );
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

  public static class TaggedOldWidget {
    private String taggedName;
    private List<String> taggedSsn = new ArrayList<>();
    private List<TaggedOldPii> piiArray = new ArrayList<>();
    private Map<String, TaggedOldPii> piiMap = new HashMap<>();
    private int size;
    private int version;

    public TaggedOldWidget() {}
    public TaggedOldWidget(String taggedName) {
      this.taggedName = taggedName;
    }

    @JsonProperty("name")
    public String getTaggedName() {
      return taggedName;
    }

    @JsonProperty("name")
    public void setTaggedName(String name) {
      this.taggedName = name;
    }

    @JsonProperty("ssn")
    public List<String> getTaggedSsn() {
      return taggedSsn;
    }

    @JsonProperty("ssn")
    public void withTaggedSsn(List<String> ssn) {
      this.taggedSsn = ssn;
    }

    public List<TaggedOldPii> getPiiArray() {
      return piiArray;
    }

    public void setPiiArray(List<TaggedOldPii> pii) {
      this.piiArray = pii;
    }

    public Map<String, TaggedOldPii> getPiiMap() {
      return piiMap;
    }

    public void setPiiMap(Map<String, TaggedOldPii> pii) {
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
      return taggedName.equals(widget.name)
          && Objects.equals(taggedSsn, widget.ssn)
          && Objects.equals(piiArray, widget.piiArray)
          && Objects.equals(piiMap, widget.piiMap)
          && size == widget.size
          && version == widget.version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(taggedName, taggedSsn, piiArray, piiMap, size, version);
    }
  }

  public static class TaggedOldPii {
    @JsonProperty("pii")
    private String taggedPii;

    public TaggedOldPii() {}
    public TaggedOldPii(String taggedPii) {
      this.taggedPii = taggedPii;
    }

    public String getTaggedPii() {
      return taggedPii;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TaggedOldPii pii1 = (TaggedOldPii) o;
      return Objects.equals(taggedPii, pii1.taggedPii);
    }

    @Override
    public int hashCode() {
      return Objects.hash(taggedPii);
    }
  }
}
