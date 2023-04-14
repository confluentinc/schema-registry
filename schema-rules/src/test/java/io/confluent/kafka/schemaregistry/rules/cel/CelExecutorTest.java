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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.DlqAction;
import io.confluent.kafka.schemaregistry.rules.PiiProto;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Pii;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Widget;
import io.confluent.kafka.schemaregistry.rules.WidgetWithRefProto.WidgetWithRef;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CelExecutorTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer avroKeySerializer;
  private final KafkaAvroDeserializer avroKeyDeserializer;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final KafkaProtobufSerializer<Widget> protobufSerializer;
  private final KafkaProtobufSerializer<WidgetWithRef> protobufWithRefSerializer;
  private final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  private final KafkaJsonSchemaSerializer<OldWidget> jsonSchemaSerializer;
  private final KafkaJsonSchemaSerializer<AnnotatedOldWidget> jsonSchemaSerializer2;
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

    Map<String, Object> defaultConfig = new HashMap<>();
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, "cel,cel-field");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".cel.class",
        CelExecutor.class.getName());
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".cel-field.class",
        CelFieldExecutor.class.getName());
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS, "dlq");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".dlq.class",
        DlqAction.class.getName());
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".dlq.param." + DlqAction.TOPIC,
        "dlq-topic");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".dlq.param." + DlqAction.PRODUCER,
        producer);
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, defaultConfig);
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, defaultConfig);
    avroKeySerializer = new KafkaAvroSerializer(schemaRegistry);
    avroKeySerializer.configure(defaultConfig, true);
    avroKeyDeserializer = new KafkaAvroDeserializer(schemaRegistry);
    avroKeyDeserializer.configure(defaultConfig, true);

    Map<String, Object> reflectionProps = new HashMap<>(defaultConfig);
    reflectionProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionAvroSerializer = new KafkaAvroSerializer(schemaRegistry, reflectionProps);
    reflectionAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, reflectionProps);

    protobufSerializer = new KafkaProtobufSerializer<>(schemaRegistry, defaultConfig);
    protobufWithRefSerializer = new KafkaProtobufSerializer<>(schemaRegistry, defaultConfig);
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, defaultConfig);

    jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, defaultConfig);
    jsonSchemaSerializer2 = new KafkaJsonSchemaSerializer<>(schemaRegistry, defaultConfig);
    jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry, defaultConfig);
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

  private Schema createWidgetSchemaNoTags() {
    String userSchema = "{\"type\":\"record\",\"name\":\"OldWidget\",\"namespace\":\"io.confluent.kafka.schemaregistry.rules.cel.CelExecutorTest\",\"fields\":\n"
        + "[{\"name\": \"name\", \"type\": \"string\"},\n"
        + "{\"name\": \"ssn\", \"type\": { \"type\": \"array\", \"items\": \"string\"}},\n"
        + "{\"name\": \"piiArray\", \"type\": { \"type\": \"array\", \"items\": { \"type\": \"record\", \"name\":\"OldPii\", \"fields\":\n"
        + "[{\"name\": \"pii\", \"type\": \"string\"}]}}},\n"
        + "{\"name\": \"piiMap\", \"type\": { \"type\": \"map\", \"values\": \"OldPii\"}}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private Schema createWidgetSchemaWithGuard() {
    String userSchema = "{\"type\":\"record\",\"name\":\"OldWidget\",\"namespace\":\"io.confluent.kafka.schemaregistry.rules.cel.CelExecutorTest\",\"fields\":\n"
        + "[{\"name\": \"name\", \"type\": \"string\"},\n"
        + "{\"name\": \"ssn\", \"type\": { \"type\": \"array\", \"items\": \"string\"}},\n"
        + "{\"name\": \"piiArray\", \"type\": { \"type\": \"array\", \"items\": { \"type\": \"record\", \"name\":\"OldPii\", \"fields\":\n"
        + "[{\"name\": \"pii\", \"type\": \"string\"}]}}},\n"
        + "{\"name\": \"piiMap\", \"type\": { \"type\": \"map\", \"values\": \"OldPii\"}},\n"
        + "{\"name\": \"size\", \"type\": \"int\"},{\"name\": \"version\", \"type\": \"int\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private Schema createFixedSchema() {
    String fixedSchema = "{\"name\": \"Fixed\",\n"
        + "   \"type\": \"fixed\",\n"
        + "  \"size\" : 4\n"
        + "}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(fixedSchema);
    return schema;
  }

  private Schema createComplexSchema() {
    return new Schema.Parser().parse(
        "{\"namespace\": \"namespace\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"test\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"null\", \"type\": \"null\"},\n"
            + "     {\"name\": \"boolean\", \"type\": \"boolean\"},\n"
            + "     {\"name\": \"int\", \"type\": \"int\"},\n"
            + "     {\"name\": \"long\", \"type\": \"long\"},\n"
            + "     {\"name\": \"float\", \"type\": \"float\"},\n"
            + "     {\"name\": \"double\", \"type\": \"double\"},\n"
            + "     {\"name\": \"bytes\", \"type\": \"bytes\"},\n"
            + "     {\"name\": \"string\", \"type\": \"string\", \"aliases\": [\"string_alias\"]},\n"
            + "     {\"name\": \"enum\",\n"
            + "       \"type\": {\n"
            + "         \"name\": \"Kind\",\n"
            + "         \"type\": \"enum\",\n"
            + "         \"symbols\" : [\"ONE\", \"TWO\", \"THREE\"]\n"
            + "       }\n"
            + "     },\n"
            + "     {\"name\": \"array\",\n"
            + "       \"type\": {\n"
            + "         \"type\": \"array\",\n"
            + "         \"items\" : \"string\"\n"
            + "       }\n"
            + "     },\n"
            + "     {\"name\": \"map\",\n"
            + "       \"type\": {\n"
            + "         \"type\": \"map\",\n"
            + "         \"values\" : \"string\"\n"
            + "       }\n"
            + "     },\n"
            + "     {\"name\": \"union\", \"type\": [\"null\", \"string\"]},\n"
            + "     {\"name\": \"fixed\",\n"
            + "       \"type\": {\n"
            + "         \"name\": \"Fixed\",\n"
            + "         \"type\": \"fixed\",\n"
            + "         \"size\" : 4\n"
            + "       }\n"
            + "     }\n"
            + "]\n"
            + "}");
  }

  private IndexedRecord createComplexRecord() {
    Schema enumSchema = createEnumSchema();
    Schema fixedSchema = createFixedSchema();
    Schema schema = createComplexSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("null", null);
    avroRecord.put("boolean", true);
    avroRecord.put("int", 1);
    avroRecord.put("long", 2L);
    avroRecord.put("float", 3.0f);
    avroRecord.put("double", 4.0d);
    avroRecord.put("bytes", ByteBuffer.wrap(new byte[]{0, 1, 2}));
    avroRecord.put("string", "testUser");
    avroRecord.put("enum", new GenericData.EnumSymbol(enumSchema, "ONE"));
    avroRecord.put("array", ImmutableList.of("hi", "there"));
    avroRecord.put("map", ImmutableMap.of("bye", "there"));
    avroRecord.put("union", "zap");
    avroRecord.put("fixed", new GenericData.Fixed(fixedSchema, new byte[]{0, 0, 0, 0}));
    return avroRecord;
  }

  @Test
  public void testKafkaAvroSerializer() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.READ,
        CelExecutor.TYPE, null, null, "message.name == \"testUser\" && message.kind == \"ONE\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
  }

  @Test
  public void testKafkaAvroSerializerFieldTransform() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null, "name == \"name\" ; value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    GenericRecord obj = (GenericRecord) avroDeserializer.deserialize(topic, bytes);
    assertEquals("testUser-suffix", obj.get("name").toString());
  }

  @Test
  public void testKafkaAvroSerializerFieldTransformComplex() throws Exception {
    IndexedRecord avroRecord = createComplexRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null, "name == \"string\" ; value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    GenericRecord obj = (GenericRecord) avroDeserializer.deserialize(topic, bytes);
    assertEquals("testUser-suffix", obj.get("string").toString());
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerConstraintException() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.READ,
        CelExecutor.TYPE, null, null, "message.name != \"testUser\" || message.kind != \"ONE\"",
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
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.READ,
        CelExecutor.TYPE, null, null, "message.name != \"testUser\" || message.kind != \"ONE\"",
        null, "NONE", false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
  }

  @Test
  public void testKafkaAvroSerializerConstraintDlq() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.READ,
        CelExecutor.TYPE, null, null, "message.name != \"testUser\" || message.kind != \"ONE\"",
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
  public void testKafkaAvroSerializerConstraintDlqWithKey() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    schemaRegistry.register(topic + "-key", avroSchema);

    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.READ,
        CelExecutor.TYPE, null, null, "message.name != \"testUser\" || message.kind != \"ONE\"",
        null, "DLQ", false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    try {
      byte[] keyBytes = avroKeySerializer.serialize(topic, avroRecord);
      byte[] valueBytes = avroSerializer.serialize(topic, avroRecord);
      avroKeyDeserializer.deserialize(topic, keyBytes);
      avroDeserializer.deserialize(topic, valueBytes);
      fail("Should send to DLQ and throw exception");
    } catch (SerializationException e) {
      // expected
    }

    ArgumentCaptor<ProducerRecord> argument = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer).send(argument.capture(), any(Callback.class));
    // Verify producer record has both key and value
    assertNotNull(argument.getValue().key());
    assertNotNull(argument.getValue().value());
  }

  @Test
  public void testKafkaAvroSerializerReflection() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    Schema schema = ReflectData.get().getSchema(widget.getClass());
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.READ,
        CelExecutor.TYPE, null, null, "message.name == \"alice\"", null, null, false);
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
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix\"",
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
  public void testKafkaAvroSerializerReflectionFieldTransformNoTags() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchemaNoTags();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null, "value + \"-suffix\"",
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
  public void testKafkaAvroSerializerReflectionFieldTransformWithGuard() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchemaWithGuard();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null, "typeName == 'STRING'; value + \"-suffix\"",
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
    assertEquals(123, ((OldWidget)obj).getSize());
  }

  @Test
  public void testKafkaAvroSerializerReflectionFieldTransformWithSameTag() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix\"",
        null, null, false);
    Rule rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix2\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule, rule2));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object should be a Widget",
        OldWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
    assertEquals("alice-suffix2", ((OldWidget)obj).getName());
    assertEquals("123-suffix2", ((OldWidget)obj).getSsn().get(0));
    assertEquals("456-suffix2", ((OldWidget)obj).getSsn().get(1));
    assertEquals("789-suffix2", ((OldWidget)obj).getPiiArray().get(0).getPii());
    assertEquals("012-suffix2", ((OldWidget)obj).getPiiArray().get(1).getPii());
    assertEquals("345-suffix2", ((OldWidget)obj).getPiiMap().get("key1").getPii());
    assertEquals("678-suffix2", ((OldWidget)obj).getPiiMap().get("key2").getPii());
  }

  @Test
  public void testKafkaAvroSerializerNewMapTransform() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelExecutor.TYPE, null, null, "{'name': 'Bob', 'kind': 'TWO'}",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    GenericRecord obj = (GenericRecord) avroDeserializer.deserialize(topic, bytes);
    assertEquals("Bob", obj.get("name").toString());
    assertEquals("TWO", obj.get("kind").toString());
  }

  @Test
  public void testKafkaAvroSerializerIdentityTransform() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelExecutor.TYPE, null, null, "message",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    GenericRecord obj = (GenericRecord) avroDeserializer.deserialize(topic, bytes);
    assertEquals(avroRecord, obj);
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerBadTransform() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelExecutor.TYPE, null, null, "message.name == \"testUser\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    avroSerializer.serialize(topic, avroRecord);
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerNullTransform() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelExecutor.TYPE, null, null, "null",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    avroSerializer.serialize(topic, avroRecord);
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
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix\"",
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
  public void testKafkaProtobufSerializerFieldTransformWithRef() throws Exception {
    byte[] bytes;
    Object obj;

    schemaRegistry.register("Pii.proto", new ProtobufSchema(PiiProto.Pii.getDescriptor()).copy(1));

    WidgetWithRef widget = WidgetWithRef.newBuilder()
        .setName("alice")
        .addSsn("123")
        .addSsn("456")
        .addPiiArray(PiiProto.Pii.newBuilder().setPii("789").build())
        .addPiiArray(PiiProto.Pii.newBuilder().setPii("012").build())
        .putPiiMap("key1", PiiProto.Pii.newBuilder().setPii("345").build())
        .putPiiMap("key2", PiiProto.Pii.newBuilder().setPii("678").build())
        .setSize(123)
        .build();
    List<SchemaReference> refs =
        Collections.singletonList(new SchemaReference("Pii.proto", "Pii.proto", 1));
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType(), refs);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    protobufSchema = protobufSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobufWithRefSerializer.serialize(topic, widget);

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
  public void testKafkaProtobufSerializerNewMessageTransform() throws Exception {
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
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelExecutor.TYPE, null, null, "io.confluent.kafka.schemaregistry.rules.Widget{ name: \"Bob\" }",
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
        "Bob",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
  }

  @Test
  public void testKafkaProtobufSerializerIdentityTransform() throws Exception {
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
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelExecutor.TYPE, null, null, "message",
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
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix\"",
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
  public void testKafkaJsonSchemaSerializerFieldTransformWithRef() throws Exception {
    byte[] bytes;
    Object obj;

    String refStr = "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{"
        + "\"pii\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]}}}";
    schemaRegistry.register("OldPii.json", new JsonSchema(refStr).copy(1));

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"ssn\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"type\":\"string\"}}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"piiArray\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"$ref\":\"OldPii.json\"}}]},"
        + "\"piiMap\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"object\",\"additionalProperties\":{\"$ref\":\"OldPii.json\"}}]},"
        + "\"size\":{\"type\":\"integer\"},"
        + "\"version\":{\"type\":\"integer\"}},"
        + "\"required\":[\"size\",\"version\"]}";
    List<SchemaReference> refs =
        Collections.singletonList(new SchemaReference("OldPii.json", "OldPii.json", 1));
    Map<String, String> resolvedRefs = Collections.singletonMap("OldPii.json", refStr);
    JsonSchema jsonSchema = new JsonSchema(schemaStr, refs, resolvedRefs, null);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix\"",
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
  public void testKafkaJsonSchemaSerializerAnnotatedFieldTransform() throws Exception {
    byte[] bytes;
    Object obj;

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
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix\"",
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

  @Test
  public void testKafkaJsonSchemaSerializerNewMapTransform() throws Exception {
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
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelExecutor.TYPE, null, null, "{'name': 'Bob'}",
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
        "Bob",
        ((JsonNode)obj).get("name").textValue()
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
}
