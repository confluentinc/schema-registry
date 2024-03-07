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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.DlqAction;
import io.confluent.kafka.schemaregistry.rules.PiiProto;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Kind;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Pii;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Widget;
import io.confluent.kafka.schemaregistry.rules.WidgetProto2;
import io.confluent.kafka.schemaregistry.rules.WidgetProto2.Widget2;
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
  private final KafkaProtobufSerializer<Widget2> protobuf2Serializer;
  private final KafkaProtobufSerializer<WidgetWithRef> protobufWithRefSerializer;
  private final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  private final KafkaJsonSchemaSerializer<OldWidget> jsonSchemaSerializer;
  private final KafkaJsonSchemaSerializer<AnnotatedOldWidget> jsonSchemaSerializer2;
  private final KafkaJsonSchemaSerializer<JsonNode> jsonSchemaSerializer3;
  private final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;
  private final String topic;
  private final KafkaProducer<byte[], byte[]> producer;
  private final KafkaProducer<byte[], byte[]> producer2;

  private static final ObjectMapper mapper = new ObjectMapper();

  public CelExecutorTest() {
    topic = "test";
    schemaRegistry = new MockSchemaRegistryClient(ImmutableList.of(
        new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()));
    producer = mock(KafkaProducer.class);
    when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(
        CompletableFuture.completedFuture(null));
    producer2 = mock(KafkaProducer.class);
    when(producer2.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(
        CompletableFuture.completedFuture(null));

    Map<String, Object> defaultConfig = new HashMap<>();
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_SIZE, "0");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, "cel,cel-field");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".cel.class",
        CelExecutor.class.getName());
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".cel-field.class",
        CelFieldExecutor.class.getName());
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS, "cel,cel_field");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".cel.class",
        DlqAction.class.getName());
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".cel.param." + DlqAction.DLQ_TOPIC,
        "dlq-topic");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".cel.param." + DlqAction.PRODUCER,
        producer);
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".cel.param." + DlqAction.DLQ_AUTO_FLUSH,
        true);
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".cel_field.class",
        DlqAction.class.getName());
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".cel_field.param." + DlqAction.DLQ_TOPIC,
        "dlq-topic2");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".cel_field.param." + DlqAction.PRODUCER,
        producer2);
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".cel_field.param." + DlqAction.DLQ_AUTO_FLUSH,
        true);
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
    protobuf2Serializer = new KafkaProtobufSerializer<>(schemaRegistry, defaultConfig);
    protobufWithRefSerializer = new KafkaProtobufSerializer<>(schemaRegistry, defaultConfig);
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, defaultConfig);

    jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, defaultConfig);
    jsonSchemaSerializer2 = new KafkaJsonSchemaSerializer<>(schemaRegistry, defaultConfig);
    jsonSchemaSerializer3 = new KafkaJsonSchemaSerializer<>(schemaRegistry, defaultConfig);
    jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry, defaultConfig);
  }

  private Schema createEnumSchema() {
    String enumSchema = "{\"name\": \"Kind\",\"namespace\": \"example.avro\",\n"
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
        + "\"fields\": ["
        + "{\"name\": \"name\", \"type\": \"string\"}, "
        + "{\"name\": \"lastName\", \"type\": [\"null\", \"string\"]}, "
        + "{\"name\": \"fullName\", \"type\": [\"null\", \"string\"]}, "
        + "{\"name\": \"mybytes\", \"type\": \"bytes\"}, "
        + "{\"name\": \"myint\", \"type\": \"int\"}, "
        + "{\"name\": \"mylong\", \"type\": \"long\"}, "
        + "{\"name\": \"myfloat\", \"type\": \"float\"}, "
        + "{\"name\": \"mydouble\", \"type\": \"double\"}, "
        + "{\"name\": \"myboolean\", \"type\": \"boolean\"}, "
        + "{\"name\": \"mynull\", \"type\": \"null\"}, "
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
    return createUserRecord("testUser");
  }

  private IndexedRecord createUserRecord(String name) {
    Schema enumSchema = createEnumSchema();
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", name);
    avroRecord.put("mybytes", ByteBuffer.wrap(new byte[] { 0 }));
    avroRecord.put("myint", 1);
    avroRecord.put("mylong", 2L);
    avroRecord.put("myfloat", 3.0f);
    avroRecord.put("mydouble", 4.0d);
    avroRecord.put("myboolean", true);
    avroRecord.put("kind", new GenericData.EnumSymbol(enumSchema, "ONE"));
    return avroRecord;
  }

  private Schema createWidgetSchema() {
    String userSchema = "{\"type\":\"record\",\"name\":\"OldWidget\",\"namespace\":\"io.confluent.kafka.schemaregistry.rules.cel.CelExecutorTest\",\"fields\":\n"
        + "[{\"name\": \"name\", \"type\": \"string\",\"confluent:tags\": [\"PII\"]},\n"
        + "{\"name\": \"lastName\", \"type\": \"string\"},\n"
        + "{\"name\": \"fullName\", \"type\": \"string\"},\n"
        + "{\"name\": \"myint\", \"type\": \"int\"}, "
        + "{\"name\": \"mylong\", \"type\": \"long\"}, "
        + "{\"name\": \"myfloat\", \"type\": \"float\"}, "
        + "{\"name\": \"mydouble\", \"type\": \"double\"}, "
        + "{\"name\": \"myboolean\", \"type\": \"boolean\"}, "
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
        + "{\"name\": \"lastName\", \"type\": \"string\"},\n"
        + "{\"name\": \"fullName\", \"type\": \"string\"},\n"
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
        + "{\"name\": \"lastName\", \"type\": \"string\"},\n"
        + "{\"name\": \"fullName\", \"type\": \"string\"},\n"
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
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name == \"testUser\" && size(message.name) == 8 && message.kind == \"ONE\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
  }

  @Test
  public void testKafkaAvroSerializerFieldConstraint() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null, "name == 'name' ; value == \"testUser\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
  }

  @Test(expected = SerializationException.class)
  public void testKafkaAvroSerializerFieldConstraintException() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null, "name == 'name' ; value != \"testUser\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    avroSerializer.serialize(topic, avroRecord);
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
  public void testKafkaAvroSerializerFieldTransformExternalTag() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    ((GenericRecord)avroRecord).put("lastName", "smith");
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "name == \"lastName\" ; value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    Metadata metadata = new Metadata(Collections.singletonMap(
        "example.avro.User.lastName", ImmutableSet.of("PII")), null, null);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    GenericRecord obj = (GenericRecord) avroDeserializer.deserialize(topic, bytes);
    assertEquals("smith-suffix", obj.get("lastName").toString());
  }

  @Test
  public void testKafkaAvroSerializerFieldTransformUsingMessage() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    ((GenericRecord) avroRecord).put("lastName", "smith");
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    List<Rule> rules = new ArrayList<>();
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"fullName\" ; dyn(value) == null ? message.name + \" \" + message.lastName : dyn(value)",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mybytes\" ; value == b\"\\x00\" ? b\"\\x01\" : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule3", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myint\" ; value == 1 ? 2 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule4", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mylong\" ; value == 2 ? 3 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule5", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myfloat\" ; value == 3.0 ? 4.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule6", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mydouble\" ; value == 4.0 ? 5.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule7", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myboolean\" ; value == true ? false : value",
        null, null, false);
    rules.add(rule);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), rules);
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    GenericRecord obj = (GenericRecord) avroDeserializer.deserialize(topic, bytes);
    assertEquals("testUser smith", obj.get("fullName").toString());
    assertArrayEquals(new byte[] {1}, ((ByteBuffer)obj.get("mybytes")).array());
    assertEquals(2, ((Integer)obj.get("myint")).intValue());
    assertEquals(3L, ((Long)obj.get("mylong")).longValue());
    assertEquals(4f, ((Float)obj.get("myfloat")).floatValue(), 0.1);
    assertEquals(5d, ((Double)obj.get("mydouble")).doubleValue(), 0.1);
    assertFalse(((Boolean)obj.get("myboolean")).booleanValue());
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
  public void testKafkaAvroSerializerConstraintGuard() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.READ,
        CelExecutor.TYPE, null, null, "has(message.foo) ; message.name != \"testUser\" || message.kind != \"ONE\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
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
    verifyNoInteractions(producer2);
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
    verifyNoInteractions(producer2);
  }

  @Test
  public void testKafkaAvroSerializerReflection() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("");
    widget.setFullName("");
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
        "Returned object does not match",
        OldWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
  }

  @Test
  public void testKafkaAvroSerializerReflectionFieldTransform() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("");
    widget.setFullName("");
    widget.setMyint(1);
    widget.setMylong(2L);
    widget.setMyfloat(3.0f);
    widget.setMydouble(4.0d);
    widget.setMyboolean(true);
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
        "Returned object does not match",
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
  public void testKafkaAvroSerializerReflectionFieldTransformUsingMessage() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("smith");
    widget.setMyint(1);
    widget.setMylong(2L);
    widget.setMyfloat(3.0f);
    widget.setMydouble(4.0d);
    widget.setMyboolean(true);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    List<Rule> rules = new ArrayList<>();
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"fullName\" ; dyn(value) == null ? message.name + \" \" + message.lastName : dyn(value)",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mybytes\" ; value == b\"\\x00\" ? b\"\\x01\" : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule3", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myint\" ; value == 1 ? 2 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule4", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mylong\" ; value == 2 ? 3 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule5", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myfloat\" ; value == 3.0 ? 4.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule6", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mydouble\" ; value == 4.0 ? 5.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule7", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myboolean\" ; value == true ? false : value",
        null, null, false);
    rules.add(rule);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), rules);
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object does not match",
        OldWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
    assertEquals("alice", ((OldWidget)obj).getName());
    assertEquals("alice smith", ((OldWidget)obj).getFullName());
    assertEquals(2, ((OldWidget)obj).getMyint());
    assertEquals(3L, ((OldWidget)obj).getMylong());
    assertEquals(4f, ((OldWidget)obj).getMyfloat(), 0.1);
    assertEquals(5d, ((OldWidget)obj).getMydouble(), 0.1);
    assertFalse(((OldWidget)obj).isMyboolean());
    assertEquals("123", ((OldWidget)obj).getSsn().get(0));
    assertEquals("456", ((OldWidget)obj).getSsn().get(1));
    assertEquals("789", ((OldWidget)obj).getPiiArray().get(0).getPii());
    assertEquals("012", ((OldWidget)obj).getPiiArray().get(1).getPii());
    assertEquals("345", ((OldWidget)obj).getPiiMap().get("key1").getPii());
    assertEquals("678", ((OldWidget)obj).getPiiMap().get("key2").getPii());
  }

  @Test
  public void testKafkaAvroSerializerReflectionFieldTransformIgnoreGuardSeparator() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("");
    widget.setFullName("");
    widget.setMyint(1);
    widget.setMylong(2L);
    widget.setMyfloat(3.0f);
    widget.setMydouble(4.0d);
    widget.setMyboolean(true);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("cel.ignore.guard.separator", "true"), "value + \"-suffix;\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object does not match",
        OldWidget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
    assertEquals("alice-suffix;", ((OldWidget)obj).getName());
    assertEquals("123-suffix;", ((OldWidget)obj).getSsn().get(0));
    assertEquals("456-suffix;", ((OldWidget)obj).getSsn().get(1));
    assertEquals("789-suffix;", ((OldWidget)obj).getPiiArray().get(0).getPii());
    assertEquals("012-suffix;", ((OldWidget)obj).getPiiArray().get(1).getPii());
    assertEquals("345-suffix;", ((OldWidget)obj).getPiiMap().get("key1").getPii());
    assertEquals("678-suffix;", ((OldWidget)obj).getPiiMap().get("key2").getPii());
  }

  @Test
  public void testKafkaAvroSerializerReflectionFieldTransformNoTags() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("");
    widget.setFullName("");
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
        "Returned object does not match",
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
    widget.setLastName("");
    widget.setFullName("");
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
        "Returned object does not match",
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
  public void testKafkaAvroSerializerReflectionFieldTransformWithTagGuard() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("");
    widget.setFullName("");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null, "tags.exists_one(x, x == 'PII') ; value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object does not match",
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
    widget.setLastName("");
    widget.setFullName("");
    widget.setMyint(1);
    widget.setMylong(2L);
    widget.setMyfloat(3.0f);
    widget.setMydouble(4.0d);
    widget.setMyboolean(true);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    widget.setPiiMap(ImmutableMap.of("key1", new OldPii("345"), "key2", new OldPii("678")));
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix2\"",
        null, null, false);
    Rule rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, ImmutableSortedSet.of("PII"), null, "value + \"-suffix\"",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule, rule2));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object does not match",
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
        CelExecutor.TYPE, null, null,
        "{'name': 'Bob', 'lastName': null, 'fullName': null, 'mybytes': b\"\\x00\", "
            + "'myint': 1, 'mylong': 2, 'myfloat': 3, 'mydouble': 4, "
            + "'myboolean': true, 'mynull': null, 'kind': 'TWO'}",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    GenericRecord obj = (GenericRecord) avroDeserializer.deserialize(topic, bytes);
    assertEquals("Bob", obj.get("name").toString());
    assertNull(obj.get("lastName"));
    assertNull(obj.get("fullName"));
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
  public void testKafkaAvroSerializerStringFunctions() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name.charAt(0) == 't'"
            + " && message.name.indexOf('e') == 1"
            + " && [message.name,'1'].join() == 'testUser1'"
            + " && message.name.lastIndexOf('e') == 6"
            + " && message.name.lowerAscii() == 'testuser'"
            + " && message.name.replace('User','Customer') == 'testCustomer'"
            + " && message.name.split('U') == ['test', 'ser']"
            + " && ' testUser '.trim() == 'testUser'"
            + " && message.name.upperAscii() == 'TESTUSER'",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
  }

  @Test
  public void testKafkaAvroSerializerBuiltinFunctions() throws Exception {
    IndexedRecord avroRecord = createUserRecord("bob@confluent.com");
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name.isEmail()",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));

    IndexedRecord avroRecord2 = createUserRecord("bob.@confluent.com");
    assertThrows(SerializationException.class, () -> avroSerializer.serialize(topic, avroRecord2));

    avroRecord = createUserRecord("localhost");
    rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name.isHostname()",
        null, null, false);
    ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));

    IndexedRecord avroRecord3 = createUserRecord("local_host");
    assertThrows(SerializationException.class, () -> avroSerializer.serialize(topic, avroRecord3));

    avroRecord = createUserRecord("127.0.0.1");
    rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name.isIpv4()",
        null, null, false);
    ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));

    IndexedRecord avroRecord4 = createUserRecord("foo");
    assertThrows(SerializationException.class, () -> avroSerializer.serialize(topic, avroRecord4));

    avroRecord = createUserRecord("2001:db8:85a3:0:0:8a2e:370:7334");
    rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name.isIpv6()",
        null, null, false);
    ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));

    IndexedRecord avroRecord5 = createUserRecord("foo");
    assertThrows(SerializationException.class, () -> avroSerializer.serialize(topic, avroRecord5));

    avroRecord = createUserRecord("http://confluent.com/index.html");
    rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name.isUri()",
        null, null, false);
    ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));

    IndexedRecord avroRecord6 = createUserRecord("foo");
    assertThrows(SerializationException.class, () -> avroSerializer.serialize(topic, avroRecord6));

    avroRecord = createUserRecord("/index.html");
    rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name.isUriRef()",
        null, null, false);
    ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));

    IndexedRecord avroRecord7 = createUserRecord("::foo");
    assertThrows(SerializationException.class, () -> avroSerializer.serialize(topic, avroRecord7));

    avroRecord = createUserRecord("fa02a430-892f-4160-97cd-6e3d1bc14494");
    rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name.isUuid()",
        null, null, false);
    ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));

    IndexedRecord avroRecord8 = createUserRecord("::foo");
    assertThrows(SerializationException.class, () -> avroSerializer.serialize(topic, avroRecord8));
  }

  @Test
  public void testKafkaProtobufSerializer() throws Exception {
    byte[] bytes;
    Object obj;

    Widget widget = Widget.newBuilder()
        .setName("alice")
        .setKind(Kind.ONE)
        .addSsn("123")
        .addSsn("456")
        .addPiiArray(Pii.newBuilder().setPii("789").build())
        .addPiiArray(Pii.newBuilder().setPii("012").build())
        .putPiiMap("key1", Pii.newBuilder().setPii("345").build())
        .putPiiMap("key2", Pii.newBuilder().setPii("678").build())
        .setSize(123)
        .build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name == \"alice\" && size(message.name) == 5 && message.kind == 1",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));

    protobufSchema = protobufSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobufSerializer.serialize(topic, widget);
    obj = protobufDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object does not match",
        "alice",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
  }
  @Test
  public void testKafkaProtobuf2Serializer() throws Exception {
    byte[] bytes;
    Object obj;

    Widget2 widget = Widget2.newBuilder()
        .setName("alice")
        .setKind(WidgetProto2.Kind.ONE)
        .addSsn("123")
        .addSsn("456")
        .addPiiArray(WidgetProto2.Pii.newBuilder().setPii("789").build())
        .addPiiArray(WidgetProto2.Pii.newBuilder().setPii("012").build())
        .putPiiMap("key1", WidgetProto2.Pii.newBuilder().setPii("345").build())
        .putPiiMap("key2", WidgetProto2.Pii.newBuilder().setPii("678").build())
        .setSize(123)
        .build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name == \"alice\" && size(message.name) == 5 && message.kind == 1",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));

    protobufSchema = protobufSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobuf2Serializer.serialize(topic, widget);
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
  }

  @Test(expected = SerializationException.class)
  public void testKafkaProtobufSerializerFieldConstraintException() throws Exception {
    byte[] bytes;
    Object obj;

    Widget widget = Widget.newBuilder()
        .setName("alice")
        .setKind(Kind.ONE)
        .addSsn("123")
        .addSsn("456")
        .addPiiArray(Pii.newBuilder().setPii("789").build())
        .addPiiArray(Pii.newBuilder().setPii("012").build())
        .putPiiMap("key1", Pii.newBuilder().setPii("345").build())
        .putPiiMap("key2", Pii.newBuilder().setPii("678").build())
        .setSize(123)
        .build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == 'name' ; value != 'alice'",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));

    protobufSchema = protobufSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobufSerializer.serialize(topic, widget);
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
        "Returned object does not match",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object does not match",
        "alice-suffix",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
    assertEquals(
        "Returned object does not match",
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
        "Returned object does not match",
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
        "Returned object does not match",
        ImmutableList.of("345-suffix", "678-suffix"),
        ssnMapValues
    );
  }

  @Test
  public void testKafkaProtobufSerializerFieldTransformExternalTag() throws Exception {
    byte[] bytes;
    Object obj;

    Widget widget = Widget.newBuilder()
        .setName("alice")
        .setLastName("smith")
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
    Metadata metadata = new Metadata(Collections.singletonMap(
        "io.confluent.kafka.schemaregistry.rules.Widget.lastName", ImmutableSet.of("PII")), null, null);
    protobufSchema = protobufSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobufSerializer.serialize(topic, widget);

    obj = protobufDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object does not match",
        "alice-suffix",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
    assertEquals(
        "Returned object does not match",
        "smith-suffix",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("lastName"))
    );
    assertEquals(
        "Returned object does not match",
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
        "Returned object does not match",
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
        "Returned object does not match",
        ImmutableList.of("345-suffix", "678-suffix"),
        ssnMapValues
    );
  }

  @Test
  public void testKafkaProtobufSerializerFieldTransformUsingMessage() throws Exception {
    byte[] bytes;
    Object obj;

    Widget widget = Widget.newBuilder()
        .setName("alice")
        .setLastName("smith")
        .setMybytes(ByteString.copyFrom(new byte[]{0}))
        .setMyint(1)
        .setMylong(2)
        .setMyfloat(3f)
        .setMydouble(4d)
        .setMyboolean(true)
        .addSsn("123")
        .addSsn("456")
        .addPiiArray(Pii.newBuilder().setPii("789").build())
        .addPiiArray(Pii.newBuilder().setPii("012").build())
        .putPiiMap("key1", Pii.newBuilder().setPii("345").build())
        .putPiiMap("key2", Pii.newBuilder().setPii("678").build())
        .setSize(123)
        .build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    List<Rule> rules = new ArrayList<>();
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"fullName\" ; value == \"\" ? message.name + \" \" + message.lastName : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mybytes\" ; value == b\"\\x00\" ? b\"\\x01\" : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule3", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myint\" ; value == 1 ? 2 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule4", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mylong\" ; value == 2 ? 3 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule5", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myfloat\" ; value == 3.0 ? 4.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule6", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mydouble\" ; value == 4.0 ? 5.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule7", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myboolean\" ; value == true ? false : value",
        null, null, false);
    rules.add(rule);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), rules);
    protobufSchema = protobufSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobufSerializer.serialize(topic, widget);

    obj = protobufDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object does not match",
        "alice",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
    assertEquals(
        "Returned object does not match",
        "alice smith",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("fullName"))
    );
    assertEquals(
        "Returned object does not match",
        ByteString.copyFrom(new byte[]{1}),
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("mybytes"))
    );
    assertEquals(
        "Returned object does not match",
        2,
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("myint"))
    );
    assertEquals(
        "Returned object does not match",
        3L,
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("mylong"))
    );
    assertEquals(
        "Returned object does not match",
        4f,
        (Float) ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("myfloat")),
        0.1
    );
    assertEquals(
        "Returned object does not match",
        5d,
        (Double) ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("mydouble")),
        0.1
    );
    assertFalse(
        "Returned object does not match",
        (Boolean) ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("myboolean"))
    );
    assertEquals(
        "Returned object does not match",
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
        "Returned object does not match",
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
        "Returned object does not match",
        ImmutableList.of("345", "678"),
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
        "Returned object does not match",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object does not match",
        "alice-suffix",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
    assertEquals(
        "Returned object does not match",
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
        "Returned object does not match",
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
        "Returned object does not match",
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
        "Returned object does not match",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object does not match",
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
        "Returned object does not match",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object does not match",
        "alice",
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("name"))
    );
    assertEquals(
        "Returned object does not match",
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
        "Returned object does not match",
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
        "Returned object does not match",
        ImmutableList.of("345", "678"),
        ssnMapValues
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializer() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name == \"alice\" && size(message.name) == 5",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    jsonSchema = jsonSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer.serialize(topic, widget);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice",
        ((JsonNode)obj).get("name").textValue()
    );
  }

  @Test(expected = SerializationException.class)
  public void testKafkaJsonSchemaSerializerFieldConstraintException() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == 'name' ; value != 'alice'",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    jsonSchema = jsonSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer.serialize(topic, widget);
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
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice-suffix",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "123-suffix",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "456-suffix",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "789-suffix",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "012-suffix",
        ((JsonNode)obj).get("piiArray").get(1).get("pii").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerFieldTransformExternalTag() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("smith");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
    Metadata metadata = new Metadata(Collections.singletonMap(
        "$.lastName", ImmutableSet.of("PII")), null, null);
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer.serialize(topic, widget);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice-suffix",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "smith-suffix",
        ((JsonNode)obj).get("lastName").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "123-suffix",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "456-suffix",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "789-suffix",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "012-suffix",
        ((JsonNode)obj).get("piiArray").get(1).get("pii").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerFieldTransformUsingMessage() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("smith");
    widget.setMyint(1);
    widget.setMylong(2L);
    widget.setMyfloat(3.0f);
    widget.setMydouble(4.0d);
    widget.setMyboolean(true);
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
    List<Rule> rules = new ArrayList<>();
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"fullName\" ; dyn(value) == null ? message.name + \" \" + message.lastName : dyn(value)",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule3", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myint\" ; value == 1 ? 2 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule4", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mylong\" ; value == 2 ? 3 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule5", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myfloat\" ; value == 3.0 ? 4.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule6", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mydouble\" ; value == 4.0 ? 5.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule7", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myboolean\" ; value == true ? false : value",
        null, null, false);
    rules.add(rule);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), rules);
    jsonSchema = jsonSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer.serialize(topic, widget);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "alice smith",
        ((JsonNode)obj).get("fullName").textValue()
    );
    assertEquals(
        "Returned object does not match",
        2,
        ((JsonNode)obj).get("myint").intValue()
    );
    assertEquals(
        "Returned object does not match",
        3L,
        ((JsonNode)obj).get("mylong").longValue()
    );
    assertEquals(
        "Returned object does not match",
        4f,
        ((JsonNode)obj).get("myfloat").floatValue(),
        0.1
    );
    assertEquals(
        "Returned object does not match",
        5d,
        ((JsonNode)obj).get("mydouble").doubleValue(),
        0.1
    );
    assertFalse(
        "Returned object does not match",
        ((JsonNode)obj).get("myboolean").booleanValue()
    );
    assertEquals(
        "Returned object does not match",
        "123",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "456",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "789",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "012",
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
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice-suffix",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "123-suffix",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "456-suffix",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "789-suffix",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object does not match",
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
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice-suffix",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "123-suffix",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "456-suffix",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "789-suffix",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object does not match",
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
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "Bob",
        ((JsonNode)obj).get("name").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerIdentityTransform() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
        CelExecutor.TYPE, null, null, "message",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    jsonSchema = jsonSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer.serialize(topic, widget);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice",
        ((JsonNode)obj).get("name").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerJsonNode() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null,
        "message.name == \"alice\" && size(message.name) == 5",
        null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    jsonSchema = jsonSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode jsonNode = mapper.valueToTree(widget);
    ObjectNode objectNode = JsonSchemaUtils.envelope(jsonSchema, jsonNode);
    bytes = jsonSchemaSerializer3.serialize(topic, objectNode);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice",
        ((JsonNode)obj).get("name").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerJsonNodeFieldTransform() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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

    JsonNode jsonNode = mapper.valueToTree(widget);
    ObjectNode objectNode = JsonSchemaUtils.envelope(jsonSchema, jsonNode);
    bytes = jsonSchemaSerializer3.serialize(topic, objectNode);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice-suffix",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "123-suffix",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "456-suffix",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "789-suffix",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "012-suffix",
        ((JsonNode)obj).get("piiArray").get(1).get("pii").textValue()
    );
  }

  @Test
  public void testKafkaJsonSchemaSerializerJsonNodeFieldTransformUsingMessage() throws Exception {
    byte[] bytes;
    Object obj;

    OldWidget widget = new OldWidget("alice");
    widget.setLastName("smith");
    widget.setMyint(1);
    widget.setMylong(2L);
    widget.setMyfloat(3.0f);
    widget.setMydouble(4.0d);
    widget.setMyboolean(true);
    widget.setSize(123);
    widget.setSsn(ImmutableList.of("123", "456"));
    widget.setPiiArray(ImmutableList.of(new OldPii("789"), new OldPii("012")));
    String schemaStr = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Old Widget\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
        + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}],"
        + "\"confluent:tags\": [ \"PII\" ]},"
        + "\"lastName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"fullName\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
        + "\"myint\":{\"type\":\"integer\"},"
        + "\"mylong\":{\"type\":\"integer\"},"
        + "\"myfloat\":{\"type\":\"number\"},"
        + "\"mydouble\":{\"type\":\"number\"},"
        + "\"myboolean\":{\"type\":\"boolean\"},"
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
    List<Rule> rules = new ArrayList<>();
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"fullName\" ; dyn(value) == null ? message.name + \" \" + message.lastName : dyn(value)",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule3", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myint\" ; value == 1 ? 2 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule4", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mylong\" ; value == 2 ? 3 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule5", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myfloat\" ; value == 3.0 ? 4.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule6", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"mydouble\" ; value == 4.0 ? 5.0 : value",
        null, null, false);
    rules.add(rule);
    rule = new Rule("myRule7", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, null,
        "name == \"myboolean\" ; value == true ? false : value",
        null, null, false);
    rules.add(rule);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), rules);
    jsonSchema = jsonSchema.copy(null, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode jsonNode = mapper.valueToTree(widget);
    ObjectNode objectNode = JsonSchemaUtils.envelope(jsonSchema, jsonNode);
    bytes = jsonSchemaSerializer3.serialize(topic, objectNode);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object does not match",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object does not match",
        "alice",
        ((JsonNode)obj).get("name").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "alice smith",
        ((JsonNode)obj).get("fullName").textValue()
    );
    assertEquals(
        "Returned object does not match",
        2,
        ((JsonNode)obj).get("myint").intValue()
    );
    assertEquals(
        "Returned object does not match",
        3L,
        ((JsonNode)obj).get("mylong").longValue()
    );
    assertEquals(
        "Returned object does not match",
        4f,
        ((JsonNode)obj).get("myfloat").floatValue(),
        0.1
    );
    assertEquals(
        "Returned object does not match",
        5d,
        ((JsonNode)obj).get("mydouble").doubleValue(),
        0.1
    );
    assertFalse(
        "Returned object does not match",
        ((JsonNode)obj).get("myboolean").booleanValue()
    );
    assertEquals(
        "Returned object does not match",
        "123",
        ((JsonNode)obj).get("ssn").get(0).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "456",
        ((JsonNode)obj).get("ssn").get(1).textValue()
    );
    assertEquals(
        "Returned object does not match",
        "789",
        ((JsonNode)obj).get("piiArray").get(0).get("pii").textValue()
    );
    assertEquals(
        "Returned object does not match",
        "012",
        ((JsonNode)obj).get("piiArray").get(1).get("pii").textValue()
    );
  }

  public static class OldWidget {
    private String name;
    private String lastName;
    private String fullName;
    private int myint;
    private long mylong;
    private float myfloat;
    private double mydouble;
    private boolean myboolean;
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

    public String getLastName() {
      return lastName;
    }

    public void setLastName(String lastName) {
      this.lastName = lastName;
    }

    public String getFullName() {
      return fullName;
    }

    public void setFullName(String fullName) {
      this.fullName = fullName;
    }

    public int getMyint() {
      return myint;
    }

    public void setMyint(int myint) {
      this.myint = myint;
    }

    public long getMylong() {
      return mylong;
    }

    public void setMylong(long mylong) {
      this.mylong = mylong;
    }

    public float getMyfloat() {
      return myfloat;
    }

    public void setMyfloat(float myfloat) {
      this.myfloat = myfloat;
    }

    public double getMydouble() {
      return mydouble;
    }

    public void setMydouble(double mydouble) {
      this.mydouble = mydouble;
    }

    public boolean isMyboolean() {
      return myboolean;
    }

    public void setMyboolean(boolean myboolean) {
      this.myboolean = myboolean;
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
