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

package io.confluent.kafka.schemaregistry.rules.jsonata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
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
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.NewSpecificWidget;
import io.confluent.kafka.schemaregistry.rules.NewWidgetProto;
import io.confluent.kafka.schemaregistry.rules.WidgetProto;
import io.confluent.kafka.schemaregistry.rules.WidgetProto.Widget;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.junit.Test;

public class JsonataExecutorTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroSerializer reflectionAvroSerializer2;
  private final KafkaAvroSerializer reflectionAvroSerializer3;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final KafkaAvroDeserializer reflectionAvroDeserializer2;
  private final KafkaAvroDeserializer reflectionAvroDeserializer3;
  private final KafkaAvroDeserializer specificAvroDeserializer;
  private final KafkaProtobufSerializer<WidgetProto.Widget> protobufSerializer;
  private final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  private final KafkaProtobufDeserializer<NewWidgetProto.NewWidget> specificProtobufDeserializer;
  private final KafkaJsonSchemaSerializer<OldWidget> jsonSchemaSerializer;
  private final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;
  private final KafkaJsonSchemaDeserializer<NewWidget> specificJsonSchemaDeserializer;
  private final String topic;

  public JsonataExecutorTest() {
    topic = "test";
    schemaRegistry = new MockSchemaRegistryClient(ImmutableList.of(
        new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()));

    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, "false");
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v1");
    defaultConfig.put(KafkaAvroSerializerConfig.LATEST_COMPATIBILITY_STRICT, "false");
    defaultConfig.put(KafkaAvroSerializerConfig.RULE_EXECUTORS, "jsonata");
    defaultConfig.put(KafkaAvroSerializerConfig.RULE_EXECUTORS + ".jsonata.class",
        JsonataExecutor.class.getName());
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v2");
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, new HashMap(defaultConfig));

    HashMap<String, String> reflectionProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    reflectionProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "false");
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v1");
    reflectionProps.put(KafkaAvroDeserializerConfig.LATEST_COMPATIBILITY_STRICT, "false");
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS, "jsonata");
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".jsonata.class",
        JsonataExecutor.class.getName());
    reflectionAvroSerializer = new KafkaAvroSerializer(schemaRegistry, reflectionProps);
    reflectionAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, reflectionProps);
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v2");
    reflectionAvroSerializer2 = new KafkaAvroSerializer(schemaRegistry, reflectionProps);
    reflectionAvroDeserializer2 = new KafkaAvroDeserializer(schemaRegistry, reflectionProps);
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v3");
    reflectionAvroSerializer3 = new KafkaAvroSerializer(schemaRegistry, reflectionProps);
    reflectionAvroDeserializer3 = new KafkaAvroDeserializer(schemaRegistry, reflectionProps);

    HashMap<String, String> specificProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    specificProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    specificProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    specificProps.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "false");
    specificProps.put(KafkaAvroDeserializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v2");
    specificProps.put(KafkaAvroDeserializerConfig.LATEST_COMPATIBILITY_STRICT, "false");
    specificProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    specificProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS, "jsonata");
    specificProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".jsonata.class",
        JsonataExecutor.class.getName());
    specificAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, specificProps);

    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v1");
    protobufSerializer = new KafkaProtobufSerializer<>(schemaRegistry, new HashMap(defaultConfig));
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v2");
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, new HashMap(defaultConfig));

    specificProps.put(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG, "true");
    specificProtobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, specificProps);

    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v1");
    jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, new HashMap(defaultConfig));
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v2");
    jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry, new HashMap(defaultConfig));

    specificJsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry, specificProps, NewWidget.class);
  }

  private Schema createNewGenericWidgetSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"NewGenericWidget\"," +
        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"},"
        + "{\"name\": \"height\", \"type\": \"int\"},"
        + "{\"name\": \"version\", \"type\": \"int\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  @Test
  public void testKafkaAvroSerializerGenericRecord() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    Schema schema = ReflectData.get().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = createNewGenericWidgetSchema();
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, ruleString, null, null);
    RuleSet ruleSet = new RuleSet(Collections.singletonList(rule), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = avroDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        GenericRecord.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        123,
        ((GenericRecord)obj).get("height")
    );
  }

  @Test
  public void testKafkaAvroSerializerReflectionRecord() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    Schema schema = ReflectData.get().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = ReflectData.get().getSchema(NewWidget.class);
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, ruleString, null, null);
    RuleSet ruleSet = new RuleSet(Collections.singletonList(rule), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer2.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        NewWidget.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        123,
        ((NewWidget)obj).getHeight()
    );
  }

  @Test
  public void testKafkaAvroSerializerSpecificRecord() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    Schema schema = ReflectData.get().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = NewSpecificWidget.getClassSchema();
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, ruleString, null, null);
    RuleSet ruleSet = new RuleSet(Collections.singletonList(rule), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = specificAvroDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        NewSpecificWidget.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        123,
        ((NewSpecificWidget)obj).getHeight()
    );
  }

  @Test
  public void testKafkaAvroSerializerFullyCompatible() throws Exception {
    byte[] bytes;
    Object obj;

    String rule1To2 =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";
    String rule2To1 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])";
    String rule2To3 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])";
    String rule3To2 =
        "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])";

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    Schema schema = ReflectData.get().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = ReflectData.get().getSchema(NewWidget.class);
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule1", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, rule1To2, null, null);
    Rule rule2 = new Rule("myRule2", RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, rule2To1, null, null);
    RuleSet ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = ReflectData.get().getSchema(NewerWidget.class);
    avroSchema = new AvroSchema(schema);
    rule = new Rule("myRule1", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, rule2To3, null, null);
    rule2 = new Rule("myRule2", RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, rule3To2, null, null);
    ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v3");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    deserializeWithAllVersions(bytes);

    NewWidget newWidget = new NewWidget("alice");
    newWidget.setHeight(123);
    bytes = reflectionAvroSerializer2.serialize(topic, newWidget);

    deserializeWithAllVersions(bytes);

    NewerWidget newerWidget = new NewerWidget("alice");
    newerWidget.setLength(123);
    bytes = reflectionAvroSerializer3.serialize(topic, newerWidget);

    deserializeWithAllVersions(bytes);
  }

  private void deserializeWithAllVersions(byte[] bytes) {
    Object obj;
    obj = reflectionAvroDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a OldWidget",
        OldWidget.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a OldWidget",
        123,
        ((OldWidget)obj).getSize()
    );
    obj = reflectionAvroDeserializer2.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        NewWidget.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        123,
        ((NewWidget)obj).getHeight()
    );
    obj = reflectionAvroDeserializer3.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewerWidget",
        NewerWidget.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewerWidget",
        123,
        ((NewerWidget)obj).getLength()
    );
  }

  @Test
  public void testKafkaProtobufSerializerGeneric() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    Widget widget = WidgetProto.Widget.newBuilder().setName("alice").setSize(123).build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    protobufSchema = protobufSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", protobufSchema);

    protobufSchema = new ProtobufSchema(NewWidgetProto.NewWidget.getDescriptor());
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, ruleString, null, null);
    RuleSet ruleSet = new RuleSet(Collections.singletonList(rule), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    protobufSchema = protobufSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobufSerializer.serialize(topic, widget);

    obj = protobufDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        DynamicMessage.class.isInstance(obj)
    );
    DynamicMessage dynamicMessage = (DynamicMessage) obj;
    Descriptor dynamicDesc = dynamicMessage.getDescriptorForType();
    assertEquals(
        "Returned object should be a NewWidget",
        123,
        ((DynamicMessage)obj).getField(dynamicDesc.findFieldByName("height"))
    );
  }

  @Test
  public void testKafkaProtobufSerializerSpecific() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    Widget widget = WidgetProto.Widget.newBuilder().setName("alice").setSize(123).build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    protobufSchema = protobufSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", protobufSchema);

    protobufSchema = new ProtobufSchema(NewWidgetProto.NewWidget.getDescriptor());
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, ruleString, null, null);
    RuleSet ruleSet = new RuleSet(Collections.singletonList(rule), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    protobufSchema = protobufSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    bytes = protobufSerializer.serialize(topic, widget);

    obj = specificProtobufDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        NewWidgetProto.NewWidget.class.isInstance(obj)
    );
    NewWidgetProto.NewWidget newWidget = (NewWidgetProto.NewWidget) obj;
    assertEquals(
        "Returned object should be a NewWidget",
        123,
        newWidget.getHeight()
    );
  }

  @Test
  public void testKafkaJsonSerializerGeneric() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    JsonSchema jsonSchema = JsonSchemaUtils.getSchema(widget);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    jsonSchema = jsonSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", jsonSchema);

    NewWidget newWidget = new NewWidget("alice");
    jsonSchema = JsonSchemaUtils.getSchema(newWidget);
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, ruleString, null, null);
    RuleSet ruleSet = new RuleSet(Collections.singletonList(rule), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer.serialize(topic, widget);

    obj = jsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        JsonNode.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        123,
        ((JsonNode)obj).get("height").intValue()
    );
  }

  @Test
  public void testKafkaJsonSerializerSpecific() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    OldWidget widget = new OldWidget("alice");
    widget.setSize(123);
    JsonSchema jsonSchema = JsonSchemaUtils.getSchema(widget);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    jsonSchema = jsonSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", jsonSchema);

    NewWidget newWidget = new NewWidget("alice");
    jsonSchema = JsonSchemaUtils.getSchema(newWidget);
    Rule rule = new Rule("myRule", RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, ruleString, null, null);
    RuleSet ruleSet = new RuleSet(Collections.singletonList(rule), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    bytes = jsonSchemaSerializer.serialize(topic, widget);

    obj = specificJsonSchemaDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        NewWidget.class.isInstance(obj)
    );
    assertEquals(
        "Returned object should be a NewWidget",
        123,
        ((NewWidget)obj).getHeight()
    );
  }

  public static class OldWidget {
    private String name;
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
          && size == widget.size
          && version == widget.version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, size, version);
    }
  }

  public static class NewWidget {
    private String name;
    private int height;
    private int version;

    public NewWidget() {}
    public NewWidget(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getHeight() {
      return height;
    }

    public void setHeight(int height) {
      this.height = height;
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
      NewWidget widget = (NewWidget) o;
      return name.equals(widget.name)
          && height == widget.height
          && version == widget.version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, height, version);
    }
  }

  public static class NewerWidget {
    private String name;
    private int length;
    private int version;

    public NewerWidget() {}
    public NewerWidget(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getLength() {
      return length;
    }

    public void setLength(int length) {
      this.length = length;
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
      NewerWidget widget = (NewerWidget) o;
      return name.equals(widget.name)
          && length == widget.length
          && version == widget.version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, length, version);
    }
  }
}
