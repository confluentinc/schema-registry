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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
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
import io.confluent.kafka.schemaregistry.rules.ExpiringSpecificWidget;
import io.confluent.kafka.schemaregistry.rules.ExpiringSpecificWidgetProto;
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
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
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
  private final KafkaProtobufSerializer<ExpiringSpecificWidgetProto.ExpiringSpecificWidget> protobufSerializer2;
  private final KafkaProtobufDeserializer<DynamicMessage> protobufDeserializer;
  private final KafkaProtobufDeserializer<NewWidgetProto.NewWidget> specificProtobufDeserializer;
  private final KafkaJsonSchemaSerializer<OldWidget> jsonSchemaSerializer;
  private final KafkaJsonSchemaSerializer<ExpiringWidget> jsonSchemaSerializer2;
  private final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;
  private final KafkaJsonSchemaDeserializer<NewWidget> specificJsonSchemaDeserializer;
  private final String topic;
  private final UUID id = UUID.fromString("2182b6f9-6422-43d8-819e-822b2b678eec");

  public JsonataExecutorTest() {
    topic = "test";
    schemaRegistry = new MockSchemaRegistryClient(ImmutableList.of(
        new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()));

    Map<String, Object> defaultConfig = new HashMap<>();
    defaultConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, "false");
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v1");
    defaultConfig.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, "true");
    defaultConfig.put(KafkaAvroSerializerConfig.LATEST_COMPATIBILITY_STRICT, "false");
    defaultConfig.put(KafkaAvroSerializerConfig.RULE_EXECUTORS, "jsonata");
    defaultConfig.put(KafkaAvroSerializerConfig.RULE_EXECUTORS + ".jsonata.class",
        JsonataExecutor.class);
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, defaultConfig);
    Map<String, Object> defaultConfig2 = new HashMap<>(defaultConfig);
    defaultConfig2.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v2");
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, defaultConfig2);

    Map<String, Object> reflectionProps = new HashMap<>(defaultConfig);
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionAvroSerializer = new KafkaAvroSerializer(schemaRegistry, reflectionProps);
    reflectionAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, reflectionProps);
    Map<String, Object> reflectionProps2 = new HashMap<>(reflectionProps);
    reflectionProps2.put(KafkaAvroDeserializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v2");
    reflectionAvroSerializer2 = new KafkaAvroSerializer(schemaRegistry, reflectionProps2);
    reflectionAvroDeserializer2 = new KafkaAvroDeserializer(schemaRegistry, reflectionProps2);
    Map<String, Object> reflectionProps3 = new HashMap<>(reflectionProps);
    reflectionProps3.put(KafkaAvroDeserializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v3");
    reflectionAvroSerializer3 = new KafkaAvroSerializer(schemaRegistry, reflectionProps3);
    reflectionAvroDeserializer3 = new KafkaAvroDeserializer(schemaRegistry, reflectionProps3);

    Map<String, Object> specificProps2 = new HashMap<>(defaultConfig2);
    specificProps2.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    specificAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, specificProps2);

    protobufSerializer = new KafkaProtobufSerializer<>(schemaRegistry, defaultConfig);
    protobufSerializer2 = new KafkaProtobufSerializer<>(schemaRegistry, defaultConfig);
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, defaultConfig2);

    specificProps2.put(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG, "true");
    specificProtobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry, specificProps2);

    jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, defaultConfig);
    jsonSchemaSerializer2 = new KafkaJsonSchemaSerializer<>(schemaRegistry, defaultConfig);
    jsonSchemaDeserializer =
        new KafkaJsonSchemaDeserializer<>(schemaRegistry, defaultConfig2, JsonNode.class);

    specificJsonSchemaDeserializer =
        new KafkaJsonSchemaDeserializer<>(schemaRegistry, specificProps2, NewWidget.class);
  }

  private Schema createNewGenericWidgetSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"NewGenericWidget\"," +
        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"},"
        + "{\"name\": \"height\", \"type\": \"int\"},"
        + "{\"name\": \"version\", \"type\": \"int\"},"
        + "{\"name\": \"id\", \"type\":"
        + "{\"type\": \"string\", \"logicalType\": \"uuid\"}}"
        + "]}";

    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private Schema createExpiringWidgetSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"ExpiringWidget\"," +
        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"},"
        + "{\"name\": \"size\", \"type\": \"int\"},"
        + "{\"name\": \"version\", \"type\": \"int\"},"
        + "{\"name\": \"id\", \"type\":"
        + "{\"type\": \"string\", \"logicalType\": \"uuid\"}},"
        + "{\"name\": \"expiration\", \"type\":"
        + "{\"type\": \"string\", \"logicalType\": \"date\"}}"
        + "]}";

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

    OldWidget widget = new OldWidget(id, "alice");
    widget.setSize(123);
    Schema schema = AvroSchemaUtils.getReflectData().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, null);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = createNewGenericWidgetSchema();
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
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
    assertEquals(
        id,
        ((GenericRecord)obj).get("id")
    );
  }

  @Test
  public void testKafkaAvroSerializerReflectionRecord() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    OldWidget widget = new OldWidget(id, "alice");
    widget.setSize(123);
    Schema schema = AvroSchemaUtils.getReflectData().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, null);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = AvroSchemaUtils.getReflectData().getSchema(NewWidget.class);
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
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

    OldWidget widget = new OldWidget(id, "alice");
    widget.setSize(123);
    Schema schema = AvroSchemaUtils.getReflectData().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, null);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = NewSpecificWidget.getClassSchema();
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
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

    String rule1To2 =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";
    String rule2To1 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])";
    String rule2To3 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])";
    String rule3To2 =
        "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])";

    OldWidget widget = new OldWidget(id, "alice");
    widget.setSize(123);
    Schema schema = AvroSchemaUtils.getReflectData().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, null);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = AvroSchemaUtils.getReflectData().getSchema(NewWidget.class);
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule1", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, rule1To2, null, null, false);
    Rule rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, null, rule2To1, null, null, false);
    RuleSet ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = AvroSchemaUtils.getReflectData().getSchema(NewerWidget.class);
    avroSchema = new AvroSchema(schema);
    rule = new Rule("myRule1", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, rule2To3, null, null, false);
    rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, null, rule3To2, null, null, false);
    ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v3");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    deserializeWithAllVersions(bytes);

    NewWidget newWidget = new NewWidget(id, "alice");
    newWidget.setHeight(123);
    bytes = reflectionAvroSerializer2.serialize(topic, newWidget);

    deserializeWithAllVersions(bytes);

    NewerWidget newerWidget = new NewerWidget(id, "alice");
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
  public void testKafkaAvroCondition() throws Exception {
    String ruleString = "$.expiration * 86400000 > $millis()";

    ExpiringSpecificWidget widget = new ExpiringSpecificWidget();
    widget.setName("alice");
    widget.setSize(123);
    widget.setExpiration(LocalDate.now());
    Schema schema = ExpiringSpecificWidget.getClassSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    assertThrows(SerializationException.class, () -> avroSerializer.serialize(topic, widget));
  }

  @Test
  public void testKafkaProtobufSerializerGeneric() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    Widget widget = WidgetProto.Widget.newBuilder()
        .setName("alice")
        .setMybytes(ByteString.EMPTY)
        .setSize(123)
        .build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    protobufSchema = protobufSchema.copy(metadata, null);
    schemaRegistry.register(topic + "-value", protobufSchema);

    protobufSchema = new ProtobufSchema(NewWidgetProto.NewWidget.getDescriptor());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
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

    Widget widget = WidgetProto.Widget.newBuilder()
        .setName("alice")
        .setMybytes(ByteString.EMPTY)
        .setSize(123)
        .build();
    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    protobufSchema = protobufSchema.copy(metadata, null);
    schemaRegistry.register(topic + "-value", protobufSchema);

    protobufSchema = new ProtobufSchema(NewWidgetProto.NewWidget.getDescriptor());
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
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
  public void testKafkaProtobufCondition() throws Exception {
    String ruleString = "$toMillis($.expiration) > $millis()";

    ExpiringSpecificWidgetProto.ExpiringSpecificWidget widget =
        ExpiringSpecificWidgetProto.ExpiringSpecificWidget.newBuilder()
            .setName("alice")
            .setSize(123)
            .setExpiration(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();

    ProtobufSchema protobufSchema = new ProtobufSchema(widget.getDescriptorForType());
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    protobufSchema = protobufSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", protobufSchema);

    assertThrows(SerializationException.class, () -> protobufSerializer2.serialize(topic, widget));
  }

  @Test
  public void testKafkaJsonSerializerGeneric() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    OldWidget widget = new OldWidget(id, "alice");
    widget.setSize(123);
    JsonSchema jsonSchema = JsonSchemaUtils.getSchema(widget);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    jsonSchema = jsonSchema.copy(metadata, null);
    schemaRegistry.register(topic + "-value", jsonSchema);

    NewWidget newWidget = new NewWidget(id, "alice");
    jsonSchema = JsonSchemaUtils.getSchema(newWidget);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
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

    OldWidget widget = new OldWidget(id, "alice");
    widget.setSize(123);
    JsonSchema jsonSchema = JsonSchemaUtils.getSchema(widget);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    jsonSchema = jsonSchema.copy(metadata, null);
    schemaRegistry.register(topic + "-value", jsonSchema);

    NewWidget newWidget = new NewWidget(id, "alice");
    jsonSchema = JsonSchemaUtils.getSchema(newWidget);
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
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

  @Test
  public void testKafkaJsonCondition() throws Exception {
    String ruleString = "$toMillis($.expiration) > $millis()";

    ExpiringWidget widget = new ExpiringWidget();
    widget.setName("alice");
    widget.setSize(123);
    widget.setExpiration(LocalDate.now());
    JsonSchema jsonSchema = JsonSchemaUtils.getSchema(widget);
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITE,
        JsonataExecutor.TYPE, null, null, ruleString, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    jsonSchema = jsonSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", jsonSchema);

    assertThrows(SerializationException.class, () -> jsonSchemaSerializer2.serialize(topic, widget));
  }

  @JsonSchemaInject(strings = {@JsonSchemaString(path="javaType",
      value= "io.confluent.kafka.schemaregistry.rules.jsonata.JsonataExecutorTest$OldWidget")})
  public static class OldWidget {
    private UUID id;
    private String name;
    private int size;
    private int version;

    public OldWidget() {}
    public OldWidget(UUID id, String name) {
      this.id = id;
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

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OldWidget widget = (OldWidget) o;
      return name.equals(widget.name)
          && size == widget.size
          && version == widget.version
          && Objects.equals(id, widget.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, size, version);
    }
  }

  @JsonSchemaInject(strings = {@JsonSchemaString(path="javaType",
      value= "io.confluent.kafka.schemaregistry.rules.jsonata.JsonataExecutorTest$NewWidget")})
  public static class NewWidget {
    private UUID id;
    private String name;
    private int height;
    private int version;

    public NewWidget() {}
    public NewWidget(UUID id, String name) {
      this.id = id;
      this.name = name;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
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
          && version == widget.version
          && Objects.equals(id, widget.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, height, version);
    }
  }

  @JsonSchemaInject(strings = {@JsonSchemaString(path="javaType",
      value= "io.confluent.kafka.schemaregistry.rules.jsonata.JsonataExecutorTest$NewerWidget")})
  public static class NewerWidget {
    private UUID id;
    private String name;
    private int length;
    private int version;

    public NewerWidget() {}
    public NewerWidget(UUID id, String name) {
      this.id = id;
      this.name = name;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
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
          && version == widget.version
          && Objects.equals(id, widget.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, length, version);
    }
  }

  @JsonSchemaInject(strings = {@JsonSchemaString(path="javaType",
      value= "io.confluent.kafka.schemaregistry.rules.jsonata.JsonataExecutorTest$ExpiringWidget")})
  public static class ExpiringWidget {
    private UUID id;
    private String name;
    private int size;
    private int version;
    private LocalDate expiration;

    public ExpiringWidget() {}
    public ExpiringWidget(UUID id, String name) {
      this.id = id;
      this.name = name;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
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

    public LocalDate getExpiration() {
      return expiration;
    }

    public void setExpiration(LocalDate expiration) {
      this.expiration = expiration;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ExpiringWidget widget = (ExpiringWidget) o;
      return name.equals(widget.name)
          && size == widget.size
          && version == widget.version
          && Objects.equals(id, widget.id)
          && Objects.equals(expiration, widget.expiration);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, size, version, expiration);
    }
  }
}
