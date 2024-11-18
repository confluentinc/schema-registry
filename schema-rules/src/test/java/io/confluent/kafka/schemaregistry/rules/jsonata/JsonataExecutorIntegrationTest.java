/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package io.confluent.kafka.schemaregistry.rules.jsonata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.NewSpecificWidget;
import io.confluent.kafka.schemaregistry.rules.NewSpecificWidgetProto;
import io.confluent.kafka.schemaregistry.rules.NewerSpecificWidget;
import io.confluent.kafka.schemaregistry.rules.NewerSpecificWidgetProto;
import io.confluent.kafka.schemaregistry.rules.SpecificWidget;
import io.confluent.kafka.schemaregistry.rules.SpecificWidgetProto;
import io.confluent.kafka.schemaregistry.rules.jsonata.JsonataExecutorTest.NewWidget;
import io.confluent.kafka.schemaregistry.rules.jsonata.JsonataExecutorTest.NewerWidget;
import io.confluent.kafka.schemaregistry.rules.jsonata.JsonataExecutorTest.OldWidget;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.RuleSetHandler;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonataExecutorIntegrationTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(JsonataExecutorIntegrationTest.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

  private static final String TOPIC = "widget";

  private static final UUID ID = UUID.fromString("2182b6f9-6422-43d8-819e-822b2b678eec");

  public JsonataExecutorIntegrationTest() {
    super(1, true);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    ((KafkaSchemaRegistry) restApp.schemaRegistry()).setRuleSetHandler(new RuleSetHandler() {
      public void handle(String subject, ConfigUpdateRequest request) {
      }

      public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
      }

      public io.confluent.kafka.schemaregistry.storage.RuleSet transform(RuleSet ruleSet) {
        return ruleSet != null
            ? new io.confluent.kafka.schemaregistry.storage.RuleSet(ruleSet)
            : null;
      }
    });
  }

  private static Properties createConsumerProps(
      String brokerList, String schemaRegistryUrl, String applicationVersion,
      Map<String, Object> additionalProps) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", "avroGroup" + applicationVersion);
    props.put("session.timeout.ms", "6000"); // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
    props.put("use.latest.with.metadata", "application.version=" + applicationVersion);
    props.put("avro.use.logical.type.converters", "true");
    props.putAll(additionalProps);
    return props;
  }

  private static Consumer<String, Object> createConsumer(Properties props) {
    return new KafkaConsumer<>(props);
  }

  private static List<Map.Entry<String, Object>> consume(Consumer<String, Object> consumer, String topic,
      int numMessages) {
    List<Map.Entry<String, Object>> recordList = new ArrayList<>();

    consumer.subscribe(Arrays.asList(topic));

    int i = 0;
    do {
      ConsumerRecords<String, Object> records = consumer.poll(1000);
      for (ConsumerRecord<String, Object> record : records) {
        recordList.add(new SimpleEntry<>(record.key(), record.value()));
        i++;
      }
    } while (i < numMessages);

    return recordList;
  }

  private static Properties createProducerProps(
      String brokerList, String schemaRegistryUrl, String applicationVersion,
      Map<String, Object> additionalProps) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put("auto.register.schemas", "false");
    props.put("use.latest.with.metadata", "application.version=" + applicationVersion);
    props.put("avro.use.logical.type.converters", "true");
    props.put("latest.compatibility.strict", "false");
    props.putAll(additionalProps);
    return props;
  }

  private static Producer createProducer(Properties props) {
    return new KafkaProducer(props);
  }

  private static void produce(Producer producer, String topic, String key, Object object) throws Exception {
    ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, object);
    producer.send(record).get();
  }

  @Test
  public void testAvroReflectionFullyCompatible() throws Exception {
    registerReflectSchemas(restApp.restConnect);
    Map<String, Object> additionalProps = new HashMap<>();
    additionalProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    additionalProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    additionalProps.put(KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    List<Object> payloads = new ArrayList<>();
    OldWidget widget = new OldWidget(ID, "alice");
    widget.setSize(123);
    payloads.add(widget);
    NewWidget newWidget = new NewWidget(ID, "alice");
    newWidget.setHeight(123);
    payloads.add(newWidget);
    NewerWidget newerWidget = new NewerWidget(ID, "alice");
    newerWidget.setLength(123);
    payloads.add(newerWidget);
    produceAndConsume(additionalProps, payloads);
  }

  private static void registerReflectSchemas(String schemaRegistryUrl) throws Exception {
    String rule1To2 =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";
    String rule2To1 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])";
    String rule2To3 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])";
    String rule3To2 =
        "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])";

    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
        new RestService(schemaRegistryUrl),
        10,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    Config config = new Config();
    config.setCompatibilityLevel("NONE");
    schemaRegistry.updateConfig(TOPIC + "-value", config);

    Schema schema = AvroSchemaUtils.getReflectData().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, null);
    schemaRegistry.register(TOPIC + "-value", avroSchema);

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
    schemaRegistry.register(TOPIC + "-value", avroSchema);

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
    schemaRegistry.register(TOPIC + "-value", avroSchema);
  }

  @Test
  public void testAvroGenericFullyCompatible() throws Exception {
    registerSpecificSchemas(restApp.restConnect);
    Map<String, Object> additionalProps = new HashMap<>();
    additionalProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    additionalProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    List<Object> payloads = new ArrayList<>();
    GenericRecord avroRecord = new GenericData.Record(SpecificWidget.SCHEMA$);
    avroRecord.put("name", "alice");
    avroRecord.put("size", 123);
    avroRecord.put("version", 0);
    payloads.add(avroRecord);
    avroRecord = new GenericData.Record(NewSpecificWidget.SCHEMA$);
    avroRecord.put("name", "alice");
    avroRecord.put("height", 123);
    avroRecord.put("version", 0);
    payloads.add(avroRecord);
    avroRecord = new GenericData.Record(NewerSpecificWidget.SCHEMA$);
    avroRecord.put("name", "alice");
    avroRecord.put("length", 123);
    avroRecord.put("version", 0);
    payloads.add(avroRecord);
    produceAndConsume(additionalProps, payloads);
  }

  @Test
  public void testAvroSpecificFullyCompatible() throws Exception {
    registerSpecificSchemas(restApp.restConnect);
    Map<String, Object> additionalProps = new HashMap<>();
    additionalProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    additionalProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    additionalProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    List<Object> payloads = new ArrayList<>();
    SpecificWidget widget = new SpecificWidget();
    widget.setName("alice");
    widget.setSize(123);
    payloads.add(widget);
    NewSpecificWidget newWidget = new NewSpecificWidget();
    newWidget.setName("alice");
    newWidget.setHeight(123);
    payloads.add(newWidget);
    NewerSpecificWidget newerWidget = new NewerSpecificWidget();
    newerWidget.setName("alice");
    newerWidget.setLength(123);
    payloads.add(newerWidget);
    produceAndConsume(additionalProps, payloads);
  }

  private static void registerSpecificSchemas(String schemaRegistryUrl) throws Exception {
    String rule1To2 =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";
    String rule2To1 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])";
    String rule2To3 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])";
    String rule3To2 =
        "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])";

    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
        new RestService(schemaRegistryUrl),
        10,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    Config config = new Config();
    config.setCompatibilityLevel("NONE");
    schemaRegistry.updateConfig(TOPIC + "-value", config);

    Schema schema = SpecificWidget.SCHEMA$;
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, null);
    schemaRegistry.register(TOPIC + "-value", avroSchema);

    schema = NewSpecificWidget.SCHEMA$;
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule1", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, rule1To2, null, null, false);
    Rule rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, null, rule2To1, null, null, false);
    RuleSet ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(TOPIC + "-value", avroSchema);

    schema = NewerSpecificWidget.SCHEMA$;
    avroSchema = new AvroSchema(schema);
    rule = new Rule("myRule1", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, rule2To3, null, null, false);
    rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, null, rule3To2, null, null, false);
    ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v3");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(TOPIC + "-value", avroSchema);
  }

  @Test
  public void testProtobufSpecificFullyCompatible() throws Exception {
    registerProtobufSchemas(restApp.restConnect);
    Map<String, Object> additionalProps = new HashMap<>();
    additionalProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
    additionalProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    additionalProps.put(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG, "true");
    List<Object> payloads = new ArrayList<>();
    SpecificWidgetProto.SpecificWidget.Builder widget = SpecificWidgetProto.SpecificWidget.newBuilder();
    widget.setName("alice");
    widget.setSize(123);
    payloads.add(widget.build());
    NewSpecificWidgetProto.NewSpecificWidget.Builder newWidget = NewSpecificWidgetProto.NewSpecificWidget.newBuilder();
    newWidget.setName("alice");
    newWidget.setHeight(123);
    payloads.add(newWidget.build());
    NewerSpecificWidgetProto.NewerSpecificWidget.Builder newerWidget = NewerSpecificWidgetProto.NewerSpecificWidget.newBuilder();
    newerWidget.setName("alice");
    newerWidget.setLength(123);
    payloads.add(newerWidget.build());
    produceAndConsume(additionalProps, payloads);
  }

  @Test
  public void testProtobufGenericFullyCompatible() throws Exception {
    registerProtobufSchemas(restApp.restConnect);
    Map<String, Object> additionalProps = new HashMap<>();
    additionalProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
    additionalProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    List<Object> payloads = new ArrayList<>();

    ProtobufSchema schema = new ProtobufSchema(SpecificWidgetProto.SpecificWidget.getDescriptor());
    DynamicMessage.Builder builder = schema.newMessageBuilder();
    builder.setField(builder.getDescriptorForType().findFieldByName("name"), "alice");
    builder.setField(builder.getDescriptorForType().findFieldByName("size"), 123);
    payloads.add(builder.build());
    schema = new ProtobufSchema(NewSpecificWidgetProto.NewSpecificWidget.getDescriptor());
    builder = schema.newMessageBuilder();
    builder.setField(builder.getDescriptorForType().findFieldByName("name"), "alice");
    builder.setField(builder.getDescriptorForType().findFieldByName("height"), 123);
    payloads.add(builder.build());
    schema = new ProtobufSchema(NewerSpecificWidgetProto.NewerSpecificWidget.getDescriptor());
    builder = schema.newMessageBuilder();
    builder.setField(builder.getDescriptorForType().findFieldByName("name"), "alice");
    builder.setField(builder.getDescriptorForType().findFieldByName("length"), 123);
    payloads.add(builder.build());
    produceAndConsume(additionalProps, payloads);
  }

  private static void registerProtobufSchemas(String schemaRegistryUrl) throws Exception {
    String rule1To2 =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";
    String rule2To1 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])";
    String rule2To3 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])";
    String rule3To2 =
        "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])";

    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
        new RestService(schemaRegistryUrl),
        10,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    Config config = new Config();
    config.setCompatibilityLevel("NONE");
    schemaRegistry.updateConfig(TOPIC + "-value", config);

    ProtobufSchema schema = new ProtobufSchema(SpecificWidgetProto.SpecificWidget.getDescriptor());
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    schema = schema.copy(metadata, null);
    schemaRegistry.register(TOPIC + "-value", schema);

    schema = new ProtobufSchema(NewSpecificWidgetProto.NewSpecificWidget.getDescriptor());
    Rule rule = new Rule("myRule1", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, rule1To2, null, null, false);
    Rule rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, null, rule2To1, null, null, false);
    RuleSet ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    schema = schema.copy(metadata, ruleSet);
    schemaRegistry.register(TOPIC + "-value", schema);

    schema = new ProtobufSchema(NewerSpecificWidgetProto.NewerSpecificWidget.getDescriptor());
    rule = new Rule("myRule1", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, rule2To3, null, null, false);
    rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, null, rule3To2, null, null, false);
    ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v3");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    schema = schema.copy(metadata, ruleSet);
    schemaRegistry.register(TOPIC + "-value", schema);
  }

  @Test
  public void testJsonSchemaPojoFullyCompatible() throws Exception {
    registerJsonSchemas(restApp.restConnect);
    Map<String, Object> additionalProps = new HashMap<>();
    additionalProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
    additionalProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
    List<Object> payloads = new ArrayList<>();
    OldWidget widget = new OldWidget(ID, "alice");
    widget.setSize(123);
    payloads.add(widget);
    NewWidget newWidget = new NewWidget(ID, "alice");
    newWidget.setHeight(123);
    payloads.add(newWidget);
    NewerWidget newerWidget = new NewerWidget(ID, "alice");
    newerWidget.setLength(123);
    payloads.add(newerWidget);
    produceAndConsume(additionalProps, payloads);
  }

  @Test
  public void testJsonSchemaJsonNodeFullyCompatible() throws Exception {
    registerJsonSchemas(restApp.restConnect);
    Map<String, Object> additionalProps = new HashMap<>();
    additionalProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
    additionalProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
    additionalProps.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class);
    List<Object> payloads = new ArrayList<>();
    OldWidget widget = new OldWidget(ID, "alice");
    widget.setSize(123);
    payloads.add(mapper.valueToTree(widget));
    NewWidget newWidget = new NewWidget(ID, "alice");
    newWidget.setHeight(123);
    payloads.add(mapper.valueToTree(newWidget));
    NewerWidget newerWidget = new NewerWidget(ID, "alice");
    newerWidget.setLength(123);
    payloads.add(mapper.valueToTree(newerWidget));
    produceAndConsume(additionalProps, payloads);
  }

  private static void registerJsonSchemas(String schemaRegistryUrl) throws Exception {
    String rule1To2 =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";
    String rule2To1 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])";
    String rule2To3 =
        "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])";
    String rule3To2 =
        "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])";

    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
        new RestService(schemaRegistryUrl),
        10,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    Config config = new Config();
    config.setCompatibilityLevel("NONE");
    schemaRegistry.updateConfig(TOPIC + "-value", config);

    JsonSchema schema = JsonSchemaUtils.getSchema(new OldWidget());
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    schema = schema.copy(metadata, null);
    schemaRegistry.register(TOPIC + "-value", schema);

    schema = JsonSchemaUtils.getSchema(new NewWidget());
    Rule rule = new Rule("myRule1", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, rule1To2, null, null, false);
    Rule rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, null, rule2To1, null, null, false);
    RuleSet ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    schema = schema.copy(metadata, ruleSet);
    schemaRegistry.register(TOPIC + "-value", schema);

    schema = JsonSchemaUtils.getSchema(new NewerWidget());
    rule = new Rule("myRule1", null, RuleKind.TRANSFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, null, null, rule2To3, null, null, false);
    rule2 = new Rule("myRule2", null, RuleKind.TRANSFORM, RuleMode.DOWNGRADE,
        JsonataExecutor.TYPE, null, null, rule3To2, null, null, false);
    ruleSet = new RuleSet(ImmutableList.of(rule, rule2), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v3");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    schema = schema.copy(metadata, ruleSet);
    schemaRegistry.register(TOPIC + "-value", schema);
  }

  private void produceAndConsume(Map<String, Object> additionalProps, List<Object> payloads) throws Exception {
    Properties producerProps = createProducerProps(brokerList, restApp.restConnect, "v1", additionalProps);
    try (Producer producer = createProducer(producerProps)) {
      produce(producer, TOPIC, "key1", payloads.get(0));
    }
    producerProps = createProducerProps(brokerList, restApp.restConnect, "v2", additionalProps);
    try (Producer producer = createProducer(producerProps)) {
      produce(producer, TOPIC, "key2", payloads.get(1));
    }
    producerProps = createProducerProps(brokerList, restApp.restConnect, "v3", additionalProps);
    try (Producer producer = createProducer(producerProps)) {
      produce(producer, TOPIC, "key3", payloads.get(2));
    }

    Properties consumerProps = createConsumerProps(brokerList, restApp.restConnect, "v1", additionalProps);
    try (Consumer<String, Object> consumer = createConsumer(consumerProps)) {
      List<Map.Entry<String, Object>> recordList = consume(consumer, TOPIC, 3);
      Map.Entry<String, Object> entry = recordList.get(0);
      assertMessagesEqual(entry.getValue(), payloads.get(0));
    }

    consumerProps = createConsumerProps(brokerList, restApp.restConnect, "v2", additionalProps);
    try (Consumer<String, Object> consumer = createConsumer(consumerProps)) {
      List<Map.Entry<String, Object>> recordList = consume(consumer, TOPIC, 3);
      Map.Entry<String, Object> entry = recordList.get(0);
      assertMessagesEqual(entry.getValue(), payloads.get(1));
    }

    consumerProps = createConsumerProps(brokerList, restApp.restConnect, "v3", additionalProps);
    try (Consumer<String, Object> consumer = createConsumer(consumerProps)) {
      List<Map.Entry<String, Object>> recordList = consume(consumer, TOPIC, 3);
      Map.Entry<String, Object> entry = recordList.get(0);
      assertMessagesEqual(entry.getValue(), payloads.get(2));
    }
  }

  private void assertMessagesEqual(Object o1, Object o2) {
    if (o1 instanceof DynamicMessage) {
      assertTrue(o2 instanceof DynamicMessage);
      DynamicMessage d2 = (DynamicMessage) o2;
      for (Map.Entry<FieldDescriptor, Object> entry : ((DynamicMessage) o1).getAllFields().entrySet()) {
        FieldDescriptor f1 = entry.getKey();
        assertEquals(entry.getValue(), d2.getField(d2.getDescriptorForType().findFieldByName(f1.getName())));
      }
    } else {
      assertEquals(o1, o2);
    }
  }
}

