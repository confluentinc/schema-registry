/*
 * Copyright 2025 Confluent Inc.
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
 */

package io.confluent.connect.protobuf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class ProtobufConverterBackupTest {

  private static final String TOPIC = "test-topic";
  private static final String SCHEMA_TYPE = "PROTOBUF";
  private static final String SR_URL_KEY = "schema.registry.url";
  private static final String FAKE_SR_URL = "http://fake-url";
  private static final String BACKUP_ENABLED_KEY = "schema.backup.enabled";

  private static final String FIELD_NAME = "name";
  private static final String FIELD_VALUE = "value";
  private static final String FIELD_KEY = "key";
  private static final String FIELD_ID = "id";
  private static final String FIELD_CITY = "city";
  private static final String FIELD_STREET = "street";
  private static final String FIELD_COUNTRY = "country";
  private static final String VALUE_ALICE = "Alice";
  private static final String VALUE_BOB = "Bob";
  private static final String VALUE_US = "US";

  private static final String PROTO_HEADER =
      "syntax = \"proto3\";\n"
      + "package io.confluent.test;\n";

  private static final String KITCHEN_SINK_PROTO =
      PROTO_HEADER
      + "message ComplexEvent {\n"
      + "  string id = 1;\n"
      + "  Priority priority = 2;\n"
      + "  Address home_addr = 3;\n"
      + "  Address work_addr = 4;\n"
      + "  repeated Contact contacts = 5;\n"
      + "  map<string, Metadata> attributes = 6;\n"
      + "  oneof target {\n"
      + "    string url = 7;\n"
      + "    Endpoint endpoint = 8;\n"
      + "  }\n"
      + "  message Nested {\n"
      + "    string label = 1;\n"
      + "    enum Kind { A = 0; B = 1; C = 2; }\n"
      + "    Kind kind = 2;\n"
      + "  }\n"
      + "  Nested nested = 9;\n"
      + "}\n"
      + "message Address {\n"
      + "  string street = 1;\n"
      + "  string city = 2;\n"
      + "  string country = 3;\n"
      + "}\n"
      + "message Contact {\n"
      + "  string name = 1;\n"
      + "  Address address = 2;\n"
      + "}\n"
      + "message Metadata {\n"
      + "  string key = 1;\n"
      + "  string value = 2;\n"
      + "}\n"
      + "message Endpoint {\n"
      + "  string host = 1;\n"
      + "  int32 port = 2;\n"
      + "}\n"
      + "enum Priority { LOW = 0; MEDIUM = 1; HIGH = 2; }\n";

  // Source-cluster SR + converters (default for all tests).
  private final SchemaRegistryClient schemaRegistry;
  private final ProtobufConverter converter;
  private final ProtobufConverter plainConverter;
  private KafkaProtobufSerializer<DynamicMessage> serializer;
  private KafkaProtobufDeserializer<DynamicMessage> deserializer;

  // Target-cluster SR + converter for cross-cluster restore tests.
  private final SchemaRegistryClient targetSchemaRegistry;
  private final ProtobufConverter targetConverter;
  private KafkaProtobufDeserializer<DynamicMessage> targetDeserializer;

  private int topicCounter = 0;

  public ProtobufConverterBackupTest() {
    schemaRegistry = new MockSchemaRegistryClient(
        ImmutableList.of(new ProtobufSchemaProvider()));
    converter = new ProtobufConverter(schemaRegistry);
    plainConverter = new ProtobufConverter(schemaRegistry);
    targetSchemaRegistry = new MockSchemaRegistryClient(
        ImmutableList.of(new ProtobufSchemaProvider()));
    targetConverter = new ProtobufConverter(targetSchemaRegistry);
  }

  @Before
  public void setUp() {
    Map<String, Object> backupConfig = new HashMap<>();
    backupConfig.put(SR_URL_KEY, FAKE_SR_URL);
    backupConfig.put(BACKUP_ENABLED_KEY, "true");
    converter.configure(backupConfig, false);
    targetConverter.configure(backupConfig, false);

    Map<String, Object> plainConfig = Collections.singletonMap(SR_URL_KEY, FAKE_SR_URL);
    plainConverter.configure(plainConfig, false);

    serializer = new KafkaProtobufSerializer<>(schemaRegistry);
    serializer.configure(plainConfig, false);

    deserializer = new KafkaProtobufDeserializer<>(schemaRegistry);
    deserializer.configure(plainConfig, false);

    targetDeserializer = new KafkaProtobufDeserializer<>(targetSchemaRegistry);
    targetDeserializer.configure(plainConfig, false);
  }

  private static KafkaProtobufSerializer<DynamicMessage> newReferenceAwareProtobufSerializer(
      SchemaRegistryClient sr) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_KEY, FAKE_SR_URL);
    cfg.put("auto.register.schemas", "false");
    cfg.put("use.latest.version", "true");
    KafkaProtobufSerializer<DynamicMessage> s = new KafkaProtobufSerializer<>(sr);
    s.configure(cfg, false);
    return s;
  }

  private String nextTopic() {
    return TOPIC + "-" + (topicCounter++);
  }

  private static void assertWrapperShape(SchemaAndValue wrapped, String expectedType) {
    assertNotNull("wrapped result", wrapped);
    assertNotNull("wrapped schema", wrapped.schema());
    assertEquals("wrapper schema name", BackupWrapper.NAME, wrapped.schema().name());
    Struct w = (Struct) wrapped.value();
    assertEquals("schema type", expectedType, w.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull("schema subject", w.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertNotNull("raw schema", w.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertNotNull("data field", w.get(BackupWrapper.FIELD_DATA));
    assertNotNull("schema ID", w.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertTrue("schema ID > 0", w.getInt32(BackupWrapper.FIELD_SCHEMA_ID) > 0);
    assertNotNull("schema version", w.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
  }

  private static void assertBytesExact(byte[] original, byte[] restored) {
    assertNotNull("original bytes", original);
    assertNotNull("restored bytes", restored);
    assertArrayEquals("byte-exact roundtrip", original, restored);
  }

  private static void assertWireSchemaIdPreserved(byte[] original, byte[] restored) {
    assertTrue("original wire-format long enough", original.length >= 5);
    assertTrue("restored wire-format long enough", restored.length >= 5);
    assertEquals("original magic byte", (byte) 0x00, original[0]);
    assertEquals("restored magic byte", (byte) 0x00, restored[0]);
    int origId = ByteBuffer.wrap(original, 1, 4).getInt();
    int restoredId = ByteBuffer.wrap(restored, 1, 4).getInt();
    assertEquals("wire-format schema ID preserved", origId, restoredId);
  }

  private static void assertRawSchemaEquivalent(SchemaAndValue wrapped, ProtobufSchema original) {
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertNotNull("raw schema in wrapper", raw);
    ProtobufSchema restored = new ProtobufSchema(raw);
    assertEquals("raw schema canonical equality",
        original.canonicalString(), restored.canonicalString());
  }

  private static void assertValueEqual(Object originalDeser, Object restoredDeser) {
    assertEquals("deserialized value equality", originalDeser, restoredDeser);
  }

  private static void assertWrapperSchemaIdMatchesSource(
      SchemaAndValue wrapped, byte[] sourceBytes) {
    int sourceWireId = ByteBuffer.wrap(sourceBytes, 1, 4).getInt();
    Integer wrapperId = ((Struct) wrapped.value()).getInt32(BackupWrapper.FIELD_SCHEMA_ID);
    assertNotNull("wrapper schema ID populated", wrapperId);
    assertEquals("wrapper.schemaId matches source wire ID at bytes[1..4]",
        sourceWireId, wrapperId.intValue());
  }

  private void assertRawSchemaMatchesSourceRegistered(
      SchemaAndValue wrapped, byte[] sourceBytes) throws Exception {
    int sourceWireId = ByteBuffer.wrap(sourceBytes, 1, 4).getInt();
    ParsedSchema sourceRegistered = schemaRegistry.getSchemaById(sourceWireId);
    String wrapperRaw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertNotNull("wrapper rawSchema populated", wrapperRaw);
    assertEquals("wrapper rawSchema canonically equals source-registered schema",
        sourceRegistered.canonicalString(), new ProtobufSchema(wrapperRaw).canonicalString());
  }

  private static void assertPayloadBytesEqual(byte[] source, byte[] restored) {
    assertTrue("source has 5-byte header + payload", source.length >= 5);
    assertTrue("restored has 5-byte header + payload", restored.length >= 5);
    byte[] sourcePayload = Arrays.copyOfRange(source, 5, source.length);
    byte[] restoredPayload = Arrays.copyOfRange(restored, 5, restored.length);
    assertArrayEquals("payload bytes (bytes[5..end]) equal", sourcePayload, restoredPayload);
  }

  private static void assertCrossClusterBytesEquivalent(byte[] sourceBytes,
      byte[] restoredBytes) {
    assertTrue("source has 5-byte header + payload", sourceBytes.length >= 5);
    assertTrue("restored has 5-byte header + payload", restoredBytes.length >= 5);
    assertEquals("source magic byte", (byte) 0x00, sourceBytes[0]);
    assertEquals("restored magic byte", (byte) 0x00, restoredBytes[0]);
    int sourceId = ByteBuffer.wrap(sourceBytes, 1, 4).getInt();
    int restoredId = ByteBuffer.wrap(restoredBytes, 1, 4).getInt();
    assertTrue("source wire ID positive", sourceId > 0);
    assertTrue("restored wire ID positive", restoredId > 0);
    assertPayloadBytesEqual(sourceBytes, restoredBytes);
  }

  private void assertFullFidelityRoundTrip(String topic, ProtobufSchema schema,
      DynamicMessage message) {
    byte[] originalBytes = serializer.serialize(topic, null, message, schema);
    assertNotNull("serialization produced bytes", originalBytes);

    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    assertRawSchemaEquivalent(wrapped, schema);
    Message originalDeser = deserializer.deserialize(topic, originalBytes);
    Message restoredDeser = deserializer.deserialize(topic, restoredBytes);
    assertValueEqual(originalDeser, restoredDeser);
  }

  private Map<String, Object> backupConfigWith(String key, String value) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_KEY, FAKE_SR_URL);
    cfg.put(BACKUP_ENABLED_KEY, "true");
    cfg.put(key, value);
    return cfg;
  }

  // ================ Default-behavior gates ================

  @Test
  public void testDefaultWrapperShape() {
    Schema schema = SchemaBuilder.struct()
        .name("TestRecord")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field("count", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "rec-1").put("count", 10);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, serialized);

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    Struct w = (Struct) wrapped.value();
    assertTrue("subject contains topic",
        w.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT).contains(TOPIC));
  }

  @Test
  public void testDefaultBytesExactForStruct() {
    Schema schema = SchemaBuilder.struct()
        .name("ExactMsg")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field(FIELD_VALUE, Schema.INT32_SCHEMA)
        .build();
    Struct original = new Struct(schema).put(FIELD_NAME, "exact").put(FIELD_VALUE, 77);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, original);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }

  @Test
  public void testDefaultWireSchemaIdPreserved() {
    Schema schema = SchemaBuilder.struct()
        .name("IdCheck")
        .field(FIELD_ID, Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, 42);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }

  @Test
  public void testDefaultRawSchemaSemanticallyEqual() {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message Simple {\n"
        + "  string name = 1;\n"
        + "  int32 value = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    DynamicMessage message = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_NAME), "raw-check")
        .setField(desc.findFieldByName(FIELD_VALUE), 42)
        .build();

    byte[] originalBytes = serializer.serialize(topic, null, message, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);

    assertRawSchemaEquivalent(wrapped, schema);
  }

  @Test
  public void testDefaultSharedMessageTypeAtMultipleFieldsSurvives() {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message Person {\n"
        + "  string name = 1;\n"
        + "  Address home_addr = 2;\n"
        + "  Address work_addr = 3;\n"
        + "}\n"
        + "message Address {\n"
        + "  string street = 1;\n"
        + "  string city = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor personDesc = schema.toDescriptor();
    Descriptor addressDesc = personDesc.findFieldByName("home_addr").getMessageType();

    DynamicMessage homeAddr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "123 Home St")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Hometown")
        .build();
    DynamicMessage workAddr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "456 Work Ave")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Workville")
        .build();
    DynamicMessage person = DynamicMessage.newBuilder(personDesc)
        .setField(personDesc.findFieldByName(FIELD_NAME), VALUE_ALICE)
        .setField(personDesc.findFieldByName("home_addr"), homeAddr)
        .setField(personDesc.findFieldByName("work_addr"), workAddr)
        .build();

    assertFullFidelityRoundTrip(topic, schema, person);
  }

  @Test
  public void testDefaultSharedTypeAtThreeFields() {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message Company {\n"
        + "  string name = 1;\n"
        + "  Address hq = 2;\n"
        + "  Address warehouse = 3;\n"
        + "  Address billing = 4;\n"
        + "}\n"
        + "message Address {\n"
        + "  string line1 = 1;\n"
        + "  string city = 2;\n"
        + "  string country = 3;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor companyDesc = schema.toDescriptor();
    Descriptor addrDesc = companyDesc.findFieldByName("hq").getMessageType();

    DynamicMessage hq = DynamicMessage.newBuilder(addrDesc)
        .setField(addrDesc.findFieldByName("line1"), "1 HQ Plaza")
        .setField(addrDesc.findFieldByName(FIELD_CITY), "San Francisco")
        .setField(addrDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage warehouse = DynamicMessage.newBuilder(addrDesc)
        .setField(addrDesc.findFieldByName("line1"), "50 Warehouse Dr")
        .setField(addrDesc.findFieldByName(FIELD_CITY), "Denver")
        .setField(addrDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage billing = DynamicMessage.newBuilder(addrDesc)
        .setField(addrDesc.findFieldByName("line1"), "PO Box 100")
        .setField(addrDesc.findFieldByName(FIELD_CITY), "Austin")
        .setField(addrDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage company = DynamicMessage.newBuilder(companyDesc)
        .setField(companyDesc.findFieldByName(FIELD_NAME), "Confluent")
        .setField(companyDesc.findFieldByName("hq"), hq)
        .setField(companyDesc.findFieldByName("warehouse"), warehouse)
        .setField(companyDesc.findFieldByName("billing"), billing)
        .build();

    assertFullFidelityRoundTrip(topic, schema, company);
  }

  @Test
  public void testDefaultRepeatedNestedWithSharedType() {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message Team {\n"
        + "  string name = 1;\n"
        + "  repeated Member members = 2;\n"
        + "}\n"
        + "message Member {\n"
        + "  string name = 1;\n"
        + "  Role role = 2;\n"
        + "}\n"
        + "message Role {\n"
        + "  string title = 1;\n"
        + "  int32 level = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor teamDesc = schema.toDescriptor();
    Descriptor memberDesc = teamDesc.findFieldByName("members").getMessageType();
    Descriptor roleDesc = memberDesc.findFieldByName("role").getMessageType();

    DynamicMessage r1 = DynamicMessage.newBuilder(roleDesc)
        .setField(roleDesc.findFieldByName("title"), "Engineer")
        .setField(roleDesc.findFieldByName("level"), 3)
        .build();
    DynamicMessage r2 = DynamicMessage.newBuilder(roleDesc)
        .setField(roleDesc.findFieldByName("title"), "Manager")
        .setField(roleDesc.findFieldByName("level"), 5)
        .build();
    DynamicMessage m1 = DynamicMessage.newBuilder(memberDesc)
        .setField(memberDesc.findFieldByName(FIELD_NAME), VALUE_ALICE)
        .setField(memberDesc.findFieldByName("role"), r1)
        .build();
    DynamicMessage m2 = DynamicMessage.newBuilder(memberDesc)
        .setField(memberDesc.findFieldByName(FIELD_NAME), VALUE_BOB)
        .setField(memberDesc.findFieldByName("role"), r2)
        .build();
    DynamicMessage team = DynamicMessage.newBuilder(teamDesc)
        .setField(teamDesc.findFieldByName(FIELD_NAME), "Platform")
        .addRepeatedField(teamDesc.findFieldByName("members"), m1)
        .addRepeatedField(teamDesc.findFieldByName("members"), m2)
        .build();

    assertFullFidelityRoundTrip(topic, schema, team);
  }

  @Test
  public void testDefaultComplexRealisticSchemaAllAxesPass() {
    String topic = nextTopic();
    ProtobufSchema schema = new ProtobufSchema(KITCHEN_SINK_PROTO);
    Descriptor eventDesc = schema.toDescriptor();
    Descriptor addressDesc = eventDesc.findFieldByName("home_addr").getMessageType();
    Descriptor contactDesc = eventDesc.findFieldByName("contacts").getMessageType();
    Descriptor endpointDesc = eventDesc.findFieldByName("endpoint").getMessageType();
    Descriptor nestedDesc = eventDesc.findFieldByName("nested").getMessageType();
    FieldDescriptor attrsField = eventDesc.findFieldByName("attributes");
    Descriptor metaEntry = attrsField.getMessageType();
    // The map value type is Metadata (a message), not a plain string.
    Descriptor metadataDesc = metaEntry.findFieldByName(FIELD_VALUE).getMessageType();

    DynamicMessage homeAddr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "1 Home")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Hometown")
        .setField(addressDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage workAddr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "2 Work")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Worktown")
        .setField(addressDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage contactAddr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "3 Friend Ln")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Elsewhere")
        .setField(addressDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage contact = DynamicMessage.newBuilder(contactDesc)
        .setField(contactDesc.findFieldByName(FIELD_NAME), VALUE_BOB)
        .setField(contactDesc.findFieldByName("address"), contactAddr)
        .build();
    DynamicMessage endpoint = DynamicMessage.newBuilder(endpointDesc)
        .setField(endpointDesc.findFieldByName("host"), "api.local")
        .setField(endpointDesc.findFieldByName("port"), 443)
        .build();
    DynamicMessage nested = DynamicMessage.newBuilder(nestedDesc)
        .setField(nestedDesc.findFieldByName("label"), "n1")
        .setField(nestedDesc.findFieldByName("kind"),
            nestedDesc.findFieldByName("kind").getEnumType().findValueByName("B"))
        .build();

    DynamicMessage event = DynamicMessage.newBuilder(eventDesc)
        .setField(eventDesc.findFieldByName(FIELD_ID), "evt-1")
        .setField(eventDesc.findFieldByName("priority"),
            eventDesc.findFieldByName("priority").getEnumType().findValueByName("HIGH"))
        .setField(eventDesc.findFieldByName("home_addr"), homeAddr)
        .setField(eventDesc.findFieldByName("work_addr"), workAddr)
        .addRepeatedField(eventDesc.findFieldByName("contacts"), contact)
        .addRepeatedField(attrsField,
            DynamicMessage.newBuilder(metaEntry)
                .setField(metaEntry.findFieldByName(FIELD_KEY), "env")
                .setField(metaEntry.findFieldByName(FIELD_VALUE),
                    DynamicMessage.newBuilder(metadataDesc)
                        .setField(metadataDesc.findFieldByName(FIELD_KEY), "env-key")
                        .setField(metadataDesc.findFieldByName(FIELD_VALUE), "prod")
                        .build())
                .build())
        .setField(eventDesc.findFieldByName("endpoint"), endpoint)
        .setField(eventDesc.findFieldByName("nested"), nested)
        .build();

    assertFullFidelityRoundTrip(topic, schema, event);
  }

  @Test
  public void testIdempotencyKitchenSinkStableAcrossCycles() {
    Schema schema = SchemaBuilder.struct()
        .name("IdempotencyTarget")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field(FIELD_VALUE, Schema.INT32_SCHEMA)
        .field(FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema)
        .put(FIELD_ID, "idem-1")
        .put(FIELD_VALUE, 42)
        .put(FIELD_NAME, "stable");

    byte[] wire0 = plainConverter.fromConnectData(TOPIC, schema, value);
    int id0 = ByteBuffer.wrap(wire0, 1, 4).getInt();

    SchemaAndValue wrapped1 = converter.toConnectData(TOPIC, wire0);
    byte[] restored1 = converter.fromConnectData(TOPIC, wrapped1.schema(), wrapped1.value());
    int id1 = ByteBuffer.wrap(restored1, 1, 4).getInt();

    SchemaAndValue wrapped2 = converter.toConnectData(TOPIC, restored1);
    byte[] restored2 = converter.fromConnectData(TOPIC, wrapped2.schema(), wrapped2.value());
    int id2 = ByteBuffer.wrap(restored2, 1, 4).getInt();

    assertEquals("schema ID stable across cycle 1", id0, id1);
    assertEquals("schema ID stable across cycle 2", id1, id2);
    assertArrayEquals("wire bytes stable across cycles", restored1, restored2);

    Struct w1 = (Struct) wrapped1.value();
    Struct w2 = (Struct) wrapped2.value();
    assertEquals("wrapper raw schema stable",
        w1.getString(BackupWrapper.FIELD_RAW_SCHEMA),
        w2.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertEquals("wrapper schema ID stable",
        w1.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        w2.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
  }

  @Test
  public void testDefaultBytesFieldPreserved() {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message BytesRec {\n"
        + "  string id = 1;\n"
        + "  bytes payload = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    byte[] payload = new byte[]{0x00, 0x7F, (byte) 0x80, (byte) 0xFF};
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "b-1")
        .setField(desc.findFieldByName("payload"), ByteString.copyFrom(payload))
        .build();
    assertFullFidelityRoundTrip(topic, schema, msg);
  }

  @Test
  public void testDefaultEmptyCollectionsPreserved() {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message EmptyColl {\n"
        + "  string id = 1;\n"
        + "  repeated string tags = 2;\n"
        + "  map<string, int32> scores = 3;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "e-1")
        .build();
    assertFullFidelityRoundTrip(topic, schema, msg);
  }

  @Test
  public void testDefaultUnicodeInPayloadPreserved() {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message UniRec {\n"
        + "  string id = 1;\n"
        + "  string label = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "u-1")
        .setField(desc.findFieldByName("label"), "Hello world unicode: cafe naive")
        .build();
    assertFullFidelityRoundTrip(topic, schema, msg);
  }

  @Test
  public void testDefaultMapFieldSemanticEquivalencePermittedReorder() {
    // Protobuf wire format has no defined element order for maps, so byte fidelity is
    // not asserted here; multi-entry maps may re-emit entries in a different order.
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message MapRec {\n"
        + "  string id = 1;\n"
        + "  map<string, int32> scores = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    FieldDescriptor scoresField = desc.findFieldByName("scores");
    Descriptor entryDesc = scoresField.getMessageType();
    Map<String, Integer> scores = new HashMap<>();
    scores.put("engagement", 95);
    scores.put("risk", 10);
    scores.put("velocity", 42);
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "m-1");
    for (Map.Entry<String, Integer> e : scores.entrySet()) {
      builder.addRepeatedField(scoresField,
          DynamicMessage.newBuilder(entryDesc)
              .setField(entryDesc.findFieldByName(FIELD_KEY), e.getKey())
              .setField(entryDesc.findFieldByName(FIELD_VALUE), e.getValue())
              .build());
    }
    DynamicMessage msg = builder.build();

    byte[] originalBytes = serializer.serialize(topic, null, msg, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    Message originalDeser = deserializer.deserialize(topic, originalBytes);
    Message restoredDeser = deserializer.deserialize(topic, restoredBytes);
    assertValueEqual(originalDeser, restoredDeser);
  }

  @Test
  public void testDefaultPristineRestoreWithRawProtoProducer() throws Exception {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message PristineEvent {\n"
        + "  string id = 1;\n"
        + "  int32 count = 2;\n"
        + "  string note = 3;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "pristine-1")
        .setField(desc.findFieldByName("count"), 42)
        .setField(desc.findFieldByName("note"), "e2e")
        .build();

    byte[] sourceBytes = serializer.serialize(topic, null, msg, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWrapperSchemaIdMatchesSource(wrapped, sourceBytes);
    assertRawSchemaMatchesSourceRegistered(wrapped, sourceBytes);
    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertPayloadBytesEqual(sourceBytes, restoredBytes);
    assertBytesExact(sourceBytes, restoredBytes);
    Message originalDeser = deserializer.deserialize(topic, sourceBytes);
    Message restoredDeser = deserializer.deserialize(topic, restoredBytes);
    assertValueEqual(originalDeser, restoredDeser);
  }

  @Test
  public void testDefaultPristineRestoreMegaKitchenSinkAllTypesAndReferences() throws Exception {
    // wrapper.for.raw.primitives=false is required because the mega schema contains
    // google.protobuf.*Value fields; default true unwraps them and corrupts wire fidelity.
    converter.configure(backupConfigWith("wrapper.for.raw.primitives", "false"), false);

    String topic = nextTopic();

    String addressProto = "syntax = \"proto3\";\n"
        + "package io.confluent.test.shared;\n"
        + "message Address {\n"
        + "  string street = 1;\n"
        + "  string city = 2;\n"
        + "  string country = 3;\n"
        + "}\n";
    ProtobufSchema addressSchema = new ProtobufSchema(addressProto);
    schemaRegistry.register(ADDRESS_REFNAME, addressSchema);

    String personProto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "import \"shared/address.proto\";\n"
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "message MegaPerson {\n"
        + "  string id = 1;\n"
        + "  int32 age = 2;\n"
        + "  int64 balance_cents = 3;\n"
        + "  double rating = 4;\n"
        + "  bool active = 5;\n"
        + "  bytes payload = 6;\n"
        + "  Priority priority = 7;\n"
        + "  io.confluent.test.shared.Address home_addr = 8;\n"
        + "  io.confluent.test.shared.Address work_addr = 9;\n"
        + "  google.protobuf.StringValue nickname = 10;\n"
        + "  google.protobuf.Int32Value score = 11;\n"
        + "  google.protobuf.BoolValue verified = 12;\n"
        + "  optional string tagline = 13;\n"
        + "  repeated string tags = 14;\n"
        + "  map<string, int32> attrs = 15;\n"
        + "  oneof contact {\n"
        + "    string email = 16;\n"
        + "    string phone = 17;\n"
        + "  }\n"
        + "  Nested nested = 18;\n"
        + "  message Nested {\n"
        + "    string label = 1;\n"
        + "    enum Kind { A = 0; B = 1; C = 2; }\n"
        + "    Kind kind = 2;\n"
        + "  }\n"
        + "}\n"
        + "enum Priority { LOW = 0; MEDIUM = 1; HIGH = 2; }\n";
    SchemaReference addressRef =
        new SchemaReference(ADDRESS_REFNAME, ADDRESS_REFNAME, 1);
    Map<String, String> resolvedRefs = new HashMap<>();
    resolvedRefs.put(ADDRESS_REFNAME, addressProto);
    ProtobufSchema personSchema = new ProtobufSchema(
        personProto, Collections.singletonList(addressRef), resolvedRefs, null, null);
    schemaRegistry.register(topic + "-value", personSchema);

    Descriptor personDesc = personSchema.toDescriptor();
    Descriptor addressDesc = personDesc.findFieldByName("home_addr").getMessageType();
    Descriptor nestedDesc = personDesc.findFieldByName("nested").getMessageType();

    DynamicMessage home = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "1 Home Ln")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Hometown")
        .setField(addressDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage work = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "2 Work Ave")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Workville")
        .setField(addressDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage nested = DynamicMessage.newBuilder(nestedDesc)
        .setField(nestedDesc.findFieldByName("label"), "n1")
        .setField(nestedDesc.findFieldByName("kind"),
            nestedDesc.findFieldByName("kind").getEnumType().findValueByName("B"))
        .build();
    // Build wrapper submessages
    Descriptor strValDesc = personDesc.findFieldByName("nickname").getMessageType();
    Descriptor intValDesc = personDesc.findFieldByName("score").getMessageType();
    Descriptor boolValDesc = personDesc.findFieldByName("verified").getMessageType();
    DynamicMessage nickname = DynamicMessage.newBuilder(strValDesc)
        .setField(strValDesc.findFieldByName(FIELD_VALUE), "nick1").build();
    DynamicMessage score = DynamicMessage.newBuilder(intValDesc)
        .setField(intValDesc.findFieldByName(FIELD_VALUE), 100).build();
    DynamicMessage verified = DynamicMessage.newBuilder(boolValDesc)
        .setField(boolValDesc.findFieldByName(FIELD_VALUE), true).build();
    // Single-key map to avoid Protobuf-spec-permitted reorder
    FieldDescriptor attrsField = personDesc.findFieldByName("attrs");
    Descriptor attrsEntryDesc = attrsField.getMessageType();
    DynamicMessage attrEntry = DynamicMessage.newBuilder(attrsEntryDesc)
        .setField(attrsEntryDesc.findFieldByName(FIELD_KEY), "env")
        .setField(attrsEntryDesc.findFieldByName(FIELD_VALUE), 1)
        .build();

    DynamicMessage person = DynamicMessage.newBuilder(personDesc)
        .setField(personDesc.findFieldByName(FIELD_ID), "person-1")
        .setField(personDesc.findFieldByName("age"), 30)
        .setField(personDesc.findFieldByName("balance_cents"), 100_000L)
        .setField(personDesc.findFieldByName("rating"), 4.5)
        .setField(personDesc.findFieldByName("active"), true)
        .setField(personDesc.findFieldByName("payload"),
            ByteString.copyFrom(new byte[]{0x00, 0x7F, (byte) 0x80, (byte) 0xFF}))
        .setField(personDesc.findFieldByName("priority"),
            personDesc.findFieldByName("priority").getEnumType().findValueByName("HIGH"))
        .setField(personDesc.findFieldByName("home_addr"), home)
        .setField(personDesc.findFieldByName("work_addr"), work)
        .setField(personDesc.findFieldByName("nickname"), nickname)
        .setField(personDesc.findFieldByName("score"), score)
        .setField(personDesc.findFieldByName("verified"), verified)
        .setField(personDesc.findFieldByName("tagline"), "Mega tagline")
        .addRepeatedField(personDesc.findFieldByName("tags"), "premium")
        .addRepeatedField(attrsField, attrEntry)
        .setField(personDesc.findFieldByName("email"), "person@x.com")
        .setField(personDesc.findFieldByName("nested"), nested)
        .build();

    byte[] sourceBytes = serializer.serialize(topic, null, person, personSchema);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWrapperSchemaIdMatchesSource(wrapped, sourceBytes);
    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertPayloadBytesEqual(sourceBytes, restoredBytes);
    assertBytesExact(sourceBytes, restoredBytes);

    // Cross-subject reference metadata explicitly captured on the wrapper.
    Struct wrapperStruct = (Struct) wrapped.value();
    String refTree = wrapperStruct.getString(BackupWrapper.FIELD_REFERENCE_TREE);
    String directRefs = wrapperStruct.getString(BackupWrapper.FIELD_DIRECT_REFS);
    assertNotNull("mega wrapper captures reference tree", refTree);
    assertNotNull("mega wrapper captures direct refs", directRefs);
    assertTrue("reference tree mentions Address refName: " + refTree,
        refTree.contains(ADDRESS_REFNAME));
    assertTrue("direct refs mention Address subject: " + directRefs,
        directRefs.contains(ADDRESS_REFNAME));

    // Source SR still resolves both the main schema (with its reference) and the Address schema.
    ParsedSchema registeredPerson = schemaRegistry.getLatestSchemaMetadata(topic + "-value") != null
        ? schemaRegistry.getSchemaBySubjectAndId(topic + "-value",
            schemaRegistry.getLatestSchemaMetadata(topic + "-value").getId())
        : null;
    assertNotNull("Person schema resolvable from source SR after restore", registeredPerson);
    List<SchemaReference> personRefs = registeredPerson.references();
    assertEquals("Person has one direct reference (Address)", 1, personRefs.size());
    assertEquals("Person reference refName matches Address",
        ADDRESS_REFNAME, personRefs.get(0).getName());
    assertEquals("Person reference subject matches Address",
        ADDRESS_REFNAME, personRefs.get(0).getSubject());
    assertNotNull("Address schema resolvable from source SR by subject",
        schemaRegistry.getLatestSchemaMetadata(ADDRESS_REFNAME));

    // Semantic: restored bytes still parse via the same schema + reference chain.
    Message restoredDeser = deserializer.deserialize(topic, restoredBytes);
    assertNotNull("restored bytes parseable via schema", restoredDeser);
    assertEquals("restored bytes deserialize to the same message",
        person.toString(), restoredDeser.toString());
  }

  private static final String ADDRESS_SCHEMA_PROTO =
      "syntax = \"proto3\";\n"
          + "package io.confluent.test.shared;\n"
          + "message Address {\n"
          + "  string street = 1;\n"
          + "  string city = 2;\n"
          + "  string country = 3;\n"
          + "}\n";
  // For Protobuf references, both the refName and the SR subject use the .proto filename.
  private static final String ADDRESS_REFNAME = "shared/address.proto";
  private static final String ADDRESS_SUBJECT = "shared/address.proto";

  private static final String PERSON_WITH_REF_SCHEMA_PROTO =
      "syntax = \"proto3\";\n"
          + "package io.confluent.test;\n"
          + "import \"shared/address.proto\";\n"
          + "message PersonRef {\n"
          + "  string name = 1;\n"
          + "  io.confluent.test.shared.Address home_addr = 2;\n"
          + "  io.confluent.test.shared.Address work_addr = 3;\n"
          + "}\n";

  private RefTestFixture registerAndProducePersonWithAddressRef() throws Exception {
    String topic = nextTopic();
    ProtobufSchema addressSchema = new ProtobufSchema(ADDRESS_SCHEMA_PROTO);
    schemaRegistry.register(ADDRESS_SUBJECT, addressSchema);
    SchemaReference addressRef = new SchemaReference(ADDRESS_REFNAME, ADDRESS_SUBJECT, 1);
    Map<String, String> resolved = new HashMap<>();
    resolved.put(ADDRESS_REFNAME, ADDRESS_SCHEMA_PROTO);
    ProtobufSchema personSchema = new ProtobufSchema(
        PERSON_WITH_REF_SCHEMA_PROTO, Collections.singletonList(addressRef),
        resolved, null, null);
    schemaRegistry.register(topic + "-value", personSchema);

    Descriptor personDesc = personSchema.toDescriptor();
    Descriptor addressDesc = personDesc.findFieldByName("home_addr").getMessageType();
    DynamicMessage addr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "1 Main St")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Springfield")
        .setField(addressDesc.findFieldByName(FIELD_COUNTRY), VALUE_US)
        .build();
    DynamicMessage person = DynamicMessage.newBuilder(personDesc)
        .setField(personDesc.findFieldByName(FIELD_NAME), VALUE_ALICE)
        .setField(personDesc.findFieldByName("home_addr"), addr)
        .setField(personDesc.findFieldByName("work_addr"), addr)
        .build();
    KafkaProtobufSerializer<DynamicMessage> refSerializer =
        newReferenceAwareProtobufSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, person);
    return new RefTestFixture(topic, sourceBytes);
  }

  private static final class RefTestFixture {
    final String topic;
    final byte[] sourceBytes;
    RefTestFixture(String topic, byte[] sourceBytes) {
      this.topic = topic;
      this.sourceBytes = sourceBytes;
    }
  }

  @Test
  public void testDefaultPristineRestoreCrossClusterWithCrossSubjectReferences()
      throws Exception {
    RefTestFixture fx = registerAndProducePersonWithAddressRef();
    int sourceWireId = ByteBuffer.wrap(fx.sourceBytes, 1, 4).getInt();
    String sourcePersonCanonical =
        schemaRegistry.getSchemaById(sourceWireId).canonicalString();

    SchemaAndValue wrapped = converter.toConnectData(fx.topic, fx.sourceBytes);

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWrapperSchemaIdMatchesSource(wrapped, fx.sourceBytes);
    Struct wrapperStruct = (Struct) wrapped.value();
    assertEquals("wrapper rawSchema equals source-registered canonical form",
        sourcePersonCanonical,
        wrapperStruct.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    String refTree = wrapperStruct.getString(BackupWrapper.FIELD_REFERENCE_TREE);
    String directRefs = wrapperStruct.getString(BackupWrapper.FIELD_DIRECT_REFS);
    assertNotNull("reference tree captured", refTree);
    assertNotNull("direct refs captured", directRefs);
    assertTrue("reference tree mentions Address refName: " + refTree,
        refTree.contains(ADDRESS_REFNAME));
    assertTrue("direct refs mention Address subject: " + directRefs,
        directRefs.contains(ADDRESS_SUBJECT));

    assertTrue("target SR is empty before restore",
        targetSchemaRegistry.getAllSubjects().isEmpty());
    byte[] restoredBytes = targetConverter.fromConnectData(
        fx.topic, wrapped.schema(), wrapped.value());

    assertTrue("target SR has Address subject",
        targetSchemaRegistry.getAllSubjects().contains(ADDRESS_SUBJECT));
    assertTrue("target SR has Person subject",
        targetSchemaRegistry.getAllSubjects().contains(fx.topic + "-value"));

    assertCrossClusterBytesEquivalent(fx.sourceBytes, restoredBytes);

    int targetWireId = ByteBuffer.wrap(restoredBytes, 1, 4).getInt();
    ParsedSchema targetPersonSchema = targetSchemaRegistry.getSchemaById(targetWireId);
    assertNotNull("target SR resolves the restored wire ID", targetPersonSchema);
    assertEquals("target Person canonical == source Person canonical",
        sourcePersonCanonical, targetPersonSchema.canonicalString());
    List<SchemaReference> targetRefs = targetPersonSchema.references();
    assertEquals("target Person has one direct reference (Address)", 1, targetRefs.size());
    assertEquals("target ref refName matches source",
        ADDRESS_REFNAME, targetRefs.get(0).getName());
    assertEquals("target ref subject matches source", ADDRESS_SUBJECT,
        targetRefs.get(0).getSubject());
    assertTrue("target ref version is a positive integer",
        targetRefs.get(0).getVersion() > 0);

    Message sourceValue = deserializer.deserialize(fx.topic, fx.sourceBytes);
    Message targetValue = targetDeserializer.deserialize(fx.topic, restoredBytes);
    assertNotNull("source deserialized non-null", sourceValue);
    assertNotNull("target deserialized non-null", targetValue);
    assertEquals("Protobuf textual form of restored record equals source's",
        sourceValue.toString(), targetValue.toString());
  }

  // ================ Cross-subject reference negatives ================

  @Test
  public void testEdgeReferenceTreeMissingEntryThrows() throws Exception {
    RefTestFixture fx = registerAndProducePersonWithAddressRef();
    SchemaAndValue wrapped = converter.toConnectData(fx.topic, fx.sourceBytes);

    Struct original = (Struct) wrapped.value();
    Schema wrapperSchema = wrapped.schema();
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        original.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        original.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION),
        original.getString(BackupWrapper.FIELD_SCHEMA_TYPE),
        original.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        original.getString(BackupWrapper.FIELD_RAW_SCHEMA),
        "{}",
        original.getString(BackupWrapper.FIELD_DIRECT_REFS));
    Struct badWrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.get(BackupWrapper.FIELD_DATA), fields);

    try {
      converter.fromConnectData(fx.topic, wrapperSchema, badWrapper);
      fail("Expected DataException for empty reference tree with populated direct refs");
    } catch (DataException e) {
      assertEquals("Corrupt backup wrapper: partial reference metadata "
          + "(treeEmpty=true, directRefsEmpty=false)", e.getMessage());
    }
  }

  @Test
  public void testEdgeCorruptReferenceTreeJsonThrows() throws Exception {
    RefTestFixture fx = registerAndProducePersonWithAddressRef();
    SchemaAndValue wrapped = converter.toConnectData(fx.topic, fx.sourceBytes);

    Struct original = (Struct) wrapped.value();
    Schema wrapperSchema = wrapped.schema();
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        original.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        original.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION),
        original.getString(BackupWrapper.FIELD_SCHEMA_TYPE),
        original.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        original.getString(BackupWrapper.FIELD_RAW_SCHEMA),
        "{ not valid json",
        original.getString(BackupWrapper.FIELD_DIRECT_REFS));
    Struct badWrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.get(BackupWrapper.FIELD_DATA), fields);

    try {
      converter.fromConnectData(fx.topic, wrapperSchema, badWrapper);
      fail("Expected DataException on corrupt reference tree JSON");
    } catch (DataException e) {
      assertEquals("Cannot parse reference tree JSON for restore. "
          + "Backup metadata may be corrupt.", e.getMessage());
    }
  }

  @Test
  public void testEdgeDirectRefsWithoutReferenceTreeThrows() throws Exception {
    RefTestFixture fx = registerAndProducePersonWithAddressRef();
    SchemaAndValue wrapped = converter.toConnectData(fx.topic, fx.sourceBytes);

    Struct original = (Struct) wrapped.value();
    Schema wrapperSchema = wrapped.schema();
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        original.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        original.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION),
        original.getString(BackupWrapper.FIELD_SCHEMA_TYPE),
        original.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        original.getString(BackupWrapper.FIELD_RAW_SCHEMA),
        null,
        original.getString(BackupWrapper.FIELD_DIRECT_REFS));
    Struct badWrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.get(BackupWrapper.FIELD_DATA), fields);

    try {
      converter.fromConnectData(fx.topic, wrapperSchema, badWrapper);
      fail("Expected DataException on directRefs-without-reference-tree corruption");
    } catch (DataException e) {
      assertEquals("Corrupt backup wrapper: partial reference metadata "
          + "(treeEmpty=true, directRefsEmpty=false)", e.getMessage());
    }
  }

  // ================ Config-lossiness pairs (same config on/off) ================

  @Test
  public void testEnumNamesIntForEnumsOffSurvive() {
    // Use default configuration (int.for.enums defaults to false).
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message Event {\n"
        + "  string id = 1;\n"
        + "  EventType type = 2;\n"
        + "}\n"
        + "enum EventType { UNKNOWN = 0; CLICK = 1; VIEW = 2; PURCHASE = 3; }\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    DynamicMessage event = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "evt-1")
        .setField(desc.findFieldByName("type"),
            desc.findFieldByName("type").getEnumType().findValueByName("PURCHASE"))
        .build();

    assertFullFidelityRoundTrip(topic, schema, event);
    // Confirm the raw schema still declares the enum symbol names.
    byte[] originalBytes = serializer.serialize(topic, null, event, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertTrue("raw schema retains enum symbol PURCHASE", raw.contains("PURCHASE"));
  }

  @Test
  public void testEnumNamesIntForEnumsOnConnectSchemaLosesSymbols() {
    // Re-configure with int.for.enums=true
    converter.configure(backupConfigWith("int.for.enums", "true"), false);

    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message Event {\n"
        + "  string id = 1;\n"
        + "  EventType type = 2;\n"
        + "}\n"
        + "enum EventType { UNKNOWN = 0; CLICK = 1; VIEW = 2; PURCHASE = 3; }\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    DynamicMessage event = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "evt-1")
        .setField(desc.findFieldByName("type"),
            desc.findFieldByName("type").getEnumType().findValueByName("PURCHASE"))
        .build();

    byte[] originalBytes = serializer.serialize(topic, null, event, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    // Bytes and wire ID still preserved. restore path uses the raw .proto.
    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    // Raw .proto text is still source-of-truth so it still contains the symbols.
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertTrue("raw .proto DSL still contains PURCHASE", raw.contains("PURCHASE"));
  }

  @Test
  public void testConnectMetaDataOnSharedTypeSurvives() {
    // Default config. connect.meta.data defaults to true.
    testDefaultSharedMessageTypeAtMultipleFieldsSurvives();
  }

  @Test
  public void testConnectMetaDataOffConnectSchemaLosesTags() {
    converter.configure(backupConfigWith("connect.meta.data", "false"), false);

    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message Person {\n"
        + "  string name = 1;\n"
        + "  Address home_addr = 2;\n"
        + "  Address work_addr = 3;\n"
        + "}\n"
        + "message Address {\n"
        + "  string street = 1;\n"
        + "  string city = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor personDesc = schema.toDescriptor();
    Descriptor addressDesc = personDesc.findFieldByName("home_addr").getMessageType();

    DynamicMessage homeAddr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "1 Home")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Hometown")
        .build();
    DynamicMessage workAddr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName(FIELD_STREET), "2 Work")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Worktown")
        .build();
    DynamicMessage person = DynamicMessage.newBuilder(personDesc)
        .setField(personDesc.findFieldByName(FIELD_NAME), VALUE_ALICE)
        .setField(personDesc.findFieldByName("home_addr"), homeAddr)
        .setField(personDesc.findFieldByName("work_addr"), workAddr)
        .build();

    byte[] originalBytes = serializer.serialize(topic, null, person, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    // Converter-only roundtrip: raw .proto restores wire bytes fine.
    assertBytesExact(originalBytes, restoredBytes);
    // But the Connect schema built from these bytes lacks field-tag parameters,
    // which is what an AvroFormat writer would need for the shared-type case.
  }

  @Test
  public void testWrapperForRawPrimitivesFalsePreservesNestedWrappers() throws Exception {
    converter.configure(backupConfigWith("wrapper.for.raw.primitives", "false"), false);

    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "message WrapMsg {\n"
        + "  string id = 1;\n"
        + "  google.protobuf.StringValue nickname = 2;\n"
        + "  google.protobuf.Int32Value score = 3;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    Descriptor strValDesc = desc.findFieldByName("nickname").getMessageType();
    Descriptor intValDesc = desc.findFieldByName("score").getMessageType();
    DynamicMessage nickname = DynamicMessage.newBuilder(strValDesc)
        .setField(strValDesc.findFieldByName(FIELD_VALUE), "nick").build();
    DynamicMessage score = DynamicMessage.newBuilder(intValDesc)
        .setField(intValDesc.findFieldByName(FIELD_VALUE), 42).build();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "w-1")
        .setField(desc.findFieldByName("nickname"), nickname)
        .setField(desc.findFieldByName("score"), score)
        .build();

    byte[] originalBytes = serializer.serialize(topic, null, msg, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    // Semantic: restored bytes parse as the same message with wrapper fields intact
    Message restoredDeser = deserializer.deserialize(topic, restoredBytes);
    assertEquals("nickname wrapper preserved after restore",
        msg.toString(), restoredDeser.toString());
  }

  @Test
  public void testWrapperForRawPrimitivesTrueDefaultCorruptsNestedWrappers() throws Exception {
    // Default true unwraps nested google.protobuf.*Value wrappers during toConnectData;
    // restored bytes differ from the original and may not deserialize via the same schema.
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "message WrapMsgCorrupt {\n"
        + "  string id = 1;\n"
        + "  google.protobuf.StringValue nickname = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    Descriptor strValDesc = desc.findFieldByName("nickname").getMessageType();
    DynamicMessage nickname = DynamicMessage.newBuilder(strValDesc)
        .setField(strValDesc.findFieldByName(FIELD_VALUE), "nick").build();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "corrupt-1")
        .setField(desc.findFieldByName("nickname"), nickname)
        .build();

    byte[] originalBytes = serializer.serialize(topic, null, msg, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    // Wire schema ID is preserved (SR content dedupe), but bytes differ because nested
    // wrappers get unwrapped to primitives.
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    assertFalse("restored bytes differ from original (wrappers unwrapped)",
        Arrays.equals(originalBytes, restoredBytes));
  }

  // ================ Edge cases and error handling ================

  @Test
  public void testEdgeNullValue() {
    SchemaAndValue result = converter.toConnectData(TOPIC, null);
    assertNull(result.schema());
    assertNull(result.value());
  }

  @Test
  public void testEdgeBackupDisabledNoWrapping() {
    Schema schema = SchemaBuilder.struct()
        .name("NoBackup")
        .field("x", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("x", 1);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = plainConverter.toConnectData(TOPIC, serialized);

    assertNotNull(result.schema());
    assertNotEquals(BackupWrapper.NAME, result.schema().name());
  }

  @Test
  public void testEdgeNonWrapperSchemaSerializesNormally() {
    Schema schema = SchemaBuilder.struct()
        .name("DirectMsg")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "direct");

    byte[] result = converter.fromConnectData(TOPIC, schema, value);
    assertNotNull(result);
    assertTrue(result.length > 5);
    assertEquals(0x00, result[0]);
  }

  @Test
  public void testEdgeMissingDataFieldThrows() {
    Schema badSchema = SchemaBuilder.struct()
        .name(BackupWrapper.NAME)
        .field(BackupWrapper.FIELD_SCHEMA_ID, Schema.INT32_SCHEMA)
        .build();
    Struct badWrapper = new Struct(badSchema).put(BackupWrapper.FIELD_SCHEMA_ID, 1);

    try {
      converter.fromConnectData(TOPIC, badSchema, badWrapper);
      fail("Expected DataException for wrapper missing 'data' field");
    } catch (DataException e) {
      assertEquals("Malformed backup wrapper: missing '" + BackupWrapper.FIELD_DATA + "' field",
          e.getMessage());
    }
  }

  @Test
  public void testEdgeNullRawSchemaThrows() {
    Schema schema = SchemaBuilder.struct()
        .name("Fallback")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "fallback");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);

    Struct original = (Struct) wrapped.value();
    Schema wrapperSchema = wrapped.schema();
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        original.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        original.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION),
        original.getString(BackupWrapper.FIELD_SCHEMA_TYPE),
        original.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        null, null, null);
    Struct modifiedWrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.get(BackupWrapper.FIELD_DATA), fields);

    try {
      converter.fromConnectData(TOPIC, wrapperSchema, modifiedWrapper);
      fail("Expected DataException for null rawSchema");
    } catch (DataException e) {
      assertTrue("message references rawSchema field: " + e.getMessage(),
          e.getMessage().contains(BackupWrapper.FIELD_RAW_SCHEMA));
      assertTrue("message mentions pristine restore: " + e.getMessage(),
          e.getMessage().contains("pristine restore"));
    }
  }

  @Test
  public void testEdgeBasicRoundtripDeserializes() {
    Schema schema = SchemaBuilder.struct()
        .name("RoundTripMsg")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field(FIELD_VALUE, Schema.INT32_SCHEMA)
        .build();
    Struct original = new Struct(schema)
        .put(FIELD_NAME, "test")
        .put(FIELD_VALUE, 99);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, original);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    SchemaAndValue restoredData = plainConverter.toConnectData(TOPIC, restoredBytes);
    Struct restored = (Struct) restoredData.value();
    assertEquals("test", restored.getString(FIELD_NAME));
    assertEquals(Integer.valueOf(99), restored.getInt32(FIELD_VALUE));
  }

  @Test
  public void testEdgeRestoreProducesMagicByte() {
    Schema schema = SchemaBuilder.struct()
        .name("SimpleMsg")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "hello");

    byte[] original = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, original);
    byte[] restored = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertNotNull(restored);
    assertTrue("restored bytes non-empty", restored.length > 0);
    assertEquals("restored bytes start with magic byte", 0x00, restored[0]);
  }

  @Test
  public void testEdgeSchemaTypeMismatchThrows() {
    Schema schema = SchemaBuilder.struct()
        .name("Mismatch")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "type-mismatch");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);

    Struct original = (Struct) wrapped.value();
    Schema wrapperSchema = wrapped.schema();
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        original.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        original.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION),
        "AVRO",
        original.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        original.getString(BackupWrapper.FIELD_RAW_SCHEMA),
        null, null);
    Struct badWrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.get(BackupWrapper.FIELD_DATA), fields);

    try {
      converter.fromConnectData(TOPIC, wrapperSchema, badWrapper);
      fail("Expected DataException on schema type mismatch");
    } catch (DataException e) {
      assertEquals("ProtobufConverter received wrapper with schemaType='AVRO', "
          + "expected 'PROTOBUF'", e.getMessage());
    }
  }

  @Test
  public void testEdgeCorruptedWireIdRejected() {
    Schema schema = SchemaBuilder.struct()
        .name("Corrupt")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "corrupt");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    byte[] corrupted = originalBytes.clone();
    ByteBuffer.wrap(corrupted, 1, 4).putInt(Integer.MAX_VALUE - 1);

    try {
      converter.toConnectData(TOPIC, corrupted);
      fail("Expected DataException for corrupted wire schema ID");
    } catch (DataException e) {
      assertEquals("Failed to deserialize data for topic " + TOPIC + " to Protobuf:",
          e.getMessage());
    }
  }

  @Test
  public void testEdgeEmptyPayloadHandled() {
    byte[] empty = new byte[0];
    try {
      converter.toConnectData(TOPIC, empty);
      fail("Expected DataException for zero-length payload");
    } catch (DataException e) {
      assertEquals("Failed to deserialize data for topic " + TOPIC + " to Protobuf:",
          e.getMessage());
    }
  }

  @Test
  public void testEdgeSinkWrapErrorClassifiedAsBackupException() throws Exception {
    // A SR client that fetches individual schemas normally (so deserialize succeeds)
    // but throws on getAllVersionsById, which only the wrap path uses.
    SchemaRegistryClient wrapFailingSr = new MockSchemaRegistryClient(
        ImmutableList.of(new ProtobufSchemaProvider())) {
      @Override
      public Collection<SubjectVersion> getAllVersionsById(int id) {
        throw new SerializationException("simulated SR unavailable during wrap");
      }
    };
    ProtobufConverter wrapFailingConverter = new ProtobufConverter(wrapFailingSr);
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_KEY, FAKE_SR_URL);
    cfg.put(BACKUP_ENABLED_KEY, "true");
    wrapFailingConverter.configure(cfg, false);

    String topic = nextTopic();
    String protoDef = "syntax = \"proto3\"; package io.confluent.test; "
        + "message WrapFail { string id = 1; }";
    ProtobufSchema protoSchema = new ProtobufSchema(protoDef);
    wrapFailingSr.register(topic + "-value", protoSchema);
    Descriptor desc = protoSchema.toDescriptor();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("id"), "wrap-fail").build();
    KafkaProtobufSerializer<DynamicMessage> refSerializer =
        newReferenceAwareProtobufSerializer(wrapFailingSr);
    byte[] bytes = refSerializer.serialize(topic, null, msg, protoSchema);

    try {
      wrapFailingConverter.toConnectData(topic, bytes);
      fail("Expected DataException when wrap-path SR call fails");
    } catch (DataException e) {
      assertEquals("Failed to wrap Protobuf backup for topic " + topic, e.getMessage());
      assertTrue("cause is the simulated SerializationException",
          e.getCause() instanceof SerializationException);
    }
  }

  @Test
  public void testHeaderSchemaIdSerializerBackupCapturesSchemaId() {
    SchemaRegistryClient sr = new MockSchemaRegistryClient(
        ImmutableList.of(new ProtobufSchemaProvider()));
    ProtobufConverter headerBackupConverter = new ProtobufConverter(sr);
    ProtobufConverter headerPlainConverter = new ProtobufConverter(sr);
    Map<String, Object> backupCfg = new HashMap<>();
    backupCfg.put(SR_URL_KEY, FAKE_SR_URL);
    backupCfg.put(BACKUP_ENABLED_KEY, "true");
    backupCfg.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    backupCfg.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_DESERIALIZER,
        "io.confluent.kafka.serializers.schema.id.DualSchemaIdDeserializer");
    headerBackupConverter.configure(backupCfg, false);
    Map<String, Object> plainCfg = new HashMap<>(backupCfg);
    plainCfg.remove(BACKUP_ENABLED_KEY);
    headerPlainConverter.configure(plainCfg, false);

    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.HeaderIdRecord")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "h-1").put(FIELD_NAME, "header-test");

    Headers headers = new RecordHeaders();
    byte[] serialized = headerPlainConverter.fromConnectData(TOPIC, headers, schema, value);
    assertNotNull("value schema ID header present",
        headers.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER));

    SchemaAndValue wrapped = headerBackupConverter.toConnectData(TOPIC, headers, serialized);

    assertNotNull("wrapper produced", wrapped);
    assertEquals("wrapper schema name", BackupWrapper.NAME, wrapped.schema().name());
    Struct w = (Struct) wrapped.value();
    assertEquals("schema type", SCHEMA_TYPE, w.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull("raw schema captured", w.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertNotNull("wrapper populates schemaGuid from header",
        w.getString(BackupWrapper.FIELD_SCHEMA_GUID));

    Headers restoredHeaders = new RecordHeaders();
    byte[] restoredBytes = headerBackupConverter.fromConnectData(
        TOPIC, restoredHeaders, wrapped.schema(), wrapped.value());
    assertNotNull("restored value schema ID header present",
        restoredHeaders.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER));

    SchemaAndValue restoredDeser = headerPlainConverter.toConnectData(
        TOPIC, restoredHeaders, restoredBytes);
    Struct restored = (Struct) restoredDeser.value();
    assertEquals("id preserved", "h-1", restored.getString(FIELD_ID));
    assertEquals("name preserved", "header-test", restored.getString(FIELD_NAME));
  }

  // ================ Config-mismatch scenarios (backup with X, restore with X flipped) ================

  @Test
  public void testConfigMismatchWrapperForRawPrimitivesFalseOnSinkOnlyStillWorks()
      throws Exception {
    // Sink sets wrapper.for.raw.primitives=false; source uses default (true). Restore
    // reconstructs the schema from the wrapper's raw .proto, so source-side default is safe.
    converter.configure(backupConfigWith("wrapper.for.raw.primitives", "false"), false);

    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "message MismatchMsg {\n"
        + "  string id = 1;\n"
        + "  google.protobuf.StringValue nickname = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    Descriptor strValDesc = desc.findFieldByName("nickname").getMessageType();
    DynamicMessage nickname = DynamicMessage.newBuilder(strValDesc)
        .setField(strValDesc.findFieldByName(FIELD_VALUE), "nick").build();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "mm-1")
        .setField(desc.findFieldByName("nickname"), nickname)
        .build();

    byte[] originalBytes = serializer.serialize(topic, null, msg, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);

    // Flip the same converter to default (true). represents source-side default config.
    converter.configure(backupConfigWith("wrapper.for.raw.primitives", "true"), false);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }

  @Test
  public void testConfigMismatchWrapperForNullablesTrueOnSinkFailsOnProto3Optional() {
    // wrapper.for.nullables=true does not mark proto3 explicit optional scalars as .optional()
    // in the derived Connect schema, so records that omit the field fail Struct validation.
    converter.configure(backupConfigWith("wrapper.for.nullables", "true"), false);

    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message OptionalMsg {\n"
        + "  string id = 1;\n"
        + "  optional string tagline = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    // Record OMITS 'tagline' (proto3 explicit optional not set)
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "no-tagline")
        .build();
    byte[] originalBytes = serializer.serialize(topic, null, msg, schema);

    try {
      converter.toConnectData(topic, originalBytes);
      fail("Expected DataException on proto3 optional + wrapper.for.nullables=true "
          + "with tagline field absent from the record");
    } catch (DataException e) {
      assertTrue("message mentions required-field violation: " + e.getMessage(),
          e.getMessage().contains("Invalid value: null used for required field"));
      assertTrue("message identifies the tagline field: " + e.getMessage(),
          e.getMessage().contains("tagline"));
    }
  }

  @Test
  public void testConfigMismatchConnectMetaDataOnRestoreOffDocumentedLoss() {
    String topic = nextTopic();
    String proto = PROTO_HEADER
        + "message MetaMismatch {\n"
        + "  string id = 1;\n"
        + "  int32 count = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "meta-1")
        .setField(desc.findFieldByName("count"), 7)
        .build();

    byte[] originalBytes = serializer.serialize(topic, null, msg, schema);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);

    // Flip restore side to connect.meta.data=false; wrapper's raw .proto still drives restore.
    converter.configure(backupConfigWith("connect.meta.data", "false"), false);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }
}
