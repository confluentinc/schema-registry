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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
  private static final String FIELD_NAME = "name";
  private static final String FIELD_TEXT = "text";
  private static final String FIELD_VALUE = "value";
  private static final String FIELD_DATA = "data";
  private static final String FIELD_KEY = "key";
  private static final String FIELD_ID = "id";
  private static final String FIELD_CITY = "city";
  private static final String FIELD_HOST = "host";
  private static final String FIELD_PORT = "port";
  private static final String VALUE_ALICE = "Alice";
  private static final String VALUE_BOB = "Bob";

  private final SchemaRegistryClient schemaRegistry;
  private final ProtobufConverter converter;
  private final ProtobufConverter plainConverter;
  private KafkaProtobufSerializer<DynamicMessage> serializer;
  private KafkaProtobufDeserializer<DynamicMessage> deserializer;
  private int topicCounter = 0;

  public ProtobufConverterBackupTest() {
    schemaRegistry = new MockSchemaRegistryClient(
        ImmutableList.of(new ProtobufSchemaProvider()));
    converter = new ProtobufConverter(schemaRegistry);
    plainConverter = new ProtobufConverter(schemaRegistry);
  }

  @Before
  public void setUp() {
    Map<String, Object> backupConfig = new HashMap<>();
    backupConfig.put("schema.registry.url", "http://fake-url");
    backupConfig.put("schema.backup.enabled", "true");
    converter.configure(backupConfig, false);

    plainConverter.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"),
        false);

    serializer = new KafkaProtobufSerializer<>(schemaRegistry);
    serializer.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"),
        false);

    deserializer = new KafkaProtobufDeserializer<>(schemaRegistry);
    deserializer.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"),
        false);
  }

  private String nextTopic() {
    return TOPIC + "-" + (topicCounter++);
  }

  private void assertBackupRoundTrip(String topic, ProtobufSchema schema,
      DynamicMessage message) {
    byte[] originalBytes = serializer.serialize(topic, null, message, schema);
    assertNotNull("Serialization should produce bytes", originalBytes);

    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    assertEquals(BackupWrapper.NAME, wrapped.schema().name());
    Struct wrapper = (Struct) wrapped.value();
    assertNotNull("Raw schema should be captured",
        wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));

    byte[] restoredBytes = converter.fromConnectData(
        topic, wrapped.schema(), wrapped.value());
    assertNotNull("Restored bytes should not be null", restoredBytes);

    Message original = deserializer.deserialize(topic, originalBytes);
    Message restored = deserializer.deserialize(topic, restoredBytes);
    assertEquals("Restored protobuf message must equal original", original, restored);
    assertFalse("Restored bytes should not be empty", restoredBytes.length == 0);
  }

  @Test
  public void testBackupToConnectDataWrapsMetadata() {
    Schema schema = SchemaBuilder.struct()
        .name("TestRecord")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field("count", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema)
        .put(FIELD_ID, "rec-1")
        .put("count", 10);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    assertNotNull(result);
    assertNotNull(result.schema());
    assertEquals(BackupWrapper.NAME, result.schema().name());

    Struct wrapper = (Struct) result.value();
    assertNotNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertEquals(SCHEMA_TYPE, wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertNotNull(wrapper.get(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testBackupFromConnectDataRestores() {
    Schema schema = SchemaBuilder.struct()
        .name("SimpleMsg")
        .field(FIELD_TEXT, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_TEXT, "hello");

    byte[] original = plainConverter.fromConnectData(TOPIC, schema, value);
    assertNotNull(original);

    SchemaAndValue wrapped = converter.toConnectData(TOPIC, original);
    assertEquals(BackupWrapper.NAME, wrapped.schema().name());

    byte[] restored = converter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    assertNotNull(restored);
    assertEquals(0x00, restored[0]);
  }

  @Test
  public void testBackupRoundTrip() {
    Schema schema = SchemaBuilder.struct()
        .name("RoundTripMsg")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field(FIELD_VALUE, Schema.INT32_SCHEMA)
        .build();
    Struct original = new Struct(schema)
        .put(FIELD_NAME, "test")
        .put(FIELD_VALUE, 99);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, original);

    // Backup wraps, then restore unwraps
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    SchemaAndValue restoredData = plainConverter.toConnectData(TOPIC, restoredBytes);
    Struct restored = (Struct) restoredData.value();
    assertEquals("test", restored.getString(FIELD_NAME));
    assertEquals(Integer.valueOf(99), restored.getInt32(FIELD_VALUE));
  }

  @Test
  public void testBackupDisabledNoWrapping() {
    Schema schema = SchemaBuilder.struct()
        .name("NoBackupMsg")
        .field("x", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("x", 1);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = plainConverter.toConnectData(TOPIC, serialized);

    assertNotNull(result.schema());
    assertNotEquals(BackupWrapper.NAME, result.schema().name());
  }

  @Test
  public void testBackupNullValue() {
    SchemaAndValue result = converter.toConnectData(TOPIC, null);
    assertNull(result.schema());
    assertNull(result.value());
  }

  @Test
  public void testBackupPrimitiveType() {
    Schema schema = SchemaBuilder.struct()
        .name("PrimitiveHolder")
        .field("val", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("val", "test");

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    assertEquals(BackupWrapper.NAME, result.schema().name());
    Struct wrapper = (Struct) result.value();
    assertEquals(SCHEMA_TYPE, wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
  }

  @Test
  public void testBackupWrapperFields() {
    Schema schema = SchemaBuilder.struct()
        .name("FieldCheck")
        .field(FIELD_DATA, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_DATA, "check");

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    Struct wrapper = (Struct) result.value();
    assertEquals(SCHEMA_TYPE, wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertTrue(wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT)
        .contains(TOPIC));
  }

  @Test
  public void testBackupSchemaIdExtracted() {
    Schema schema = SchemaBuilder.struct()
        .name("IdCheck")
        .field("n", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("n", 7);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    Struct wrapper = (Struct) result.value();
    int wrappedId = wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID);
    assertTrue(wrappedId > 0);
  }

  @Test
  public void testBackupNonWrapperSchemaNormalSerialization() {
    Schema schema = SchemaBuilder.struct()
        .name("DirectMsg")
        .field(FIELD_TEXT, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_TEXT, "direct");

    byte[] result = converter.fromConnectData(TOPIC, schema, value);
    assertNotNull(result);
    assertTrue(result.length > 5);
    assertEquals(0x00, result[0]);
  }

  @Test
  public void testBackupRestoreMissingDataField() {
    Schema badSchema = SchemaBuilder.struct()
        .name(BackupWrapper.NAME)
        .field(BackupWrapper.FIELD_SCHEMA_ID, Schema.INT32_SCHEMA)
        .build();
    Struct badWrapper = new Struct(badSchema)
        .put(BackupWrapper.FIELD_SCHEMA_ID, 1);

    try {
      converter.fromConnectData(TOPIC, badSchema, badWrapper);
      fail("Expected DataException");
    } catch (DataException e) {
      assertTrue(e.getMessage().contains("data")
          || e.getMessage().contains("restore")
          || e.getMessage().contains("Failed"));
    }
  }

  @Test
  public void testBackupRoundTripBytesExact() {
    Schema schema = SchemaBuilder.struct()
        .name("ExactMsg")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field(FIELD_VALUE, Schema.INT32_SCHEMA)
        .build();
    Struct original = new Struct(schema)
        .put(FIELD_NAME, "exact")
        .put(FIELD_VALUE, 77);

    byte[] originalBytes = plainConverter.fromConnectData(
        TOPIC, schema, original);

    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    assertArrayEquals(originalBytes, restoredBytes);
  }

  @Test
  public void testBackupRoundTripSharedMessageType() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
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
        .setField(addressDesc.findFieldByName("street"), "123 Home St")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Hometown")
        .build();
    DynamicMessage workAddr = DynamicMessage.newBuilder(addressDesc)
        .setField(addressDesc.findFieldByName("street"), "456 Work Ave")
        .setField(addressDesc.findFieldByName(FIELD_CITY), "Workville")
        .build();
    DynamicMessage person = DynamicMessage.newBuilder(personDesc)
        .setField(personDesc.findFieldByName("name"), VALUE_ALICE)
        .setField(personDesc.findFieldByName("home_addr"), homeAddr)
        .setField(personDesc.findFieldByName("work_addr"), workAddr)
        .build();

    assertBackupRoundTrip(topic, schema, person);
  }

  @Test
  public void testBackupRoundTripAllPrimitiveTypes() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message AllPrimitives {\n"
        + "  int32 int32_val = 1;\n"
        + "  int64 int64_val = 2;\n"
        + "  float float_val = 3;\n"
        + "  double double_val = 4;\n"
        + "  bool bool_val = 5;\n"
        + "  string string_val = 6;\n"
        + "  bytes bytes_val = 7;\n"
        + "  uint32 uint32_val = 8;\n"
        + "  uint64 uint64_val = 9;\n"
        + "  sint32 sint32_val = 10;\n"
        + "  sint64 sint64_val = 11;\n"
        + "  fixed32 fixed32_val = 12;\n"
        + "  fixed64 fixed64_val = 13;\n"
        + "  sfixed32 sfixed32_val = 14;\n"
        + "  sfixed64 sfixed64_val = 15;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();

    DynamicMessage message = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("int32_val"), 42)
        .setField(desc.findFieldByName("int64_val"), 123456789L)
        .setField(desc.findFieldByName("float_val"), 3.14f)
        .setField(desc.findFieldByName("double_val"), 2.718281828)
        .setField(desc.findFieldByName("bool_val"), true)
        .setField(desc.findFieldByName("string_val"), "hello")
        .setField(desc.findFieldByName("bytes_val"),
            ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
        .setField(desc.findFieldByName("uint32_val"), 100)
        .setField(desc.findFieldByName("uint64_val"), 999999L)
        .setField(desc.findFieldByName("sint32_val"), -42)
        .setField(desc.findFieldByName("sint64_val"), -123456789L)
        .setField(desc.findFieldByName("fixed32_val"), 77)
        .setField(desc.findFieldByName("fixed64_val"), 88L)
        .setField(desc.findFieldByName("sfixed32_val"), -77)
        .setField(desc.findFieldByName("sfixed64_val"), -88L)
        .build();

    assertBackupRoundTrip(topic, schema, message);
  }

  @Test
  public void testBackupRoundTripNestedMessages() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Order {\n"
        + "  int32 id = 1;\n"
        + "  Customer customer = 2;\n"
        + "  Address shipping = 3;\n"
        + "}\n"
        + "message Customer {\n"
        + "  string name = 1;\n"
        + "  Address billing = 2;\n"
        + "}\n"
        + "message Address {\n"
        + "  string street = 1;\n"
        + "  string city = 2;\n"
        + "  string zip = 3;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor orderDesc = schema.toDescriptor();
    Descriptor custDesc = orderDesc.findFieldByName("customer").getMessageType();
    Descriptor addrDesc = orderDesc.findFieldByName("shipping").getMessageType();

    DynamicMessage billingAddr = DynamicMessage.newBuilder(addrDesc)
        .setField(addrDesc.findFieldByName("street"), "100 Bill Ln")
        .setField(addrDesc.findFieldByName(FIELD_CITY), "Billtown")
        .setField(addrDesc.findFieldByName("zip"), "11111")
        .build();
    DynamicMessage shippingAddr = DynamicMessage.newBuilder(addrDesc)
        .setField(addrDesc.findFieldByName("street"), "200 Ship Rd")
        .setField(addrDesc.findFieldByName(FIELD_CITY), "Shipville")
        .setField(addrDesc.findFieldByName("zip"), "22222")
        .build();
    DynamicMessage customer = DynamicMessage.newBuilder(custDesc)
        .setField(custDesc.findFieldByName("name"), VALUE_BOB)
        .setField(custDesc.findFieldByName("billing"), billingAddr)
        .build();
    DynamicMessage order = DynamicMessage.newBuilder(orderDesc)
        .setField(orderDesc.findFieldByName(FIELD_ID), 1001)
        .setField(orderDesc.findFieldByName("customer"), customer)
        .setField(orderDesc.findFieldByName("shipping"), shippingAddr)
        .build();

    assertBackupRoundTrip(topic, schema, order);
  }

  @Test
  public void testBackupRoundTripRepeatedFields() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Classroom {\n"
        + "  string name = 1;\n"
        + "  repeated string students = 2;\n"
        + "  repeated int32 scores = 3;\n"
        + "  repeated Student enrollments = 4;\n"
        + "}\n"
        + "message Student {\n"
        + "  string name = 1;\n"
        + "  int32 grade = 2;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor classDesc = schema.toDescriptor();
    Descriptor studentDesc = classDesc.findFieldByName("enrollments").getMessageType();

    DynamicMessage s1 = DynamicMessage.newBuilder(studentDesc)
        .setField(studentDesc.findFieldByName("name"), VALUE_ALICE)
        .setField(studentDesc.findFieldByName("grade"), 95)
        .build();
    DynamicMessage s2 = DynamicMessage.newBuilder(studentDesc)
        .setField(studentDesc.findFieldByName("name"), VALUE_BOB)
        .setField(studentDesc.findFieldByName("grade"), 88)
        .build();

    DynamicMessage classroom = DynamicMessage.newBuilder(classDesc)
        .setField(classDesc.findFieldByName("name"), "Math 101")
        .addRepeatedField(classDesc.findFieldByName("students"), VALUE_ALICE)
        .addRepeatedField(classDesc.findFieldByName("students"), VALUE_BOB)
        .addRepeatedField(classDesc.findFieldByName("students"), "Charlie")
        .addRepeatedField(classDesc.findFieldByName("scores"), 95)
        .addRepeatedField(classDesc.findFieldByName("scores"), 88)
        .addRepeatedField(classDesc.findFieldByName("scores"), 72)
        .addRepeatedField(classDesc.findFieldByName("enrollments"), s1)
        .addRepeatedField(classDesc.findFieldByName("enrollments"), s2)
        .build();

    assertBackupRoundTrip(topic, schema, classroom);
  }

  @Test
  public void testBackupRoundTripMapFields() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Config {\n"
        + "  string name = 1;\n"
        + "  map<string, string> properties = 2;\n"
        + "  map<string, int32> counts = 3;\n"
        + "  map<int32, string> id_names = 4;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();

    FieldDescriptor propsField = desc.findFieldByName("properties");
    Descriptor propsEntry = propsField.getMessageType();
    FieldDescriptor countsField = desc.findFieldByName("counts");
    Descriptor countsEntry = countsField.getMessageType();
    FieldDescriptor idNamesField = desc.findFieldByName("id_names");
    Descriptor idNamesEntry = idNamesField.getMessageType();

    DynamicMessage.Builder builder = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("name"), "test-config");

    builder.addRepeatedField(propsField,
        DynamicMessage.newBuilder(propsEntry)
            .setField(propsEntry.findFieldByName(FIELD_KEY), FIELD_HOST)
            .setField(propsEntry.findFieldByName("value"), "localhost")
            .build());
    builder.addRepeatedField(propsField,
        DynamicMessage.newBuilder(propsEntry)
            .setField(propsEntry.findFieldByName(FIELD_KEY), FIELD_PORT)
            .setField(propsEntry.findFieldByName("value"), "8080")
            .build());

    builder.addRepeatedField(countsField,
        DynamicMessage.newBuilder(countsEntry)
            .setField(countsEntry.findFieldByName(FIELD_KEY), "errors")
            .setField(countsEntry.findFieldByName("value"), 5)
            .build());

    builder.addRepeatedField(idNamesField,
        DynamicMessage.newBuilder(idNamesEntry)
            .setField(idNamesEntry.findFieldByName(FIELD_KEY), 1)
            .setField(idNamesEntry.findFieldByName("value"), "first")
            .build());

    assertBackupRoundTrip(topic, schema, builder.build());
  }

  @Test
  public void testBackupRoundTripEnumType() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Event {\n"
        + "  string id = 1;\n"
        + "  EventType type = 2;\n"
        + "  Priority priority = 3;\n"
        + "}\n"
        + "enum EventType {\n"
        + "  UNKNOWN = 0;\n"
        + "  CLICK = 1;\n"
        + "  VIEW = 2;\n"
        + "  PURCHASE = 3;\n"
        + "}\n"
        + "enum Priority {\n"
        + "  LOW = 0;\n"
        + "  MEDIUM = 1;\n"
        + "  HIGH = 2;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();

    DynamicMessage event = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(FIELD_ID), "evt-123")
        .setField(desc.findFieldByName("type"),
            desc.findFieldByName("type").getEnumType().findValueByName("PURCHASE"))
        .setField(desc.findFieldByName("priority"),
            desc.findFieldByName("priority").getEnumType().findValueByName("HIGH"))
        .build();

    assertBackupRoundTrip(topic, schema, event);
  }

  @Test
  public void testBackupRoundTripOneofFields() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Notification {\n"
        + "  string id = 1;\n"
        + "  oneof channel {\n"
        + "    string email = 2;\n"
        + "    string sms = 3;\n"
        + "    PushConfig push = 4;\n"
        + "  }\n"
        + "}\n"
        + "message PushConfig {\n"
        + "  string device_token = 1;\n"
        + "  bool silent = 2;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor notifDesc = schema.toDescriptor();
    Descriptor pushDesc = notifDesc.findFieldByName("push").getMessageType();

    // Test with string oneof branch
    DynamicMessage emailNotif = DynamicMessage.newBuilder(notifDesc)
        .setField(notifDesc.findFieldByName(FIELD_ID), "n-1")
        .setField(notifDesc.findFieldByName("email"), "user@test.com")
        .build();
    assertBackupRoundTrip(topic, schema, emailNotif);

    // Test with message oneof branch
    String topic2 = nextTopic();
    DynamicMessage pushNotif = DynamicMessage.newBuilder(notifDesc)
        .setField(notifDesc.findFieldByName(FIELD_ID), "n-2")
        .setField(notifDesc.findFieldByName("push"),
            DynamicMessage.newBuilder(pushDesc)
                .setField(pushDesc.findFieldByName("device_token"), "tok-abc")
                .setField(pushDesc.findFieldByName("silent"), true)
                .build())
        .build();
    assertBackupRoundTrip(topic2, schema, pushNotif);
  }

  @Test
  public void testBackupRoundTripOptionalFields() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Profile {\n"
        + "  string name = 1;\n"
        + "  optional string nickname = 2;\n"
        + "  optional int32 age = 3;\n"
        + "  optional bool active = 4;\n"
        + "  string email = 5;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();

    // With optional fields set
    DynamicMessage withOptionals = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("name"), VALUE_ALICE)
        .setField(desc.findFieldByName("nickname"), "Ali")
        .setField(desc.findFieldByName("age"), 30)
        .setField(desc.findFieldByName("active"), true)
        .setField(desc.findFieldByName("email"), "alice@test.com")
        .build();
    assertBackupRoundTrip(topic, schema, withOptionals);

    // With optional fields NOT set (defaults)
    String topic2 = nextTopic();
    DynamicMessage withoutOptionals = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("name"), VALUE_BOB)
        .setField(desc.findFieldByName("email"), "bob@test.com")
        .build();
    assertBackupRoundTrip(topic2, schema, withoutOptionals);
  }

  @Test
  public void testBackupRoundTripNestedEnum() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Outer {\n"
        + "  string label = 1;\n"
        + "  Inner details = 2;\n"
        + "  message Inner {\n"
        + "    string value = 1;\n"
        + "    enum Status {\n"
        + "      DRAFT = 0;\n"
        + "      PUBLISHED = 1;\n"
        + "      ARCHIVED = 2;\n"
        + "    }\n"
        + "    Status status = 2;\n"
        + "  }\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor outerDesc = schema.toDescriptor();
    Descriptor innerDesc = outerDesc.findFieldByName("details").getMessageType();

    DynamicMessage inner = DynamicMessage.newBuilder(innerDesc)
        .setField(innerDesc.findFieldByName("value"), "content")
        .setField(innerDesc.findFieldByName("status"),
            innerDesc.findFieldByName("status").getEnumType()
                .findValueByName("PUBLISHED"))
        .build();
    DynamicMessage outer = DynamicMessage.newBuilder(outerDesc)
        .setField(outerDesc.findFieldByName("label"), "doc-1")
        .setField(outerDesc.findFieldByName("details"), inner)
        .build();

    assertBackupRoundTrip(topic, schema, outer);
  }

  @Test
  public void testBackupRoundTripSharedTypeAtThreeFields() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
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
        .setField(addrDesc.findFieldByName("country"), "US")
        .build();
    DynamicMessage warehouse = DynamicMessage.newBuilder(addrDesc)
        .setField(addrDesc.findFieldByName("line1"), "50 Warehouse Dr")
        .setField(addrDesc.findFieldByName(FIELD_CITY), "Denver")
        .setField(addrDesc.findFieldByName("country"), "US")
        .build();
    DynamicMessage billing = DynamicMessage.newBuilder(addrDesc)
        .setField(addrDesc.findFieldByName("line1"), "PO Box 100")
        .setField(addrDesc.findFieldByName(FIELD_CITY), "Austin")
        .setField(addrDesc.findFieldByName("country"), "US")
        .build();
    DynamicMessage company = DynamicMessage.newBuilder(companyDesc)
        .setField(companyDesc.findFieldByName("name"), "Confluent")
        .setField(companyDesc.findFieldByName("hq"), hq)
        .setField(companyDesc.findFieldByName("warehouse"), warehouse)
        .setField(companyDesc.findFieldByName("billing"), billing)
        .build();

    assertBackupRoundTrip(topic, schema, company);
  }

  @Test
  public void testBackupRoundTripNonContiguousTags() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Sparse {\n"
        + "  string name = 1;\n"
        + "  int32 code = 5;\n"
        + "  bool active = 10;\n"
        + "  string desc = 20;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();

    DynamicMessage message = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("name"), "sparse-test")
        .setField(desc.findFieldByName("code"), 404)
        .setField(desc.findFieldByName("active"), false)
        .setField(desc.findFieldByName("desc"), "not found")
        .build();

    assertBackupRoundTrip(topic, schema, message);
  }

  @Test
  public void testBackupRoundTripRepeatedNestedWithSharedType() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
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
        .setField(memberDesc.findFieldByName("name"), VALUE_ALICE)
        .setField(memberDesc.findFieldByName("role"), r1)
        .build();
    DynamicMessage m2 = DynamicMessage.newBuilder(memberDesc)
        .setField(memberDesc.findFieldByName("name"), VALUE_BOB)
        .setField(memberDesc.findFieldByName("role"), r2)
        .build();
    DynamicMessage team = DynamicMessage.newBuilder(teamDesc)
        .setField(teamDesc.findFieldByName("name"), "Platform")
        .addRepeatedField(teamDesc.findFieldByName("members"), m1)
        .addRepeatedField(teamDesc.findFieldByName("members"), m2)
        .build();

    assertBackupRoundTrip(topic, schema, team);
  }

  @Test
  public void testBackupRoundTripMapWithMessageValue() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Registry {\n"
        + "  string id = 1;\n"
        + "  map<string, Service> services = 2;\n"
        + "}\n"
        + "message Service {\n"
        + "  string host = 1;\n"
        + "  int32 port = 2;\n"
        + "  bool healthy = 3;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor regDesc = schema.toDescriptor();
    FieldDescriptor svcField = regDesc.findFieldByName("services");
    Descriptor entryDesc = svcField.getMessageType();
    Descriptor svcDesc = entryDesc.findFieldByName("value").getMessageType();

    DynamicMessage svc1 = DynamicMessage.newBuilder(svcDesc)
        .setField(svcDesc.findFieldByName(FIELD_HOST), "api.local")
        .setField(svcDesc.findFieldByName(FIELD_PORT), 8080)
        .setField(svcDesc.findFieldByName("healthy"), true)
        .build();
    DynamicMessage svc2 = DynamicMessage.newBuilder(svcDesc)
        .setField(svcDesc.findFieldByName(FIELD_HOST), "db.local")
        .setField(svcDesc.findFieldByName(FIELD_PORT), 5432)
        .setField(svcDesc.findFieldByName("healthy"), false)
        .build();

    DynamicMessage registry = DynamicMessage.newBuilder(regDesc)
        .setField(regDesc.findFieldByName(FIELD_ID), "cluster-1")
        .addRepeatedField(svcField,
            DynamicMessage.newBuilder(entryDesc)
                .setField(entryDesc.findFieldByName(FIELD_KEY), "api")
                .setField(entryDesc.findFieldByName("value"), svc1)
                .build())
        .addRepeatedField(svcField,
            DynamicMessage.newBuilder(entryDesc)
                .setField(entryDesc.findFieldByName(FIELD_KEY), "database")
                .setField(entryDesc.findFieldByName("value"), svc2)
                .build())
        .build();

    assertBackupRoundTrip(topic, schema, registry);
  }

  @Test
  public void testBackupRoundTripDefaultsAndEmpty() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message Defaults {\n"
        + "  string name = 1;\n"
        + "  int32 count = 2;\n"
        + "  bool flag = 3;\n"
        + "  double rate = 4;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();

    // All defaults (empty message — proto3 defaults are 0/""/false)
    DynamicMessage empty = DynamicMessage.newBuilder(desc).build();
    assertBackupRoundTrip(topic, schema, empty);
  }

  @Test
  public void testBackupRoundTripComplexCombined() {
    String topic = nextTopic();
    String proto = "syntax = \"proto3\";\n"
        + "package io.confluent.test;\n"
        + "message ComplexRecord {\n"
        + "  string id = 1;\n"
        + "  repeated Tag tags = 2;\n"
        + "  map<string, string> metadata = 3;\n"
        + "  optional string description = 4;\n"
        + "  Status status = 5;\n"
        + "  oneof target {\n"
        + "    string url = 6;\n"
        + "    Endpoint endpoint = 7;\n"
        + "  }\n"
        + "  Endpoint primary = 8;\n"
        + "  Endpoint secondary = 9;\n"
        + "}\n"
        + "message Tag {\n"
        + "  string key = 1;\n"
        + "  string value = 2;\n"
        + "}\n"
        + "message Endpoint {\n"
        + "  string host = 1;\n"
        + "  int32 port = 2;\n"
        + "}\n"
        + "enum Status {\n"
        + "  UNKNOWN = 0;\n"
        + "  ACTIVE = 1;\n"
        + "  INACTIVE = 2;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor recDesc = schema.toDescriptor();
    Descriptor tagDesc = recDesc.findFieldByName("tags").getMessageType();
    Descriptor epDesc = recDesc.findFieldByName("primary").getMessageType();
    FieldDescriptor metaField = recDesc.findFieldByName("metadata");
    Descriptor metaEntry = metaField.getMessageType();

    DynamicMessage tag1 = DynamicMessage.newBuilder(tagDesc)
        .setField(tagDesc.findFieldByName(FIELD_KEY), "env")
        .setField(tagDesc.findFieldByName("value"), "prod")
        .build();
    DynamicMessage tag2 = DynamicMessage.newBuilder(tagDesc)
        .setField(tagDesc.findFieldByName(FIELD_KEY), "region")
        .setField(tagDesc.findFieldByName("value"), "us-west")
        .build();
    DynamicMessage primary = DynamicMessage.newBuilder(epDesc)
        .setField(epDesc.findFieldByName(FIELD_HOST), "primary.local")
        .setField(epDesc.findFieldByName(FIELD_PORT), 443)
        .build();
    DynamicMessage secondary = DynamicMessage.newBuilder(epDesc)
        .setField(epDesc.findFieldByName(FIELD_HOST), "secondary.local")
        .setField(epDesc.findFieldByName(FIELD_PORT), 8443)
        .build();
    DynamicMessage oneofEndpoint = DynamicMessage.newBuilder(epDesc)
        .setField(epDesc.findFieldByName(FIELD_HOST), "target.local")
        .setField(epDesc.findFieldByName(FIELD_PORT), 9090)
        .build();

    DynamicMessage record = DynamicMessage.newBuilder(recDesc)
        .setField(recDesc.findFieldByName(FIELD_ID), "rec-complex")
        .addRepeatedField(recDesc.findFieldByName("tags"), tag1)
        .addRepeatedField(recDesc.findFieldByName("tags"), tag2)
        .addRepeatedField(metaField,
            DynamicMessage.newBuilder(metaEntry)
                .setField(metaEntry.findFieldByName(FIELD_KEY), "owner")
                .setField(metaEntry.findFieldByName("value"), "team-a")
                .build())
        .setField(recDesc.findFieldByName("description"), "complex test")
        .setField(recDesc.findFieldByName("status"),
            recDesc.findFieldByName("status").getEnumType().findValueByName("ACTIVE"))
        .setField(recDesc.findFieldByName("endpoint"), oneofEndpoint)
        .setField(recDesc.findFieldByName("primary"), primary)
        .setField(recDesc.findFieldByName("secondary"), secondary)
        .build();

    assertBackupRoundTrip(topic, schema, record);
  }

  @Test
  public void testBackupSchemaVersionPresent() {
    Schema schema = SchemaBuilder.struct()
        .name("VersionCheck")
        .field("x", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("x", 42);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    Struct wrapper = (Struct) result.value();
    assertNotNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
  }

  @Test
  public void testBackupRestoreNullRawSchemaFallback() {
    Schema schema = SchemaBuilder.struct()
        .name("FallbackMsg")
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

    byte[] restored = converter.fromConnectData(TOPIC, wrapperSchema, modifiedWrapper);
    assertNotNull(restored);
    assertEquals(0x00, restored[0]);
  }
}
