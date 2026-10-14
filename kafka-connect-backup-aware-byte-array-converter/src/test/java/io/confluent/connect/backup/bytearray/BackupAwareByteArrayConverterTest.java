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

package io.confluent.connect.backup.bytearray;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class BackupAwareByteArrayConverterTest {

  private static final String TOPIC = "test-topic";
  private static final String SR_URL_CONFIG = "schema.registry.url";
  private static final String SR_URL_VALUE = "http://fake-url";
  private static final String BACKUP_ENABLED_CONFIG =
      SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG;

  private SchemaRegistryClient schemaRegistry;
  private BackupAwareByteArrayConverter backupConverter;
  private BackupAwareByteArrayConverter passthroughConverter;

  @Before
  public void setUp() {
    schemaRegistry = new MockSchemaRegistryClient(Arrays.<SchemaProvider>asList(
        new AvroSchemaProvider(),
        new ProtobufSchemaProvider(),
        new JsonSchemaProvider()));

    backupConverter = new BackupAwareByteArrayConverter(schemaRegistry);
    Map<String, Object> backupCfg = new HashMap<>();
    backupCfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    backupCfg.put(BACKUP_ENABLED_CONFIG, "true");
    backupConverter.configure(backupCfg, false);

    passthroughConverter = new BackupAwareByteArrayConverter(schemaRegistry);
    passthroughConverter.configure(
        Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
  }

  // ---------------- Passthrough mode ----------------

  @Test
  public void testPassthroughValueBytesUnchanged() {
    byte[] payload = new byte[] {1, 2, 3, 4, 5};
    SchemaAndValue sv = passthroughConverter.toConnectData(TOPIC, payload);
    assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, sv.schema());
    assertArrayEquals(payload, (byte[]) sv.value());

    byte[] out = passthroughConverter.fromConnectData(TOPIC, sv.schema(), sv.value());
    assertArrayEquals(payload, out);
  }

  @Test
  public void testPassthroughNullValueReturnsBytesSchemaWithNullValue() {
    // Match plain ByteArrayConverter: schema=OPTIONAL_BYTES, value=null.
    // Ensures downstream sinks (e.g. S3SinkConnector in BACKUP_FULL_RECORD mode)
    // can wrap tombstones instead of dropping them.
    SchemaAndValue sv = passthroughConverter.toConnectData(TOPIC, null);
    assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, sv.schema());
    assertNull(sv.value());

    byte[] out = passthroughConverter.fromConnectData(TOPIC, null, null);
    assertNull(out);
  }

  @Test
  public void testPassthroughByteBufferValue() {
    byte[] payload = new byte[] {9, 8, 7};
    byte[] out = passthroughConverter.fromConnectData(
        TOPIC, Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(payload));
    assertArrayEquals(payload, out);
  }

  // ---------------- Avro backup/restore ----------------

  @Test
  public void testBackupWrapsAvroMetadata() throws Exception {
    byte[] wireBytes = serializeAvro("Alice", 30);
    SchemaAndValue result = backupConverter.toConnectData(TOPIC, wireBytes);

    assertEquals(BackupWrapper.NAME, result.schema().name());
    Struct wrapper = (Struct) result.value();
    assertEquals(SchemaBackupConfig.TYPE_AVRO,
        wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertTrue(wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT).contains(TOPIC));
    assertArrayEquals(wireBytes, wrapper.getBytes(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testAvroBackupRestoreBytesExact() throws Exception {
    byte[] original = serializeAvro("Bob", 42);
    SchemaAndValue wrapped = backupConverter.toConnectData(TOPIC, original);
    byte[] restored = backupConverter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());
    assertArrayEquals(original, restored);
  }

  @Test
  public void testAvroRoundTripValueSemantics() throws Exception {
    byte[] original = serializeAvro("Carol", 25);
    SchemaAndValue wrapped = backupConverter.toConnectData(TOPIC, original);
    byte[] restored = backupConverter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    KafkaAvroDeserializer deser = new KafkaAvroDeserializer(schemaRegistry);
    deser.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    GenericRecord record = (GenericRecord) deser.deserialize(TOPIC, restored);
    assertEquals("Carol", record.get("name").toString());
    assertEquals(25, record.get("age"));
  }

  // ---------------- Protobuf backup/restore ----------------

  @Test
  public void testProtobufBackupWrapsMetadata() throws Exception {
    byte[] wireBytes = serializeProto("hello-proto");
    SchemaAndValue result = backupConverter.toConnectData(TOPIC, wireBytes);

    Struct wrapper = (Struct) result.value();
    assertEquals(SchemaBackupConfig.TYPE_PROTOBUF,
        wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertArrayEquals(wireBytes, wrapper.getBytes(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testProtobufBackupRestoreBytesExact() throws Exception {
    byte[] original = serializeProto("proto-exact");
    SchemaAndValue wrapped = backupConverter.toConnectData(TOPIC, original);
    byte[] restored = backupConverter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());
    // Because target SR is same as source SR, ids match → bytes are identical.
    assertArrayEquals(original, restored);
  }

  @Test
  public void testProtobufRoundTripValueSemantics() throws Exception {
    byte[] original = serializeProto("value-check");
    SchemaAndValue wrapped = backupConverter.toConnectData(TOPIC, original);
    byte[] restored = backupConverter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());

    KafkaProtobufDeserializer<DynamicMessage> deser =
        new KafkaProtobufDeserializer<>(schemaRegistry);
    deser.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    DynamicMessage msg = deser.deserialize(TOPIC, restored);
    Descriptor desc = msg.getDescriptorForType();
    assertEquals("value-check",
        msg.getField(desc.findFieldByName("text")));
  }

  // ---------------- JSON Schema backup/restore ----------------

  @Test
  public void testJsonSchemaBackupWrapsMetadata() throws Exception {
    byte[] wireBytes = serializeJson("{\"name\":\"json-user\",\"age\":22}");
    SchemaAndValue result = backupConverter.toConnectData(TOPIC, wireBytes);

    Struct wrapper = (Struct) result.value();
    assertEquals(SchemaBackupConfig.TYPE_JSON_SCHEMA,
        wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    assertArrayEquals(wireBytes, wrapper.getBytes(BackupWrapper.FIELD_DATA));
  }

  @Test
  public void testJsonSchemaBackupRestoreBytesExact() throws Exception {
    byte[] original = serializeJson("{\"name\":\"json-exact\",\"age\":33}");
    SchemaAndValue wrapped = backupConverter.toConnectData(TOPIC, original);
    byte[] restored = backupConverter.fromConnectData(
        TOPIC, wrapped.schema(), wrapped.value());
    assertArrayEquals(original, restored);
  }

  // ---------------- Fail-loud paths ----------------

  @Test
  public void testRestoreMissingRawSchemaThrows() throws Exception {
    // Build a valid wrapper first, then hand-modify to remove rawSchema.
    byte[] wireBytes = serializeAvro("null-raw", 1);
    SchemaAndValue wrapped = backupConverter.toConnectData(TOPIC, wireBytes);
    Struct src = (Struct) wrapped.value();

    Struct broken = new Struct(wrapped.schema())
        .put(BackupWrapper.FIELD_DATA, src.getBytes(BackupWrapper.FIELD_DATA))
        .put(BackupWrapper.FIELD_SCHEMA_ID, src.getInt32(BackupWrapper.FIELD_SCHEMA_ID))
        .put(BackupWrapper.FIELD_SCHEMA_TYPE, SchemaBackupConfig.TYPE_AVRO)
        .put(BackupWrapper.FIELD_SCHEMA_SUBJECT,
            src.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT))
        .put(BackupWrapper.FIELD_RAW_SCHEMA, null);

    try {
      backupConverter.fromConnectData(TOPIC, wrapped.schema(), broken);
      fail("Expected DataException for null rawSchema");
    } catch (DataException e) {
      assertTrue(e.getMessage(),
          e.getMessage().contains(BackupWrapper.FIELD_RAW_SCHEMA));
      assertTrue(e.getMessage(), e.getMessage().contains("pristine restore"));
    }
  }

  @Test
  public void testRestoreMissingDataFieldThrows() {
    Schema noDataSchema = SchemaBuilder.struct()
        .name(BackupWrapper.NAME)
        .field(BackupWrapper.FIELD_SCHEMA_ID, Schema.INT32_SCHEMA)
        .build();
    Struct badWrapper = new Struct(noDataSchema)
        .put(BackupWrapper.FIELD_SCHEMA_ID, 1);
    try {
      backupConverter.fromConnectData(TOPIC, noDataSchema, badWrapper);
      fail("Expected DataException for missing data field");
    } catch (DataException e) {
      assertTrue(e.getMessage(),
          e.getMessage().contains(BackupWrapper.FIELD_DATA));
    }
  }

  @Test
  public void testRestoreUnknownSchemaTypeThrows() throws Exception {
    byte[] wireBytes = serializeAvro("bad-type", 1);
    SchemaAndValue wrapped = backupConverter.toConnectData(TOPIC, wireBytes);
    Struct src = (Struct) wrapped.value();

    Struct broken = new Struct(wrapped.schema())
        .put(BackupWrapper.FIELD_DATA, src.getBytes(BackupWrapper.FIELD_DATA))
        .put(BackupWrapper.FIELD_SCHEMA_ID, src.getInt32(BackupWrapper.FIELD_SCHEMA_ID))
        .put(BackupWrapper.FIELD_SCHEMA_TYPE, "MYSTERY_TYPE")
        .put(BackupWrapper.FIELD_SCHEMA_SUBJECT,
            src.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT))
        .put(BackupWrapper.FIELD_RAW_SCHEMA,
            src.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    try {
      backupConverter.fromConnectData(TOPIC, wrapped.schema(), broken);
      fail("Expected DataException for unknown schemaType");
    } catch (DataException e) {
      assertTrue(e.getMessage(),
          e.getMessage().contains("Unsupported schemaType")
              || e.getMessage().contains("MYSTERY_TYPE"));
    }
  }

  @Test
  public void testBackupMalformedHeaderThrows() {
    // Bytes with an invalid magic byte (0x7F): SchemaId.fromBytes rejects it.
    byte[] junk = new byte[] {0x7F, 0, 0, 0, 1, 42, 42, 42};
    try {
      backupConverter.toConnectData(TOPIC, junk);
      fail("Expected DataException for malformed wire header");
    } catch (DataException e) {
      assertNotNull(e.getMessage());
    }
  }

  // ---------------- Tombstones ----------------

  @Test
  public void testBackupNullValueReturnsBytesSchemaWithNullValue() {
    SchemaAndValue sv = backupConverter.toConnectData(TOPIC, null);
    assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, sv.schema());
    assertNull(sv.value());
  }

  @Test
  public void testRestoreNullValueReturnsNull() {
    byte[] out = backupConverter.fromConnectData(TOPIC, null, null);
    assertNull(out);
  }

  // ---------------- Passthrough of non-wrapper Struct in backup mode ----------------

  @Test
  public void testBackupModeBytePassthroughOnNonWrapper() {
    byte[] payload = new byte[] {10, 20, 30};
    // Non-wrapper schema with byte[] value → passthrough.
    byte[] out = backupConverter.fromConnectData(
        TOPIC, Schema.OPTIONAL_BYTES_SCHEMA, payload);
    assertArrayEquals(payload, out);
  }

  @Test
  public void testBackupModeUnsupportedTypeThrows() {
    try {
      backupConverter.fromConnectData(TOPIC, Schema.STRING_SCHEMA, "not-bytes");
      fail("Expected DataException for unsupported type");
    } catch (DataException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("cannot serialize"));
    }
  }

  // ---------------- Wrapper sanity ----------------

  @Test
  public void testWrappedDataIsExactlyOriginalWireBytes() throws Exception {
    byte[] wire = serializeAvro("bytes-check", 7);
    SchemaAndValue result = backupConverter.toConnectData(TOPIC, wire);
    Struct wrapper = (Struct) result.value();
    byte[] dataField = wrapper.getBytes(BackupWrapper.FIELD_DATA);
    assertArrayEquals(wire, dataField);
    assertFalse("wire bytes should be non-empty", dataField.length == 0);
  }

  // ---------------- Helpers to build wire-format bytes ----------------

  private byte[] serializeAvro(String name, int age) {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"User\","
            + "\"namespace\":\"io.confluent.test\","
            + "\"fields\":["
            + "{\"name\":\"name\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"int\"}"
            + "]}";
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(schemaStr);
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("name", name);
    record.put("age", age);

    KafkaAvroSerializer ser = new KafkaAvroSerializer(schemaRegistry);
    ser.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    return ser.serialize(TOPIC, record);
  }

  private byte[] serializeProto(String text) {
    String proto =
        "syntax = \"proto3\";\n"
            + "package io.confluent.test;\n"
            + "message Note {\n"
            + "  string text = 1;\n"
            + "}\n";
    ProtobufSchema schema = new ProtobufSchema(proto);
    Descriptor desc = schema.toDescriptor();
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("text"), text)
        .build();

    KafkaProtobufSerializer<DynamicMessage> ser =
        new KafkaProtobufSerializer<>(schemaRegistry);
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    ser.configure(cfg, false);
    return ser.serialize(TOPIC, null, msg, schema);
  }

  private byte[] serializeJson(String json) throws Exception {
    String schemaStr =
        "{\"type\":\"object\","
            + "\"properties\":{"
            + "\"name\":{\"type\":\"string\"},"
            + "\"age\":{\"type\":\"integer\"}"
            + "}}";
    JsonSchema schema = new JsonSchema(schemaStr);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(json);

    KafkaJsonSchemaSerializer<JsonNode> ser =
        new KafkaJsonSchemaSerializer<>(schemaRegistry);
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    cfg.put("auto.register.schemas", "true");
    ser.configure(cfg, false);
    return ser.serialize(TOPIC, node);
  }
}
