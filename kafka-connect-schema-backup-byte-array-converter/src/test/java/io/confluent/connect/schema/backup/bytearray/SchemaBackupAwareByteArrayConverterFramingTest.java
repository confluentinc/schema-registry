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

package io.confluent.connect.schema.backup.bytearray;

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
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

/**
 * Restore-side wire framing for {@link SchemaBackupAwareByteArrayConverter}.
 *
 * <p>The source framing is taken from the backup wrapper, and only the target framing is
 * left on the restored record.
 */
public class SchemaBackupAwareByteArrayConverterFramingTest {

  private static final String TOPIC = "framing-topic";
  private static final String SR_URL_CONFIG = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
  private static final String SR_URL_VALUE = "http://fake-url";
  private static final String TRUE = "true";
  private static final String HEADER_SERIALIZER = HeaderSchemaIdSerializer.class.getName();
  private static final String VALUE_HEADER = SchemaId.VALUE_SCHEMA_ID_HEADER;
  private static final String KEY_HEADER = SchemaId.KEY_SCHEMA_ID_HEADER;
  private static final String UNRELATED_GUID = "6c8b3c3e-1f0e-4c8e-9c4c-2b1a0f6e9d11";
  private static final int PREFIX_LENGTH = 5;
  private static final int BOGUS_ID_OFFSET = 1000;
  private static final byte INVALID_MAGIC = 0x7F;
  private static final byte[] MANGLED_HEADER = {
      (byte) 0xEF, (byte) 0xBF, (byte) 0xBD, (byte) 0xEF, (byte) 0xBF, (byte) 0xBD};

  private static final String FIELD_NOTE = "note";
  private static final String FIELD_COUNT = "count";
  private static final String FIELD_LABEL = "label";
  private static final String FIELD_TEXT = "text";
  private static final String LABEL_VALUE = "payload-starts-with-zero";
  private static final int COUNT_VALUE = 7;
  private static final String TEXT_VALUE = "second-message";
  private static final String SECOND_MESSAGE = "Second";

  /** First field is a null union branch, so the Avro payload starts with 0x00. */
  private static final String NULLABLE_FIRST_AVRO_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Nullable\",\"namespace\":\"io.confluent.test\","
          + "\"fields\":["
          + "{\"name\":\"" + FIELD_NOTE + "\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"" + FIELD_COUNT + "\",\"type\":\"int\"},"
          + "{\"name\":\"" + FIELD_LABEL + "\",\"type\":\"string\"}"
          + "]}";

  /** Serializing the second message gives non-empty Protobuf message indexes ([1]). */
  private static final String TWO_MESSAGE_PROTO =
      "syntax = \"proto3\";\n"
          + "package io.confluent.test;\n"
          + "message First {\n"
          + "  string a = 1;\n"
          + "}\n"
          + "message " + SECOND_MESSAGE + " {\n"
          + "  string " + FIELD_TEXT + " = 1;\n"
          + "}\n";

  private static final String JSON_SCHEMA =
      "{\"title\":\"io.confluent.test.Framed\",\"type\":\"object\","
          + "\"properties\":{"
          + "\"" + FIELD_LABEL + "\":{\"type\":\"string\"},"
          + "\"" + FIELD_COUNT + "\":{\"type\":\"integer\"}},"
          + "\"required\":[\"" + FIELD_LABEL + "\",\"" + FIELD_COUNT + "\"]}";
  private static final ObjectMapper JSON = new ObjectMapper();

  private SchemaRegistryClient schemaRegistry;
  private SchemaBackupAwareByteArrayConverter valueConverter;
  private SchemaBackupAwareByteArrayConverter headerTargetValueConverter;
  private SchemaBackupAwareByteArrayConverter keyConverter;

  @Before
  public void setUp() {
    schemaRegistry = new MockSchemaRegistryClient(Arrays.<SchemaProvider>asList(
        new AvroSchemaProvider(),
        new ProtobufSchemaProvider(),
        new JsonSchemaProvider()));

    valueConverter = newConverter(backupConfig(), false);

    Map<String, Object> headerTarget = backupConfig();
    headerTarget.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HEADER_SERIALIZER);
    headerTargetValueConverter = newConverter(headerTarget, false);

    keyConverter = newConverter(backupConfig(), true);
  }

  // ---------------- Header-framed source (bytes are the bare payload) ----------------

  @Test
  public void testHeaderFramedAvroRestoresWhenHeaderIsMissing() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeAvro(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);
    assertNull(wrapper(wrapped).getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertNotNull(wrapper(wrapped).getString(BackupWrapper.FIELD_SCHEMA_GUID));

    // No header on the restored record: the payload's leading 0x00 is not a prefix.
    Headers restoreHeaders = new RecordHeaders();
    byte[] restored = valueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(payload, Arrays.copyOfRange(restored, PREFIX_LENGTH, restored.length));
    assertNullableRecord(deserializeAvro(restoreHeaders, restored));
  }

  @Test
  public void testHeaderFramedAvroDoesNotNeedReadableHeader() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeAvro(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    Headers restoreHeaders = new RecordHeaders();
    restoreHeaders.add(VALUE_HEADER, MANGLED_HEADER);
    byte[] restored = valueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertNull(restoreHeaders.lastHeader(VALUE_HEADER));
    assertNullableRecord(deserializeAvro(restoreHeaders, restored));
  }

  @Test
  public void testHeaderFramedAvroToPrefixTargetRemovesSourceHeader() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeAvro(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    Headers restoreHeaders = copy(sourceHeaders);
    byte[] restored = valueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertNull(restoreHeaders.lastHeader(VALUE_HEADER));
    assertEquals(SchemaId.MAGIC_BYTE_V0, restored[0]);
    assertNullableRecord(deserializeAvro(restoreHeaders, restored));
  }

  @Test
  public void testHeaderFramedAvroToHeaderTargetLeavesSingleTargetHeader() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeAvro(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    // Simulates the source registry's GUID, which differs from the target's.
    Headers restoreHeaders = new RecordHeaders();
    byte[] sourceGuidHeader = guidHeader(UNRELATED_GUID);
    restoreHeaders.add(VALUE_HEADER, sourceGuidHeader);
    byte[] restored = headerTargetValueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(payload, restored);
    assertEquals(1, countHeaders(restoreHeaders, VALUE_HEADER));
    assertFalse(Arrays.equals(sourceGuidHeader,
        restoreHeaders.lastHeader(VALUE_HEADER).value()));
    assertNullableRecord(deserializeAvro(restoreHeaders, restored));
  }

  // ---------------- Prefix-framed source ----------------

  @Test
  public void testPrefixFramedAvroIgnoresUnrelatedHeader() {
    byte[] original = serializeAvro(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);
    assertNotNull(wrapper(wrapped).getInt32(BackupWrapper.FIELD_SCHEMA_ID));

    // A stale header must not override the prefix framing recorded in the wrapper.
    Headers restoreHeaders = new RecordHeaders();
    restoreHeaders.add(VALUE_HEADER, guidHeader(UNRELATED_GUID));
    byte[] restored = valueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(original, restored);
    assertNull(restoreHeaders.lastHeader(VALUE_HEADER));
  }

  @Test
  public void testPrefixFramedAvroToHeaderTarget() {
    byte[] original = serializeAvro(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);

    Headers restoreHeaders = new RecordHeaders();
    byte[] restored = headerTargetValueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(Arrays.copyOfRange(original, PREFIX_LENGTH, original.length), restored);
    assertEquals(1, countHeaders(restoreHeaders, VALUE_HEADER));
    assertNullableRecord(deserializeAvro(restoreHeaders, restored));
  }

  @Test
  public void testPrefixFramedIdMismatchThrows() {
    byte[] original = serializeAvro(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);
    Struct tampered = copyWrapper(wrapper(wrapped));
    tampered.put(BackupWrapper.FIELD_SCHEMA_ID,
        wrapper(wrapped).getInt32(BackupWrapper.FIELD_SCHEMA_ID) + BOGUS_ID_OFFSET);

    assertRestoreFails(valueConverter, new RecordHeaders(), wrapped, tampered,
        "but the stored bytes carry schema ID");
  }

  @Test
  public void testPrefixFramedBytesWithoutPrefixThrows() {
    byte[] original = serializeAvro(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);
    Struct tampered = copyWrapper(wrapper(wrapped));
    byte[] noPrefix = Arrays.copyOf(original, original.length);
    noPrefix[0] = INVALID_MAGIC;
    tampered.put(BackupWrapper.FIELD_DATA, noPrefix);

    assertRestoreFails(valueConverter, new RecordHeaders(), wrapped, tampered,
        "do not start with a valid schema ID prefix");
  }

  // ---------------- Protobuf message indexes ----------------

  @Test
  public void testProtobufPrefixFramedMessageIndexesBytesExact() {
    byte[] original = serializeSecondMessage(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);

    byte[] restored = valueConverter.fromConnectData(
        TOPIC, new RecordHeaders(), wrapped.schema(), wrapped.value());

    assertArrayEquals(original, restored);
  }

  @Test
  public void testProtobufHeaderFramedMessageIndexesComeFromHeader() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeSecondMessage(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    Headers restoreHeaders = copy(sourceHeaders);
    byte[] restored = valueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertNull(restoreHeaders.lastHeader(VALUE_HEADER));
    assertSecondMessage(deserializeProto(restoreHeaders, restored));
  }

  @Test
  public void testProtobufHeaderFramedMissingHeaderThrows() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeSecondMessage(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    assertRestoreFails(valueConverter, new RecordHeaders(), wrapped, wrapper(wrapped),
        VALUE_HEADER + " header is missing on restore");
  }

  @Test
  public void testProtobufHeaderFramedMangledHeaderThrows() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeSecondMessage(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    Headers restoreHeaders = new RecordHeaders();
    restoreHeaders.add(VALUE_HEADER, MANGLED_HEADER);

    assertRestoreFails(valueConverter, restoreHeaders, wrapped, wrapper(wrapped),
        "header cannot be parsed");
  }

  @Test
  public void testProtobufHeaderFramedToHeaderTargetKeepsMessageIndexes() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeSecondMessage(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    Headers restoreHeaders = copy(sourceHeaders);
    byte[] restored = headerTargetValueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(payload, restored);
    assertEquals(1, countHeaders(restoreHeaders, VALUE_HEADER));
    assertSecondMessage(deserializeProto(restoreHeaders, restored));
  }

  @Test
  public void testProtobufPrefixFramedToHeaderTargetMovesMessageIndexesToHeader() {
    byte[] original = serializeSecondMessage(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);

    Headers restoreHeaders = new RecordHeaders();
    byte[] restored = headerTargetValueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertEquals(1, countHeaders(restoreHeaders, VALUE_HEADER));
    assertTrue("message indexes must leave the payload", restored.length < original.length);
    assertSecondMessage(deserializeProto(restoreHeaders, restored));
  }

  // ---------------- JSON Schema (same framing cases as Avro) ----------------

  @Test
  public void testJsonSchemaPrefixFramedBytesExact() {
    byte[] original = serializeJsonSchema(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);

    byte[] restored = valueConverter.fromConnectData(
        TOPIC, new RecordHeaders(), wrapped.schema(), wrapped.value());

    assertArrayEquals(original, restored);
  }

  @Test
  public void testJsonSchemaHeaderFramedToPrefixTargetRemovesSourceHeader() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeJsonSchema(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);
    assertNotNull(wrapper(wrapped).getString(BackupWrapper.FIELD_SCHEMA_GUID));

    Headers restoreHeaders = copy(sourceHeaders);
    byte[] restored = valueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertNull(restoreHeaders.lastHeader(VALUE_HEADER));
    assertEquals(SchemaId.MAGIC_BYTE_V0, restored[0]);
    assertArrayEquals(payload, Arrays.copyOfRange(restored, PREFIX_LENGTH, restored.length));
    assertJsonRecord(deserializeJsonSchema(restoreHeaders, restored));
  }

  @Test
  public void testJsonSchemaHeaderFramedToHeaderTargetLeavesSingleTargetHeader() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeJsonSchema(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    Headers restoreHeaders = new RecordHeaders();
    restoreHeaders.add(VALUE_HEADER, guidHeader(UNRELATED_GUID));
    byte[] restored = headerTargetValueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(payload, restored);
    assertEquals(1, countHeaders(restoreHeaders, VALUE_HEADER));
    assertJsonRecord(deserializeJsonSchema(restoreHeaders, restored));
  }

  @Test
  public void testJsonSchemaPrefixFramedToHeaderTarget() {
    byte[] original = serializeJsonSchema(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);

    Headers restoreHeaders = new RecordHeaders();
    byte[] restored = headerTargetValueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(Arrays.copyOfRange(original, PREFIX_LENGTH, original.length), restored);
    assertEquals(1, countHeaders(restoreHeaders, VALUE_HEADER));
    assertJsonRecord(deserializeJsonSchema(restoreHeaders, restored));
  }

  @Test
  public void testJsonSchemaHeaderFramedRestoresWhenHeaderIsMissing() {
    Headers sourceHeaders = new RecordHeaders();
    byte[] payload = serializeJsonSchema(sourceHeaders, true);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, sourceHeaders, payload);

    Headers restoreHeaders = new RecordHeaders();
    byte[] restored = valueConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(payload, Arrays.copyOfRange(restored, PREFIX_LENGTH, restored.length));
    assertJsonRecord(deserializeJsonSchema(restoreHeaders, restored));
  }

  // ---------------- Key side and older wrappers ----------------

  @Test
  public void testKeyConverterRemovesOnlyKeyHeader() {
    byte[] original = serializeAvro(null, false);
    SchemaAndValue wrapped = keyConverter.toConnectData(TOPIC, original);

    Headers restoreHeaders = new RecordHeaders();
    byte[] unrelated = guidHeader(UNRELATED_GUID);
    restoreHeaders.add(KEY_HEADER, unrelated);
    restoreHeaders.add(VALUE_HEADER, unrelated);
    byte[] restored = keyConverter.fromConnectData(
        TOPIC, restoreHeaders, wrapped.schema(), wrapped.value());

    assertArrayEquals(original, restored);
    assertNull(restoreHeaders.lastHeader(KEY_HEADER));
    assertArrayEquals(unrelated, restoreHeaders.lastHeader(VALUE_HEADER).value());
  }

  @Test
  public void testWrapperWithoutIdOrGuidKeepsHeaderDrivenParsing() {
    byte[] original = serializeAvro(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);
    Struct legacy = copyWrapper(wrapper(wrapped));
    legacy.put(BackupWrapper.FIELD_SCHEMA_ID, null);
    legacy.put(BackupWrapper.FIELD_SCHEMA_GUID, null);

    byte[] restored = valueConverter.fromConnectData(
        TOPIC, new RecordHeaders(), wrapped.schema(), legacy);

    assertArrayEquals(original, restored);
  }

  @Test
  public void testRestoreWithNullHeadersStillWorksForPrefixFraming() {
    byte[] original = serializeAvro(null, false);
    SchemaAndValue wrapped = valueConverter.toConnectData(TOPIC, original);

    byte[] restored = valueConverter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertArrayEquals(original, restored);
  }

  // ---------------- Helpers ----------------

  private Map<String, Object> backupConfig() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    cfg.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, TRUE);
    return cfg;
  }

  private SchemaBackupAwareByteArrayConverter newConverter(
      Map<String, Object> cfg, boolean isKey) {
    SchemaBackupAwareByteArrayConverter converter =
        new SchemaBackupAwareByteArrayConverter(schemaRegistry);
    converter.configure(cfg, isKey);
    return converter;
  }

  private Map<String, Object> serdeConfig(boolean headerFraming) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    if (headerFraming) {
      cfg.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HEADER_SERIALIZER);
    }
    return cfg;
  }

  private byte[] serializeAvro(Headers headers, boolean headerFraming) {
    org.apache.avro.Schema schema =
        new org.apache.avro.Schema.Parser().parse(NULLABLE_FIRST_AVRO_SCHEMA);
    GenericRecord record = new GenericData.Record(schema);
    record.put(FIELD_NOTE, null);
    record.put(FIELD_COUNT, COUNT_VALUE);
    record.put(FIELD_LABEL, LABEL_VALUE);

    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
    serializer.configure(serdeConfig(headerFraming), false);
    byte[] bytes = serializer.serialize(TOPIC, headers, record);
    if (headerFraming) {
      assertEquals("payload must start with 0x00 for this test", 0, bytes[0]);
    }
    return bytes;
  }

  private GenericRecord deserializeAvro(Headers headers, byte[] bytes) {
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistry);
    deserializer.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    return (GenericRecord) deserializer.deserialize(TOPIC, headers, bytes);
  }

  private static void assertNullableRecord(GenericRecord record) {
    assertNull(record.get(FIELD_NOTE));
    assertEquals(COUNT_VALUE, record.get(FIELD_COUNT));
    assertEquals(LABEL_VALUE, record.get(FIELD_LABEL).toString());
  }

  private byte[] serializeSecondMessage(Headers headers, boolean headerFraming) {
    ProtobufSchema fileSchema = new ProtobufSchema(TWO_MESSAGE_PROTO);
    ProtobufSchema schema = fileSchema.copy(fileSchema.name().replace("First", SECOND_MESSAGE));
    Descriptor descriptor = schema.toDescriptor(schema.name());
    DynamicMessage message = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName(FIELD_TEXT), TEXT_VALUE)
        .build();

    KafkaProtobufSerializer<DynamicMessage> serializer =
        new KafkaProtobufSerializer<>(schemaRegistry);
    serializer.configure(serdeConfig(headerFraming), false);
    return serializer.serialize(TOPIC, headers, message, schema);
  }

  private DynamicMessage deserializeProto(Headers headers, byte[] bytes) {
    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(schemaRegistry);
    deserializer.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    return deserializer.deserialize(TOPIC, headers, bytes);
  }

  private static void assertSecondMessage(DynamicMessage message) {
    Descriptor descriptor = message.getDescriptorForType();
    assertEquals(SECOND_MESSAGE, descriptor.getName());
    assertEquals(TEXT_VALUE, message.getField(descriptor.findFieldByName(FIELD_TEXT)));
  }

  private byte[] serializeJsonSchema(Headers headers, boolean headerFraming) {
    JsonNode payload = JSON.createObjectNode()
        .put(FIELD_LABEL, LABEL_VALUE)
        .put(FIELD_COUNT, COUNT_VALUE);
    KafkaJsonSchemaSerializer<Object> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistry);
    serializer.configure(serdeConfig(headerFraming), false);
    return serializer.serialize(TOPIC, headers,
        JsonSchemaUtils.envelope(new JsonSchema(JSON_SCHEMA), payload));
  }

  private JsonNode deserializeJsonSchema(Headers headers, byte[] bytes) {
    KafkaJsonSchemaDeserializer<Object> deserializer =
        new KafkaJsonSchemaDeserializer<>(schemaRegistry);
    deserializer.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    return JSON.valueToTree(deserializer.deserialize(TOPIC, headers, bytes));
  }

  private static void assertJsonRecord(JsonNode node) {
    assertEquals(LABEL_VALUE, node.get(FIELD_LABEL).asText());
    assertEquals(COUNT_VALUE, node.get(FIELD_COUNT).asInt());
  }

  private static byte[] guidHeader(String guid) {
    return new SchemaId(SchemaBackupConfig.TYPE_AVRO, null, UUID.fromString(guid)).guidToBytes();
  }

  private static Struct wrapper(SchemaAndValue wrapped) {
    return (Struct) wrapped.value();
  }

  private static Struct copyWrapper(Struct source) {
    Struct copy = new Struct(source.schema());
    for (Field field : source.schema().fields()) {
      copy.put(field, source.get(field));
    }
    return copy;
  }

  private static Headers copy(Headers headers) {
    Headers copy = new RecordHeaders();
    for (Header header : headers) {
      copy.add(header.key(), header.value());
    }
    return copy;
  }

  private static int countHeaders(Headers headers, String key) {
    int count = 0;
    for (Header ignored : headers.headers(key)) {
      count++;
    }
    return count;
  }

  private static void assertRestoreFails(
      SchemaBackupAwareByteArrayConverter converter, Headers headers,
      SchemaAndValue wrapped, Struct wrapperValue, String expectedMessage) {
    try {
      converter.fromConnectData(TOPIC, headers, wrapped.schema(), wrapperValue);
      fail("Expected DataException containing: " + expectedMessage);
    } catch (DataException e) {
      assertTrue(e.getMessage(), messageChainContains(e, expectedMessage));
    }
  }

  private static boolean messageChainContains(Throwable t, String expected) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur.getMessage() != null && cur.getMessage().contains(expected)) {
        return true;
      }
    }
    return false;
  }
}
