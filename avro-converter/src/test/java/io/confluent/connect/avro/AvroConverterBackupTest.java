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

package io.confluent.connect.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class AvroConverterBackupTest {

  private static final String TOPIC = "test-topic";
  private static final String SCHEMA_TYPE = "AVRO";
  private static final String SR_URL_KEY = "schema.registry.url";
  private static final String FAKE_SR_URL = "http://fake-url";
  private static final String BACKUP_ENABLED_KEY = "schema.backup.enabled";

  private static final String FIELD_NAME = "name";
  private static final String FIELD_ID = "id";
  private static final String FIELD_COUNT = "count";

  // Source-cluster SR + converters (default for all tests).
  private final SchemaRegistryClient schemaRegistry;
  private final AvroConverter converter;
  private final AvroConverter plainConverter;
  private KafkaAvroSerializer rawAvroSerializer;
  private KafkaAvroDeserializer rawAvroDeserializer;

  private final SchemaRegistryClient targetSchemaRegistry;
  private final AvroConverter targetConverter;
  private KafkaAvroDeserializer targetAvroDeserializer;

  private int topicCounter = 0;

  public AvroConverterBackupTest() {
    schemaRegistry = new MockSchemaRegistryClient();
    converter = new AvroConverter(schemaRegistry);
    plainConverter = new AvroConverter(schemaRegistry);
    targetSchemaRegistry = new MockSchemaRegistryClient();
    targetConverter = new AvroConverter(targetSchemaRegistry);
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

    rawAvroSerializer = new KafkaAvroSerializer(schemaRegistry);
    rawAvroSerializer.configure(plainConfig, false);
    rawAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
    rawAvroDeserializer.configure(plainConfig, false);

    targetAvroDeserializer = new KafkaAvroDeserializer(targetSchemaRegistry);
    targetAvroDeserializer.configure(plainConfig, false);
  }

  private static KafkaAvroSerializer newReferenceAwareAvroSerializer(
      SchemaRegistryClient sr) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_KEY, FAKE_SR_URL);
    cfg.put("auto.register.schemas", "false");
    cfg.put("use.latest.version", "true");
    KafkaAvroSerializer s = new KafkaAvroSerializer(sr);
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
    AvroSchema wrapperParsed = new AvroSchema(wrapperRaw);
    assertEquals("wrapper rawSchema canonically equals source-registered schema",
        sourceRegistered.canonicalString(), wrapperParsed.canonicalString());
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
    // Payload byte-exactness is the semantic-equivalence guarantee.
    assertPayloadBytesEqual(sourceBytes, restoredBytes);
  }

  private void assertFullFidelityRoundTrip(String topic, Schema connectSchema, Struct value) {
    byte[] originalBytes = plainConverter.fromConnectData(topic, connectSchema, value);
    assertNotNull("plain serialization produced bytes", originalBytes);

    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertNotNull("raw schema captured", raw);
    AvroSchema restoredParsed = new AvroSchema(raw);
    assertNotNull("raw schema parses", restoredParsed.canonicalString());
    SchemaAndValue originalDeser = plainConverter.toConnectData(topic, originalBytes);
    SchemaAndValue restoredDeser = plainConverter.toConnectData(topic, restoredBytes);
    assertValueEqual(originalDeser.value(), restoredDeser.value());
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
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "Alice").put("age", 30);

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
        .name("Exact")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field(FIELD_COUNT, Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "order-1").put(FIELD_COUNT, 42);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
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
    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.RawCheck")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field("value", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "raw-check").put("value", 42);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, serialized);

    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertNotNull("raw schema captured", raw);
    AvroSchema restored = new AvroSchema(raw);
    // Round-trip through AvroSchema canonicalString must be stable.
    AvroSchema roundtripped = new AvroSchema(restored.canonicalString());
    assertEquals("raw schema canonical stable",
        restored.canonicalString(), roundtripped.canonicalString());
  }

  @Test
  public void testDefaultSingletonUnionFlattenedOptionalString() {
    Schema schema = SchemaBuilder.struct()
        .name("Nullable")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field("nickname", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct with = new Struct(schema).put(FIELD_NAME, "Alice").put("nickname", "Al");
    Struct without = new Struct(schema).put(FIELD_NAME, "Bob"); // nickname null

    assertFullFidelityRoundTrip(TOPIC, schema, with);
    assertFullFidelityRoundTrip(TOPIC, schema, without);
  }

  @Test
  public void testDefaultComplexRealisticSchemaAllAxesPass() {
    Schema addressSchema = SchemaBuilder.struct()
        .name("io.confluent.test.Address")
        .field("street", Schema.STRING_SCHEMA)
        .field("city", Schema.STRING_SCHEMA)
        .field("country", Schema.STRING_SCHEMA)
        .build();

    Schema contactSchema = SchemaBuilder.struct()
        .name("io.confluent.test.Contact")
        .field("name", Schema.STRING_SCHEMA)
        .field("email", Schema.OPTIONAL_STRING_SCHEMA)
        .field("address", addressSchema)
        .build();

    Schema profileSchema = SchemaBuilder.struct()
        .name("io.confluent.test.Profile")
        .field("id", Schema.STRING_SCHEMA)
        .field("primary", addressSchema)
        .field("secondary", addressSchema)  // shared named type
        .field("contacts", SchemaBuilder.array(contactSchema).build())
        .field("attributes", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .field("created", Timestamp.SCHEMA)          // logical type: timestamp-millis
        .field("balance", Decimal.schema(2))         // logical type: decimal(scale=2)
        .field("optional_note", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    Struct hqAddr = new Struct(addressSchema)
        .put("street", "1 HQ Plaza")
        .put("city", "San Francisco")
        .put("country", "US");
    Struct branchAddr = new Struct(addressSchema)
        .put("street", "2 Branch Ln")
        .put("city", "Austin")
        .put("country", "US");
    Struct contactAddr = new Struct(addressSchema)
        .put("street", "3 Friend Rd")
        .put("city", "Boston")
        .put("country", "US");
    Struct contact = new Struct(contactSchema)
        .put("name", "Bob")
        .put("email", "bob@test.com")
        .put("address", contactAddr);

    Map<String, String> attrs = new HashMap<>();
    attrs.put("env", "prod");
    attrs.put("region", "us-west");

    Struct profile = new Struct(profileSchema)
        .put("id", "profile-1")
        .put("primary", hqAddr)
        .put("secondary", branchAddr)
        .put("contacts", Collections.singletonList(contact))
        .put("attributes", attrs)
        .put("created", new Date(1700000000000L))
        .put("balance", new BigDecimal("1234.56"))
        .put("optional_note", null);

    String topic = nextTopic();
    byte[] originalBytes = plainConverter.fromConnectData(topic, profileSchema, profile);
    SchemaAndValue wrapped = converter.toConnectData(topic, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertNotNull("raw Avro schema captured", raw);
    AvroSchema restoredSchema = new AvroSchema(raw);
    assertNotNull("raw schema parses cleanly", restoredSchema.canonicalString());
    SchemaAndValue originalDeser = plainConverter.toConnectData(topic, originalBytes);
    SchemaAndValue restoredDeser = plainConverter.toConnectData(topic, restoredBytes);
    assertValueEqual(originalDeser.value(), restoredDeser.value());
  }

  @Test
  public void testIdempotencyKitchenSinkStableAcrossCycles() {
    Schema schema = SchemaBuilder.struct()
        .name("IdempotencyTarget")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field(FIELD_COUNT, Schema.INT32_SCHEMA)
        .field(FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema)
        .put(FIELD_ID, "idem-1")
        .put(FIELD_COUNT, 42)
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
  public void testDefaultSharedNamedTypeAtMultipleFieldsSurvives() {
    Schema addressSchema = SchemaBuilder.struct()
        .name("Address")
        .field("street", Schema.STRING_SCHEMA)
        .field("city", Schema.STRING_SCHEMA)
        .parameter("connect.custom", "shared-address")
        .build();
    Schema personSchema = SchemaBuilder.struct()
        .name("Person")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field("homeAddress", addressSchema)
        .field("workAddress", addressSchema)
        .build();
    Struct value = new Struct(personSchema)
        .put(FIELD_NAME, "Alice")
        .put("homeAddress", new Struct(addressSchema)
            .put("street", "1 Home Ln").put("city", "Hometown"))
        .put("workAddress", new Struct(addressSchema)
            .put("street", "2 Work Ave").put("city", "Workville"));

    assertFullFidelityRoundTrip(TOPIC, personSchema, value);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, personSchema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertNotNull("raw Avro schema captured", raw);
    assertTrue("shared-type params captured in raw schema",
        raw.contains("shared-address"));
  }

  @Test
  public void testDefaultLogicalTypesPreserved() {
    Schema schema = SchemaBuilder.struct()
        .name("LogicalTypes")
        .field("price", Decimal.builder(2).build())
        .field("createdOn", org.apache.kafka.connect.data.Date.builder().build())
        .field("updatedAt", Timestamp.builder().build())
        .build();
    Struct value = new Struct(schema)
        .put("price", new BigDecimal("19.99"))
        .put("createdOn", new java.util.Date(0))
        .put("updatedAt", new java.util.Date(1_700_000_000_000L));

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    SchemaAndValue restoredData = plainConverter.toConnectData(TOPIC, restoredBytes);
    Struct restored = (Struct) restoredData.value();
    assertEquals("decimal preserved", new BigDecimal("19.99"), restored.get("price"));
    assertEquals("date preserved", new java.util.Date(0), restored.get("createdOn"));
    assertEquals("timestamp preserved",
        new java.util.Date(1_700_000_000_000L), restored.get("updatedAt"));
  }

  @Test
  public void testDefaultBytesFieldPreserved() {
    Schema schema = SchemaBuilder.struct()
        .name("BytesHolder")
        .field("payload", Schema.BYTES_SCHEMA)
        .build();
    byte[] rawBytes = new byte[]{0x00, 0x01, 0x7F, (byte) 0x80, (byte) 0xFF, 0x42};
    Struct value = new Struct(schema).put("payload", ByteBuffer.wrap(rawBytes));

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    SchemaAndValue restoredData = plainConverter.toConnectData(TOPIC, restoredBytes);
    ByteBuffer restoredPayload = (ByteBuffer) ((Struct) restoredData.value()).get("payload");
    assertArrayEquals("byte payload preserved",
        rawBytes, Arrays.copyOfRange(restoredPayload.array(),
            restoredPayload.arrayOffset(), restoredPayload.arrayOffset() + rawBytes.length));
  }

  @Test
  public void testDefaultEmptyCollectionsPreserved() {
    Schema schema = SchemaBuilder.struct()
        .name("EmptyCollections")
        .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .build();
    Struct value = new Struct(schema)
        .put("tags", Collections.emptyList())
        .put("attrs", Collections.emptyMap());

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    SchemaAndValue restoredData = plainConverter.toConnectData(TOPIC, restoredBytes);
    Struct restored = (Struct) restoredData.value();
    assertTrue("empty tags array preserved",
        ((java.util.List<?>) restored.get("tags")).isEmpty());
    assertTrue("empty attrs map preserved",
        ((java.util.Map<?, ?>) restored.get("attrs")).isEmpty());
  }

  @Test
  public void testDefaultPristineRestoreWithRawAvroProducer() throws Exception {
    String topic = nextTopic();
    org.apache.avro.Schema rawAvroSchema = org.apache.avro.SchemaBuilder
        .record("PristineEvent").namespace("io.confluent.test").fields()
        .requiredString("id")
        .requiredInt("count")
        .optionalString("note")
        .endRecord();
    org.apache.avro.generic.GenericRecord record =
        new org.apache.avro.generic.GenericRecordBuilder(rawAvroSchema)
            .set("id", "pristine-1")
            .set("count", 42)
            .set("note", "e2e")
            .build();

    byte[] sourceBytes = rawAvroSerializer.serialize(topic, record);
    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWrapperSchemaIdMatchesSource(wrapped, sourceBytes);
    assertRawSchemaMatchesSourceRegistered(wrapped, sourceBytes);
    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertPayloadBytesEqual(sourceBytes, restoredBytes);
    assertBytesExact(sourceBytes, restoredBytes);
    Object originalDeser = rawAvroDeserializer.deserialize(topic, sourceBytes);
    Object restoredDeser = rawAvroDeserializer.deserialize(topic, restoredBytes);
    assertValueEqual(originalDeser, restoredDeser);
  }

  @Test
  public void testDefaultPristineRestoreMegaKitchenSinkAllTypesAndReferences() throws Exception {
    converter.configure(
        backupConfigWith("enhanced.avro.schema.support", "true"), false);

    String topic = nextTopic();

    String addressSubject = "shared.Address";
    org.apache.avro.Schema addressAvro = org.apache.avro.SchemaBuilder
        .record("Address").namespace("io.confluent.test.shared").fields()
        .requiredString("street")
        .requiredString("city")
        .requiredString("country")
        .endRecord();
    schemaRegistry.register(addressSubject, new AvroSchema(addressAvro));

    org.apache.avro.Schema priorityAvro = org.apache.avro.SchemaBuilder
        .enumeration("Priority").namespace("io.confluent.test")
        .symbols("LOW", "MEDIUM", "HIGH");
    org.apache.avro.Schema uuidAvro = org.apache.avro.SchemaBuilder
        .fixed("Uuid16").namespace("io.confluent.test").size(16);
    org.apache.avro.Schema timestampMillisAvro = org.apache.avro.LogicalTypes.timestampMillis()
        .addToSchema(org.apache.avro.SchemaBuilder.builder().longType());
    org.apache.avro.Schema dateAvro = org.apache.avro.LogicalTypes.date()
        .addToSchema(org.apache.avro.SchemaBuilder.builder().intType());
    org.apache.avro.Schema decimalAvro = org.apache.avro.LogicalTypes.decimal(10, 2)
        .addToSchema(org.apache.avro.SchemaBuilder.builder().bytesType());

    org.apache.avro.Schema personAvro = org.apache.avro.SchemaBuilder
        .record("MegaPerson").namespace("io.confluent.test").fields()
        // Primitives
        .requiredString("id")
        .requiredInt("age")
        .requiredLong("balanceCents")
        .requiredDouble("rating")
        .requiredBoolean("active")
        .name("payload").type().bytesType().noDefault()
        // Logical types
        .name("createdAtMs").type(timestampMillisAvro).noDefault()
        .name("birthDate").type(dateAvro).noDefault()
        .name("priceUsd").type(decimalAvro).noDefault()
        // Enum + Fixed
        .name("priority").type(priorityAvro).noDefault()
        .name("guid").type(uuidAvro).noDefault()
        // Cross-subject reference used at TWO fields (shared named type)
        .name("homeAddress").type(addressAvro).noDefault()
        .name("workAddress").type(addressAvro).noDefault()
        // Array + Map + Nullable
        .name("tags").type().array().items().stringType().noDefault()
        .name("attrs").type().map().values().stringType().noDefault()
        .name("bio").type().nullable().stringType().stringDefault("")
        .endRecord();

    SchemaReference addressRef = new SchemaReference(
        "io.confluent.test.shared.Address", addressSubject, 1);
    Map<String, String> resolvedRefs = new HashMap<>();
    resolvedRefs.put("io.confluent.test.shared.Address", addressAvro.toString());
    AvroSchema personSchema = new AvroSchema(
        personAvro.toString(),
        Collections.singletonList(addressRef),
        resolvedRefs,
        null);
    schemaRegistry.register(topic + "-value", personSchema);

    org.apache.avro.generic.GenericRecord homeAddrRec =
        new org.apache.avro.generic.GenericRecordBuilder(addressAvro)
            .set("street", "1 Home Ln").set("city", "Hometown").set("country", "US")
            .build();
    org.apache.avro.generic.GenericRecord workAddrRec =
        new org.apache.avro.generic.GenericRecordBuilder(addressAvro)
            .set("street", "2 Work Ave").set("city", "Workville").set("country", "US")
            .build();
    org.apache.avro.Conversions.DecimalConversion decConv =
        new org.apache.avro.Conversions.DecimalConversion();
    ByteBuffer priceBytes = decConv.toBytes(
        new BigDecimal("123.45"), decimalAvro, org.apache.avro.LogicalTypes.decimal(10, 2));
    byte[] guidRaw = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    org.apache.avro.generic.GenericData.Fixed guidFixed =
        new org.apache.avro.generic.GenericData.Fixed(uuidAvro, guidRaw);
    org.apache.avro.generic.GenericData.EnumSymbol prioritySymbol =
        new org.apache.avro.generic.GenericData.EnumSymbol(priorityAvro, "HIGH");

    org.apache.avro.generic.GenericRecord person =
        new org.apache.avro.generic.GenericRecordBuilder(personAvro)
            .set("id", "person-1")
            .set("age", 30)
            .set("balanceCents", 100_000L)
            .set("rating", 4.5)
            .set("active", true)
            .set("payload", ByteBuffer.wrap(new byte[]{0x00, 0x7F, (byte) 0x80, (byte) 0xFF}))
            .set("createdAtMs", 1_700_000_000_000L)
            .set("birthDate", 15_000)
            .set("priceUsd", priceBytes)
            .set("priority", prioritySymbol)
            .set("guid", guidFixed)
            .set("homeAddress", homeAddrRec)
            .set("workAddress", workAddrRec)
            .set("tags", Arrays.asList("premium", "verified"))
            .set("attrs", Collections.singletonMap("env", "prod"))
            .set("bio", "test bio")
            .build();

    byte[] sourceBytes = rawAvroSerializer.serialize(topic, person);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWrapperSchemaIdMatchesSource(wrapped, sourceBytes);
    assertRawSchemaMatchesSourceRegistered(wrapped, sourceBytes);
    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertPayloadBytesEqual(sourceBytes, restoredBytes);
    assertBytesExact(sourceBytes, restoredBytes);

    // Semantic equivalence via re-deserialization through the raw Avro path.
    Object originalDeser = rawAvroDeserializer.deserialize(topic, sourceBytes);
    Object restoredDeser = rawAvroDeserializer.deserialize(topic, restoredBytes);
    assertValueEqual(originalDeser, restoredDeser);
  }

  private static final String ADDRESS_SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"Address\","
          + "\"namespace\":\"io.confluent.test.shared\","
          + "\"fields\":["
          + "{\"name\":\"street\",\"type\":\"string\"},"
          + "{\"name\":\"city\",\"type\":\"string\"},"
          + "{\"name\":\"country\",\"type\":\"string\"}]}";
  private static final String ADDRESS_FQCN = "io.confluent.test.shared.Address";
  private static final String ADDRESS_SUBJECT = "shared.Address";

  private static final String PERSON_WITH_REF_SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"PersonRef\","
          + "\"namespace\":\"io.confluent.test\","
          + "\"fields\":["
          + "{\"name\":\"name\",\"type\":\"string\"},"
          + "{\"name\":\"homeAddress\",\"type\":"
          + "{\"type\":\"record\",\"name\":\"Address\","
          + "\"namespace\":\"io.confluent.test.shared\","
          + "\"fields\":["
          + "{\"name\":\"street\",\"type\":\"string\"},"
          + "{\"name\":\"city\",\"type\":\"string\"},"
          + "{\"name\":\"country\",\"type\":\"string\"}]}},"
          + "{\"name\":\"workAddress\",\"type\":\"io.confluent.test.shared.Address\"}]}";

  private RefTestFixture registerAndProducePersonWithAddressRef() throws Exception {
    String topic = nextTopic();
    schemaRegistry.register(ADDRESS_SUBJECT, new AvroSchema(ADDRESS_SCHEMA_JSON));
    SchemaReference addressRef = new SchemaReference(ADDRESS_FQCN, ADDRESS_SUBJECT, 1);
    Map<String, String> resolved = new HashMap<>();
    resolved.put(ADDRESS_FQCN, ADDRESS_SCHEMA_JSON);
    AvroSchema personSchema = new AvroSchema(
        PERSON_WITH_REF_SCHEMA_JSON, Collections.singletonList(addressRef), resolved, null);
    schemaRegistry.register(topic + "-value", personSchema);

    org.apache.avro.Schema personAvroSchema = personSchema.rawSchema();
    org.apache.avro.Schema addressAvroSchema =
        personAvroSchema.getField("homeAddress").schema();
    org.apache.avro.generic.GenericRecord addr =
        new org.apache.avro.generic.GenericRecordBuilder(addressAvroSchema)
            .set("street", "1 Main St").set("city", "Springfield").set("country", "US")
            .build();
    org.apache.avro.generic.GenericRecord person =
        new org.apache.avro.generic.GenericRecordBuilder(personAvroSchema)
            .set("name", "Alice")
            .set("homeAddress", addr)
            .set("workAddress", addr)
            .build();
    KafkaAvroSerializer refSerializer = newReferenceAwareAvroSerializer(schemaRegistry);
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
    assertTrue("reference tree mentions Address FQCN: " + refTree,
        refTree.contains(ADDRESS_FQCN));
    assertTrue("direct refs mention Address subject: " + directRefs,
        directRefs.contains(ADDRESS_SUBJECT));

    // Target SR is empty; restore must re-register Address and Person there.
    assertTrue("target SR is empty before restore",
        targetSchemaRegistry.getAllSubjects().isEmpty());
    byte[] restoredBytes = targetConverter.fromConnectData(
        fx.topic, wrapped.schema(), wrapped.value());

    assertTrue("target SR has Address subject",
        targetSchemaRegistry.getAllSubjects().contains(ADDRESS_SUBJECT));
    assertTrue("target SR has Person subject",
        targetSchemaRegistry.getAllSubjects().contains(fx.topic + "-value"));

    // Wire IDs differ across clusters; payload bytes MUST be byte-exact.
    assertCrossClusterBytesEquivalent(fx.sourceBytes, restoredBytes);

    int targetWireId = ByteBuffer.wrap(restoredBytes, 1, 4).getInt();
    ParsedSchema targetPersonSchema = targetSchemaRegistry.getSchemaById(targetWireId);
    assertNotNull("target SR resolves the restored wire ID", targetPersonSchema);
    assertEquals("target Person canonical == source Person canonical",
        sourcePersonCanonical, targetPersonSchema.canonicalString());
    List<SchemaReference> targetRefs = targetPersonSchema.references();
    assertEquals("target Person has one direct reference (Address)", 1, targetRefs.size());
    assertEquals("target ref FQCN matches source",
        ADDRESS_FQCN, targetRefs.get(0).getName());
    assertEquals("target ref subject matches source", ADDRESS_SUBJECT,
        targetRefs.get(0).getSubject());
    assertTrue("target ref version is a positive integer",
        targetRefs.get(0).getVersion() > 0);

    Object sourceValue = rawAvroDeserializer.deserialize(fx.topic, fx.sourceBytes);
    Object targetValue = targetAvroDeserializer.deserialize(fx.topic, restoredBytes);
    assertNotNull("source deserialized non-null", sourceValue);
    assertNotNull("target deserialized non-null", targetValue);
    assertEquals("Avro-JSON textual form of restored record equals source's",
        sourceValue.toString(), targetValue.toString());
  }

  // ================ Cross-subject reference negatives ================

  @Test
  public void testEdgeReferenceTreeMissingEntryThrows() throws Exception {
    RefTestFixture fx = registerAndProducePersonWithAddressRef();
    SchemaAndValue wrapped = converter.toConnectData(fx.topic, fx.sourceBytes);

    // Rebuild wrapper with EMPTY reference tree while directRefs still lists Address.
    Struct original = (Struct) wrapped.value();
    Schema wrapperSchema = wrapped.schema();
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        original.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        original.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION),
        original.getString(BackupWrapper.FIELD_SCHEMA_TYPE),
        original.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        original.getString(BackupWrapper.FIELD_RAW_SCHEMA),
        "{}",  // reference tree emptied
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
        "{ not valid json",  // corrupt JSON
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
        null,  // reference tree cleared
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

  @Test
  public void testDefaultUnicodeInPayloadPreserved() {
    Schema schema = SchemaBuilder.struct()
        .name("UnicodePayload")
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    String unicode = "Hello 世界 🚀 café";
    Struct value = new Struct(schema).put(FIELD_NAME, unicode);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    SchemaAndValue restoredData = plainConverter.toConnectData(TOPIC, restoredBytes);
    assertEquals("unicode string preserved",
        unicode, ((Struct) restoredData.value()).getString(FIELD_NAME));
  }

  // ================ Config-lossiness pairs (same config on/off) ================

  @Test
  public void testEnhancedAvroSupportOnPreservesPackageInSchemaName() {
    converter.configure(backupConfigWith("enhanced.avro.schema.support", "true"), false);

    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.Namespaced")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "n-1");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    // Raw Avro schema retains the fully qualified name.
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertTrue("raw schema retains namespace io.confluent.test",
        raw.contains("io.confluent.test"));
  }

  @Test
  public void testEnhancedAvroSupportOffBackupStillPreservesRawSchema() {
    // Default config (enhanced.avro.schema.support defaults to false).
    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.Namespaced")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "n-1");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    // Wire bytes still preserved because raw Avro schema is source-of-truth for restore.
    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }

  @Test
  public void testConnectMetaDataOnEmbedsConnectParamsInAvroSchema() {
    // Default config (connect.meta.data defaults to true).
    Schema schema = SchemaBuilder.struct()
        .name("MetaCheck")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .parameter("custom.param", "custom-value")
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "m-1");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);

    // The Avro schema captured in the wrapper includes connect.parameters when meta.data is on.
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertTrue("raw schema contains connect.parameters when meta.data on",
        raw.contains("connect.parameters"));
  }

  @Test
  public void testConnectMetaDataOffDropsConnectParamsFromAvroSchema() {
    converter.configure(backupConfigWith("connect.meta.data", "false"), false);
    plainConverter.configure(
        backupConfigWith("connect.meta.data", "false"), false);

    Schema schema = SchemaBuilder.struct()
        .name("MetaCheck")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .parameter("custom.param", "custom-value")
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "m-1");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);

    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    // Wire bytes still roundtrip fine because backup uses raw schema as source-of-truth.
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());
    assertBytesExact(originalBytes, restoredBytes);
    // But Connect-side parameters are NOT embedded in the Avro schema.
    assertFalse("raw schema drops connect.parameters when meta.data off",
        raw.contains("custom.param"));
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
    Schema schema = Schema.STRING_SCHEMA;
    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, "hello");
    SchemaAndValue result = plainConverter.toConnectData(TOPIC, serialized);

    assertNotNull(result.schema());
    assertNotEquals(BackupWrapper.NAME, result.schema().name());
  }

  @Test
  public void testEdgeNonWrapperSchemaSerializesNormally() {
    Schema schema = SchemaBuilder.struct()
        .name("Direct")
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
        .name("Basic")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field(FIELD_COUNT, Schema.INT32_SCHEMA)
        .build();
    Struct original = new Struct(schema).put(FIELD_ID, "b-1").put(FIELD_COUNT, 7);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, original);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    SchemaAndValue restoredData = plainConverter.toConnectData(TOPIC, restoredBytes);
    Struct restored = (Struct) restoredData.value();
    assertEquals("b-1", restored.getString(FIELD_ID));
    assertEquals(Integer.valueOf(7), restored.getInt32(FIELD_COUNT));
  }

  @Test
  public void testEdgePrimitiveSchemaWrapping() {
    byte[] serialized = plainConverter.fromConnectData(
        TOPIC, Schema.STRING_SCHEMA, "test-value");

    SchemaAndValue result = converter.toConnectData(TOPIC, serialized);

    assertEquals(BackupWrapper.NAME, result.schema().name());
    Struct wrapper = (Struct) result.value();
    assertEquals(SCHEMA_TYPE, wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
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

    // Rebuild wrapper with schema type deliberately set to PROTOBUF (wrong for Avro converter).
    Struct original = (Struct) wrapped.value();
    Schema wrapperSchema = wrapped.schema();
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        original.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        original.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION),
        "PROTOBUF",
        original.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        original.getString(BackupWrapper.FIELD_RAW_SCHEMA),
        null, null);
    Struct badWrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.get(BackupWrapper.FIELD_DATA), fields);

    try {
      converter.fromConnectData(TOPIC, wrapperSchema, badWrapper);
      fail("Expected DataException on schema type mismatch");
    } catch (DataException e) {
      assertEquals("AvroConverter received wrapper with schemaType='PROTOBUF', "
          + "expected 'AVRO'", e.getMessage());
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
    // Overwrite the 4-byte schema ID header with an unlikely-to-exist ID.
    ByteBuffer.wrap(corrupted, 1, 4).putInt(Integer.MAX_VALUE - 1);

    try {
      converter.toConnectData(TOPIC, corrupted);
      fail("Expected DataException for corrupted wire schema ID");
    } catch (DataException e) {
      assertEquals("Failed to deserialize data for topic " + TOPIC + " to Avro:",
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
      assertEquals("Failed to deserialize data for topic " + TOPIC + " to Avro:",
          e.getMessage());
    }
  }

  @Test
  public void testEdgeSinkWrapErrorClassifiedAsBackupException() throws Exception {
    // A SR client that fetches individual schemas normally (so deserialize succeeds)
    // but throws on getAllVersionsById, which only the wrap path uses.
    SchemaRegistryClient wrapFailingSr = new MockSchemaRegistryClient() {
      @Override
      public Collection<SubjectVersion> getAllVersionsById(int id) {
        throw new SerializationException("simulated SR unavailable during wrap");
      }
    };
    AvroConverter wrapFailingConverter = new AvroConverter(wrapFailingSr);
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_KEY, FAKE_SR_URL);
    cfg.put(BACKUP_ENABLED_KEY, "true");
    wrapFailingConverter.configure(cfg, false);

    String topic = nextTopic();
    org.apache.avro.Schema rawAvroSchema = org.apache.avro.SchemaBuilder
        .record("WrapFail").namespace("io.confluent.test").fields()
        .requiredString("id")
        .endRecord();
    wrapFailingSr.register(topic + "-value", new AvroSchema(rawAvroSchema));
    org.apache.avro.generic.GenericRecord record =
        new org.apache.avro.generic.GenericRecordBuilder(rawAvroSchema)
            .set("id", "wrap-fail").build();
    KafkaAvroSerializer refSerializer = newReferenceAwareAvroSerializer(wrapFailingSr);
    byte[] bytes = refSerializer.serialize(topic, record);

    try {
      wrapFailingConverter.toConnectData(topic, bytes);
      fail("Expected DataException when wrap-path SR call fails");
    } catch (DataException e) {
      assertEquals("Failed to wrap Avro backup for topic " + topic, e.getMessage());
      assertTrue("cause is the simulated SerializationException",
          e.getCause() instanceof SerializationException);
    }
  }

  @Test
  public void testHeaderSchemaIdSerializerBackupCapturesSchemaId() {
    SchemaRegistryClient sr = new MockSchemaRegistryClient();
    AvroConverter headerBackupConverter = new AvroConverter(sr);
    AvroConverter headerPlainConverter = new AvroConverter(sr);
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
        .name("HeaderIdRecord")
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
  public void testConfigMismatchEnhancedAvroSchemaSupportOnRestoreOffDocumentedLoss() {
    // Backup side: enhanced.avro.schema.support=true
    converter.configure(backupConfigWith("enhanced.avro.schema.support", "true"), false);
    plainConverter.configure(
        backupConfigWith("enhanced.avro.schema.support", "true"), false);

    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.test.Mismatch")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "m-1");
    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);

    // Restore side: flip to false. Wrapper's rawSchema drives restore, so bytes match.
    converter.configure(backupConfigWith("enhanced.avro.schema.support", "false"), false);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }

  @Test
  public void testConfigMismatchEnhancedAvroSupportOffWithEnumSchemaDocumentsLoss() {
    // Producer side: enhanced.avro.schema.support=true so the enum-parameterized Connect
    // schema is turned into a proper Avro ENUM on serialize.
    plainConverter.configure(
        backupConfigWith("enhanced.avro.schema.support", "true"), false);

    Schema enumField = SchemaBuilder.string()
        .name("Priority")
        .parameter("io.confluent.connect.avro.Enum", "Priority")
        .parameter("io.confluent.connect.avro.Enum.LOW", "0")
        .parameter("io.confluent.connect.avro.Enum.MEDIUM", "1")
        .parameter("io.confluent.connect.avro.Enum.HIGH", "2")
        .build();
    Schema schema = SchemaBuilder.struct()
        .name("EnumEvent")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field("priority", enumField)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "e-1").put("priority", "HIGH");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);

    // Backup side: enhanced=false. Wrapper still preserves the raw Avro schema (with enum)
    // because rawSchema comes from the SR-registered schema, not from AvroData conversion.
    converter.configure(
        backupConfigWith("enhanced.avro.schema.support", "false"), false);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertTrue("wrapper raw schema declares an enum type: " + raw,
        raw.contains("\"type\":\"enum\""));
    assertTrue("wrapper raw schema names the Priority enum: " + raw,
        raw.contains("\"name\":\"Priority\""));
    assertTrue("wrapper raw schema lists enum symbols LOW,MEDIUM,HIGH: " + raw,
        raw.contains("\"LOW\"") && raw.contains("\"MEDIUM\"") && raw.contains("\"HIGH\""));

    // AvroData drops the enum reconstruction when enhanced=false; serialize then
    // fails because the raw schema still declares an ENUM.
    try {
      converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());
      fail("Expected DataException restore MUST fail when enhanced.avro.schema.support "
          + "is off and the source schema has an enum");
    } catch (DataException e) {
      assertEquals("Failed to restore Avro backup for topic " + TOPIC, e.getMessage());
      Throwable rootCause = null;
      Throwable c = e.getCause();
      while (c != null) {
        if (c instanceof org.apache.avro.AvroTypeException) {
          rootCause = c;
          break;
        }
        c = c.getCause();
      }
      assertNotNull("cause chain contains AvroTypeException", rootCause);
      assertTrue("AvroTypeException identifies string-not-enum: " + rootCause.getMessage(),
          rootCause.getMessage().contains("java.lang.String")
              && rootCause.getMessage().contains("Priority"));
    }
  }

  @Test
  public void testConfigMismatchConnectMetaDataOnRestoreOffDocumentedLoss() {
    // Backup side: connect.meta.data=true (default)
    Schema schema = SchemaBuilder.struct()
        .name("MetaMismatch")
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .parameter("custom.param", "custom-value")
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "meta-1");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    String rawBackup = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertTrue("raw schema captured with connect.parameters",
        rawBackup.contains("connect.parameters"));

    // Restore side: flip to false. Bytes still match because rawSchema is what re-registers.
    converter.configure(backupConfigWith("connect.meta.data", "false"), false);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }

  // Helper: assertFalse for the config-off tests above.
  private static void assertFalse(String message, boolean condition) {
    if (condition) {
      throw new AssertionError(message);
    }
  }
}
