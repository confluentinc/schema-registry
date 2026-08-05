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

package io.confluent.connect.json;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
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

public class JsonSchemaConverterBackupTest {

  private static final String TOPIC = "test-topic";
  private static final String SCHEMA_TYPE = "JSON_SCHEMA";
  private static final String SR_URL_KEY = "schema.registry.url";
  private static final String FAKE_SR_URL = "http://fake-url";
  private static final String BACKUP_ENABLED_KEY = "schema.backup.enabled";

  private static final String FIELD_NAME = "name";
  private static final String FIELD_ID = "id";
  private static final String FIELD_COUNT = "count";

  private static final ObjectMapper JSON = new ObjectMapper();

  private final SchemaRegistryClient schemaRegistry;
  private final JsonSchemaConverter converter;
  private final JsonSchemaConverter plainConverter;
  private KafkaJsonSchemaSerializer<Object> rawJsonSerializer;
  private KafkaJsonSchemaDeserializer<Object> rawJsonDeserializer;

  private final SchemaRegistryClient targetSchemaRegistry;
  private final JsonSchemaConverter targetConverter;
  private KafkaJsonSchemaDeserializer<Object> targetJsonDeserializer;

  private int topicCounter = 0;

  public JsonSchemaConverterBackupTest() {
    schemaRegistry = new MockSchemaRegistryClient(
        ImmutableList.of(new JsonSchemaProvider()));
    converter = new JsonSchemaConverter(schemaRegistry);
    plainConverter = new JsonSchemaConverter(schemaRegistry);
    targetSchemaRegistry = new MockSchemaRegistryClient(
        ImmutableList.of(new JsonSchemaProvider()));
    targetConverter = new JsonSchemaConverter(targetSchemaRegistry);
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

    rawJsonSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry);
    rawJsonSerializer.configure(plainConfig, false);
    rawJsonDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry);
    rawJsonDeserializer.configure(plainConfig, false);

    targetJsonDeserializer = new KafkaJsonSchemaDeserializer<>(targetSchemaRegistry);
    targetJsonDeserializer.configure(plainConfig, false);
  }

  private static KafkaJsonSchemaSerializer<Object> newReferenceAwareJsonSchemaSerializer(
      SchemaRegistryClient sr) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_KEY, FAKE_SR_URL);
    cfg.put("auto.register.schemas", "false");
    cfg.put("use.latest.version", "true");
    cfg.put("latest.compatibility.strict", "false");
    KafkaJsonSchemaSerializer<Object> s = new KafkaJsonSchemaSerializer<>(sr);
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
    JsonSchema wrapperParsed = new JsonSchema(wrapperRaw);
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
  }

  private static void assertJsonPayloadSemanticallyEqual(byte[] source, byte[] restored)
      throws Exception {
    assertTrue("source has 5-byte header + payload", source.length >= 5);
    assertTrue("restored has 5-byte header + payload", restored.length >= 5);
    byte[] sourcePayload = Arrays.copyOfRange(source, 5, source.length);
    byte[] restoredPayload = Arrays.copyOfRange(restored, 5, restored.length);
    JsonNode sourceJson = JSON.readTree(sourcePayload);
    JsonNode restoredJson = JSON.readTree(restoredPayload);
    // Source keys must appear in restored with equal values. Restored may add null-fills
    // for schema-optional fields omitted on wire.
    sourceJson.fields().forEachRemaining(entry -> {
      JsonNode restoredValue = restoredJson.get(entry.getKey());
      assertNotNull("restored missing key " + entry.getKey(), restoredValue);
      assertEquals("value differs for key " + entry.getKey(),
          entry.getValue(), restoredValue);
    });
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
  public void testDefaultBytesExactForStructWithPrimitives() {
    Schema schema = SchemaBuilder.struct()
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field(FIELD_COUNT, Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "item-1").put(FIELD_COUNT, 5);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }

  @Test
  public void testDefaultWireSchemaIdPreserved() {
    Schema schema = SchemaBuilder.struct()
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
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .field("value", Schema.INT32_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_NAME, "raw-check").put("value", 42);

    byte[] serialized = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, serialized);

    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertNotNull("raw schema captured", raw);
    JsonSchema restored = new JsonSchema(raw);
    JsonSchema roundtripped = new JsonSchema(restored.canonicalString());
    assertEquals("raw schema canonical stable",
        restored.canonicalString(), roundtripped.canonicalString());
  }

  @Test
  public void testDefaultComplexRealisticSchemaAllAxesPass() {
    Schema addressSchema = SchemaBuilder.struct()
        .field("street", Schema.STRING_SCHEMA)
        .field("city", Schema.STRING_SCHEMA)
        .build();

    Schema contactSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("email", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    Schema profileSchema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("address", addressSchema)
        .field("contacts", SchemaBuilder.array(contactSchema).build())
        .field("optional_note", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    Struct addr = new Struct(addressSchema)
        .put("street", "1 Main St")
        .put("city", "Springfield");
    Struct contact1 = new Struct(contactSchema)
        .put("name", "Alice")
        .put("email", "alice@test.com");
    Struct contact2 = new Struct(contactSchema)
        .put("name", "Bob")
        .put("email", null);

    Struct profile = new Struct(profileSchema)
        .put("id", "p-1")
        .put("address", addr)
        .put("contacts", Arrays.asList(contact1, contact2))
        .put("optional_note", null);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, profileSchema, profile);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    SchemaAndValue originalDeser = plainConverter.toConnectData(TOPIC, originalBytes);
    SchemaAndValue restoredDeser = plainConverter.toConnectData(TOPIC, restoredBytes);
    assertValueEqual(originalDeser.value(), restoredDeser.value());
  }

  @Test
  public void testIdempotencyKitchenSinkStableAcrossCycles() {
    Schema schema = SchemaBuilder.struct()
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
  public void testDefaultSharedStructTypeAtMultipleFieldsSurvives() {
    Schema addressSchema = SchemaBuilder.struct()
        .name("Address")
        .field("street", Schema.STRING_SCHEMA)
        .field("city", Schema.STRING_SCHEMA)
        .build();
    Schema personSchema = SchemaBuilder.struct()
        .name("Person")
        .field("name", Schema.STRING_SCHEMA)
        .field("home", addressSchema)
        .field("work", addressSchema)
        .build();

    Struct home = new Struct(addressSchema).put("street", "1 Home").put("city", "Springfield");
    Struct work = new Struct(addressSchema).put("street", "2 Work").put("city", "Springfield");
    Struct person = new Struct(personSchema)
        .put("name", "Alice")
        .put("home", home)
        .put("work", work);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, personSchema, person);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    SchemaAndValue restoredDeser = plainConverter.toConnectData(TOPIC, restoredBytes);
    Struct restored = (Struct) restoredDeser.value();
    assertEquals("Alice", restored.getString("name"));
    assertEquals("1 Home", ((Struct) restored.get("home")).getString("street"));
    assertEquals("2 Work", ((Struct) restored.get("work")).getString("street"));
  }

  @Test
  public void testDefaultBytesFieldPreserved() {
    Schema schema = SchemaBuilder.struct()
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field("payload", Schema.BYTES_SCHEMA)
        .build();
    byte[] payload = new byte[]{0x00, 0x7F, (byte) 0x80, (byte) 0xFF};
    Struct value = new Struct(schema).put(FIELD_ID, "b-1").put("payload", payload);

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    SchemaAndValue restoredDeser = plainConverter.toConnectData(TOPIC, restoredBytes);
    assertArrayEquals("bytes field preserved",
        payload, (byte[]) ((Struct) restoredDeser.value()).get("payload"));
  }

  @Test
  public void testDefaultEmptyCollectionsPreserved() {
    Schema schema = SchemaBuilder.struct()
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();
    Struct value = new Struct(schema)
        .put(FIELD_ID, "e-1")
        .put("tags", Collections.emptyList());

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    SchemaAndValue restoredDeser = plainConverter.toConnectData(TOPIC, restoredBytes);
    Struct restored = (Struct) restoredDeser.value();
    assertEquals("empty array preserved",
        Collections.emptyList(), restored.get("tags"));
    assertEquals(FIELD_ID + " preserved", "e-1", restored.getString(FIELD_ID));
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

  @Test
  public void testDefaultPristineRestoreWithRawJsonSchemaProducer() throws Exception {
    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"PristineEvent\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"count\":{\"type\":\"integer\"},"
        + "\"note\":{\"type\":\"string\"}},"
        + "\"required\":[\"id\",\"count\",\"note\"],"
        + "\"additionalProperties\":false}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree("{\"id\":\"pristine-1\",\"count\":42,\"note\":\"e2e\"}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWrapperSchemaIdMatchesSource(wrapped, sourceBytes);
    assertRawSchemaMatchesSourceRegistered(wrapped, sourceBytes);
    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertJsonPayloadSemanticallyEqual(sourceBytes, restoredBytes);
    Object originalDeser = rawJsonDeserializer.deserialize(topic, sourceBytes);
    Object restoredDeser = rawJsonDeserializer.deserialize(topic, restoredBytes);
    assertValueEqual(originalDeser, restoredDeser);
  }

  @Test
  public void testDefaultPristineRestoreMegaKitchenSinkAllTypesAndReferences() throws Exception {
    // use.optional.for.nonrequired=true is required for the mega record because 'nickname'
    // is declared as nullable via ["string","null"] and treated as optional at Connect level.
    converter.configure(backupConfigWith("use.optional.for.nonrequired", "true"), false);

    String topic = nextTopic();

    JsonSchema addressSchema = new JsonSchema(ADDRESS_SCHEMA_JSON);
    schemaRegistry.register(ADDRESS_SUBJECT, addressSchema);

    SchemaReference addressRef =
        new SchemaReference(ADDRESS_REFNAME, ADDRESS_SUBJECT, 1);
    Map<String, String> resolvedRefs = ImmutableMap.of(
        ADDRESS_REFNAME, addressSchema.canonicalString());
    JsonSchema personSchema = new JsonSchema(
        MEGA_PERSON_SCHEMA_JSON,
        Collections.singletonList(addressRef),
        resolvedRefs,
        null);
    schemaRegistry.register(topic + "-value", personSchema);

    JsonNode record = JSON.readTree(
        "{"
            + "\"id\":\"person-1\","
            + "\"age\":30,"
            + "\"balanceCents\":100000,"
            + "\"rating\":4.5,"
            + "\"active\":true,"
            + "\"payload\":\"AH+A/w==\","
            + "\"createdAtMs\":1700000000000,"
            + "\"birthDate\":\"1990-01-15\","
            + "\"priceUsd\":123.45,"
            + "\"priority\":\"HIGH\","
            + "\"guid\":\"11111111-1111-4111-8111-111111111111\","
            + "\"homeAddress\":{\"street\":\"1 Home Ln\",\"city\":\"Hometown\","
            + "\"country\":\"US\"},"
            + "\"workAddress\":{\"street\":\"2 Work Ave\",\"city\":\"Workville\","
            + "\"country\":\"US\"},"
            + "\"tags\":[\"premium\",\"verified\"],"
            + "\"attrs\":{\"env\":\"prod\"},"
            + "\"contact\":\"person@x.com\","
            + "\"nickname\":\"nick1\","
            + "\"nested\":{\"label\":\"n1\",\"kind\":\"B\"}"
            + "}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWrapperSchemaIdMatchesSource(wrapped, sourceBytes);
    assertRawSchemaMatchesSourceRegistered(wrapped, sourceBytes);
    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertJsonPayloadSemanticallyEqual(sourceBytes, restoredBytes);

    // Cross-subject reference metadata explicitly captured on the wrapper.
    Struct wrapperStruct = (Struct) wrapped.value();
    String refTree = wrapperStruct.getString(BackupWrapper.FIELD_REFERENCE_TREE);
    String directRefs = wrapperStruct.getString(BackupWrapper.FIELD_DIRECT_REFS);
    assertNotNull("mega wrapper captures reference tree", refTree);
    assertNotNull("mega wrapper captures direct refs", directRefs);
    assertTrue("reference tree mentions Address refName: " + refTree,
        refTree.contains(ADDRESS_REFNAME));
    assertTrue("direct refs mention Address subject: " + directRefs,
        directRefs.contains(ADDRESS_SUBJECT));

    // Source SR still resolves both Person (with its Address reference) and Address.
    ParsedSchema registeredPerson = schemaRegistry.getSchemaBySubjectAndId(
        topic + "-value",
        schemaRegistry.getLatestSchemaMetadata(topic + "-value").getId());
    assertNotNull("Person schema resolvable from source SR after backup", registeredPerson);
    List<SchemaReference> personRefs = registeredPerson.references();
    assertEquals("Person has one direct reference (Address)", 1, personRefs.size());
    assertEquals("Person reference refName matches Address",
        ADDRESS_REFNAME, personRefs.get(0).getName());
    assertEquals("Person reference subject matches Address",
        ADDRESS_SUBJECT, personRefs.get(0).getSubject());
    assertNotNull("Address schema resolvable from source SR by subject",
        schemaRegistry.getLatestSchemaMetadata(ADDRESS_SUBJECT));

    // Semantic equivalence via re-deserialization through the raw JSON Schema path.
    Object originalDeser = rawJsonDeserializer.deserialize(topic, sourceBytes);
    Object restoredDeser = rawJsonDeserializer.deserialize(topic, restoredBytes);
    assertValueEqual(originalDeser, restoredDeser);
  }

  // attrs carries connect.type=map to activate the Connect map branch; without the hint
  // JsonSchemaData drops entries.
  private static final String MEGA_PERSON_SCHEMA_JSON =
      "{\"type\":\"object\",\"title\":\"MegaPerson\","
          + "\"properties\":{"
          + "\"id\":{\"type\":\"string\"},"
          + "\"age\":{\"type\":\"integer\"},"
          + "\"balanceCents\":{\"type\":\"integer\"},"
          + "\"rating\":{\"type\":\"number\"},"
          + "\"active\":{\"type\":\"boolean\"},"
          + "\"payload\":{\"type\":\"string\",\"format\":\"binary\"},"
          + "\"createdAtMs\":{\"type\":\"integer\"},"
          + "\"birthDate\":{\"type\":\"string\",\"format\":\"date\"},"
          + "\"priceUsd\":{\"type\":\"number\"},"
          + "\"priority\":{\"type\":\"string\",\"enum\":[\"LOW\",\"MEDIUM\",\"HIGH\"]},"
          + "\"guid\":{\"type\":\"string\",\"format\":\"uuid\"},"
          + "\"homeAddress\":{\"$ref\":\"shared/address.json\"},"
          + "\"workAddress\":{\"$ref\":\"shared/address.json\"},"
          + "\"tags\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
          + "\"attrs\":{\"type\":\"object\","
          + "\"additionalProperties\":{\"type\":\"string\"},"
          + "\"connect.type\":\"map\"},"
          + "\"contact\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"integer\"}]},"
          + "\"nickname\":{\"type\":[\"string\",\"null\"]},"
          + "\"nested\":{\"type\":\"object\","
          + "\"properties\":{\"label\":{\"type\":\"string\"},"
          + "\"kind\":{\"type\":\"string\",\"enum\":[\"A\",\"B\",\"C\"]}},"
          + "\"required\":[\"label\",\"kind\"]}"
          + "},"
          + "\"required\":[\"id\",\"age\",\"balanceCents\",\"rating\",\"active\","
          + "\"payload\",\"createdAtMs\",\"birthDate\",\"priceUsd\",\"priority\",\"guid\","
          + "\"homeAddress\",\"workAddress\",\"tags\",\"attrs\",\"contact\",\"nested\"]}";

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
    assertJsonPayloadSemanticallyEqual(fx.sourceBytes, restoredBytes);

    int targetWireId = ByteBuffer.wrap(restoredBytes, 1, 4).getInt();
    ParsedSchema targetPersonSchema = targetSchemaRegistry.getSchemaById(targetWireId);
    assertNotNull("target SR resolves the restored wire ID", targetPersonSchema);
    assertEquals("target Person canonical == source Person canonical",
        sourcePersonCanonical, targetPersonSchema.canonicalString());
    List<SchemaReference> targetRefs = targetPersonSchema.references();
    assertEquals("target Person has one direct reference (Address)", 1, targetRefs.size());
    assertEquals("target ref name matches source",
        ADDRESS_REFNAME, targetRefs.get(0).getName());
    assertEquals("target ref subject matches source", ADDRESS_SUBJECT,
        targetRefs.get(0).getSubject());
    assertTrue("target ref version is a positive integer",
        targetRefs.get(0).getVersion() > 0);

    Object sourceValue = rawJsonDeserializer.deserialize(fx.topic, fx.sourceBytes);
    Object targetValue = targetJsonDeserializer.deserialize(fx.topic, restoredBytes);
    assertNotNull("source deserialized non-null", sourceValue);
    assertNotNull("target deserialized non-null", targetValue);
    assertEquals("restored record equals source semantically", sourceValue, targetValue);
  }

  // ================ Cross-subject reference negatives ================

  private static final String ADDRESS_REFNAME = "shared/address.json";
  private static final String ADDRESS_SUBJECT = "shared/address.json";
  private static final String ADDRESS_SCHEMA_JSON =
      "{\"type\":\"object\",\"title\":\"Address\","
          + "\"properties\":{"
          + "\"street\":{\"type\":\"string\"},"
          + "\"city\":{\"type\":\"string\"},"
          + "\"country\":{\"type\":\"string\"}},"
          + "\"required\":[\"street\",\"city\",\"country\"],"
          + "\"additionalProperties\":false}";
  private static final String PERSON_WITH_REF_SCHEMA_JSON =
      "{\"type\":\"object\",\"title\":\"PersonRef\","
          + "\"properties\":{"
          + "\"name\":{\"type\":\"string\"},"
          + "\"homeAddress\":{\"$ref\":\"shared/address.json\"},"
          + "\"workAddress\":{\"$ref\":\"shared/address.json\"}},"
          + "\"required\":[\"name\",\"homeAddress\",\"workAddress\"],"
          + "\"additionalProperties\":false}";

  private RefTestFixture registerAndProducePersonWithAddressRef() throws Exception {
    String topic = nextTopic();
    JsonSchema addressSchema = new JsonSchema(ADDRESS_SCHEMA_JSON);
    schemaRegistry.register(ADDRESS_SUBJECT, addressSchema);
    SchemaReference addressRef =
        new SchemaReference(ADDRESS_REFNAME, ADDRESS_SUBJECT, 1);
    Map<String, String> resolved = ImmutableMap.of(
        ADDRESS_REFNAME, addressSchema.canonicalString());
    JsonSchema personSchema = new JsonSchema(
        PERSON_WITH_REF_SCHEMA_JSON,
        Collections.singletonList(addressRef),
        resolved,
        null);
    schemaRegistry.register(topic + "-value", personSchema);

    JsonNode record = JSON.readTree(
        "{\"name\":\"Alice\","
            + "\"homeAddress\":{\"street\":\"1 Main St\",\"city\":\"Springfield\","
            + "\"country\":\"US\"},"
            + "\"workAddress\":{\"street\":\"1 Main St\",\"city\":\"Springfield\","
            + "\"country\":\"US\"}}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);
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
  public void testUseOptionalForNonRequiredOnPreservesRoundTripWithOmittedFields() throws Exception {
    converter.configure(backupConfigWith("use.optional.for.nonrequired", "true"), false);

    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"OptionalFields\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"nickname\":{\"type\":\"string\"}},"
        + "\"required\":[\"id\"],"
        + "\"additionalProperties\":false}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree("{\"id\":\"only-required\"}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWrapperShape(wrapped, SCHEMA_TYPE);
    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertJsonPayloadSemanticallyEqual(sourceBytes, restoredBytes);
  }

  @Test
  public void testUseOptionalForNonRequiredOffFailsOnOmittedNonRequiredField() throws Exception {
    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"OptionalFieldsOff\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"nickname\":{\"type\":\"string\"}},"
        + "\"required\":[\"id\"],"
        + "\"additionalProperties\":false}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree("{\"id\":\"only-required\"}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);

    try {
      converter.toConnectData(topic, sourceBytes);
      fail("Expected DataException when use.optional.for.nonrequired is default false "
          + "and record omits a non-required property");
    } catch (DataException e) {
      assertTrue("message identifies null-for-required-field problem: " + e.getMessage(),
          e.getMessage().contains("Invalid value: null used for required field")
              || e.getMessage().contains("Invalid null value for required"));
    }
  }

  @Test
  public void testAdditionalPropertiesTypedMapWithoutConnectTypeHintLosesContents() throws Exception {
    converter.configure(backupConfigWith("use.optional.for.nonrequired", "true"), false);

    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"WithScores\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"scores\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"integer\"}}},"
        + "\"required\":[\"id\"]}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree("{\"id\":\"map-1\",\"scores\":{\"a\":1,\"b\":2}}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    Object restoredDeser = rawJsonDeserializer.deserialize(topic, restoredBytes);
    String restoredJson = restoredDeser.toString();
    assertTrue("id preserved on restored record: " + restoredJson,
        restoredJson.contains("map-1"));
    assertTrue("scores.a entry dropped without connect.type:map hint: " + restoredJson,
        !restoredJson.contains("\"a\"") && !restoredJson.contains("=1"));
    assertTrue("scores.b entry dropped without connect.type:map hint: " + restoredJson,
        !restoredJson.contains("\"b\"") && !restoredJson.contains("=2"));
  }

  @Test
  public void testAdditionalPropertiesTrueObjectLosesArbitraryExtras() throws Exception {
    converter.configure(backupConfigWith("use.optional.for.nonrequired", "true"), false);

    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"WithExtras\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"extras\":{\"type\":\"object\",\"additionalProperties\":true}},"
        + "\"required\":[\"id\"]}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree(
        "{\"id\":\"extras-1\",\"extras\":{\"foo\":\"bar\",\"n\":9}}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    Object restoredDeser = rawJsonDeserializer.deserialize(topic, restoredBytes);
    String restoredJson = restoredDeser.toString();
    assertTrue("id preserved on restored record: " + restoredJson,
        restoredJson.contains("extras-1"));
    assertTrue("extras.foo dropped without connect.type:map hint: " + restoredJson,
        !restoredJson.contains("\"foo\"") && !restoredJson.contains("bar"));
    assertTrue("extras.n dropped without connect.type:map hint: " + restoredJson,
        !restoredJson.contains("\"n\"") && !restoredJson.contains("=9"));
  }

  @Test
  public void testObjectAdditionalPropertiesFalseSchemaExplicitlyRestricts() {
    converter.configure(backupConfigWith("object.additional.properties", "false"), false);
    plainConverter.configure(
        backupConfigWith("object.additional.properties", "false"), false);

    Schema schema = SchemaBuilder.struct()
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "a-1");

    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
    String raw = ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_RAW_SCHEMA);
    assertTrue("raw schema restricts additional properties: " + raw,
        raw.contains("\"additionalProperties\":false"));
  }

  @Test
  public void testDecimalFormatBase64DefaultPreservesRoundTrip() throws Exception {
    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"WithNumber\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"amount\":{\"type\":\"number\"}},"
        + "\"required\":[\"id\",\"amount\"],"
        + "\"additionalProperties\":false}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree("{\"id\":\"dec-1\",\"amount\":123.45}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertJsonPayloadSemanticallyEqual(sourceBytes, restoredBytes);
  }

  @Test
  public void testDecimalFormatNumericSymmetricPreservesRoundTrip() throws Exception {
    converter.configure(backupConfigWith("decimal.format", "NUMERIC"), false);

    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"WithNumberNumeric\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"amount\":{\"type\":\"number\"}},"
        + "\"required\":[\"id\",\"amount\"],"
        + "\"additionalProperties\":false}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree("{\"id\":\"dec-2\",\"amount\":99.99}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);

    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertJsonPayloadSemanticallyEqual(sourceBytes, restoredBytes);
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
        .name("Direct")
        .field("text", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("text", "direct");

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
      assertEquals("Malformed backup wrapper: missing '" + BackupWrapper.FIELD_RAW_SCHEMA
          + "' for topic " + TOPIC + ". Cannot guarantee pristine restore.", e.getMessage());
    }
  }

  @Test
  public void testEdgeBasicRoundtripDeserializes() {
    Schema schema = SchemaBuilder.struct()
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
      assertEquals("JsonSchemaConverter received wrapper with schemaType='AVRO', "
          + "expected 'JSON_SCHEMA'", e.getMessage());
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
      assertEquals("Converting byte[] to Kafka Connect data failed due to "
          + "serialization error of topic " + TOPIC + ": ", e.getMessage());
    }
  }

  @Test
  public void testEdgeSinkWrapErrorClassifiedAsBackupException() throws Exception {
    // A SR client that fetches individual schemas normally (so deserialize succeeds)
    // but throws on getAllVersionsById, which only the wrap path uses.
    SchemaRegistryClient wrapFailingSr = new MockSchemaRegistryClient(
        ImmutableList.of(new JsonSchemaProvider())) {
      @Override
      public Collection<SubjectVersion> getAllVersionsById(int id) {
        throw new SerializationException("simulated SR unavailable during wrap");
      }
    };
    JsonSchemaConverter wrapFailingConverter = new JsonSchemaConverter(wrapFailingSr);
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_KEY, FAKE_SR_URL);
    cfg.put(BACKUP_ENABLED_KEY, "true");
    wrapFailingConverter.configure(cfg, false);

    String topic = nextTopic();
    String schemaJson = "{\"type\":\"object\","
        + "\"properties\":{\"id\":{\"type\":\"string\"}},"
        + "\"required\":[\"id\"],\"additionalProperties\":false}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    wrapFailingSr.register(topic + "-value", jsonSchema);
    JsonNode record = JSON.readTree("{\"id\":\"wrap-fail\"}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(wrapFailingSr);
    byte[] bytes = refSerializer.serialize(topic, record);

    try {
      wrapFailingConverter.toConnectData(topic, bytes);
      fail("Expected DataException when wrap-path SR call fails");
    } catch (DataException e) {
      assertEquals("Failed to wrap JSON Schema backup for topic " + topic, e.getMessage());
      assertTrue("cause is the simulated SerializationException",
          e.getCause() instanceof SerializationException);
    }
  }

  @Test
  public void testHeaderSchemaIdSerializerBackupCapturesSchemaId() {
    SchemaRegistryClient sr = new MockSchemaRegistryClient(
        ImmutableList.of(new JsonSchemaProvider()));
    JsonSchemaConverter headerBackupConverter = new JsonSchemaConverter(sr);
    JsonSchemaConverter headerPlainConverter = new JsonSchemaConverter(sr);
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
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "h-1").put(FIELD_NAME, "header-test");

    Headers headers = new RecordHeaders();
    byte[] serialized = headerPlainConverter.fromConnectData(TOPIC, headers, schema, value);

    assertNotNull("value schema ID header present", headers.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER));

    SchemaAndValue wrapped = headerBackupConverter.toConnectData(TOPIC, headers, serialized);

    assertNotNull("wrapper produced", wrapped);
    assertEquals("wrapper schema name", BackupWrapper.NAME, wrapped.schema().name());
    Struct w = (Struct) wrapped.value();
    assertEquals("schema type", SCHEMA_TYPE, w.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull("raw schema captured", w.getString(BackupWrapper.FIELD_RAW_SCHEMA));
    // Header-path populates schemaGuid; schemaId may be inferred by SR client depending on
    // the deserializer implementation. At minimum, guid must be present.
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
  public void testConfigMismatchUseOptionalForNonRequiredSinkOnSourceOffStillWorks()
      throws Exception {
    converter.configure(backupConfigWith("use.optional.for.nonrequired", "true"), false);

    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"MismatchOptional\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"nickname\":{\"type\":\"string\"}},"
        + "\"required\":[\"id\"],"
        + "\"additionalProperties\":false}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree("{\"id\":\"only-req\"}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);
    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);

    converter.configure(backupConfigWith("use.optional.for.nonrequired", "false"), false);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertJsonPayloadSemanticallyEqual(sourceBytes, restoredBytes);
  }

  @Test
  public void testConfigMismatchDecimalFormatBase64OnSinkNumericOnSourceStillWorks()
      throws Exception {
    String topic = nextTopic();
    String schemaJson = "{"
        + "\"type\":\"object\","
        + "\"title\":\"MismatchDecimal\","
        + "\"properties\":{"
        + "\"id\":{\"type\":\"string\"},"
        + "\"amount\":{\"type\":\"number\"}},"
        + "\"required\":[\"id\",\"amount\"],"
        + "\"additionalProperties\":false}";
    JsonSchema jsonSchema = new JsonSchema(schemaJson);
    schemaRegistry.register(topic + "-value", jsonSchema);

    JsonNode record = JSON.readTree("{\"id\":\"dec-mm\",\"amount\":50.5}");
    KafkaJsonSchemaSerializer<Object> refSerializer =
        newReferenceAwareJsonSchemaSerializer(schemaRegistry);
    byte[] sourceBytes = refSerializer.serialize(topic, record);
    SchemaAndValue wrapped = converter.toConnectData(topic, sourceBytes);

    converter.configure(backupConfigWith("decimal.format", "NUMERIC"), false);
    byte[] restoredBytes = converter.fromConnectData(topic, wrapped.schema(), wrapped.value());

    assertWireSchemaIdPreserved(sourceBytes, restoredBytes);
    assertJsonPayloadSemanticallyEqual(sourceBytes, restoredBytes);
  }

  @Test
  public void testConfigMismatchObjectAdditionalPropertiesSinkFalseSourceDefaultStillWorks() {
    converter.configure(backupConfigWith("object.additional.properties", "false"), false);
    plainConverter.configure(
        backupConfigWith("object.additional.properties", "false"), false);

    Schema schema = SchemaBuilder.struct()
        .field(FIELD_ID, Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put(FIELD_ID, "asymm-1");
    byte[] originalBytes = plainConverter.fromConnectData(TOPIC, schema, value);
    SchemaAndValue wrapped = converter.toConnectData(TOPIC, originalBytes);

    converter.configure(backupConfigWith("object.additional.properties", "true"), false);
    byte[] restoredBytes = converter.fromConnectData(TOPIC, wrapped.schema(), wrapped.value());

    assertBytesExact(originalBytes, restoredBytes);
    assertWireSchemaIdPreserved(originalBytes, restoredBytes);
  }
}
