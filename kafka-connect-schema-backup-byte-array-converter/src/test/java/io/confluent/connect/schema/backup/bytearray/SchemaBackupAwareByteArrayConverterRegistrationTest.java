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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

/**
 * Target-side schema registration for {@link SchemaBackupAwareByteArrayConverter}:
 * references, subject and context strategies, and the serializer settings it honours
 * ({@code auto.register.schemas}, {@code normalize.schemas}, {@code propagate.schema.tags})
 * or rejects on restore.
 */
public class SchemaBackupAwareByteArrayConverterRegistrationTest {

  private static final String SOURCE_TOPIC = "orders";
  private static final String RESTORE_TOPIC = "restored-orders";
  private static final String VALUE_SUFFIX = "-value";
  private static final String TOPIC_RECORD_SEPARATOR = "-";
  private static final String SR_URL_CONFIG = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
  private static final String SR_URL_VALUE = "http://fake-url";
  private static final String TRUE = "true";
  private static final String FALSE = "false";
  private static final String CONTEXT = "restore-ctx";
  private static final String UNSET_SCHEMA_ID = String.valueOf(
      AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID_DEFAULT);
  private static final String SAMPLE_SCHEMA_ID = "42";
  private static final String SAMPLE_SCHEMA_GUID = "6c8b3c3e-1f0e-4c8e-9c4c-2b1a0f6e9d11";
  private static final String SAMPLE_METADATA = "app=orders";
  private static final int PREFIX_LENGTH = 5;
  private static final int FORBIDDEN_STATUS = 403;
  private static final int FORBIDDEN_CODE = 40301;
  private static final String FORBIDDEN_MESSAGE = "read-only credentials";

  private static final String NAMESPACE = "io.confluent.test";
  private static final String ADDRESS_NAME = NAMESPACE + ".Address";
  private static final String ORDER_NAME = NAMESPACE + ".Order";
  private static final String ADDRESS_SUBJECT = "address";
  private static final String FIELD_CITY = "city";
  private static final String FIELD_ID = "id";
  private static final String FIELD_SHIP_TO = "shipTo";
  private static final String CITY_VALUE = "Chennai";
  private static final String ID_VALUE = "order-1";

  private static final String ADDRESS_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"" + NAMESPACE + "\","
          + "\"fields\":[{\"name\":\"" + FIELD_CITY + "\",\"type\":\"string\"}]}";
  private static final String ORDER_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"" + NAMESPACE + "\","
          + "\"fields\":["
          + "{\"name\":\"" + FIELD_ID + "\",\"type\":\"string\"},"
          + "{\"name\":\"" + FIELD_SHIP_TO + "\",\"type\":\"" + ADDRESS_NAME + "\"}"
          + "]}";
  /** Order without references, for tests that only need one schema. */
  private static final String SIMPLE_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"" + NAMESPACE + "\","
          + "\"fields\":[{\"name\":\"" + FIELD_ID + "\",\"type\":\"string\"}]}";

  /** Protobuf and JSON Schema forms of the simple Order, for per-format checks. */
  private static final String SIMPLE_PROTO =
      "syntax = \"proto3\";\n"
          + "package " + NAMESPACE + ";\n"
          + "message Order {\n"
          + "  string " + FIELD_ID + " = 1;\n"
          + "}\n";
  private static final String SIMPLE_JSON_SCHEMA =
      "{\"title\":\"" + ORDER_NAME + "\",\"type\":\"object\","
          + "\"properties\":{\"" + FIELD_ID + "\":{\"type\":\"string\"}},"
          + "\"required\":[\"" + FIELD_ID + "\"]}";
  /** Registered first on the source so source and target schema IDs differ. */
  private static final String FILLER_SUBJECT = "filler" + VALUE_SUFFIX;
  private static final String FILLER_SCHEMA = "\"string\"";
  private static final ObjectMapper JSON = new ObjectMapper();

  private enum Format { AVRO, PROTOBUF, JSON_SCHEMA }

  private RecordingSchemaRegistryClient source;
  private RecordingSchemaRegistryClient target;

  @Before
  public void setUp() {
    source = new RecordingSchemaRegistryClient();
    target = new RecordingSchemaRegistryClient();
  }

  // ---------------- References ----------------

  @Test
  public void testReferencesAreRegisteredBeforeMainSchemaOnTarget() throws Exception {
    byte[] original = orderWithReferenceBytes();
    byte[] restored = backupThenRestore(original, Collections.emptyMap());

    assertTrue(target.getAllSubjects().contains(ADDRESS_SUBJECT));
    assertTrue(target.getAllSubjects().contains(RESTORE_TOPIC + VALUE_SUFFIX));
    assertEquals(ADDRESS_SUBJECT, target.registeredSubjects.get(0));
    assertPayloadUnchanged(original, restored);
    assertOrderRecord(deserializeOnTarget(restored));
  }

  @Test
  public void testAutoRegisterFalseUsesExistingSchemasAndNeverRegisters() throws Exception {
    byte[] original = orderWithReferenceBytes();
    preRegisterOrderOnTarget(RESTORE_TOPIC + VALUE_SUFFIX);
    target.readOnly = true;

    byte[] restored = backupThenRestore(original,
        Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, FALSE));

    assertPayloadUnchanged(original, restored);
    assertOrderRecord(deserializeOnTarget(restored));
  }

  @Test
  public void testAutoRegisterFalseFailsWhenSchemaMissingOnTarget() throws Exception {
    byte[] original = orderWithReferenceBytes();
    target.readOnly = true;

    try {
      backupThenRestore(original,
          Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, FALSE));
      fail("Expected restore to fail when the schema is not on the target registry");
    } catch (ConnectException e) {
      assertTrue(target.registeredSubjects.isEmpty());
    }
  }

  @Test
  public void testAutoRegisterDefaultFailsOnReadOnlyRegistry() throws Exception {
    byte[] original = orderWithReferenceBytes();
    preRegisterOrderOnTarget(RESTORE_TOPIC + VALUE_SUFFIX);
    target.readOnly = true;

    try {
      backupThenRestore(original, Collections.emptyMap());
      fail("Expected auto.register.schemas=true to attempt registration");
    } catch (ConfigException e) {
      // BackupExceptionMapper surfaces SR authorization failures as ConfigException.
      assertTrue(e.getMessage(), e.getMessage().contains(String.valueOf(FORBIDDEN_STATUS)));
    }
  }

  // ---------------- normalize.schemas and propagate.schema.tags ----------------

  @Test
  public void testNormalizeAndPropagateTagsArePassedToRegistration() throws Exception {
    Map<String, Object> restoreCfg = new HashMap<>();
    restoreCfg.put(AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, TRUE);
    restoreCfg.put(AbstractKafkaSchemaSerDeConfig.PROPAGATE_SCHEMA_TAGS, TRUE);

    backupThenRestore(simpleBytes(), restoreCfg);

    assertEquals(Collections.singletonList(Boolean.TRUE), target.normalizeFlags);
    assertEquals(Collections.singletonList(Boolean.TRUE), target.propagateTagFlags);
  }

  @Test
  public void testNormalizeAndPropagateTagsDefaultToFalse() throws Exception {
    backupThenRestore(simpleBytes(), Collections.emptyMap());

    assertEquals(Collections.singletonList(Boolean.FALSE), target.normalizeFlags);
    assertEquals(Collections.singletonList(Boolean.FALSE), target.propagateTagFlags);
  }

  // ---------------- Subject and context strategies ----------------

  @Test
  public void testDefaultStrategyFollowsRestoreTopic() throws Exception {
    backupThenRestore(simpleBytes(), Collections.emptyMap());

    assertEquals(Collections.singletonList(RESTORE_TOPIC + VALUE_SUFFIX),
        target.registeredSubjects);
  }

  @Test
  public void testRecordNameStrategyIsHonoured() throws Exception {
    backupThenRestore(simpleBytes(), Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
        RecordNameStrategy.class.getName()));

    assertEquals(Collections.singletonList(ORDER_NAME), target.registeredSubjects);
  }

  @Test
  public void testTopicRecordNameStrategyIsHonoured() throws Exception {
    backupThenRestore(simpleBytes(), Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
        TopicRecordNameStrategy.class.getName()));

    assertEquals(Collections.singletonList(RESTORE_TOPIC + TOPIC_RECORD_SEPARATOR + ORDER_NAME),
        target.registeredSubjects);
  }

  @Test
  public void testContextNameStrategyQualifiesSubject() throws Exception {
    backupThenRestore(simpleBytes(), Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        FixedContextNameStrategy.class.getName()));

    assertEquals(
        Collections.singletonList(
            QualifiedSubject.normalizeContext(CONTEXT) + RESTORE_TOPIC + VALUE_SUFFIX),
        target.registeredSubjects);
  }

  @Test
  public void testSubjectStrategyReceivesRegistryClient() throws Exception {
    ClientCapturingStrategy.lastClient = null;
    backupThenRestore(simpleBytes(), Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
        ClientCapturingStrategy.class.getName()));

    assertNotNull(ClientCapturingStrategy.lastClient);
  }

  // ---------------- Same checks for every format ----------------
  // Subject naming, ID rewriting and the auto.register.schemas=false lookup each depend
  // on the format's own schema type, so they are checked for Avro, Protobuf and JSON Schema.

  @Test
  public void testAvroRestoreUsesTargetSchemaId() throws Exception {
    assertRestoreUsesTargetSchemaId(Format.AVRO);
  }

  @Test
  public void testProtobufRestoreUsesTargetSchemaId() throws Exception {
    assertRestoreUsesTargetSchemaId(Format.PROTOBUF);
  }

  @Test
  public void testJsonSchemaRestoreUsesTargetSchemaId() throws Exception {
    assertRestoreUsesTargetSchemaId(Format.JSON_SCHEMA);
  }

  @Test
  public void testProtobufRecordNameStrategyUsesMessageName() throws Exception {
    assertRecordNameStrategy(Format.PROTOBUF);
  }

  @Test
  public void testJsonSchemaRecordNameStrategyUsesTitle() throws Exception {
    assertRecordNameStrategy(Format.JSON_SCHEMA);
  }

  @Test
  public void testProtobufAutoRegisterFalseFindsExistingSchema() throws Exception {
    assertAutoRegisterFalseFindsExistingSchema(Format.PROTOBUF);
  }

  @Test
  public void testJsonSchemaAutoRegisterFalseFindsExistingSchema() throws Exception {
    assertAutoRegisterFalseFindsExistingSchema(Format.JSON_SCHEMA);
  }

  @Test
  public void testProtobufAutoRegisterFalseFailsWhenSchemaMissingOnTarget() throws Exception {
    assertAutoRegisterFalseFailsWhenMissing(Format.PROTOBUF);
  }

  @Test
  public void testJsonSchemaAutoRegisterFalseFailsWhenSchemaMissingOnTarget() throws Exception {
    assertAutoRegisterFalseFailsWhenMissing(Format.JSON_SCHEMA);
  }

  private void assertRestoreUsesTargetSchemaId(Format format) throws Exception {
    byte[] original = produceSimple(format);
    byte[] restored = backupThenRestore(original, Collections.emptyMap());

    int sourceId = schemaIdOf(original);
    int targetId = schemaIdOf(restored);
    assertNotEquals("restored bytes must carry the target ID", sourceId, targetId);
    assertEquals(source.getSchemaById(sourceId).canonicalString(),
        target.getSchemaById(targetId).canonicalString());
    assertEquals(Collections.singletonList(RESTORE_TOPIC + VALUE_SUFFIX),
        target.registeredSubjects);
    assertPayloadUnchanged(original, restored);
    assertEquals(ID_VALUE, decodeIdOnTarget(format, restored));
  }

  private void assertRecordNameStrategy(Format format) throws Exception {
    byte[] restored = backupThenRestore(produceSimple(format), Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
        RecordNameStrategy.class.getName()));

    assertEquals(Collections.singletonList(ORDER_NAME), target.registeredSubjects);
    assertEquals(ID_VALUE, decodeIdOnTarget(format, restored));
  }

  private void assertAutoRegisterFalseFindsExistingSchema(Format format) throws Exception {
    byte[] original = produceSimple(format);
    target.register(RESTORE_TOPIC + VALUE_SUFFIX, simpleSchema(format));
    target.resetRecording();
    target.readOnly = true;

    byte[] restored = backupThenRestore(original,
        Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, FALSE));

    assertTrue(target.registeredSubjects.isEmpty());
    assertPayloadUnchanged(original, restored);
    assertEquals(ID_VALUE, decodeIdOnTarget(format, restored));
  }

  private void assertAutoRegisterFalseFailsWhenMissing(Format format) throws Exception {
    byte[] original = produceSimple(format);
    target.readOnly = true;
    try {
      backupThenRestore(original,
          Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, FALSE));
      fail("Expected restore to fail when the " + format + " schema is not on the target");
    } catch (ConnectException e) {
      assertTrue(target.registeredSubjects.isEmpty());
    }
  }

  // ---------------- Settings rejected on restore ----------------

  @Test
  public void testUseLatestVersionRejectedOnRestore() throws Exception {
    assertRestoreRejected(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, TRUE);
  }

  @Test
  public void testUseLatestWithMetadataRejectedOnRestore() throws Exception {
    assertRestoreRejected(
        AbstractKafkaSchemaSerDeConfig.USE_LATEST_WITH_METADATA, SAMPLE_METADATA);
  }

  @Test
  public void testUseSchemaIdRejectedOnRestore() throws Exception {
    assertRestoreRejected(AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID, SAMPLE_SCHEMA_ID);
  }

  @Test
  public void testUseSchemaGuidRejectedOnRestore() throws Exception {
    assertRestoreRejected(AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_GUID, SAMPLE_SCHEMA_GUID);
  }

  @Test
  public void testDefaultsOfRejectedSettingsAreAllowed() throws Exception {
    Map<String, Object> restoreCfg = new HashMap<>();
    restoreCfg.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, FALSE);
    restoreCfg.put(AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID, UNSET_SCHEMA_ID);

    byte[] original = simpleBytes();
    assertPayloadUnchanged(original, backupThenRestore(original, restoreCfg));
  }

  @Test
  public void testRejectedSettingsDoNotAffectBackup() throws Exception {
    Map<String, Object> cfg = converterConfig();
    cfg.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, TRUE);
    SchemaBackupAwareByteArrayConverter backup = newConverter(source, cfg);

    SchemaAndValue wrapped = backup.toConnectData(SOURCE_TOPIC, simpleBytes());

    assertNotNull(wrapped.value());
  }

  // ---------------- Helpers ----------------

  private void assertRestoreRejected(String key, String value) throws Exception {
    try {
      backupThenRestore(simpleBytes(), Collections.singletonMap(key, value));
      fail("Expected ConfigException for " + key);
    } catch (ConfigException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(key));
      assertTrue(target.registeredSubjects.isEmpty());
    }
  }

  private byte[] backupThenRestore(byte[] original, Map<String, ?> restoreOverrides) {
    SchemaBackupAwareByteArrayConverter backup = newConverter(source, converterConfig());
    SchemaAndValue wrapped = backup.toConnectData(SOURCE_TOPIC, original);

    Map<String, Object> restoreCfg = converterConfig();
    restoreCfg.putAll(restoreOverrides);
    SchemaBackupAwareByteArrayConverter restore = newConverter(target, restoreCfg);
    return restore.fromConnectData(
        RESTORE_TOPIC, new RecordHeaders(), wrapped.schema(), wrapped.value());
  }

  private static Map<String, Object> converterConfig() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    cfg.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, TRUE);
    return cfg;
  }

  private static SchemaBackupAwareByteArrayConverter newConverter(
      MockSchemaRegistryClient client, Map<String, Object> cfg) {
    SchemaBackupAwareByteArrayConverter converter = new SchemaBackupAwareByteArrayConverter(client);
    converter.configure(cfg, false);
    return converter;
  }

  private AvroSchema orderSchemaWithReference() {
    List<SchemaReference> refs = Collections.singletonList(
        new SchemaReference(ADDRESS_NAME, ADDRESS_SUBJECT, 1));
    return new AvroSchema(ORDER_SCHEMA, refs,
        Collections.singletonMap(ADDRESS_NAME, ADDRESS_SCHEMA), null);
  }

  /** Registers Address and Order on the source registry and returns Order wire bytes. */
  private byte[] orderWithReferenceBytes() throws Exception {
    source.register(ADDRESS_SUBJECT, new AvroSchema(ADDRESS_SCHEMA));
    AvroSchema order = orderSchemaWithReference();
    int id = source.register(SOURCE_TOPIC + VALUE_SUFFIX, order);
    source.resetRecording();

    org.apache.avro.Schema avro = order.rawSchema();
    GenericRecord address =
        new GenericData.Record(avro.getField(FIELD_SHIP_TO).schema());
    address.put(FIELD_CITY, CITY_VALUE);
    GenericRecord record = new GenericData.Record(avro);
    record.put(FIELD_ID, ID_VALUE);
    record.put(FIELD_SHIP_TO, address);
    return prefixed(id, encode(avro, record));
  }

  private void preRegisterOrderOnTarget(String subject) throws Exception {
    target.register(ADDRESS_SUBJECT, new AvroSchema(ADDRESS_SCHEMA));
    target.register(subject, orderSchemaWithReference());
    target.resetRecording();
  }

  private byte[] simpleBytes() throws Exception {
    AvroSchema schema = new AvroSchema(SIMPLE_SCHEMA);
    int id = source.register(SOURCE_TOPIC + VALUE_SUFFIX, schema);
    source.resetRecording();
    GenericRecord record = new GenericData.Record(schema.rawSchema());
    record.put(FIELD_ID, ID_VALUE);
    return prefixed(id, encode(schema.rawSchema(), record));
  }

  private static byte[] encode(org.apache.avro.Schema schema, GenericRecord record)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private static byte[] prefixed(int id, byte[] payload) {
    return ByteBuffer.allocate(PREFIX_LENGTH + payload.length)
        .put((byte) 0).putInt(id).put(payload).array();
  }

  private static void assertPayloadUnchanged(byte[] original, byte[] restored) {
    assertArrayEquals(
        Arrays.copyOfRange(original, PREFIX_LENGTH, original.length),
        Arrays.copyOfRange(restored, PREFIX_LENGTH, restored.length));
  }

  private GenericRecord deserializeOnTarget(byte[] bytes) {
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(target);
    deserializer.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    return (GenericRecord) deserializer.deserialize(RESTORE_TOPIC, bytes);
  }

  private static void assertOrderRecord(GenericRecord record) {
    assertEquals(ID_VALUE, record.get(FIELD_ID).toString());
    GenericRecord shipTo = (GenericRecord) record.get(FIELD_SHIP_TO);
    assertEquals(CITY_VALUE, shipTo.get(FIELD_CITY).toString());
  }

  private static ParsedSchema simpleSchema(Format format) {
    switch (format) {
      case AVRO:
        return new AvroSchema(SIMPLE_SCHEMA);
      case PROTOBUF:
        return new ProtobufSchema(SIMPLE_PROTO);
      default:
        return new JsonSchema(SIMPLE_JSON_SCHEMA);
    }
  }

  /** Produces the simple Order on the source registry with that format's serializer. */
  private byte[] produceSimple(Format format) throws Exception {
    source.register(FILLER_SUBJECT, new AvroSchema(FILLER_SCHEMA));
    Map<String, Object> serdeCfg = Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE);
    byte[] bytes;
    switch (format) {
      case AVRO: {
        AvroSchema schema = new AvroSchema(SIMPLE_SCHEMA);
        int id = source.register(SOURCE_TOPIC + VALUE_SUFFIX, schema);
        GenericRecord record = new GenericData.Record(schema.rawSchema());
        record.put(FIELD_ID, ID_VALUE);
        bytes = prefixed(id, encode(schema.rawSchema(), record));
        break;
      }
      case PROTOBUF: {
        ProtobufSchema schema = new ProtobufSchema(SIMPLE_PROTO);
        Descriptor descriptor = schema.toDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName(FIELD_ID), ID_VALUE)
            .build();
        KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(source);
        serializer.configure(serdeCfg, false);
        bytes = serializer.serialize(SOURCE_TOPIC, null, message, schema);
        break;
      }
      default: {
        JsonSchema schema = new JsonSchema(SIMPLE_JSON_SCHEMA);
        JsonNode payload = JSON.createObjectNode().put(FIELD_ID, ID_VALUE);
        KafkaJsonSchemaSerializer<Object> serializer = new KafkaJsonSchemaSerializer<>(source);
        serializer.configure(serdeCfg, false);
        bytes = serializer.serialize(SOURCE_TOPIC, JsonSchemaUtils.envelope(schema, payload));
        break;
      }
    }
    source.resetRecording();
    return bytes;
  }

  private String decodeIdOnTarget(Format format, byte[] bytes) {
    Map<String, Object> serdeCfg = Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE);
    switch (format) {
      case AVRO:
        return deserializeOnTarget(bytes).get(FIELD_ID).toString();
      case PROTOBUF: {
        KafkaProtobufDeserializer<DynamicMessage> deserializer =
            new KafkaProtobufDeserializer<>(target);
        deserializer.configure(serdeCfg, false);
        DynamicMessage message = deserializer.deserialize(RESTORE_TOPIC, bytes);
        return (String) message.getField(
            message.getDescriptorForType().findFieldByName(FIELD_ID));
      }
      default: {
        KafkaJsonSchemaDeserializer<Object> deserializer = new KafkaJsonSchemaDeserializer<>(target);
        deserializer.configure(serdeCfg, false);
        JsonNode node = JSON.valueToTree(deserializer.deserialize(RESTORE_TOPIC, bytes));
        return node.get(FIELD_ID).asText();
      }
    }
  }

  private static int schemaIdOf(byte[] bytes) {
    return ByteBuffer.wrap(bytes, 1, Integer.BYTES).getInt();
  }

  /** Records registration calls and can simulate read-only credentials. */
  private static class RecordingSchemaRegistryClient extends MockSchemaRegistryClient {
    final List<String> registeredSubjects = new ArrayList<>();
    final List<Boolean> normalizeFlags = new ArrayList<>();
    final List<Boolean> propagateTagFlags = new ArrayList<>();
    boolean readOnly;

    RecordingSchemaRegistryClient() {
      super(Arrays.<SchemaProvider>asList(
          new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()));
    }

    void resetRecording() {
      registeredSubjects.clear();
      normalizeFlags.clear();
      propagateTagFlags.clear();
    }

    private void checkWritable() throws RestClientException {
      if (readOnly) {
        throw new RestClientException(FORBIDDEN_MESSAGE, FORBIDDEN_STATUS, FORBIDDEN_CODE);
      }
    }

    @Override
    public int register(String subject, ParsedSchema schema)
        throws IOException, RestClientException {
      checkWritable();
      registeredSubjects.add(subject);
      return super.register(subject, schema);
    }

    @Override
    public RegisterSchemaResponse registerWithResponse(
        String subject, ParsedSchema schema, boolean normalize, boolean propagateSchemaTags)
        throws IOException, RestClientException {
      checkWritable();
      registeredSubjects.add(subject);
      normalizeFlags.add(normalize);
      propagateTagFlags.add(propagateSchemaTags);
      return super.registerWithResponse(subject, schema, normalize, propagateSchemaTags);
    }
  }

  /** Puts every subject in a fixed context. */
  public static class FixedContextNameStrategy implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return CONTEXT;
    }
  }

  /** TopicNameStrategy that records whether the converter handed it a client. */
  public static class ClientCapturingStrategy extends TopicNameStrategy {
    static volatile SchemaRegistryClient lastClient;

    @Override
    public void setSchemaRegistryClient(SchemaRegistryClient client) {
      lastClient = client;
    }
  }
}
