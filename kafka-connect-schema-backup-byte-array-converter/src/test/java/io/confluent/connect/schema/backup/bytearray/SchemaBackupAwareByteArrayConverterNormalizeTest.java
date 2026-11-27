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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
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
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code normalize.schemas=true} on restore, for schemas whose normalized text differs from
 * the source text: the target stores the normalized schema, the payload bytes are unchanged
 * and records decode on the target.
 */
public class SchemaBackupAwareByteArrayConverterNormalizeTest {

  private static final String TOPIC = "events";
  private static final String RESTORE_TOPIC = "restored-events";
  private static final String TARGET_SUBJECT = RESTORE_TOPIC + "-value";
  private static final String SR_URL_CONFIG =
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
  private static final String SR_URL_VALUE = "http://fake-url";
  private static final String BACKUP_ENABLED = "schema.backup.enabled";
  private static final String TRUE = "true";
  private static final ObjectMapper JSON = new ObjectMapper();

  /** Custom properties out of order; normalizing sorts them. */
  private static final String AVRO_SCHEMA = "{\"type\":\"record\",\"name\":\"Order\","
      + "\"namespace\":\"io.confluent.test\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\",\"zeta\":\"1\",\"alpha\":\"2\"},"
      + "{\"name\":\"qty\",\"type\":\"int\"}]}";
  private static final String FIELD_ID = "id";
  private static final String FIELD_QTY = "qty";
  private static final String ID_VALUE = "o-1";
  private static final int QTY_VALUE = 3;

  /** Properties out of key order; normalizing sorts them. */
  private static final String JSON_SCHEMA = "{\"type\":\"object\",\"title\":\"Order\","
      + "\"properties\":{\"qty\":{\"type\":\"integer\"},\"id\":{\"type\":\"string\"}}}";
  private static final String JSON_VALUE = "{\"qty\":3,\"id\":\"o-1\"}";

  /** No map-entry messages: normalizing changes the text but not message positions. */
  private static final String PROTO_SCHEMA = "syntax = \"proto3\";\n"
      + "package io.confluent.test;\n"
      + "message First {\n  string a = 1;\n}\n"
      + "message Second {\n  int32 z = 2;\n  string b = 1;\n}\n";
  private static final String SECOND = "io.confluent.test.Second";
  private static final String FIELD_B = "b";

  private SchemaRegistryClient source;
  private SchemaRegistryClient target;

  @Before
  public void setUp() {
    source = newRegistry();
    target = newRegistry();
  }

  @Test
  public void testAvroRestoresUnchangedPayloadUnderNormalizedSchema() throws Exception {
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(source);
    serializer.configure(srConfig(), false);
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(AVRO_SCHEMA));
    record.put(FIELD_ID, ID_VALUE);
    record.put(FIELD_QTY, QTY_VALUE);
    byte[] original = serializer.serialize(TOPIC, record);

    byte[] restored = normalizedRestore(original);

    assertNormalizedOnTarget(new AvroSchema(AVRO_SCHEMA), original, restored);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(target);
    deserializer.configure(srConfig(), false);
    GenericRecord decoded = (GenericRecord) deserializer.deserialize(RESTORE_TOPIC, restored);
    assertEquals(ID_VALUE, decoded.get(FIELD_ID).toString());
    assertEquals(QTY_VALUE, decoded.get(FIELD_QTY));
  }

  @Test
  public void testJsonSchemaRestoresUnchangedPayloadUnderNormalizedSchema() throws Exception {
    KafkaJsonSchemaSerializer<Object> serializer = new KafkaJsonSchemaSerializer<>(source);
    serializer.configure(srConfig(), false);
    JsonNode value = JSON.readTree(JSON_VALUE);
    byte[] original = serializer.serialize(TOPIC,
        JsonSchemaUtils.envelope(new JsonSchema(JSON_SCHEMA), value));

    byte[] restored = normalizedRestore(original);

    assertNormalizedOnTarget(new JsonSchema(JSON_SCHEMA), original, restored);
    KafkaJsonSchemaDeserializer<Object> deserializer = new KafkaJsonSchemaDeserializer<>(target);
    deserializer.configure(srConfig(), false);
    assertEquals(value, JSON.valueToTree(deserializer.deserialize(RESTORE_TOPIC, restored)));
  }

  @Test
  public void testProtobufRestoresUnchangedPayloadUnderNormalizedSchema() throws Exception {
    KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(source);
    serializer.configure(srConfig(), false);
    Descriptor descriptor = new ProtobufSchema(PROTO_SCHEMA).copy(SECOND).toDescriptor();
    DynamicMessage message = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName(FIELD_B), ID_VALUE).build();
    byte[] original = serializer.serialize(TOPIC, message);

    byte[] restored = normalizedRestore(original);

    assertEquals(indexesOf(original), indexesOf(restored));
    assertNormalizedOnTarget(new ProtobufSchema(PROTO_SCHEMA), original, restored);
    KafkaProtobufDeserializer<Message> deserializer = new KafkaProtobufDeserializer<>(target);
    deserializer.configure(srConfig(), false);
    Message decoded = deserializer.deserialize(RESTORE_TOPIC, restored);
    assertEquals(SECOND, decoded.getDescriptorForType().getFullName());
    assertEquals(ID_VALUE,
        decoded.getField(decoded.getDescriptorForType().findFieldByName(FIELD_B)));
  }

  private byte[] normalizedRestore(byte[] original) {
    SchemaAndValue wrapped = converter(source, new HashMap<>()).toConnectData(TOPIC, original);
    Map<String, Object> restoreCfg = new HashMap<>();
    restoreCfg.put(AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, TRUE);
    return converter(target, restoreCfg)
        .fromConnectData(RESTORE_TOPIC, wrapped.schema(), wrapped.value());
  }

  /**
   * The target stores the normalized text (which differs from the source text), the restored
   * record points at it, and the payload after the schema ID framing is unchanged.
   */
  private void assertNormalizedOnTarget(ParsedSchema sourceSchema, byte[] original,
      byte[] restored) throws Exception {
    String normalized = sourceSchema.normalize().canonicalString();
    assertNotEquals(sourceSchema.canonicalString(), normalized);
    int targetId = target.getLatestSchemaMetadata(TARGET_SUBJECT).getId();
    assertEquals(targetId, ByteBuffer.wrap(restored, 1, 4).getInt());
    assertEquals(normalized, target.getSchemaById(targetId).canonicalString());
    String type = sourceSchema.schemaType();
    assertArrayEquals(payloadOf(original, type), payloadOf(restored, type));
  }

  /** Bytes after the schema ID framing (and, for Protobuf, the message indexes). */
  private static byte[] payloadOf(byte[] bytes, String schemaType) {
    ByteBuffer rest = new SchemaId(schemaType).fromBytes(ByteBuffer.wrap(bytes));
    byte[] out = new byte[rest.remaining()];
    rest.get(out);
    return out;
  }

  private static List<Integer> indexesOf(byte[] bytes) {
    SchemaId schemaId = new SchemaId(ProtobufSchema.TYPE);
    schemaId.fromBytes(ByteBuffer.wrap(bytes));
    return schemaId.getMessageIndexes();
  }

  private static Map<String, Object> srConfig() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    return cfg;
  }

  private static SchemaBackupAwareByteArrayConverter converter(
      SchemaRegistryClient registry, Map<String, Object> cfg) {
    Map<String, Object> converterCfg = new HashMap<>(cfg);
    converterCfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    converterCfg.put(BACKUP_ENABLED, TRUE);
    SchemaBackupAwareByteArrayConverter converter =
        new SchemaBackupAwareByteArrayConverter(registry);
    converter.configure(converterCfg, false);
    return converter;
  }

  private static SchemaRegistryClient newRegistry() {
    return new MockSchemaRegistryClient(ImmutableList.of(
        new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()));
  }
}
