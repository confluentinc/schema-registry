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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

/**
 * Protobuf files with several messages: with ProtobufConverter in backup mode, the subject
 * follows the record's message on backup and restore.
 */
public class ProtobufConverterBackupMessageNameTest {

  private static final String TOPIC = "events";
  private static final String RESTORE_TOPIC = "restored-events";
  private static final String SR_URL_CONFIG =
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
  private static final String SR_URL_VALUE = "http://fake-url";
  private static final String BACKUP_ENABLED = "schema.backup.enabled";
  private static final String VALUE_STRATEGY =
      AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
  private static final String KEY_STRATEGY =
      AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;
  private static final String RECORD_STRATEGY = RecordNameStrategy.class.getName();
  private static final String TOPIC_RECORD_STRATEGY = TopicRecordNameStrategy.class.getName();
  private static final String VALUE_ID_SERIALIZER =
      AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER;
  private static final String HEADER_SERIALIZER = HeaderSchemaIdSerializer.class.getName();
  // Required by the storage validator for ProtobufConverter backup and restore.
  private static final String ENHANCED_SCHEMA_SUPPORT = "enhanced.protobuf.schema.support";
  private static final String WRAPPER_FOR_RAW_PRIMITIVES = "wrapper.for.raw.primitives";
  private static final String TRUE = "true";
  private static final String FALSE = "false";

  private static final String FIRST = "io.confluent.test.First";
  private static final String SECOND = "io.confluent.test.Second";
  private static final String INNER = "io.confluent.test.Second.Inner";
  private static final String VALUE = "hello";
  private static final Map<String, String> FIELD_OF = new HashMap<>();

  static {
    FIELD_OF.put(FIRST, "a");
    FIELD_OF.put(SECOND, "b");
    FIELD_OF.put(INNER, "c");
  }

  private static final String MULTI_MESSAGE_PROTO = "syntax = \"proto3\";\n"
      + "package io.confluent.test;\n"
      + "message First {\n  string a = 1;\n}\n"
      + "message Second {\n  string b = 1;\n  message Inner {\n    string c = 1;\n  }\n}\n";


  // A file that imports money.proto; records use its second message and a nested one.
  private static final String MONEY_PROTO_NAME = "money.proto";
  private static final String MONEY_SUBJECT = "money";
  private static final String MONEY_PROTO = "syntax = \"proto3\";\n"
      + "package io.confluent.test;\n"
      + "message Money {\n  int64 units = 1;\n}\n";
  private static final String PAYMENT_PROTO = "syntax = \"proto3\";\n"
      + "package io.confluent.test;\n"
      + "import \"" + MONEY_PROTO_NAME + "\";\n"
      + "message Quote {\n  string q = 1;\n}\n"
      + "message Payment {\n  Money amount = 1;\n  message Fee {\n    Money fee = 1;\n  }\n}\n";
  private static final String PAYMENT = "io.confluent.test.Payment";
  private static final String FEE = "io.confluent.test.Payment.Fee";
  private static final String FIELD_AMOUNT = "amount";
  private static final String FIELD_FEE = "fee";
  private static final String FIELD_UNITS = "units";
  private static final long UNITS = 7L;
  private static final int FIRST_VERSION = 1;

  private static final String UNRELATED_SUBJECT = "unrelated-value";
  private static final String UNRELATED_PROTO =
      "syntax = \"proto3\";\nmessage Unrelated {\n  int32 x = 1;\n}\n";

  private SchemaRegistryClient source;
  private SchemaRegistryClient target;

  @Before
  public void setUp() throws Exception {
    source = newRegistry();
    target = newRegistry();
    // Target IDs differ from source IDs, so a restored ID always comes from the target.
    target.register(UNRELATED_SUBJECT, new ProtobufSchema(UNRELATED_PROTO));
  }

  // ---------------- Subject strategies ----------------

  @Test
  public void testRecordNameStrategyUsesEachRecordsMessage() throws Exception {
    Map<String, Object> cfg = strategy(VALUE_STRATEGY, RECORD_STRATEGY);
    for (String message : Arrays.asList(FIRST, SECOND, INNER)) {
      assertBackupAndRestore(cfg, message, message, message);
    }
    assertEquals(Arrays.asList(FIRST, SECOND, INNER, UNRELATED_SUBJECT),
        sorted(target.getAllSubjects()));
  }

  @Test
  public void testTopicRecordNameStrategyUsesEachRecordsMessage() throws Exception {
    Map<String, Object> cfg = strategy(VALUE_STRATEGY, TOPIC_RECORD_STRATEGY);
    for (String message : Arrays.asList(FIRST, SECOND, INNER)) {
      assertBackupAndRestore(cfg, message,
          TOPIC + "-" + message, RESTORE_TOPIC + "-" + message);
    }
  }

  @Test
  public void testInterleavedMessagesKeepTheirOwnSubjects() throws Exception {
    Map<String, Object> cfg = strategy(VALUE_STRATEGY, RECORD_STRATEGY);
    ProtobufConverter backup = converter(source, cfg, false);
    ProtobufConverter restore = converter(target, cfg, false);
    for (String message : Arrays.asList(FIRST, SECOND, FIRST, INNER, SECOND, INNER)) {
      byte[] original = produce(source, cfg, false, null, message);
      SchemaAndValue wrapped = backup.toConnectData(TOPIC, original);
      assertEquals(message, subjectOf(wrapped));
      byte[] restored = restore.fromConnectData(RESTORE_TOPIC, wrapped.schema(), wrapped.value());
      assertMessage(decode(target, restored), message);
    }
  }

  @Test
  public void testKeySideUsesTheRecordsMessage() throws Exception {
    Map<String, Object> cfg = strategy(KEY_STRATEGY, RECORD_STRATEGY);
    byte[] original = produce(source, cfg, true, null, SECOND);
    SchemaAndValue wrapped = converter(source, cfg, true).toConnectData(TOPIC, original);
    assertEquals(SECOND, subjectOf(wrapped));
    converter(target, cfg, true).fromConnectData(RESTORE_TOPIC, wrapped.schema(), wrapped.value());
    assertEquals(Arrays.asList(SECOND, UNRELATED_SUBJECT), sorted(target.getAllSubjects()));
  }

  // ---------------- Header framing and auto.register.schemas=false ----------------

  @Test
  public void testHeaderFramedRecordUsesItsMessage() throws Exception {
    Map<String, Object> cfg = strategy(VALUE_STRATEGY, RECORD_STRATEGY);
    Map<String, Object> producerCfg = new HashMap<>(cfg);
    producerCfg.put(VALUE_ID_SERIALIZER, HEADER_SERIALIZER);
    Headers headers = new RecordHeaders();
    byte[] original = produce(source, producerCfg, false, headers, INNER);

    SchemaAndValue wrapped = converter(source, cfg, false).toConnectData(TOPIC, headers, original);
    assertEquals(INNER, subjectOf(wrapped));
    byte[] restored = converter(target, cfg, false)
        .fromConnectData(RESTORE_TOPIC, wrapped.schema(), wrapped.value());

    assertMessage(decode(target, restored), INNER);
    assertEquals(Arrays.asList(INNER, UNRELATED_SUBJECT), sorted(target.getAllSubjects()));
  }

  @Test
  public void testAutoRegisterFalseLooksUpTheMessageSubject() throws Exception {
    Map<String, Object> cfg = strategy(VALUE_STRATEGY, RECORD_STRATEGY);
    produce(target, cfg, false, null, SECOND);
    int expectedId = target.getLatestSchemaMetadata(SECOND).getId();

    Map<String, Object> restoreCfg = new HashMap<>(cfg);
    restoreCfg.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, FALSE);
    byte[] original = produce(source, cfg, false, null, SECOND);
    SchemaAndValue wrapped = converter(source, cfg, false).toConnectData(TOPIC, original);
    byte[] restored = converter(target, restoreCfg, false)
        .fromConnectData(RESTORE_TOPIC, wrapped.schema(), wrapped.value());

    assertEquals(expectedId, schemaIdOf(restored));
    assertMessage(decode(target, restored), SECOND);
    assertFalse(target.getAllSubjects().contains(FIRST));
  }


  @Test
  public void testImportedReferenceWithNonFirstAndNestedMessages() throws Exception {
    Map<String, Object> cfg = strategy(VALUE_STRATEGY, RECORD_STRATEGY);
    source.register(MONEY_SUBJECT, new ProtobufSchema(MONEY_PROTO));
    for (String[] messageAndField : new String[][] {{PAYMENT, FIELD_AMOUNT}, {FEE, FIELD_FEE}}) {
      String message = messageAndField[0];
      String field = messageAndField[1];
      byte[] original = importingRecord(message, field);

      SchemaAndValue wrapped = converter(source, cfg, false).toConnectData(TOPIC, original);
      assertEquals(message, subjectOf(wrapped));
      assertEquals(Integer.valueOf(FIRST_VERSION),
          ((Struct) wrapped.value()).getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
      byte[] restored = converter(target, cfg, false)
          .fromConnectData(RESTORE_TOPIC, wrapped.schema(), wrapped.value());

      assertEquals(target.getLatestSchemaMetadata(message).getId(), schemaIdOf(restored));
      Message decoded = decode(target, restored);
      assertEquals(message, decoded.getDescriptorForType().getFullName());
      Message money = (Message) decoded.getField(
          decoded.getDescriptorForType().findFieldByName(field));
      assertEquals(UNITS, money.getField(money.getDescriptorForType().findFieldByName(FIELD_UNITS)));
    }
    assertEquals(Arrays.asList(PAYMENT, FEE, MONEY_SUBJECT, UNRELATED_SUBJECT),
        sorted(target.getAllSubjects()));
  }

  // ---------------- Helpers ----------------

  private void assertBackupAndRestore(Map<String, Object> cfg, String message,
      String backupSubject, String targetSubject) throws Exception {
    byte[] original = produce(source, cfg, false, null, message);
    SchemaAndValue wrapped = converter(source, cfg, false).toConnectData(TOPIC, original);
    Struct wrapper = (Struct) wrapped.value();
    assertEquals(backupSubject, wrapper.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertEquals(Integer.valueOf(source.getLatestSchemaMetadata(backupSubject).getVersion()),
        wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));

    byte[] restored = converter(target, cfg, false)
        .fromConnectData(RESTORE_TOPIC, wrapped.schema(), wrapped.value());

    assertEquals(target.getLatestSchemaMetadata(targetSubject).getId(), schemaIdOf(restored));
    assertMessage(decode(target, restored), message);
  }


  /** A record of {@code message} from PAYMENT_PROTO, registered under that message's subject. */
  private byte[] importingRecord(String message, String field) throws Exception {
    ProtobufSchema payment = new ProtobufSchema(PAYMENT_PROTO,
        Collections.singletonList(
            new SchemaReference(MONEY_PROTO_NAME, MONEY_SUBJECT, FIRST_VERSION)),
        Collections.singletonMap(MONEY_PROTO_NAME, MONEY_PROTO), null, null);
    int id = source.register(message, payment);
    ProtobufSchema named = payment.copy(message);
    SchemaId schemaId = new SchemaId(ProtobufSchema.TYPE, id, (String) null);
    schemaId.setMessageIndexes(payment.toMessageIndexes(message, false).indexes());
    Descriptor descriptor = named.toDescriptor();
    FieldDescriptor moneyField = descriptor.findFieldByName(field);
    DynamicMessage money = DynamicMessage.newBuilder(moneyField.getMessageType())
        .setField(moneyField.getMessageType().findFieldByName(FIELD_UNITS), UNITS)
        .build();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(schemaId.idToBytes());
    out.write(DynamicMessage.newBuilder(descriptor).setField(moneyField, money).build()
        .toByteArray());
    return out.toByteArray();
  }

  private static byte[] produce(SchemaRegistryClient registry, Map<String, Object> cfg,
      boolean isKey, Headers headers, String messageName) {
    KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(registry);
    serializer.configure(withUrl(cfg), isKey);
    Descriptor descriptor = new ProtobufSchema(MULTI_MESSAGE_PROTO).copy(messageName)
        .toDescriptor();
    DynamicMessage record = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName(FIELD_OF.get(messageName)), VALUE)
        .build();
    return headers != null
        ? serializer.serialize(TOPIC, headers, record)
        : serializer.serialize(TOPIC, record);
  }

  private static Message decode(SchemaRegistryClient registry, byte[] bytes) {
    KafkaProtobufDeserializer<Message> deserializer = new KafkaProtobufDeserializer<>(registry);
    deserializer.configure(withUrl(new HashMap<>()), false);
    return deserializer.deserialize(RESTORE_TOPIC, bytes);
  }

  private static void assertMessage(Message decoded, String messageName) {
    Descriptor descriptor = decoded.getDescriptorForType();
    assertEquals(messageName, descriptor.getFullName());
    assertEquals(VALUE, decoded.getField(descriptor.findFieldByName(FIELD_OF.get(messageName))));
  }

  private static ProtobufConverter converter(
      SchemaRegistryClient registry, Map<String, Object> cfg, boolean isKey) {
    Map<String, Object> converterCfg = withUrl(cfg);
    converterCfg.put(BACKUP_ENABLED, TRUE);
    converterCfg.put(ENHANCED_SCHEMA_SUPPORT, TRUE);
    converterCfg.put(WRAPPER_FOR_RAW_PRIMITIVES, FALSE);
    ProtobufConverter converter = new ProtobufConverter(registry);
    converter.configure(converterCfg, isKey);
    return converter;
  }

  private static Map<String, Object> strategy(String key, String strategy) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(key, strategy);
    return cfg;
  }

  private static Map<String, Object> withUrl(Map<String, Object> cfg) {
    Map<String, Object> out = new HashMap<>(cfg);
    out.put(SR_URL_CONFIG, SR_URL_VALUE);
    return out;
  }

  private static String subjectOf(SchemaAndValue wrapped) {
    return ((Struct) wrapped.value()).getString(BackupWrapper.FIELD_SCHEMA_SUBJECT);
  }

  private static int schemaIdOf(byte[] bytes) {
    return ByteBuffer.wrap(bytes, 1, 4).getInt();
  }

  private static List<String> sorted(Collection<String> values) {
    List<String> out = new ArrayList<>(values);
    Collections.sort(out);
    return out;
  }

  private static SchemaRegistryClient newRegistry() {
    return new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
  }
}
