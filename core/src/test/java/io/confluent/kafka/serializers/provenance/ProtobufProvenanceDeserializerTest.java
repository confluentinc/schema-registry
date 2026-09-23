/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.serializers.provenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Protobuf read with {@code use.provenance}: wire-compatible type changes read exactly as without
 * provenance; a reused field number, and messages of a multi-message file, follow provenance.
 */
class ProtobufProvenanceDeserializerTest {

  private static final String TOPIC = "proto";
  private static final String SUBJECT = TOPIC + "-value";

  private static final String ORDER = "message Order { int32 id = 1; string item = 2; }";
  private static final String REFUND = "message Refund { int32 id = 1; int32 amount = 2; }";

  private ProvenanceMockSchemaRegistryClient client;
  private KafkaProtobufSerializer<DynamicMessage> serializer;

  @BeforeEach
  void init() {
    client = new ProvenanceMockSchemaRegistryClient();
    serializer = new KafkaProtobufSerializer<>(client, config(null));
  }

  // --- Type changes: identical both ways -------------------------------------------------------

  @Test
  void int32WidensToInt64() throws Exception {
    assertEquals(7L, get(sameBothWays(row("int32 f = 1;"), row("int64 f = 1;"), set("f", 7)), "f"));
  }

  @Test
  void int64NarrowsToInt32AsProtobufTruncates() throws Exception {
    assertEquals(5, get(sameBothWays(row("int64 f = 1;"), row("int32 f = 1;"),
        set("f", (1L << 32) + 5)), "f"));
  }

  @Test
  void uint32WidensToInt64() throws Exception {
    assertEquals(7L, get(sameBothWays(row("uint32 f = 1;"), row("int64 f = 1;"), set("f", 7)), "f"));
  }

  @Test
  void anEnumReadsAsItsNumber() throws Exception {
    ProtobufSchema writer = row("Color f = 1;", "enum Color { RED = 0; GREEN = 1; }");
    assertEquals(1, get(sameBothWays(writer, row("int32 f = 1;"),
        b -> b.setField(field(b, "f"), enumValue(writer, "GREEN"))), "f"));
  }

  @Test
  void aStringReadsAsBytes() throws Exception {
    assertEquals(ByteString.copyFromUtf8("ada"),
        get(sameBothWays(row("string f = 1;"), row("bytes f = 1;"), set("f", "ada")), "f"));
  }

  @Test
  void bytesReadAsAString() throws Exception {
    assertEquals("ada", get(sameBothWays(row("bytes f = 1;"), row("string f = 1;"),
        set("f", ByteString.copyFromUtf8("ada"))), "f"));
  }

  @Test
  void anUnknownEnumValueReadsTheSameBothWays() throws Exception {
    ProtobufSchema writer = row("Color f = 1;", "enum Color { RED = 0; GREEN = 1; BLUE = 2; }");
    sameBothWays(writer, row("Color f = 1;", "enum Color { RED = 0; GREEN = 1; }"),
        b -> b.setField(field(b, "f"), enumValue(writer, "BLUE")));
  }

  @Test
  void aRepeatedElementWidens() throws Exception {
    List<?> values = (List<?>) get(sameBothWays(row("repeated int32 f = 1;"),
        row("repeated int64 f = 1;"), b -> {
          b.addRepeatedField(field(b, "f"), 1);
          b.addRepeatedField(field(b, "f"), 2);
        }), "f");
    assertEquals(2L, values.get(1));
  }

  @Test
  void aOneofBranchWidens() throws Exception {
    assertEquals(7L, get(sameBothWays(row("oneof choice { int32 a = 1; string b = 2; }"),
        row("oneof choice { int64 a = 1; string b = 2; }"), set("a", 7)), "a"));
  }

  @Test
  void aNestedFieldWidens() throws Exception {
    DynamicMessage row = sameBothWays(row("Inner inner = 1;", "message Inner { int32 n = 1; }"),
        row("Inner inner = 1;", "message Inner { int64 n = 1; }"), b -> {
          Descriptor inner = field(b, "inner").getMessageType();
          b.setField(field(b, "inner"), DynamicMessage.newBuilder(inner)
              .setField(inner.findFieldByName("n"), 7).build());
        });
    assertEquals(7L, get((DynamicMessage) get(row, "inner"), "n"));
  }

  // --- History and multi-message ---------------------------------------------------------------

  @Test
  void aReusedNumberDoesNotInheritTheOldFieldsData() throws Exception {
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    ProtobufSchema v2 = row("int32 id = 1;");
    ProtobufSchema v3 = row("int32 id = 1;", "string memo = 2;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "ada"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("", get(read(v3, bytes, "v1"), "memo"));
    assertEquals("ada", get(read(v3, bytes, null), "memo"));
  }

  @Test
  void aMessageIsReadAsTheReadersMessageOfTheSameNameWhenTheFileIsReordered()
      throws Exception {
    ProtobufSchema writer = file(ORDER, REFUND);
    ProtobufSchema reader =
        file(REFUND, "message Order { int32 id = 1; string item = 2; string note = 3; }");
    byte[] bytes = write(writer, refund(writer, 5, 42));
    client.register(SUBJECT, reader);

    DynamicMessage row = read(reader, bytes, "v1");
    assertEquals("p.Refund", row.getDescriptorForType().getFullName());
    assertEquals(42, get(row, "amount"));
  }

  @Test
  void aSingleMessageWriterIsReadUnderAMultiMessageReader() throws Exception {
    ProtobufSchema writer = file(REFUND);
    ProtobufSchema reader = file(ORDER, REFUND);
    byte[] bytes = write(writer, refund(writer, 5, 42));
    client.register(SUBJECT, reader);

    DynamicMessage row = read(reader, bytes, "v1");
    assertEquals("p.Refund", row.getDescriptorForType().getFullName());
    assertEquals(42, get(row, "amount"));
  }

  @Test
  void aMessageTheReaderDoesNotDeclareFailsTheRecord() throws Exception {
    ProtobufSchema writer = file(ORDER, REFUND);
    ProtobufSchema reader = file(REFUND, "message Other { int32 x = 1; }");
    Descriptor order = writer.toDescriptor("p.Order");
    byte[] bytes = write(writer,
        DynamicMessage.newBuilder(order).setField(order.findFieldByName("id"), 7).build());
    client.register(SUBJECT, reader);

    Exception e = assertThrows(Exception.class, () -> read(reader, bytes, "v1"));
    assertTrue(trace(e).contains("p.Order, which the reader schema does not declare"), trace(e));
  }

  @Test
  void aReusedNumberInTheSecondMessageDoesNotInheritTheOldData() throws Exception {
    ProtobufSchema v1 = file(ORDER, "message Refund { int32 id = 1; string note = 2; }");
    ProtobufSchema v2 = file(ORDER, "message Refund { int32 id = 1; }");
    ProtobufSchema v3 = file(ORDER, "message Refund { int32 id = 1; string memo = 2; }");
    Descriptor refund = v1.toDescriptor("p.Refund");
    byte[] bytes = write(v1, DynamicMessage.newBuilder(refund)
        .setField(refund.findFieldByName("id"), 5)
        .setField(refund.findFieldByName("note"), "ada").build());
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("", get(read(v3, bytes, "v1"), "memo"));
    assertEquals("ada", get(read(v3, bytes, null), "memo"));
  }

  // --- Helpers -----------------------------------------------------------------------------------

  private DynamicMessage sameBothWays(ProtobufSchema writer, ProtobufSchema reader,
      Consumer<DynamicMessage.Builder> record) throws Exception {
    byte[] bytes = write(writer, record);
    client.register(SUBJECT, reader);
    DynamicMessage on = read(reader, bytes, "v1");
    DynamicMessage off = read(reader, bytes, null);
    assertEquals(off.toString(), on.toString());
    assertFalse(on.toString().isEmpty() && !off.toString().isEmpty());
    return on;
  }

  private byte[] write(ProtobufSchema writer, Consumer<DynamicMessage.Builder> record)
      throws Exception {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(writer.toDescriptor());
    record.accept(builder);
    return write(writer, builder.build());
  }

  private byte[] write(ProtobufSchema writer, DynamicMessage message) throws Exception {
    client.register(SUBJECT, writer);
    return serializer.serialize(TOPIC, message);
  }

  private DynamicMessage read(ProtobufSchema reader, byte[] bytes, String provenance) {
    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(client, config(provenance));
    return (DynamicMessage) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> reader).getValue();
  }

  private static DynamicMessage refund(ProtobufSchema schema, int id, int amount) {
    Descriptor refund = schema.toDescriptor("p.Refund");
    return DynamicMessage.newBuilder(refund)
        .setField(refund.findFieldByName("id"), id)
        .setField(refund.findFieldByName("amount"), amount).build();
  }

  private static Object get(DynamicMessage message, String name) {
    return message.getField(message.getDescriptorForType().findFieldByName(name));
  }

  private static Consumer<DynamicMessage.Builder> set(String name, Object value) {
    return b -> b.setField(field(b, name), value);
  }

  private static FieldDescriptor field(DynamicMessage.Builder builder, String name) {
    return builder.getDescriptorForType().findFieldByName(name);
  }

  private static Object enumValue(ProtobufSchema schema, String name) {
    return schema.toDescriptor().findFieldByName("f").getEnumType().findValueByName(name);
  }

  private static String trace(Exception e) {
    StringWriter trace = new StringWriter();
    e.printStackTrace(new PrintWriter(trace));
    return trace.toString();
  }

  private static Map<String, Object> config(String provenance) {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", "bogus");
    config.put("auto.register.schemas", false);
    config.put("use.latest.version", false);
    if (provenance != null) {
      config.put("use.provenance", provenance);
    }
    return config;
  }

  // One message, Row, holding the given members.
  private static ProtobufSchema row(String... members) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage Row {\n  "
        + String.join("\n  ", members) + "\n}\n");
  }

  // A file of the given top-level declarations.
  private static ProtobufSchema file(String... members) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n" + String.join("\n", members)
        + "\n");
  }
}
