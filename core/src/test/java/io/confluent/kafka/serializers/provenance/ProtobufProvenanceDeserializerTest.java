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
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.test.ReaddedMapProto.ReaddedMap;
import io.confluent.kafka.serializers.protobuf.test.ReaddedProto.Readded;
import io.confluent.kafka.serializers.protobuf.test.Root.ReferrerMessage;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Protobuf read with {@code provenance.algorithm}: wire-compatible type changes read exactly as without
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
  void aRecordNamingItsSchemaByGuidAloneIsReadByProvenance() throws Exception {
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    ProtobufSchema v2 = row("int32 id = 1;");
    ProtobufSchema v3 = row("int32 id = 1;", "string memo = 2;");
    client.register(SUBJECT, v1);
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(v1.toDescriptor());
    builder.setField(field(builder, "id"), 7).setField(field(builder, "note"), "ada");
    RecordHeaders headers = new RecordHeaders();
    Map<String, Object> byGuid = config(null);
    byGuid.put("value.schema.id.serializer", HeaderSchemaIdSerializer.class.getName());
    byte[] bytes = new KafkaProtobufSerializer<DynamicMessage>(client, byGuid)
        .serialize(TOPIC, headers, builder.build());
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage on = (DynamicMessage) new KafkaProtobufDeserializer<>(client, config("v1"))
        .deserializeWithSchema(TOPIC, headers, bytes, writer -> v3).getValue();
    assertEquals("", get(on, "memo"));
  }

  @Test
  void aRenumberedReadComesBackInTheReadersOwnNumbers() throws Exception {
    // Renumbering is only for parsing. A message handed over in the renumbered descriptor could
    // not be addressed by the reader's own field descriptors, and would forward memo under the
    // throwaway number.
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    ProtobufSchema v2 = row("int32 id = 1;");
    ProtobufSchema v3 = row("int32 id = 1;", "string memo = 2;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "ada"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage read = read(v3, bytes, "v1");
    FieldDescriptor memo = v3.toDescriptor().findFieldByName("memo");
    assertEquals(2, read.getDescriptorForType().findFieldByName("memo").getNumber());
    assertEquals(7, read.getField(v3.toDescriptor().findFieldByName("id")));
    DynamicMessage forwarded = DynamicMessage.parseFrom(v3.toDescriptor(),
        read.toBuilder().setField(memo, "new").build().toByteArray());
    assertEquals("new", forwarded.getField(memo));
    assertTrue(forwarded.getUnknownFields().asMap().isEmpty());
  }

  @Test
  void aReadRuleWritingToAMovedFieldWritesUnderItsOwnNumber() throws Exception {
    // Domain rules run once the read is back in the reader's own numbers: a value they write to a
    // moved field would otherwise sit under the throwaway number, and be lost to unknown fields.
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    ProtobufSchema v2 = row("int32 id = 1;");
    Rule fill = new Rule("fill", null, RuleKind.TRANSFORM, RuleMode.READ, "CEL_FIELD", null, null,
        "name == 'memo' ; 'filled'", null, null, false);
    ProtobufSchema v3 = row("int32 id = 1;", "string memo = 2;")
        .copy(null, new RuleSet(null, Collections.singletonList(fill)));
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "ada"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage read = read(v3, bytes, "v1");
    assertEquals("filled", read.getField(v3.toDescriptor().findFieldByName("memo")));
    assertTrue(read.getUnknownFields().asMap().isEmpty());
  }

  @Test
  void aReusedNumbersOldDataIsNotKeptInUnknownFields() throws Exception {
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    ProtobufSchema v2 = row("int32 id = 1;");
    ProtobufSchema v3 = row("int32 id = 1;", "string memo = 2;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "ada"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    // Nor is it kept aside, to come back if the message is written out again.
    assertTrue(read(v3, bytes, "v1").getUnknownFields().asMap().isEmpty());
  }

  @Test
  void aNewFieldOfAnImportedTypeReusingANumberReadsUnset() throws Exception {
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    ProtobufSchema v2 = row("int32 id = 1;");
    ProtobufSchema v3 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"google/protobuf/duration.proto\";\n"
        + "message Row {\n  int32 id = 1;\n  google.protobuf.Duration d = 2;\n}\n");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "ada"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    // d moves; nothing under it needs a number, so Duration's own fields are left alone.
    DynamicMessage read = read(v3, bytes, "v1");
    assertEquals(7, get(read, "id"));
    assertFalse(read.hasField(read.getDescriptorForType().findFieldByName("d")));
  }

  @Test
  void aSharedMessageUsedByANewFieldKeepsTheOldFieldsData() throws Exception {
    ProtobufSchema v1 = row("In a = 1;", "message In { int32 x = 1; }");
    ProtobufSchema v2 = row("In a = 1;", "In b = 2;", "message In { int32 x = 1; }");
    byte[] bytes = write(v1, b -> {
      Descriptor in = field(b, "a").getMessageType();
      b.setField(field(b, "a"), DynamicMessage.newBuilder(in)
          .setField(in.findFieldByName("x"), 5).build());
    });
    client.register(SUBJECT, v2);

    assertEquals(5, get((DynamicMessage) get(read(v2, bytes, "v1"), "a"), "x"));
  }

  @Test
  void aNewOneofInARepeatedMessageStillRenumbersItsMembers() throws Exception {
    // The oneof is new, so it moves, but it is no field: its member memo reuses note's number.
    ProtobufSchema v1 = row("repeated E es = 1;", "message E { int32 x = 1; string note = 2; }");
    ProtobufSchema v2 = row("repeated E es = 1;", "message E { int32 x = 1; }");
    ProtobufSchema v3 = row("repeated E es = 1;",
        "message E { int32 x = 1; oneof c { string memo = 2; } }");
    byte[] bytes = write(v1, b -> {
      Descriptor e = field(b, "es").getMessageType();
      b.addRepeatedField(field(b, "es"), DynamicMessage.newBuilder(e)
          .setField(e.findFieldByName("x"), 7).setField(e.findFieldByName("note"), "ada").build());
    });
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage element = (DynamicMessage) ((List<?>) get(read(v3, bytes, "v1"), "es")).get(0);
    assertEquals(7, get(element, "x"));
    assertEquals("", get(element, "memo"));
  }

  @Test
  void aRequiredReaderFieldReusingANumberFailsTheRecord() throws Exception {
    ProtobufSchema v1 = proto2("required int32 id = 1;", "optional string note = 2;");
    ProtobufSchema v2 = proto2("required int32 id = 1;");
    ProtobufSchema v3 = proto2("required int32 id = 1;", "required string memo = 2;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "ada"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    // memo moves, so the record has no value for a field the reader requires.
    assertThrows(Exception.class, () -> read(v3, bytes, "v1"));
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

  @Test
  void aNestedMessageWrittenAsTheRecordIsRenumbered() throws Exception {
    // A nested record type has no location of its own under the file's first message; its
    // fields are found through the top-level messages.
    String row = "message Row { message Inner { int32 x = 1; %s } Inner i = 1; }";
    ProtobufSchema v1 = file(String.format(row, "string y = 2;"));
    ProtobufSchema v2 = file(String.format(row, ""));
    ProtobufSchema v3 = file(String.format(row, "string z = 2;"));
    Descriptor inner = v1.toDescriptor("p.Row.Inner");
    byte[] bytes = write(v1, DynamicMessage.newBuilder(inner)
        .setField(inner.findFieldByName("x"), 4)
        .setField(inner.findFieldByName("y"), "old").build());
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("old", get(read(v3, bytes, null), "z"));
    DynamicMessage read = read(v3, bytes, "v1");
    assertEquals(4, get(read, "x"));
    assertEquals("", get(read, "z"));
  }

  @Test
  void aNestedMessageOfTheSecondMessageIsRenumbered() throws Exception {
    String box = "message Box { message Item { int32 x = 1; %s } Item i = 1; }";
    ProtobufSchema v1 = file(ORDER, String.format(box, "string y = 2;"));
    ProtobufSchema v2 = file(ORDER, String.format(box, ""));
    ProtobufSchema v3 = file(ORDER, String.format(box, "string z = 2;"));
    Descriptor item = v1.toDescriptor("p.Box.Item");
    byte[] bytes = write(v1, DynamicMessage.newBuilder(item)
        .setField(item.findFieldByName("x"), 4)
        .setField(item.findFieldByName("y"), "old").build());
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("", get(read(v3, bytes, "v1"), "z"));
  }

  @Test
  void eachMessageOfAFileKeepsItsOwnRenumbering() throws Exception {
    // Records of two messages share the writer's schema id, and one deserializer caches what it
    // made of each; Refund's number 2 is reused, Order's continues.
    ProtobufSchema v1 = file(ORDER, "message Refund { int32 id = 1; string note = 2; }");
    ProtobufSchema v2 = file(ORDER, "message Refund { int32 id = 1; }");
    ProtobufSchema v3 = file(ORDER, "message Refund { int32 id = 1; string memo = 2; }");
    Descriptor order = v1.toDescriptor("p.Order");
    Descriptor refund = v1.toDescriptor("p.Refund");
    byte[] anOrder = write(v1, DynamicMessage.newBuilder(order)
        .setField(order.findFieldByName("id"), 1)
        .setField(order.findFieldByName("item"), "kept").build());
    byte[] aRefund = write(v1, DynamicMessage.newBuilder(refund)
        .setField(refund.findFieldByName("id"), 2)
        .setField(refund.findFieldByName("note"), "old").build());
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(client, config("v1"));
    for (byte[] bytes : Arrays.asList(anOrder, aRefund, anOrder)) {
      DynamicMessage read = (DynamicMessage) deserializer.deserializeWithSchema(
          TOPIC, new RecordHeaders(), bytes, writer -> v3).getValue();
      if (read.getDescriptorForType().getName().equals("Order")) {
        assertEquals("kept", get(read, "item"));
      } else {
        assertEquals("", get(read, "memo"));
      }
    }
  }

  @Test
  void aReadRuleLeavingARequiredFieldUnsetFailsTheRecord() throws Exception {
    // A message no longer parsed from bytes after the rules is checked as the parse would.
    ProtobufSchema v1 = proto2("required int32 id = 1;", "optional string s = 2;");
    Rule clear = new Rule("clear", null, RuleKind.TRANSFORM, RuleMode.READ, ClearId.TYPE, null,
        null, null, null, null, false);
    ProtobufSchema reader = v1.copy(null, new RuleSet(null, Collections.singletonList(clear)));
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "s"), "x"));
    Map<String, Object> config = config(null);
    config.put("rule.executors", "clear");
    config.put("rule.executors.clear.class", ClearId.class.getName());

    assertThrows(SerializationException.class, () ->
        new KafkaProtobufDeserializer<>(client, config)
            .deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, writer -> reader));
  }

  @Test
  void aGeneratedClassReaderGetsNoOldValue() throws Exception {
    // A renumbered read ends in the reader's own numbers, which the class parses as any record;
    // with no reader configured, the class's own schema is the reader.
    String head = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n"
        + "option java_outer_classname = \"ReaddedProto\";\n";
    ProtobufSchema v1 = new ProtobufSchema(head
        + "message Readded {\n  int32 id = 1;\n  string note = 2;\n}\n");
    ProtobufSchema v2 = new ProtobufSchema(head + "message Readded {\n  int32 id = 1;\n}\n");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "old"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, new ProtobufSchema(Readded.getDescriptor()));

    assertEquals("old", readClass(Readded.class, bytes, null, false).getMemo());
    assertEquals("", readClass(Readded.class, bytes, "v1", false).getMemo());
    assertEquals("", readClass(Readded.class, bytes, "v1", true).getMemo());
  }

  @Test
  void aGeneratedClassIsMatchedToTheTextItWasGeneratedFrom() throws Exception {
    // Registered from its .proto text, not the class: the class's own schema spells the map as an
    // entry message and message types fully qualified, so it matches only once normalized.
    String head = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n"
        + "option java_outer_classname = \"ReaddedMapProto\";\n";
    ProtobufSchema v1 = new ProtobufSchema(head
        + "message ReaddedMap {\n  int32 id = 1;\n  string note = 2;\n}\n");
    ProtobufSchema v2 = new ProtobufSchema(head + "message ReaddedMap {\n  int32 id = 1;\n}\n");
    ProtobufSchema v3 = new ProtobufSchema(head + "message ReaddedMap {\n  int32 id = 1;\n"
        + "  string memo = 2;\n  map<string, int32> counts = 3;\n}\n");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "old"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("old", readClass(ReaddedMap.class, bytes, null, false).getMemo());
    assertEquals("", readClass(ReaddedMap.class, bytes, "v1", false).getMemo());
  }

  @Test
  void aWriterRuleSeesARenumberedClassReadInTheWritersNumbers() throws Exception {
    // With no reader configured the rules are the writer's; they must not reach the class's new
    // field through the number its own field used to have.
    String head = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n"
        + "option java_outer_classname = \"ReaddedProto\";\n";
    Rule mark = new Rule("mark", null, RuleKind.TRANSFORM, RuleMode.READ, "CEL_FIELD", null, null,
        "name == 'note' ; value + '!'", null, null, false);
    ProtobufSchema v1 = new ProtobufSchema(head
        + "message Readded {\n  int32 id = 1;\n  string note = 2;\n}\n")
        .copy(null, new RuleSet(null, Collections.singletonList(mark)));
    ProtobufSchema v2 = new ProtobufSchema(head + "message Readded {\n  int32 id = 1;\n}\n");
    // Framed by hand: the serializer looks up the message's own schema, which has no rules.
    DynamicMessage.Builder record = DynamicMessage.newBuilder(v1.toDescriptor());
    record.setField(field(record, "id"), 7).setField(field(record, "note"), "old");
    byte[] body = record.build().toByteArray();
    byte[] bytes = ByteBuffer.allocate(6 + body.length).put((byte) 0)
        .putInt(client.register(SUBJECT, v1)).put((byte) 0).put(body).array();
    client.register(SUBJECT, v2);
    client.register(SUBJECT, new ProtobufSchema(Readded.getDescriptor()));

    assertEquals("old!", readClass(Readded.class, bytes, null, false).getMemo());
    assertEquals("", readClass(Readded.class, bytes, "v1", false).getMemo());
  }

  @Test
  void aWriterConditionSeesItsOwnFieldUnderAClassReader() throws Exception {
    // With no reader configured the rules are the writer's, over the writer's own record; what the
    // class does not pair with it is dropped only after them.
    String head = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n"
        + "option java_outer_classname = \"ReaddedProto\";\n";
    Rule check = new Rule("check", null, RuleKind.CONDITION, RuleMode.READ, "CEL", null, null,
        "message.note == 'old'", null, null, false);
    ProtobufSchema v1 = new ProtobufSchema(head
        + "message Readded {\n  int32 id = 1;\n  string note = 2;\n}\n")
        .copy(null, new RuleSet(null, Collections.singletonList(check)));
    ProtobufSchema v2 = new ProtobufSchema(head + "message Readded {\n  int32 id = 1;\n}\n");
    // Framed by hand: the serializer looks up the message's own schema, which has no rules.
    DynamicMessage.Builder record = DynamicMessage.newBuilder(v1.toDescriptor());
    record.setField(field(record, "id"), 7).setField(field(record, "note"), "old");
    byte[] body = record.build().toByteArray();
    byte[] bytes = ByteBuffer.allocate(6 + body.length).put((byte) 0)
        .putInt(client.register(SUBJECT, v1)).put((byte) 0).put(body).array();
    client.register(SUBJECT, v2);
    client.register(SUBJECT, new ProtobufSchema(Readded.getDescriptor()));

    assertEquals("old", readClass(Readded.class, bytes, null, false).getMemo());
    assertEquals("", readClass(Readded.class, bytes, "v1", false).getMemo());
  }

  @Test
  void aReaderIsMatchedOnlyToAVersionWithItsReferences() throws Exception {
    // One text importing three versions of a dependency; a reader matched by structure (rules
    // merged on, so not by id) is the version importing what it imports.
    String dep = "syntax = \"proto3\";\npackage d;\nmessage D {\n  int32 x = 1;\n%s}\n";
    String[] deps = {String.format(dep, "  string y = 2;\n"), String.format(dep, ""),
        String.format(dep, "  string z = 2;\n")};
    String main = "syntax = \"proto3\";\npackage p;\nimport \"d.proto\";\n"
        + "message Row {\n  int32 id = 1;\n  d.D d = 2;\n}\n";
    List<ProtobufSchema> versions = new ArrayList<>();
    for (int i = 0; i < deps.length; i++) {
      client.register("dep", new ProtobufSchema(deps[i]));
      ProtobufSchema version = new ProtobufSchema(main,
          Collections.singletonList(new SchemaReference("d.proto", "dep", i + 1)),
          Collections.singletonMap("d.proto", deps[i]), null, null);
      client.register(SUBJECT, version);
      versions.add(version);
    }
    Descriptor row = versions.get(0).toDescriptor();
    Descriptor d = row.findFieldByName("d").getMessageType();
    byte[] body = DynamicMessage.newBuilder(row).setField(row.findFieldByName("id"), 7)
        .setField(row.findFieldByName("d"), DynamicMessage.newBuilder(d)
            .setField(d.findFieldByName("x"), 1).setField(d.findFieldByName("y"), "old").build())
        .build().toByteArray();
    byte[] bytes = ByteBuffer.allocate(6 + body.length).put((byte) 0)
        .putInt(client.getId(SUBJECT, versions.get(0))).put((byte) 0).put(body).array();
    Rule check = new Rule("check", null, RuleKind.CONDITION, RuleMode.READ, "CEL", null, null,
        "message.id == 7", null, null, false);
    ProtobufSchema reader = versions.get(0)
        .copy(null, new RuleSet(null, Collections.singletonList(check)));

    DynamicMessage read = read(reader, bytes, "v1");
    assertEquals("old",
        ((DynamicMessage) get(read, "d")).getField(d.findFieldByName("y")));
  }

  @Test
  void aClassReaderIsTheLatestVersionItEquals() throws Exception {
    // v1 is spelled as the class's own descriptor (the map an entry message), so equals the class
    // exactly; v3 re-adds memo as text. The class is the latest version it equals, normalized.
    String head = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n"
        + "option java_outer_classname = \"ReaddedMapProto\";\n";
    ProtobufSchema v1 = new ProtobufSchema(ReaddedMap.getDescriptor());
    ProtobufSchema v2 = new ProtobufSchema(head + "message ReaddedMap {\n  int32 id = 1;\n"
        + "  map<string, int32> counts = 3;\n}\n");
    ProtobufSchema v3 = new ProtobufSchema(head + "message ReaddedMap {\n  int32 id = 1;\n"
        + "  string memo = 2;\n  map<string, int32> counts = 3;\n}\n");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "memo"), "old"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("old", readClass(ReaddedMap.class, bytes, null, false).getMemo());
    assertEquals("", readClass(ReaddedMap.class, bytes, "v1", false).getMemo());
  }

  @Test
  void aClassReaderIsNotTheTextReaderItEquals() throws Exception {
    // One deserializer reads first with a text reader spelled as the class (v1), then with the
    // class: the class is still the latest version it equals, whatever the text reader was.
    ProvenanceMockSchemaRegistryClient client = new ProvenanceMockSchemaRegistryClient();
    String head = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n"
        + "option java_outer_classname = \"ReaddedMapProto\";\n";
    ProtobufSchema v1 = new ProtobufSchema(ReaddedMap.getDescriptor());
    int id1 = client.register(SUBJECT, v1);
    client.register(SUBJECT, new ProtobufSchema(head + "message ReaddedMap {\n  int32 id = 1;\n"
        + "  map<string, int32> counts = 3;\n}\n"));
    client.register(SUBJECT, new ProtobufSchema(head + "message ReaddedMap {\n  int32 id = 1;\n"
        + "  string memo = 2;\n  map<string, int32> counts = 3;\n}\n"));
    DynamicMessage record = DynamicMessage.newBuilder(v1.toDescriptor())
        .setField(v1.toDescriptor().findFieldByName("id"), 7)
        .setField(v1.toDescriptor().findFieldByName("memo"), "old").build();
    byte[] body = record.toByteArray();
    byte[] bytes = ByteBuffer.allocate(6 + body.length).put((byte) 0).putInt(id1).put((byte) 0)
        .put(body).array();
    Map<String, Object> config = config("v1");
    config.put("specific.protobuf.value.type", ReaddedMap.class);
    KafkaProtobufDeserializer<ReaddedMap> deserializer = new KafkaProtobufDeserializer<>(client);
    deserializer.configure(config, false);
    ProtobufSchema text = new ProtobufSchema(ReaddedMap.getDescriptor());

    assertEquals("old", ((ReaddedMap) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> text).getValue()).getMemo());
    assertEquals("", ((ReaddedMap) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> null).getValue()).getMemo());
  }

  @Test
  void aClassImportingAnotherFileIsMatchedToTheVersionReferencingIt() throws Exception {
    // root.proto imports ref.proto and a built-in; the class's schema has no references, so it is
    // matched by what its descriptor imports. root_id is dropped and re-added: a new column.
    String ref = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n"
        + "message ReferencedMessage {\n  string ref_id = 1;\n  bool is_active = 2;\n}\n";
    client.register("ref", new ProtobufSchema(ref));
    List<Integer> ids = new ArrayList<>();
    for (String fields : new String[] {"string root_id = 1;\n  int32 extra = 3;\n", "",
        "string root_id = 1;\n"}) {
      ids.add(client.register(SUBJECT, new ProtobufSchema("syntax = \"proto3\";\n"
          + "package io.confluent.kafka.serializers.protobuf.test;\n"
          + "import \"ref.proto\";\nimport \"confluent/meta.proto\";\n"
          + "message ReferrerMessage {\n"
          + "  option (.confluent.message_meta).doc = \"ReferrerMessage\";\n  " + fields
          + "  ReferencedMessage ref = 2\n"
          + "      [(.confluent.field_meta) = { doc: \"ReferencedMessage\" }];\n"
          + "}\n", Collections.singletonList(new SchemaReference("ref.proto", "ref", 1)),
          Collections.singletonMap("ref.proto", ref), null, null)));
    }
    Descriptor v1 = ((ProtobufSchema) client.getSchemaById(ids.get(0))).toDescriptor();
    byte[] body = DynamicMessage.newBuilder(v1).setField(v1.findFieldByName("root_id"), "old")
        .build().toByteArray();
    byte[] bytes = ByteBuffer.allocate(6 + body.length).put((byte) 0).putInt(ids.get(0))
        .put((byte) 0).put(body).array();

    assertEquals("old", readClass(ReferrerMessage.class, bytes, null, false).getRootId());
    assertEquals("", readClass(ReferrerMessage.class, bytes, "v1", false).getRootId());
  }

  @Test
  void concurrentFirstReadsShareOneProjector() throws Exception {
    // The projector holds which readers a class derived and which ids were supplied; a second
    // one built by a race would forget them.
    Method projector =
        AbstractKafkaProtobufDeserializer.class.getDeclaredMethod("provenanceProjector");
    projector.setAccessible(true);
    ExecutorService threads = Executors.newFixedThreadPool(16);
    try {
      for (int round = 0; round < 200; round++) {
        KafkaProtobufDeserializer<DynamicMessage> deserializer =
            new KafkaProtobufDeserializer<>(client, config("v1"));
        CyclicBarrier start = new CyclicBarrier(16);
        List<Future<Object>> built = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
          built.add(threads.submit(() -> {
            start.await();
            return projector.invoke(deserializer);
          }));
        }
        Set<Object> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Future<Object> future : built) {
          distinct.add(future.get());
        }
        assertEquals(1, distinct.size());
      }
    } finally {
      threads.shutdownNow();
    }
  }

  @Test
  void aFreshNumberIsNoneTheWriterWritesUnder() throws Exception {
    // c moves off b's number to a fresh one; the writer writes z there, which must not be parsed
    // into c, a message it does not parse as.
    String n = "message N { int32 x = 1; }";
    ProtobufSchema v1 = file("message Row { int32 a = 1; int64 b = 2; string z = 536870911; }", n);
    ProtobufSchema v2 = file("message Row { int32 a = 1; }", n);
    ProtobufSchema v3 = file("message Row { int32 a = 1; N c = 2; }", n);
    byte[] bytes = write(v1, b -> b.setField(field(b, "a"), 7).setField(field(b, "b"), 5L)
        .setField(field(b, "z"), "zzzz"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage read = read(v3, bytes, "v1");
    assertEquals(7, get(read, "a"));
    assertFalse(read.hasField(read.getDescriptorForType().findFieldByName("c")));
  }

  @Test
  void aWriterReservingEveryFreeNumberStillLeavesOneToMoveTo() throws Exception {
    // The writer reserves all above 2, so writes nothing there: c moves into that range rather
    // than the pair falling back and c reading b's value.
    ProtobufSchema v1 = row("int32 a = 1;", "string b = 2;", "reserved 3 to max;");
    ProtobufSchema v2 = row("int32 a = 1;");
    ProtobufSchema v3 = row("int32 a = 1;", "string c = 2;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "a"), 7).setField(field(b, "b"), "old"));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("old", get(read(v3, bytes, null), "c"));
    assertEquals("", get(read(v3, bytes, "v1"), "c"));
  }

  @Test
  void aFreshNumberAvoidsTheWriterUnderAMessageRenamedSinceIt() throws Exception {
    // A was renamed B, so B's members are new and move; the writer's A still writes z at the top
    // number, which must not be parsed into x, a message it does not parse as.
    String n = "message N { int32 q = 1; }";
    ProtobufSchema v1 = file("message Row { message A { string z = 536870911; } A n = 1; }", n);
    ProtobufSchema v2 = file(
        "message Row { message A { string z = 536870911; } A n = 1; int32 pad = 2; }", n);
    ProtobufSchema v3 = file("message Row { message B { N x = 1; } B n = 1; int32 pad = 2; }", n);
    byte[] bytes = write(v1, b -> {
      FieldDescriptor nf = field(b, "n");
      DynamicMessage.Builder a = DynamicMessage.newBuilder(nf.getMessageType());
      a.setField(nf.getMessageType().findFieldByName("z"), "zzzz");
      b.setField(nf, a.build());
    });
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage inner = (DynamicMessage) get(read(v3, bytes, "v1"), "n");
    assertFalse(inner.hasField(inner.getDescriptorForType().findFieldByName("x")));
  }

  @Test
  void aFreshNumberAvoidsTheWritersExtensionsUnderAMessageRenamedSinceIt() throws Exception {
    // A was renamed B, so x moves; the writer's A keeps extension data at the top number, which
    // must not be parsed into x, a message it does not parse as.
    String head = "syntax = \"proto2\";\npackage p;\n";
    String n = "message N { optional int32 q = 1; }\n";
    ProtobufSchema v1 = new ProtobufSchema(head + "message Row { message A { optional int32 q = 1;"
        + " optional string old = 3; extensions 100 to max; } optional A n = 1; }\n" + n);
    ProtobufSchema v2 = new ProtobufSchema(head + "message Row { message A { optional int32 q = 1;"
        + " extensions 100 to max; } optional A n = 1; optional int32 pad = 2; }\n" + n);
    ProtobufSchema v3 = new ProtobufSchema(head + "message Row { message B { optional int32 q = 1;"
        + " optional N x = 3; } optional B n = 1; optional int32 pad = 2; }\n" + n);
    byte[] bytes = write(v1, b -> {
      FieldDescriptor inner = field(b, "n");
      DynamicMessage.Builder a = DynamicMessage.newBuilder(inner.getMessageType());
      a.setField(inner.getMessageType().findFieldByName("q"), 7);
      a.setUnknownFields(UnknownFieldSet.newBuilder().addField(536870911, UnknownFieldSet.Field
          .newBuilder().addLengthDelimited(ByteString.copyFromUtf8("zzzz")).build()).build());
      b.setField(inner, a.build());
    });
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage inner = (DynamicMessage) get(read(v3, bytes, "v1"), "n");
    assertEquals(7, inner.getField(inner.getDescriptorForType().findFieldByName("q")));
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

  private <M extends Message> M readClass(Class<M> type, byte[] bytes, String provenance,
      boolean latest) {
    Map<String, Object> config = config(provenance);
    config.put("specific.protobuf.value.type", type);
    config.put("use.latest.version", latest);
    KafkaProtobufDeserializer<M> deserializer = new KafkaProtobufDeserializer<>(client);
    deserializer.configure(config, false);
    return deserializer.deserialize(TOPIC, bytes);
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
      config.put("provenance.algorithm", provenance);
    }
    return config;
  }

  // One message, Row, holding the given members.
  private static ProtobufSchema row(String... members) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage Row {\n  "
        + String.join("\n  ", members) + "\n}\n");
  }

  private static ProtobufSchema proto2(String... members) {
    return new ProtobufSchema("syntax = \"proto2\";\npackage p;\nmessage Row {\n  "
        + String.join("\n  ", members) + "\n}\n");
  }

  // A file of the given top-level declarations.
  private static ProtobufSchema file(String... members) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n" + String.join("\n", members)
        + "\n");
  }

  /** A READ transform that clears the required {@code id}. */
  public static class ClearId implements RuleExecutor {
    static final String TYPE = "CLEAR_ID";

    @Override
    public String type() {
      return TYPE;
    }

    @Override
    public Object transform(RuleContext ctx, Object message) {
      DynamicMessage m = (DynamicMessage) message;
      return m.toBuilder().clearField(m.getDescriptorForType().findFieldByName("id"))
          .buildPartial();
    }
  }
}
