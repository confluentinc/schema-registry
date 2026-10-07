/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.serializers.provenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
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
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.protobuf.test.ReaddedMapProto.ReaddedMap;
import io.confluent.kafka.serializers.protobuf.test.ReaddedProto.Readded;
import io.confluent.kafka.serializers.protobuf.test.Root.ReferrerMessage;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
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
  void aReusedNumberInAnImportedMessageReadsUnset() throws Exception {
    // active reuses flag's number in the imported N: it moves in a copy of d.proto, which Row is
    // built against, while the well-known type's file passes through untouched.
    String dep = "syntax = \"proto3\";\npackage d;\nmessage N {\n  int32 x = 1;\n%s}\n";
    List<ProtobufSchema> versions = importing("syntax = \"proto3\";\npackage p;\n"
        + "import \"d.proto\";\nimport \"google/protobuf/timestamp.proto\";\n"
        + "message Row {\n  int32 id = 1;\n  d.N n = 2;\n  google.protobuf.Timestamp t = 3;\n}\n",
        String.format(dep, "  bool flag = 2;\n"), String.format(dep, ""),
        String.format(dep, "  bool active = 2;\n"));
    byte[] bytes = writeById(versions.get(0), b -> {
      Descriptor n = field(b, "n").getMessageType();
      Descriptor t = field(b, "t").getMessageType();
      b.setField(field(b, "id"), 7)
          .setField(field(b, "n"), DynamicMessage.newBuilder(n).setField(n.findFieldByName("x"), 1)
              .setField(n.findFieldByName("flag"), true).build())
          .setField(field(b, "t"), DynamicMessage.newBuilder(t)
              .setField(t.findFieldByName("seconds"), 42L).build());
    });

    DynamicMessage read = read(versions.get(2), bytes, "v1");
    DynamicMessage n = (DynamicMessage) get(read, "n");
    assertEquals(1, get(n, "x"));
    assertFalse(n.hasField(n.getDescriptorForType().findFieldByName("active")));
    assertEquals(42L, get((DynamicMessage) get(read, "t"), "seconds"));
    DynamicMessage nativeRead = read(versions.get(2), bytes, null);
    assertEquals(true, get((DynamicMessage) get(nativeRead, "n"), "active"));
  }

  @Test
  void aClassWhoseImportedMessageReusesANumberReadsItUnset() throws Exception {
    // The generated ReferrerMessage reads ReferencedMessage.is_active, which reuses the number
    // of a dropped field of the imported file: unset, as the class's own fields would be.
    String ref = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n"
        + "message ReferencedMessage {\n  string ref_id = 1;\n%s}\n";
    List<ProtobufSchema> versions = new ArrayList<>();
    String[] refs = {String.format(ref, "  bool was_active = 2;\n"), String.format(ref, ""),
        String.format(ref, "  bool is_active = 2;\n")};
    for (int i = 0; i < refs.length; i++) {
      client.register("ref", new ProtobufSchema(refs[i]));
      ProtobufSchema version = new ProtobufSchema("syntax = \"proto3\";\n"
          + "package io.confluent.kafka.serializers.protobuf.test;\n"
          + "import \"ref.proto\";\nimport \"confluent/meta.proto\";\n"
          + "message ReferrerMessage {\n"
          + "  option (.confluent.message_meta).doc = \"ReferrerMessage\";\n"
          + "  string root_id = 1;\n"
          + "  ReferencedMessage ref = 2\n"
          + "      [(.confluent.field_meta) = { doc: \"ReferencedMessage\" }];\n"
          + "}\n", Collections.singletonList(new SchemaReference("ref.proto", "ref", i + 1)),
          Collections.singletonMap("ref.proto", refs[i]), null, null);
      client.register(SUBJECT, version);
      versions.add(version);
    }
    byte[] bytes = writeById(versions.get(0), b -> {
      Descriptor r = field(b, "ref").getMessageType();
      b.setField(field(b, "root_id"), "r").setField(field(b, "ref"), DynamicMessage.newBuilder(r)
          .setField(r.findFieldByName("ref_id"), "a")
          .setField(r.findFieldByName("was_active"), true).build());
    });

    assertTrue(readClass(ReferrerMessage.class, bytes, null, false).getRef().getIsActive());
    ReferrerMessage read = readClass(ReferrerMessage.class, bytes, "v1", false);
    assertEquals("a", read.getRef().getRefId());
    assertFalse(read.getRef().getIsActive());
  }

  @Test
  void aReusedNumberInAMessageImportedPubliclyReadsUnset() throws Exception {
    // Row imports wrap.proto, which re-exports leaf.proto's N: the copies are made through it.
    String leaf = "syntax = \"proto3\";\npackage d;\nmessage N {\n  int32 x = 1;\n%s}\n";
    String wrap = "syntax = \"proto3\";\npackage w;\nimport public \"leaf.proto\";\n";
    String[] leaves = {String.format(leaf, "  bool flag = 2;\n"), String.format(leaf, ""),
        String.format(leaf, "  bool active = 2;\n")};
    List<ProtobufSchema> versions = new ArrayList<>();
    for (int i = 0; i < leaves.length; i++) {
      client.register("leaf", new ProtobufSchema(leaves[i]));
      SchemaReference toLeaf = new SchemaReference("leaf.proto", "leaf", i + 1);
      client.register("wrap", new ProtobufSchema(wrap, Collections.singletonList(toLeaf),
          Collections.singletonMap("leaf.proto", leaves[i]), null, null));
      Map<String, String> resolved = new LinkedHashMap<>();
      resolved.put("wrap.proto", wrap);
      resolved.put("leaf.proto", leaves[i]);
      ProtobufSchema version = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
          + "import \"wrap.proto\";\nmessage Row {\n  int32 id = 1;\n  d.N n = 2;\n}\n",
          Arrays.asList(new SchemaReference("wrap.proto", "wrap", i + 1), toLeaf), resolved,
          null, null);
      client.register(SUBJECT, version);
      versions.add(version);
    }
    byte[] bytes = writeById(versions.get(0), b -> {
      Descriptor n = field(b, "n").getMessageType();
      b.setField(field(b, "n"), DynamicMessage.newBuilder(n)
          .setField(n.findFieldByName("x"), 1).setField(n.findFieldByName("flag"), true).build());
    });

    DynamicMessage n = (DynamicMessage) get(read(versions.get(2), bytes, "v1"), "n");
    assertEquals(1, get(n, "x"));
    assertFalse(n.hasField(n.getDescriptorForType().findFieldByName("active")));
  }

  @Test
  void aMessageMovedToAnotherPackageReadsItsRestartedMembersUnset() throws Exception {
    // A package move is a new type, its members new: they are now pruned in the imported file
    // rather than the pair falling back and handing them the old values.
    String a = "syntax = \"proto3\";\npackage a;\nmessage N {\n  int32 x = 1;\n}\n";
    String b = "syntax = \"proto3\";\npackage b;\nmessage N {\n  int32 x = 1;\n}\n";
    client.register("na", new ProtobufSchema(a));
    client.register("nb", new ProtobufSchema(b));
    ProtobufSchema v1 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"n.proto\";\nmessage Row {\n  int32 id = 1;\n  a.N n = 2;\n}\n",
        Collections.singletonList(new SchemaReference("n.proto", "na", 1)),
        Collections.singletonMap("n.proto", a), null, null);
    ProtobufSchema v2 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"n2.proto\";\nmessage Row {\n  int32 id = 1;\n  b.N n = 2;\n}\n",
        Collections.singletonList(new SchemaReference("n2.proto", "nb", 1)),
        Collections.singletonMap("n2.proto", b), null, null);
    client.register(SUBJECT, v1);
    byte[] bytes = writeById(v1, row -> {
      Descriptor n = field(row, "n").getMessageType();
      row.setField(field(row, "id"), 7).setField(field(row, "n"),
          DynamicMessage.newBuilder(n).setField(n.findFieldByName("x"), 11).build());
    });
    client.register(SUBJECT, v2);

    assertEquals(11, get((DynamicMessage) get(read(v2, bytes, null), "n"), "x"));
    DynamicMessage read = read(v2, bytes, "v1");
    assertEquals(7, get(read, "id"));
    assertEquals(0, get((DynamicMessage) get(read, "n"), "x"));
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
  void aReusedNumberWhoseElementsChangeKindIsNew() throws Exception {
    // repeated M becoming repeated int32 under one number: the messages' bytes are no ints.
    ProtobufSchema v1 = file("message Row {\n  repeated M x = 1;\n}",
        "message M {\n  int32 a = 1;\n}");
    ProtobufSchema v2 = file("message Row {\n  repeated int32 x = 1;\n}",
        "message M {\n  int32 a = 1;\n}");
    Descriptor row = v1.toDescriptor("p.Row");
    Descriptor m = v1.toDescriptor("p.M");
    byte[] body = DynamicMessage.newBuilder(row).addRepeatedField(row.findFieldByName("x"),
        DynamicMessage.newBuilder(m).setField(m.findFieldByName("a"), 300).build()).build()
        .toByteArray();
    byte[] bytes = ByteBuffer.allocate(6 + body.length).put((byte) 0)
        .putInt(client.register(SUBJECT, v1)).put((byte) 0).put(body).array();
    client.register(SUBJECT, v2);

    assertEquals(Collections.emptyList(), get(read(v2, bytes, "v1"), "x"));
  }

  @Test
  void aNewOneofInAMapValueKeepsTheValuesOtherFields() throws Exception {
    // The oneof's names end at the entry's value field, which is no location: it must not move
    // the value, and x, which continues, keeps its value.
    ProtobufSchema v1 = row("map<string, E> es = 1;",
        "message E { int32 x = 1; string note = 2; }");
    ProtobufSchema v2 = row("map<string, E> es = 1;", "message E { int32 x = 1; }");
    ProtobufSchema v3 = row("map<string, E> es = 1;",
        "message E { int32 x = 1; oneof c { string memo = 2; } }");
    Descriptor row = v1.toDescriptor();
    Descriptor entry = row.findFieldByName("es").getMessageType();
    Descriptor e = entry.findFieldByName("value").getMessageType();
    byte[] body = DynamicMessage.newBuilder(row).addRepeatedField(row.findFieldByName("es"),
        DynamicMessage.newBuilder(entry).setField(entry.findFieldByName("key"), "k")
            .setField(entry.findFieldByName("value"), DynamicMessage.newBuilder(e)
                .setField(e.findFieldByName("x"), 7).setField(e.findFieldByName("note"), "ada")
                .build())
            .build())
        .build().toByteArray();
    // Framed by hand: the serializer's own schema spells the map as an entry message.
    byte[] bytes = ByteBuffer.allocate(6 + body.length).put((byte) 0)
        .putInt(client.register(SUBJECT, v1)).put((byte) 0).put(body).array();
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage value = (DynamicMessage) get(
        (DynamicMessage) ((List<?>) get(read(v3, bytes, "v1"), "es")).get(0), "value");
    assertEquals(7, get(value, "x"));
    assertEquals("", get(value, "memo"));
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
  void aMessageTheOneMessageReaderDoesNotDeclareFailsTheRecord() throws Exception {
    ProtobufSchema writer = file("message Order { int32 id = 1; int32 total = 2; }", REFUND);
    ProtobufSchema reader = file("message Order { int32 id = 1; int32 total = 2; }");
    byte[] bytes = write(writer, refund(writer, 7, 42));
    client.register(SUBJECT, reader);

    // Without provenance the refund's amount reads as the order's total.
    assertEquals(42, get(read(reader, bytes, null), "total"));
    Exception e = assertThrows(Exception.class, () -> read(reader, bytes, "v1"));
    assertTrue(trace(e).contains("p.Refund, which the reader schema does not declare"), trace(e));
  }

  @Test
  void aMessageReaddedAfterAGapDoesNotInheritTheOldData() throws Exception {
    // M is dropped in v2 and re-added in v3 with a new field at the old number.
    String host = "message Host { int32 h = 1; }";
    ProtobufSchema v1 = file("message M { int32 old = 1; }", host);
    Descriptor m = v1.toDescriptor("p.M");
    byte[] bytes = write(v1,
        DynamicMessage.newBuilder(m).setField(m.findFieldByName("old"), 5).build());
    client.register(SUBJECT, file(host));
    ProtobufSchema v3 = file(host, "message M { int32 neu = 1; }");
    client.register(SUBJECT, v3);

    assertEquals(5, get(read(v3, bytes, null), "neu"));
    assertEquals(0, get(read(v3, bytes, "v1"), "neu"));
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
  void aNestedMessageWrittenAsTheRecordWithASingleMessageReaderIsReadAsWritten() throws Exception {
    // The reader's file has one top-level message, so provenance is single-message, rooted at
    // it: a nested record type has no location there, and reads as without provenance.
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
    assertEquals("old", get(read, "z"));
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
  void aReaderWithOptionsNoVersionHasIsMatchedByStructure() throws Exception {
    // Message and field options only document or steer code generation: the reader is still v3.
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "old"));
    client.register(SUBJECT, row("int32 id = 1;"));
    client.register(SUBJECT, row("int32 id = 1;", "string memo = 2;"));
    ProtobufSchema reader = row("option deprecated = true;", "int32 id = 1;",
        "string memo = 2 [deprecated = true, json_name = \"MEMO\"];");

    assertEquals("", get(read(reader, bytes, "v1"), "memo"));
  }

  @Test
  void aReaderWithAServiceOrItsOwnOptionsNoVersionHasIsMatchedByStructure() throws Exception {
    // A service holds no data, and an option extension only declares an option: still v3.
    String head = "syntax = \"proto3\";\npackage p;\n";
    String row = "message Row {\n  int32 id = 1;\n  string memo = 2%s;\n}\n";
    for (String reader : new String[] {
        head + String.format(row, "") + "service S {\n  rpc Get(Row) returns (Row);\n}\n",
        head + "import \"google/protobuf/descriptor.proto\";\n"
            + "extend google.protobuf.FieldOptions {\n  string label = 50001;\n}\n"
            + String.format(row, " [(label) = \"x\"]")}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaProtobufSerializer<>(client, config(null));
      assertEquals("", get(read(new ProtobufSchema(reader), readdedMemo(), "v1"), "memo"),
          reader);
    }
  }

  @Test
  void aSubjectHoldingAnotherFormatStillFindsItsProtobufVersion() throws Exception {
    // The latest version is Avro: compared first, and simply not the reader.
    byte[] bytes = readdedMemo();
    client.register(SUBJECT, new AvroSchema("{\"type\":\"record\",\"name\":\"Row\","
        + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"));
    ProtobufSchema reader = row("int32 id = 1;", "string memo = 2 [deprecated = true];");
    assertEquals("", get(read(reader, bytes, "v1"), "memo"));
  }

  @Test
  void aReaderDeclaringMembersOutOfNumberOrderIsMatchedByStructure() throws Exception {
    // Members are paired by number, not declaration order: the reader is still v3.
    assertEquals("", get(read(row("string memo = 2;", "int32 id = 1;"), readdedMemo(), "v1"),
        "memo"));

    client = new ProvenanceMockSchemaRegistryClient();
    serializer = new KafkaProtobufSerializer<>(client, config(null));
    String oneof = "oneof k {\n    %s\n  }";
    byte[] bytes = write(
        row("int32 id = 1;", String.format(oneof, "string note = 2; int32 n = 3;")),
        b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "old"));
    client.register(SUBJECT, row("int32 id = 1;", String.format(oneof, "int32 n = 3;")));
    client.register(SUBJECT,
        row("int32 id = 1;", String.format(oneof, "string memo = 2; int32 n = 3;")));
    DynamicMessage read = read(
        row("int32 id = 1;", String.format(oneof, "int32 n = 3; string memo = 2;")), bytes, "v1");
    assertEquals("", get(read, "memo"));
    assertEquals(7, get(read, "id"));
  }

  @Test
  void anOlderReaderDifferingOnlyInAWrappedUnionsNumbersIsNotTakenForTheLatest() throws Exception {
    // v2 renumbers the wrapper's branches and v3 adds an option. The reader, v1 spelled
    // otherwise, is v1, not v3: its b is number 1, v2's a, so it reads v2's a.
    client.register(SUBJECT, wrapped("", 2, 1));
    byte[] bytes = write(wrapped("", 1, 2), b -> {
      FieldDescriptor us = field(b, "us");
      b.setField(field(b, "id"), 7).addRepeatedField(us,
          DynamicMessage.newBuilder(us.getMessageType())
              .setField(us.getMessageType().findFieldByName("a"), "old").build());
    });
    String option = "option deprecated = true;\n  ";
    client.register(SUBJECT, wrapped(option, 1, 2));

    DynamicMessage read = read(wrapped(option, 2, 1), bytes, "v1");
    DynamicMessage element = (DynamicMessage) ((List<?>) get(read, "us")).get(0);
    assertEquals(7, get(read, "id"));
    assertEquals("old", get(element, "b"));
  }

  // Row holding an array of unions, its Flink wrapper's branches a and b numbered as given.
  private static ProtobufSchema wrapped(String option, int a, int b) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"confluent/meta.proto\";\nmessage Row {\n  " + option + "int32 id = 1;\n"
        + "  repeated UW us = 2 [(confluent.field_meta) = {params: [{key: \"flink.wrapped\", "
        + "value: \"true\"}]}];\n  message UW {\n    oneof value {\n      string a = " + a + ";\n"
        + "      string b = " + b + ";\n    }\n  }\n}\n");
  }

  private static final String OPTIONS = "syntax = \"proto3\";\npackage o;\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "extend google.protobuf.FieldOptions {\n  string label = 50001;\n}\n";

  @Test
  void aReaderImportingAFileOfOnlyOptionsIsMatchedByStructure() throws Exception {
    // The option file declares no type; the reader is still v3, which imports nothing.
    byte[] bytes = readdedMemo();
    client.register("opts", new ProtobufSchema(OPTIONS));
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"opts.proto\";\nmessage Row {\n  int32 id = 1;\n"
        + "  string memo = 2 [(o.label) = \"x\"];\n}\n",
        Collections.singletonList(new SchemaReference("opts.proto", "opts", 1)),
        Collections.singletonMap("opts.proto", OPTIONS), null, null);
    assertEquals("", get(read(reader, bytes, "v1"), "memo"));
  }

  @Test
  void versionsImportingAFileOfOnlyOptionsOrOnlyPublicImportsHaveProvenance() throws Exception {
    // Every version imports it: an option file, or wrap.proto, empty but for a public import of
    // leaf.proto, directly or through wrap1.proto, empty but for its own.
    String leaf = "syntax = \"proto3\";\npackage com;\nmessage Foo {\n  string id = 1;\n}\n";
    String wrap1 = "syntax = \"proto3\";\npackage com;\nimport public \"leaf.proto\";\n";
    String wrap2 = "syntax = \"proto3\";\npackage com;\nimport public \"wrap1.proto\";\n";
    for (int levels = 0; levels <= 2; levels++) {
      boolean reExport = levels > 0;
      client = new ProvenanceMockSchemaRegistryClient();
      List<SchemaReference> references = new ArrayList<>();
      Map<String, String> resolved = new LinkedHashMap<>();
      String head;
      String foo;
      if (reExport) {
        client.register("leaf", new ProtobufSchema(leaf));
        List<SchemaReference> below = new ArrayList<>();
        Map<String, String> belowResolved = new LinkedHashMap<>();
        if (levels == 2) {
          client.register("wrap1", new ProtobufSchema(wrap1,
              Collections.singletonList(new SchemaReference("leaf.proto", "leaf", 1)),
              Collections.singletonMap("leaf.proto", leaf), null, null));
          below.add(new SchemaReference("wrap1.proto", "wrap1", 1));
          belowResolved.put("wrap1.proto", wrap1);
        }
        below.add(new SchemaReference("leaf.proto", "leaf", 1));
        belowResolved.put("leaf.proto", leaf);
        String wrap = levels == 2 ? wrap2 : wrap1;
        client.register("wrap", new ProtobufSchema(wrap, below, belowResolved, null, null));
        references.add(new SchemaReference("wrap.proto", "wrap", 1));
        references.addAll(below);
        resolved.put("wrap.proto", wrap);
        resolved.putAll(belowResolved);
        head = "import \"wrap.proto\";\n";
        foo = "  com.Foo foo = 3;\n";
      } else {
        client.register("opts", new ProtobufSchema(OPTIONS));
        references.add(new SchemaReference("opts.proto", "opts", 1));
        resolved.put("opts.proto", OPTIONS);
        head = "import \"opts.proto\";\n";
        foo = "";
      }
      String memo = reExport ? "  string memo = 2;\n" : "  string memo = 2 [(o.label) = \"x\"];\n";
      List<ProtobufSchema> versions = new ArrayList<>();
      for (String members : new String[] {"  string note = 2;\n", "", memo}) {
        ProtobufSchema version = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n" + head
            + "message Row {\n  int32 id = 1;\n" + members + foo + "}\n",
            references, resolved, null, null);
        client.register(SUBJECT, version);
        versions.add(version);
      }
      Descriptor row = versions.get(0).toDescriptor();
      byte[] body = DynamicMessage.newBuilder(row).setField(row.findFieldByName("id"), 7)
          .setField(row.findFieldByName("note"), "old").build().toByteArray();
      byte[] bytes = ByteBuffer.allocate(6 + body.length).put((byte) 0)
          .putInt(client.getId(SUBJECT, versions.get(0))).put((byte) 0).put(body).array();

      DynamicMessage read = read(versions.get(2), bytes, "v1");
      assertEquals("", get(read, "memo"), "levels " + levels);
      assertEquals(7, get(read, "id"), "levels " + levels);
    }
  }

  @Test
  void aVersionWithTooManyLocationsIsReadWithoutProvenance() throws Exception {
    // M0 reaches M16 by two fields at each level: too many locations, so no provenance, and the
    // record reads as written, memo and all, where computing it would exhaust the registry.
    byte[] bytes = write(doubling("string note = 3;"), b -> b.setField(field(b, "note"), "old"));
    client.register(SUBJECT, doubling(""));
    client.register(SUBJECT, doubling("string memo = 3;"));

    assertEquals("old", get(read(doubling("string memo = 3;"), bytes, "v1"), "memo"));
  }

  // M0, holding two fields of M1 and the given member, through M16, each holding two of the next.
  private static ProtobufSchema doubling(String member) {
    StringBuilder file = new StringBuilder("syntax = \"proto3\";\npackage p;\n");
    for (int i = 0; i < 16; i++) {
      file.append("message M").append(i).append(" {\n  M").append(i + 1).append(" a = 1;\n  M")
          .append(i + 1).append(" b = 2;\n").append(i == 0 ? "  " + member + "\n" : "")
          .append("}\n");
    }
    return new ProtobufSchema(file.append("message M16 {\n  int32 id = 1;\n}\n").toString());
  }

  @Test
  void aMessageStandingFirstOnlyInAnInteriorVersionIsNotTakenForTheRecords() throws Exception {
    // v2 puts B before A: in single-message provenance its root is B, another message, so A's
    // fields restart there rather than chain note through B's y into memo.
    String head = "syntax = \"proto3\";\npackage p;\n";
    byte[] bytes = write(new ProtobufSchema(head + "message A {\n  int32 id = 1;\n"
        + "  string note = 2;\n}\n"), b -> b.setField(field(b, "id"), 7)
        .setField(field(b, "note"), "old"));
    client.register(SUBJECT, new ProtobufSchema(head + "message B {\n  int32 x = 1;\n"
        + "  string y = 2;\n}\nmessage A {\n  int32 id = 1;\n}\n"));
    ProtobufSchema v3 = new ProtobufSchema(head + "message A {\n  int32 id = 1;\n"
        + "  string memo = 2;\n}\n");
    client.register(SUBJECT, v3);

    assertEquals("", get(read(v3, bytes, "v1"), "memo"));
  }

  @Test
  void aRecordOfAnotherThanItsFilesFirstMessageReadBySingleMessageIsReadAsWritten()
      throws Exception {
    // The reader's file has one message, so provenance is single-message, rooted at each file's
    // first message: B, written second, has no locations there, and reads as without provenance.
    String head = "syntax = \"proto3\";\npackage p;\nmessage A {\n  int32 a = 1;\n}\n";
    ProtobufSchema v1 = new ProtobufSchema(head + "message B {\n  int32 id = 1;\n"
        + "  string note = 2;\n}\n");
    Descriptor b = v1.toDescriptor("p.B");
    byte[] body = DynamicMessage.newBuilder(b).setField(b.findFieldByName("id"), 7)
        .setField(b.findFieldByName("note"), "old").build().toByteArray();
    int id = client.register(SUBJECT, v1);
    byte[] indexes = v1.toMessageIndexes("p.B").toByteArray();
    byte[] bytes = ByteBuffer.allocate(5 + indexes.length + body.length).put((byte) 0).putInt(id)
        .put(indexes).put(body).array();
    client.register(SUBJECT, new ProtobufSchema(head + "message B {\n  int32 id = 1;\n}\n"));
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "message B {\n  int32 id = 1;\n  string memo = 2;\n}\n");
    client.register(SUBJECT, reader);

    DynamicMessage read = read(reader, bytes, "v1");
    assertEquals(7, get(read, "id"));
    assertEquals("old", get(read, "memo"));
  }

  @Test
  void aRecordOfTheSecondMessageFirstLeavesTheFirstMessagesProvenanceAlone() throws Exception {
    // One writer id, two messages: B's failure is its own, so A, read after it, still is A.
    String b = "message B {\n  int32 x = 1;\n}\n";
    ProtobufSchema v1 = file("message A {\n  int32 id = 1;\n  string note = 2;\n}\n" + b);
    int id = client.register(SUBJECT, v1);
    client.register(SUBJECT, file("message A {\n  int32 id = 1;\n}\n" + b));
    ProtobufSchema reader = file("message A {\n  int32 id = 1;\n  string memo = 2;\n}\n");
    client.register(SUBJECT, reader);
    Descriptor a = v1.toDescriptor("p.A");
    Descriptor bb = v1.toDescriptor("p.B");
    byte[] ofB = framed(id, v1, "p.B",
        DynamicMessage.newBuilder(bb).setField(bb.findFieldByName("x"), 3).build());
    byte[] ofA = framed(id, v1, "p.A", DynamicMessage.newBuilder(a)
        .setField(a.findFieldByName("id"), 7).setField(a.findFieldByName("note"), "old").build());

    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(client, config("v1"));
    assertThrows(SerializationException.class,
        () -> deserializer.deserializeWithSchema(TOPIC, new RecordHeaders(), ofB, w -> reader));
    DynamicMessage read = (DynamicMessage) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), ofA, w -> reader).getValue();
    assertEquals(7, get(read, "id"));
    assertEquals("", get(read, "memo"));
  }

  @Test
  void aRecordOfAnotherThanItsFilesFirstMessageUnderRecordNameStrategyIsReadAsWritten()
      throws Exception {
    // The writer comes back unnamed by its record's name: the check still sees B, not A.
    String a = "message A {\n  int32 a = 1;\n}\n";
    String subject = "p.B";
    ProtobufSchema v1 = file(a + "message B {\n  int32 id = 1;\n  string note = 2;\n}\n");
    int id = client.register(subject, v1);
    client.register(subject, file(a + "message B {\n  int32 id = 1;\n}\n"));
    ProtobufSchema reader = file("message B {\n  int32 id = 1;\n  string memo = 2;\n}\n");
    client.register(subject, reader);
    Descriptor b = v1.toDescriptor("p.B");
    byte[] bytes = framed(id, v1, "p.B", DynamicMessage.newBuilder(b)
        .setField(b.findFieldByName("id"), 7).setField(b.findFieldByName("note"), "old").build());

    Map<String, Object> config = config("v1");
    config.put("value.subject.name.strategy", RecordNameStrategy.class.getName());
    DynamicMessage read = (DynamicMessage) new KafkaProtobufDeserializer<DynamicMessage>(
        client, config).deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, w -> reader)
        .getValue();
    assertEquals(7, get(read, "id"));
    assertEquals("old", get(read, "memo"));
  }

  @Test
  void aRecordUnderTopicRecordNameStrategyIsReadByItsOwnMessagesProvenance() throws Exception {
    // The subject is the topic and the record's message; B, the file's second, keeps its own.
    String subject = TOPIC + "-p.B";
    String a = "message A {\n  int32 a = 1;\n}\n";
    ProtobufSchema v1 = file(a + "message B {\n  int32 id = 1;\n  string note = 2;\n}\n");
    int id = client.register(subject, v1);
    client.register(subject, file(a + "message B {\n  int32 id = 1;\n}\n"));
    ProtobufSchema reader = file(a + "message B {\n  int32 id = 1;\n  string memo = 2;\n}\n");
    client.register(subject, reader);
    Descriptor b = v1.toDescriptor("p.B");
    byte[] bytes = framed(id, v1, "p.B", DynamicMessage.newBuilder(b)
        .setField(b.findFieldByName("id"), 7).setField(b.findFieldByName("note"), "old").build());

    Map<String, Object> config = config("v1");
    config.put("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());
    DynamicMessage read = (DynamicMessage) new KafkaProtobufDeserializer<DynamicMessage>(
        client, config).deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, w -> reader)
        .getValue();
    assertEquals("p.B", read.getDescriptorForType().getFullName());
    assertEquals(7, get(read, "id"));
    assertEquals("", get(read, "memo"));
  }

  // A record of the named message of a file registered under the given id, as the wire frames it.
  private static byte[] framed(int id, ProtobufSchema file, String message, DynamicMessage record) {
    byte[] indexes = file.toMessageIndexes(message).toByteArray();
    byte[] body = record.toByteArray();
    return ByteBuffer.allocate(5 + indexes.length + body.length).put((byte) 0).putInt(id)
        .put(indexes).put(body).array();
  }

  // A v1 record with note "old", then v2 dropping note and v3 adding memo at its number.
  private byte[] readdedMemo() throws Exception {
    byte[] bytes = write(row("int32 id = 1;", "string note = 2;"),
        b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "old"));
    client.register(SUBJECT, row("int32 id = 1;"));
    client.register(SUBJECT, row("int32 id = 1;", "string memo = 2;"));
    return bytes;
  }

  @Test
  void aGeneratedClassIsMatchedToATextWithoutItsFileOptions() throws Exception {
    // Registered by a producer in another language: no java_outer_classname, which steers only
    // code generation, so the class still stands for the latest version.
    String head = "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n";
    ProtobufSchema v1 = new ProtobufSchema(head
        + "message Readded {\n  int32 id = 1;\n  string note = 2;\n}\n");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "old"));
    client.register(SUBJECT, new ProtobufSchema(head + "message Readded {\n  int32 id = 1;\n}\n"));
    client.register(SUBJECT, new ProtobufSchema(head
        + "message Readded {\n  int32 id = 1;\n  string memo = 2;\n}\n"));

    assertEquals("old", readClass(Readded.class, bytes, null, false).getMemo());
    assertEquals("", readClass(Readded.class, bytes, "v1", false).getMemo());
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

  @Test
  void aNestedRecordWhoseUsesAllRestartedReadsDefaults() throws Exception {
    // In's only use, y, is dropped and re-added as y2: nothing of In continues, so i is new.
    String x = "message X { int32 z = 1; }";
    ProtobufSchema v1 = file("message M { In y = 1; message In { int32 i = 1; } }", x);
    ProtobufSchema v2 = file("message M { message In { int32 i = 1; } }", x);
    ProtobufSchema v3 = file("message M { In y2 = 1; message In { int32 i = 1; } }", x);
    Descriptor in = v1.toDescriptor("p.M.In");
    byte[] bytes = write(v1,
        DynamicMessage.newBuilder(in).setField(in.findFieldByName("i"), 5).build());
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals(5, get(read(v3, bytes, null), "i"));
    assertEquals(0, get(read(v3, bytes, "v1"), "i"));
  }

  @Test
  void aFieldRetypedToAnotherTopLevelMessageTakesNoValue() throws Exception {
    // x moves from S to T, which another top-level message declares: t1 is new, and the read must
    // not fall back to parsing the writer's s1 into it.
    String m = "message M { int32 a = 1; %s x = 2; }";
    String s = "message S { int32 s1 = 1; string s2 = 2; }";
    String t = "message T { int32 t1 = 1; string t2 = 2; }";
    String x = "message X { int32 z = 1; }";
    ProtobufSchema v1 = file(String.format(m, "S"), s, t, x);
    ProtobufSchema v2 = file(String.format(m, "T"), s, t, x);
    Descriptor sd = v1.toDescriptor("p.S");
    byte[] bytes = write(v1, b -> b.setField(field(b, "a"), 7).setField(field(b, "x"),
        DynamicMessage.newBuilder(sd).setField(sd.findFieldByName("s1"), 5).build()));
    client.register(SUBJECT, v2);

    DynamicMessage off = (DynamicMessage) get(read(v2, bytes, null), "x");
    assertEquals(5, off.getField(off.getDescriptorForType().findFieldByName("t1")));
    DynamicMessage on = read(v2, bytes, "v1");
    assertEquals(7, get(on, "a"));
    DynamicMessage onX = (DynamicMessage) get(on, "x");
    assertEquals(0, onX.getField(onX.getDescriptorForType().findFieldByName("t1")));
  }

  @Test
  void aNestedRecordTakesNoValueOfASwappedNestedType() throws Exception {
    // In2 is renamed Inner as the old Inner goes: x's q continues, but the writer's Inner.p, at
    // q's number, is no field the reader's Inner continues.
    String x = "message X { int32 z = 1; }";
    ProtobufSchema v1 = file("message M { int32 a = 1; In2 x = 2; "
        + "message Inner { int32 p = 1; } message In2 { int32 q = 1; } }", x);
    ProtobufSchema v2 =
        file("message M { int32 a = 1; Inner x = 2; message Inner { int32 q = 1; } }", x);
    Descriptor inner = v1.toDescriptor("p.M.Inner");
    byte[] bytes = write(v1,
        DynamicMessage.newBuilder(inner).setField(inner.findFieldByName("p"), 5).build());
    client.register(SUBJECT, v2);

    assertEquals(5, get(read(v2, bytes, null), "q"));
    assertEquals(0, get(read(v2, bytes, "v1"), "q"));
  }

  @Test
  void aNestedRecordIgnoresConflictsOutsideItsMessage() throws Exception {
    // N's s2 moves from T to S, so S needs two numberings; a record of p.M.Inner never parses N,
    // so Inner's k2, new, must not fall back to the writer's k.
    String n = "message N { S s1 = 1; %s s2 = 2; }";
    String st = "message S { int32 x = 1; }\nmessage T { int32 y = 1; }";
    String m = "message M { Inner a = 1; message Inner { int32 i = 1; %s } }";
    ProtobufSchema v1 = file(String.format(m, "int32 k = 2;"), String.format(n, "T"), st);
    ProtobufSchema v2 = file(String.format(m, ""), String.format(n, "S"), st);
    ProtobufSchema v3 = file(String.format(m, "int32 k2 = 2;"), String.format(n, "S"), st);
    Descriptor inner = v1.toDescriptor("p.M.Inner");
    byte[] bytes = write(v1, DynamicMessage.newBuilder(inner)
        .setField(inner.findFieldByName("i"), 4).setField(inner.findFieldByName("k"), 5).build());
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals(5, get(read(v3, bytes, null), "k2"));
    DynamicMessage read = read(v3, bytes, "v1");
    assertEquals(4, get(read, "i"));
    assertEquals(0, get(read, "k2"));
  }

  @Test
  void aOneofInTheRootMessageIsReadOverTheWire() throws Exception {
    // The oneof's location has no names of its own: the response must still carry them, as [].
    ProtobufSchema v1 = row("int32 a = 1;", "oneof o { int32 b = 2; string s = 3; }");
    ProtobufSchema v2 = row("int32 a = 1;", "oneof o { int32 b = 2; string s = 3; }",
        "int32 d = 4;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "a"), 7).setField(field(b, "b"), 5));
    client.register(SUBJECT, v2);

    DynamicMessage read = read(v2, bytes, "v1");
    assertEquals(7, get(read, "a"));
    assertEquals(5, get(read, "b"));
  }

  @Test
  void aOneofInAUserWrittenEntrysValueRestartsAlone() throws Exception {
    // The converter reads KvEntry as a map by its shape: the oneof is spelled at the value
    // slot, and its restart used to move the whole value, x included.
    String entry = "repeated KvEntry kv = 1; message KvEntry { string key = 1; In value = 2; }";
    String oneof = "oneof o { int32 a = 2; string b = 3; }";
    ProtobufSchema v1 = row(entry, "message In { int32 x = 1; " + oneof + " }");
    ProtobufSchema v2 = row(entry, "message In { int32 x = 1; }");
    ProtobufSchema v3 = row(entry, "message In { int32 x = 1; " + oneof + " }", "int32 z = 4;");
    byte[] bytes = write(v1, entryRecord(v1, Map.of("x", 7, "a", 5)));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage value = entryValue(read(v3, bytes, "v1"));
    assertEquals(7, get(value, "x"));
    assertEquals(0, get(value, "a"));
  }

  @Test
  void twoOneofsInAUserWrittenEntrysValueRestartApart() throws Exception {
    // One oneof restarts and one continues: taken for the value field, they asked for two
    // numberings of it, and the read fell back to giving the restarted a its old value.
    String entry = "repeated KvEntry kv = 1; message KvEntry { string key = 1; In value = 2; }";
    String o = "oneof o { int32 a = 2; string s = 4; }";
    String p = "oneof p { int32 c = 3; string t = 5; }";
    ProtobufSchema v1 = row(entry, "message In { int32 x = 1; " + o + " " + p + " }");
    ProtobufSchema v2 = row(entry, "message In { int32 x = 1; " + p + " }");
    ProtobufSchema v3 = row(entry, "message In { int32 x = 1; " + o + " " + p + " }",
        "int32 z = 9;");
    byte[] bytes = write(v1, entryRecord(v1, Map.of("x", 7, "a", 5, "c", 9)));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage value = entryValue(read(v3, bytes, "v1"));
    assertEquals(7, get(value, "x"));
    assertEquals(9, get(value, "c"));
    assertEquals(0, get(value, "a"));
  }

  @Test
  void aReaderPinnedToAVersionIsReadAsThatVersion() throws Exception {
    // v3 equals v1 but for metadata, and note was dropped at v2, so v3's note is new. A reader
    // registered nowhere is matched by structure to v3; pinned, it is v1, through its copy.
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "ada"));
    client.register(SUBJECT, row("int32 id = 1;"));
    client.register(SUBJECT, withMetadata(v1, "v3"));
    ProtobufSchema merged = withMetadata(v1, "merged");
    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(client, config("v1"));

    DynamicMessage byStructure = (DynamicMessage) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> merged, false).getValue();
    DynamicMessage pinned = (DynamicMessage) deserializer.deserializeWithReaderSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> ReaderSchema.of(merged, SUBJECT, 1), false)
        .getValue();
    assertEquals("", get(byStructure, "note"));
    assertEquals("ada", get(pinned, "note"));
  }

  @Test
  void aOneofInANestedUserWrittenEntrysValueRestartsAlone() throws Exception {
    // A map of maps is written as nested entry messages: the oneof sits two slots below the
    // map, and its restart used to move the whole inner value, x included.
    String in = "message In { int32 x = 1; %s }";
    String oneof = "oneof o { int32 a = 2; string b = 3; }";
    ProtobufSchema v1 = row(MAP_OF_MAPS, String.format(in, oneof));
    ProtobufSchema v2 = row(MAP_OF_MAPS, String.format(in, ""));
    ProtobufSchema v3 = row(MAP_OF_MAPS, String.format(in, oneof), "int32 z = 9;");
    byte[] bytes = write(v1, along(v1, "kv[].value[].value", Map.of("x", 7, "a", 5)));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage value = at(read(v3, bytes, "v1"), "kv[].value[].value");
    assertEquals(7, get(value, "x"));
    assertEquals(0, get(value, "a"));
  }

  @Test
  void twoOneofsInANestedUserWrittenEntrysValueRestartApart() throws Exception {
    // One oneof restarts and one continues: taken for the inner value field, they asked for two
    // numberings of it, and the read fell back to giving the restarted a its old value.
    String in = "message In { int32 x = 1; %s oneof p { int32 c = 4; string t = 5; } }";
    String oneof = "oneof o { int32 a = 2; string b = 3; }";
    ProtobufSchema v1 = row(MAP_OF_MAPS, String.format(in, oneof));
    ProtobufSchema v2 = row(MAP_OF_MAPS, String.format(in, ""));
    ProtobufSchema v3 = row(MAP_OF_MAPS, String.format(in, oneof), "int32 z = 9;");
    byte[] bytes = write(v1, along(v1, "kv[].value[].value", Map.of("x", 7, "a", 5, "c", 9)));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage value = at(read(v3, bytes, "v1"), "kv[].value[].value");
    assertEquals(7, get(value, "x"));
    assertEquals(9, get(value, "c"));
    assertEquals(0, get(value, "a"));
  }

  @Test
  void aOneofInAUserWrittenEntrysRepeatedValueRestartsAlone() throws Exception {
    // A map of lists: the oneof sits in the list's element, below the entry's value slot.
    String entry = "repeated VEntry kv = 1; "
        + "message VEntry { string key = 1; repeated In value = 2; }";
    String in = "message In { int32 x = 1; %s }";
    String oneof = "oneof o { int32 a = 2; string b = 3; }";
    ProtobufSchema v1 = row(entry, String.format(in, oneof));
    ProtobufSchema v2 = row(entry, String.format(in, ""));
    ProtobufSchema v3 = row(entry, String.format(in, oneof), "int32 z = 9;");
    byte[] bytes = write(v1, along(v1, "kv[].value[]", Map.of("x", 7, "a", 5)));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage read = at(read(v3, bytes, "v1"), "kv[]");
    assertEquals(1, ((List<?>) get(read, "value")).size());
    DynamicMessage value = at(read, "value[]");
    assertEquals(7, get(value, "x"));
    assertEquals(0, get(value, "a"));
  }

  @Test
  void aWrappedUnionFieldNamedValueInAMapOfListsIsNoOneof() throws Exception {
    // In a map of lists, In's wrapped union field value is spelled as a oneof at the list's slot
    // would be; taken for one, its restart was not moved, and it read as set with no branch.
    String wrapped = "UW value = 2 [(confluent.field_meta) = {params: [{key: \"flink.wrapped\", "
        + "value: \"true\"}]}];";
    String rest = "repeated KvEntry kv = 1; "
        + "message KvEntry { string key = 1; repeated In value = 2; } "
        + "message UW { oneof value { int32 a = 2; string b = 3; } } ";
    ProtobufSchema v1 = withMeta(rest + "message In { int32 x = 1; " + wrapped + " }");
    ProtobufSchema v2 = withMeta(rest + "message In { int32 x = 1; }");
    ProtobufSchema v3 = withMeta(rest + "message In { int32 x = 1; " + wrapped + " } int32 z = 9;");
    Descriptor uw = v1.toDescriptor("p.Row.UW");
    DynamicMessage union = DynamicMessage.newBuilder(uw).setField(uw.findFieldByName("a"), 5).build();
    byte[] bytes = write(v1, along(v1, "kv[].value[]", Map.of("x", 7, "value", union)));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage in = at(read(v3, bytes, "v1"), "kv[].value[]");
    assertEquals(7, get(in, "x"));
    assertFalse(in.hasField(in.getDescriptorForType().findFieldByName("value")));
  }

  @Test
  void aOneofInAFlinkWrappedListOrMapRestartsAlone() throws Exception {
    // Flink wraps a nullable list or map in a message whose payload is named value: In's oneof is
    // then spelled below the wrapper's payload, and was taken for it, moving the whole list.
    String wrapped = " [(confluent.field_meta) = {params: [{key: \"flink.wrapped\", "
        + "value: \"true\"}]}]";
    String list = "WL ins = 1" + wrapped + "; message WL { repeated In value = 1; } ";
    String map = "WM ins = 1" + wrapped + "; message WM { repeated InEntry value = 1; } "
        + "message InEntry { string key = 1; In value = 2; } ";
    String[][] shapes = {{list, "ins.value[]"}, {map, "ins.value[].value"}};
    String oneof = "oneof o { int32 a = 2; string s = 4; }";
    for (String[] shape : shapes) {
      for (boolean two : new boolean[] {false, true}) {
        // With a second oneof continuing, the two asked for two numberings and fell back,
        // giving the restarted a its old value.
        String other = two ? "oneof p { int32 c = 3; string t = 5; }" : "";
        client = new ProvenanceMockSchemaRegistryClient();
        serializer = new KafkaProtobufSerializer<>(client, config(null));
        ProtobufSchema v1 = withMeta(shape[0] + "message In { int32 x = 1; " + oneof + " "
            + other + " }");
        ProtobufSchema v2 = withMeta(shape[0] + "message In { int32 x = 1; " + other + " }");
        ProtobufSchema v3 = withMeta(shape[0] + "message In { int32 x = 1; " + oneof + " "
            + other + " } int32 z = 9;");
        byte[] bytes = write(v1, along(v1, shape[1],
            two ? Map.of("x", 7, "a", 5, "c", 9) : Map.of("x", 7, "a", 5)));
        client.register(SUBJECT, v2);
        client.register(SUBJECT, v3);

        DynamicMessage read = read(v3, bytes, "v1");
        assertEquals(1, ((List<?>) get(at(read, "ins"), "value")).size());
        DynamicMessage in = at(read, shape[1]);
        assertEquals(7, get(in, "x"));
        assertEquals(0, get(in, "a"));
        if (two) {
          assertEquals(9, get(in, "c"));
        }
      }
    }
  }

  @Test
  void aPinDoesNotReachALaterEqualReaderThroughTheSharedNamedCopy() throws Exception {
    // v3 equals v1 but for metadata, and note was dropped at v2, so v3's note is new. Equal
    // readers share one named copy in the deserializer: a pin put on it reached unpinned readers.
    ProtobufSchema v1 = row("int32 id = 1;", "string note = 2;");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "note"), "ada"));
    client.register(SUBJECT, row("int32 id = 1;"));
    client.register(SUBJECT, withMetadata(v1, "v3"));
    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(client, config("v1"));
    ProtobufSchema pinnedReader = withMetadata(v1, "merged");

    assertEquals("ada", get(readPinned(deserializer, bytes, pinnedReader, 1), "note"));
    assertEquals("", get((DynamicMessage) deserializer.deserializeWithSchema(TOPIC,
        new RecordHeaders(), bytes, w -> withMetadata(v1, "merged"), false).getValue(), "note"));
    assertEquals("", get(readPinned(deserializer, bytes, withMetadata(v1, "merged"), 3), "note"));
    assertEquals("ada", get(readPinned(deserializer, bytes, pinnedReader, 1), "note"));
  }

  @Test
  void pinsInterleavedAcrossAMultiMessageFileKeepToTheirOwnReads() throws Exception {
    // v3 equals v1 but for metadata, and note was dropped at v2, so v3's notes are new. Records of
    // A and B are read pinned to v1, pinned to v3, and unpinned, in turn, on one deserializer.
    String messages = "message A {\n  int32 id = 1;%s\n}\nmessage B {\n  int32 id = 1;%s\n}\n";
    ProtobufSchema v1 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + String.format(messages, " string note = 2;", " string note = 2;"));
    ProtobufSchema v2 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + String.format(messages, "", ""));
    int id = client.register(SUBJECT, v1);
    Map<String, Object> pinToV1 = config(null);
    pinToV1.put("use.schema.id", id);
    KafkaProtobufSerializer<DynamicMessage> writer = new KafkaProtobufSerializer<>(client, pinToV1);
    Map<String, byte[]> records = new HashMap<>();
    for (String message : new String[] {"p.A", "p.B"}) {
      Descriptor descriptor = v1.toDescriptor(message);
      records.put(message, writer.serialize(TOPIC, DynamicMessage.newBuilder(descriptor)
          .setField(descriptor.findFieldByName("id"), 7)
          .setField(descriptor.findFieldByName("note"), "ada").build()));
    }
    client.register(SUBJECT, v2);
    client.register(SUBJECT, withMetadata(v1, "v3"));
    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(client, config("v1"));
    ProtobufSchema pinned = withMetadata(v1, "merged");

    String[][] reads = {{"p.A", "1"}, {"p.B", "-"}, {"p.B", "1"}, {"p.A", "-"},
        {"p.A", "3"}, {"p.B", "3"}, {"p.A", "1"}, {"p.B", "-"}};
    for (String[] read : reads) {
      byte[] bytes = records.get(read[0]);
      DynamicMessage got = read[1].equals("-")
          ? (DynamicMessage) deserializer.deserializeWithSchema(TOPIC, new RecordHeaders(), bytes,
              w -> withMetadata(v1, "merged"), false).getValue()
          : readPinned(deserializer, bytes, pinned, Integer.parseInt(read[1]));
      assertEquals(read[1].equals("1") ? "ada" : "", get(got, "note"),
          read[0] + " read pinned to " + read[1]);
    }
  }

  @Test
  void aFreshNumberAvoidsTheWriterUnderAFieldRetypedToAnExistingMessage() throws Exception {
    // f's type changes from A to B, both declared all along: f's data is still an A, so B's new b
    // must take no number A writes under, here A's hi at the highest.
    String messages = "message A { int32 x = 1; string hi = 536870911; }\n"
        + "message B { C b = 1; }\nmessage C { int32 z = 1; }";
    ProtobufSchema v1 = file("message Row { int32 id = 1; A f = 2; }", messages);
    ProtobufSchema v2 = file("message Row { int32 id = 1; B f = 2; }", messages);
    Descriptor a = v1.toDescriptor("p.A");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "f"),
        DynamicMessage.newBuilder(a).setField(a.findFieldByName("hi"), "\u0000").build()));
    client.register(SUBJECT, v2);

    DynamicMessage read = read(v2, bytes, "v1");
    assertEquals(7, get(read, "id"));
    DynamicMessage f = (DynamicMessage) get(read, "f");
    assertFalse(f.hasField(f.getDescriptorForType().findFieldByName("b")));
  }

  @Test
  void aFreshNumberAvoidsTheWriterOfAMessageMovedOutOfAnImport() throws Exception {
    // A moves from an import into the file, dropping hi and adding b: the writer's A is the
    // imported one, so b's fresh number must avoid its numbers too.
    String dep = "syntax = \"proto3\";\npackage p;\n"
        + "message A { int32 x = 1; string hi = 536870911; }\n";
    client.register("dep", new ProtobufSchema(dep));
    ProtobufSchema v1 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\nimport \"a.proto\";\n"
        + "message Row { int32 id = 1; A f = 2; }\n",
        Collections.singletonList(new SchemaReference("a.proto", "dep", 1)),
        Collections.singletonMap("a.proto", dep), null, null);
    ProtobufSchema v2 = file("message Row { int32 id = 1; A f = 2; }",
        "message A { int32 x = 1; C b = 3; }\nmessage C { int32 z = 1; }");
    int id = client.register(SUBJECT, v1);
    Descriptor row = v1.toDescriptor();
    Descriptor a = row.findFieldByName("f").getMessageType();
    byte[] bytes = framed(id, v1, "p.Row", DynamicMessage.newBuilder(row)
        .setField(row.findFieldByName("id"), 7).setField(row.findFieldByName("f"),
            DynamicMessage.newBuilder(a).setField(a.findFieldByName("hi"), "\u0000").build())
        .build());
    client.register(SUBJECT, v2);

    DynamicMessage read = read(v2, bytes, "v1");
    assertEquals(7, get(read, "id"));
    DynamicMessage f = (DynamicMessage) get(read, "f");
    assertFalse(f.hasField(f.getDescriptorForType().findFieldByName("b")));
  }

  @Test
  void aFreshNumberAvoidsTheWriterUnderAMapValueRetypedToAnExistingMessage() throws Exception {
    // A map value's type changes from A to B: the value has no location of its own, yet its data
    // is still an A's, so B's new b must take no number A writes under.
    String messages = "message A { int32 x = 1; string hi = 536870911; }\n"
        + "message B { C b = 1; }\nmessage C { int32 z = 1; }";
    ProtobufSchema v1 = file("message Row { int32 id = 1; map<string, A> f = 2; }", messages);
    ProtobufSchema v2 = file("message Row { int32 id = 1; map<string, B> f = 2; }", messages);
    int id = client.register(SUBJECT, v1);
    Descriptor row = v1.toDescriptor();
    Descriptor entry = row.findFieldByName("f").getMessageType();
    Descriptor a = v1.toDescriptor("p.A");
    byte[] bytes = framed(id, v1, "p.Row", DynamicMessage.newBuilder(row)
        .setField(row.findFieldByName("id"), 7).addRepeatedField(row.findFieldByName("f"),
            DynamicMessage.newBuilder(entry).setField(entry.findFieldByName("key"), "k")
                .setField(entry.findFieldByName("value"), DynamicMessage.newBuilder(a)
                    .setField(a.findFieldByName("hi"), "\u0000").build()).build())
        .build());
    client.register(SUBJECT, v2);

    DynamicMessage read = read(v2, bytes, "v1");
    assertEquals(7, get(read, "id"));
    DynamicMessage value =
        (DynamicMessage) get((DynamicMessage) ((List<?>) get(read, "f")).get(0), "value");
    assertFalse(value.hasField(value.getDescriptorForType().findFieldByName("b")));
  }

  @Test
  void aFreshNumberAvoidsTheWriterUnderARetypeInASingleMessageFile() throws Exception {
    // As above with the file's one message, so locations name no message: A and B are named
    // nested types of Row.
    String named = "option (confluent.message_meta) = "
        + "{params: [{key: \"logical.named\", value: \"true\"}]}; ";
    String nested = "message A { " + named + "int32 x = 1; string hi = 536870911; } "
        + "message B { " + named + "C b = 1; } message C { int32 z = 1; }";
    ProtobufSchema v1 = withMeta("int32 id = 1; A f = 2; " + nested);
    ProtobufSchema v2 = withMeta("int32 id = 1; B f = 2; " + nested);
    Descriptor a = v1.toDescriptor("p.Row.A");
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7).setField(field(b, "f"),
        DynamicMessage.newBuilder(a).setField(a.findFieldByName("hi"), "\u0000").build()));
    client.register(SUBJECT, v2);

    DynamicMessage read = read(v2, bytes, "v1");
    assertEquals(7, get(read, "id"));
    DynamicMessage f = (DynamicMessage) get(read, "f");
    assertFalse(f.hasField(f.getDescriptorForType().findFieldByName("b")));
  }

  @Test
  void aReAddedFieldFindsANumberBelowItsExtensionsBesideALargerMessage() throws Exception {
    // M's numbers end at extensions 100 to max, and Big takes 1 to 99: avoiding Big's numbers too
    // leaves none, so memo must move by avoiding M's own, not fall back to its old value.
    StringBuilder big = new StringBuilder("message Big {");
    for (int i = 1; i <= 99; i++) {
      big.append(" optional int32 b").append(i).append(" = ").append(i).append(";");
    }
    String head = "syntax = \"proto2\";\npackage p;\n"
        + "message Row { optional int32 id = 1; optional M u = 2; optional Big b = 3; }\n";
    String tail = " extensions 100 to max; }\n" + big + " }\n";
    ProtobufSchema v1 = new ProtobufSchema(head
        + "message M { optional int32 x = 1; optional int32 note = 2;" + tail);
    ProtobufSchema v2 = new ProtobufSchema(head + "message M { optional int32 x = 1;" + tail);
    ProtobufSchema v3 = new ProtobufSchema(head
        + "message M { optional int32 x = 1; optional int32 memo = 2;" + tail);
    int id = client.register(SUBJECT, v1);
    Descriptor row = v1.toDescriptor("p.Row");
    Descriptor m = v1.toDescriptor("p.M");
    byte[] bytes = framed(id, v1, "p.Row", DynamicMessage.newBuilder(row)
        .setField(row.findFieldByName("id"), 7).setField(row.findFieldByName("u"),
            DynamicMessage.newBuilder(m).setField(m.findFieldByName("note"), 77).build())
        .build());
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    DynamicMessage read = read(v3, bytes, "v1");
    assertEquals(7, get(read, "id"));
    DynamicMessage u = (DynamicMessage) get(read, "u");
    assertFalse(u.hasField(u.getDescriptorForType().findFieldByName("memo")));
  }

  @Test
  void aMessageChainNestingPastTheDepthLimitReadsWithoutProvenance() throws Exception {
    // Past the depth limit the history has no provenance: read natively, once warned, rather
    // than every record failing as the walk exhausts the stack.
    StringBuilder chain = new StringBuilder();
    for (int i = 0; i < 2000; i++) {
      chain.append("message M").append(i).append(" { M").append(i + 1).append(" p = 1; }\n");
    }
    chain.append("message M2000 { int32 x = 1; }");
    ProtobufSchema v1 = file("message Row { int32 id = 1; M0 m = 2; }", chain.toString());
    ProtobufSchema v2 = file("message Row { int32 id = 1; M0 m = 2; string memo = 3; }",
        chain.toString());
    byte[] bytes = write(v1, b -> b.setField(field(b, "id"), 7));
    client.register(SUBJECT, v2);

    assertEquals(7, get(read(v2, bytes, "v1"), "id"));
  }

  @Test
  void aDerivedTypeThatIsNoMessageIsNotInitializedUnderProvenance() throws Exception {
    // The writer names a class that is no message: as without provenance, it is never initialized.
    ProtobufSchema schema = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "option java_package = \"io.confluent.kafka.serializers.provenance\";\n"
        + "option java_multiple_files = true;\nmessage NotAMessage {\n  int32 id = 1;\n}\n");
    int id = client.register(SUBJECT, schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(id).array());
    out.write(0);
    out.write(new byte[] {8, 7});
    Map<String, Object> config = config("v1");
    config.put("derive.type", true);
    KafkaProtobufDeserializer<Message> deserializer = new KafkaProtobufDeserializer<>(client, config);
    assertThrows(SerializationException.class,
        () -> deserializer.deserialize(TOPIC, out.toByteArray()));
    assertNull(System.getProperty(NotAMessage.INITIALIZED));
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

  private static DynamicMessage readPinned(KafkaProtobufDeserializer<DynamicMessage> deserializer,
      byte[] bytes, ProtobufSchema reader, int version) {
    return (DynamicMessage) deserializer.deserializeWithReaderSchema(TOPIC, new RecordHeaders(),
        bytes, w -> ReaderSchema.of(reader, SUBJECT, version), false).getValue();
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

  // A Row holding one KvEntry, keyed "k", whose In value has the given fields set.
  private static DynamicMessage entryRecord(ProtobufSchema schema, Map<String, Object> fields) {
    Descriptor kv = schema.toDescriptor("p.Row.KvEntry");
    Descriptor in = schema.toDescriptor("p.Row.In");
    DynamicMessage.Builder value = DynamicMessage.newBuilder(in);
    fields.forEach((name, v) -> value.setField(in.findFieldByName(name), v));
    DynamicMessage entry = DynamicMessage.newBuilder(kv)
        .setField(kv.findFieldByName("key"), "k")
        .setField(kv.findFieldByName("value"), value.build()).build();
    return DynamicMessage.newBuilder(schema.toDescriptor())
        .addRepeatedField(schema.toDescriptor().findFieldByName("kv"), entry).build();
  }

  private static DynamicMessage entryValue(DynamicMessage record) {
    return (DynamicMessage) get((DynamicMessage) ((List<?>) get(record, "kv")).get(0), "value");
  }

  // One message, Row, holding the given members, in a file importing Confluent's field options.
  private static ProtobufSchema withMeta(String members) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"confluent/meta.proto\";\nmessage Row {\n  " + members + "\n}\n");
  }

  // A map of maps, as nested entry messages, whose inner values are In.
  private static final String MAP_OF_MAPS = "repeated OuterEntry kv = 1; "
      + "message OuterEntry { string key = 1; repeated InnerEntry value = 2; } "
      + "message InnerEntry { string key = 1; In value = 2; }";

  // A Row holding one message along steps ("[]" for one element of a repeated field), keyed "k"
  // wherever an entry has a key, ending at an In with the given fields set.
  private static DynamicMessage along(ProtobufSchema schema, String steps,
      Map<String, Object> fields) {
    Descriptor in = schema.toDescriptor("p.Row.In");
    DynamicMessage.Builder leaf = DynamicMessage.newBuilder(in);
    fields.forEach((name, v) -> leaf.setField(in.findFieldByName(name), v));
    return (DynamicMessage) along(schema.toDescriptor(), steps.split("\\."), 0, leaf.build());
  }

  private static Object along(Descriptor message, String[] steps, int i, DynamicMessage leaf) {
    String name = steps[i].replace("[]", "");
    FieldDescriptor field = message.findFieldByName(name);
    Object child = i + 1 == steps.length ? leaf
        : along(field.getMessageType(), steps, i + 1, leaf);
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(message);
    if (message.findFieldByName("key") != null) {
      builder.setField(message.findFieldByName("key"), "k");
    }
    if (field.isRepeated()) {
      builder.addRepeatedField(field, child);
    } else {
      builder.setField(field, child);
    }
    return builder.build();
  }

  // The message along steps, taking the first element of each repeated field.
  private static DynamicMessage at(DynamicMessage message, String steps) {
    DynamicMessage current = message;
    for (String step : steps.split("\\.")) {
      Object value = get(current, step.replace("[]", ""));
      current = (DynamicMessage) (step.endsWith("[]") ? ((List<?>) value).get(0) : value);
    }
    return current;
  }

  private static DynamicMessage refund(ProtobufSchema schema, int id, int amount) {
    Descriptor refund = schema.toDescriptor("p.Refund");
    return DynamicMessage.newBuilder(refund)
        .setField(refund.findFieldByName("id"), id)
        .setField(refund.findFieldByName("amount"), amount).build();
  }

  private static ProtobufSchema withMetadata(ProtobufSchema schema, String value) {
    return schema.copy(new Metadata(null, Collections.singletonMap("version", value), null), null);
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
  // A record of writer's first message, framed by the id writer is registered under.
  private byte[] writeById(ProtobufSchema writer, Consumer<DynamicMessage.Builder> record)
      throws Exception {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(writer.toDescriptor());
    record.accept(builder);
    byte[] body = builder.build().toByteArray();
    return ByteBuffer.allocate(6 + body.length).put((byte) 0)
        .putInt(client.getId(SUBJECT, writer)).put((byte) 0).put(body).array();
  }

  // One version of main per dependency version, each importing it as d.proto from "dep".
  private List<ProtobufSchema> importing(String main, String... deps) throws Exception {
    List<ProtobufSchema> versions = new ArrayList<>();
    for (int i = 0; i < deps.length; i++) {
      client.register("dep", new ProtobufSchema(deps[i]));
      ProtobufSchema version = new ProtobufSchema(main,
          Collections.singletonList(new SchemaReference("d.proto", "dep", i + 1)),
          Collections.singletonMap("d.proto", deps[i]), null, null);
      client.register(SUBJECT, version);
      versions.add(version);
    }
    return versions;
  }

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
