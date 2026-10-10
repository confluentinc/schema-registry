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

package io.confluent.kafka.serializers.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

/** A response the reader cannot follow fails every record from that writer. */
public class ProtoProvenanceProjectionTest {

  private static final ProtobufSchema READER = new ProtobufSchema(
      "syntax = \"proto3\";\npackage p;\nmessage Row {\n  int32 a = 1;\n}\n");

  @Test
  public void aLocationWithoutNamesFailsEveryRecord() {
    assertThrows(SerializationException.class, () -> ProtoProvenanceProjection.of(READER, null,
        mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), new ProvenanceField(Arrays.asList(2), null, "SCALAR", 2))),
        false));
  }

  @Test
  public void aLocationWithoutAKindFailsEveryRecord() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> ProtoProvenanceProjection.of(READER, null, mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(new ProvenanceField(Arrays.asList(1), Arrays.asList("a"), null, 1))),
            false));
    assertTrue(e.getMessage(), e.getMessage().contains("no kind for location"));
  }

  @Test
  public void aLocationNotInTheReaderFailsEveryRecord() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> ProtoProvenanceProjection.of(READER, null, mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), p(2, "ghost"))), false));
    assertTrue(e.getMessage(), e.getMessage().contains("[ghost] of schema id 2"));
  }

  @Test
  public void aFieldWithNoWriterCounterpartIsLeftOutOfTheParse() {
    // No number is taken for it, so no data under any number can be parsed into it.
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto2\";\npackage p;\nmessage Row {\n"
        + "  optional int32 a = 1;\n  optional string c = 2;\n  extensions 100 to max;\n}\n");
    ProtoProvenanceProjection projection = ProtoProvenanceProjection.of(reader, null,
        mapping(Arrays.asList(p(1, "a")), Arrays.asList(p(1, "a"), p(2, "c"))), false);
    assertNull(projection.schema().toDescriptor().findFieldByName("c"));
    assertEquals(1, projection.schema().toDescriptor().findFieldByName("a").getNumber());
    assertTrue(projection.movedAny());
  }

  @Test
  public void aOneofLeftWithNoMemberIsNoOneof() {
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage Row {\n"
        + "  int32 a = 1;\n  oneof u { string c = 2; }\n  oneof w { string d = 3; string e = 4; }\n"
        + "}\n");
    ProtoProvenanceProjection projection = ProtoProvenanceProjection.of(reader, null,
        mapping(Arrays.asList(p(1, "a"), p(4, "d")),
            Arrays.asList(p(1, "a"), p(2, "c"), p(4, "d"), p(5, "e"))), false);
    Descriptor row = projection.schema().toDescriptor();
    assertEquals(1, row.getOneofs().size());
    assertEquals("w", row.findFieldByName("d").getContainingOneof().getName());
    assertNull(row.findFieldByName("e"));
  }

  @Test
  public void aNestedRecordMessageNoLocationReachesHasNoProvenance() {
    // Locations start at the file's top-level messages: A.Inner, used by no field, has none, so
    // a record of it is read without provenance rather than silently as written.
    String file = "syntax = \"proto3\";\npackage p;\nmessage A {\n  int32 id = 1;\n%s"
        + "  message Inner {\n    string memo = 1;\n  }\n}\n";
    ProtobufSchema unused = new ProtobufSchema(String.format(file, ""));
    ProtobufSchema reader = new ProtobufSchema(unused.toDescriptor("p.A.Inner"));
    assertThrows(ProvenanceUnavailableException.class, () -> ProtoProvenanceProjection.of(
        reader, unused, mapping(Arrays.asList(p(1, "p.A", "id")),
            Arrays.asList(p(1, "p.A", "id"))), true));

    // Used by a field, it is reached through it.
    ProtobufSchema used = new ProtobufSchema(String.format(file, "  Inner inner = 2;\n"));
    ProtobufSchema usedReader = new ProtobufSchema(used.toDescriptor("p.A.Inner"));
    List<ProvenanceField> both = Arrays.asList(p(1, "p.A", "id"), p(2, "p.A", "inner"),
        p(3, "p.A", "inner", "memo"));
    assertEquals(usedReader, ProtoProvenanceProjection.of(usedReader, used,
        mapping(both, both), true).schema());
  }

  @Test
  public void aNestedRecordMessageReachedThroughAOneofOrAMapValueHasProvenance() {
    String file = "syntax = \"proto3\";\npackage p;\nmessage A {\n  int32 id = 1;\n%s"
        + "  message Inner {\n    string memo = 1;\n  }\n}\n";
    String[][] uses = {
        {"  oneof k {\n    Inner i = 2;\n  }\n", "i"},
        {"  map<string, Inner> m = 2;\n", "m"}};
    for (String[] use : uses) {
      ProtobufSchema writer = new ProtobufSchema(String.format(file, use[0]));
      ProtobufSchema reader = new ProtobufSchema(writer.toDescriptor("p.A.Inner"));
      List<ProvenanceField> locations = "i".equals(use[1])
          ? Arrays.asList(p(1, "p.A", "id"), p(2, "p.A"), p(3, "p.A", "i"),
              p(4, "p.A", "i", "memo"))
          : Arrays.asList(p(1, "p.A", "id"), p(2, "p.A", "m"),
              p(3, "p.A", "m", "value", "memo"));
      assertEquals(use[1], reader, ProtoProvenanceProjection.of(reader, writer,
          mapping(locations, locations), true).schema());
    }
  }

  @Test
  public void theReadersOwnParseIsClearedOfWhatLiesUnderAMovedNumberAtAnyDepth() {
    ProtobufSchema writer = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "message Row {\n  int32 a = 1;\n  In in = 3;\n}\nmessage In {\n  int32 x = 1;\n}\n");
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "message Row {\n  int32 a = 1;\n  int32 c = 2;\n  In in = 3;\n}\n"
        + "message In {\n  int32 x = 1;\n  int32 y = 2;\n}\n");
    ProtoProvenanceProjection projection = ProtoProvenanceProjection.of(reader, writer,
        mapping(Arrays.asList(p(1, "a"), p(3, "in"), p(4, "in", "x")),
            Arrays.asList(p(1, "a"), p(2, "c"), p(3, "in"), p(4, "in", "x"), p(5, "in", "y"))),
        false);
    Descriptor row = reader.toDescriptor();
    Descriptor in = row.findFieldByName("in").getMessageType();
    DynamicMessage plain = DynamicMessage.newBuilder(row).setField(row.findFieldByName("a"), 7)
        .setField(row.findFieldByName("in"), DynamicMessage.newBuilder(in)
            .setField(in.findFieldByName("x"), 8).build()).build();
    List<DynamicMessage> held = Arrays.asList(plain,
        plain.toBuilder().setField(row.findFieldByName("c"), 9).build(),
        plain.toBuilder().setField(row.findFieldByName("in"), DynamicMessage.newBuilder(in)
            .setField(in.findFieldByName("x"), 8).setField(in.findFieldByName("y"), 9).build())
            .build(),
        plain.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
            .addField(2, UnknownFieldSet.Field.newBuilder().addFixed32(9).build()).build())
            .build());
    for (DynamicMessage message : held) {
      DynamicMessage.Builder cleared = message.toBuilder();
      assertTrue(projection.clearMoved(cleared));
      assertEquals(plain, cleared.build());
    }
  }

  @Test
  public void aMovedOneofMemberBesideAKeptOneIsNotCleared() {
    // Last one wins: s may have been on the wire before u, so the reader's parse lost it.
    ProtobufSchema writer = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "message Row {\n  int32 a = 1;\n  int32 u = 4;\n  string s = 5;\n}\n");
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "message Row {\n  int32 a = 1;\n  oneof o {\n    int32 u = 4;\n    string s = 5;\n"
        + "  }\n}\n");
    ProtoProvenanceProjection projection = ProtoProvenanceProjection.of(reader, writer,
        mapping(Arrays.asList(p(1, "a"), p(2, "u"), p(4, "s")),
            Arrays.asList(p(1, "a"), p(3, "u"), p(4, "s"))), false);
    Descriptor row = reader.toDescriptor();
    assertFalse(projection.clearMoved(DynamicMessage.newBuilder(row)
        .setField(row.findFieldByName("u"), 5)));
    assertTrue(projection.clearMoved(DynamicMessage.newBuilder(row)
        .setField(row.findFieldByName("s"), "s")));
  }

  @Test
  public void theSingleParseEqualsTheParseLeavingTheMovedFieldsOut() throws Exception {
    // Each record parsed as the deserializer does, against the reader less its moved fields.
    String row = "syntax = \"proto3\";\npackage p;\nmessage Row {\n  int32 a = 1;\n%s}\n";
    byte[] a = varint(1, 7);
    assertSameParse(row, "  int32 x = 2;\n", "  int32 x = 2;\n",
        Arrays.asList(p(1, "a"), p(2, "x")), Arrays.asList(p(1, "a"), p(3, "x")),
        a, cat(a, varint(2, 5)), cat(a, text(2, "old")), cat(a, varint(2, 5), varint(2, 6)));
    assertSameParse(row, "", "  string added = 3;\n",
        Arrays.asList(p(1, "a")), Arrays.asList(p(1, "a"), p(3, "added")),
        a, cat(a, text(3, "beyond")), cat(a, bytes(3, new byte[] {(byte) 0xff})));
    List<ProvenanceField> into = Arrays.asList(p(1, "a"), p(3, "u"), p(4, "s"));
    for (String writer : new String[] {"  int32 u = 4;\n", "  int32 u = 4;\n  string s = 5;\n"}) {
      assertSameParse(row, writer, "  oneof o {\n    int32 u = 4;\n    string s = 5;\n  }\n",
          writer.contains("s") ? Arrays.asList(p(1, "a"), p(2, "u"), p(4, "s"))
              : Arrays.asList(p(1, "a"), p(2, "u")),
          into, a, cat(a, varint(4, 5)), cat(a, varint(4, 5), text(5, "s")),
          cat(a, text(5, "s"), varint(4, 5)));
    }
    String nested = "  In in = 2;\n  repeated In items = 3;\n  map<string, In> m = 4;\n}\n"
        + "message In {\n  int32 b = 1;\n  int32 z = 9;\n";
    byte[] z = cat(varint(1, 1), varint(9, 7));
    assertSameParse(row, nested, nested,
        Arrays.asList(p(1, "a"), p(2, "in"), p(5, "in", "z"), p(3, "items"),
            p(6, "items", "z"), p(4, "m"), p(7, "m", "value", "z")),
        Arrays.asList(p(1, "a"), p(2, "in"), p(8, "in", "z"), p(3, "items"),
            p(9, "items", "z"), p(4, "m"), p(10, "m", "value", "z")),
        a, cat(a, bytes(2, z)), cat(a, bytes(3, varint(1, 2)), bytes(3, z)),
        cat(a, bytes(4, cat(text(1, "k"), bytes(2, z)))));
  }

  @Test
  public void aKeptMessageMemberSplitByAMovedOneIsMergedAsTheProjectedParseMergesIt()
      throws Exception {
    // Two writer records concatenated, k {x: 1} u: 5 | k {y: 2}: the reader's own parse lets u,
    // new in k's oneof, displace k, and the second k then replaces it rather than merging.
    String row = "syntax = \"proto3\";\npackage p;\nmessage Row {\n  int32 a = 1;\n%s}\n";
    String in = "}\nmessage In {\n  int32 x = 1;\n  int32 y = 2;\n";
    assertSameParse(row, "  In k = 2;\n  int32 u = 4;\n" + in,
        "  oneof o {\n    In k = 2;\n    int32 u = 4;\n  }\n" + in,
        Arrays.asList(p(1, "a"), p(2, "k"), p(3, "k", "x"), p(4, "k", "y"), p(5, "u")),
        Arrays.asList(p(1, "a"), p(2, "k"), p(3, "k", "x"), p(4, "k", "y"), p(6, "u")),
        cat(varint(1, 7), bytes(2, varint(1, 1)), varint(4, 5), bytes(2, varint(2, 2))));
  }

  @Test
  public void theReaderLessItsMovedFieldsIsBuiltOnlyForARecordNeedingIt() throws Exception {
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "message Row {\n  int32 a = 1;\n  oneof o {\n    int32 u = 4;\n    string s = 5;\n"
        + "  }\n}\n");
    ProtoProvenanceProjection projection = ProtoProvenanceProjection.of(reader, null,
        mapping(Arrays.asList(p(1, "a"), p(4, "s")),
            Arrays.asList(p(1, "a"), p(3, "u"), p(4, "s"))), false);
    assertFalse(projection.prunedBuilt());
    for (int i = 0; i < 100; i++) {
      parse(projection, reader, cat(varint(1, i), text(5, "s")));
    }
    assertFalse(projection.prunedBuilt());
    parse(projection, reader, varint(4, 5));
    assertTrue(projection.prunedBuilt());

    ProtoProvenanceProjection unmoved = ProtoProvenanceProjection.of(READER, null,
        mapping(Arrays.asList(p(1, "a")), Arrays.asList(p(1, "a"))), false);
    assertTrue(unmoved.prunedBuilt());
    assertSame(READER, unmoved.schema());
  }

  // The writer is the reader with its moved fields under their writer pids.
  private static void assertSameParse(String row, String writerFields, String readerFields,
      List<ProvenanceField> writerLocations, List<ProvenanceField> readerLocations,
      byte[]... records) throws Exception {
    ProtobufSchema writer = new ProtobufSchema(String.format(row, writerFields));
    ProtobufSchema reader = new ProtobufSchema(String.format(row, readerFields));
    ProtoProvenanceProjection projection = ProtoProvenanceProjection.of(reader, writer,
        mapping(writerLocations, readerLocations), false);
    for (byte[] record : records) {
      Message pruned = DynamicMessage.parseFrom(projection.schema().toDescriptor(), record);
      Message expected = DynamicMessage.parseFrom(reader.toDescriptor(),
          projection.dropMoved(pruned).toByteString());
      assertEquals(readerFields, expected, parse(projection, reader, record));
    }
  }

  private static Message parse(ProtoProvenanceProjection projection, ProtobufSchema reader,
      byte[] record) throws Exception {
    return AbstractKafkaProtobufDeserializer.parseProjected(projection, reader,
        ByteBuffer.wrap(record), 0, record.length);
  }

  private static byte[] varint(int number, long value) {
    return field(number, UnknownFieldSet.Field.newBuilder().addVarint(value).build());
  }

  private static byte[] text(int number, String value) {
    return bytes(number, value.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] bytes(int number, byte[] value) {
    return field(number,
        UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFrom(value)).build());
  }

  private static byte[] field(int number, UnknownFieldSet.Field field) {
    return UnknownFieldSet.newBuilder().addField(number, field).build().toByteArray();
  }

  private static byte[] cat(byte[]... parts) {
    ByteString all = ByteString.EMPTY;
    for (byte[] part : parts) {
      all = all.concat(ByteString.copyFrom(part));
    }
    return all.toByteArray();
  }

  private static ProvenanceMapping mapping(List<ProvenanceField> writer,
      List<ProvenanceField> reader) {
    return ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, "STRUCT", writer),
        new ProvenanceVersion(2, 2, "STRUCT", reader))), 1, 2);
  }

  // The projection reads only pids and names; the path just has to be unique.
  private static ProvenanceField p(int pid, String... names) {
    return new ProvenanceField(Arrays.asList(pid), Arrays.asList(names), "SCALAR", pid);
  }
}
