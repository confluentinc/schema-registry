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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
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
            Arrays.asList(p(1, "a"), new ProvenanceField(Arrays.asList(2), null, 2))), false));
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
    assertNull(projection.schema.toDescriptor().findFieldByName("c"));
    assertEquals(1, projection.schema.toDescriptor().findFieldByName("a").getNumber());
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
    Descriptor row = projection.schema.toDescriptor();
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
        mapping(both, both), true).schema);
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
          mapping(locations, locations), true).schema);
    }
  }

  private static ProvenanceMapping mapping(List<ProvenanceField> writer,
      List<ProvenanceField> reader) {
    return ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, writer), new ProvenanceVersion(2, 2, reader))), 1, 2);
  }

  // The projection reads only pids and names; the path just has to be unique.
  private static ProvenanceField p(int pid, String... names) {
    return new ProvenanceField(Arrays.asList(pid), Arrays.asList(names), pid);
  }
}
