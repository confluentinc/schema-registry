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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
public class ProtoProvenanceRenumbererTest {

  private static final ProtobufSchema READER = new ProtobufSchema(
      "syntax = \"proto3\";\npackage p;\nmessage Row {\n  int32 a = 1;\n}\n");

  @Test
  public void aLocationWithoutNamesFailsEveryRecord() {
    assertThrows(SerializationException.class, () -> ProtoProvenanceRenumberer.renumber(READER, null,
        mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), new ProvenanceField(Arrays.asList(2), null, 2))), false));
  }

  @Test
  public void aLocationNotInTheReaderFailsEveryRecord() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> ProtoProvenanceRenumberer.renumber(READER, null, mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), p(2, "ghost"))), false));
    assertTrue(e.getMessage(), e.getMessage().contains("[ghost] of schema id 2"));
  }

  @Test(timeout = 2000)
  public void aFreshNumberJumpsAnExtensionRangeToTheMaximum() {
    // Stepping through the range number by number takes seconds.
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto2\";\npackage p;\nmessage Row {\n"
        + "  optional int32 a = 1;\n  optional string c = 2;\n  extensions 100 to max;\n}\n");
    ProtoProvenanceRenumberer.Renumbered renumbered = ProtoProvenanceRenumberer.renumber(reader, null,
        mapping(Arrays.asList(p(1, "a")), Arrays.asList(p(1, "a"), p(2, "c"))), false);
    assertEquals(99, renumbered.schema.toDescriptor().findFieldByName("c").getNumber());
  }

  @Test
  public void aFreshNumberIsNeverOneTheImplementationReserves() {
    ProtobufSchema reader = new ProtobufSchema("syntax = \"proto2\";\npackage p;\nmessage Row {\n"
        + "  optional int32 a = 1;\n  optional string c = 2;\n  extensions 20000 to max;\n}\n");
    ProtoProvenanceRenumberer.Renumbered renumbered = ProtoProvenanceRenumberer.renumber(reader, null,
        mapping(Arrays.asList(p(1, "a")), Arrays.asList(p(1, "a"), p(2, "c"))), false);
    assertEquals(18_999, renumbered.schema.toDescriptor().findFieldByName("c").getNumber());
  }

  @Test
  public void aNestedRecordMessageNoLocationReachesHasNoProvenance() {
    // Locations start at the file's top-level messages: A.Inner, used by no field, has none, so
    // a record of it is read without provenance rather than silently as written.
    String file = "syntax = \"proto3\";\npackage p;\nmessage A {\n  int32 id = 1;\n%s"
        + "  message Inner {\n    string memo = 1;\n  }\n}\n";
    ProtobufSchema unused = new ProtobufSchema(String.format(file, ""));
    ProtobufSchema reader = new ProtobufSchema(unused.toDescriptor("p.A.Inner"));
    assertThrows(ProvenanceUnavailableException.class, () -> ProtoProvenanceRenumberer.renumber(
        reader, unused, mapping(Arrays.asList(p(1, "p.A", "id")),
            Arrays.asList(p(1, "p.A", "id"))), true));

    // Used by a field, it is reached through it.
    ProtobufSchema used = new ProtobufSchema(String.format(file, "  Inner inner = 2;\n"));
    ProtobufSchema usedReader = new ProtobufSchema(used.toDescriptor("p.A.Inner"));
    List<ProvenanceField> both = Arrays.asList(p(1, "p.A", "id"), p(2, "p.A", "inner"),
        p(3, "p.A", "inner", "memo"));
    assertEquals(usedReader, ProtoProvenanceRenumberer.renumber(usedReader, used,
        mapping(both, both), true).schema);
  }

  private static ProvenanceMapping mapping(List<ProvenanceField> writer,
      List<ProvenanceField> reader) {
    return ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, writer), new ProvenanceVersion(2, 2, reader))), 1, 2);
  }

  // The renumberer reads only pids and names; the path just has to be unique.
  private static ProvenanceField p(int pid, String... names) {
    return new ProvenanceField(Arrays.asList(pid), Arrays.asList(names), pid);
  }
}
