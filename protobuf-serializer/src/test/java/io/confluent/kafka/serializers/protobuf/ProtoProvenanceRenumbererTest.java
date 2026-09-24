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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
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
    assertThrows(SerializationException.class, () -> ProtoProvenanceRenumberer.renumber(READER,
        mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), new ProvenanceField(Arrays.asList(2), null, 2))), false));
  }

  @Test
  public void aLocationNotInTheReaderFailsEveryRecord() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> ProtoProvenanceRenumberer.renumber(READER, mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), p(2, "ghost"))), false));
    assertTrue(e.getMessage(), e.getMessage().contains("[ghost] of schema id 2"));
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
