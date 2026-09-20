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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceAlgorithm;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** How a history's versions are read: each when the computation reaches it, and only once. */
class ProvenanceHistoryTest {

  @Test
  void eachVersionIsParsedOnceAndOnlyWhenReached() {
    List<Integer> asked = new ArrayList<>();
    List<ParsedSchemaHolder> schemas = new ArrayList<>();
    schemas.add(counted(asked, 0, record("{\"name\":\"a\",\"type\":\"int\"}")));
    schemas.add(counted(asked, 1, record("{\"name\":\"a\",\"type\":\"int\"}",
        "{\"name\":\"b\",\"type\":\"int\"}")));
    schemas.add(counted(asked, 2, record("{\"name\":\"b\",\"type\":\"int\"}")));

    ProvenanceHistory.compute("s", history(3), schemas, false);
    assertThat(asked).containsExactly(0, 1, 2);
  }

  @Test
  void aFailureIsTheFirstInVersionOrder() {
    // v2 continues a twice, by two aliases: ambiguous. v3 is never reached, so its own fault
    // is not the answer, though a computation converting every version first would give it.
    List<Integer> asked = new ArrayList<>();
    List<ParsedSchemaHolder> schemas = new ArrayList<>();
    schemas.add(counted(asked, 0, record("{\"name\":\"a\",\"type\":\"int\"}")));
    schemas.add(counted(asked, 1, record(
        "{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]}",
        "{\"name\":\"c\",\"type\":\"int\",\"aliases\":[\"a\"]}")));
    schemas.add(new ParsedSchemaHolder() {
      @Override
      public ParsedSchema schema() {
        throw new IllegalStateException("parsed though never reached");
      }

      @Override
      public void clear() {
      }
    });

    assertThatThrownBy(() -> ProvenanceHistory.compute("s", history(3), schemas, false))
        .isInstanceOf(AmbiguousProvenanceException.class);
    assertThat(asked).containsExactly(0, 1);
  }

  @Test
  void aVersionWithNoLogicalFormOrARecursiveTypeIsNamed() {
    // v2 fails: a Struct field (no logical form, 42201) or a recursive message (42213). The
    // error names the version to fix, as an ambiguity or a limit already does.
    String v1 = "message R { int32 id = 1; }\n";
    String struct = "import \"google/protobuf/struct.proto\";\n"
        + "message R { int32 id = 1; google.protobuf.Struct meta = 2; }\n";
    String recursive = "message R { int32 id = 1; N n = 2; }\nmessage N { N next = 1; }\n";
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", protoHistory(2),
        ProvenanceHistory.held(List.of(proto(v1), proto(struct))), false))
        .isInstanceOf(ValidationException.class).hasMessageStartingWith("Version 2: ");
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", protoHistory(2),
        ProvenanceHistory.held(List.of(proto(v1), proto(recursive))), false))
        .isInstanceOf(RecursiveTypeException.class).hasMessageStartingWith("Version 2: ");
  }

  @Test
  void theEndsAlonePairAsTheWholeRangeDoes() {
    // a is dropped at v2 and re-added at v4: computed without keeping the interior, the ends
    // still carry every version's effect.
    List<ParsedSchema> versions = new ArrayList<>();
    versions.add(record("{\"name\":\"a\",\"type\":\"int\"}",
        "{\"name\":\"b\",\"type\":\"int\"}"));
    versions.add(record("{\"name\":\"b\",\"type\":\"int\"}"));
    versions.add(record("{\"name\":\"b\",\"type\":\"long\"}"));
    versions.add(record("{\"name\":\"a\",\"type\":\"int\"}",
        "{\"name\":\"b\",\"type\":\"long\"}"));

    SchemaProvenance whole = ProvenanceHistory.compute("s", history(4),
        ProvenanceHistory.held(versions), false, true, ProvenanceAlgorithm.LATEST_NAME);
    SchemaProvenance ends = ProvenanceHistory.compute("s", history(4),
        ProvenanceHistory.held(versions), false, false, ProvenanceAlgorithm.LATEST_NAME);
    assertThat(ends.getVersions()).containsExactly(
        whole.getVersions().get(0), whole.getVersions().get(3));
  }

  private static ParsedSchemaHolder counted(List<Integer> asked, int index, ParsedSchema schema) {
    return new ParsedSchemaHolder() {
      @Override
      public ParsedSchema schema() {
        asked.add(index);
        return schema;
      }

      @Override
      public void clear() {
      }
    };
  }

  private static List<SchemaMetadata> history(int versions) {
    List<SchemaMetadata> history = new ArrayList<>();
    for (int i = 1; i <= versions; i++) {
      history.add(new SchemaMetadata(i, i, "AVRO", Collections.emptyList(), ""));
    }
    return history;
  }

  private static List<SchemaMetadata> protoHistory(int versions) {
    List<SchemaMetadata> history = new ArrayList<>();
    for (int i = 1; i <= versions; i++) {
      history.add(new SchemaMetadata(i, i, "PROTOBUF", Collections.emptyList(), ""));
    }
    return history;
  }

  private static ProtobufSchema proto(String messages) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n" + messages);
  }

  private static AvroSchema record(String... fields) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + String.join(",", fields) + "]}");
  }
}
