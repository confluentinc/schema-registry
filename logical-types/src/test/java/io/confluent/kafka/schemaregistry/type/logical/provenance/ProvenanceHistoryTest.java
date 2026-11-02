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
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** How a history's versions are read: each when the computation reaches it, and only once. */
class ProvenanceHistoryTest {

  @Test
  void missingVersionsAreTheNumbersBetweenTheFirstAndTheLast() {
    assertThat(ProvenanceHistory.missingVersions(List.of(2, 3, 6, 8))).containsExactly(4, 5, 7);
    assertThat(ProvenanceHistory.missingVersions(List.of(2, 3, 4))).isEmpty();
    assertThat(ProvenanceHistory.missingVersions(List.of(5))).isEmpty();
  }

  @Test
  void missingVersionsAreCappedWithATrailingZero() {
    int max = ProvenanceHistory.MAX_MISSING_VERSIONS;
    // Exactly the cap fits with no marker; one more is marked.
    assertThat(ProvenanceHistory.missingVersions(List.of(1, max + 2)))
        .containsExactlyElementsOf(range(2, max + 1));

    List<Integer> expected = new ArrayList<>(range(2, max));
    expected.add(0);
    assertThat(ProvenanceHistory.missingVersions(List.of(1, max + 3)))
        .containsExactlyElementsOf(expected);
  }

  private static List<Integer> range(int first, int last) {
    List<Integer> numbers = new ArrayList<>();
    for (int n = first; n <= last; n++) {
      numbers.add(n);
    }
    return numbers;
  }

  @Test
  void eachVersionIsParsedOnceAndOnlyWhenReached() {
    List<Integer> asked = new ArrayList<>();
    List<ParsedSchemaHolder> schemas = new ArrayList<>();
    schemas.add(counted(asked, 0, record("{\"name\":\"a\",\"type\":\"int\"}")));
    schemas.add(counted(asked, 1, record("{\"name\":\"a\",\"type\":\"int\"}",
        "{\"name\":\"b\",\"type\":\"int\"}")));
    schemas.add(counted(asked, 2, record("{\"name\":\"b\",\"type\":\"int\"}")));

    ProvenanceHistory.compute("s", history(3), schemas);
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

    assertThatThrownBy(() -> ProvenanceHistory.compute("s", history(3), schemas))
        .isInstanceOf(AmbiguousProvenanceException.class);
    assertThat(asked).containsExactly(0, 1);
  }

  @Test
  void aVersionWithNoLogicalFormOrARecursiveTypeIsNamed() {
    // v2 fails: a Struct field (no logical form, 42201) or a recursive message (42220). The
    // error names the version to fix, as an ambiguity or a limit already does.
    String v1 = "message R { int32 id = 1; }\n";
    String struct = "import \"google/protobuf/struct.proto\";\n"
        + "message R { int32 id = 1; google.protobuf.Struct meta = 2; }\n";
    String recursive = "message R { int32 id = 1; N n = 2; }\nmessage N { N next = 1; }\n";
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", protoHistory(2),
        ProvenanceHistory.held(List.of(proto(v1), proto(struct)))))
        .isInstanceOf(ValidationException.class).hasMessageStartingWith("Version 2: ");
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", protoHistory(2),
        ProvenanceHistory.held(List.of(proto(v1), proto(recursive)))))
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
        ProvenanceHistory.held(versions), true);
    SchemaProvenance ends = ProvenanceHistory.compute("s", history(4),
        ProvenanceHistory.held(versions), false);
    assertThat(ends.getVersions()).containsExactly(
        whole.getVersions().get(0), whole.getVersions().get(3));
  }

  // ---- A root that is only a reference to a type another subject defines ----

  private static final String O = "syntax = \"proto3\";\npackage q;\n"
      + "message O {\n  int32 x = 1;\n  string y = 2;\n%s}\n";

  @Test
  void aVersionReExportingItsRootMessageContinuesItsFields() {
    // The file declares nothing but a public import, as the logical-type writer registers an
    // external root: its records carry O's fields, so they continue on both sides.
    String o = String.format(O, "");
    SchemaProvenance p = ProvenanceHistory.compute("s", protoHistory(3), ProvenanceHistory.held(
        List.of(new ProtobufSchema(o), reExport(o), new ProtobufSchema(String.format(O,
            "  int64 z = 3;\n")))));
    assertThat(kinds(p)).containsExactly("STRUCT", "STRUCT", "STRUCT");
    assertThat(pids(p)).containsExactly(List.of(1, 2), List.of(1, 2), List.of(1, 2, 3));
  }

  @Test
  void aHistoryOfOnlyReExportsFollowsTheMessageReExported() {
    // y is dropped, then its number reused by w: w is new, x continues throughout.
    SchemaProvenance p = ProvenanceHistory.compute("s", protoHistory(3), ProvenanceHistory.held(
        List.of(reExport(String.format(O, "")), reExport(String.format(O, "").replace(
            "  string y = 2;\n", "")), reExport(String.format(O, "").replace("string y",
            "string w")))));
    assertThat(pids(p)).containsExactly(List.of(1, 2), List.of(1), List.of(1, 3));
  }

  @Test
  void aReExportingVersionIsComputedInMultiMessageMode() {
    String o = String.format(O, "");
    SchemaProvenance p = ProvenanceHistory.compute("s", protoHistory(2), ProvenanceHistory.held(
        List.of(new ProtobufSchema(o), reExport(o))));
    assertThat(pids(p).get(1)).isNotEmpty().isEqualTo(pids(p).get(0));
  }

  @Test
  void aChainOfReExportsHasNoLogicalForm() {
    // Protobuf resolves one level of re-export only, so a serializer cannot use this either.
    String o = String.format(O, "");
    String inner = "syntax = \"proto3\";\nimport public \"o.proto\";\n";
    String outer = "syntax = \"proto3\";\nimport public \"w.proto\";\n";
    ProtobufSchema chain = new ProtobufSchema(outer,
        List.of(new SchemaReference("w.proto", "w", 1), new SchemaReference("o.proto", "o", 1)),
        Map.of("w.proto", inner, "o.proto", o), null, null);
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", protoHistory(1),
        ProvenanceHistory.held(List.of(chain))))
        .isInstanceOf(ValidationException.class).hasMessageStartingWith("Version 1: ");
  }

  @Test
  void aReExportedFileImportingAWellKnownMessageContinuesAcrossBothForms() {
    // Any and Duration are inlined in the file itself, so they must be in its re-export too:
    // converted through a descriptor, they became named types and their members restarted.
    String o = "syntax = \"proto3\";\npackage q;\nimport \"google/protobuf/any.proto\";\n"
        + "import \"google/protobuf/duration.proto\";\nmessage O {\n  int32 x = 1;\n"
        + "  repeated google.protobuf.Any a = 2;\n  google.protobuf.Duration d = 3;\n}\n";
    for (boolean multi : new boolean[] {false, true}) {
      SchemaProvenance p = ProvenanceHistory.compute("s", protoHistory(3), ProvenanceHistory.held(
          List.of(new ProtobufSchema(o), reExport(o), new ProtobufSchema(o))));
      List<List<Integer>> pids = pids(p);
      assertThat(pids.get(1)).as("multi=" + multi).isEqualTo(pids.get(0));
      assertThat(pids.get(2)).as("multi=" + multi).isEqualTo(pids.get(0));
    }
  }

  @Test
  void aReExportOfOnlyEnumsHasNoLogicalForm() {
    // As the file it re-exports: no message, so no root.
    String e = "syntax = \"proto3\";\npackage q;\nenum E {\n  A = 0;\n}\n";
    for (ProtobufSchema version : List.of(new ProtobufSchema(e), reExport(e))) {
      assertThatThrownBy(() -> ProvenanceHistory.compute("s", protoHistory(1),
          ProvenanceHistory.held(List.of(version))))
          .isInstanceOf(ValidationException.class).hasMessageStartingWith("Version 1: ");
    }
  }

  @Test
  void aReExportOfABuiltInFileImportingAnotherContinuesItsMembers() {
    // type.proto imports any.proto, api.proto type.proto, datetime.proto duration.proto: as in the
    // file registered directly, those imports are inlined, so their members continue.
    for (String file : List.of("google/protobuf/type.proto", "google/protobuf/api.proto",
        "google/type/datetime.proto")) {
      ProtobufSchema wrapper = reExportBuiltIn(file);
      ProtobufSchema direct = new ProtobufSchema(
          new ProtobufSchema(wrapper.toDescriptor()).canonicalString());
      for (boolean multi : new boolean[] {false, true}) {
        SchemaProvenance p = ProvenanceHistory.compute("s", protoHistory(2),
            ProvenanceHistory.held(List.of(direct, wrapper)));
        assertThat(p.getVersions().get(1).getFields()).as(file + " multi=" + multi)
            .isEqualTo(p.getVersions().get(0).getFields());
      }
    }
  }

  @Test
  void aReExportOfABuiltInFileOfOnlyEnumsHasNoLogicalForm() {
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", protoHistory(1),
        ProvenanceHistory.held(List.of(reExportBuiltIn("google/type/dayofweek.proto")))))
        .isInstanceOf(ValidationException.class).hasMessageStartingWith("Version 1: ");
  }

  @Test
  void anAvroRootNamingAReferencedRecordContinuesItsFields() {
    String o = "{\"type\":\"record\",\"name\":\"O\",\"namespace\":\"q\",\"fields\":["
        + "{\"name\":\"x\",\"type\":\"int\"},{\"name\":\"y\",\"type\":\"string\"}%s]}";
    String plain = String.format(o, "");
    AvroSchema bare = new AvroSchema("\"q.O\"",
        List.of(new SchemaReference("o.avsc", "o", 1)), Map.of("o.avsc", plain), null);
    SchemaProvenance p = ProvenanceHistory.compute("s", history(3), ProvenanceHistory.held(List.of(
        new AvroSchema(plain), bare, new AvroSchema(String.format(o,
            ",{\"name\":\"z\",\"type\":\"long\",\"default\":0}")))));
    assertThat(kinds(p)).containsExactly("STRUCT", "STRUCT", "STRUCT");
    assertThat(pids(p)).containsExactly(List.of(1, 2), List.of(1, 2), List.of(1, 2, 3));
  }

  @Test
  void aJsonRootReferencingAnotherSchemaContinuesItsProperties() {
    String o = "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"integer\"},"
        + "\"y\":{\"type\":\"string\"}%s}}";
    JsonSchema bare = new JsonSchema("{\"$ref\":\"o.json\"}",
        List.of(new SchemaReference("o.json", "o", 1)),
        Map.of("o.json", String.format(o, "")), null);
    SchemaProvenance p = ProvenanceHistory.compute("s", jsonHistory(3), ProvenanceHistory.held(
        List.of(new JsonSchema(String.format(o, "")), bare, new JsonSchema(String.format(o,
            ",\"z\":{\"type\":\"number\"}")))));
    assertThat(kinds(p)).containsExactly("STRUCT", "STRUCT", "STRUCT");
    assertThat(pids(p)).containsExactly(List.of(1, 2), List.of(1, 2), List.of(1, 2, 3));
  }

  private static ProtobufSchema reExport(String o) {
    return new ProtobufSchema("syntax = \"proto3\";\nimport public \"o.proto\";\n",
        Collections.singletonList(new SchemaReference("o.proto", "o", 1)),
        Collections.singletonMap("o.proto", o), null, null);
  }

  // A file declaring nothing but a public import of a file Protobuf bundles.
  private static ProtobufSchema reExportBuiltIn(String file) {
    return new ProtobufSchema("syntax = \"proto3\";\nimport public \"" + file + "\";\n");
  }

  private static List<List<Integer>> pids(SchemaProvenance provenance) {
    return provenance.getVersions().stream().map(v -> v.getFields() == null
        ? Collections.<Integer>emptyList()
        : v.getFields().stream().map(ProvenanceField::getPid).collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  private static List<String> kinds(SchemaProvenance provenance) {
    return provenance.getVersions().stream().map(ProvenanceVersion::getKind)
        .collect(Collectors.toList());
  }

  private static List<SchemaMetadata> jsonHistory(int versions) {
    List<SchemaMetadata> history = new ArrayList<>();
    for (int i = 1; i <= versions; i++) {
      history.add(new SchemaMetadata(i, i, "JSON", Collections.emptyList(), ""));
    }
    return history;
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
