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

package io.confluent.kafka.schemaregistry.type.logical.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceComputer;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceReport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * {@link FieldPairing#byIds}: members paired by their locations' provenance check ids, against
 * {@link FieldPairing#byName()}, under Iceberg and Flink rules.
 */
class FieldPairingTest {

  private static final String INT = "{\"type\":\"integer\"}";

  // --- A rename continues -----------------------------------------------------------------------

  @Test
  void anAvroRenameIsOneColumnByIds() {
    History h = history(
        avro("{\"name\":\"a\",\"type\":\"int\"}"),
        avro("{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]}"));
    assertThat(h.byName(Mode.ICEBERG_V2, 0, 1))
        .containsExactlyInAnyOrder("FIELD_DELETED@a", "REQUIRED_FIELD_ADDED@b");
    assertThat(h.byName(Mode.FLINK, 0, 1)).containsExactly("REQUIRED_FIELD_ADDED@b");
    assertThat(h.byIds(Mode.ICEBERG_V2, 0, 1)).isEmpty();
    assertThat(h.byIds(Mode.FLINK, 0, 1)).isEmpty();
  }

  @Test
  void aProtobufRenameIsOneColumnByIds() {
    // JSON has no renames: a property's name is its identity.
    History h = history(proto("int32 a = 1;"), proto("int32 b = 1;"));
    assertThat(h.byName(Mode.ICEBERG_V2, 0, 1)).contains("FIELD_DELETED@a");
    assertThat(h.byIds(Mode.ICEBERG_V2, 0, 1)).isEmpty();
    assertThat(h.byIds(Mode.FLINK, 0, 1)).isEmpty();
  }

  // --- One name, another id -----------------------------------------------------------------

  @Test
  void aRenumberedProtobufFieldIsADeletionAndAnAdditionByIds() {
    History h = history(proto("int32 a = 1;"), proto("int32 a = 2;"));
    assertThat(h.byName(Mode.ICEBERG_V2, 0, 1)).isEmpty();
    assertThat(h.byIds(Mode.ICEBERG_V2, 0, 1)).contains("FIELD_DELETED@a");
  }

  @Test
  void aFieldDroppedAndReAddedIsANewColumnAcrossTheGap() {
    // v0 against v2, as a transitive level compares them: by name a continues, by ids it is new.
    List<History> histories = Arrays.asList(
        history(avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"k\",\"type\":\"int\"}"),
            avro("{\"name\":\"k\",\"type\":\"int\"}"),
            avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"k\",\"type\":\"int\"}")),
        history(proto("int32 a = 1;\n  int32 k = 2;"), proto("int32 k = 2;"),
            proto("int32 a = 1;\n  int32 k = 2;")),
        history(json("{\"a\":" + INT + ",\"k\":" + INT + "}"), json("{\"k\":" + INT + "}"),
            json("{\"a\":" + INT + ",\"k\":" + INT + "}")));
    for (History h : histories) {
      assertThat(h.byName(Mode.ICEBERG_V2, 0, 2)).as(h.type).isEmpty();
      assertThat(h.byIds(Mode.ICEBERG_V2, 0, 2)).as(h.type).contains("FIELD_DELETED@a");
    }
  }

  // --- A kind change pairs by check id --------------------------------------------------------

  @Test
  void aKindChangeIsATypeChangeByCheckIdsAndADropByPids() {
    List<History> histories = Arrays.asList(
        history(avro("{\"name\":\"f\",\"type\":\"int\"}"),
            avro("{\"name\":\"f\",\"type\":{\"type\":\"record\",\"name\":\"M\","
                + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}}")),
        history(proto("int32 f = 1;"), proto("M f = 1;\n  message M { int32 a = 1; }")),
        history(json("{\"f\":" + INT + "}"),
            json("{\"f\":{\"type\":\"object\",\"properties\":{\"a\":" + INT + "}}}")));
    for (History h : histories) {
      for (Mode mode : new Mode[] {Mode.ICEBERG_V2, Mode.FLINK}) {
        assertThat(h.byIds(mode, 0, 1)).as("%s %s", h.type, mode)
            .containsExactly("TYPE_MISMATCH@f")
            .isEqualTo(h.byName(mode, 0, 1));
      }
      assertThat(h.byPids(Mode.ICEBERG_V2, 0, 1)).as(h.type).contains("FIELD_DELETED@f");
    }
  }

  // --- Branches and collections ---------------------------------------------------------------

  @Test
  void anAvroBranchRenamedByAnAliasIsOneBranchByIds() {
    String a = "{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    String b = "{\"type\":\"record\",\"name\":\"B\",\"aliases\":[\"A\"],"
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    // A second branch keeps a union: [null, A] alone is a nullable A.
    History h = history(avro("{\"name\":\"u\",\"type\":[\"null\"," + a + ",\"string\"]}"),
        avro("{\"name\":\"u\",\"type\":[\"null\"," + b + ",\"string\"]}"));
    assertThat(h.byName(Mode.ICEBERG_V2, 0, 1)).contains("FIELD_DELETED@u.A");
    assertThat(h.byIds(Mode.ICEBERG_V2, 0, 1)).isEmpty();
  }

  @Test
  void membersUnderACollectionPairByIds() {
    // A field renamed inside a list's element and a map's value, keeping its alias or number.
    String m = "{\"type\":\"record\",\"name\":\"M\",\"fields\":[{\"name\":\"%s\",\"type\":\"int\""
        + "%s}]}";
    String before = String.format(m, "a", "");
    String after = String.format(m, "b", ",\"aliases\":[\"a\"]");
    List<History> histories = Arrays.asList(
        history(avro("{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":" + before + "}}"),
            avro("{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":" + after + "}}")),
        history(avro("{\"name\":\"v\",\"type\":{\"type\":\"map\",\"values\":" + before + "}}"),
            avro("{\"name\":\"v\",\"type\":{\"type\":\"map\",\"values\":" + after + "}}")),
        history(proto("repeated M l = 1;\n  message M { int32 a = 1; }"),
            proto("repeated M l = 1;\n  message M { int32 b = 1; }")),
        history(proto("map<string, M> v = 1;\n  message M { int32 a = 1; }"),
            proto("map<string, M> v = 1;\n  message M { int32 b = 1; }")));
    for (History h : histories) {
      assertThat(h.byName(Mode.ICEBERG_V2, 0, 1)).as(h.type)
          .anyMatch(f -> f.startsWith("FIELD_DELETED@") && f.endsWith(".a"));
      assertThat(h.byIds(Mode.ICEBERG_V2, 0, 1)).as(h.type).isEmpty();
      assertThat(h.byIds(Mode.FLINK, 0, 1)).as(h.type).isEmpty();
    }
  }

  // --- Shared named types ---------------------------------------------------------------------

  @Test
  void aTypeUsedTwiceIsComparedPerUseByIds() {
    // w's type changes and changes back, so its members are new at v2 while h's continue: by
    // name the pair (A, A) is compared once, at h; by ids w's use is compared on its own.
    String a = "{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    String b = "{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    String file = "syntax = \"proto3\";\npackage p;\n" + "import \"confluent/meta.proto\";\noption (confluent.file_meta) = { params: [ { key: \"root\", value: \"p.Row\" } ] };\n"
        + "message Row {\n  A h = 1;\n  %s w = 2;\n}\n"
        + "message A {\n  int32 x = 1;\n}\nmessage B {\n  int32 x = 1;\n}\n";
    String ref = "{\"$ref\":\"#/definitions/A\"}";
    String defs = ",\"definitions\":{\"A\":{\"type\":\"object\",\"properties\":{\"x\":" + INT
        + "}}}";
    List<History> histories = Arrays.asList(
        history(avro("{\"name\":\"h\",\"type\":" + a + "},{\"name\":\"w\",\"type\":\"A\"}"),
            avro("{\"name\":\"h\",\"type\":" + a + "},{\"name\":\"w\",\"type\":" + b + "}"),
            avro("{\"name\":\"h\",\"type\":" + a + "},{\"name\":\"w\",\"type\":\"A\"}")),
        history(new ProtobufSchema(String.format(file, "A")),
            new ProtobufSchema(String.format(file, "B")),
            new ProtobufSchema(String.format(file, "A"))),
        history(jsonWith("{\"h\":" + ref + ",\"w\":" + ref + "}", defs),
            jsonWith("{\"h\":" + ref + ",\"w\":{\"type\":\"object\",\"properties\":{}}}", defs),
            jsonWith("{\"h\":" + ref + ",\"w\":" + ref + "}", defs)));
    for (History h : histories) {
      assertThat(h.byName(Mode.ICEBERG_V2, 0, 2)).as(h.type).isEmpty();
      assertThat(h.byIds(Mode.ICEBERG_V2, 0, 2)).as(h.type)
          .contains("FIELD_DELETED@w.x").noneMatch(f -> f.contains("@h"));
    }
  }

  // --- The ids must cover the walk ------------------------------------------------------------

  @Test
  void aMemberWithNoIdIsAnInconsistency() {
    History h = history(avro("{\"name\":\"a\",\"type\":\"int\"}"),
        avro("{\"name\":\"a\",\"type\":\"long\"}"));
    FieldPairing none = FieldPairing.byIds(Collections.emptyMap(), Collections.emptyMap());
    assertThatThrownBy(() -> LogicalTypeChecker.compare(Mode.ICEBERG_V2, h.versions.get(0),
        h.versions.get(1), none))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("No id");
  }

  @Test
  void twoMembersOfOneStructWithOneIdAreAnInconsistencyOnEitherSide() {
    History h = history(
        avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}"),
        avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}"));
    Map<List<Integer>, Integer> distinct = Map.of(List.of(0), 1, List.of(1), 2);
    Map<List<Integer>, Integer> shared = Map.of(List.of(0), 1, List.of(1), 1);
    for (FieldPairing pairing : Arrays.asList(FieldPairing.byIds(shared, distinct),
        FieldPairing.byIds(distinct, shared))) {
      assertThatThrownBy(() -> LogicalTypeChecker.compare(Mode.ICEBERG_V2, h.versions.get(0),
          h.versions.get(1), pairing))
          .isInstanceOf(IllegalStateException.class).hasMessageContaining("Two members");
    }
  }

  // -------------------------------------------------------------------------------------------

  /** A subject's versions as logical types, and their provenance report. */
  private static final class History {
    final String type;
    final List<LogicalType> versions;
    final ProvenanceReport report;

    History(String type, List<LogicalType> versions, ProvenanceReport report) {
      this.type = type;
      this.versions = versions;
      this.report = report;
    }

    List<String> byName(Mode mode, int writer, int reader) {
      return findings(mode, writer, reader, FieldPairing.byName());
    }

    List<String> byIds(Mode mode, int writer, int reader) {
      return findings(mode, writer, reader, FieldPairing.byIds(
          report.getVersions().get(writer).checkIds(),
          report.getVersions().get(reader).checkIds()));
    }

    List<String> byPids(Mode mode, int writer, int reader) {
      return findings(mode, writer, reader,
          FieldPairing.byIds(pids(writer), pids(reader)));
    }

    private Map<List<Integer>, Integer> pids(int version) {
      Map<List<Integer>, Integer> pids = new HashMap<>();
      report.getVersions().get(version).getMembers()
          .forEach(m -> pids.put(m.getPath(), m.getId()));
      return pids;
    }

    private List<String> findings(Mode mode, int writer, int reader, FieldPairing pairing) {
      return LogicalTypeChecker.compare(mode, versions.get(writer), versions.get(reader), pairing)
          .getIncompatibilities().stream()
          .map(f -> f.getRule() + "@" + f.getPath())
          .collect(Collectors.toList());
    }
  }

  private static History history(ParsedSchema... schemas) {
    List<LogicalType> versions = new ArrayList<>();
    for (ParsedSchema schema : schemas) {
      versions.add(ProvenanceHistory.logicalTypeOf(schema));
    }
    String type = schemas[0].schemaType();
    ProvenanceReport report = ProvenanceComputer.report(
        Collections.nCopies(versions.size(), SchemaType.of(type)), versions::get, i -> true);
    return new History(type, versions, report);
  }

  private static AvroSchema avro(String fields) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + fields + "]}");
  }

  private static ProtobufSchema proto(String body) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage Row {\n  " + body
        + "\n}\n");
  }

  private static JsonSchema json(String properties) {
    return jsonWith(properties, "");
  }

  private static JsonSchema jsonWith(String properties, String rest) {
    return new JsonSchema("{\"type\":\"object\",\"properties\":" + properties + rest + "}");
  }
}
