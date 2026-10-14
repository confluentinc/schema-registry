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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Check ids: what the logical compatibility checks pair by. They follow pids, but a location new
 * only by a kind change, and everything under it, keeps the check id of what it would continue.
 */
class ProvenanceCheckIdTest {

  private static final String INT = "{\"type\":\"integer\"}";
  private static final String OBJ_A = "{\"type\":\"object\",\"properties\":{\"a\":" + INT + "}}";
  private static final String RECORD_M = "{\"type\":\"record\",\"name\":\"M\","
      + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}";

  // --- No kind change: check ids pair as pids do ----------------------------------------------

  @Test
  void whereNoKindChangesCheckIdsPairAsPids() {
    // Renames, drops, re-adds and renumbers restart or continue both ids alike.
    assertPairAlike(report(
        avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"string\"}"),
        avro("{\"name\":\"c\",\"type\":\"long\",\"aliases\":[\"a\"]},"
            + "{\"name\":\"d\",\"type\":\"int\"}"),
        avro("{\"name\":\"c\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}")));
    assertPairAlike(report(
        proto("int32 a = 1;\n  string b = 2;"),
        proto("int32 a = 3;\n  string bb = 2;"),
        proto("int64 a = 3;")));
    assertPairAlike(report(
        json("{\"a\":" + INT + ",\"b\":" + OBJ_A + "}"),
        json("{\"b\":" + OBJ_A + ",\"c\":" + INT + "}"),
        json("{\"a\":" + INT + "}")));
  }

  // --- A kind change keeps the check id --------------------------------------------------------

  @Test
  void anAvroFieldChangingKindKeepsItsCheckId() {
    ProvenanceReport r = report(
        avro("{\"name\":\"f\",\"type\":\"int\"}"),
        avro("{\"name\":\"f\",\"type\":" + RECORD_M + "}"),
        avro("{\"name\":\"f\",\"type\":\"int\"}"));
    assertKindChangeKeepsCheckId(r, 0);
  }

  @Test
  void aProtobufFieldChangingKindKeepsItsCheckId() {
    ProvenanceReport r = report(
        proto("int32 f = 1;"),
        proto("M f = 1;\n  message M { int32 a = 1; }"),
        proto("int32 f = 1;"));
    assertKindChangeKeepsCheckId(r, 0);
  }

  @Test
  void aJsonPropertyChangingKindKeepsItsCheckId() {
    ProvenanceReport r = report(
        json("{\"f\":" + INT + "}"),
        json("{\"f\":" + OBJ_A + "}"),
        json("{\"f\":" + INT + "}"));
    assertKindChangeKeepsCheckId(r, 0);
  }

  @Test
  void aOneBranchJsonUnionUnwrappedKeepsItsCheckId() {
    ProvenanceReport r = report(
        json("{\"x\":{\"oneOf\":[" + INT + "]}}"), json("{\"x\":" + INT + "}"));
    assertThat(member(r, 1, 0).getId()).isNotEqualTo(member(r, 0, 0).getId());
    assertThat(member(r, 1, 0).getCheckId()).isEqualTo(member(r, 0, 0).getCheckId());
  }

  @Test
  void aCollectionWhoseElementsChangeKindKeepsItsCheckId() {
    List<ProvenanceReport> reports = Arrays.asList(
        report(json("{\"x\":{\"type\":\"array\",\"items\":" + OBJ_A + "}}"),
            json("{\"x\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}")),
        report(proto("repeated M x = 1;\n  message M { int32 a = 1; }"),
            proto("repeated int32 x = 1;")),
        report(avro("{\"name\":\"x\",\"type\":{\"type\":\"map\",\"values\":" + RECORD_M + "}}"),
            avro("{\"name\":\"x\",\"type\":{\"type\":\"map\",\"values\":\"int\"}}")));
    for (ProvenanceReport r : reports) {
      assertThat(member(r, 1, 0).getId()).isNotEqualTo(member(r, 0, 0).getId());
      assertThat(member(r, 1, 0).getCheckId()).isEqualTo(member(r, 0, 0).getCheckId());
    }
  }

  @Test
  void anAvroFieldRenamedAsItChangesKindPairsWithTheNameItLeft() {
    // b [a] takes a and restarts; the a beside it is new to both ids.
    ProvenanceReport r = report(
        avro("{\"name\":\"a\",\"type\":\"int\"}"),
        avro("{\"name\":\"b\",\"type\":" + RECORD_M + ",\"aliases\":[\"a\"]},"
            + "{\"name\":\"a\",\"type\":\"int\"}"));
    assertRenamedKindChange(r);
  }

  @Test
  void aProtobufFieldRenamedAsItChangesKindPairsByItsNumber() {
    // JSON has no renames: a property's name is its identity.
    ProvenanceReport r = report(
        proto("int32 a = 1;"),
        proto("M b = 1;\n  int32 a = 2;\n  message M { int32 x = 1; }"));
    assertRenamedKindChange(r);
  }

  // --- Under a kind change ---------------------------------------------------------------------

  @Test
  void membersUnderAKindChangePairInEveryFormat() {
    // ARRAY<STRUCT> -> MULTISET<STRUCT> keeps the element's step, so its members pair (an
    // element is a collection step, not a location: a is at [0, 0, 0]).
    for (SchemaType type : SchemaType.values()) {
      ProvenanceReport r = Pids.report(type, Arrays.asList(
          lt(field(type, "x", 1, Schema.createArray(struct(field(type, "a", 1, null))))),
          lt(field(type, "x", 1, Schema.createMultiset(struct(field(type, "a", 1, null)))))));
      for (Integer[] path : new Integer[][] {{0}, {0, 0, 0}}) {
        assertThat(member(r, 1, path).getId()).as("%s %s", type, path)
            .isNotEqualTo(member(r, 0, path).getId());
        assertThat(member(r, 1, path).getCheckId()).as("%s %s", type, path)
            .isEqualTo(member(r, 0, path).getCheckId());
      }
    }
  }

  @Test
  void anAmbiguityUnderAKindChangeLeavesTheGroupUnpaired() {
    // c aliases both a and b: an error where pids pair, but no pid pairs under a kind change.
    // Avro only: Protobuf and JSON never pair two peers with one previous location.
    Field c = new Field("c", Schema.create(Schema.Type.INT), 0, null, false, null, null,
        Map.of(Schema.AVRO_ALIASES, "a,b"));
    ProvenanceReport r = Pids.report(SchemaType.AVRO, Arrays.asList(
        lt(field(SchemaType.AVRO, "x", 1, Schema.createArray(struct(
            field(SchemaType.AVRO, "a", 1, null), field(SchemaType.AVRO, "b", 2, null))))),
        lt(field(SchemaType.AVRO, "x", 1, Schema.createMultiset(struct(c))))));
    assertThat(member(r, 1, 0).getCheckId()).isEqualTo(member(r, 0, 0).getCheckId());
    assertThat(member(r, 1, 0, 0, 0).getCheckId())
        .isNotIn(member(r, 0, 0, 0, 0).getCheckId(), member(r, 0, 0, 0, 1).getCheckId());
  }

  @Test
  void anAmbiguityOfThePeersAloneStillThrowsUnderAKindChange() {
    // Two members of one number are ambiguous with or without a previous version.
    assertThatThrownBy(() -> Pids.report(SchemaType.PROTOBUF, Arrays.asList(
        lt(field(SchemaType.PROTOBUF, "x", 1,
            Schema.createArray(struct(field(SchemaType.PROTOBUF, "a", 1, null))))),
        lt(field(SchemaType.PROTOBUF, "x", 1, Schema.createMultiset(struct(
            field(SchemaType.PROTOBUF, "a", 1, null),
            field(SchemaType.PROTOBUF, "b", 1, null))))))))
        .isInstanceOf(AmbiguousProvenanceException.class)
        .hasMessageContaining("Multiple entities resolve to the same logical identity");
  }

  // -------------------------------------------------------------------------------------------

  // v0 scalar, v1 struct, v2 scalar at path: the pid restarts twice, the check id never.
  private static void assertKindChangeKeepsCheckId(ProvenanceReport r, Integer... path) {
    int first = member(r, 0, path).getId();
    assertThat(member(r, 1, path).getId()).isNotEqualTo(first);
    assertThat(member(r, 2, path).getId()).isNotEqualTo(first)
        .isNotEqualTo(member(r, 1, path).getId());
    int check = member(r, 0, path).getCheckId();
    assertThat(member(r, 1, path).getCheckId()).isEqualTo(check);
    assertThat(member(r, 2, path).getCheckId()).isEqualTo(check);
  }

  // v0 a at [0]; v1 b at [0] renamed from a with a kind change, and a new a at [1].
  private static void assertRenamedKindChange(ProvenanceReport r) {
    ProvenanceReport.Member a = member(r, 0, 0);
    assertThat(member(r, 1, 0).getId()).isNotEqualTo(a.getId());
    assertThat(member(r, 1, 0).getCheckId()).isEqualTo(a.getCheckId());
    assertThat(member(r, 1, 1).getId()).isNotEqualTo(a.getId());
    assertThat(member(r, 1, 1).getCheckId()).isNotEqualTo(a.getCheckId());
  }

  // Pids and check ids are one-to-one across the whole report.
  private static void assertPairAlike(ProvenanceReport r) {
    Map<Integer, Integer> checkIdOf = new HashMap<>();
    Map<Integer, Integer> idOf = new HashMap<>();
    for (ProvenanceReport.Version version : r.getVersions()) {
      for (ProvenanceReport.Member m : version.getMembers()) {
        assertThat(checkIdOf.computeIfAbsent(m.getId(), k -> m.getCheckId())).as("%s", m)
            .isEqualTo(m.getCheckId());
        assertThat(idOf.computeIfAbsent(m.getCheckId(), k -> m.getId())).as("%s", m)
            .isEqualTo(m.getId());
      }
    }
  }

  private static ProvenanceReport.Member member(ProvenanceReport r, int version,
      Integer... path) {
    List<Integer> at = Arrays.asList(path);
    return r.getVersions().get(version).getMembers().stream()
        .filter(m -> m.getPath().equals(at)).findFirst()
        .orElseThrow(() -> new AssertionError("no member at " + at + " in version " + version));
  }

  private static ProvenanceReport report(ParsedSchema... versions) {
    List<LogicalType> types = new ArrayList<>();
    for (ParsedSchema version : versions) {
      types.add(ProvenanceHistory.logicalTypeOf(version, false));
    }
    return Pids.report(SchemaType.of(versions[0].schemaType()), types);
  }

  private static LogicalType lt(Field... fields) {
    return new LogicalType(struct(fields));
  }

  private static Schema struct(Field... fields) {
    return Schema.createStruct(Arrays.asList(fields));
  }

  // A field of type (INT when null), numbered as a Protobuf field is.
  private static Field field(SchemaType format, String name, int number, Schema type) {
    Map<String, Object> params = format == SchemaType.PROTOBUF
        ? Map.of(Schema.PROTOBUF_FIELD_NUMBER, String.valueOf(number)) : null;
    return new Field(name, type != null ? type : Schema.create(Schema.Type.INT), 0, null, false,
        null, null, params);
  }

  private static AvroSchema avro(String fields) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + fields + "]}");
  }

  private static ProtobufSchema proto(String body) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage Row {\n  " + body
        + "\n}\n");
  }

  private static JsonSchema json(String properties) {
    return new JsonSchema("{\"type\":\"object\",\"properties\":" + properties + "}");
  }
}
