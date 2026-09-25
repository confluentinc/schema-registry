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
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Identity rules that keep a `pid` where only the schema's spelling changed: JSON named types are
 * transparent, a Protobuf oneof follows its members' numbers, and an Avro union branch follows its
 * type's aliases or, unambiguously, its promotion family.
 */
class ProvenanceIdentityRulesTest {

  private static final String OBJ_A = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}}}";

  // --- JSON: named types are transparent ------------------------------------------------------

  @Test
  void inliningAJsonReferenceKeepsItsMembersPids() {
    List<ProvenanceVersion> v = compute(
        json("{\"o\":" + OBJ_A + "}", null),
        json("{\"o\":{\"$ref\":\"#/definitions/O\"}}", "{\"O\":" + OBJ_A + "}"));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void renamingAJsonDefinitionKeepsItsMembersPids() {
    List<ProvenanceVersion> v = compute(
        json("{\"o\":{\"$ref\":\"#/definitions/O\"}}", "{\"O\":" + OBJ_A + "}"),
        json("{\"o\":{\"$ref\":\"#/definitions/P\"}}", "{\"P\":" + OBJ_A + "}"));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void anEarlierGeneratedReferenceNameDoesNotShiftALaterOnesPids() {
    List<ProvenanceVersion> v = compute(
        json("{\"p\":{\"$ref\":\"#/properties/q\"},\"q\":" + OBJ_A + "}", null),
        json("{\"a0\":{\"$ref\":\"#/properties/b0\"},\"b0\":" + OBJ_A + ","
            + "\"p\":{\"$ref\":\"#/properties/q\"},\"q\":" + OBJ_A + "}", null));
    // p is the first property in v1 and the third in v2; its a keeps its pid all the same.
    assertThat(pid(v, 1, 2, 0)).isEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void aSharedJsonDefinitionStillHasOnePidPerUse() {
    List<ProvenanceVersion> v = compute(json(
        "{\"x\":{\"$ref\":\"#/definitions/O\"},\"y\":{\"$ref\":\"#/definitions/O\"}}",
        "{\"O\":" + OBJ_A + "}"));
    assertThat(pid(v, 0, 0, 0)).isNotEqualTo(pid(v, 0, 1, 0));
  }

  @Test
  void aRecursiveJsonReferenceStillHasNoProvenance() {
    assertThatThrownBy(() -> compute(json("{\"n\":{\"$ref\":\"#/definitions/N\"}}",
        "{\"N\":{\"type\":\"object\",\"properties\":{\"next\":{\"$ref\":\"#/definitions/N\"}}}}")))
        .isInstanceOf(RecursiveTypeException.class);
  }

  // --- Protobuf: a oneof follows its members' numbers -----------------------------------------

  @Test
  void renamingAOneofKeepsItsPidAndItsMembers() {
    List<ProvenanceVersion> v = compute(
        proto("oneof c { int32 a = 1; string b = 2; }"),
        proto("oneof d { int32 a = 1; string b = 2; }"));
    assertThat(pids(v, 1)).isEqualTo(pids(v, 0));
  }

  @Test
  void aJsonBranchInsertedInFrontLeavesTheOthersTheirOwn() {
    // V1 names unhinted branches by position; inserting C in front shifts A and B, which keep
    // their own identities by content (members and discriminator), not their positions'.
    String a = branch("a", "\"x\":{\"type\":\"number\"}");
    String b = branch("b", "\"y\":{\"type\":\"number\"}");
    String c = branch("c", "\"z\":{\"type\":\"number\"}");
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + a + "," + b + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + c + "," + a + "," + b + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 1, 1))).isEqualTo(before.get(path(0, 0, 1)));
    assertThat(after.get(path(0, 2, 1))).isEqualTo(before.get(path(0, 1, 1)));
    assertThat(after.get(path(0, 0, 1))).isNotIn(before.values());
  }

  @Test
  void jsonBranchesSharingMemberNamesAreToldApartByTheirDiscriminator() {
    String a = branch("a", "\"x\":{\"type\":\"number\"}");
    String b = branch("b", "\"x\":{\"type\":\"number\"}");
    String c = branch("c", "\"x\":{\"type\":\"number\"}");
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + a + "," + b + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + c + "," + a + "," + b + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 1, 1))).isEqualTo(before.get(path(0, 0, 1)));
    assertThat(after.get(path(0, 2, 1))).isEqualTo(before.get(path(0, 1, 1)));
    assertThat(after.get(path(0, 0, 1))).isNotIn(before.values());
  }

  @Test
  void jsonBranchesContentCannotTellApartKeepTheirPositions() {
    String same = "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"number\"}}}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + same + "," + same + "]}}", null),
        json("{\"u\":{\"description\":\"v2\",\"oneOf\":[" + same + "," + same + "]}}",
            null));
    assertThat(pids(v, 1)).isEqualTo(pids(v, 0));
  }

  private static String branch(String kind, String members) {
    return "{\"type\":\"object\",\"properties\":{\"kind\":{\"enum\":[\"" + kind + "\"]},"
        + members + "}}";
  }

  @Test
  void aOneofSplitInThreeContinuesInThePartHoldingTheLowestNumber() {
    List<ProvenanceVersion> v = compute(
        proto("oneof c { int32 a = 1; string b = 2; bool d = 3; }"),
        proto("oneof c1 { int32 a = 1; } oneof c2 { string b = 2; } oneof c3 { bool d = 3; }"));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    // v1: c at [0], a, b, d at [0, 0..2]; v2: c1, c2, c3 at [0..2], each member at [i, 0].
    assertThat(after.get(path(0))).isEqualTo(before.get(path(0)));
    assertThat(after.get(path(0, 0))).isEqualTo(before.get(path(0, 0)));
    assertThat(after.get(path(1))).isNotIn(before.values());
    assertThat(after.get(path(2))).isNotIn(before.values());
  }

  @Test
  void aOneofSplitInTwoContinuesInThePartKeepingMostOfIt() {
    // Both parts share member numbers with c; the one holding the lowest keeps it, the other is
    // new, and so is the field that moved into it.
    List<ProvenanceVersion> v = compute(
        proto("oneof c { int32 a = 1; string b = 2; }"),
        proto("oneof c1 { int32 a = 1; } oneof c2 { string b = 2; }"));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    // v1: c at [0], a at [0, 0], b at [0, 1]; v2: c1 at [0], a at [0, 0], c2 at [1], b at [1, 0].
    assertThat(after.get(path(0))).isEqualTo(before.get(path(0)));
    assertThat(after.get(path(0, 0))).isEqualTo(before.get(path(0, 0)));
    assertThat(after.get(path(1))).isNotIn(before.values());
    assertThat(after.get(path(1, 0))).isNotIn(before.values());
  }

  @Test
  void aFieldMovedIntoAOneofIsNew() {
    List<ProvenanceVersion> v = compute(
        proto("oneof c { int32 a = 1; } string b = 2;"),
        proto("oneof c { int32 a = 1; string b = 2; }"));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    // v1: b at [0], c at [1], a at [1, 0]; v2: c at [0], a at [0, 0], b at [0, 1].
    assertThat(after.get(path(0))).isEqualTo(before.get(path(1)));
    assertThat(after.get(path(0, 0))).isEqualTo(before.get(path(1, 0)));
    assertThat(after.get(path(0, 1))).isNotIn(before.values());
  }

  // --- Avro: union branches -------------------------------------------------------------------

  @Test
  void aBranchTypeRenamedWithAnAliasKeepsItsPidAndItsFields() {
    String a = "{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    String b = "{\"type\":\"record\",\"name\":\"B\",\"aliases\":[\"A\"],"
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    List<ProvenanceVersion> v = compute(
        avro("[\"null\"," + a + ",\"string\"]"), avro("[\"null\"," + b + ",\"string\"]"));
    assertThat(pids(v, 1)).isEqualTo(pids(v, 0));
  }

  @Test
  void aBranchPromotedUnambiguouslyKeepsItsPid() {
    List<ProvenanceVersion> v = compute(
        avro("[\"null\",\"int\",\"string\"]"), avro("[\"null\",\"long\",\"string\"]"));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void aBranchWithTwoPromotionsIsNew() {
    List<ProvenanceVersion> v = compute(
        avro("[\"int\",\"string\"]"), avro("[\"long\",\"float\",\"string\"]"));
    assertThat(pid(v, 1, 0, 0)).isNotIn(pids(v, 0).values());
    assertThat(pid(v, 1, 0, 1)).isNotIn(pids(v, 0).values());
  }

  @Test
  void aBranchDroppedAndReAddedIsNew() {
    List<ProvenanceVersion> v = compute(avro("[\"int\",\"string\"]"),
        avro("[\"string\",\"boolean\"]"), avro("[\"int\",\"string\"]", "v3"));
    assertThat(pid(v, 2, 0, 0)).isNotEqualTo(pid(v, 0, 0, 0));
  }

  // -------------------------------------------------------------------------------------------

  private static List<ProvenanceVersion> compute(ParsedSchema... versions) {
    List<ProvenanceHistory.Entry> history = new ArrayList<>();
    for (int i = 0; i < versions.length; i++) {
      history.add(new ProvenanceHistory.Entry(i + 1, i + 1, false));
    }
    return ProvenanceHistory.compute("s", history, Arrays.asList(versions)).getVersions();
  }

  private static Map<List<Integer>, Integer> pids(List<ProvenanceVersion> versions, int version) {
    Map<List<Integer>, Integer> pids = new HashMap<>();
    for (ProvenanceField field : versions.get(version).getFields()) {
      pids.put(field.getPath(), field.getPid());
    }
    return pids;
  }

  private static Integer pid(List<ProvenanceVersion> versions, int version, Integer... path) {
    Integer pid = pids(versions, version).get(path(path));
    assertThat(pid).as("pid at %s in version %s", Arrays.toString(path), version).isNotNull();
    return pid;
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
  }

  private static JsonSchema json(String properties, String definitions) {
    return new JsonSchema("{\"type\":\"object\",\"properties\":" + properties
        + (definitions != null ? ",\"definitions\":" + definitions : "") + "}");
  }

  private static ProtobufSchema proto(String body) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage Row {\n" + body + "\n}\n");
  }

  private static AvroSchema avro(String union) {
    return avro(union, "");
  }

  private static AvroSchema avro(String union, String doc) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"doc\":\"" + doc + "\","
        + "\"fields\":[{\"name\":\"f\",\"type\":" + union + "}]}");
  }
}
