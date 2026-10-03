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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import java.time.Duration;
import java.util.Collections;
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

  @Test
  void aMapWhoseKeyAndValueShareATypeIsNoCycle() {
    // P as key and value is P beside itself: the value becoming Q changes no kind, so the map
    // and its key continue, and only the value's member is new.
    String entries = "{\"m\":{\"type\":\"array\",\"connect.type\":\"map\",\"items\":"
        + "{\"type\":\"object\",\"properties\":{\"key\":{\"$ref\":\"#/definitions/P\"},"
        + "\"value\":{\"$ref\":\"#/definitions/%s\"}}}}}";
    String definitions = "{\"P\":{\"type\":\"object\",\"properties\":"
        + "{\"a\":{\"type\":\"integer\"}}},\"Q\":{\"type\":\"object\",\"properties\":"
        + "{\"b\":{\"type\":\"integer\"}}}}";
    List<ProvenanceVersion> v = compute(json(String.format(entries, "P"), definitions),
        json(String.format(entries, "Q"), definitions));
    // m at [0], its key's a at [0, 0, 0], its value's member at [0, 1, 0].
    assertThat(pid(v, 1, 0)).isEqualTo(pid(v, 0, 0));
    assertThat(pid(v, 1, 0, 0, 0)).isEqualTo(pid(v, 0, 0, 0, 0));
    assertThat(pid(v, 1, 0, 1, 0)).isNotIn(pids(v, 0).values());
  }

  @Test
  void aCollectionHoldingItselfStillHasNoProvenance() {
    // The kind of what a collection holds must not follow it round: the recursion is named.
    assertThatThrownBy(() -> compute(json("{\"x\":{\"$ref\":\"#/definitions/L\"}}",
        "{\"L\":{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/L\"}}}")))
        .isInstanceOf(RecursiveTypeException.class);
    assertThatThrownBy(() -> compute(json("{\"x\":{\"$ref\":\"#/definitions/M\"}}",
        "{\"M\":{\"type\":\"object\",\"connect.type\":\"map\","
            + "\"additionalProperties\":{\"$ref\":\"#/definitions/M\"}}}")))
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
  void aJsonBranchThatMovesAndGainsAMemberContinues() {
    // B moves first and gains w: no branch has its content, and its position holds A's, but it
    // alone shares a member with the old B.
    String a = branch("a", "\"x\":{\"type\":\"number\"}");
    String b = branch("b", "\"y\":{\"type\":\"number\"}");
    String bw = branch("b", "\"y\":{\"type\":\"number\"},\"w\":{\"type\":\"number\"}");
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + a + "," + b + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + bw + "," + a + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    // v2 sorts B's members: kind, w, y.
    assertThat(after.get(path(0, 0))).isEqualTo(before.get(path(0, 1)));
    assertThat(after.get(path(0, 0, 2))).isEqualTo(before.get(path(0, 1, 1)));
    assertThat(after.get(path(0, 1))).isEqualTo(before.get(path(0, 0)));
  }

  @Test
  void aMemberEveryBranchHasTellsNoBranchApart() {
    // Both branches share id, so neither overlaps one old branch alone; the members beyond it
    // pair them, where position would cross them over.
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + titled(null, "id", "a1") + ","
            + titled(null, "id", "b1") + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + titled(null, "id", "b1", "b2") + ","
            + titled(null, "id", "a1", "a2") + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    // Members sort by name: a1 or b1 first, id last.
    assertThat(after.get(path(0, 0))).isEqualTo(before.get(path(0, 1)));
    assertThat(after.get(path(0, 0, 0))).isEqualTo(before.get(path(0, 1, 0)));
    assertThat(after.get(path(0, 0, 2))).isEqualTo(before.get(path(0, 1, 1)));
    assertThat(after.get(path(0, 1))).isEqualTo(before.get(path(0, 0)));
    assertThat(after.get(path(0, 1, 0))).isEqualTo(before.get(path(0, 0, 0)));
  }

  @Test
  void aBranchHintedAnewTakesNoOldBranchThroughTheMembersItShares() {
    // The renamed hint makes the second branch new; sharing id and e, it must not take the first
    // branch from its successor, which dropped id.
    String hints = ",\"confluent:union\":[{},{\"name\":\"%s\"},{}]";
    String j = titled(null, "id", "j");
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + titled(null, "e", "id") + "," + titled(null, "e", "g", "id")
            + "," + j + "]" + String.format(hints, "H32") + "}}", null),
        json("{\"u\":{\"oneOf\":[" + titled(null, "e") + "," + titled(null, "e", "g", "id")
            + "," + j + "]" + String.format(hints, "H33") + "}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 0))).isEqualTo(before.get(path(0, 0)));
    assertThat(after.get(path(0, 0, 0))).isEqualTo(before.get(path(0, 0, 0)));
    assertThat(after.get(path(0, 1))).isNotIn(before.values());
    assertThat(after.get(path(0, 2))).isEqualTo(before.get(path(0, 2)));
  }

  @Test
  void aBranchLeftOnlyAMemberEveryBranchHadStillContinues() {
    // Once A is paired, B is the one old branch left: id, though every branch had it, links it,
    // and B moved, so its position cannot.
    String nested = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"number\"},"
        + "\"n\":" + titled(null, "y") + "}}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + titled(null, "id", "x") + ","
            + titled(null, "id", "y") + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + nested + "," + titled(null, "id", "x") + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 0))).isEqualTo(before.get(path(0, 1)));
    assertThat(after.get(path(0, 0, 0))).isEqualTo(before.get(path(0, 1, 0)));
  }

  @Test
  void aMemberEveryBranchHadStillTellsNothingOnceOneBranchIsLeft() {
    // With A paired, B is the one old branch left; m1 and m2, which every old branch had, must not
    // hand it to the new N over B', its successor, whichever comes first.
    String a = titled(null, "a", "id", "m1", "m2");
    String bPrime = titled(null, "b", "id");
    String n = titled(null, "id", "m1", "m2", "n");
    for (boolean newFirst : new boolean[] {false, true}) {
      List<ProvenanceVersion> v = compute(
          json("{\"u\":{\"oneOf\":[" + a + "," + titled(null, "b", "id", "m1", "m2") + "]}}",
              null),
          json("{\"u\":{\"oneOf\":[" + a + "," + (newFirst ? n + "," + bPrime
              : bPrime + "," + n) + "]}}", null));
      Map<List<Integer>, Integer> before = pids(v, 0);
      Map<List<Integer>, Integer> after = pids(v, 1);
      int successor = newFirst ? 2 : 1;
      assertThat(after.get(path(0, 0))).isEqualTo(before.get(path(0, 0)));
      assertThat(after.get(path(0, successor))).isEqualTo(before.get(path(0, 1)));
      assertThat(after.get(path(0, successor, 0))).isEqualTo(before.get(path(0, 1, 0)));
      assertThat(after.get(path(0, 3 - successor))).isNotIn(before.values());
    }
  }

  @Test
  void aBranchPassedOverBeforeAPairingLeavesOneOldBranchStillContinues() {
    // Seen first, {x} ties on the envelope x; once {w,x,z} takes P2, P1 is the one left, whichever
    // order the two come in.
    String p1 = titled(null, "x", "y");
    String p2 = titled(null, "x", "z");
    for (boolean lastFirst : new boolean[] {false, true}) {
      String x = titled(null, "x");
      String wxz = titled(null, "w", "x", "z");
      List<ProvenanceVersion> v = compute(
          json("{\"u\":{\"oneOf\":[" + p1 + "," + p2 + "]}}", null),
          json("{\"u\":{\"oneOf\":[" + titled(null, "n") + ","
              + (lastFirst ? wxz + "," + x : x + "," + wxz) + "]}}", null));
      Map<List<Integer>, Integer> before = pids(v, 0);
      Map<List<Integer>, Integer> after = pids(v, 1);
      assertThat(after.get(path(0, lastFirst ? 2 : 1))).isEqualTo(before.get(path(0, 0)));
      assertThat(after.get(path(0, lastFirst ? 1 : 2))).isEqualTo(before.get(path(0, 1)));
    }
  }

  @Test
  void aBranchPairedByItsHintBlocksNoOtherBranchsTag() {
    // H, paired by its hint, shares R's tag; Q, moved, still continues R by that tag.
    String h = branch("k1", "\"a\":{\"type\":\"number\"}");
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + h + "," + branch("k1", "\"b\":{\"type\":\"number\"}")
            + "," + branch("k3", "\"t\":{\"type\":\"number\"}") + "],"
            + "\"confluent:union\":[{\"name\":\"H\"},{},{}]}}", null),
        json("{\"u\":{\"oneOf\":[" + branch("k1", "\"c\":{\"type\":\"number\"}") + ","
            + h + "],\"confluent:union\":[{},{\"name\":\"H\"}]}}", null));
    assertThat(pids(v, 1).get(path(0, 0))).isEqualTo(pids(v, 0).get(path(0, 1)));
    assertThat(pids(v, 1).get(path(0, 1))).isEqualTo(pids(v, 0).get(path(0, 0)));
  }

  @Test
  void aTaggedBranchContinuesItsTagRatherThanAPosition() {
    // P overlaps both Q (through x) and R (through kind), so overlap cannot tell; its tag names R,
    // which position alone would have passed over for Q.
    String q = "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"number\"}}}";
    String r = branch("v", "\"y\":{\"type\":\"number\"}");
    String p = branch("v", "\"x\":{\"type\":\"number\"}");
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + q + "," + r + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + p + "]}}", null));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 1));
  }

  @Test
  void aTagWhoseValueHoldsASlashIsStillATag() {
    // The tag's value, not a nesting step: P continues R by its tag, not Q by position.
    String q = "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"number\"}}}";
    String r = branch("v/1", "\"y\":{\"type\":\"number\"}");
    String p = branch("v/1", "\"x\":{\"type\":\"number\"}");
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + q + "," + r + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + p + "]}}", null));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 1));
  }

  @Test
  void aVersionNestingTooDeepHasNoProvenance() {
    // Each message holds the next: no text nests, yet locations do, past the depth limit (1000),
    // where the walk would otherwise exhaust the stack.
    StringBuilder chain = new StringBuilder("syntax = \"proto3\";\npackage p;\n");
    for (int i = 0; i < 2000; i++) {
      chain.append("message M").append(i).append(" { M").append(i + 1).append(" p = 1; }\n");
    }
    String text = chain.append("message M2000 { int32 x = 1; }\n").toString();
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", Collections.singletonList(
        new SchemaMetadata(17, 7, "PROTOBUF", Collections.emptyList(), text)),
        ProvenanceHistory.held(Collections.singletonList(new ProtobufSchema(text))), false))
        .isInstanceOf(TooManyLocationsException.class)
        .hasMessageContaining("Version 7 nests locations more than 1000 deep");
  }

  @Test
  void anotherProtobufMessageAtTheRootIsAnotherEntity() {
    // Single-message provenance roots each version at its file's first message: B is not A.
    String head = "syntax = \"proto3\";\npackage p;\n";
    ProtobufSchema a = new ProtobufSchema(head + "message A {\n  int32 id = 1;\n}\n");
    ProtobufSchema b = new ProtobufSchema(head + "message B {\n  int32 id = 1;\n}\n"
        + "message A {\n  int32 id = 1;\n}\n");
    List<ProvenanceVersion> v = compute(a, b, a);
    assertThat(pid(v, 1, 0)).isNotEqualTo(pid(v, 0, 0));
    assertThat(pid(v, 2, 0)).isNotEqualTo(pid(v, 1, 0));
    List<ProvenanceVersion> same = compute(a, a);
    assertThat(pid(same, 1, 0)).isEqualTo(pid(same, 0, 0));
  }

  @Test
  void aVersionWithTooManyLocationsHasNoProvenance() {
    // M_i uses M_i+1 twice, so each level doubles the locations: past the limit, no provenance.
    // The message names the version by its number.
    String doubling = doublingMessages(16);
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", Collections.singletonList(
        new SchemaMetadata(17, 7, "PROTOBUF", Collections.emptyList(), doubling)),
        ProvenanceHistory.held(Collections.singletonList(new ProtobufSchema(doubling))), false))
        .isInstanceOf(TooManyLocationsException.class)
        .hasMessageContaining("Version 7 has more than " + ProvenanceComputer.MAX_LOCATIONS);
  }

  @Test
  void aHistoryWithTooManyLocationsHasNoProvenance() {
    // Each version is under the limit, but every version's locations are kept at once.
    ProtobufSchema large = new ProtobufSchema(doublingMessages(15));
    ParsedSchema[] versions = new ParsedSchema[6];
    Arrays.fill(versions, large);
    assertThatThrownBy(() -> compute(versions))
        .isInstanceOf(TooManyLocationsException.class)
        .hasMessageContaining("The history has more than "
            + ProvenanceComputer.MAX_REPORT_LOCATIONS);
  }

  @Test
  void aVersionWithNoSchemaTypeIsAvro() {
    // As Schema Registry leaves an Avro schema's type unset.
    AvroSchema avro = new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"}]}");
    assertThat(ProvenanceHistory.compute("s", Collections.singletonList(
        new SchemaMetadata(1, 1, null, Collections.emptyList(), avro.canonicalString())),
        ProvenanceHistory.held(Collections.singletonList(avro)), false)
        .getVersions().get(0).getFields()).hasSize(1);
  }

  @Test
  void anAmbiguousHistoryNamesTheVersionByItsNumber() {
    // Versions 5 and 6: the computer counts them 0 and 1, the message names 6.
    String a = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[%s]}";
    List<ParsedSchema> versions = Arrays.asList(
        new AvroSchema(String.format(a, "{\"name\":\"a\",\"type\":\"int\"}")),
        new AvroSchema(String.format(a, "{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]},"
            + "{\"name\":\"c\",\"type\":\"int\",\"aliases\":[\"a\"]}")));
    List<SchemaMetadata> history = Arrays.asList(
        new SchemaMetadata(15, 5, "AVRO", Collections.emptyList(), ""),
        new SchemaMetadata(16, 6, "AVRO", Collections.emptyList(), ""));
    assertThatThrownBy(() -> ProvenanceHistory.compute("s", history,
        ProvenanceHistory.held(versions), false))
        .isInstanceOf(AmbiguousProvenanceException.class)
        .hasMessageContaining("version 6");
  }

  // Messages M_0 to M_n, each but the last holding two fields of the next.
  private static String doublingMessages(int n) {
    StringBuilder file = new StringBuilder("syntax = \"proto3\";\npackage p;\n");
    for (int i = 0; i < n; i++) {
      file.append("message M").append(i).append(" {\n  M").append(i + 1).append(" a = 1;\n  M")
          .append(i + 1).append(" b = 2;\n}\n");
    }
    return file.append("message M").append(n).append(" {\n  int32 id = 1;\n}\n").toString();
  }

  @Test
  void aWrappersBranchesFollowTheirNumbersWhenFlinkReordersThem() {
    // Flink re-emits a reordered union numbering its wrapper positionally: b takes a's number.
    List<ProvenanceVersion> v = compute(flinkRow("a", "b"), flinkRow("b", "a"));
    assertThat(pidByNames(v, 1, "us", "b")).isEqualTo(pidByNames(v, 0, "us", "a"));
    assertThat(pidByNames(v, 1, "us", "a")).isEqualTo(pidByNames(v, 0, "us", "b"));
  }

  @Test
  void aWrapperReorderedWithItsNumbersContinues() {
    List<ProvenanceVersion> v = compute(
        wrappedArray("string a = 1;\n      string b = 2;"),
        wrappedArray("string b = 2;\n      string a = 1;"));
    assertThat(pidByNames(v, 1, "us", "a")).isEqualTo(pidByNames(v, 0, "us", "a"));
    assertThat(pidByNames(v, 1, "us", "b")).isEqualTo(pidByNames(v, 0, "us", "b"));
  }

  @Test
  void aSingularWrappedUnionFieldKeepsItsWrappersNumbers() {
    // u is field 2 of Row; a and b are fields 1 and 2 of its wrapper, not 2 and 3 of Row.
    String uw = "  message UW {\n    oneof value {\n      string a = 1;\n      string b = 2;\n"
        + "    }\n  }";
    List<ProvenanceVersion> v = compute(
        wrappedProto("  int32 id = 1;\n  UW u = 2" + WRAPPED + ";\n" + uw),
        wrappedProto("  int32 id = 1;\n  UW u = 2" + WRAPPED + ";\n  string x = 3;\n" + uw));
    assertThat(pidByNames(v, 1, "u", "a")).isEqualTo(pidByNames(v, 0, "u", "a"));
    assertThat(pidByNames(v, 1, "u", "b")).isEqualTo(pidByNames(v, 0, "u", "b"));
  }

  @Test
  void aJsonUnionCollapsingToANullableScalarRestarts() {
    // oneOf [string, null] is a nullable string: the union's kind changes, so u is a new location.
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"integer\"}]}}", null),
        json("{\"u\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"null\"}]}}", null));
    assertThat(pidByNames(v, 1, "u")).isNotEqualTo(pidByNames(v, 0, "u"));
  }

  @Test
  void aProtobufListFlinkWrappedContinues() {
    // The wrapper is transparent: both are an ARRAY<SCALAR> at xs.
    List<ProvenanceVersion> v = compute(
        wrappedProto("  int32 id = 1;\n  repeated int32 xs = 2;"),
        wrappedProto("  int32 id = 1;\n  WL xs = 2" + WRAPPED + ";\n"
            + "  message WL {\n    repeated int32 value = 1;\n  }"));
    assertThat(pidByNames(v, 1, "xs")).isEqualTo(pidByNames(v, 0, "xs"));
  }

  private static final String WRAPPED =
      " [(confluent.field_meta) = {params: [{key: \"flink.wrapped\", value: \"true\"}]}]";

  private static ProtobufSchema wrappedProto(String members) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"confluent/meta.proto\";\nmessage Row {\n" + members + "\n}\n");
  }

  // Row holding an array of unions, its wrapper's branches declared as given.
  private static ProtobufSchema wrappedArray(String branches) {
    return wrappedProto("  int32 id = 1;\n  repeated UW us = 2" + WRAPPED + ";\n"
        + "  message UW {\n    oneof value {\n      " + branches + "\n    }\n  }");
  }

  // Row as Flink writes it: an id and an array of a union of the given string branches.
  private static ProtobufSchema flinkRow(String... branches) {
    List<UnionBranch> union = new ArrayList<>();
    for (String branch : branches) {
      union.add(new UnionBranch(branch, Schema.createString().setNullable(true)));
    }
    Schema row = Schema.createStruct(Arrays.asList(
        new Field("id", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("us", Schema.createArray(Schema.createUnion(union).setNullable(true))
            .setNullable(false), 1))).setNullable(false);
    return LogicalTypeToProtoConverter.fromLogicalType(new LogicalType(row), "Row");
  }

  // The pid of the location whose native names end with the given ones.
  private static Integer pidByNames(List<ProvenanceVersion> versions, int version,
      String... names) {
    List<String> suffix = Arrays.asList(names);
    for (ProvenanceField field : versions.get(version).getFields()) {
      List<String> own = field.getNames();
      if (own.size() >= suffix.size()
          && own.subList(own.size() - suffix.size(), own.size()).equals(suffix)) {
        return field.getPid();
      }
    }
    throw new AssertionError("No location " + suffix + " in version " + version);
  }

  @Test
  void aTagWhoseNameHoldsASlashIsStillATag() {
    // A name, not a nesting step: P continues R by its tag, not Q by position.
    String q = "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"number\"}}}";
    String tag = "\"a/b\":{\"enum\":[\"v\"]},";
    String r = "{\"type\":\"object\",\"properties\":{" + tag + "\"y\":{\"type\":\"number\"}}}";
    String p = "{\"type\":\"object\",\"properties\":{" + tag + "\"x\":{\"type\":\"number\"}}}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + q + "," + r + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + p + "]}}", null));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 1));
  }

  @Test
  void aRenamedTagWhoseNameHoldsAnEqualsSignStillContinues() {
    // k=1 renamed k=2 is a tag renamed, as k to kx would be; its members carry the branch.
    String moved = "{\"type\":\"object\",\"properties\":{\"q\":{\"const\":\"y\"},"
        + "\"c\":{\"type\":\"integer\"}}}";
    String kept = "{\"type\":\"object\",\"properties\":{\"%s\":{\"const\":\"x\"},"
        + "\"a\":{\"type\":\"string\"},\"s\":{\"type\":\"string\"}}}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + String.format(kept, "k=1") + "," + moved + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + moved + "," + String.format(kept, "k=2") + "]}}", null));
    assertThat(pid(v, 1, 0, 1)).isEqualTo(pid(v, 0, 0, 0));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 1));
  }

  @Test
  void aWideUnionWhoseBranchesAllChangedIsMatchedInTimeQuadraticInItsWidth() {
    // 1,600 untagged branches, each gaining a member in v2: every one reaches the overlap phase,
    // whose crossing test listed every other branch per candidate pair, though none can cross.
    StringBuilder v1 = new StringBuilder();
    StringBuilder v2 = new StringBuilder();
    for (int i = 0; i < 1600; i++) {
      StringBuilder members = new StringBuilder();
      for (int m = 0; m < 5; m++) {
        members.append(m > 0 ? "," : "").append("\"u").append(i).append('_').append(m)
            .append("\":{\"type\":\"string\"}");
      }
      // Members most branches share, but not all: they tell branches apart, so pairs overlap.
      for (int c = 0; c < 5 && i % 3 != 0; c++) {
        members.append(",\"c").append(c).append("\":{\"type\":\"string\"}");
      }
      String branch = "{\"type\":\"object\",\"properties\":{" + members;
      v1.append(i > 0 ? "," : "").append(branch).append("}}");
      v2.append(i > 0 ? "," : "").append(branch).append(",\"added\":{\"type\":\"string\"}}}");
    }
    List<ProvenanceVersion> versions = assertTimeoutPreemptively(Duration.ofSeconds(12),
        () -> compute(json("{\"e\":{\"oneOf\":[" + v1 + "]}}", null),
            json("{\"e\":{\"oneOf\":[" + v2 + "]}}", null)));
    for (int i = 0; i < 1600; i++) {
      assertThat(pid(versions, 1, 0, i)).isEqualTo(pid(versions, 0, 0, i));
    }
  }

  @Test
  void aJsonBranchContinuesTheSoleBranchOfItsScalarFamily() {
    // A memberless branch has only its type for content, so no earlier phase pairs a type change:
    // the one branch whose values can coincide with its own continues it, as a property would.
    String[][] changes = {
        {"{\"type\":\"integer\"}", "{\"type\":\"number\"}"},
        {"{\"type\":\"number\"}", "{\"type\":\"integer\"}"},
        {"{\"type\":\"string\"}", "{\"type\":\"string\",\"minLength\":2,\"maxLength\":2}"},
        {"{\"type\":\"string\",\"enum\":[\"a\",\"b\"]}", "{\"type\":\"string\"}"},
        {"{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}",
            "{\"type\":\"array\",\"items\":{\"type\":\"number\"}}"}};
    for (String[] change : changes) {
      String union = "{\"x\":{\"oneOf\":[%s,{\"type\":\"boolean\"}]}}";
      List<ProvenanceVersion> v = compute(json(String.format(union, change[0]), null),
          json(String.format(union, change[1]), null));
      assertThat(pid(v, 1, 0, 0)).as(change[1]).isEqualTo(pid(v, 0, 0, 0));
      assertThat(pid(v, 1, 0, 1)).as(change[1]).isEqualTo(pid(v, 0, 0, 1));
    }
  }

  @Test
  void aJsonStringBranchBecomingBytesKeepsItsPid() {
    // JSON writes Connect bytes as base64 strings, so a string and a bytes branch left unpaired
    // continue, as Avro's string and bytes do.
    String s = "{\"type\":\"string\"}";
    String bytes = "{\"type\":\"string\",\"connect.type\":\"bytes\"}";
    String union = "{\"x\":{\"oneOf\":[%s,{\"type\":\"boolean\"}]}}";
    for (String[] change : new String[][] {{s, bytes}, {bytes, s}}) {
      List<ProvenanceVersion> v = compute(json(String.format(union, change[0]), null),
          json(String.format(union, change[1]), null));
      assertThat(pid(v, 1, 0, 0)).as(change[1]).isEqualTo(pid(v, 0, 0, 0));
    }
  }

  @Test
  void aJsonCharacterAndBinaryBranchEachKeepTheirFamilysPairing() {
    // Within each family first: the string continues as the enum and the fixed bytes as the
    // bytes, rather than the two families' four branches leaving every one ambiguous.
    String fixed = "{\"type\":\"string\",\"connect.type\":\"bytes\","
        + "\"flink.minLength\":4,\"flink.maxLength\":4}";
    String v1 = "{\"x\":{\"oneOf\":[{\"type\":\"string\"}," + fixed + "]}}";
    String v2 = "{\"x\":{\"oneOf\":[{\"type\":\"string\",\"enum\":[\"a\"]},"
        + "{\"type\":\"string\",\"connect.type\":\"bytes\"}]}}";
    List<ProvenanceVersion> v = compute(json(v1, null), json(v2, null));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
    assertThat(pid(v, 1, 0, 1)).isEqualTo(pid(v, 0, 0, 1));
  }

  @Test
  void aNumericEnumBranchIsInTheCharacterFamilyAsTheLogicalTypeSpellsIt() {
    // The logical type spells every enum symbol as a string, so a numeric enum is an ENUM, of the
    // character family, until the converter records a symbol's JSON type.
    String union = "{\"x\":{\"oneOf\":[%s,{\"type\":\"boolean\"}]}}";
    List<ProvenanceVersion> v = compute(json(String.format(union, "{\"enum\":[1,2]}"), null),
        json(String.format(union, "{\"type\":\"string\"}"), null));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void aJsonBranchPairingDoesNotDependOnBranchOrder() {
    // {a,g} and t2{a,e,g} each overlap both {a} and {a,e,g}: whichever pairing came first in
    // union order decided what the other crossed. Listed either way, they pair alike.
    String[] v1 = {tagged("t2", "c", "e"), tagged(null, "a", "c"), tagged("t2", "d"),
        tagged(null, "a"), tagged(null, "a", "e", "g")};
    String[] v2 = {tagged("t2", "c", "d", "e"), tagged("t2", "d", "f"), tagged(null, "a", "g"),
        tagged("t2", "a", "e", "g"), tagged(null, "a", "c")};
    String[] reordered = {v2[2], v2[3], v2[4], v2[0], v2[1]};
    List<ProvenanceVersion> listed = compute(union(v1), union(v2));
    List<ProvenanceVersion> other = compute(union(v1), union(reordered));
    assertThat(pid(other, 1, 0, 0)).isEqualTo(pid(listed, 1, 0, 2));
    assertThat(pid(other, 1, 0, 1)).isEqualTo(pid(listed, 1, 0, 3));
  }

  // An object branch holding a string member of each name, tagged kind = tag unless null.
  private static String tagged(String tag, String... members) {
    List<String> properties = new ArrayList<>();
    if (tag != null) {
      properties.add("\"kind\":{\"const\":\"" + tag + "\"}");
    }
    for (String member : members) {
      properties.add("\"" + member + "\":{\"type\":\"string\"}");
    }
    return "{\"type\":\"object\",\"properties\":{" + String.join(",", properties) + "}}";
  }

  private static JsonSchema union(String... branches) {
    return json("{\"x\":{\"oneOf\":[" + String.join(",", branches) + "]}}", null);
  }

  @Test
  void aTitleSharedByTwoNewBranchesContinuesNeither() {
    // One title, two branches holding it: the title tells neither apart, and neither continues
    // by order.
    String titled =
        "{\"type\":\"object\",\"title\":\"T\",\"properties\":{\"%s\":{\"type\":\"string\"}}}";
    List<ProvenanceVersion> v = compute(
        union(String.format(titled, "a"), "{\"type\":\"boolean\"}"),
        union(String.format(titled, "b"), String.format(titled, "c"), "{\"type\":\"boolean\"}"));
    Map<List<Integer>, Integer> before = pids(v, 0);
    assertThat(pid(v, 1, 0, 0)).isNotIn(before.values());
    assertThat(pid(v, 1, 0, 1)).isNotIn(before.values());
  }

  @Test
  void aJsonBranchOfAnotherScalarFamilyIsNew() {
    String union = "{\"x\":{\"oneOf\":[{\"type\":\"%s\"},{\"type\":\"boolean\"}]}}";
    List<ProvenanceVersion> v = compute(json(String.format(union, "integer"), null),
        json(String.format(union, "string"), null));
    assertThat(pid(v, 1, 0, 0)).isNotEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void aJsonIntegerBranchWidenedWhereTwoNumbersCouldTakeItIsNew() {
    String v1 = "{\"x\":{\"oneOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}}";
    String v2 = "{\"x\":{\"oneOf\":[{\"type\":\"number\",\"maximum\":0},"
        + "{\"type\":\"number\",\"minimum\":1},{\"type\":\"string\"}]}}";
    List<ProvenanceVersion> v = compute(json(v1, null), json(v2, null));
    assertThat(pid(v, 1, 0, 0)).isNotEqualTo(pid(v, 0, 0, 0));
    assertThat(pid(v, 1, 0, 1)).isNotEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void aJsonBranchWidenedBesideTwoOfItsFamilyIsNew() {
    // The values phase runs before position (decided 2026-10-02): number sees both integers as
    // candidates, and only position, after it, tells the two apart.
    String union = "{\"x\":{\"oneOf\":[{\"type\":\"integer\"},{\"type\":\"%s\"}]}}";
    List<ProvenanceVersion> v = compute(json(String.format(union, "integer"), null),
        json(String.format(union, "number"), null));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
    assertThat(pid(v, 1, 0, 1)).isNotEqualTo(pid(v, 0, 0, 1));
  }

  // An object branch titled so, holding one number property of each name.
  private static String titled(String title, String... numbers) {
    StringBuilder properties = new StringBuilder();
    for (String name : numbers) {
      properties.append(properties.length() == 0 ? "" : ",")
          .append("\"").append(name).append("\":{\"type\":\"number\"}");
    }
    return "{\"type\":\"object\"" + (title == null ? "" : ",\"title\":\"" + title + "\"")
        + ",\"properties\":{" + properties + "}}";
  }

  @Test
  void aTitledBranchWhoseMembersAllChangedContinuesByItsTitle() {
    // Nothing of the content is left to tell, but the title names it.
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + titled("Card", "x") + "," + titled("Bank", "y") + "]}}",
            null),
        json("{\"u\":{\"oneOf\":[" + titled("Bank", "w") + "," + titled("Card", "z") + "]}}",
            null));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 1));
    assertThat(pid(v, 1, 0, 1)).isEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void aTitleOnTheDefinitionABranchRefersToCounts() {
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[{\"$ref\":\"#/definitions/C\"},{\"$ref\":\"#/definitions/B\"}]}}",
            "{\"C\":" + titled("Card", "x") + ",\"B\":" + titled("Bank", "y") + "}"),
        json("{\"u\":{\"oneOf\":[{\"$ref\":\"#/definitions/B\"},{\"$ref\":\"#/definitions/C\"}]}}",
            "{\"C\":" + titled("Card", "z") + ",\"B\":" + titled("Bank", "w") + "}"));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 1));
    assertThat(pid(v, 1, 0, 1)).isEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void aTitleSharedOrChangedLeavesTheBranchToItsContent() {
    // Shared by two branches, a title tells nothing; changed, it is no guard: content decides.
    List<ProvenanceVersion> shared = compute(
        json("{\"u\":{\"oneOf\":[" + titled("Pay", "x") + "," + titled("Pay", "y") + "]}}",
            null),
        json("{\"u\":{\"oneOf\":[" + titled("Pay", "y") + "," + titled("Pay", "x") + "]}}",
            null));
    assertThat(pid(shared, 1, 0, 0)).isEqualTo(pid(shared, 0, 0, 1));
    List<ProvenanceVersion> renamed = compute(
        json("{\"u\":{\"oneOf\":[" + titled("Card", "x") + "," + titled("Bank", "y") + "]}}",
            null),
        json("{\"u\":{\"oneOf\":[" + titled("Bank", "y") + "," + titled("CreditCard", "x")
            + "]}}", null));
    assertThat(pid(renamed, 1, 0, 1)).isEqualTo(pid(renamed, 0, 0, 0));
  }

  @Test
  void aTitleNeverPairsAcrossATagOrAHint() {
    // Same title, but the tag says another kind of record, or the hints say another branch.
    String card = "{\"type\":\"object\",\"title\":\"Pay\",\"properties\":"
        + "{\"kind\":{\"enum\":[\"%s\"]},\"%s\":{\"type\":\"number\"}}}";
    List<ProvenanceVersion> tagged = compute(
        json("{\"u\":{\"oneOf\":[" + String.format(card, "a", "x") + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + String.format(card, "b", "y") + "]}}", null));
    assertThat(pid(tagged, 1, 0, 0)).isNotIn(pids(tagged, 0).values());
    List<ProvenanceVersion> hinted = compute(
        json("{\"u\":{\"oneOf\":[" + titled("Pay", "x") + "],"
            + "\"confluent:union\":[{\"name\":\"H1\"}]}}", null),
        json("{\"u\":{\"oneOf\":[" + titled("Pay", "y") + "],"
            + "\"confluent:union\":[{\"name\":\"H2\"}]}}", null));
    assertThat(pid(hinted, 1, 0, 0)).isNotIn(pids(hinted, 0).values());
  }

  @Test
  void aConnectTypeTitleNamesNoBranch() {
    // org.apache.kafka.connect.data.* marks a logical type, which content already records.
    String date = "{\"type\":\"integer\",\"title\":\"org.apache.kafka.connect.data.Date\"}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + date + "," + titled(null, "x") + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + titled(null, "y") + "," + date + "]}}", null));
    // The date branch continues by content, as before; the object branch, changed, is new.
    assertThat(pid(v, 1, 0, 1)).isEqualTo(pid(v, 0, 0, 0));
    assertThat(pid(v, 1, 0, 0)).isNotIn(pids(v, 0).values());
  }

  @Test
  void aBranchHintedOtherwiseIsNewWhateverItsContentOrTag() {
    // A hint is the branch's name, as an Avro type's is: renamed, the branch is another.
    String n = "{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"number\"}}}";
    String t = branch("a", "\"x\":{\"type\":\"number\"}");
    for (String member : new String[] {n, t}) {
      List<ProvenanceVersion> v = compute(
          json("{\"u\":{\"oneOf\":[" + member + "],\"confluent:union\":[{\"name\":\"H1\"}]}}",
              null),
          json("{\"u\":{\"oneOf\":[" + member + "],\"confluent:union\":[{\"name\":\"H2\"}]}}",
              null));
      assertThat(pid(v, 1, 0, 0)).isNotIn(pids(v, 0).values());
    }
  }

  @Test
  void aHintAddedOrRemovedKeepsTheBranch() {
    String n = "{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"number\"}}}";
    String i = "{\"type\":\"object\",\"properties\":{\"i\":{\"type\":\"number\"}}}";
    String plain = "{\"u\":{\"oneOf\":[" + n + "," + i + "]}}";
    String hinted = "{\"u\":{\"oneOf\":[" + n + "," + i + "],"
        + "\"confluent:union\":[{\"name\":\"Card\"},{}]}}";
    for (String[] pair : new String[][] {{plain, hinted}, {hinted, plain}}) {
      List<ProvenanceVersion> v = compute(json(pair[0], null), json(pair[1], null));
      assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
    }
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
  void jsonArrayBranchesAreToldApartByTheirItems() {
    // Both branches are arrays: their items' members tell them apart when they swap.
    String x = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":"
        + "{\"x\":{\"type\":\"number\"}}}}";
    String y = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":"
        + "{\"y\":{\"type\":\"number\"}}}}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + x + "," + y + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + y + "," + x + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 0, 0, 0))).isEqualTo(before.get(path(0, 1, 0, 0)));
    assertThat(after.get(path(0, 1, 0, 0))).isEqualTo(before.get(path(0, 0, 0, 0)));
  }

  @Test
  void jsonBranchesDifferingBelowTheTopAreToldApart() {
    String p = "{\"type\":\"object\",\"properties\":{\"o\":{\"type\":\"object\","
        + "\"properties\":{\"p\":{\"type\":\"number\"}}}}}";
    String q = "{\"type\":\"object\",\"properties\":{\"o\":{\"type\":\"object\","
        + "\"properties\":{\"q\":{\"type\":\"number\"}}}}}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + p + "," + q + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + q + "," + p + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 0, 0, 0))).isEqualTo(before.get(path(0, 1, 0, 0)));
    assertThat(after.get(path(0, 1, 0, 0))).isEqualTo(before.get(path(0, 0, 0, 0)));
  }

  @Test
  void jsonBranchesWithNoMembersKeepTheirPositions() {
    String empty = "{\"type\":\"object\"}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + empty + "," + empty + ",{\"type\":\"string\"}]}}",
            null),
        json("{\"u\":{\"description\":\"v2\",\"oneOf\":[" + empty + "," + empty
            + ",{\"type\":\"string\"}]}}", null));
    assertThat(pids(v, 1)).isEqualTo(pids(v, 0));
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

  @Test
  void aNewBranchDoesNotContinueOneWhoseDiscriminatorChanged() {
    // A's discriminator changes, so A is new; C shares A's member but is no continuation of it.
    String x = "\"x\":{\"type\":\"number\"}";
    String b = "{\"type\":\"object\",\"properties\":{\"y\":{\"type\":\"number\"}}}";
    String c = "{\"type\":\"object\",\"properties\":{" + x + "}}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + branch("a", x) + "," + b + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + branch("z", x) + "," + b + "," + c + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 2))).isNotIn(before.values());
    assertThat(after.get(path(0, 2, 0))).isNotIn(before.values());
  }

  @Test
  void aBranchKeepingItsDiscriminatorContinuesOverOneAtItsPosition() {
    // A gains w and moves; C, sharing x, takes A's old position. A keeps its discriminator.
    String x = "\"x\":{\"type\":\"number\"}";
    String b = "{\"type\":\"object\",\"properties\":{\"y\":{\"type\":\"number\"}}}";
    String c = "{\"type\":\"object\",\"properties\":{" + x + "}}";
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + branch("a", x) + "," + b + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + c + "," + branch("a", x + ",\"w\":{\"type\":\"number\"}")
            + "," + b + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 1))).isEqualTo(before.get(path(0, 0)));
    assertThat(after.get(path(0, 0))).isNotIn(before.values());
  }

  @Test
  void aBranchWhoseDiscriminatorChangedDoesNotContinueAPeerSharingAMember() {
    // A's discriminator changes as P goes: A shares x with both, so it continues neither.
    String x = "\"x\":{\"type\":\"number\"}";
    String p = "{\"type\":\"object\",\"properties\":{" + x + "}}";
    String b = branch("b", "\"y\":{\"type\":\"number\"}");
    List<ProvenanceVersion> v = compute(
        json("{\"u\":{\"oneOf\":[" + p + "," + branch("a", x) + "," + b + "]}}", null),
        json("{\"u\":{\"oneOf\":[" + branch("z", x) + "," + b + "]}}", null));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    assertThat(after.get(path(0, 0))).isNotIn(before.values());
    assertThat(after.get(path(0, 1))).isEqualTo(before.get(path(0, 2)));
  }

  private static String branch(String kind, String members) {
    return "{\"type\":\"object\",\"properties\":{\"kind\":{\"enum\":[\"" + kind + "\"]},"
        + members + "}}";
  }

  @Test
  void aMessageReferencingTheRootLeavesTheRootsFieldsAlone() {
    // The converter keeps the root a reference once a peer uses it; it is still the root.
    ProtobufSchema alone = new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\nmessage Row { int32 a = 1; N n = 2; }\n"
            + "message N { int32 q = 1; }\n");
    ProtobufSchema referenced = new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\nmessage Row { int32 a = 1; N n = 2; }\n"
            + "message N { int32 q = 1; }\nmessage Envelope { Row row = 1; }\n");
    List<ProvenanceVersion> v = compute(alone, referenced, alone);
    for (int version = 1; version < 3; version++) {
      assertThat(pid(v, version, 0)).isEqualTo(pid(v, 0, 0));
      assertThat(pid(v, version, 1)).isEqualTo(pid(v, 0, 1));
      assertThat(pid(v, version, 1, 0)).isEqualTo(pid(v, 0, 1, 0));
    }
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

  @Test
  void aFieldMovedOutOfAOneofIsNew() {
    // The mirror of the move into a oneof, and the direction BACKWARD allows: b changes parent.
    List<ProvenanceVersion> v = compute(
        proto("oneof c { int32 a = 1; string b = 2; }"),
        proto("oneof c { int32 a = 1; } string b = 2;"));
    Map<List<Integer>, Integer> before = pids(v, 0);
    Map<List<Integer>, Integer> after = pids(v, 1);
    // v1: c at [0], a at [0, 0], b at [0, 1]; v2: b at [0], c at [1], a at [1, 0].
    assertThat(after.get(path(1))).isEqualTo(before.get(path(0)));
    assertThat(after.get(path(1, 0))).isEqualTo(before.get(path(0, 0)));
    assertThat(after.get(path(0))).isNotIn(before.values());
  }

  @Test
  void aMemberMovedBetweenOneofsLeavesTheOthersTheirPids() {
    // p shares members with o and with its old self: it takes the one it shares most with that
    // o did not keep. b, which moved between them, is new.
    List<ProvenanceVersion> v = compute(
        proto("oneof o { int32 a = 1; string b = 2; } oneof p { int32 c = 3; int32 d = 4; }"),
        proto("oneof o { int32 a = 1; } oneof p { string b = 2; int32 c = 3; int32 d = 4; }"));
    // v1: o at [0], a, b at [0, 0..1], p at [1], c, d at [1, 0..1]; v2: o at [0], a at [0, 0],
    // p at [1], b, c, d at [1, 0..2].
    assertThat(pid(v, 1, 0)).isEqualTo(pid(v, 0, 0));
    assertThat(pid(v, 1, 1)).isEqualTo(pid(v, 0, 1));
    assertThat(pid(v, 1, 1, 0)).isNotIn(pids(v, 0).values());
    assertThat(pid(v, 1, 1, 1)).isEqualTo(pid(v, 0, 1, 0));
    assertThat(pid(v, 1, 1, 2)).isEqualTo(pid(v, 0, 1, 1));
  }

  @Test
  void twoOneofsMergedContinueTheOneHoldingTheLowestNumber() {
    List<ProvenanceVersion> v = compute(
        proto("oneof o { int32 a = 1; } oneof p { int32 c = 3; }"),
        proto("oneof o { int32 a = 1; int32 c = 3; }"));
    assertThat(pid(v, 1, 0)).isEqualTo(pid(v, 0, 0));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
    assertThat(pid(v, 1, 0, 1)).isNotIn(pids(v, 0).values());
  }

  @Test
  void aOneofSharingSeveralNeverTakesOneAnotherContinues() {
    // p alone continues o; q, sharing more with o, takes what is left: r.
    List<ProvenanceVersion> v = compute(
        proto("oneof o { int32 a = 1; int32 b = 2; int32 c = 3; } oneof r { int32 d = 4; }"),
        proto("oneof p { int32 a = 1; } oneof q { int32 b = 2; int32 c = 3; int32 d = 4; }"));
    assertThat(pid(v, 1, 0)).isEqualTo(pid(v, 0, 0));
    assertThat(pid(v, 1, 1)).isEqualTo(pid(v, 0, 1));
    assertThat(pid(v, 1, 1, 2)).isEqualTo(pid(v, 0, 1, 0));
    assertThat(pid(v, 1, 1, 0)).isNotIn(pids(v, 0).values());
  }

  @Test
  void twoOneofsPickingTheSamePreviousOneLeaveTheOtherToTheLoser() {
    // q and r each share one member with o and one with p; q keeps o (the lower number), and r
    // takes p, which is left, rather than restarting.
    List<ProvenanceVersion> v = compute(
        proto("oneof o { int32 a = 1; int32 b = 2; } oneof p { int32 c = 3; int32 d = 4; }"),
        proto("oneof q { int32 a = 1; int32 c = 3; } oneof r { int32 b = 2; int32 d = 4; }"));
    // v1: o at [0], a, b at [0, 0..1], p at [1], c, d at [1, 0..1]; v2: q at [0], a, c at
    // [0, 0..1], r at [1], b, d at [1, 0..1].
    assertThat(pid(v, 1, 0)).isEqualTo(pid(v, 0, 0));
    assertThat(pid(v, 1, 1)).isEqualTo(pid(v, 0, 1));
    assertThat(pid(v, 1, 1, 1)).isEqualTo(pid(v, 0, 1, 1));
  }

  // --- What a collection holds ----------------------------------------------------------------

  @Test
  void aCollectionWhoseElementsChangeKindIsNew() {
    // A list of structs becoming a list of strings has no SQL ALTER, as a struct becoming a
    // string has none: the collection is new, in every format.
    List<ProvenanceVersion> j = compute(
        json("{\"x\":{\"type\":\"array\",\"items\":{\"type\":\"object\","
            + "\"properties\":{\"a\":{\"type\":\"integer\"}}}}}", null),
        json("{\"x\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}", null));
    assertThat(pid(j, 1, 0)).isNotIn(pids(j, 0).values());
    List<ProvenanceVersion> p = compute(
        proto("repeated M x = 1;\n  message M { int32 a = 1; }"), proto("repeated int32 x = 1;"));
    assertThat(pid(p, 1, 0)).isNotIn(pids(p, 0).values());
    List<ProvenanceVersion> a = compute(
        avro("{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"M\","
            + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}}"),
        avro("{\"type\":\"map\",\"values\":\"int\"}"));
    assertThat(pid(a, 1, 0)).isNotIn(pids(a, 0).values());
  }

  @Test
  void aCollectionWhoseElementsKeepTheirKindContinues() {
    List<ProvenanceVersion> v = compute(
        json("{\"x\":{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}}", null),
        json("{\"x\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}", null));
    assertThat(pid(v, 1, 0)).isEqualTo(pid(v, 0, 0));
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
  void aBranchWithTwoPromotionsContinuesTheOneAvroReadsItInto() {
    // Avro reads an int into the first branch, in union order, it promotes to.
    List<ProvenanceVersion> v = compute(
        avro("[\"int\",\"string\"]"), avro("[\"long\",\"float\",\"string\"]"));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
    assertThat(pid(v, 1, 0, 1)).isNotIn(pids(v, 0).values());

    v = compute(avro("[\"int\",\"string\"]"), avro("[\"float\",\"long\",\"string\"]"));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
    assertThat(pid(v, 1, 0, 1)).isNotIn(pids(v, 0).values());
  }

  @Test
  void aBranchNarrowedWithinItsFamilyStillContinues() {
    // Avro cannot read a long into an int, but the family rule pairs them; restarting would lose
    // the column for no misattribution avoided.
    List<ProvenanceVersion> v = compute(
        avro("[\"long\",\"string\"]"), avro("[\"int\",\"string\"]"));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 0));
  }

  @Test
  void twoBranchesPromotedIntoOneAreNew() {
    List<ProvenanceVersion> v = compute(
        avro("[\"int\",\"long\",\"string\"]"), avro("[\"double\",\"string\"]"));
    assertThat(pid(v, 1, 0, 0)).isNotIn(pids(v, 0).values());
  }

  @Test
  void aBranchAvroMergesIntoAContinuingBranchIsNew() {
    // Avro reads the int into long, which continues the long: the int has no branch of its own.
    List<ProvenanceVersion> v = compute(
        avro("[\"int\",\"long\",\"string\"]"), avro("[\"long\",\"float\",\"string\"]"));
    assertThat(pid(v, 1, 0, 0)).isEqualTo(pid(v, 0, 0, 1));
    assertThat(pid(v, 1, 0, 1)).isNotIn(pids(v, 0).values());
  }

  @Test
  void aOneBranchJsonUnionUnwrappedIsANewColumn() {
    // V1 keeps a bare one-branch oneOf as a union, so unwrapping it changes x's kind, though
    // no document changes: x is new, as the V1 columns are.
    List<ProvenanceVersion> v = compute(
        json("{\"x\":{\"oneOf\":[{\"type\":\"integer\"}]}}", null),
        json("{\"x\":{\"type\":\"integer\"}}", null));
    assertThat(pid(v, 1, 0)).isNotIn(pids(v, 0).values());
  }

  @Test
  void aBranchDroppedAndReAddedIsNew() {
    List<ProvenanceVersion> v = compute(avro("[\"int\",\"string\"]"),
        avro("[\"string\",\"boolean\"]"), avro("[\"int\",\"string\"]", "v3"));
    assertThat(pid(v, 2, 0, 0)).isNotEqualTo(pid(v, 0, 0, 0));
  }

  // -------------------------------------------------------------------------------------------

  private static List<ProvenanceVersion> compute(ParsedSchema... versions) {
    List<SchemaMetadata> history = new ArrayList<>();
    for (int i = 0; i < versions.length; i++) {
      history.add(new SchemaMetadata(i + 1, i + 1, versions[i].schemaType(),
          Collections.emptyList(), ""));
    }
    return ProvenanceHistory.compute("s", history,
        ProvenanceHistory.held(Arrays.asList(versions)), false).getVersions();
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
