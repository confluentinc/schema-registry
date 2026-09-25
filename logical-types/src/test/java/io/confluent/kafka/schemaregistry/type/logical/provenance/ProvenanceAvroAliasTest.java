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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Provenance follows Avro's own alias semantics, as {@code avro_aliases.md} records them from
 * Avro's decoder and compatibility checker: an alias names a writer's actual name and beats an
 * exact match; a type continues location by location; and where Avro itself cannot say which
 * field an old one became, the history is ambiguous.
 */
class ProvenanceAvroAliasTest {

  private static final String I = "\"int\"";
  private static final String S = "\"string\"";

  @Test
  void aRenamedFieldTakesTheValueAndANewFieldItsOldName() {
    List<Map<String, Integer>> pids = pids(
        avro(f("name", S)),
        avro(fa("full_name", S, "name"), f("name", S)),
        avro(fa("full_name", S, "name"), f("name", S), f("extra", I)));
    assertThat(pids.get(1).get("full_name")).isEqualTo(pids.get(0).get("name"));
    assertThat(pids.get(1).get("name")).isNotEqualTo(pids.get(0).get("name"));
    // The alias kept in the next version is carried forward, not a second claim.
    assertThat(pids.get(2).get("full_name")).isEqualTo(pids.get(1).get("full_name"));
    assertThat(pids.get(2).get("name")).isEqualTo(pids.get(1).get("name"));
  }

  @Test
  void whatAvroCannotResolveIsAmbiguous() {
    // A swap (the checker throws), two fields aliasing one (the decoder takes the last), one
    // field aliasing two present ones (a duplicate field).
    assertAmbiguous(avro(f("a", I), f("b", I)), avro(fa("b", I, "a"), fa("a", I, "b")));
    assertAmbiguous(avro(f("a", I)), avro(fa("b", I, "a"), fa("c", I, "a")));
    assertAmbiguous(avro(f("a", I), f("b", I)), avro(fa("c", I, "a", "b")));
  }

  @Test
  void anAliasNamesWhatTheWriterWasActuallyCalled() {
    // Its own name: nothing. A name never seen: nothing. A former alias: nothing. A former
    // canonical name, two renames back: the identity.
    assertThat(same(pids(avro(f("a", I)), avro(fa("a", I, "a"))), "a", "a")).isTrue();
    assertThat(same(pids(avro(f("a", I)), avro(fa("c", I, "a", "nope"))), "a", "c")).isTrue();
    assertThat(same(pids(avro(fa("a", I, "x")), avro(fa("b", I, "x"))), "a", "b")).isFalse();
    List<Map<String, Integer>> chain = pids(avro(f("a", I)), avro(fa("b", I, "a")),
        avro(fa("c", I, "a")));
    assertThat(chain.get(2).get("c")).isEqualTo(chain.get(0).get("a"));
  }

  @Test
  void anAliasToADroppedFieldReconnectsOnANewInterval() {
    List<Map<String, Integer>> pids = pids(avro(f("a", I)), avro(f("z", I)),
        avro(fa("b", I, "a"), f("z", I)));
    assertThat(pids.get(2).get("b")).isNotEqualTo(pids.get(0).get("a"));
  }

  @Test
  void anAliasKeptWhileANewFieldTakesTheAliasedName() {
    List<Map<String, Integer>> pids = pids(avro(fa("b", I, "a")), avro(fa("b", I, "a"), f("a", I)));
    assertThat(pids.get(1).get("b")).isEqualTo(pids.get(0).get("b"));
    assertThat(pids.get(1)).containsKey("a");
    assertThat(pids.get(0)).doesNotContainKey("a");
  }

  @Test
  void twoTypesMergedByAliasKeepEachLocation() {
    String addr = rec("Address", f("street", S));
    String bill = rec("BillingAddress", f("line1", S) + "," + f("zip", S));
    String merged = rec("Address", fa("street", S, "line1"), "BillingAddress");
    List<Map<String, Integer>> plain = pids(avro(f("shipping", addr), f("billing", bill)),
        avro(f("shipping", merged), f("billing", "\"Address\"")));
    assertThat(same(plain, "shipping.street", "shipping.street")).isTrue();
    assertThat(same(plain, "billing.line1", "billing.street")).isTrue();

    List<Map<String, Integer>> inUnions = pids(
        avro(f("shipping", u(addr)), f("billing", u(bill))),
        avro(f("shipping", u(merged)), f("billing", u("\"Address\""))));
    assertThat(same(inUnions, "shipping.Address.street", "shipping.Address.street")).isTrue();
    assertThat(same(inUnions, "billing.BillingAddress.line1", "billing.Address.street")).isTrue();
  }

  @Test
  void oneTypeSplitInTwoKeepsEachLocation() {
    String a = rec("A", f("x", I));
    String c = rec("C", f("x", I), "A");
    List<Map<String, Integer>> plain = pids(avro(f("u", a), f("v", "\"A\"")),
        avro(f("u", a), f("v", c)));
    assertThat(same(plain, "u.x", "u.x") && same(plain, "v.x", "v.x")).isTrue();
    List<Map<String, Integer>> inUnions = pids(avro(f("u", u(a)), f("v", u("\"A\""))),
        avro(f("u", u(a)), f("v", u(c))));
    assertThat(same(inUnions, "u.A.x", "u.A.x") && same(inUnions, "v.A.x", "v.C.x")).isTrue();
    // Both new types aliasing the old one: each location still has a single candidate.
    List<Map<String, Integer>> both = pids(avro(f("u", a), f("v", "\"A\"")),
        avro(f("u", rec("B", f("x", I), "A")), f("v", c)));
    assertThat(same(both, "u.x", "u.x") && same(both, "v.x", "v.x")).isTrue();
  }

  @Test
  void twoTypesSwappedByAliasesKeepEachLocation() {
    List<Map<String, Integer>> pids = pids(
        avro(f("u", u(rec("A", f("x", I)))), f("v", u(rec("B", f("y", I))))),
        avro(f("u", u(rec("B", f("x", I), "A"))), f("v", u(rec("A", f("y", I), "B")))));
    assertThat(same(pids, "u.A.x", "u.B.x") && same(pids, "v.B.y", "v.A.y")).isTrue();
  }

  @Test
  void aTypeRenamedWhileANewTypeTakesItsNameInAUnion() {
    String renamed = rec("A2", f("x", I), "A");
    String reused = rec("A", f("q", S));
    // A real union on both sides: V1 collapses [null, A], which adds no branch step.
    List<Map<String, Integer>> pids = pids(avro(f("u", "[\"null\"," + S + "," + rec("A", f("x", I))
            + "]")),
        avro(f("u", "[\"null\"," + S + "," + renamed + "," + reused + "]")),
        avro(f("u", "[\"null\"," + S + "," + renamed + "," + reused + "]"), f("extra", I)));
    assertThat(same(pids, "u.A.x", "u.A2.x")).isTrue();
    assertThat(pids.get(0)).doesNotContainKey("u.A.q");
    assertThat(pids.get(2).get("u.A2.x")).isEqualTo(pids.get(1).get("u.A2.x"));
    assertThat(pids.get(2).get("u.A.q")).isEqualTo(pids.get(1).get("u.A.q"));
  }

  @Test
  void aTypeRenamedWithoutAnAliasOrToAnotherNamespaceIsNew() {
    assertThat(same(pids(avro(f("u", rec("A", f("x", I)))), avro(f("u", rec("B", f("x", I))))),
        "u.x", "u.x")).isFalse();
    // An unqualified alias takes the aliasing type's namespace: n2.B aliased A means n2.A.
    assertThat(same(pids(
        avro(f("u", "{\"type\":\"record\",\"name\":\"A\",\"namespace\":\"n1\",\"fields\":["
            + f("x", I) + "]}")),
        avro(f("u", "{\"type\":\"record\",\"name\":\"B\",\"namespace\":\"n2\",\"aliases\":"
            + "[\"A\"],\"fields\":[" + f("x", I) + "]}"))), "u.x", "u.x")).isFalse();
  }

  @Test
  void aFixedBranchRenamedByAnAliasContinues() {
    // A fixed has no named type in the logical type; its aliases still carry its branch.
    String f2 = "{\"type\":\"fixed\",\"name\":\"F2\",\"size\":4}";
    List<Map<String, Integer>> pids = pids(
        avro(f("u", "[\"null\",{\"type\":\"fixed\",\"name\":\"F1\",\"size\":4}," + f2 + "]")),
        avro(f("u", "[\"null\",{\"type\":\"fixed\",\"name\":\"G1\",\"size\":4,"
            + "\"aliases\":[\"F1\"]}," + f2 + "]")));
    assertThat(same(pids, "u.F1", "u.G1")).isTrue();
    assertThat(same(pids, "u.F2", "u.F2")).isTrue();
  }

  @Test
  void aTypeMovedOutOfTheNullNamespaceContinuesByADottedAlias() {
    // Avro spells an alias in the null namespace ".A" when the aliasing type has a namespace.
    List<Map<String, Integer>> pids = pids(
        avro(f("u", "[\"string\"," + rec("A", f("x", I)) + "]")),
        avro(f("u", "[\"string\",{\"type\":\"record\",\"name\":\"B\",\"namespace\":\"n\","
            + "\"aliases\":[\".A\"],\"fields\":[" + f("x", I) + "]}]")));
    assertThat(same(pids, "u.A.x", "u.n.B.x")).isTrue();
  }

  @Test
  void aBranchKeepsItsIdentityWhenItsSimpleNameStopsColliding() {
    // The logical type names a branch by its simple name unless another shares it; Avro, and so
    // provenance, by its full name.
    String n1 = "{\"type\":\"record\",\"name\":\"A\",\"namespace\":\"n1\",\"fields\":["
        + f("x", I) + "]}";
    String n2 = "{\"type\":\"record\",\"name\":\"A\",\"namespace\":\"n2\",\"fields\":["
        + f("x", I) + "]}";
    String renamed = "{\"type\":\"record\",\"name\":\"B\",\"namespace\":\"n1\",\"aliases\":"
        + "[\"n1.A\"],\"fields\":[" + f("x", I) + "]}";
    List<Map<String, Integer>> pids = pids(avro(f("u", "[" + n1 + "," + n2 + "]")),
        avro(f("u", "[" + renamed + "," + n2 + "]")));
    assertThat(same(pids, "u.n1.A.x", "u.n1.B.x")).isTrue();
    assertThat(same(pids, "u.n2.A.x", "u.n2.A.x")).isTrue();
  }

  @Test
  void aBranchNamedByAHintIsIdentifiedByItsType() {
    // A confluent:union hint renames a branch in the logical type only; Avro finds it by type.
    String hinted = "{\"name\":\"u\",\"type\":[\"int\",\"string\"],"
        + "\"confluent:union\":[{\"name\":\"%s\"},{\"name\":\"%s\"}]}";
    List<Map<String, Integer>> renamed = pids(avro(String.format(hinted, "num", "txt")),
        avro(String.format(hinted, "number", "text")));
    assertThat(same(renamed, "u.int", "u.int") && same(renamed, "u.string", "u.string")).isTrue();
    // And a hinted int branch still promotes to long.
    List<Map<String, Integer>> promoted = pids(avro(String.format(hinted, "num", "txt")),
        avro(String.format(hinted, "num", "txt").replace("[\"int\",", "[\"long\",")));
    assertThat(same(promoted, "u.int", "u.long")).isTrue();
  }

  @Test
  void aLocationWhoseTypeChangesAndChangesBackStartsOver() {
    String a = rec("A", f("x", I));
    List<Map<String, Integer>> avro = pids(avro(f("w", a), f("u", "\"A\"")),
        avro(f("w", a), f("u", rec("B", f("x", I)))), avro(f("w", a), f("u", "\"A\"")));
    assertThat(avro.get(2).get("u.x")).isNotEqualTo(avro.get(0).get("u.x"));
    assertThat(avro.get(2).get("w.x")).isEqualTo(avro.get(0).get("w.x"));

    List<Map<String, Integer>> proto = pids(proto("A"), proto("B"), proto("A"));
    assertThat(proto.get(2).get("u.x")).isNotEqualTo(proto.get(0).get("u.x"));
    assertThat(proto.get(2).get("w.x")).isEqualTo(proto.get(0).get("w.x"));
  }

  // -------------------------------------------------------------------------------------------

  @Test
  void aNestedTypeWhoseNamespaceChangesWithoutAnAliasContinues() {
    // Renaming the root into another namespace moves the nested types inheriting it, with no
    // alias; Avro's checker compares short names, and so does provenance where names fail.
    String nested = "{\"name\":\"o\",\"type\":{\"type\":\"record\",\"name\":\"N\","
        + "\"fields\":[" + f("x", I) + "]}}";
    List<Map<String, Integer>> pids = pids(
        new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"a.b\","
            + "\"fields\":[" + nested + "]}"),
        new AvroSchema("{\"type\":\"record\",\"name\":\"R2\",\"namespace\":\"a.c\","
            + "\"aliases\":[\"a.b.R\"],\"fields\":[" + nested + "]}"));
    assertThat(same(pids, "o.x", "o.x")).isTrue();
  }

  @Test
  void aBranchWhoseNamespaceChangesWithoutAnAliasContinues() {
    List<Map<String, Integer>> pids = pids(
        avro(f("u", "[\"null\",\"string\"," + nsRec("n1", "A") + "]")),
        avro(f("u", "[\"null\",\"string\"," + nsRec("n2", "A") + "]")));
    assertThat(same(pids, "u.n1.A", "u.n2.A")).isTrue();
    assertThat(same(pids, "u.n1.A.x", "u.n2.A.x")).isTrue();
  }

  @Test
  void aShortNameSharedByTwoBranchesContinuesNeither() {
    // n2.A continues by its name; n3.A could be n1.A only by a short name n2.A shares.
    List<Map<String, Integer>> pids = pids(
        avro(f("u", "[\"null\"," + nsRec("n1", "A") + "," + nsRec("n2", "A") + "]")),
        avro(f("u", "[\"null\"," + nsRec("n3", "A") + "," + nsRec("n2", "A") + "]")));
    assertThat(same(pids, "u.n2.A", "u.n2.A")).isTrue();
    assertThat(same(pids, "u.n1.A", "u.n3.A")).isFalse();
  }

  @Test
  void aTypeRenamedWithoutAnAliasStillRestarts() {
    List<Map<String, Integer>> pids = pids(
        avro(f("o", rec("N", f("x", I)))),
        avro(f("o", rec("M", f("x", I)))));
    assertThat(same(pids, "o.x", "o.x")).isFalse();
  }

  private static String nsRec(String namespace, String name) {
    return "{\"type\":\"record\",\"name\":\"" + name + "\",\"namespace\":\"" + namespace
        + "\",\"fields\":[" + f("x", I) + "]}";
  }

  @Test
  void aTypeNestedInTheRootsNameLeavesTheRootsFieldsAlone() {
    // A type named inside the root (as a reflect nested class is) keeps the root a reference in
    // the logical type; dropping or adding one must not move the root's own fields.
    String e = "{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"namespace\":\"q.R\","
        + "\"symbols\":[\"X\"]}}";
    String n = "{\"name\":\"n\",\"type\":\"string\"}";
    List<Map<String, Integer>> pids = pids(
        new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"q\",\"fields\":[" + n
            + "," + e + "]}"),
        new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"q\",\"fields\":[" + n
            + "]}"));
    assertThat(same(pids, "n", "n")).isTrue();
  }

  @Test
  void aFixedBranchWhoseNamespaceChangesWithoutAnAliasContinues() {
    // A fixed is a binary in the logical type; its native step still carries its short name.
    List<Map<String, Integer>> pids = pids(
        avro("{\"name\":\"u\",\"type\":[\"null\",\"string\",{\"type\":\"fixed\",\"name\":\"F\","
            + "\"namespace\":\"n1\",\"size\":4}]}"),
        avro("{\"name\":\"u\",\"type\":[\"null\",\"string\",{\"type\":\"fixed\",\"name\":\"F\","
            + "\"namespace\":\"n2\",\"size\":4}]}"));
    assertThat(same(pids, "u.n1.F", "u.n2.F")).isTrue();
  }

  @Test
  void aMalformedUnionHintIsRejectedByName() {
    for (String hint : new String[] {"{\"x\":1}", "\"text\"", "[\"a\",\"b\"]",
        "[{\"name\":1},{\"name\":2}]", "[null,null]", "[{\"name\":\"a\",\"doc\":5},{}]"}) {
      assertThatThrownBy(() -> pids(avro("{\"name\":\"u\",\"type\":[\"int\",\"string\"],"
          + "\"confluent:union\":" + hint + "}")))
          .as(hint).isInstanceOf(ValidationException.class);
    }
  }

  private static void assertAmbiguous(ParsedSchema... versions) {
    assertThatThrownBy(() -> pids(versions)).isInstanceOf(AmbiguousProvenanceException.class);
  }

  /** Whether the location spelled {@code first} in the first version continues as {@code last}. */
  private static boolean same(List<Map<String, Integer>> pids, String first, String last) {
    Integer before = pids.get(0).get(first);
    assertThat(before).as(first).isNotNull();
    return before.equals(pids.get(pids.size() - 1).get(last));
  }

  /** Every version's pids, keyed by the location's names joined with dots. */
  private static List<Map<String, Integer>> pids(ParsedSchema... versions) {
    List<ProvenanceHistory.Entry> entries = new ArrayList<>();
    for (int i = 0; i < versions.length; i++) {
      entries.add(new ProvenanceHistory.Entry(i + 1, i + 1, false));
    }
    List<Map<String, Integer>> pids = new ArrayList<>();
    for (ProvenanceVersion version : ProvenanceHistory.compute("s", entries,
        Arrays.asList(versions), false).getVersions()) {
      Map<String, Integer> byNames = new HashMap<>();
      for (ProvenanceField field : version.getFields()) {
        byNames.put(String.join(".", field.getNames()), field.getPid());
      }
      pids.add(byNames);
    }
    return pids;
  }

  private static AvroSchema avro(String... fields) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + String.join(",", fields) + "]}");
  }

  private static ProtobufSchema proto(String uType) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage R {\n  A w = 1;\n  "
        + uType + " u = 2;\n}\nmessage A {\n  int32 x = 1;\n}\nmessage B {\n  int32 x = 1;\n}\n");
  }

  private static String u(String branch) {
    return "[\"null\"," + branch + "]";
  }

  private static String f(String name, String type) {
    return "{\"name\":\"" + name + "\",\"type\":" + type + "}";
  }

  private static String fa(String name, String type, String... aliases) {
    return "{\"name\":\"" + name + "\",\"type\":" + type + ",\"aliases\":[\""
        + String.join("\",\"", aliases) + "\"]}";
  }

  private static String rec(String name, String fields, String... aliases) {
    return "{\"type\":\"record\",\"name\":\"" + name + "\"" + (aliases.length > 0
        ? ",\"aliases\":[\"" + String.join("\",\"", aliases) + "\"]" : "")
        + ",\"fields\":[" + fields + "]}";
  }
}
