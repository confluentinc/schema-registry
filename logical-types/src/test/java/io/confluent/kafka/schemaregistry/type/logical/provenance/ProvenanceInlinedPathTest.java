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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ProvenanceComputer#report}: every member at the path a consumer walks once it
 * has inlined every named type, with its native names and an id allocated per location, so that
 * two uses of one shared type stay apart.
 */
class ProvenanceInlinedPathTest {

  // -------------------------------------------------------------------------------------------
  // Inlining
  // -------------------------------------------------------------------------------------------

  @Test
  void inlinesNamedTypes() {
    assertThat(ids(report(nested()), 0).keySet()).containsExactly(
        path(0),        // id
        path(1),        // addr
        path(1, 0));    // addr.city, reached through the reference
  }

  @Test
  void aSharedNamedTypeIsALocationPerUse() {
    // The single most important property for a consumer with no shared types: home.city and
    // work.city must be told apart. A type is matched where it is used, so each use is its own.
    Map<List<Integer>, Integer> ids = ids(report(twoAddresses()), 0);
    assertThat(ids.get(path(0, 0))).isNotEqualTo(ids.get(path(1, 0)));
  }

  @Test
  void aRecursiveSchemaCannotBeInlined() {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Node", Schema.createStruct(Arrays.asList(
        field("value"),
        new Field("next", Schema.createNamedTypeRef("Node"), 1))));
    assertThatThrownBy(() -> report(new LogicalType(Schema.createNamedTypeRef("Node"), namedTypes)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("recursive named type: Node");
  }

  // -------------------------------------------------------------------------------------------
  // Paths and names
  // -------------------------------------------------------------------------------------------

  @Test
  void pathsAndNamesCoverEveryStepKind() {
    assertThat(report(everyStep()).getVersions().get(0).getMembers()).extracting(
        ProvenanceReport.Member::getPath, ProvenanceReport.Member::getNames).containsExactly(
        tuple(path(0), names("id")),
        tuple(path(1), names("addr")),
        tuple(path(1, 0), names("addr", "acme.Address", "city")), // the entry step, then city
        tuple(path(2), names("items")),
        tuple(path(2, 0, 0), names("items", null, "sku")),        // array element
        tuple(path(3), names("byId")),
        tuple(path(3, 1, 0), names("byId", null, "label")),       // map value
        tuple(path(4), names("choice")),
        tuple(path(4, 0), names("choice", "a")),                 // union branches
        tuple(path(4, 1), names("choice", "b")));
  }

  @Test
  void anEdgeWithNoRecordedStepsLeavesItsNamesUnknown() {
    assertThat(report(lt(struct(new Field("o", struct(field("a")), 0)))).getVersions().get(0)
        .getMembers()).extracting(ProvenanceReport.Member::getNames).containsOnlyNulls();
  }

  @Test
  void membersComeOutInPathOrder() {
    // Lexicographic on the index sequences, which is the pre-order walk -- and the order ids are
    // allocated in, so it is part of the contract rather than an accident of the walk.
    List<List<Integer>> paths = report(everyStep()).getVersions().get(0).getMembers().stream()
        .map(ProvenanceReport.Member::getPath).collect(Collectors.toList());

    List<List<Integer>> sorted = paths.stream().sorted((a, b) -> {
      for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
        int cmp = Integer.compare(a.get(i), b.get(i));
        if (cmp != 0) {
          return cmp;
        }
      }
      return Integer.compare(a.size(), b.size());
    }).collect(Collectors.toList());

    assertThat(paths).isEqualTo(sorted);
  }

  @Test
  void aCollectionStepIsNoMember() {
    // items -> sku, with nothing for the element itself: a step is not a location.
    assertThat(ids(report(lt(struct(arrayOf("items", struct(field("sku")))))), 0).keySet())
        .containsExactly(path(0), path(0, 0, 0));
  }

  @Test
  void aRetypedCollectionIsNewWithItsMembers() {
    // ARRAY<ROW> to MAP<K, ROW>: a change of category, so the field and its members are new.
    ProvenanceReport report = ProvenanceComputer.report(Arrays.asList(
        lt(struct(arrayOf("items", struct(field("sku"))))),
        lt(struct(mapOf("items", struct(field("sku")))))), IdentityPolicy.AVRO);

    assertThat(ids(report, 1).get(path(0))).isNotIn(ids(report, 0).values());
    assertThat(ids(report, 1).get(path(0, 1, 0))).isNotIn(ids(report, 0).values());
  }

  @Test
  void aHistoryChangingFormatStartsEveryLocationAfresh() {
    // Versions of different policies match nothing, whatever their names say; the versions after
    // the change match each other again.
    LogicalType ab = lt(struct(field("a"), field("b")));
    ProvenanceReport report = ProvenanceComputer.report(Arrays.asList(ab, ab, ab),
        Arrays.asList(IdentityPolicy.AVRO, IdentityPolicy.JSON, IdentityPolicy.JSON));

    assertThat(ids(report, 0)).containsExactly(entryOfId(path(0), 1), entryOfId(path(1), 2));
    assertThat(ids(report, 1)).containsExactly(entryOfId(path(0), 3), entryOfId(path(1), 4));
    assertThat(ids(report, 2)).containsExactly(entryOfId(path(0), 3), entryOfId(path(1), 4));
  }

  // -------------------------------------------------------------------------------------------
  // report() -- what a provenance endpoint serves
  // -------------------------------------------------------------------------------------------

  @Test
  void reportDerivesIcebergStyleIds() {
    ProvenanceReport report = ProvenanceComputer.report(Arrays.asList(
        lt(struct(field("id"), field("name"))),
        lt(struct(field("id"), field("full_name", "name"))),   // rename
        lt(struct(field("id"))),                               // drop
        lt(struct(field("id"), field("full_name")))),          // re-add
        IdentityPolicy.AVRO);

    assertThat(ids(report, 0)).containsExactly(entryOfId(path(0), 1), entryOfId(path(1), 2));
    // A rename keeps its id, which is what lets Iceberg treat it as the same column.
    assertThat(ids(report, 1)).containsExactly(entryOfId(path(0), 1), entryOfId(path(1), 2));
    assertThat(ids(report, 2)).containsExactly(entryOfId(path(0), 1));
    // Re-added under the same name: absent from v2, so a new id. 2 is never reused.
    assertThat(ids(report, 3)).containsExactly(entryOfId(path(0), 1), entryOfId(path(1), 3));

    assertThat(report.getLastId()).isEqualTo(3);
    assertThat(report.getVersions()).extracting(ProvenanceReport.Version::getIndex)
        .containsExactly(0, 1, 2, 3);
  }

  @Test
  void reportGivesSharedTypesOneIdPerUseSite() {
    // One definition-level rename, two inlined locations, each keeping its own id. Allocating per
    // entity instead would give home.city and work.city the same id and let a consumer pair them.
    ProvenanceReport report = ProvenanceComputer.report(
        Arrays.asList(twoAddresses(), twoAddressesRenamed()), IdentityPolicy.AVRO);

    assertThat(ids(report, 0)).containsExactly(
        entryOfId(path(0), 1), entryOfId(path(0, 0), 2), entryOfId(path(0, 1), 3),
        entryOfId(path(1), 4), entryOfId(path(1, 0), 5), entryOfId(path(1, 1), 6));
    // zip became postcode in the one shared definition; both copies keep their own id.
    assertThat(ids(report, 1)).containsExactly(
        entryOfId(path(0), 1), entryOfId(path(0, 0), 2), entryOfId(path(0, 1), 3),
        entryOfId(path(1), 4), entryOfId(path(1, 0), 5), entryOfId(path(1, 1), 6));
    assertThat(report.getLastId()).isEqualTo(6);
  }

  @Test
  void reportCarriesNamesAlongsideIds() {
    ProvenanceReport report = ProvenanceComputer.report(
        Arrays.asList(twoAddresses()), IdentityPolicy.AVRO);

    assertThat(report.getVersions().get(0).getMembers()).extracting(
        ProvenanceReport.Member::getNames, ProvenanceReport.Member::getId).contains(
        tuple(names("home", "city"), 2),
        tuple(names("work", "city"), 5));
  }

  @Test
  void anEmptyHistoryReportsNothing() {
    ProvenanceReport report = ProvenanceComputer.report(Arrays.asList(), IdentityPolicy.AVRO);
    assertThat(report.getVersions()).isEmpty();
    assertThat(report.getLastId()).isZero();
  }

  // -------------------------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------------------------

  @Test
  void schemaTypeSelectsThePolicy() {
    assertThat(IdentityPolicy.forSchemaType("AVRO")).isEqualTo(IdentityPolicy.AVRO);
    assertThat(IdentityPolicy.forSchemaType("PROTOBUF")).isEqualTo(IdentityPolicy.PROTOBUF);
    assertThat(IdentityPolicy.forSchemaType("JSON")).isEqualTo(IdentityPolicy.JSON);
    // Never guesses.
    assertThatThrownBy(() -> IdentityPolicy.forSchemaType("XML"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("XML");
    assertThatThrownBy(() -> IdentityPolicy.forSchemaType(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static ProvenanceReport report(LogicalType version) {
    return ProvenanceComputer.report(Arrays.asList(version), IdentityPolicy.AVRO);
  }

  private static Map<List<Integer>, Integer> ids(ProvenanceReport report, int version) {
    return report.getVersions().get(version).getMembers().stream().collect(Collectors.toMap(
        ProvenanceReport.Member::getPath, ProvenanceReport.Member::getId, (a, b) -> a,
        LinkedHashMap::new));
  }

  private static org.assertj.core.groups.Tuple tuple(Object... values) {
    return org.assertj.core.groups.Tuple.tuple(values);
  }

  /** A root exercising every kind of path step. */
  private static LogicalType everyStep() {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("acme.Address", struct(field("city")));
    return new LogicalType(struct(
        field("id"),
        // An entry step, as an Avro collapsed union records it: spelled only on the way down.
        new Field("addr", Schema.createNamedTypeRef("acme.Address")
            .setNativeEntryNames(names("acme.Address")), 1).setNativeNames(names("addr")),
        arrayOf("items", struct(field("sku"))),
        mapOf("byId", struct(field("label"))),
        new Field("choice", Schema.createUnion(Arrays.asList(
            new UnionBranch("a", Schema.create(Schema.Type.INT)).setNativeNames(names("a")),
            new UnionBranch("b", Schema.create(Schema.Type.INT)).setNativeNames(names("b")))), 4)
            .setNativeNames(names("choice"))), namedTypes);
  }

  /** {@code acme.Order { id, addr: acme.Address { city } }}, rooted at a reference. */
  private static LogicalType nested() {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("acme.Order", struct(
        field("id"),
        new Field("addr", Schema.createNamedTypeRef("acme.Address"), 1)));
    namedTypes.put("acme.Address", struct(field("city")));
    return new LogicalType(Schema.createNamedTypeRef("acme.Order"), namedTypes);
  }

  private static LogicalType twoAddresses() {
    return sharedAddress("zip");
  }

  private static LogicalType twoAddressesRenamed() {
    return sharedAddress("postcode", "zip");
  }

  /** A root of two fields sharing one named type: {@code { home: Address, work: Address }}. */
  private static LogicalType sharedAddress(String second, String... aliases) {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", struct(field("city"), field(second, aliases)));
    return new LogicalType(struct(
        new Field("home", Schema.createNamedTypeRef("Address"), 0).setNativeNames(names("home")),
        new Field("work", Schema.createNamedTypeRef("Address"), 1).setNativeNames(names("work"))),
        namedTypes);
  }

  private static Field arrayOf(String name, Schema element) {
    return new Field(name, Schema.createArray(element).setElementNativeNames(names((String) null)),
        0).setNativeNames(names(name));
  }

  private static Field mapOf(String name, Schema value) {
    return new Field(name, Schema.createMap(Schema.createString(), value)
        .setKeyNativeNames(names()).setValueNativeNames(names((String) null)), 0)
        .setNativeNames(names(name));
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
  }

  private static List<String> names(String... parts) {
    return Arrays.asList(parts);
  }

  private static Map.Entry<List<Integer>, Integer> entryOfId(List<Integer> path, int id) {
    return new AbstractMap.SimpleEntry<>(path, id);
  }

  private static LogicalType lt(Schema root) {
    return new LogicalType(root);
  }

  private static Schema struct(Field... fields) {
    return Schema.createStruct(Arrays.asList(fields));
  }

  private static Field field(String name, String... aliases) {
    Map<String, Object> params = aliases.length == 0
        ? null : Map.of(Schema.AVRO_ALIASES, String.join(",", aliases));
    return new Field(name, Schema.create(Schema.Type.INT), 0, null, false, null, null, params)
        .setNativeNames(names(name));
  }
}
