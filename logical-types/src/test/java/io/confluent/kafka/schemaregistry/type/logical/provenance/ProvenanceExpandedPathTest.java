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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the inlined views — {@link ProvenanceResult#expandedProvenance},
 * {@link ProvenanceResult#expandedCorrespondence} and {@link ProvenanceResult#positionMapping} —
 * which re-key provenance from definition sites onto the paths a consumer walks once it has
 * expanded every named type.
 */
class ProvenanceExpandedPathTest {

  @Test
  void expandedProvenanceInlinesNamedTypes() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(nested()));

    Map<List<Integer>, Provenance> expanded = result.expandedProvenance(0);
    assertThat(expanded.keySet()).containsExactly(
        path(0),        // id
        path(1),        // addr
        path(1, 0));    // addr.city, reached through the reference

    // The same entities, addressed at their definition sites.
    assertThat(expanded.get(path(1, 0)))
        .isEqualTo(result.at(0, PathKey.ofNamedType("acme.Address").child(0)));
    // Named types are definitions, not locations, so they have no expanded path.
    assertThat(expanded.values()).doesNotContain(result.at(0, PathKey.ofNamedType("acme.Address")));
  }

  @Test
  void aSharedNamedTypeIsInlinedAtEveryUseSite() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(shared()));

    Map<List<Integer>, Provenance> expanded = result.expandedProvenance(0);
    assertThat(expanded.keySet()).containsExactly(path(0), path(0, 0), path(1), path(1, 0));
    // One logical entity, two physical locations.
    assertThat(expanded.get(path(0, 0))).isEqualTo(expanded.get(path(1, 0)));
  }

  @Test
  void expandedCorrespondenceKeepsUseSitesApartForASharedType() {
    // v1 swaps the two fields that share type T. Both copies of T.x carry one provenance, so
    // joining the two inlined maps on provenance could not tell the use sites apart -- only the
    // prefix does. a.x must map to a.x and b.x to b.x, not crosswise.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(shared(), sharedSwapped()));

    Map<List<Integer>, List<Integer>> expanded = result.expandedCorrespondence(1, 0);
    assertThat(expanded).containsOnly(
        entryOf(path(0), path(1)),          // target b   <- source b
        entryOf(path(0, 0), path(1, 0)),    // target b.x <- source b.x
        entryOf(path(1), path(0)),          // target a   <- source a
        entryOf(path(1, 0), path(0, 0)));   // target a.x <- source a.x
  }

  @Test
  void expandedCorrespondenceFollowsARenameThroughAReference() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        nestedWithCity("city"), nestedWithCity("town", "city")));

    assertThat(result.expandedCorrespondence(1, 0)).containsEntry(path(1, 0), path(1, 0));
  }

  @Test
  void positionMappingMarksColumnsTheSourceNeverHad() {
    // The rename has to land in its own version: a field aliasing a name a live sibling still
    // holds claims two identities at once, which is rejected rather than projected.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("id"), field("name"))),
        lt(struct(field("id"), field("full_name", "name"))),
        lt(struct(field("id"), field("full_name"), field("name")))));

    // Reading a v0 record into the v2 row: name feeds full_name, and v2's name has no source.
    assertThat(result.positionMappings(2, 0).get(path()).getSourcePositions())
        .containsExactly(0, 1, ProvenanceResult.ABSENT);
    // The other direction has nothing to fill: every v0 column is fed by v2.
    assertThat(result.positionMappings(0, 2).get(path()).getSourcePositions())
        .containsExactly(0, 1);
  }

  @Test
  void positionMappingsSeeThroughAReferencedRoot() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(nested(), nested()));
    assertThat(result.positionMappings(1, 0).get(path()).getSourcePositions())
        .containsExactly(0, 1);
  }

  @Test
  void aRecursiveSchemaCannotBeInlined() {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Node", Schema.createStruct(Arrays.asList(
        field("value"),
        new Field("next", Schema.createNamedTypeRef("Node"), 1))));
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        new LogicalType(Schema.createNamedTypeRef("Node"), namedTypes)));

    assertThatThrownBy(() -> result.expandedProvenance(0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("recursive named type: Node");
  }

  @Test
  void theComputedSchemasAreRetained() {
    LogicalType v0 = lt(struct(field("a")));
    LogicalType v1 = lt(struct(field("a"), field("b")));
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(v0, v1));

    assertThat(result.version(0)).isSameAs(v0);
    assertThat(result.versions()).containsExactly(v0, v1);
  }

  @Test
  void pathKeyExposesItsPosition() {
    PathKey nested = PathKey.ofNamedType("acme.Order").child(3).child(1);
    assertThat(nested.position()).isEqualTo(1);
    assertThat(nested.isRoot()).isFalse();

    assertThat(PathKey.ofRoot().isRoot()).isTrue();
    assertThatThrownBy(PathKey.ofRoot()::position).isInstanceOf(IllegalStateException.class);
  }

  // -------------------------------------------------------------------------------------------
  // Nested projection -- what a plan builder reads to project below the top-level row
  // -------------------------------------------------------------------------------------------

  @Test
  void expandedCorrespondenceDescendsIntoArrayElements() {
    // items keeps its identity, so the element struct stays in scope and its fields survive a
    // reorder. The element step is 0, giving the inner fields paths of the form [items, 0, field].
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(arrayOf("items", struct(field("sku"), field("qty"), field("note"))))),
        lt(struct(arrayOf("items", struct(field("qty"), field("sku")))))));

    assertThat(result.expandedCorrespondence(1, 0)).containsOnly(
        entryOf(path(0), path(0)),
        entryOf(path(0, 0, 0), path(0, 0, 1)),     // qty moved from index 1 to 0
        entryOf(path(0, 0, 1), path(0, 0, 0)));    // sku moved from index 0 to 1

    // Read the other way, note has no counterpart and simply never appears.
    assertThat(result.expandedCorrespondence(0, 1))
        .containsEntry(path(0, 0, 0), path(0, 0, 1))
        .doesNotContainKey(path(0, 0, 2));
  }

  @Test
  void expandedCorrespondenceDescendsIntoMapValues() {
    // A map appends 0 for its key and 1 for its value, so a row-typed value sits under [field, 1].
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(mapOf("byId", struct(field("name"), field("age"))))),
        lt(struct(mapOf("byId", struct(field("age")))))));

    assertThat(result.expandedCorrespondence(1, 0)).containsOnly(
        entryOf(path(0), path(0)),
        entryOf(path(0, 1, 0), path(0, 1, 1)));    // age moved from index 1 to 0
  }

  @Test
  void expandedCorrespondenceDescendsThroughReferencedRows() {
    // The nested row is a named type, so its members are keyed under it at their definition site.
    // Inlining is what turns those into the [addr, field] paths a RowType walk produces.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        orderWithAddress("city", "zip"),
        orderWithAddress("zip", "city")));

    assertThat(result.expandedCorrespondence(1, 0)).containsOnly(
        entryOf(path(0), path(0)),
        entryOf(path(1), path(1)),
        entryOf(path(1, 0), path(1, 1)),
        entryOf(path(1, 1), path(1, 0)));
    // Definition-site keying puts the same two fields under the named type instead.
    assertThat(result.correspondence(1, 0)).containsEntry(
        PathKey.ofNamedType("acme.Address").child(0),
        PathKey.ofNamedType("acme.Address").child(1));
  }

  @Test
  void aNestedRowAddedInTheTargetHasNoSourceAtAnyDepth() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("id"))),
        lt(struct(field("id"), new Field("addr", struct(field("city")), 1)))));

    Map<List<Integer>, List<Integer>> expanded = result.expandedCorrespondence(1, 0);
    assertThat(expanded).containsOnly(entryOf(path(0), path(0)));
    // Absent at every depth, so a plan marks the whole column absent rather than descending.
    assertThat(expanded).doesNotContainKey(path(1)).doesNotContainKey(path(1, 0));
    assertThat(result.positionMappings(1, 0).get(path()).getSourcePositions())
        .containsExactly(0, ProvenanceResult.ABSENT);
  }

  @Test
  void aRetypedCollectionStopsTheDescent() {
    // ARRAY<ROW> to MAP<K, ROW>: the members inside move to a different scope, so they are not the
    // same entities and no path pair below the field is expressible.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(arrayOf("items", struct(field("sku"))))),
        lt(struct(mapOf("items", struct(field("sku")))))));

    assertThat(result.expandedCorrespondence(1, 0)).containsOnly(entryOf(path(0), path(0)));
  }

  // -------------------------------------------------------------------------------------------
  // Position mappings, absences and the identity fast path
  // -------------------------------------------------------------------------------------------

  @Test
  void positionMappingsAreKeyedByContainerPath() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        orderWithAddress("city", "zip"),
        orderWithAddress("zip", "city")));

    Map<List<Integer>, PositionMapping> mappings = result.positionMappings(1, 0);
    assertThat(mappings.keySet()).containsExactlyInAnyOrder(path(), path(1));
    // The root row is unchanged; the referenced row underneath field 1 has its members swapped.
    assertThat(mappings.get(path()).getSourcePositions()).containsExactly(0, 1);
    assertThat(mappings.get(path(1)).getSourcePositions()).containsExactly(1, 0);
    assertThat(mappings.get(path(1)).getSourceArity()).isEqualTo(2);
  }

  @Test
  void positionMappingsCarryTheSourceArityNotTheTargets() {
    // The source row has one more column, which a consumer must ask the source row for.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"), field("b"), field("c"))),
        lt(struct(field("a"), field("b")))));

    PositionMapping root = result.positionMappings(1, 0).get(path());
    assertThat(root.getSourcePositions()).containsExactly(0, 1);
    assertThat(root.getSourceArity()).isEqualTo(3);
    assertThat(root.isIdentity()).isFalse();
  }

  @Test
  void absencesSayWhyAMemberIsUnmatched() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("id"))),
        lt(struct(field("id"), new Field("addr", struct(field("city")), 1)))));

    assertThat(result.absences(1, 0)).containsOnly(
        entryOfAbsence(path(1), Absence.NOT_IN_SOURCE),
        entryOfAbsence(path(1, 0), Absence.PARENT_ABSENT));
    // The position mapping says the same thing in the form a plan consumes.
    assertThat(result.positionMappings(1, 0).get(path()).getSourcePositions())
        .containsExactly(0, ProvenanceResult.ABSENT);
  }

  @Test
  void aRetypedCollectionIsReportedAsDivergence() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(arrayOf("items", struct(field("sku"))))),
        lt(struct(mapOf("items", struct(field("sku")))))));

    // items itself corresponds -- it kept its identity. What is inside it does not.
    assertThat(result.expandedCorrespondence(1, 0)).containsOnly(entryOf(path(0), path(0)));
    assertThat(result.absences(1, 0))
        .containsEntry(path(0, 1, 0), Absence.TYPE_DIVERGED);
  }

  @Test
  void identityIsRecognisedAndAnythingElseIsNot() {
    LogicalType flat = lt(struct(field("a"), field("b")));
    ProvenanceResult same = ProvenanceComputer.compute(Arrays.asList(flat, flat));
    assertThat(same.isIdentity(1, 0)).isTrue();

    ProvenanceResult reordered = ProvenanceComputer.compute(Arrays.asList(
        flat, lt(struct(field("b"), field("a")))));
    assertThat(reordered.isIdentity(1, 0)).isFalse();

    ProvenanceResult widened = ProvenanceComputer.compute(Arrays.asList(
        flat, lt(struct(field("a"), field("b"), field("c")))));
    // c has no source, so the projection is not a no-op even though a and b are in place.
    assertThat(widened.isIdentity(1, 0)).isFalse();
    // Read the other way every target column is in place, but the source is wider.
    assertThat(widened.isIdentity(0, 1)).isFalse();
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /** {@code acme.Order { id, addr: acme.Address { city } }}, rooted at a reference. */
  private static LogicalType nested() {
    return nestedWithCity("city");
  }

  private static LogicalType nestedWithCity(String cityName, String... aliases) {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("acme.Order", struct(
        field("id"),
        new Field("addr", Schema.createNamedTypeRef("acme.Address"), 1)));
    namedTypes.put("acme.Address", struct(field(cityName, aliases)));
    return new LogicalType(Schema.createNamedTypeRef("acme.Order"), namedTypes);
  }

  /** A root of two fields that share one named type. */
  private static LogicalType shared() {
    return sharedRoot("a", "b");
  }

  private static LogicalType sharedSwapped() {
    return sharedRoot("b", "a");
  }

  private static LogicalType sharedRoot(String first, String second) {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("T", struct(field("x")));
    return new LogicalType(struct(
        new Field(first, Schema.createNamedTypeRef("T"), 0),
        new Field(second, Schema.createNamedTypeRef("T"), 1)), namedTypes);
  }

  private static Field arrayOf(String name, Schema element) {
    return new Field(name, Schema.createArray(element), 0);
  }

  private static Field mapOf(String name, Schema value) {
    return new Field(name, Schema.createMap(Schema.createString(), value), 0);
  }

  /** {@code acme.Order { id, addr: acme.Address { ... } }}, rooted at a reference. */
  private static LogicalType orderWithAddress(String... addressFields) {
    Field[] fields = new Field[addressFields.length];
    for (int i = 0; i < addressFields.length; i++) {
      fields[i] = field(addressFields[i]);
    }
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("acme.Order", struct(
        field("id"),
        new Field("addr", Schema.createNamedTypeRef("acme.Address"), 1)));
    namedTypes.put("acme.Address", struct(fields));
    return new LogicalType(Schema.createNamedTypeRef("acme.Order"), namedTypes);
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
  }

  private static Map.Entry<List<Integer>, Absence> entryOfAbsence(
      List<Integer> path, Absence absence) {
    return new java.util.AbstractMap.SimpleEntry<>(path, absence);
  }

  private static Map.Entry<List<Integer>, List<Integer>> entryOf(
      List<Integer> from, List<Integer> to) {
    return new java.util.AbstractMap.SimpleEntry<>(from, to);
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
    return new Field(name, Schema.create(Schema.Type.INT), 0, null, false, null, null, params);
  }
}
