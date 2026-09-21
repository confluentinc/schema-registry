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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ProvenanceResult#inlinedProvenance} — provenance re-keyed from definition
 * sites onto the paths a consumer walks once it has inlined every named type, and located by the
 * chain of provenances rather than by the path, so two uses of one shared type stay apart.
 */
class ProvenanceInlinedPathTest {

  // -------------------------------------------------------------------------------------------
  // Inlining
  // -------------------------------------------------------------------------------------------

  @Test
  void inlinesNamedTypes() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(nested()));

    Map<List<Integer>, LocatedProvenance> inlined = result.inlinedProvenance(0);
    assertThat(inlined.keySet()).containsExactly(
        path(0),        // id
        path(1),        // addr
        path(1, 0));    // addr.city, reached through the reference

    assertThat(inlined.get(path(1, 0)).getEntity())
        .isEqualTo(result.at(0, PathKey.ofNamedType("acme.Address").child(0)));
    // The chain locates it: the addr field, then city within it.
    assertThat(inlined.get(path(1, 0)).getChain())
        .containsExactly(inlined.get(path(1)).getEntity(),
            inlined.get(path(1, 0)).getEntity());
  }

  @Test
  void aSharedNamedTypeIsOneEntityButTwoLocations() {
    // The single most important property for a consumer with no shared types: home.city and
    // work.city are the same logical entity and must still be told apart.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(twoAddresses()));

    Map<List<Integer>, LocatedProvenance> inlined = result.inlinedProvenance(0);
    LocatedProvenance homeCity = inlined.get(path(0, 0));
    LocatedProvenance workCity = inlined.get(path(1, 0));

    assertThat(homeCity.getEntity()).isEqualTo(workCity.getEntity());
    assertThat(homeCity).isNotEqualTo(workCity);
    assertThat(homeCity.depth()).isEqualTo(2);
  }

  @Test
  void aRecursiveSchemaCannotBeInlined() {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Node", Schema.createStruct(Arrays.asList(
        field("value"),
        new Field("next", Schema.createNamedTypeRef("Node"), 1))));
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        new LogicalType(Schema.createNamedTypeRef("Node"), namedTypes)));

    assertThatThrownBy(() -> result.inlinedProvenance(0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("recursive named type: Node");
  }

  // -------------------------------------------------------------------------------------------
  // Path grammar
  // -------------------------------------------------------------------------------------------

  @Test
  void pathsDescendIntoCollections() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(
            arrayOf("items", struct(field("sku"), field("qty"))),
            mapOf("byId", struct(field("label")))))));

    assertThat(result.inlinedProvenance(0).keySet()).containsExactly(
        path(0),          // items
        path(0, 0, 0),    // items[].sku   -- element step is 0
        path(0, 0, 1),    // items[].qty
        path(1),          // byId
        path(1, 1, 0));   // byId{value}.label -- map value step is 1
  }

  @Test
  void aCollectionStepDoesNotLengthenTheChain() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(arrayOf("items", struct(field("sku")))))));

    Map<List<Integer>, LocatedProvenance> inlined = result.inlinedProvenance(0);
    // items -> sku, with nothing for the element itself: a step is not an entity.
    assertThat(inlined.get(path(0, 0, 0)).depth()).isEqualTo(2);
    assertThat(inlined.get(path(0, 0, 0)).getChain().get(0))
        .isEqualTo(inlined.get(path(0)).getEntity());
  }

  @Test
  void aRetypedCollectionRelocatesItsMembers() {
    // ARRAY<ROW> to MAP<K, ROW>: the inner members move to a different scope, so they are not the
    // same entities and their locations do not match either.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(arrayOf("items", struct(field("sku"))))),
        lt(struct(mapOf("items", struct(field("sku")))))));

    assertThat(result.inlinedProvenance(1).get(path(0, 1, 0)))
        .isNotEqualTo(result.inlinedProvenance(0).get(path(0, 0, 0)));
  }

  // -------------------------------------------------------------------------------------------
  // Column-id derivation, the Tableflow use
  // -------------------------------------------------------------------------------------------

  @Test
  void derivesIcebergStyleColumnIds() {
    // Allocate the next integer the first time a location is seen, walking versions in order. The
    // spec's three rules then fall out: unique, monotonic, never reused.
    List<LogicalType> history = Arrays.asList(
        lt(struct(field("id"), field("name"))),
        lt(struct(field("id"), field("full_name", "name"))),   // rename
        lt(struct(field("id"))),                               // drop
        lt(struct(field("id"), field("full_name"))));          // re-add
    List<Map<List<Integer>, Integer>> ids = allocate(
        ProvenanceComputer.compute(history, IdentityPolicy.AVRO), history.size());

    assertThat(ids.get(0)).containsExactly(entryOfId(path(0), 1), entryOfId(path(1), 2));
    // A rename keeps its id, which is what lets Iceberg treat it as the same column.
    assertThat(ids.get(1)).containsExactly(entryOfId(path(0), 1), entryOfId(path(1), 2));
    assertThat(ids.get(2)).containsExactly(entryOfId(path(0), 1));
    // Re-added under the same name: a new presence interval, so a new id. 2 is never reused.
    assertThat(ids.get(3)).containsExactly(entryOfId(path(0), 1), entryOfId(path(1), 3));
  }

  @Test
  void sharedTypesGetOneIdPerUseSite() {
    // One definition-level rename, two inlined locations, each keeping its own id. Allocating per
    // entity instead would give home.city and work.city the same id and let a consumer pair them.
    List<LogicalType> history = Arrays.asList(twoAddresses(), twoAddressesRenamed());
    List<Map<List<Integer>, Integer>> ids = allocate(
        ProvenanceComputer.compute(history, IdentityPolicy.AVRO), history.size());

    assertThat(ids.get(0)).containsExactly(
        entryOfId(path(0), 1), entryOfId(path(0, 0), 2), entryOfId(path(0, 1), 3),
        entryOfId(path(1), 4), entryOfId(path(1, 0), 5), entryOfId(path(1, 1), 6));
    // zip became postcode in the one shared definition; both copies keep their own id.
    assertThat(ids.get(1)).containsExactly(
        entryOfId(path(0), 1), entryOfId(path(0, 0), 2), entryOfId(path(0, 1), 3),
        entryOfId(path(1), 4), entryOfId(path(1, 0), 5), entryOfId(path(1, 1), 6));
  }

  // -------------------------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------------------------

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
  // Helpers
  // -------------------------------------------------------------------------------------------

  /** The allocation a consumer performs: next integer the first time a location is seen. */
  private static List<Map<List<Integer>, Integer>> allocate(ProvenanceResult result, int versions) {
    Map<LocatedProvenance, Integer> idByLocation = new LinkedHashMap<>();
    List<Map<List<Integer>, Integer>> byVersion = new ArrayList<>(versions);
    int nextId = 1;
    for (int v = 0; v < versions; v++) {
      Map<List<Integer>, Integer> ids = new LinkedHashMap<>();
      for (Map.Entry<List<Integer>, LocatedProvenance> e
          : result.inlinedProvenance(v).entrySet()) {
        Integer id = idByLocation.get(e.getValue());
        if (id == null) {
          id = nextId++;
          idByLocation.put(e.getValue(), id);
        }
        ids.put(e.getKey(), id);
      }
      byVersion.add(ids);
    }
    return byVersion;
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

  /** A root of two fields sharing one named type: {@code { home: Address, work: Address }}. */
  private static LogicalType twoAddresses() {
    return sharedAddress("zip");
  }

  private static LogicalType twoAddressesRenamed() {
    return sharedAddress("postcode", "zip");
  }

  private static LogicalType sharedAddress(String second, String... aliases) {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", struct(field("city"), field(second, aliases)));
    return new LogicalType(struct(
        new Field("home", Schema.createNamedTypeRef("Address"), 0),
        new Field("work", Schema.createNamedTypeRef("Address"), 1)), namedTypes);
  }

  private static Field arrayOf(String name, Schema element) {
    return new Field(name, Schema.createArray(element), 0);
  }

  private static Field mapOf(String name, Schema value) {
    return new Field(name, Schema.createMap(Schema.createString(), value), 0);
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
  }

  private static Map.Entry<List<Integer>, Integer> entryOfId(List<Integer> path, int id) {
    return new java.util.AbstractMap.SimpleEntry<>(path, id);
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
