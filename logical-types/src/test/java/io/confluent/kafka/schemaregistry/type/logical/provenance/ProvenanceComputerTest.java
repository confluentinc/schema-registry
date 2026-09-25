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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ProvenanceComputer}, worked against the scenarios the provenance model is
 * specified by: rename continuity, broken presence intervals, alias pruning, nested scope, and the
 * two identity signals a {@link LogicalType} can carry.
 */
class ProvenanceComputerTest {

  // ---------------------------------------------------------------------------------------------
  // Continuity -- an unbroken interval keeps one provenance
  // ---------------------------------------------------------------------------------------------

  @Test
  void unchangedFieldKeepsProvenanceAcrossVersions() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct(field("name"))),
        lt(struct(field("name")))));

    assertThat(at(result, 0, 0)).isEqualTo(at(result, 1, 0)).isEqualTo(at(result, 2, 0));
    assertThat(at(result, 0, 0).getPresenceStartVersion()).isZero();
  }

  @Test
  void avroAliasCarriesProvenanceAcrossRename() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name")))));

    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 1, 0).getPresenceStartVersion()).isZero();
  }

  @Test
  void protobufNumberCarriesProvenanceAcrossRename() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(numbered("name", 1))),
        lt(struct(numbered("full_name", 1)))));

    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 1, 0).getIdentity().getFieldNumber()).isEqualTo(1);
  }

  // ---------------------------------------------------------------------------------------------
  // Separation -- a broken interval never keeps its provenance
  // ---------------------------------------------------------------------------------------------

  @Test
  void reintroducedFieldStartsNewInterval() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct()),
        lt(struct(field("name")))));

    assertThat(at(result, 2, 0)).isNotEqualTo(at(result, 0, 0));
    assertThat(at(result, 2, 0).getPresenceStartVersion()).isEqualTo(2);
    assertThat(result.intersection(0, 2)).isEmpty();
    // Under Avro rules a canonical name reappearing after a gap mints a distinct identity rather
    // than reconnecting: only an explicit alias may reconnect to a dormant one.
    assertThat(at(result, 2, 0).getIdentity()).isNotEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 2, 0).getIdentity().getIdentityOriginVersion()).isEqualTo(2);
  }

  @Test
  void renameSurvivesButDeletionBreaksTheInterval() {
    // v0: name -> P0;  v1: full_name aliases [name] -> P0;  v2: absent;  v3: full_name -> P3.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))),
        lt(struct()),
        lt(struct(field("full_name")))));

    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 3, 0)).isNotEqualTo(at(result, 0, 0));
    assertThat(at(result, 3, 0).getPresenceStartVersion()).isEqualTo(3);
  }

  @Test
  void aliasToAnAbsentFieldKeepsIdentityButNotProvenance() {
    // The alias says full_name is historically the same entity as name; the gap at v1 says it is
    // a different lifetime of it. Both must be true at once.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct()),
        lt(struct(field("full_name", "name")))));

    assertThat(at(result, 2, 0).getIdentity()).isEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 2, 0)).isNotEqualTo(at(result, 0, 0));
    assertThat(result.intersection(0, 2)).isEmpty();
    // Identity and provenance are different dimensions: the identity was minted at v0 and the
    // alias reconnects to it, while this lifetime of it only begins at v2.
    assertThat(at(result, 2, 0).getIdentity().getIdentityOriginVersion()).isEqualTo(0);
    assertThat(at(result, 2, 0).getPresenceStartVersion()).isEqualTo(2);
  }

  @Test
  void protobufNumberReuseAfterAGapStartsNewInterval() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(numbered("name", 1))),
        lt(struct()),
        lt(struct(numbered("full_name", 1)))));

    assertThat(at(result, 2, 0).getIdentity()).isEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 2, 0)).isNotEqualTo(at(result, 0, 0));
    assertThat(result.intersection(0, 2)).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Ghost identities and orphaned aliases
  // ---------------------------------------------------------------------------------------------

  @Test
  void droppingAnAliasReleasesTheNameForAFreshIdentity() {
    // v0 name -> P0;  v1 full_name aliases [name] -> P0;  v2 full_name, alias pruned;
    // v3 adds a brand-new field called name, which must NOT inherit the historical identity.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))),
        lt(struct(field("full_name"))),
        lt(struct(field("full_name"), field("name")))));

    assertThat(at(result, 3, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 3, 1)).isNotEqualTo(at(result, 0, 0));
    assertThat(at(result, 3, 1).getIdentity()).isNotEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 3, 1).getIdentity().getIdentityOriginVersion()).isEqualTo(3);
  }

  @Test
  void anAliasNamesAnyCanonicalNameTheIdentityHeld() {
    // Same ladder, but v3 aliases the v0 name. An Avro alias names what a writer's field was
    // actually called, so other reads v0's name -- and continues the identity full_name carried.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))),
        lt(struct(field("full_name"))),
        lt(struct(field("other", "name")))));

    assertThat(at(result, 3, 0)).isEqualTo(at(result, 0, 0));
  }

  @Test
  void aReturningEntityDoesNotPruneANameNowHeldBySomeoneElse() {
    // v0 a asserts the alias x.  v1 a disappears, leaving both mappings dormant.  v2 a brand-new
    // field called x takes the name over.  v3 c reconnects to a's identity through the alias a, so
    // a's stale assertion of x is revisited -- but x now belongs to the v2 field, and pruning it
    // would silently reset that field at v4.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a", "x"))),
        lt(struct()),
        lt(struct(field("x"))),
        lt(struct(field("x"), field("c", "a"))),
        lt(struct(field("x"), field("c")))));

    assertThat(at(result, 3, 0)).isEqualTo(at(result, 2, 0));
    assertThat(at(result, 4, 0)).isEqualTo(at(result, 2, 0));
    assertThat(at(result, 4, 0).getPresenceStartVersion()).isEqualTo(2);
    // c still reconnected to a's identity, on a new interval of its own.
    assertThat(at(result, 3, 1).getIdentity()).isEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 3, 1).getPresenceStartVersion()).isEqualTo(3);
  }

  // ---------------------------------------------------------------------------------------------
  // Released names -- the pre-pass
  // ---------------------------------------------------------------------------------------------

  @Test
  void releasedNameCanBeTakenOverDespiteFormerIdentityStillBeingActive() {
    // v0: a owns the canonical name a and the alias x. v1: a disappears and x is immediately
    // reused by a new field. The pre-pass releases x, but a is still marked active -- presence is
    // not cleared until the end of commit -- so validation must not reject x as taking a live
    // name. Fails if the released-name exemption in validate is removed.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a", "x"))),
        lt(struct(field("x")))));

    assertThat(at(result, 1, 0).getIdentity()).isNotEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 1, 0).getIdentity().getIdentityOriginVersion()).isEqualTo(1);
    assertThat(result.intersection(0, 1)).isEmpty();
  }

  @Test
  void dormantIdentityCannotReleaseANameTakenByAnotherIdentity() {
    // The minimal form of the stale-ownership bug. a is dormant from v1 holding the stale
    // declarations {a, x}; x is taken over at v2; at v3 c reconnects to a through the alias and
    // drops x, which must not release the name the live field now owns. Fails without the
    // ownership guard in the pre-pass -- x would re-mint at v3.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a", "x"))),
        lt(struct()),
        lt(struct(field("x"))),
        lt(struct(field("x"), field("c", "a")))));

    assertThat(at(result, 3, 0)).isEqualTo(at(result, 2, 0));
    assertThat(at(result, 3, 1).getIdentity()).isEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 3, 1).getPresenceStartVersion()).isEqualTo(3);
  }

  @Test
  void aDormantIdentityReleasesOnlyTheStaleNamesItStillOwns() {
    // lastDeclaredNames is a set and the ownership guard runs per element: of a's stale {a, x, y},
    // x has been taken over and must survive, y is still a's and is released.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a", "x", "y"))),
        lt(struct()),
        lt(struct(field("x"))),
        lt(struct(field("x"), field("c", "a"))),
        lt(struct(field("x"), field("y"), field("c")))));

    assertThat(at(result, 3, 0)).isEqualTo(at(result, 2, 0));
    assertThat(at(result, 4, 0)).isEqualTo(at(result, 2, 0));
    assertThat(at(result, 3, 1).getIdentity()).isEqualTo(at(result, 0, 0).getIdentity());
    // y was released and its mapping pruned, so the field called y at v4 is a new entity.
    assertThat(at(result, 4, 1).getIdentity()).isNotEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 4, 1).getIdentity().getIdentityOriginVersion()).isEqualTo(4);
  }

  @Test
  void canonicalNameContinuesOnlyTheIdentityItWasCommittedUnder() {
    // The converse of aFormerAliasIsNotACanonicalContinuation: a canonical name that really was
    // the identity's committed name continues it, even as the entity gains an alias.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"))),
        lt(struct(field("a", "x")))));

    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
  }

  @Test
  void anExplicitAliasOutranksACanonicalMatchForTheSameIdentity() {
    // Both peers claim a's identity at v1: the new field called a by name, and b by aliasing a.
    // Avro renames the writer's a to the field aliasing it, so b continues and a starts over.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"))),
        lt(struct(field("a"), field("b", "a")))));

    assertThat(at(result, 1, 1)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 1, 0).getIdentity()).isNotEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 1, 0).getIdentity().getIdentityOriginVersion()).isEqualTo(1);
  }

  @Test
  void aCarriedForwardAliasClaimsNothingNew() {
    // v1's rename-and-reuse, kept as is in v2: full_name's alias names the new field's identity
    // now, but full_name introduced it at v1, so both fields simply continue.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"), field("name"))),
        lt(struct(field("full_name", "name"), field("name")))));

    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 2, 0)).isEqualTo(at(result, 1, 0));
    assertThat(at(result, 2, 1)).isEqualTo(at(result, 1, 1));
  }

  @Test
  void aFormerAliasClaimsNothing() {
    // x was only ever a's alias. Avro reads aliases from the reader alone, so a reader field
    // aliasing x finds nothing in a writer whose field is called a.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a", "x"))),
        lt(struct(field("a"), field("b", "x")))));

    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 1, 1).getIdentity().getIdentityOriginVersion()).isEqualTo(1);
  }

  @Test
  void anEntityAliasingTwoHistoricalIdentitiesIsRejected() {
    assertThatThrownBy(() -> ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"), field("b"))),
        lt(struct(field("c", "a", "b"))))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("matches multiple historical identities");
  }

  @Test
  void aDormantAliasReconnectsAndThenReleasesTheNamesItDropped() {
    // v2 reconnects to a through the dormant alias, which is what retaining dormant mappings is
    // for. c declares only {c, a}, so x -- still a's -- is released and pruned, and a later field
    // called x is a new entity rather than an echo of the original.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a", "x"))),
        lt(struct()),
        lt(struct(field("c", "a"))),
        lt(struct(field("c"), field("x")))));

    assertThat(at(result, 2, 0).getIdentity()).isEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 2, 0).getPresenceStartVersion()).isEqualTo(2);
    assertThat(at(result, 3, 1).getIdentity()).isNotEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 3, 1).getIdentity().getIdentityOriginVersion()).isEqualTo(3);
  }

  @Test
  void aNameReleasedInThisVersionCannotBeInheritedInIt() {
    // v1 renames name to full_name through an alias. v2 drops that alias and, in the same version,
    // introduces a brand-new field called name -- which must not resolve through the mapping the
    // rename left behind.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))),
        lt(struct(field("full_name"), field("name")))));

    assertThat(at(result, 2, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 2, 1)).isNotEqualTo(at(result, 0, 0));
    assertThat(at(result, 2, 1).getIdentity().getIdentityOriginVersion()).isEqualTo(2);
  }

  @Test
  void aFormerAliasIsNotACanonicalContinuation() {
    // x was only ever a's alias, never the name a was committed under, so the field called x at v1
    // is a coincidental reuse rather than a's continuation -- and a, being uncontinued, is gone.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a", "x"))),
        lt(struct(field("x"), field("a")))));

    assertThat(at(result, 1, 0).getIdentity()).isNotEqualTo(at(result, 0, 0).getIdentity());
    assertThat(at(result, 1, 1)).isEqualTo(at(result, 0, 0));
  }

  @Test
  void twoEntitiesClaimingOneHistoricalIdentityByAliasIsRejected() {
    assertThatThrownBy(() -> ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"))),
        lt(struct(field("b", "a"), field("c", "a"))))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("multiple entities claim historical identity");
  }

  @Test
  void anEntityContinuingItselfAndAliasingAnotherIsRejected() {
    // p continues p and, by a new alias, q: one field cannot continue two.
    assertThatThrownBy(() -> ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("p"), field("q"))),
        lt(struct(field("p", "q"))))))
        .isInstanceOf(AmbiguousProvenanceException.class)
        .hasMessageContaining("names another identity by a new alias");
  }

  @Test
  void twoFieldsSwappedByAliasesAreRejected() {
    assertThatThrownBy(() -> ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"), field("b"))),
        lt(struct(field("b", "a"), field("a", "b"))))))
        .isInstanceOf(AmbiguousProvenanceException.class);
  }

  @Test
  void aDuplicateAliasChangesNothing() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"))),
        lt(struct(field("b", "a", "a")))));
    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
  }

  @Test
  void anAliasRepeatingTheCanonicalNameChangesNothing() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"))),
        lt(struct(field("a", "a")))));
    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
  }

  // ---------------------------------------------------------------------------------------------
  // Scope
  // ---------------------------------------------------------------------------------------------

  @Test
  void identicalNamesInDifferentRecordsDoNotCollide() {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("com.acme.User", struct(field("name")));
    namedTypes.put("com.acme.Order", struct(field("name")));
    LogicalType version = lt(struct(
        new Field("user", Schema.createNamedTypeRef("com.acme.User"), 0),
        new Field("order", Schema.createNamedTypeRef("com.acme.Order"), 1)), namedTypes);

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(version));

    Provenance user = result.at(0, PathKey.ofRoot().child(0).child(0));
    Provenance order = result.at(0, PathKey.ofRoot().child(1).child(0));
    assertThat(user).isNotNull();
    assertThat(order).isNotNull();
    assertThat(user).isNotEqualTo(order);
    assertThat(result.memberProvenance(0)).contains(user, order).hasSize(4);
  }

  @Test
  void aTypeUnreachableFromTheRootHasNoEntities() {
    // Named types are resolved where they are used, so one that is never used contributes nothing.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("id")), namedTypes("Unused", struct(field("x"))))));
    assertThat(result.byPath(0)).hasSize(1);
  }

  @Test
  void renamingARecordThroughAnAliasKeepsItsChildren() {
    Map<String, Schema> v0 = new LinkedHashMap<>();
    v0.put("com.acme.User", struct(field("city")));
    Map<String, Schema> v1 = new LinkedHashMap<>();
    v1.put("com.acme.Person", aliased(struct(field("city")), "com.acme.User"));

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(Schema.createNamedTypeRef("com.acme.User"), v0),
        lt(Schema.createNamedTypeRef("com.acme.Person"), v1)));

    PathKey city = PathKey.ofRoot().child(0);
    assertThat(result.at(1, city)).isNotNull().isEqualTo(result.at(0, city));
    assertThat(result.correspondence(0, 1)).containsEntry(city, city);
  }

  @Test
  void droppingARecordBreaksItsChildrenIntervals() {
    Map<String, Schema> present = new LinkedHashMap<>();
    present.put("com.acme.Address", struct(field("city")));

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(Schema.createNamedTypeRef("com.acme.Address"), present),
        lt(struct()),
        lt(Schema.createNamedTypeRef("com.acme.Address"), present)));

    PathKey city = PathKey.ofRoot().child(0);
    assertThat(result.at(2, city)).isNotEqualTo(result.at(0, city));
    assertThat(result.at(2, city).getPresenceStartVersion()).isEqualTo(2);
    assertThat(result.intersection(0, 2)).isEmpty();
  }

  @Test
  void aChildIntervalBreaksIndependentlyOfItsParent() {
    // address [P0] throughout, city [P0] throughout, zip [P0] -> absent -> [P2].
    Map<String, Schema> both = new LinkedHashMap<>();
    both.put("Address", struct(field("city"), field("zip")));
    Map<String, Schema> cityOnly = new LinkedHashMap<>();
    cityOnly.put("Address", struct(field("city")));

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(Schema.createNamedTypeRef("Address"), both),
        lt(Schema.createNamedTypeRef("Address"), cityOnly),
        lt(Schema.createNamedTypeRef("Address"), both)));

    PathKey address = PathKey.ofTypeUse("Address", PathKey.ofRoot());
    PathKey city = PathKey.ofRoot().child(0);
    PathKey zip = PathKey.ofRoot().child(1);

    assertThat(result.at(2, address)).isEqualTo(result.at(0, address));
    assertThat(result.at(2, city)).isEqualTo(result.at(0, city));
    assertThat(result.at(2, zip)).isNotEqualTo(result.at(0, zip));
    assertThat(result.at(2, zip).getPresenceStartVersion()).isEqualTo(2);
    // The container's own provenance intersects, so only zip is lost between v0 and v2.
    assertThat(result.intersection(0, 2)).containsExactly(result.at(0, city));
  }

  @Test
  void namedTypesAreNotPartOfTheMemberIntersection() {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", struct(field("city")));
    LogicalType version = lt(Schema.createNamedTypeRef("Address"), namedTypes);

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(version, version));

    assertThat(result.byPath(0)).containsKey(PathKey.ofNamedType("Address"));
    assertThat(result.at(0, PathKey.ofNamedType("Address")).getKind())
        .isEqualTo(EntityKind.NAMED_TYPE);
    assertThat(result.memberProvenance(0)).hasSize(1);
    assertThat(result.intersection(0, 1)).hasSize(1);
  }

  @Test
  void aRecursiveTypeIsRejected() {
    // A type is resolved at every use, and a recursive one has no finite set of uses.
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Node", struct(
        field("value"),
        new Field("next", Schema.createNamedTypeRef("Node"), 1)));

    assertThatThrownBy(() -> ProvenanceComputer.compute(Arrays.asList(
        lt(Schema.createNamedTypeRef("Node"), namedTypes))))
        .isInstanceOf(RecursiveTypeException.class);
  }

  @Test
  void aLocationWhoseTypeChangesAndChangesBackStartsOver() {
    // u holds A, then B, then A again, while w holds A throughout. A never leaves the schema, but
    // it left u, so u.x is new at v2 rather than v0's again.
    Map<String, Schema> types = namedTypes("A", struct(field("x")), "B", struct(field("x")));
    IntFunction<LogicalType> holding = t -> lt(struct(
        new Field("w", Schema.createNamedTypeRef("A"), 0),
        new Field("u", Schema.createNamedTypeRef(t == 0 ? "A" : "B"), 1)), types);

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        holding.apply(0), holding.apply(1), holding.apply(0)));

    PathKey ux = PathKey.ofRoot().child(1).child(0);
    PathKey wx = PathKey.ofRoot().child(0).child(0);
    assertThat(result.at(2, ux)).isNotEqualTo(result.at(0, ux));
    assertThat(result.at(2, ux).getPresenceStartVersion()).isEqualTo(2);
    assertThat(result.at(2, wx)).isEqualTo(result.at(0, wx));
  }

  @Test
  void twoTypesMergedByAliasKeepEachLocationsLineage() {
    // billing's BillingAddress becomes Address, which aliases it, and line1 becomes street.
    LogicalType v0 = lt(struct(
        new Field("shipping", Schema.createNamedTypeRef("Address"), 0),
        new Field("billing", Schema.createNamedTypeRef("BillingAddress"), 1)),
        namedTypes("Address", struct(field("street")),
            "BillingAddress", struct(field("line1"), field("zip"))));
    LogicalType v1 = lt(struct(
        new Field("shipping", Schema.createNamedTypeRef("Address"), 0),
        new Field("billing", Schema.createNamedTypeRef("Address"), 1)),
        namedTypes("Address", aliased(struct(field("street", "line1")), "BillingAddress")));

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(v0, v1));

    assertThat(result.at(1, PathKey.ofRoot().child(0).child(0)))
        .isEqualTo(result.at(0, PathKey.ofRoot().child(0).child(0)));
    assertThat(result.at(1, PathKey.ofRoot().child(1).child(0)))
        .isEqualTo(result.at(0, PathKey.ofRoot().child(1).child(0)));
  }

  @Test
  void oneTypeSplitInTwoKeepsEachLocationsLineage() {
    LogicalType v0 = lt(struct(
        new Field("u", Schema.createNamedTypeRef("A"), 0),
        new Field("v", Schema.createNamedTypeRef("A"), 1)),
        namedTypes("A", struct(field("x"))));
    LogicalType v1 = lt(struct(
        new Field("u", Schema.createNamedTypeRef("A"), 0),
        new Field("v", Schema.createNamedTypeRef("C"), 1)),
        namedTypes("A", struct(field("x")), "C", aliased(struct(field("x")), "A")));

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(v0, v1));

    for (int site = 0; site < 2; site++) {
      PathKey x = PathKey.ofRoot().child(site).child(0);
      assertThat(result.at(1, x)).isEqualTo(result.at(0, x));
    }
  }

  @Test
  void aTakeoverInOneScopeDoesNotAffectAnother() {
    // User.name going away and coming back must leave Order.name untouched.
    IntFunction<LogicalType> version = hasName -> lt(struct(
        new Field("user", Schema.createNamedTypeRef("User"), 0),
        new Field("order", Schema.createNamedTypeRef("Order"), 1)), namedTypes(
            "User", hasName == 1 ? struct(field("name")) : struct(),
            "Order", struct(field("name"))));
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        version.apply(1), version.apply(0), version.apply(1)));

    PathKey userName = PathKey.ofRoot().child(0).child(0);
    PathKey orderName = PathKey.ofRoot().child(1).child(0);

    assertThat(result.at(1, orderName)).isEqualTo(result.at(0, orderName));
    assertThat(result.at(2, orderName)).isEqualTo(result.at(0, orderName));
    assertThat(result.at(2, userName)).isNotEqualTo(result.at(0, userName));
    assertThat(result.at(2, userName).getPresenceStartVersion()).isEqualTo(2);
  }

  @Test
  void aRecordRenameKeepsDeeplyNestedChildren() {
    // Two levels down, through an inline struct. A child's scope is the resolved parent identity,
    // not the parent's text, so the alias carries the whole subtree.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(Schema.createNamedTypeRef("User"), namedTypes("User",
            struct(new Field("address", struct(field("city")), 0)))),
        lt(Schema.createNamedTypeRef("Person"), namedTypes("Person",
            aliased(struct(new Field("address", struct(field("city")), 0)), "User")))));

    PathKey city = PathKey.ofRoot().child(0).child(0);
    assertThat(result.at(1, city)).isNotNull().isEqualTo(result.at(0, city));
    assertThat(result.correspondence(0, 1)).containsEntry(city, city);
  }

  @Test
  void aRenamedRecordReturningAfterAGapKeepsIdentityButNotProvenance() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(Schema.createNamedTypeRef("User"), namedTypes("User", struct(field("city")))),
        lt(struct()),
        lt(Schema.createNamedTypeRef("Person"), namedTypes("Person",
            aliased(struct(field("city")), "User")))));

    PathKey user = PathKey.ofNamedType("User");
    PathKey person = PathKey.ofNamedType("Person");
    // The record's alias reconnects it to the dormant identity, on a new interval.
    assertThat(result.at(2, person).getIdentity()).isEqualTo(result.at(0, user).getIdentity());
    assertThat(result.at(2, person).getPresenceStartVersion()).isEqualTo(2);
    // city carries no alias of its own, so it mints a fresh identity even though its scope -- the
    // record's identity -- is unchanged. Reconnection is per entity, never inherited.
    PathKey city = PathKey.ofRoot().child(0);
    assertThat(result.at(2, city).getIdentity())
        .isNotEqualTo(result.at(0, city).getIdentity());
    assertThat(result.intersection(0, 2)).isEmpty();
  }

  @Test
  void reorderingNestedFieldsPreservesEveryIdentity() {
    // Identity is scope-and-name based, never path based, so moving fields around changes only
    // where they are found.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(
            new Field("a", struct(field("x"), field("y")), 0),
            new Field("b", struct(field("z")), 1))),
        lt(struct(
            new Field("b", struct(field("z")), 0),
            new Field("a", struct(field("y"), field("x")), 1)))));

    assertThat(result.intersection(0, 1)).hasSize(5);
    assertThat(result.correspondence(0, 1)).containsOnly(
        entry(PathKey.ofRoot().child(0), PathKey.ofRoot().child(1)),
        entry(PathKey.ofRoot().child(0).child(0), PathKey.ofRoot().child(1).child(1)),
        entry(PathKey.ofRoot().child(0).child(1), PathKey.ofRoot().child(1).child(0)),
        entry(PathKey.ofRoot().child(1), PathKey.ofRoot().child(0)),
        entry(PathKey.ofRoot().child(1).child(0), PathKey.ofRoot().child(0).child(0)));
  }

  // ---------------------------------------------------------------------------------------------
  // Index paths
  // ---------------------------------------------------------------------------------------------

  @Test
  void indexPathsFollowTheDefaultValuePathConventions() {
    Schema root = struct(
        field("id"),
        new Field("items", Schema.createArray(struct(field("sku"))), 1),
        new Field("tags", Schema.createMap(Schema.createString(), struct(field("label"))), 2),
        new Field("payment", Schema.createUnion(Arrays.asList(
            new UnionBranch("card", Schema.create(Schema.Type.INT)),
            new UnionBranch("cash", struct(field("amount"))))), 3));

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(lt(root)));

    assertThat(result.byPath(0).keySet()).extracting(Object::toString).containsExactly(
        "$root/0",          // id
        "$root/1",          // items
        "$root/1/0/0",      // items[].sku
        "$root/2",          // tags
        "$root/2/1/0",      // tags{value}.label
        "$root/3",          // payment
        "$root/3/0",        // payment.card
        "$root/3/1",        // payment.cash
        "$root/3/1/0");     // payment.cash.amount
  }

  @Test
  void changingACollectionKindRestartsTheMembersInside() {
    // ARRAY<STRUCT> and MAP<K, STRUCT> both step to index 0, but they are different scopes: the
    // inner members are not the same entities and must not correspond.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(new Field("f", Schema.createArray(struct(field("x"))), 0))),
        lt(struct(new Field("f", Schema.createMap(struct(field("x")),
            Schema.create(Schema.Type.INT)), 0)))));

    PathKey inner = PathKey.ofRoot().child(0).child(0).child(0);
    assertThat(result.at(1, inner)).isNotEqualTo(result.at(0, inner));
  }

  // ---------------------------------------------------------------------------------------------
  // Identity policy
  // ---------------------------------------------------------------------------------------------

  @Test
  void autoCannotSeeARenameInAMessageThatRecordsNoNumbers() {
    // The documented hole: with no numbers recorded, AUTO falls back to names and reads the
    // rename as a drop plus an add.
    List<LogicalType> versions = Arrays.asList(
        lt(struct(field("name"), field("age"))),
        lt(struct(field("full_name"), field("age"))));

    ProvenanceResult result = ProvenanceComputer.compute(versions, IdentityPolicy.AUTO);

    assertThat(at(result, 1, 0)).isNotEqualTo(at(result, 0, 0));
    assertThat(result.intersection(0, 1)).containsExactly(at(result, 0, 1));
  }

  @Test
  void protobufDerivesTheNumbersAMessageDidNotRecord() {
    List<LogicalType> versions = Arrays.asList(
        lt(struct(field("name"), field("age"))),
        lt(struct(field("full_name"), field("age"))));

    ProvenanceResult result = ProvenanceComputer.compute(versions, IdentityPolicy.PROTOBUF);

    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 0, 0).getIdentity().getFieldNumber()).isEqualTo(1);
    assertThat(at(result, 0, 1).getIdentity().getFieldNumber()).isEqualTo(2);
    assertThat(result.intersection(0, 1)).hasSize(2);
  }

  @Test
  void protobufDerivationContinuesThroughOneofBranches() {
    // Mirrors the reader's rule: regular fields take 1..n, then the oneof branches continue it.
    // The oneof container field itself has no number and stays name-identified.
    Schema oneof = Schema.createUnion(Arrays.asList(
        new UnionBranch("x", Schema.create(Schema.Type.INT)),
        new UnionBranch("y", Schema.create(Schema.Type.INT))));
    Schema renamedOneof = Schema.createUnion(Arrays.asList(
        new UnionBranch("x2", Schema.create(Schema.Type.INT)),
        new UnionBranch("y", Schema.create(Schema.Type.INT))));

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"), field("b"), new Field("choice", oneof, 2))),
        lt(struct(field("a"), field("b"), new Field("choice", renamedOneof, 2)))),
        IdentityPolicy.PROTOBUF);

    PathKey x = PathKey.ofRoot().child(2).child(0);
    assertThat(result.at(0, x).getIdentity().getFieldNumber()).isEqualTo(3);
    assertThat(result.at(1, x)).isEqualTo(result.at(0, x));
    assertThat(result.at(0, PathKey.ofRoot().child(2)).getIdentity().getFieldNumber()).isNull();
  }

  @Test
  void protobufLeavesRecordedNumbersAlone() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(numbered("a", 7), numbered("b", 3)))), IdentityPolicy.PROTOBUF);

    assertThat(at(result, 0, 0).getIdentity().getFieldNumber()).isEqualTo(7);
    assertThat(at(result, 0, 1).getIdentity().getFieldNumber()).isEqualTo(3);
  }

  @Test
  void avroIgnoresRecordedNumbers() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(numbered("name", 1))),
        lt(struct(numbered("full_name", 1)))), IdentityPolicy.AVRO);

    assertThat(at(result, 1, 0)).isNotEqualTo(at(result, 0, 0));
    assertThat(at(result, 0, 0).getIdentity().getFieldNumber()).isNull();
  }

  @Test
  void aPerVersionPolicyCarriesCorrespondenceAcrossAFormatSwitch() {
    ProvenanceResult result = ProvenanceComputer.compute(
        Arrays.asList(lt(struct(field("name"))), lt(struct(numbered("name", 1)))),
        Arrays.asList(IdentityPolicy.AVRO, IdentityPolicy.AVRO));

    assertThat(at(result, 1, 0)).isEqualTo(at(result, 0, 0));
  }

  @Test
  void policiesMustMatchVersions() {
    assertThatThrownBy(() -> ProvenanceComputer.compute(
        Arrays.asList(lt(struct(field("name")))),
        Arrays.asList(IdentityPolicy.AUTO, IdentityPolicy.AUTO)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("one policy per version");
  }

  // ---------------------------------------------------------------------------------------------
  // Validation
  // ---------------------------------------------------------------------------------------------

  @Test
  void twoFieldsSharingAnAliasThatNamesNothingAreFine() {
    // Aliases are lookups into history; one naming nothing claims nothing.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a", "shared"), field("b", "shared")))));
    assertThat(result.byPath(0)).hasSize(2);
  }

  @Test
  void twoFieldsResolvingToOneNumberIsRejected() {
    assertThatThrownBy(() -> ProvenanceComputer.compute(Arrays.asList(
        lt(struct(numbered("a", 1), numbered("b", 1))))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Multiple entities resolve to the same logical identity");
  }

  @Test
  void aNullVersionIsRejected() {
    assertThatThrownBy(() -> ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"))), null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("version 1");
  }

  @Test
  void jsonPropertyNamesAreStableAcrossAGap() {
    List<LogicalType> versions = Arrays.asList(
        lt(struct(field("name"))), lt(struct()), lt(struct(field("name"))));

    ProvenanceResult json = ProvenanceComputer.compute(versions, IdentityPolicy.JSON);
    assertThat(at(json, 2, 0).getIdentity()).isEqualTo(at(json, 0, 0).getIdentity());
    assertThat(at(json, 2, 0)).isNotEqualTo(at(json, 0, 0));
    assertThat(json.intersection(0, 2)).isEmpty();

    // Avro mints a distinct identity instead, since an alias could have reconnected to the old one.
    ProvenanceResult avro = ProvenanceComputer.compute(versions, IdentityPolicy.AVRO);
    assertThat(at(avro, 2, 0).getIdentity()).isNotEqualTo(at(avro, 0, 0).getIdentity());
  }

  @Test
  void protobufRootRenameKeepsItsFields() {
    Map<String, Schema> v0 = new LinkedHashMap<>();
    v0.put("acme.User", struct(field("city")));
    Map<String, Schema> v1 = new LinkedHashMap<>();
    v1.put("acme.Person", aliased(struct(field("city")), "acme.User"));

    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(Schema.createNamedTypeRef("acme.User"), v0),
        lt(Schema.createNamedTypeRef("acme.Person"), v1)), IdentityPolicy.PROTOBUF);

    // The root is no location, so its name is not its fields' identity, whether the converter
    // keeps it a reference or unwraps it; a message renamed where it is used still restarts.
    assertThat(result.at(1, PathKey.ofRoot().child(0)))
        .isEqualTo(result.at(0, PathKey.ofRoot().child(0)));
  }

  @Test
  void aNamedTypeAndARootFieldMayShareAName() {
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Thing", struct(field("id")));
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(new Field("Thing", Schema.createNamedTypeRef("Thing"), 0)), namedTypes)));

    Provenance namedType = result.at(0, PathKey.ofTypeUse("Thing", PathKey.ofRoot().child(0)));
    Provenance rootField = result.at(0, PathKey.ofRoot().child(0));
    assertThat(namedType.getKind()).isEqualTo(EntityKind.NAMED_TYPE);
    assertThat(rootField.getKind()).isEqualTo(EntityKind.FIELD);
    assertThat(namedType.getIdentity()).isNotEqualTo(rootField.getIdentity());
  }

  // ---------------------------------------------------------------------------------------------
  // Presence patterns -- the core invariant, enumerated rather than sampled
  // ---------------------------------------------------------------------------------------------

  @Test
  void everyPresencePatternKeepsOneProvenancePerRun() {
    // The core invariant: one provenance per continuous presence interval. A field's presence
    // across five versions is a five-bit mask, so all 32 histories are enumerable -- no sampling,
    // nothing skipped. Asserting that the presence start equals the first version of the current
    // run states the invariant directly, rather than inferring it from adjacent pairs.
    for (int pattern = 0; pattern < 32; pattern++) {
      ProvenanceResult result = ProvenanceComputer.compute(history(pattern, 1, mask -> struct(
          present(mask, 0) ? new Field[] {field("a")} : new Field[0])));

      int runStart = -1;
      for (int version = 0; version < VERSIONS; version++) {
        Provenance current = at(result, version, 0);
        if (!present(pattern >> version, 0)) {
          assertThat(current).as("pattern %s, v%s", pattern, version).isNull();
          runStart = -1;
          continue;
        }
        runStart = runStart < 0 ? version : runStart;
        assertThat(current.getPresenceStartVersion())
            .as("pattern %s, v%s", pattern, version).isEqualTo(runStart);
        // With no alias to reconnect through, each run also mints its own identity.
        assertThat(current.getIdentity().getIdentityOriginVersion())
            .as("pattern %s, v%s", pattern, version).isEqualTo(runStart);
      }
    }
  }

  @Test
  void independentPresencePatternsDoNotPerturbEachOther() {
    // All 1024 interleavings of two fields. One field's churn must not shift the other's
    // intervals, however the two patterns line up.
    for (int pattern = 0; pattern < 1024; pattern++) {
      ProvenanceResult result = ProvenanceComputer.compute(history(pattern, 2, mask -> {
        List<Field> fields = new ArrayList<>();
        if (present(mask, 0)) {
          fields.add(field("a"));
        }
        if (present(mask, 1)) {
          fields.add(field("b"));
        }
        return struct(fields.toArray(new Field[0]));
      }));

      int[] runStart = {-1, -1};
      for (int version = 0; version < VERSIONS; version++) {
        int mask = pattern >> (version * 2);
        int position = 0;
        for (int fieldIndex = 0; fieldIndex < 2; fieldIndex++) {
          if (!present(mask, fieldIndex)) {
            runStart[fieldIndex] = -1;
            continue;
          }
          runStart[fieldIndex] = runStart[fieldIndex] < 0 ? version : runStart[fieldIndex];
          assertThat(at(result, version, position).getPresenceStartVersion())
              .as("pattern %s, v%s, field %s", pattern, version, fieldIndex)
              .isEqualTo(runStart[fieldIndex]);
          position++;
        }
      }
    }
  }

  @Test
  void anAliasReconnectsFromAnyDormancyDepth() {
    // a is renamed to b, which keeps declaring the alias. Whatever the gaps, b is always the same
    // logical entity as the original a -- and always on the interval its current run began.
    for (int pattern = 0; pattern < 16; pattern++) {
      List<LogicalType> history = new ArrayList<>();
      history.add(lt(struct(field("a"))));
      for (int version = 1; version < VERSIONS; version++) {
        history.add(lt(present(pattern >> (version - 1), 0)
            ? struct(field("b", "a")) : struct()));
      }
      ProvenanceResult result = ProvenanceComputer.compute(history);

      Identity original = at(result, 0, 0).getIdentity();
      int runStart = 0;
      for (int version = 1; version < VERSIONS; version++) {
        Provenance current = at(result, version, 0);
        if (!present(pattern >> (version - 1), 0)) {
          assertThat(current).as("pattern %s, v%s", pattern, version).isNull();
          runStart = -1;
          continue;
        }
        runStart = runStart < 0 ? version : runStart;
        assertThat(current.getIdentity()).as("pattern %s, v%s", pattern, version)
            .isEqualTo(original);
        assertThat(current.getPresenceStartVersion())
            .as("pattern %s, v%s", pattern, version).isEqualTo(runStart);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Set algebra
  // ---------------------------------------------------------------------------------------------

  @Test
  void intersectionIsSymmetricAndCorrespondenceIsDirectional() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("name"), field("age"))),
        lt(struct(field("full_name", "name"), field("age"), field("address")))));

    assertThat(result.intersection(0, 1)).isEqualTo(result.intersection(1, 0)).hasSize(2);
    assertThat(result.union(0, 1)).hasSize(3);
    assertThat(result.difference(1, 0)).containsExactly(at(result, 1, 2));
    assertThat(result.difference(0, 1)).isEmpty();

    assertThat(result.correspondence(0, 1)).containsExactly(
        entry(PathKey.ofRoot().child(0), PathKey.ofRoot().child(0)),
        entry(PathKey.ofRoot().child(1), PathKey.ofRoot().child(1)));
  }

  @Test
  void correspondenceSurvivesReorderingAndSkipsAReusedName() {
    // b survives, moving from index 1 to index 0. a is dropped at v1 and a field called a is added
    // back at v2 -- a different entity that happens to reuse the name. Matching on names would map
    // it onto the original; provenance leaves it out of the correspondence entirely.
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"), field("b"))),
        lt(struct(field("b"))),
        lt(struct(field("b"), field("a")))));

    assertThat(result.correspondence(0, 2)).containsExactly(
        entry(PathKey.ofRoot().child(1), PathKey.ofRoot().child(0)));
    assertThat(at(result, 2, 1)).isNotEqualTo(at(result, 0, 0));
  }

  @Test
  void intersectionSurvivesAPartialGap() {
    ProvenanceResult result = ProvenanceComputer.compute(Arrays.asList(
        lt(struct(field("a"), field("b"), field("c"))),
        lt(struct(field("a"), field("c"))),
        lt(struct(field("a"), field("b"), field("c")))));

    assertThat(result.intersection(0, 2))
        .containsExactly(at(result, 0, 0), at(result, 0, 2));
    assertThat(at(result, 2, 1)).isNotEqualTo(at(result, 0, 1));
    assertThat(at(result, 2, 1).getPresenceStartVersion()).isEqualTo(2);
    assertThat(at(result, 2, 0)).isEqualTo(at(result, 0, 0));
    assertThat(at(result, 2, 2)).isEqualTo(at(result, 0, 2));
  }

  @Test
  void anEmptySequenceComputesNothing() {
    ProvenanceResult result = ProvenanceComputer.compute(new ArrayList<>());
    assertThat(result.versionCount()).isZero();
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static Map.Entry<PathKey, PathKey> entry(PathKey from, PathKey to) {
    return new java.util.AbstractMap.SimpleEntry<>(from, to);
  }

  /**
   * The provenance of the top-level field at {@code position} of the root schema.
   */
  private static Provenance at(ProvenanceResult result, int version, int position) {
    return result.at(version, PathKey.ofRoot().child(position));
  }

  /** Versions in an enumerated presence pattern. */
  private static final int VERSIONS = 5;

  /**
   * True when the bit for {@code index} is set in {@code mask}.
   */
  private static boolean present(int mask, int index) {
    return (mask & (1 << index)) != 0;
  }

  /**
   * A history of {@link #VERSIONS} versions, each built from its own {@code bitsPerVersion}-wide
   * slice of {@code pattern}. The slice is handed over shifted down, so a builder reads the bits
   * it needs from the low end via {@link #present}.
   */
  private static List<LogicalType> history(
      int pattern, int bitsPerVersion, IntFunction<Schema> builder) {
    List<LogicalType> versions = new ArrayList<>();
    for (int version = 0; version < VERSIONS; version++) {
      versions.add(lt(builder.apply(pattern >> (version * bitsPerVersion))));
    }
    return versions;
  }

  private static LogicalType lt(Schema root) {
    return new LogicalType(root);
  }

  private static LogicalType lt(Schema root, Map<String, Schema> namedTypes) {
    return new LogicalType(root, namedTypes);
  }

  private static Map<String, Schema> namedTypes(Object... nameThenBody) {
    Map<String, Schema> types = new LinkedHashMap<>();
    for (int i = 0; i < nameThenBody.length; i += 2) {
      types.put((String) nameThenBody[i], (Schema) nameThenBody[i + 1]);
    }
    return types;
  }

  private static Schema struct(Field... fields) {
    return Schema.createStruct(Arrays.asList(fields));
  }

  /** An INT field, optionally declaring Avro aliases. */
  private static Field field(String name, String... aliases) {
    Map<String, Object> params = aliases.length == 0
        ? null : Map.of(Schema.AVRO_ALIASES, String.join(",", aliases));
    return new Field(name, Schema.create(Schema.Type.INT), 0, null, false, null, null, params);
  }

  /** An INT field carrying an explicit Protobuf field number. */
  private static Field numbered(String name, int number) {
    return new Field(name, Schema.create(Schema.Type.INT), 0, null, false, null, null,
        Map.of(Schema.PROTOBUF_FIELD_NUMBER, String.valueOf(number)));
  }

  /** A named type body declaring Avro aliases (its previous full names). */
  private static Schema aliased(Schema schema, String... aliases) {
    return schema.setParams(Map.of(Schema.AVRO_ALIASES, String.join(",", aliases)));
  }
}
