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
 * specified by: rename continuity, gaps, aliases, nested scope, and the two signals a
 * {@link LogicalType} can carry, names and Protobuf numbers.
 */
class ProvenanceComputerTest {

  // ---------------------------------------------------------------------------------------------
  // Continuity -- an unbroken chain keeps one pid
  // ---------------------------------------------------------------------------------------------

  @Test
  void unchangedFieldKeepsItsPidAcrossVersions() {
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct(field("name"))),
        lt(struct(field("name"))));

    assertThat(pids.at(0, 0)).isEqualTo(pids.at(1, 0)).isEqualTo(pids.at(2, 0));
  }

  @Test
  void avroAliasCarriesThePidAcrossRename() {
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
  }

  @Test
  void protobufNumberCarriesThePidAcrossRename() {
    Pids pids = protobuf(
        lt(struct(numbered("name", 1))),
        lt(struct(numbered("full_name", 1))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
  }

  // ---------------------------------------------------------------------------------------------
  // Separation -- a location absent from one version is new when it returns
  // ---------------------------------------------------------------------------------------------

  @Test
  void reintroducedFieldIsNew() {
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct()),
        lt(struct(field("name"))));

    assertThat(pids.isNew(2, 0)).isTrue();
  }

  @Test
  void renameSurvivesButDeletionBreaksTheChain() {
    // v0: name;  v1: full_name aliases [name];  v2: absent;  v3: full_name, new.
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))),
        lt(struct()),
        lt(struct(field("full_name"))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.isNew(3, 0)).isTrue();
  }

  @Test
  void anAliasToAFieldAbsentFromThePreviousVersionContinuesNothing() {
    // An alias names what the previous version called a field. name is absent from v1, so
    // full_name matches nothing there.
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct()),
        lt(struct(field("full_name", "name"))));

    assertThat(pids.isNew(2, 0)).isTrue();
  }

  @Test
  void protobufNumberReuseAfterAGapIsNew() {
    Pids pids = protobuf(
        lt(struct(numbered("name", 1))),
        lt(struct()),
        lt(struct(numbered("full_name", 1))));

    assertThat(pids.isNew(2, 0)).isTrue();
  }

  // ---------------------------------------------------------------------------------------------
  // Names released and taken over
  // ---------------------------------------------------------------------------------------------

  @Test
  void droppingAnAliasReleasesTheNameForANewField() {
    // v0 name;  v1 full_name aliases [name];  v2 full_name, alias dropped;
    // v3 adds a brand-new field called name, which must not continue the old one.
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))),
        lt(struct(field("full_name"))),
        lt(struct(field("full_name"), field("name"))));

    assertThat(pids.at(3, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.isNew(3, 1)).isTrue();
  }

  @Test
  void anAliasNamingOnlyAnOlderNameContinuesNothing() {
    // Same ladder, but v3 aliases the v0 name, which v2 no longer has: other matches nothing.
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))),
        lt(struct(field("full_name"))),
        lt(struct(field("other", "name"))));

    assertThat(pids.isNew(3, 0)).isTrue();
  }

  @Test
  void aNameTakenOverAfterAGapIsUndisturbedByAnAliasToAnOlderName() {
    // v2 x takes over a name a once aliased; v3 c aliases a, absent since v1. x keeps its pid,
    // and c is new.
    Pids pids = avro(
        lt(struct(field("a", "x"))),
        lt(struct()),
        lt(struct(field("x"))),
        lt(struct(field("x"), field("c", "a"))),
        lt(struct(field("x"), field("c"))));

    assertThat(pids.at(3, 0)).isEqualTo(pids.at(2, 0));
    assertThat(pids.at(4, 0)).isEqualTo(pids.at(2, 0));
    assertThat(pids.chainStart(4, 0)).isEqualTo(2);
    assertThat(pids.isNew(3, 1)).isTrue();
    assertThat(pids.at(4, 1)).isEqualTo(pids.at(3, 1));
  }

  @Test
  void aFormerAliasIsFreeForANewFieldAtOnce() {
    // v0: a with the alias x. v1: a disappears and x is immediately used by a new field, which
    // continues nothing: x was never a name a had.
    Pids pids = avro(
        lt(struct(field("a", "x"))),
        lt(struct(field("x"))));

    assertThat(pids.isNew(1, 0)).isTrue();
  }

  @Test
  void namesAFieldOnceAliasedAreNewWhenTheyReturn() {
    // Of a's aliases {x, y}, x is taken over at v2 and keeps its pid; y at v4 matches nothing.
    Pids pids = avro(
        lt(struct(field("a", "x", "y"))),
        lt(struct()),
        lt(struct(field("x"))),
        lt(struct(field("x"), field("c", "a"))),
        lt(struct(field("x"), field("y"), field("c"))));

    assertThat(pids.at(3, 0)).isEqualTo(pids.at(2, 0));
    assertThat(pids.at(4, 0)).isEqualTo(pids.at(2, 0));
    assertThat(pids.isNew(4, 1)).isTrue();
    assertThat(pids.at(4, 2)).isEqualTo(pids.at(3, 1));
  }

  @Test
  void aFieldGainingAnAliasContinuesItself() {
    Pids pids = avro(
        lt(struct(field("a"))),
        lt(struct(field("a", "x"))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
  }

  @Test
  void anExplicitAliasOutranksAMatchByName() {
    // Both peers claim a at v1: the new field called a by name, and b by aliasing a. Avro renames
    // the writer's a to the field aliasing it, so b continues and a is new.
    Pids pids = avro(
        lt(struct(field("a"))),
        lt(struct(field("a"), field("b", "a"))));

    assertThat(pids.at(1, 1)).isEqualTo(pids.at(0, 0));
    assertThat(pids.isNew(1, 0)).isTrue();
  }

  @Test
  void aCarriedForwardAliasClaimsNothingNew() {
    // v1's rename-and-reuse, kept as is in v2: full_name's alias names the new field now, but
    // full_name introduced it at v1, so both fields simply continue.
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"), field("name"))),
        lt(struct(field("full_name", "name"), field("name"))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.at(2, 0)).isEqualTo(pids.at(1, 0));
    assertThat(pids.at(2, 1)).isEqualTo(pids.at(1, 1));
  }

  @Test
  void aFormerAliasClaimsNothing() {
    // x was only ever a's alias. Avro reads aliases from the reader alone, so a reader field
    // aliasing x finds nothing in a writer whose field is called a.
    Pids pids = avro(
        lt(struct(field("a", "x"))),
        lt(struct(field("a"), field("b", "x"))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.isNew(1, 1)).isTrue();
  }

  @Test
  void aFieldAliasingTwoPreviousFieldsIsRejected() {
    assertThatThrownBy(() -> avro(
        lt(struct(field("a"), field("b"))),
        lt(struct(field("c", "a", "b")))))
        .isInstanceOf(AmbiguousProvenanceException.class)
        .hasMessageContaining("matches both a and b");
  }

  @Test
  void anAliasAcrossAGapClaimsNoOtherName() {
    // c aliases a, absent from v1, so is new; x, which a once aliased, is new at v3.
    Pids pids = avro(
        lt(struct(field("a", "x"))),
        lt(struct()),
        lt(struct(field("c", "a"))),
        lt(struct(field("c"), field("x"))));

    assertThat(pids.isNew(2, 0)).isTrue();
    assertThat(pids.at(3, 0)).isEqualTo(pids.at(2, 0));
    assertThat(pids.isNew(3, 1)).isTrue();
  }

  @Test
  void aNameReleasedInThisVersionCannotBeInheritedInIt() {
    // v1 renames name to full_name through an alias. v2 drops that alias and, in the same version,
    // introduces a brand-new field called name -- which v1 had no field of.
    Pids pids = avro(
        lt(struct(field("name"))),
        lt(struct(field("full_name", "name"))),
        lt(struct(field("full_name"), field("name"))));

    assertThat(pids.at(2, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.isNew(2, 1)).isTrue();
  }

  @Test
  void aFormerAliasIsNotAMatchByName() {
    // x was only ever a's alias, never its name, so the field called x at v1 is a coincidental
    // reuse rather than a's continuation.
    Pids pids = avro(
        lt(struct(field("a", "x"))),
        lt(struct(field("x"), field("a"))));

    assertThat(pids.isNew(1, 0)).isTrue();
    assertThat(pids.at(1, 1)).isEqualTo(pids.at(0, 0));
  }

  @Test
  void twoFieldsClaimingOnePreviousFieldByAliasIsRejected() {
    assertThatThrownBy(() -> avro(
        lt(struct(field("a"))),
        lt(struct(field("b", "a"), field("c", "a")))))
        .isInstanceOf(AmbiguousProvenanceException.class)
        .hasMessageContaining("multiple entities claim a via aliases");
  }

  @Test
  void aFieldContinuingItselfAndAliasingAnotherIsRejected() {
    // p continues p and, by a new alias, q: one field cannot continue two.
    assertThatThrownBy(() -> avro(
        lt(struct(field("p"), field("q"))),
        lt(struct(field("p", "q")))))
        .isInstanceOf(AmbiguousProvenanceException.class)
        .hasMessageContaining("names another entity by a new alias");
  }

  @Test
  void twoFieldsSwappedByAliasesAreRejected() {
    assertThatThrownBy(() -> avro(
        lt(struct(field("a"), field("b"))),
        lt(struct(field("b", "a"), field("a", "b")))))
        .isInstanceOf(AmbiguousProvenanceException.class);
  }

  @Test
  void aDuplicateAliasChangesNothing() {
    Pids pids = avro(
        lt(struct(field("a"))),
        lt(struct(field("b", "a", "a"))));
    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
  }

  @Test
  void anAliasRepeatingTheNameChangesNothing() {
    Pids pids = avro(
        lt(struct(field("a"))),
        lt(struct(field("a", "a"))));
    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
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

    Pids pids = avro(version);

    assertThat(pids.at(0, 0, 0)).isNotNull().isNotEqualTo(pids.at(0, 1, 0));
    assertThat(pids.version(0)).hasSize(4);
  }

  @Test
  void aTypeUnreachableFromTheRootHasNoMembers() {
    // Named types are matched where they are used, so one that is never used contributes nothing.
    Pids pids = avro(lt(struct(field("id")), namedTypes("Unused", struct(field("x")))));
    assertThat(pids.version(0)).hasSize(1);
  }

  @Test
  void renamingARecordThroughAnAliasKeepsItsChildren() {
    Pids pids = avro(
        lt(struct(new Field("u", Schema.createNamedTypeRef("com.acme.User"), 0)),
            namedTypes("com.acme.User", struct(field("city")))),
        lt(struct(new Field("u", Schema.createNamedTypeRef("com.acme.Person"), 0)),
            namedTypes("com.acme.Person", aliased(struct(field("city")), "com.acme.User"))));

    assertThat(pids.at(1, 0, 0)).isNotNull().isEqualTo(pids.at(0, 0, 0));
  }

  @Test
  void renamingARecordWithoutAnAliasRestartsItsChildren() {
    Pids pids = avro(
        lt(struct(new Field("u", Schema.createNamedTypeRef("com.acme.User"), 0)),
            namedTypes("com.acme.User", struct(field("city")))),
        lt(struct(new Field("u", Schema.createNamedTypeRef("com.acme.Person"), 0)),
            namedTypes("com.acme.Person", struct(field("city")))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.isNew(1, 0, 0)).isTrue();
  }

  @Test
  void droppingARecordBreaksItsChildrensChains() {
    Map<String, Schema> present = namedTypes("com.acme.Address", struct(field("city")));

    Pids pids = avro(
        lt(Schema.createNamedTypeRef("com.acme.Address"), present),
        lt(struct()),
        lt(Schema.createNamedTypeRef("com.acme.Address"), present));

    assertThat(pids.isNew(2, 0)).isTrue();
  }

  @Test
  void aChildsChainBreaksIndependentlyOfItsParent() {
    // city throughout; zip -> absent -> new.
    IntFunction<LogicalType> address = zip -> lt(struct(
        new Field("address", Schema.createNamedTypeRef("Address"), 0)), namedTypes("Address",
            zip == 1 ? struct(field("city"), field("zip")) : struct(field("city"))));

    Pids pids = avro(address.apply(1), address.apply(0), address.apply(1));

    assertThat(pids.at(2, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.at(2, 0, 0)).isEqualTo(pids.at(0, 0, 0));
    assertThat(pids.isNew(2, 0, 1)).isTrue();
  }

  @Test
  void namedTypesAreNotMembers() {
    LogicalType version = lt(struct(new Field("a", Schema.createNamedTypeRef("Address"), 0)),
        namedTypes("Address", struct(field("city"))));

    Pids pids = avro(version, version);

    assertThat(pids.version(0).keySet()).containsExactly(path(0), path(0, 0));
    assertThat(pids.version(1)).isEqualTo(pids.version(0));
  }

  @Test
  void aRecursiveTypeIsRejected() {
    // A type is matched at every use, and a recursive one has no finite set of uses.
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Node", struct(
        field("value"),
        new Field("next", Schema.createNamedTypeRef("Node"), 1)));

    assertThatThrownBy(() -> avro(lt(Schema.createNamedTypeRef("Node"), namedTypes)))
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

    Pids pids = avro(holding.apply(0), holding.apply(1), holding.apply(0));

    assertThat(pids.isNew(2, 1, 0)).isTrue();
    assertThat(pids.at(2, 0, 0)).isEqualTo(pids.at(0, 0, 0));
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

    Pids pids = avro(v0, v1);

    assertThat(pids.at(1, 0, 0)).isEqualTo(pids.at(0, 0, 0));
    assertThat(pids.at(1, 1, 0)).isEqualTo(pids.at(0, 1, 0));
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

    Pids pids = avro(v0, v1);

    assertThat(pids.at(1, 0, 0)).isEqualTo(pids.at(0, 0, 0));
    assertThat(pids.at(1, 1, 0)).isEqualTo(pids.at(0, 1, 0));
  }

  @Test
  void aTakeoverInOneScopeDoesNotAffectAnother() {
    // User.name going away and coming back must leave Order.name untouched.
    IntFunction<LogicalType> version = hasName -> lt(struct(
        new Field("user", Schema.createNamedTypeRef("User"), 0),
        new Field("order", Schema.createNamedTypeRef("Order"), 1)), namedTypes(
            "User", hasName == 1 ? struct(field("name")) : struct(),
            "Order", struct(field("name"))));
    Pids pids = avro(version.apply(1), version.apply(0), version.apply(1));

    assertThat(pids.at(1, 1, 0)).isEqualTo(pids.at(0, 1, 0));
    assertThat(pids.at(2, 1, 0)).isEqualTo(pids.at(0, 1, 0));
    assertThat(pids.isNew(2, 0, 0)).isTrue();
  }

  @Test
  void aRecordRenameKeepsDeeplyNestedChildren() {
    // Two levels down, through an inline struct. A child is matched under its parent's match,
    // not the parent's text, so the alias carries the whole subtree.
    Pids pids = avro(
        lt(struct(new Field("u", Schema.createNamedTypeRef("User"), 0)), namedTypes("User",
            struct(new Field("address", struct(field("city")), 0)))),
        lt(struct(new Field("u", Schema.createNamedTypeRef("Person"), 0)), namedTypes("Person",
            aliased(struct(new Field("address", struct(field("city")), 0)), "User"))));

    assertThat(pids.at(1, 0, 0, 0)).isNotNull().isEqualTo(pids.at(0, 0, 0, 0));
  }

  @Test
  void aRenamedRecordReturningAfterAGapIsNew() {
    Pids pids = avro(
        lt(struct(new Field("u", Schema.createNamedTypeRef("User"), 0)),
            namedTypes("User", struct(field("city")))),
        lt(struct()),
        lt(struct(new Field("u", Schema.createNamedTypeRef("Person"), 0)),
            namedTypes("Person", aliased(struct(field("city")), "User"))));

    // u is absent from v1, so nothing under it matches either.
    assertThat(pids.isNew(2, 0)).isTrue();
    assertThat(pids.isNew(2, 0, 0)).isTrue();
  }

  @Test
  void reorderingNestedFieldsKeepsEveryPid() {
    // Matching is by name within the parent's match, never by path, so moving fields around
    // changes only where they are found.
    Pids pids = avro(
        lt(struct(
            new Field("a", struct(field("x"), field("y")), 0),
            new Field("b", struct(field("z")), 1))),
        lt(struct(
            new Field("b", struct(field("z")), 0),
            new Field("a", struct(field("y"), field("x")), 1))));

    assertThat(pids.at(1, 1)).isEqualTo(pids.at(0, 0));
    assertThat(pids.at(1, 1, 1)).isEqualTo(pids.at(0, 0, 0));
    assertThat(pids.at(1, 1, 0)).isEqualTo(pids.at(0, 0, 1));
    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 1));
    assertThat(pids.at(1, 0, 0)).isEqualTo(pids.at(0, 1, 0));
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

    assertThat(avro(lt(root)).version(0).keySet()).containsExactly(
        path(0),            // id
        path(1),            // items
        path(1, 0, 0),      // items[].sku
        path(2),            // tags
        path(2, 1, 0),      // tags{value}.label
        path(3),            // payment
        path(3, 0),         // payment.card
        path(3, 1),         // payment.cash
        path(3, 1, 0));     // payment.cash.amount
  }

  @Test
  void changingACollectionKindRestartsTheMembersInside() {
    // ARRAY<STRUCT> and MAP<K, STRUCT> both step to index 0, but under different steps: the
    // inner members do not match.
    Pids pids = avro(
        lt(struct(new Field("f", Schema.createArray(struct(field("x"))), 0))),
        lt(struct(new Field("f", Schema.createMap(struct(field("x")),
            Schema.create(Schema.Type.INT)), 0))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.isNew(1, 0, 0, 0)).isTrue();
  }

  // ---------------------------------------------------------------------------------------------
  // Identity policy
  // ---------------------------------------------------------------------------------------------

  @Test
  void protobufDerivesTheNumbersAMessageDidNotRecord() {
    // A rename in a message that records no numbers: under Avro rules a drop and an add, under
    // Protobuf's the same derived number.
    List<LogicalType> versions = Arrays.asList(
        lt(struct(field("name"), field("age"))),
        lt(struct(field("full_name"), field("age"))));

    Pids pids = Pids.of(versions, IdentityPolicy.PROTOBUF);
    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.at(1, 1)).isEqualTo(pids.at(0, 1));
    assertThat(Pids.of(versions, IdentityPolicy.AVRO).isNew(1, 0)).isTrue();
  }

  @Test
  void protobufDerivationContinuesThroughOneofBranches() {
    // Mirrors the reader's rule: regular fields take 1..n, then the oneof branches continue it,
    // so x keeps number 3 when renamed. The oneof container follows its members' numbers.
    Schema oneof = Schema.createUnion(Arrays.asList(
        new UnionBranch("x", Schema.create(Schema.Type.INT)),
        new UnionBranch("y", Schema.create(Schema.Type.INT))));
    Schema renamedOneof = Schema.createUnion(Arrays.asList(
        new UnionBranch("x2", Schema.create(Schema.Type.INT)),
        new UnionBranch("y", Schema.create(Schema.Type.INT))));

    Pids pids = protobuf(
        lt(struct(field("a"), field("b"), new Field("choice", oneof, 2))),
        lt(struct(field("a"), field("b"), new Field("renamed", renamedOneof, 2))));

    assertThat(pids.at(1, 2)).isEqualTo(pids.at(0, 2));
    assertThat(pids.at(1, 2, 0)).isEqualTo(pids.at(0, 2, 0));
  }

  @Test
  void protobufFollowsRecordedNumbersOverPositions() {
    Pids pids = protobuf(
        lt(struct(numbered("a", 7), numbered("b", 3))),
        lt(struct(numbered("x", 3), numbered("y", 7))));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 1));
    assertThat(pids.at(1, 1)).isEqualTo(pids.at(0, 0));
  }

  @Test
  void avroIgnoresRecordedNumbers() {
    Pids pids = avro(
        lt(struct(numbered("name", 1))),
        lt(struct(numbered("full_name", 1))));

    assertThat(pids.isNew(1, 0)).isTrue();
  }

  @Test
  void anAvroVersionRecordingNumbersStillMatchesByName() {
    Pids pids = Pids.of(ProvenanceComputer.report(
        Arrays.asList(lt(struct(field("name"))), lt(struct(numbered("name", 1)))),
        Arrays.asList(IdentityPolicy.AVRO, IdentityPolicy.AVRO)));

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
  }

  @Test
  void policiesMustMatchVersions() {
    assertThatThrownBy(() -> ProvenanceComputer.report(
        Arrays.asList(lt(struct(field("name")))),
        Arrays.asList(IdentityPolicy.AVRO, IdentityPolicy.AVRO)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("one policy per version");
  }

  // ---------------------------------------------------------------------------------------------
  // Validation
  // ---------------------------------------------------------------------------------------------

  @Test
  void twoFieldsSharingAnAliasThatNamesNothingAreFine() {
    // Aliases are lookups into the previous version; one naming nothing claims nothing.
    assertThat(avro(lt(struct(field("a", "shared"), field("b", "shared")))).version(0))
        .hasSize(2);
  }

  @Test
  void twoFieldsOfOneNumberAreRejected() {
    assertThatThrownBy(() -> protobuf(lt(struct(numbered("a", 1), numbered("b", 1)))))
        .isInstanceOf(AmbiguousProvenanceException.class)
        .hasMessageContaining("Multiple entities resolve to the same logical identity");
  }

  @Test
  void aNullVersionIsRejected() {
    assertThatThrownBy(() -> avro(lt(struct(field("a"))), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("version 1");
  }

  @Test
  void aPropertyReturningAfterAGapIsNewUnderEveryPolicy() {
    List<LogicalType> versions = Arrays.asList(
        lt(struct(field("name"))), lt(struct()), lt(struct(field("name"))));

    for (IdentityPolicy policy : IdentityPolicy.values()) {
      assertThat(Pids.of(versions, policy).isNew(2, 0)).as("%s", policy).isTrue();
    }
  }

  @Test
  void protobufRootRenameKeepsItsFields() {
    Pids pids = protobuf(
        lt(Schema.createNamedTypeRef("acme.User"), namedTypes("acme.User", struct(field("city")))),
        lt(Schema.createNamedTypeRef("acme.Person"),
            namedTypes("acme.Person", struct(field("city")))));

    // The root is no location, so its name does not decide its fields, whether the converter
    // keeps it a reference or unwraps it; a message renamed where it is used still restarts.
    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
  }

  @Test
  void aNamedTypeAndAFieldMayShareAName() {
    LogicalType version = lt(struct(new Field("Thing", Schema.createNamedTypeRef("Thing"), 0)),
        namedTypes("Thing", struct(field("id"))));

    Pids pids = avro(version, version);

    assertThat(pids.version(1)).isEqualTo(pids.version(0)).hasSize(2);
  }

  // ---------------------------------------------------------------------------------------------
  // Presence patterns -- the core invariant, enumerated rather than sampled
  // ---------------------------------------------------------------------------------------------

  @Test
  void everyPresencePatternKeepsOnePidPerRun() {
    // The core invariant: one pid per unbroken run of versions. A field's presence across five
    // versions is a five-bit mask, so all 32 histories are enumerable -- no sampling, nothing
    // skipped. Each run starts with a new pid and keeps it.
    for (int pattern = 0; pattern < 32; pattern++) {
      Pids pids = avro(history(pattern, 1, mask -> struct(
          present(mask, 0) ? new Field[] {field("a")} : new Field[0])));

      int runStart = -1;
      for (int version = 0; version < VERSIONS; version++) {
        if (!present(pattern >> version, 0)) {
          assertThat(pids.at(version, 0)).as("pattern %s, v%s", pattern, version).isNull();
          runStart = -1;
          continue;
        }
        runStart = runStart < 0 ? version : runStart;
        assertThat(pids.chainStart(version, 0))
            .as("pattern %s, v%s", pattern, version).isEqualTo(runStart);
        assertThat(pids.isNew(runStart, 0)).as("pattern %s, v%s", pattern, version).isTrue();
      }
    }
  }

  @Test
  void independentPresencePatternsDoNotPerturbEachOther() {
    // All 1024 interleavings of two fields. One field's churn must not shift the other's runs,
    // however the two patterns line up.
    for (int pattern = 0; pattern < 1024; pattern++) {
      Pids pids = avro(history(pattern, 2, mask -> {
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
          assertThat(pids.chainStart(version, position))
              .as("pattern %s, v%s, field %s", pattern, version, fieldIndex)
              .isEqualTo(runStart[fieldIndex]);
          position++;
        }
      }
    }
  }

  @Test
  void anAliasContinuesOnlyFromThePreviousVersion() {
    // a is renamed to b, which keeps declaring the alias. b continues a only while every version
    // since has it; after a gap it is new, and then continues itself.
    for (int pattern = 0; pattern < 16; pattern++) {
      List<LogicalType> history = new ArrayList<>();
      history.add(lt(struct(field("a"))));
      for (int version = 1; version < VERSIONS; version++) {
        history.add(lt(present(pattern >> (version - 1), 0)
            ? struct(field("b", "a")) : struct()));
      }
      Pids pids = avro(history.toArray(new LogicalType[0]));

      int runStart = 0;
      for (int version = 1; version < VERSIONS; version++) {
        if (!present(pattern >> (version - 1), 0)) {
          assertThat(pids.at(version, 0)).as("pattern %s, v%s", pattern, version).isNull();
          runStart = -1;
          continue;
        }
        runStart = runStart < 0 ? version : runStart;
        assertThat(pids.at(version, 0)).as("pattern %s, v%s", pattern, version)
            .isEqualTo(pids.at(runStart, 0));
        assertThat(pids.chainStart(version, 0))
            .as("pattern %s, v%s", pattern, version).isEqualTo(runStart);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Pairing
  // ---------------------------------------------------------------------------------------------

  @Test
  void aRenamedAndAnUnchangedFieldAreSharedAndAnAddedOneIsNot() {
    Pids pids = avro(
        lt(struct(field("name"), field("age"))),
        lt(struct(field("full_name", "name"), field("age"), field("address"))));

    assertThat(pids.shared(0, 1)).containsExactly(path(0), path(1));
    assertThat(pids.shared(1, 0)).containsExactly(path(0), path(1));
    assertThat(pids.isNew(1, 2)).isTrue();
  }

  @Test
  void pairingSurvivesReorderingAndSkipsAReusedName() {
    // b survives, moving from index 1 to index 0. a is dropped at v1 and a field called a is added
    // back at v2 -- a different field that happens to reuse the name. Matching on names would map
    // it onto the original; provenance leaves it out of the pairing entirely.
    Pids pids = avro(
        lt(struct(field("a"), field("b"))),
        lt(struct(field("b"))),
        lt(struct(field("b"), field("a"))));

    assertThat(pids.at(2, 0)).isEqualTo(pids.at(0, 1));
    assertThat(pids.shared(2, 0)).containsExactly(path(0));
  }

  @Test
  void pairingSurvivesAPartialGap() {
    Pids pids = avro(
        lt(struct(field("a"), field("b"), field("c"))),
        lt(struct(field("a"), field("c"))),
        lt(struct(field("a"), field("b"), field("c"))));

    assertThat(pids.shared(2, 0)).containsExactly(path(0), path(2));
    assertThat(pids.isNew(2, 1)).isTrue();
  }

  @Test
  void anEmptySequenceReportsNothing() {
    assertThat(ProvenanceComputer.report(new ArrayList<>(), IdentityPolicy.AVRO).getVersions())
        .isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static Pids avro(LogicalType... versions) {
    return Pids.of(Arrays.asList(versions), IdentityPolicy.AVRO);
  }

  private static Pids avro(List<LogicalType> versions) {
    return Pids.of(versions, IdentityPolicy.AVRO);
  }

  private static Pids protobuf(LogicalType... versions) {
    return Pids.of(Arrays.asList(versions), IdentityPolicy.PROTOBUF);
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
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
