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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The provenance of every entity in every version of a computed sequence, plus the set algebra
 * over those values.
 *
 * <p>Versions are addressed by their index in the sequence handed to
 * {@link ProvenanceComputer#compute}, so index {@code 0} is the first version supplied — which is
 * the first version of the whole history only when the caller anchored there. See
 * {@link Provenance} on relative versus absolute values.
 *
 * <p>The set operations run over member entities only ({@link EntityKind#isMember()}). A named
 * type's provenance establishes its members' scope and tracks the container's own lifetime, but it
 * is not part of the physical field intersection used for data mapping — a record and a field are
 * different kinds of thing and intersecting them together would be meaningless.
 */
public final class ProvenanceResult {

  private final List<LogicalType> versions;
  private final List<IdentityPolicy> policies;
  private final List<Map<PathKey, Provenance>> byVersion;
  private final List<Set<Provenance>> memberSets;

  ProvenanceResult(List<LogicalType> versions, List<IdentityPolicy> policies,
      List<Map<PathKey, Provenance>> byVersion) {
    this.versions = Collections.unmodifiableList(new ArrayList<>(versions));
    this.policies = Collections.unmodifiableList(new ArrayList<>(policies));
    List<Map<PathKey, Provenance>> paths = new ArrayList<>(byVersion.size());
    List<Set<Provenance>> members = new ArrayList<>(byVersion.size());
    for (Map<PathKey, Provenance> version : byVersion) {
      paths.add(Collections.unmodifiableMap(version));
      Set<Provenance> memberSet = new LinkedHashSet<>();
      for (Provenance p : version.values()) {
        if (p.getKind().isMember()) {
          memberSet.add(p);
        }
      }
      members.add(Collections.unmodifiableSet(memberSet));
    }
    this.byVersion = Collections.unmodifiableList(paths);
    this.memberSets = Collections.unmodifiableList(members);
  }

  /** How many versions were computed. */
  public int versionCount() {
    return byVersion.size();
  }

  /**
   * The schema this provenance was computed from, as supplied. Retained because every consumer
   * needs it back — to walk structure, to read {@code getDefaultValues()} for a column the source
   * lacks, or to size a row.
   */
  public LogicalType version(int version) {
    return versions.get(version);
  }

  /**
   * The schemas this provenance was computed from, in order.
   */
  public List<LogicalType> versions() {
    return versions;
  }

  /**
   * Every entity of one version, keyed by where it was found. Includes named types; filter on
   * {@link Provenance#getKind()} to drop them.
   */
  public Map<PathKey, Provenance> byPath(int version) {
    return byVersion.get(version);
  }

  /**
   * The provenance of the entity at {@code path} in {@code version}, or {@code null}.
   */
  public Provenance at(int version, PathKey path) {
    return byVersion.get(version).get(path);
  }

  /** The member (field and branch) provenance values of one version. */
  public Set<Provenance> memberProvenance(int version) {
    return memberSets.get(version);
  }

  /**
   * The logical members present in both versions: {@code P(a) ∩ P(b)}. Symmetric, and the basis
   * for projection and read/write mapping.
   */
  public Set<Provenance> intersection(int a, int b) {
    Set<Provenance> result = new LinkedHashSet<>(memberSets.get(a));
    result.retainAll(memberSets.get(b));
    return Collections.unmodifiableSet(result);
  }

  /**
   * {@code P(a) ∪ P(b)} — the basis for schema merging or comprehensive code bindings.
   */
  public Set<Provenance> union(int a, int b) {
    Set<Provenance> result = new LinkedHashSet<>(memberSets.get(a));
    result.addAll(memberSets.get(b));
    return Collections.unmodifiableSet(result);
  }

  /**
   * {@code P(a) - P(b)} — the basis for diffing, linting and compatibility audits.
   */
  public Set<Provenance> difference(int a, int b) {
    Set<Provenance> result = new LinkedHashSet<>(memberSets.get(a));
    result.removeAll(memberSets.get(b));
    return Collections.unmodifiableSet(result);
  }

  /**
   * The field correspondence between two versions: where each shared member sits in {@code a}
   * mapped to where the same member sits in {@code b}.
   *
   * <p>This is {@link #intersection} written out on both sides, and it is what a compute engine
   * needs to project a record read under version {@code a} onto version {@code b} — without ever
   * matching on names, and without coordinating with an out-of-band column ID system. A member
   * renamed between the two versions appears here as a pair of differing paths; a name reused by an
   * unrelated member does not appear at all.
   *
   * <p>Entries are ordered by {@code a}'s walk order. A version cannot contain two members with the
   * same provenance, so the mapping is one-to-one.
   */
  public Map<PathKey, PathKey> correspondence(int a, int b) {
    Map<Provenance, PathKey> inB = new LinkedHashMap<>();
    for (Map.Entry<PathKey, Provenance> e : byVersion.get(b).entrySet()) {
      if (e.getValue().getKind().isMember()) {
        inB.put(e.getValue(), e.getKey());
      }
    }
    Map<PathKey, PathKey> result = new LinkedHashMap<>();
    for (Map.Entry<PathKey, Provenance> e : byVersion.get(a).entrySet()) {
      if (!e.getValue().getKind().isMember()) {
        continue;
      }
      PathKey counterpart = inB.get(e.getValue());
      if (counterpart != null) {
        result.put(e.getKey(), counterpart);
      }
    }
    return Collections.unmodifiableMap(result);
  }

  // -----------------------------------------------------------------------------------------
  // Inlined view, for consumers that have inlined every named type
  // -----------------------------------------------------------------------------------------

  /**
   * One version's members keyed by inlined index path, each valued by the chain of provenances
   * that locates it — references followed, so a type shared by two fields appears once per use
   * site with a distinct chain.
   *
   * <p>This is the view a consumer needs when its own model has no shared types, such as deriving
   * Iceberg column ids or matching two versions by location: every physical location needs its own
   * identifier, where {@link #byPath} reports one entry for the shared definition.
   *
   * @throws IllegalStateException if the schema is recursive, which has no finite inlining
   */
  public List<InlinedMember> inlinedProvenance(int version) {
    return PathInliner.inline(versions.get(version), byVersion.get(version),
        ProvenanceComputer.seesThroughNamedTypes(policies.get(version)));
  }

  /**
   * Every version's members with an id allocated per location — what a provenance endpoint serves.
   *
   * <p>Ids are allocated by walking versions in the order supplied and, within a version, members
   * in path order, taking the next integer the first time a location is seen. A rename keeps its
   * id, a drop retires it, and a column re-added under an old name takes a fresh one.
   *
   * @throws IllegalStateException if a schema is recursive, or if an id would appear twice within
   *     one version — which can only happen if allocation stopped being per location
   */
  public ProvenanceReport report() {
    Map<LocatedProvenance, Integer> idByLocation = new LinkedHashMap<>();
    List<ProvenanceReport.Version> reported = new ArrayList<>(versionCount());
    int nextId = 1;

    for (int version = 0; version < versionCount(); version++) {
      List<ProvenanceReport.Member> members = new ArrayList<>();
      Set<Integer> seenThisVersion = new HashSet<>();
      for (InlinedMember member : inlinedProvenance(version)) {
        Integer id = idByLocation.get(member.getLocation());
        if (id == null) {
          id = nextId++;
          idByLocation.put(member.getLocation(), id);
        }
        if (!seenThisVersion.add(id)) {
          throw new IllegalStateException("Provenance id " + id + " appears twice in version "
              + version + ", at " + member.getPath() + ". An id must identify a location, not a "
              + "logical entity, or a consumer joining on it cannot tell two uses of one shared "
              + "named type apart.");
        }
        members.add(new ProvenanceReport.Member(
            member.getPath(), member.getNames(), id, member.getDefaultValue()));
      }
      reported.add(new ProvenanceReport.Version(version, members));
    }
    return new ProvenanceReport(reported, nextId - 1);
  }

  @Override
  public String toString() {
    return "ProvenanceResult(" + byVersion.size() + " versions of "
        + LogicalType.class.getSimpleName() + ")";
  }
}
