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

  /**
   * The value {@link #positionMapping} uses for a column the source does not have.
   */
  public static final int ABSENT = -1;

  private final List<LogicalType> versions;
  private final List<Map<PathKey, Provenance>> byVersion;
  private final List<Set<Provenance>> memberSets;

  ProvenanceResult(List<LogicalType> versions, List<Map<PathKey, Provenance>> byVersion) {
    this.versions = Collections.unmodifiableList(new ArrayList<>(versions));
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
  // Inlined views, for consumers that have expanded every named type
  // -----------------------------------------------------------------------------------------

  /**
   * One version's members keyed by inlined index path instead of definition site — the addressing
   * a consumer uses when it has followed every {@code NAMED_TYPE_REF}, as Flink's {@code RowType}
   * does.
   *
   * <p>Named types themselves do not appear: they are definitions, not locations. A type shared by
   * two fields appears once per use site, so the same {@link Provenance} may be reached by more
   * than one path.
   *
   * @throws IllegalStateException if the schema is recursive, which has no finite inlining
   */
  public Map<List<Integer>, Provenance> expandedProvenance(int version) {
    return PathExpander.expand(versions.get(version), byVersion.get(version));
  }

  /**
   * {@link #correspondence} in inlined coordinates: where each shared member sits in
   * {@code target}, mapped to where it sits in {@code source}.
   *
   * <p>This cannot be derived by joining two {@link #expandedProvenance} maps, because inlining
   * puts a shared type's members at several paths and only the prefix says which use site is
   * meant. Both versions are walked in step instead. A member the source lacks ends that subtree,
   * as does a structural divergence between the two.
   *
   * @throws IllegalStateException if either schema is recursive
   */
  public Map<List<Integer>, List<Integer>> expandedCorrespondence(int target, int source) {
    return paired(target, source).paths();
  }

  /**
   * One version's declared default values, keyed by inlined index path.
   *
   * <p>{@code LogicalType.getDefaultValues()} keys a member of a named type by the path of that
   * type's <em>first</em> occurrence, because the readers convert a named type's body once. This
   * re-keys them onto every occurrence, which is what a consumer that inlined the type needs, and
   * is what supplies a value for a column the source does not have.
   *
   * <p>A declared {@code null} default is kept as a null value, so distinguishing it from "no
   * default" means using {@code containsKey}. For projection the two are equivalent: both mean the
   * column reads as null.
   *
   * <p>Verified against the Avro reader's convention. The Protobuf reader walks through
   * synthesized wrapper structs, so its default paths carry extra steps that this does not strip.
   *
   * @throws IllegalStateException if the schema is recursive
   */
  public Map<List<Integer>, Object> expandedDefaults(int version) {
    return PathExpander.expandDefaults(versions.get(version));
  }

  /**
   * Each container's correspondence as positions, keyed by the container's inlined path — the
   * empty path for the root row, {@code [1]} for a row at target position 1, {@code [0, 0]} for
   * the row inside a collection at position 0.
   *
   * <p>The form a projecting consumer builds its plan from. It makes no assumption about how a
   * consumer nests rows within collections: walk your own type and look up the path you are at. A
   * container with no entry is one the walk stopped at, because it is absent or its types
   * diverged — see {@link #absences}.
   *
   * @throws IllegalStateException if either schema is recursive
   */
  public Map<List<Integer>, PositionMapping> positionMappings(int target, int source) {
    return paired(target, source).containers();
  }

  /**
   * Why each unmatched member of {@code target} is unmatched, keyed by inlined path.
   *
   * <p>A member missing from {@link #expandedCorrespondence} says only that nothing feeds it.
   * This says which of three things happened, which is what an operator-facing message needs.
   *
   * @throws IllegalStateException if either schema is recursive
   */
  public Map<List<Integer>, Absence> absences(int target, int source) {
    return paired(target, source).absences();
  }

  /**
   * True when projecting {@code source} onto {@code target} would be a no-op: every member of
   * every container is fed by the member at the same position, nothing is absent, and no container
   * differs in arity. A consumer can then use the source's rows unchanged.
   *
   * @throws IllegalStateException if either schema is recursive
   */
  public boolean isIdentity(int target, int source) {
    PathExpander.Correspondence paired = paired(target, source);
    if (!paired.absences().isEmpty()) {
      return false;
    }
    for (Map.Entry<List<Integer>, List<Integer>> entry : paired.paths().entrySet()) {
      if (!entry.getKey().equals(entry.getValue())) {
        return false;
      }
    }
    for (PositionMapping mapping : paired.containers().values()) {
      if (!mapping.isIdentity()) {
        return false;
      }
    }
    return true;
  }

  private PathExpander.Correspondence paired(int target, int source) {
    return PathExpander.correspond(
        versions.get(target), versions.get(source), correspondence(target, source));
  }

  @Override
  public String toString() {
    return "ProvenanceResult(" + byVersion.size() + " versions of "
        + LogicalType.class.getSimpleName() + ")";
  }
}
