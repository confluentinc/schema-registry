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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Assigns a {@link Provenance} to every field, branch and named type across a sequence of
 * {@link LogicalType} versions, so that two versions can be put in correspondence without matching
 * on names and without an out-of-band column ID system.
 *
 * <p>The sequence is the history. Because a presence interval can only be observed by seeing a
 * version in which an entity is absent, the values computed are exactly as good as the history
 * supplied: anchor at the subject's first version for absolute provenance, or hand over just the
 * window between two versions for relative provenance, which is sufficient for the pairwise
 * correspondence a compute engine needs. Values from different windows are not comparable.
 *
 * <h2>Identity</h2>
 *
 * <p>Identity rules are format-specific and a {@code LogicalType} carries no format discriminator,
 * so {@link IdentityPolicy} stands in for one. Entities are keyed at their definition site: a named
 * type's members are walked once, under that type, rather than re-walked at every reference. That
 * is what terminates the walk on a recursive type and keeps a type shared by two fields a single
 * entity. See {@link PathKey}.
 *
 * <p>A member's scope is its container's identity, so the shape of the root matters: the members of
 * an anonymous root schema sit in a different scope from the members of a named type. A version
 * that gives a previously anonymous root a name, or the reverse, therefore starts every member's
 * interval afresh — which is correct, but worth knowing before mixing LTs built by a shim (which
 * inlines everything) with LTs read from Avro or Protobuf (whose roots are named).
 *
 * <h2>Per-version transition</h2>
 *
 * <p>Each version is an atomic transition in three phases, so the result cannot depend on the order
 * entities happen to be walked in:
 *
 * <ol>
 *   <li><b>Resolve</b> — walk the version top-down, one peer group at a time, resolving every
 *       entity's identity against the state left by the previous version. Nothing is mutated, so
 *       every entity sees the same snapshot, including the parents whose identities form its
 *       scope.</li>
 *   <li><b>Validate</b> — reject a version that repeats a path, claims one name for two identities,
 *       or takes a name still held by a live entity.</li>
 *   <li><b>Commit</b> — start, continue or restart each entity's presence interval, update the name
 *       resolution index, and mark everything not present in this version inactive.</li>
 * </ol>
 *
 * <h2>Released names</h2>
 *
 * <p>Resolution reads an index built by the previous version, so before resolving a peer group it
 * has to know which of those historical names this version has let go of. Without that, a name
 * dropped in the very version another entity picks it up would still resolve to its former holder,
 * and the two would be silently merged.
 *
 * <p>The pre-pass answers it by first deciding, for each historical identity in the scope, whether
 * this version continues it — through an explicit alias, which wins, or through the canonical name
 * it was last committed under. A name is released when its identity has no continuation and was
 * active, or when the continuation no longer declares it. Comparing against the last committed
 * canonical name is what separates a genuine continuation from an entity that merely happens to
 * reuse one of the old entity's former names.
 *
 * <h2>Invariant</h2>
 *
 * <p>A logical entity is assigned exactly one provenance value for each continuous presence
 * interval. Continuously present occurrences of one entity share a provenance (so correspondence
 * survives a rename); occurrences separated by an absence never do (so a name reused later cannot
 * be mistaken for the entity that used to hold it), whether the reappearance mints a fresh identity
 * or an alias reconnects it to the historical one.
 */
public final class ProvenanceComputer {

  private ProvenanceComputer() {
  }

  /**
   * Computes provenance under {@link IdentityPolicy#AUTO}.
   */
  public static ProvenanceResult compute(List<LogicalType> versions) {
    return compute(versions, IdentityPolicy.AUTO);
  }

  /** Computes provenance, applying one identity policy to every version. */
  public static ProvenanceResult compute(List<LogicalType> versions, IdentityPolicy policy) {
    Objects.requireNonNull(versions, "versions");
    Objects.requireNonNull(policy, "policy");
    return compute(versions, Collections.nCopies(versions.size(), policy));
  }

  /**
   * Computes provenance, letting each version declare its own identity policy.
   *
   * <p>Use this for a sequence that changes format part-way: the signal that establishes identity
   * differs per format, and a single policy across the switch either reads Avro names positionally
   * or ignores Protobuf numbers. Note that an entity whose identity signal changes between versions
   * takes a new identity, and therefore a new presence interval, whatever the policies say — to
   * carry correspondence across a format migration, resolve both sides by name.
   *
   * @param versions the schema versions in chronological order
   * @param policies one policy per version, in the same order
   * @throws IllegalArgumentException if the lists differ in size, or a version is null
   * @throws IllegalStateException if a version is internally inconsistent — a repeated path, two
   *     entities resolving to one identity, one name claimed for two identities, an ambiguous
   *     alias, or a name taken from a live holder
   */
  public static ProvenanceResult compute(
      List<LogicalType> versions, List<IdentityPolicy> policies) {
    Objects.requireNonNull(versions, "versions");
    Objects.requireNonNull(policies, "policies");
    if (versions.size() != policies.size()) {
      throw new IllegalArgumentException("Expected one policy per version, got "
          + policies.size() + " policies for " + versions.size() + " versions");
    }

    History history = new History();
    List<Map<PathKey, Provenance>> byVersion = new ArrayList<>(versions.size());
    for (int version = 0; version < versions.size(); version++) {
      LogicalType logicalType = versions.get(version);
      if (logicalType == null) {
        throw new IllegalArgumentException("Null LogicalType at version " + version);
      }
      IdentityPolicy policy = policies.get(version);
      if (policy == null) {
        throw new IllegalArgumentException("Null IdentityPolicy at version " + version);
      }

      Resolver resolver = new Resolver(version, policy, history);
      resolver.resolve(logicalType);
      validate(resolver, history, version);
      byVersion.add(commit(resolver.entities, version, history));
    }
    return new ProvenanceResult(versions, byVersion);
  }

  /**
   * Computes provenance and packages it as a {@link ProvenanceReport} — every version's members
   * with an id allocated per location. The form a provenance endpoint serves.
   */
  public static ProvenanceReport report(List<LogicalType> versions, IdentityPolicy policy) {
    return compute(versions, policy).report();
  }

  // -----------------------------------------------------------------------------------------
  // Phase 2 -- Validate
  // -----------------------------------------------------------------------------------------

  /**
   * Rejects a version whose identity claims are self-contradictory. Everything here runs before
   * anything is committed: the name checks because commit overwrites index mappings and would
   * otherwise silently pick a winner, the path check because two entities sharing a key would make
   * the version's own output ambiguous.
   *
   * <p>A name the pre-pass released is exempt from the live-holder check. Released means its holder
   * either left this version or stopped declaring it, so taking it over is exactly what the
   * algorithm intends — the holder's {@code active} flag has simply not been cleared yet, since
   * that happens at the end of commit.
   */
  private static void validate(Resolver resolver, History history, int version) {
    Set<PathKey> claimedPaths = new HashSet<>();
    Map<NameKey, Identity> claimedNames = new HashMap<>();

    for (Entity entity : resolver.entities) {
      if (!claimedPaths.add(entity.path)) {
        throw new IllegalStateException(
            "Duplicate entity path detected in schema version " + version + ": " + entity.path);
      }
      for (NameKey name : entity.declaredNames) {
        Identity claimant = claimedNames.get(name);
        if (claimant != null && !claimant.equals(entity.identity)) {
          throw new IllegalStateException("Conflicting mapping detected within schema at version "
              + version + " for: " + name);
        }
        claimedNames.put(name, entity.identity);

        if (resolver.released.contains(name)) {
          continue;
        }
        Identity indexed = history.identityIndex.get(name);
        if (indexed != null && !indexed.equals(entity.identity)) {
          EntityState indexedState = history.state.get(indexed);
          if (indexedState != null && indexedState.active) {
            throw new IllegalStateException("Cannot overwrite active identity mapping at version "
                + version + " for name: " + name);
          }
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------
  // Phase 3 -- Commit
  // -----------------------------------------------------------------------------------------

  private static Map<PathKey, Provenance> commit(
      List<Entity> entities, int version, History history) {
    Map<PathKey, Provenance> provenance = new LinkedHashMap<>();
    Set<Identity> present = new HashSet<>();

    for (Entity entity : entities) {
      present.add(entity.identity);
      EntityState entityState = history.state.get(entity.identity);
      if (entityState == null) {
        entityState = new EntityState(version);
        history.state.put(entity.identity, entityState);
        history.identitiesByScope
            .computeIfAbsent(entity.identity.getScope(), k -> new LinkedHashSet<>())
            .add(entity.identity);
      } else if (!entityState.active) {
        // Identity continuity without provenance continuity: this occurrence is the same logical
        // entity, but its previous interval was broken, so a new one starts here.
        entityState.presenceStartVersion = version;
      }
      entityState.active = true;
      if (!entity.declaredNames.isEmpty()) {
        entityState.canonicalName =
            new NameKey(entity.identity.getKind(), entity.scope, entity.name);
      }
      syncNames(entity, history.identityIndex, entityState);
      provenance.put(entity.path,
          new Provenance(entity.identity, entityState.presenceStartVersion));
    }

    // Everything absent from this version ends its interval here. A later reappearance is then
    // forced to start a new one, whether it mints a fresh identity or an alias reconnects it.
    for (Map.Entry<Identity, EntityState> entry : history.state.entrySet()) {
      if (entry.getValue().active && !present.contains(entry.getKey())) {
        entry.getValue().active = false;
      }
    }
    return provenance;
  }

  /**
   * Updates the long-lived name resolution index for one present entity: prune the names it has
   * stopped declaring, then register the ones it declares now.
   *
   * <p>An entity that disappears prunes nothing — this does not run for absent entities — so its
   * names stay dormant and an alias can still reconnect to it, but commit has already marked it
   * inactive, which forces a new interval either way.
   *
   * <p>A prune only removes a mapping this entity still owns. An entity can declare a name, vanish
   * for several versions while another entity takes that name over, and then return through an
   * alias — at which point its last declarations are stale, and pruning one blindly would delete
   * the live holder's mapping and silently reset an entity that never went anywhere.
   */
  private static void syncNames(
      Entity entity, Map<NameKey, Identity> identityIndex, EntityState entityState) {
    for (NameKey declared : entityState.lastDeclaredNames) {
      if (!entity.declaredNames.contains(declared)
          && entity.identity.equals(identityIndex.get(declared))) {
        identityIndex.remove(declared);
      }
    }
    for (NameKey name : entity.declaredNames) {
      identityIndex.put(name, entity.identity);
    }
    entityState.lastDeclaredNames = entity.declaredNames;
  }

  // -----------------------------------------------------------------------------------------
  // Internal state
  // -----------------------------------------------------------------------------------------

  /** Everything carried from one version to the next. */
  private static final class History {

    /** Identity -> presence, interval start, and the names it last declared. */
    private final Map<Identity, EntityState> state = new HashMap<>();

    /**
     * Name -> identity. Long-lived: a mapping stays dormant after its entity disappears, and is
     * pruned only when a still-present entity stops declaring the name.
     */
    private final Map<NameKey, Identity> identityIndex = new HashMap<>();

    /** Identities grouped by scope, so the pre-pass need not scan the whole history. */
    private final Map<Scope, Set<Identity>> identitiesByScope = new HashMap<>();
  }

  /** What the algorithm remembers about one logical identity between versions. */
  private static final class EntityState {

    private boolean active = true;
    private int presenceStartVersion;

    /**
     * The names this identity declared during its most recent active interval. Retained while it is
     * dormant so an explicit alias can reconnect to it.
     */
    private Set<NameKey> lastDeclaredNames = Collections.emptySet();

    /**
     * The canonical name this identity was last committed under. The pre-pass relies on it to tell
     * a genuine canonical continuation from an entity coincidentally reusing a former alias.
     */
    private NameKey canonicalName;

    EntityState(int presenceStartVersion) {
      this.presenceStartVersion = presenceStartVersion;
    }
  }

  /** A name as claimed by one kind of entity within a scope — the key of the resolution index. */
  private static final class NameKey {

    private final EntityKind kind;
    private final Scope scope;
    private final String name;
    private final int hash;

    NameKey(EntityKind kind, Scope scope, String name) {
      this.kind = kind;
      this.scope = scope;
      this.name = name;
      this.hash = Objects.hash(kind, scope, name);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof NameKey)) {
        return false;
      }
      NameKey that = (NameKey) o;
      return hash == that.hash && kind == that.kind
          && Objects.equals(name, that.name) && scope.equals(that.scope);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      return scope + "." + name + " (" + kind + ")";
    }
  }

  /** One resolved occurrence: what it is, where it was found, and the names it declares. */
  private static final class Entity {

    private final Identity identity;
    private final Scope scope;
    private final String name;
    private final PathKey path;
    private final Set<NameKey> declaredNames;

    Entity(Identity identity, Scope scope, String name, PathKey path,
        Set<NameKey> declaredNames) {
      this.identity = identity;
      this.scope = scope;
      this.name = name;
      this.path = path;
      this.declaredNames = declaredNames;
    }
  }

  /**
   * One member of a peer group before its identity is resolved: a named type definition, a struct
   * field, or a union branch.
   */
  private static final class Candidate {

    private final EntityKind kind;
    private final String name;
    private final List<String> aliases;
    private final Integer number;
    private final PathKey path;
    private final Schema body;

    /** Derived Protobuf numbers to hand to this candidate's own children, if any. */
    private final Map<Object, Integer> childDerived;

    Candidate(EntityKind kind, String name, List<String> aliases, Integer number,
        PathKey path, Schema body, Map<Object, Integer> childDerived) {
      this.kind = kind;
      this.name = name;
      this.aliases = aliases != null ? aliases : Collections.emptyList();
      this.number = number;
      this.path = path;
      this.body = body;
      this.childDerived = childDerived;
    }
  }

  // -----------------------------------------------------------------------------------------
  // Phase 1 -- Resolve
  // -----------------------------------------------------------------------------------------

  /**
   * Walks one version top-down and resolves every entity's identity. Reads the {@link History} and
   * never writes it, so the whole version resolves against the snapshot the previous version left
   * — including each parent identity, which is resolved before its members so that it can form
   * their scope.
   */
  private static final class Resolver {

    private final int version;
    private final IdentityPolicy policy;
    private final History history;

    private final List<Entity> entities = new ArrayList<>();
    private final Set<Identity> seen = new HashSet<>();

    /** Every name released in this version, across all scopes. Scope-qualified, so one set does. */
    private final Set<NameKey> released = new HashSet<>();

    Resolver(int version, IdentityPolicy policy, History history) {
      this.version = version;
      this.policy = policy;
      this.history = history;
    }

    void resolve(LogicalType logicalType) {
      // The root scope holds the named type definitions and, when the root schema is not just a
      // reference, its own members. Kind keeps the two apart.
      List<Candidate> rootPeers = new ArrayList<>();
      for (Map.Entry<String, Schema> entry
          : new TreeMap<>(logicalType.getNamedTypes()).entrySet()) {
        rootPeers.add(new Candidate(EntityKind.NAMED_TYPE, entry.getKey(),
            entry.getValue() != null ? entry.getValue().getAliases() : null, null,
            PathKey.ofNamedType(entry.getKey()), entry.getValue(), Collections.emptyMap()));
      }

      Schema root = logicalType.getRootSchema();
      boolean rootHasMembers = root != null
          && (root.getType() == Schema.Type.STRUCT || root.getType() == Schema.Type.UNION);
      if (rootHasMembers) {
        rootPeers.addAll(memberCandidates(root, PathKey.ofRoot(), Collections.emptyMap()));
      }

      processGroup(rootPeers, RootScope.INSTANCE);

      if (root != null && !rootHasMembers && root.getType() != Schema.Type.NAMED_TYPE_REF) {
        // A collection or primitive at the root: its members live in a stepped scope of their own.
        processType(root, RootScope.INSTANCE, PathKey.ofRoot(), Collections.emptyMap());
      }
    }

    /** Resolves one peer group, then descends into each peer's own type. */
    private void processGroup(List<Candidate> peers, Scope scope) {
      if (peers.isEmpty()) {
        return;
      }
      Set<NameKey> releasedHere = computeReleasedNames(peers, scope);
      released.addAll(releasedHere);

      for (Candidate peer : peers) {
        Identity identity = resolveIdentity(peer, scope, releasedHere);
        if (!seen.add(identity)) {
          throw new IllegalStateException(
              "Multiple entities resolve to the same logical identity at version " + version
                  + ": " + identity + " (at " + peer.path + ")");
        }
        entities.add(new Entity(
            identity, scope, peer.name, peer.path, declaredNames(peer, scope)));
        processType(peer.body, identity, peer.path, peer.childDerived);
      }
    }

    /**
     * Walks a type down to the next peer group. A {@code NAMED_TYPE_REF} is a leaf here: its body
     * is walked at its own definition, which keeps a recursive type finite and a shared type
     * single.
     */
    private void processType(
        Schema schema, Scope scope, PathKey path, Map<Object, Integer> derived) {
      if (schema == null) {
        return;
      }
      switch (schema.getType()) {
        case STRUCT:
          processGroup(memberCandidates(schema, path, Collections.emptyMap()), scope);
          break;
        case UNION:
          // Branch numbers, when derived, come from the enclosing struct: a oneof's branches
          // continue that message's numbering.
          processGroup(memberCandidates(schema, path, derived), scope);
          break;
        case ARRAY:
        case MULTISET:
          processType(schema.getElementType(),
              StepScope.step(scope, "[]"), path.child(0), derived);
          break;
        case MAP:
          processType(schema.getKeyType(),
              StepScope.step(scope, "{key}"), path.child(0), derived);
          processType(schema.getValueType(),
              StepScope.step(scope, "{value}"), path.child(1), derived);
          break;
        default:
          // Primitives, enums and named type references have no members.
          break;
      }
    }

    /**
     * The members of a STRUCT or UNION, as candidates carrying their effective numbers. A struct
     * derives its own numbering; a union's branches continue the enclosing struct's, since that is
     * how a oneof is numbered.
     */
    private List<Candidate> memberCandidates(
        Schema container, PathKey parentPath, Map<Object, Integer> enclosingDerived) {
      List<Candidate> candidates = new ArrayList<>();
      if (container.getType() == Schema.Type.STRUCT) {
        Map<Object, Integer> derived = deriveNumbers(container);
        List<Field> fields = container.getFields();
        for (int i = 0; i < fields.size(); i++) {
          Field field = fields.get(i);
          candidates.add(new Candidate(EntityKind.FIELD, field.getName(), field.getAliases(),
              numberOf(field.getFieldNumber(), field, derived), parentPath.child(i),
              field.getSchema(), derived));
        }
      } else {
        List<UnionBranch> branches = container.getBranches();
        for (int i = 0; i < branches.size(); i++) {
          UnionBranch branch = branches.get(i);
          candidates.add(new Candidate(EntityKind.BRANCH, branch.getName(), null,
              numberOf(branch.getFieldNumber(), branch, enclosingDerived), parentPath.child(i),
              branch.getSchema(), enclosingDerived));
        }
      }
      return candidates;
    }

    // -------------------------------------------------------------------------------------
    // The released-names pre-pass
    // -------------------------------------------------------------------------------------

    /**
     * The historical names this version lets go of in {@code scope}, which resolution must not
     * resolve through. See the class javadoc for why this has to happen before any peer in the
     * group is resolved.
     */
    private Set<NameKey> computeReleasedNames(List<Candidate> peers, Scope scope) {
      Map<Identity, Set<Candidate>> byCanonical = new LinkedHashMap<>();
      Map<Identity, Set<Candidate>> byAlias = new LinkedHashMap<>();
      collectContinuationCandidates(peers, scope, byCanonical, byAlias);
      Map<Identity, Candidate> continuations = arbitrate(byCanonical, byAlias);

      Set<NameKey> releasedHere = new LinkedHashSet<>();
      for (Identity historical
          : history.identitiesByScope.getOrDefault(scope, Collections.emptySet())) {
        EntityState historicalState = history.state.get(historical);
        if (historicalState == null || historicalState.lastDeclaredNames.isEmpty()) {
          continue;
        }
        Candidate continuation = continuations.get(historical);
        // Previously active and not continued: it releases everything it still owns. Dormant and
        // not continued: it releases nothing, so an alias can still reconnect to it.
        Set<NameKey> dropped;
        if (continuation == null) {
          dropped = historicalState.active
              ? historicalState.lastDeclaredNames : Collections.emptySet();
        } else {
          dropped = new LinkedHashSet<>(historicalState.lastDeclaredNames);
          dropped.removeAll(declaredNames(continuation, scope));
        }
        for (NameKey previous : dropped) {
          // Only a name still mapped to this identity is this identity's to release. Its last
          // declarations go stale while it is dormant: another entity may have taken a name over
          // in the meantime, and releasing that one would reset a live entity that never moved.
          if (historical.equals(history.identityIndex.get(previous))) {
            releasedHere.add(previous);
          }
        }
      }
      return releasedHere;
    }

    private void collectContinuationCandidates(List<Candidate> peers, Scope scope,
        Map<Identity, Set<Candidate>> byCanonical, Map<Identity, Set<Candidate>> byAlias) {
      for (Candidate peer : peers) {
        if (!isNameResolved(peer)) {
          continue;
        }
        NameKey canonicalKey = new NameKey(peer.kind, scope, peer.name);
        Identity canonical = history.identityIndex.get(canonicalKey);
        if (canonical != null) {
          EntityState canonicalState = history.state.get(canonical);
          // Only a name the identity was last committed under is a canonical continuation. A name
          // it merely used to declare as an alias is a coincidental reuse.
          if (canonicalState != null && canonicalState.active
              && canonicalKey.equals(canonicalState.canonicalName)) {
            byCanonical.computeIfAbsent(canonical, k -> new LinkedHashSet<>()).add(peer);
          }
        }
        for (String alias : peer.aliases) {
          Identity aliased = history.identityIndex.get(new NameKey(peer.kind, scope, alias));
          if (aliased != null) {
            byAlias.computeIfAbsent(aliased, k -> new LinkedHashSet<>()).add(peer);
          }
        }
      }
    }

    /**
     * Picks each historical identity's continuation. An explicit alias is a deliberate statement
     * about lineage, so it wins; a canonical match counts only when no alias claims the identity.
     */
    private Map<Identity, Candidate> arbitrate(
        Map<Identity, Set<Candidate>> byCanonical, Map<Identity, Set<Candidate>> byAlias) {
      Set<Identity> claimed = new LinkedHashSet<>(byCanonical.keySet());
      claimed.addAll(byAlias.keySet());

      Map<Identity, Candidate> continuations = new LinkedHashMap<>();
      for (Identity historical : claimed) {
        Set<Candidate> aliasClaims =
            byAlias.getOrDefault(historical, Collections.emptySet());
        Set<Candidate> canonicalClaims =
            byCanonical.getOrDefault(historical, Collections.emptySet());
        Set<Candidate> winning = !aliasClaims.isEmpty() ? aliasClaims : canonicalClaims;
        if (winning.size() > 1) {
          throw new IllegalStateException("Ambiguous identity resolution at version " + version
              + ": multiple entities claim historical identity " + historical
              + (!aliasClaims.isEmpty() ? " via aliases" : " canonically"));
        }
        if (winning.size() == 1) {
          continuations.put(historical, winning.iterator().next());
        }
      }
      return continuations;
    }

    // -------------------------------------------------------------------------------------
    // Identity resolution
    // -------------------------------------------------------------------------------------

    private Identity resolveIdentity(Candidate peer, Scope scope, Set<NameKey> releasedHere) {
      if (peer.name == null) {
        throw new IllegalArgumentException(
            "Entity at " + peer.path + " has no name (version " + version + ")");
      }
      switch (policy) {
        case JSON:
          return new Identity(peer.kind, scope, new StringIdentity(peer.name));
        case PROTOBUF:
          // A message has no alias mechanism, so it follows its name; a oneof container field has
          // no number in either direction and follows its name too.
          return peer.number != null
              ? new Identity(peer.kind, scope, new IntegerIdentity(peer.number))
              : new Identity(peer.kind, scope, new StringIdentity(peer.name));
        case AUTO:
          if (peer.number != null) {
            return new Identity(peer.kind, scope, new IntegerIdentity(peer.number));
          }
          break;
        case AVRO:
        default:
          break;
      }
      return resolveByName(peer, scope, releasedHere);
    }

    /** Avro rules: the canonical name while it is still live, or any explicit alias. */
    private Identity resolveByName(Candidate peer, Scope scope, Set<NameKey> releasedHere) {
      validateAliases(peer);
      Set<Identity> matches = new LinkedHashSet<>();

      NameKey canonicalKey = new NameKey(peer.kind, scope, peer.name);
      if (!releasedHere.contains(canonicalKey)) {
        Identity indexed = history.identityIndex.get(canonicalKey);
        if (indexed != null) {
          EntityState indexedState = history.state.get(indexed);
          if (indexedState != null && indexedState.active) {
            matches.add(indexed);
          }
        }
      }
      // An explicit alias may reconnect to a historical identity even when it is dormant. That is
      // identity continuity only -- commit still restarts the interval.
      for (String alias : peer.aliases) {
        NameKey aliasKey = new NameKey(peer.kind, scope, alias);
        if (!releasedHere.contains(aliasKey)) {
          Identity aliased = history.identityIndex.get(aliasKey);
          if (aliased != null) {
            matches.add(aliased);
          }
        }
      }

      if (matches.size() > 1) {
        throw new IllegalStateException("Ambiguous identity resolution at version " + version
            + ": " + peer.path + " matches multiple historical identities " + matches);
      }
      if (matches.size() == 1) {
        return matches.iterator().next();
      }
      // Brand new, or a historical identity resetting. Folding the minting version into the value
      // keeps it distinct from a live entity that previously released this name.
      return new Identity(peer.kind, scope, new MintedIdentity(peer.name, version));
    }

    private void validateAliases(Candidate peer) {
      if (peer.aliases.isEmpty()) {
        return;
      }
      Set<String> seenAliases = new HashSet<>();
      for (String alias : peer.aliases) {
        if (alias == null) {
          throw new IllegalArgumentException("Null alias at " + peer.path);
        }
        if (!seenAliases.add(alias)) {
          throw new IllegalStateException("Duplicate alias '" + alias + "' at " + peer.path);
        }
        if (alias.equals(peer.name)) {
          throw new IllegalStateException(
              "Alias '" + alias + "' duplicates the canonical name at " + peer.path);
        }
      }
    }

    /**
     * The names an entity declares: its own, plus its aliases. Empty unless the entity resolves by
     * name — a number or an aliasless name needs no index, and letting such an entity into it
     * would let an unrelated name-resolved entity resolve through it.
     */
    private Set<NameKey> declaredNames(Candidate peer, Scope scope) {
      if (!isNameResolved(peer)) {
        return Collections.emptySet();
      }
      Set<NameKey> names = new LinkedHashSet<>();
      names.add(new NameKey(peer.kind, scope, peer.name));
      for (String alias : peer.aliases) {
        names.add(new NameKey(peer.kind, scope, alias));
      }
      return names;
    }

    /** True when this entity's identity is resolved through the name index. */
    private boolean isNameResolved(Candidate peer) {
      switch (policy) {
        case AVRO:
          return true;
        case AUTO:
          return peer.number == null;
        default:
          return false;
      }
    }

    // -------------------------------------------------------------------------------------
    // Protobuf number derivation
    // -------------------------------------------------------------------------------------

    private Integer numberOf(Integer recorded, Object member, Map<Object, Integer> derived) {
      if (policy == IdentityPolicy.AVRO || policy == IdentityPolicy.JSON) {
        return null;
      }
      if (recorded != null) {
        return recorded;
      }
      return derived.get(member);
    }

    /**
     * Reconstructs the field numbers of a struct that records none.
     *
     * <p>The Protobuf reader omits numbers all-or-nothing, and precisely when the numbering was the
     * sequence the writer reproduces positionally: regular fields taking 1..n in declaration order,
     * then the oneof branches continuing it. Omission is therefore itself the information, and this
     * mirrors that rule exactly rather than guessing. See {@link IdentityPolicy#PROTOBUF} for why
     * it is gated on knowing the source format.
     */
    private Map<Object, Integer> deriveNumbers(Schema struct) {
      if (policy != IdentityPolicy.PROTOBUF || recordsAnyNumber(struct)) {
        return Collections.emptyMap();
      }
      Map<Object, Integer> derived = new IdentityHashMap<>();
      int number = 1;
      for (Field field : struct.getFields()) {
        if (!isUnion(field.getSchema())) {
          derived.put(field, number++);
        }
      }
      for (Field field : struct.getFields()) {
        if (isUnion(field.getSchema())) {
          for (UnionBranch branch : field.getSchema().getBranches()) {
            derived.put(branch, number++);
          }
        }
      }
      return derived;
    }

    private static boolean recordsAnyNumber(Schema struct) {
      for (Field field : struct.getFields()) {
        if (field.getFieldNumber() != null) {
          return true;
        }
        if (isUnion(field.getSchema())) {
          for (UnionBranch branch : field.getSchema().getBranches()) {
            if (branch.getFieldNumber() != null) {
              return true;
            }
          }
        }
      }
      return false;
    }

    private static boolean isUnion(Schema schema) {
      return schema != null && schema.getType() == Schema.Type.UNION;
    }
  }
}
