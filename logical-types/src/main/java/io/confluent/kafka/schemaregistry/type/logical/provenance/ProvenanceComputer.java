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
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;

import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.TreeSet;
import java.util.function.Predicate;

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
 * so {@link IdentityPolicy} stands in for one. Named types are resolved where they are used: each
 * use is an entity scoped by the location using it, and its identity scopes the type's members.
 * A type merged, split or swapped by aliases therefore keeps every location's lineage, and a
 * location whose type changes and changes back starts over rather than taking back its old ids.
 * Under the JSON policy a named type is transparent, with no entity of its own. A recursive type
 * has no finite set of uses, and is rejected. See {@link PathKey}.
 *
 * <p>A member's scope is its container's identity, so the shape of the root matters: the members of
 * an anonymous root schema sit in a different scope from the members of a named type. A version
 * that gives a previously anonymous root a name, or the reverse, therefore starts every member's
 * interval afresh — which is correct, but worth knowing before mixing LTs built by a shim (which
 * inlines everything) with LTs read from Protobuf (whose roots are named). An Avro root that refers
 * to its own record — as a converter keeps one naming types inside it — is walked as the root, so
 * the root's name never matters to Avro.
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
 * <h2>Names and aliases</h2>
 *
 * <p>Under Avro rules, as Avro's own decoder applies them, the index holds canonical names only:
 * an alias names what a writer's field or type was actually called, never one of the writer's
 * aliases. A peer group is resolved as a whole. A peer continues the identity last committed under
 * its own name while it is active, or the identity an alias names; an explicit alias wins, as Avro
 * renames a writer field to the reader field aliasing it even when one of that name exists. A peer
 * continuing itself may carry its aliases forward, but not claim another identity with a new one.
 * Each identity has at most one continuation and each peer continues at most one identity; where
 * Avro itself cannot say which, the history is ambiguous.
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

  /** Avro's promotions, which carry an unnamed union branch's identity when unambiguous. */
  // The name V1 gives an unhinted JSON union branch, followed by its position.
  private static final String POSITIONAL_BRANCH = "connect_union_field_";
  // A JSON branch's content entries: a member's path, a discriminator's value, a leaf's type.
  private static final String MEMBER = "m:";
  private static final String DISCRIMINATOR = "d:";
  private static final String TYPE = "t:";
  // How deep a JSON branch's content looks: enough to tell usual branches apart, and bounded.
  private static final int CONTENT_DEPTH = 3;

  private static final List<Set<String>> PROMOTION_FAMILIES = Arrays.asList(
      new HashSet<>(Arrays.asList("int", "long", "float", "double")),
      new HashSet<>(Arrays.asList("string", "bytes")));

  // The native step of every Avro branch that is not a named type: a primitive's or collection's.
  private static final Set<String> AVRO_UNNAMED = new HashSet<>(Arrays.asList(
      "null", "boolean", "int", "long", "float", "double", "bytes", "string", "array", "map"));

  private ProvenanceComputer() {
  }

  /**
   * A JSON definition key never reaches the data, so under the JSON policy a named type is
   * transparent: its members are identified where it is used, as if inlined.
   */
  static boolean seesThroughNamedTypes(IdentityPolicy policy) {
    return policy == IdentityPolicy.JSON;
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
   * @throws IllegalStateException if a version is internally inconsistent — a repeated path or
   *     two entities resolving to one identity; an {@link AmbiguousProvenanceException} if names
   *     and aliases determine no single identity; a {@link RecursiveTypeException} for a recursive
   *     type
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
      validate(resolver, version);
      byVersion.add(commit(resolver.entities, version, history));
    }
    return new ProvenanceResult(versions, policies, byVersion);
  }

  /**
   * Computes provenance and packages it as a {@link ProvenanceReport} — every version's members
   * with an id allocated per location. The form a provenance endpoint serves.
   */
  public static ProvenanceReport report(List<LogicalType> versions, IdentityPolicy policy) {
    return compute(versions, policy).report();
  }

  /**
   * As {@link #report(List, IdentityPolicy)}, with a policy per version — for a history whose
   * versions were not all read from the same format.
   */
  public static ProvenanceReport report(List<LogicalType> versions,
      List<IdentityPolicy> policies) {
    return compute(versions, policies).report();
  }

  // -----------------------------------------------------------------------------------------
  // Phase 2 -- Validate
  // -----------------------------------------------------------------------------------------

  /**
   * Rejects a version that repeats a path. Runs before anything is committed, since two entities
   * sharing a key would make the version's own output ambiguous. Names need no check here: the
   * index holds canonical names alone, and arbitration gave every identity one continuation.
   */
  private static void validate(Resolver resolver, int version) {
    Set<PathKey> claimedPaths = new HashSet<>();
    for (Entity entity : resolver.entities) {
      if (!claimedPaths.add(entity.path)) {
        throw new IllegalStateException(
            "Duplicate entity path detected in schema version " + version + ": " + entity.path);
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
      if (entity.nameResolved) {
        // The canonical name alone enters the index, and its newest holder owns it. Aliases are
        // remembered on the identity, so a continuation can carry them forward.
        NameKey canonical = new NameKey(entity.identity.getKind(), entity.scope, entity.name);
        entityState.canonicalName = canonical;
        entityState.aliases = new HashSet<>(entity.aliases);
        history.identityIndex.put(canonical, entity.identity);
      }
      entityState.memberNumbers = entity.memberNumbers;
      if (entity.content != null) {
        entityState.branchName = entity.name;
        entityState.content = entity.content;
      }
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

  // -----------------------------------------------------------------------------------------
  // Internal state
  // -----------------------------------------------------------------------------------------

  /** Everything carried from one version to the next. */
  private static final class History {

    /** Identity -> presence, interval start, and the names it was last committed under. */
    private final Map<Identity, EntityState> state = new HashMap<>();

    /**
     * Canonical name -> identity. Every canonical name an identity has been committed under stays
     * mapped to it, dormant or not, until a newer entity is committed under that name. An alias
     * resolves through it, so an alias can name any name the writer actually used, and never one
     * of the writer's own aliases.
     */
    private final Map<NameKey, Identity> identityIndex = new HashMap<>();

    /** Identities grouped by scope, for the continuations that look beyond names. */
    private final Map<Scope, Set<Identity>> identitiesByScope = new HashMap<>();
  }

  /** What the algorithm remembers about one logical identity between versions. */
  private static final class EntityState {

    private boolean active = true;
    private int presenceStartVersion;

    /**
     * The canonical name this identity was last committed under. A canonical match counts only
     * against it: an entity that merely reuses one of the identity's former names is not its
     * continuation.
     */
    private NameKey canonicalName;

    /**
     * The aliases this identity declared when last committed. A continuation under the same name
     * may carry them forward; they claim nothing new.
     */
    private Set<String> aliases = Collections.emptySet();

    /** A Protobuf oneof's member field numbers when last committed; null for anything else. */
    private Set<Integer> memberNumbers;

    /** A JSON union branch's name and content when last committed; null for anything else. */
    private String branchName;
    private Set<String> content;

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

  /** One resolved occurrence: what it is, where it was found, and how it is named. */
  private static final class Entity {

    private final Identity identity;
    private final Scope scope;
    private final String name;
    private final List<String> aliases;
    private final boolean nameResolved;
    private final PathKey path;
    private final Set<Integer> memberNumbers;
    private final Set<String> content;

    Entity(Identity identity, Scope scope, String name, List<String> aliases,
        boolean nameResolved, PathKey path, Set<Integer> memberNumbers, Set<String> content) {
      this.identity = identity;
      this.scope = scope;
      this.name = name;
      this.aliases = aliases;
      this.nameResolved = nameResolved;
      this.path = path;
      this.memberNumbers = memberNumbers;
      this.content = content;
    }
  }

  /**
   * One member of a peer group before its identity is resolved: a struct field, a union branch,
   * or one use of a named type.
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

    /** Where this candidate's own members are keyed: its path, except for a named type's use. */
    private final PathKey membersAt;

    Candidate(EntityKind kind, String name, List<String> aliases, Integer number,
        PathKey path, Schema body, Map<Object, Integer> childDerived) {
      this(kind, name, aliases, number, path, body, childDerived, path);
    }

    Candidate(EntityKind kind, String name, List<String> aliases, Integer number,
        PathKey path, Schema body, Map<Object, Integer> childDerived, PathKey membersAt) {
      this.kind = kind;
      this.name = name;
      this.aliases = aliases != null ? aliases : Collections.emptyList();
      this.number = number;
      this.path = path;
      this.body = body;
      this.childDerived = childDerived;
      this.membersAt = membersAt;
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

    private Map<String, Schema> namedTypes = Collections.emptyMap();
    /** Named types being walked through; a repeat is a recursive type. */
    private final Set<String> inlining = new HashSet<>();

    Resolver(int version, IdentityPolicy policy, History history) {
      this.version = version;
      this.policy = policy;
      this.history = history;
    }

    void resolve(LogicalType logicalType) {
      // Named types are resolved where they are used, so the root scope holds only the root
      // schema's own members.
      namedTypes = logicalType.getNamedTypes();
      Schema root = logicalType.getRootSchema();
      if (policy == IdentityPolicy.AVRO && root != null
          && root.getType() == Schema.Type.NAMED_TYPE_REF) {
        // The converter keeps a root record as a reference while types are nested in its name; it
        // is still the root, its members the root scope's, as when the converter unwraps it.
        String name = root.getQualifiedName();
        Schema body = namedTypes.get(name);
        if (body != null && body.getType() == Schema.Type.STRUCT) {
          walkNamed(name, () -> processGroup(
              memberCandidates(body, PathKey.ofRoot(), Collections.emptyMap()),
              RootScope.INSTANCE));
          return;
        }
      }
      boolean rootHasMembers = root != null
          && (root.getType() == Schema.Type.STRUCT || root.getType() == Schema.Type.UNION);
      if (rootHasMembers) {
        processGroup(memberCandidates(root, PathKey.ofRoot(), Collections.emptyMap()),
            RootScope.INSTANCE);
      } else if (root != null) {
        // A reference, collection or primitive at the root.
        processType(root, RootScope.INSTANCE, PathKey.ofRoot(), Collections.emptyMap());
      }
    }

    /** Resolves one peer group, then descends into each peer's own type. */
    private void processGroup(List<Candidate> peers, Scope scope) {
      if (peers.isEmpty()) {
        return;
      }
      Map<Candidate, Identity> identities = resolveGroup(peers, scope);
      for (Candidate peer : peers) {
        Identity identity = identities.get(peer);
        if (!seen.add(identity)) {
          throw new AmbiguousProvenanceException(
              "Multiple entities resolve to the same logical identity at version " + version
                  + ": " + identity + " (at " + peer.path + ")");
        }
        entities.add(new Entity(identity, scope, peer.name, peer.aliases, isNameResolved(peer),
            peer.path, memberNumbersOf(peer), contentOf(peer)));
        if (isUseOfItsType(peer)) {
          // A named Avro branch is itself the use of its type: its members follow directly.
          String name = peer.body.getQualifiedName();
          walkNamed(name, () -> processType(namedTypes.get(name), identity, peer.membersAt,
              peer.childDerived));
        } else {
          processType(peer.body, identity, peer.membersAt, peer.childDerived);
        }
      }
    }

    /**
     * Walks a type down to the next peer group. A {@code NAMED_TYPE_REF} is walked where it is
     * used: transparently under JSON, and otherwise as one use of the type, an entity scoped by
     * the location using it, whose identity scopes the type's members.
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
        case NAMED_TYPE_REF: {
          String name = schema.getQualifiedName();
          Schema named = namedTypes.get(name);
          if (seesThroughNamedTypes()) {
            // Walked as if inlined: its members are scoped by the field referencing it, so a
            // reference changes no identity.
            walkNamed(name, () -> processType(named, scope, path, derived));
          } else if (named != null) {
            walkNamed(name, () -> processGroup(Collections.singletonList(new Candidate(
                EntityKind.NAMED_TYPE, name, typeAliases(named), null,
                PathKey.ofTypeUse(name, path), named, Collections.emptyMap(), path)), scope));
          }
          break;
        }
        default:
          // Primitives and enums have no members.
          break;
      }
    }

    private void walkNamed(String name, Runnable walk) {
      if (!inlining.add(name)) {
        throw new RecursiveTypeException(name);
      }
      try {
        walk.run();
      } finally {
        inlining.remove(name);
      }
    }

    /** A named type's aliases, which only Avro has, as full names. */
    private List<String> typeAliases(Schema named) {
      if (policy == IdentityPolicy.PROTOBUF || named.getAliases() == null) {
        return Collections.emptyList();
      }
      List<String> aliases = new ArrayList<>();
      for (String alias : named.getAliases()) {
        aliases.add(fullAlias(alias));
      }
      return aliases;
    }

    /**
     * An Avro type alias as a full name. Avro spells one in the null namespace {@code .Name} when
     * the aliasing type has a namespace of its own; its full name is {@code Name}.
     */
    private static String fullAlias(String alias) {
      return alias.startsWith(".") ? alias.substring(1) : alias;
    }

    /** True for a named Avro branch, whose name and aliases are its type's. */
    private boolean isUseOfItsType(Candidate peer) {
      return isNamedAvroBranch(peer.kind, peer.body);
    }

    private boolean isNamedAvroBranch(EntityKind kind, Schema body) {
      return policy == IdentityPolicy.AVRO && kind == EntityKind.BRANCH && body != null
          && body.getType() == Schema.Type.NAMED_TYPE_REF;
    }

    /**
     * A branch's name for identity. An Avro branch is named as Avro finds it — a named type's full
     * name, a primitive's type name — which the converter records as its native step. The logical
     * type's own name for it is shortened where it can be, lengthened where simple names collide,
     * and replaced by any hint, so a branch would change identity with its siblings or its hint.
     */
    private String branchName(UnionBranch branch) {
      if (policy == IdentityPolicy.AVRO) {
        if (isNamedAvroBranch(EntityKind.BRANCH, branch.getSchema())) {
          return branch.getSchema().getQualifiedName();
        }
        List<String> steps = branch.getNativeNames();
        if (steps != null && steps.size() == 1 && steps.get(0) != null) {
          return steps.get(0);
        }
      }
      return branch.getName();
    }

    private boolean seesThroughNamedTypes() {
      return ProvenanceComputer.seesThroughNamedTypes(policy);
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
          candidates.add(new Candidate(EntityKind.BRANCH, branchName(branch),
              branchAliases(branch), numberOf(branch.getFieldNumber(), branch, enclosingDerived),
              parentPath.child(i), branch.getSchema(), enclosingDerived));
        }
      }
      return candidates;
    }

    // -------------------------------------------------------------------------------------
    // Identity resolution
    // -------------------------------------------------------------------------------------

    /**
     * Resolves a whole peer group at once: whether an alias or a canonical name wins a historical
     * identity depends on every peer in the group.
     */
    private Map<Candidate, Identity> resolveGroup(List<Candidate> peers, Scope scope) {
      Map<Candidate, Identity> resolved = new IdentityHashMap<>();
      List<Candidate> byName = new ArrayList<>();
      for (Candidate peer : peers) {
        if (peer.name == null) {
          throw new IllegalArgumentException(
              "Entity at " + peer.path + " has no name (version " + version + ")");
        }
        if (isNameResolved(peer)) {
          byName.add(peer);
        } else if (contentOf(peer) == null) {
          resolved.put(peer, resolveByFormat(peer, scope));
        }
      }
      resolveJsonBranches(peers, scope, resolved);
      settleOneofs(peers, scope, resolved);
      arbitrate(byName, scope, resolved);

      Set<Identity> taken = new HashSet<>(resolved.values());
      for (Candidate peer : byName) {
        if (resolved.containsKey(peer)) {
          continue;
        }
        Identity promoted = policy != IdentityPolicy.AVRO ? null
            : isNamedAvroType(peer) ? shortNameContinuation(peer, scope, peers, taken)
            : peer.kind == EntityKind.BRANCH ? familyContinuation(peer, scope, peers) : null;
        // Brand new, or a historical identity resetting. Folding the minting version into the
        // value keeps it distinct from a live entity that previously released this name.
        Identity identity = promoted != null && taken.add(promoted)
            ? promoted
            : new Identity(peer.kind, scope, new MintedIdentity(peer.name, version));
        resolved.put(peer, identity);
      }
      return resolved;
    }

    /**
     * Avro rules. A peer continues the identity last committed under its own name while that is
     * active, or the identity an alias names. An explicit alias wins: Avro renames a writer field
     * to the reader field aliasing it even when a reader field of that name exists. A peer
     * continuing its own identity may carry its aliases forward, but a new one naming another
     * identity would make one entity continue two. Each identity has at most one continuation, and
     * each peer continues at most one identity.
     */
    private void arbitrate(List<Candidate> peers, Scope scope, Map<Candidate, Identity> resolved) {
      Map<Candidate, Identity> ownClaim = new IdentityHashMap<>();
      Map<Identity, Candidate> canonicalClaimant = new HashMap<>();
      for (Candidate peer : peers) {
        validateAliases(peer);
        NameKey canonicalKey = new NameKey(peer.kind, scope, peer.name);
        Identity indexed = history.identityIndex.get(canonicalKey);
        EntityState indexedState = indexed != null ? history.state.get(indexed) : null;
        if (indexedState != null && indexedState.active
            && canonicalKey.equals(indexedState.canonicalName)) {
          if (canonicalClaimant.put(indexed, peer) != null) {
            throw new AmbiguousProvenanceException("Two entities named " + peer.name
                + " at version " + version + " in " + scope);
          }
          ownClaim.put(peer, indexed);
        }
      }

      Map<Identity, Set<Candidate>> aliasClaimants = new LinkedHashMap<>();
      for (Candidate peer : peers) {
        Identity own = ownClaim.get(peer);
        for (String alias : peer.aliases) {
          Identity aliased = history.identityIndex.get(new NameKey(peer.kind, scope, alias));
          if (aliased == null || aliased.equals(own)) {
            continue;
          }
          if (own != null) {
            if (history.state.get(own).aliases.contains(alias)) {
              // Carried forward from the version that introduced it.
              continue;
            }
            throw new AmbiguousProvenanceException("Ambiguous identity resolution at version "
                + version + ": " + peer.path + " continues " + own
                + " and names another identity by a new alias: " + aliased);
          }
          aliasClaimants.computeIfAbsent(aliased, k -> new LinkedHashSet<>()).add(peer);
        }
      }

      Set<Identity> claimed = new LinkedHashSet<>(canonicalClaimant.keySet());
      claimed.addAll(aliasClaimants.keySet());
      for (Identity historical : claimed) {
        Set<Candidate> byAlias = aliasClaimants.getOrDefault(historical, Collections.emptySet());
        if (byAlias.size() > 1) {
          // Avro's decoder gives it to whichever alias is declared last; its checker, to all.
          throw new AmbiguousProvenanceException("Ambiguous identity resolution at version "
              + version + ": multiple entities claim historical identity " + historical
              + " via aliases");
        }
        Candidate winner = byAlias.isEmpty()
            ? canonicalClaimant.get(historical) : byAlias.iterator().next();
        Identity earlier = resolved.put(winner, historical);
        if (earlier != null) {
          throw new AmbiguousProvenanceException("Ambiguous identity resolution at version "
              + version + ": " + winner.path + " matches multiple historical identities "
              + Arrays.asList(earlier, historical));
        }
      }
    }

    /** Identity where the format supplies it outright: a number, or a name that is its identity. */
    private Identity resolveByFormat(Candidate peer, Scope scope) {
      switch (policy) {
        case JSON:
          return new Identity(peer.kind, scope, new StringIdentity(peer.name));
        case PROTOBUF:
          // A message has no alias mechanism, so it follows its name. A oneof container field has
          // no number and follows its members' numbers, so renaming it changes nothing.
          Set<Integer> members = memberNumbersOf(peer);
          if (members != null) {
            Identity continued = oneofContinuation(scope, members);
            return continued != null
                ? continued
                : new Identity(peer.kind, scope, new MintedIdentity(peer.name, version));
          }
          return peer.number != null
              ? new Identity(peer.kind, scope, new IntegerIdentity(peer.number))
              : new Identity(peer.kind, scope, new StringIdentity(peer.name));
        default:
          // AUTO, with a number.
          return new Identity(peer.kind, scope, new IntegerIdentity(peer.number));
      }
    }

    // -------------------------------------------------------------------------------------
    // Union branches and oneofs
    // -------------------------------------------------------------------------------------

    /** A named Avro branch's type aliases, as full names, so a renamed type keeps its branch. */
    private List<String> branchAliases(UnionBranch branch) {
      if (!isNamedAvroBranch(EntityKind.BRANCH, branch.getSchema())) {
        // A fixed has no named type; the converter records its aliases on the branch.
        List<String> recorded = policy == IdentityPolicy.AVRO ? branch.getNativeAliases() : null;
        if (recorded == null) {
          return null;
        }
        List<String> aliases = new ArrayList<>();
        for (String alias : recorded) {
          aliases.add(fullAlias(alias));
        }
        return aliases;
      }
      Schema named = namedTypes.get(branch.getSchema().getQualifiedName());
      return named != null ? typeAliases(named) : null;
    }

    /**
     * The historical branch an unnamed Avro branch promotes from, when the promotion is
     * unambiguous: this union has one branch of its family, and the scope had one live one.
     */
    private Identity familyContinuation(Candidate peer, Scope scope, List<Candidate> peers) {
      Set<String> family = familyOf(peer.name);
      if (family == null || peer.body == null
          || peer.body.getType() == Schema.Type.NAMED_TYPE_REF
          || peers.stream().filter(p -> p.kind == EntityKind.BRANCH && family.contains(p.name))
              .count() != 1) {
        return null;
      }
      Identity found = null;
      for (Identity historical
          : history.identitiesByScope.getOrDefault(scope, Collections.emptySet())) {
        if (!isLiveBranchOf(historical, family)) {
          continue;
        }
        if (found != null) {
          return null;
        }
        found = historical;
      }
      return found != null && !seen.contains(found) ? found : null;
    }

    /**
     * The historical named type a named Avro type continues when neither its name nor an alias
     * does: the one live in {@code scope} with the same short name, as Avro's checker compares
     * names. A namespace changed without an alias — as nested types inheriting a renamed root's
     * namespace are — then keeps its members. Null where a peer shares the short name.
     */
    private Identity shortNameContinuation(Candidate peer, Scope scope, List<Candidate> peers,
        Set<Identity> taken) {
      String shortName = shortName(peer.name);
      if (peers.stream().filter(p -> p.kind == peer.kind && isNamedAvroType(p)
          && shortName(p.name).equals(shortName)).count() != 1) {
        return null;
      }
      Identity found = null;
      for (Identity historical
          : history.identitiesByScope.getOrDefault(scope, Collections.emptySet())) {
        if (!isLiveNamed(historical, peer.kind, shortName)) {
          continue;
        }
        if (found != null) {
          return null;
        }
        found = historical;
      }
      return found != null && !seen.contains(found) && !taken.contains(found) ? found : null;
    }

    private boolean isLiveNamed(Identity historical, EntityKind kind, String shortName) {
      EntityState state = historical.getKind() == kind ? history.state.get(historical) : null;
      NameKey name = state != null && state.active ? state.canonicalName : null;
      return name != null && shortName(name.name).equals(shortName);
    }

    /**
     * A named type's use, or a named union branch, which is its type's use. A fixed branch is a
     * binary in the logical type; its native step, a full name rather than a type name, marks it.
     */
    private boolean isNamedAvroType(Candidate peer) {
      return peer.kind == EntityKind.NAMED_TYPE || isUseOfItsType(peer)
          || peer.kind == EntityKind.BRANCH && !AVRO_UNNAMED.contains(peer.name);
    }

    private static String shortName(String fullName) {
      return fullName.substring(fullName.lastIndexOf('.') + 1);
    }

    private boolean isLiveBranchOf(Identity historical, Set<String> family) {
      if (historical.getKind() != EntityKind.BRANCH) {
        return false;
      }
      EntityState state = history.state.get(historical);
      return state != null && state.active && state.canonicalName != null
          && family.contains(state.canonicalName.name);
    }

    /** A Protobuf oneof's member field numbers; null for anything that is not a oneof. */
    private Set<Integer> memberNumbersOf(Candidate peer) {
      if (policy != IdentityPolicy.PROTOBUF || peer.kind != EntityKind.FIELD
          || peer.number != null || !isUnion(peer.body)) {
        return null;
      }
      Set<Integer> numbers = new TreeSet<>();
      for (UnionBranch branch : peer.body.getBranches()) {
        Integer number = numberOf(branch.getFieldNumber(), branch, peer.childDerived);
        if (number != null) {
          numbers.add(number);
        }
      }
      return numbers.isEmpty() ? null : numbers;
    }

    /**
     * The live oneof in {@code scope} sharing a member number with {@code members}; null if there
     * is none, or members come from more than one.
     */
    /**
     * JSON union branches, which V1 names by position unless a hint names them: a branch inserted
     * or reordered would otherwise take another's identity. In turn: a hinted branch continues the
     * live branch of its name; a branch continues the one live branch of the same content, where
     * no peer shares it; the one live branch it alone shares a member with, and no conflicting
     * discriminator, as when it moved and its members changed; one at the same position sharing a
     * member with it, where overlap alone cannot tell; else it is new.
     */
    private void resolveJsonBranches(List<Candidate> peers, Scope scope,
        Map<Candidate, Identity> resolved) {
      List<Candidate> pending = new ArrayList<>();
      Map<Set<String>, Integer> shared = new HashMap<>();
      for (Candidate peer : peers) {
        Set<String> content = contentOf(peer);
        if (content != null) {
          pending.add(peer);
          shared.merge(content, 1, Integer::sum);
        }
      }
      if (pending.isEmpty()) {
        return;
      }
      Set<Identity> taken = new HashSet<>(resolved.values());
      for (int phase = 0; phase < 4; phase++) {
        for (Candidate peer : pending) {
          if (resolved.containsKey(peer)) {
            continue;
          }
          Set<String> content = contentOf(peer);
          Identity found;
          if (phase == 0) {
            found = peer.name.startsWith(POSITIONAL_BRANCH)
                ? null : liveBranch(scope, taken, state -> peer.name.equals(state.branchName));
          } else if (phase == 1) {
            found = shared.get(content) == 1
                ? liveBranch(scope, taken, state -> content.equals(state.content)) : null;
          } else if (phase == 2) {
            found = soleOverlap(peer, pending, resolved, scope, taken);
          } else {
            found = liveBranch(scope, taken, state -> peer.name.equals(state.branchName)
                && overlaps(content, state.content));
          }
          if (found != null) {
            taken.add(found);
            resolved.put(peer, found);
          }
        }
      }
      for (Candidate peer : pending) {
        if (!resolved.containsKey(peer)) {
          resolved.put(peer,
              new Identity(peer.kind, scope, new MintedIdentity(peer.name, version)));
        }
      }
    }

    /**
     * The one live, untaken branch {@code peer}'s content overlaps, where no other unresolved peer
     * overlaps it too: one branch sharing members with one other is its continuation, wherever it
     * sits.
     */
    private Identity soleOverlap(Candidate peer, List<Candidate> pending,
        Map<Candidate, Identity> resolved, Scope scope, Set<Identity> taken) {
      Set<String> content = contentOf(peer);
      Identity found = liveBranch(scope, taken, state -> overlaps(content, state.content));
      if (found == null) {
        return null;
      }
      Set<String> theirs = history.state.get(found).content;
      for (Candidate other : pending) {
        if (other != peer && !resolved.containsKey(other) && overlaps(contentOf(other), theirs)) {
          return null;
        }
      }
      return found;
    }

    /**
     * The one live, untaken JSON branch of {@code scope} that {@code test} accepts.
     */
    private Identity liveBranch(Scope scope, Set<Identity> taken, Predicate<EntityState> test) {
      Identity found = null;
      for (Identity historical
          : history.identitiesByScope.getOrDefault(scope, Collections.emptySet())) {
        EntityState state = taken.contains(historical) ? null : history.state.get(historical);
        boolean live = state != null && state.active && state.content != null;
        if (!live || !test.test(state)) {
          continue;
        }
        if (found != null) {
          return null;
        }
        found = historical;
      }
      return found;
    }

    /**
     * Whether two branches' contents share a member, with no discriminator of one named
     * differently by the other.
     */
    private static boolean overlaps(Set<String> mine, Set<String> theirs) {
      for (String entry : mine) {
        if (entry.startsWith(DISCRIMINATOR) && !theirs.contains(entry)) {
          String key = entry.substring(0, entry.indexOf('=', DISCRIMINATOR.length()) + 1);
          if (theirs.stream().anyMatch(other -> other.startsWith(key))) {
            return false;
          }
        }
      }
      if (mine.equals(theirs)) {
        // Nothing tells them apart, even with no members: as alike as they can be.
        return true;
      }
      return mine.stream().anyMatch(entry -> entry.startsWith(MEMBER) && theirs.contains(entry));
    }

    /**
     * A JSON union branch's content: its members' names, with the value of a one-value enum
     * (a discriminator), or its type for a branch with no members; null for anything else.
     */
    private Set<String> contentOf(Candidate peer) {
      if (policy != IdentityPolicy.JSON || peer.kind != EntityKind.BRANCH) {
        return null;
      }
      Set<String> content = new TreeSet<>();
      addContent(peer.body, "", 0, new HashSet<>(), content);
      return content;
    }

    /**
     * The content of {@code schema} at {@code prefix}: each member's path, a one-value enum's
     * value with it, and the type of what has no members, down to {@link #CONTENT_DEPTH} levels
     * — so arrays of different items, or structs differing below the top, are told apart.
     */
    private void addContent(Schema schema, String prefix, int depth, Set<String> naming,
        Set<String> content) {
      Schema body = schema;
      while (body != null && body.getType() == Schema.Type.NAMED_TYPE_REF) {
        if (!naming.add(body.getQualifiedName())) {
          return;
        }
        body = namedTypes.get(body.getQualifiedName());
      }
      if (body == null || depth > CONTENT_DEPTH) {
        return;
      }
      switch (body.getType()) {
        case STRUCT:
          for (Field field : body.getFields()) {
            Schema type = resolved(field.getSchema());
            content.add(MEMBER + prefix + field.getName());
            if (isDiscriminator(type)) {
              content.add(DISCRIMINATOR + prefix + field.getName() + "="
                  + type.getEnumValues().get(0).getSymbol());
            }
            addContent(field.getSchema(), prefix + field.getName() + "/", depth + 1,
                new HashSet<>(naming), content);
          }
          break;
        case ARRAY:
        case MULTISET:
          content.add(TYPE + prefix + body.getType().name());
          addContent(body.getElementType(), prefix + "[]/", depth + 1, naming, content);
          break;
        case MAP:
          content.add(TYPE + prefix + body.getType().name());
          addContent(body.getValueType(), prefix + "{}/", depth + 1, naming, content);
          break;
        default:
          content.add(TYPE + prefix + body.getType().name());
          break;
      }
    }

    private static boolean isDiscriminator(Schema type) {
      return type != null && type.getType() == Schema.Type.ENUM
          && type.getEnumValues().size() == 1;
    }

    private Schema resolved(Schema schema) {
      Set<String> seenNames = new HashSet<>();
      Schema current = schema;
      while (current != null && current.getType() == Schema.Type.NAMED_TYPE_REF
          && seenNames.add(current.getQualifiedName())) {
        current = namedTypes.get(current.getQualifiedName());
      }
      return current;
    }

    /**
     * Gives a historical oneof to one of the peers continuing it by member numbers: a oneof split
     * in two has each part share numbers with it. The part sharing the most keeps it — ties to
     * the one holding the lowest shared number — and the others are new.
     */
    private void settleOneofs(List<Candidate> peers, Scope scope,
        Map<Candidate, Identity> resolved) {
      Map<Identity, Candidate> keeper = new HashMap<>();
      for (Candidate peer : peers) {
        Identity identity = resolved.get(peer);
        Set<Integer> members = memberNumbersOf(peer);
        EntityState state = identity != null ? history.state.get(identity) : null;
        if (members == null || state == null || state.memberNumbers == null) {
          continue;
        }
        Candidate other = keeper.get(identity);
        Candidate kept = other == null
            || keepsOneof(peer, other, state.memberNumbers) ? peer : other;
        keeper.put(identity, kept);
        Candidate minted = other == null ? null : kept == peer ? other : peer;
        if (minted != null) {
          resolved.put(minted,
              new Identity(minted.kind, scope, new MintedIdentity(minted.name, version)));
        }
      }
    }

    private boolean keepsOneof(Candidate peer, Candidate other, Set<Integer> historical) {
      Set<Integer> mine = shared(memberNumbersOf(peer), historical);
      Set<Integer> theirs = shared(memberNumbersOf(other), historical);
      return mine.size() != theirs.size()
          ? mine.size() > theirs.size()
          : Collections.min(mine) < Collections.min(theirs);
    }

    private static Set<Integer> shared(Set<Integer> members, Set<Integer> historical) {
      Set<Integer> shared = new TreeSet<>(members);
      shared.retainAll(historical);
      return shared;
    }

    private Identity oneofContinuation(Scope scope, Set<Integer> members) {
      Identity found = null;
      for (Identity historical
          : history.identitiesByScope.getOrDefault(scope, Collections.emptySet())) {
        EntityState state = history.state.get(historical);
        if (state == null || !state.active || state.memberNumbers == null
            || Collections.disjoint(state.memberNumbers, members)) {
          continue;
        }
        if (found != null) {
          return null;
        }
        found = historical;
      }
      return found != null && !seen.contains(found) ? found : null;
    }

    /** Avro keeps aliases as a set, and an alias equal to the entity's own name changes nothing. */
    private void validateAliases(Candidate peer) {
      for (String alias : peer.aliases) {
        if (alias == null) {
          throw new IllegalArgumentException("Null alias at " + peer.path);
        }
      }
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
      // The multi-message root is synthetic: its fields name messages and never had numbers, so
      // they follow their names and reordering the file's messages leaves every id in place.
      if (policy != IdentityPolicy.PROTOBUF || recordsAnyNumber(struct)
          || Boolean.TRUE.equals(
              struct.getParams().get(ProtoToLogicalTypeConverter.MULTI_MESSAGE_ROOT_PARAM))) {
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

    private static Set<String> familyOf(String typeName) {
      for (Set<String> family : PROMOTION_FAMILIES) {
        if (family.contains(typeName)) {
          return family;
        }
      }
      return null;
    }
  }
}
