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
 * Allocates a provenance id to every member location across a sequence of {@link LogicalType}
 * versions, so that two versions can be put in correspondence without matching on names and
 * without an out-of-band column ID system.
 *
 * <h2>Matching</h2>
 *
 * <p>Each version is matched against the one before it alone. Every location — a struct field, a
 * union branch, or one use of a named type — is matched to at most one location of the previous
 * version, or to none; a matched member keeps its match's id, and any other takes a new one. A
 * location is matched only among the previous version's locations under its parent's match, so
 * the parent is matched first and scopes its members; collection steps ({@code []},
 * {@code {key}}, {@code {value}}) are part of that scope. Nothing absent from the previous version
 * is ever continued, so a range of versions pairs its versions exactly as the whole history does.
 *
 * <p>The rules are format-specific and a {@code LogicalType} carries no format discriminator, so
 * each version comes with an {@link IdentityPolicy}. Versions of different policies match nothing:
 * a history that changes format starts every location afresh at the change.
 *
 * <h2>Named types</h2>
 *
 * <p>Named types are matched where they are used: each use is a location of its own, under the
 * location using it, and it scopes the type's members. A type merged, split or swapped by aliases
 * therefore keeps every location's lineage, and each use of a shared type has its own ids. A named
 * Avro union branch is itself the use of its type. Under the JSON policy a named type is
 * transparent, as if inlined. A recursive type has no finite set of uses, and is rejected. An Avro
 * or Protobuf root that refers to its own record or message — as a converter keeps one naming
 * types inside it — is walked as the root, so the root's name never matters.
 *
 * <h2>Names and aliases</h2>
 *
 * <p>Under Avro rules, as Avro's own decoder applies them, a peer group is matched as a whole. A
 * peer continues the previous location of its own name, or the one an alias names; an alias names
 * what the previous version called a location, never one of its aliases nor any older name. An
 * explicit alias wins, as Avro renames a writer field to the reader field aliasing it even when one
 * of that name exists. A peer continuing itself may carry its aliases forward, but not claim
 * another location with a new one. Each previous location has at most one continuation and each
 * peer continues at most one; where Avro itself cannot say which, the history is ambiguous.
 *
 * <h2>Allocation</h2>
 *
 * <p>Ids are allocated walking versions in order and, within a version, members in path order —
 * the pre-order walk — taking the next integer for each member matched to nothing.
 */
public final class ProvenanceComputer {

  // The name V1 gives an unhinted JSON union branch, followed by its position.
  private static final String POSITIONAL_BRANCH = "connect_union_field_";
  // A JSON branch's content entries: a member's path, a discriminator's value, a leaf's type.
  private static final String MEMBER = "m:";
  private static final String DISCRIMINATOR = "d:";
  private static final String TYPE = "t:";
  // How deep a JSON branch's content looks: enough to tell usual branches apart, and bounded.
  private static final int CONTENT_DEPTH = 3;

  /** Avro's promotions, which carry an unnamed union branch's match when unambiguous. */
  private static final List<Set<String>> PROMOTION_FAMILIES = Arrays.asList(
      new HashSet<>(Arrays.asList("int", "long", "float", "double")),
      new HashSet<>(Arrays.asList("string", "bytes")));

  // The native step of every Avro branch that is not a named type: a primitive's or collection's.
  private static final Set<String> AVRO_UNNAMED = new HashSet<>(Arrays.asList(
      "null", "boolean", "int", "long", "float", "double", "bytes", "string", "array", "map"));

  private ProvenanceComputer() {
  }

  /**
   * As {@link #report(List, List)}, applying one identity policy to every version.
   */
  public static ProvenanceReport report(List<LogicalType> versions, IdentityPolicy policy) {
    Objects.requireNonNull(versions, "versions");
    Objects.requireNonNull(policy, "policy");
    return report(versions, Collections.nCopies(versions.size(), policy));
  }

  /**
   * Every version's members with their provenance ids — the form a provenance endpoint serves.
   *
   * @param versions the schema versions in chronological order
   * @param policies one policy per version, in the same order
   * @throws IllegalArgumentException if the lists differ in size, a version or policy is null, or
   *     an entity has no name
   * @throws AmbiguousProvenanceException if names and aliases determine no single match
   * @throws RecursiveTypeException for a recursive type
   */
  public static ProvenanceReport report(List<LogicalType> versions,
      List<IdentityPolicy> policies) {
    Objects.requireNonNull(versions, "versions");
    Objects.requireNonNull(policies, "policies");
    if (versions.size() != policies.size()) {
      throw new IllegalArgumentException("Expected one policy per version, got "
          + policies.size() + " policies for " + versions.size() + " versions");
    }
    List<ProvenanceReport.Version> reported = new ArrayList<>(versions.size());
    Node previous = null;
    IdentityPolicy previousPolicy = null;
    int nextId = 1;
    for (int version = 0; version < versions.size(); version++) {
      LogicalType logicalType = versions.get(version);
      if (logicalType == null) {
        throw new IllegalArgumentException("Null LogicalType at version " + version);
      }
      IdentityPolicy policy = policies.get(version);
      if (policy == null) {
        throw new IllegalArgumentException("Null IdentityPolicy at version " + version);
      }

      Walk walk = new Walk(version, policy, logicalType);
      Node root = walk.walk(policy == previousPolicy ? previous : null);
      List<ProvenanceReport.Member> members = new ArrayList<>(walk.members.size());
      for (Node member : walk.members) {
        member.id = member.match != null ? member.match.id : nextId++;
        members.add(new ProvenanceReport.Member(
            member.where.path, member.where.names, member.id));
      }
      reported.add(new ProvenanceReport.Version(version, members));
      previous = root;
      previousPolicy = policy;
    }
    return new ProvenanceReport(reported, nextId - 1);
  }

  // -----------------------------------------------------------------------------------------
  // Locations
  // -----------------------------------------------------------------------------------------

  private enum Kind {
    /** One use of a named type at one location: an Avro record, enum or fixed, or a message. */
    NAMED_TYPE,
    FIELD,
    BRANCH
  }

  /** One location of one version: what it is, where it is, and what it matched. */
  private static final class Node {

    private final Kind kind;
    private final String name;
    private final List<String> aliases;
    private final Integer number;
    private final Schema body;
    /** Derived Protobuf numbers to hand to this node's own members, if any. */
    private final Map<Object, Integer> childDerived;
    private final Where where;

    /** A Protobuf oneof's member numbers, a JSON branch's content; null for anything else. */
    private Set<Integer> memberNumbers;
    private Set<String> content;

    /** This node's member groups, keyed by the collection steps leading to each. */
    private final Map<String, List<Node>> groups = new HashMap<>();
    private Node match;
    private int id;

    Node(Kind kind, String name, List<String> aliases, Integer number, Schema body,
        Map<Object, Integer> childDerived, Where where) {
      this.kind = kind;
      this.name = name;
      this.aliases = aliases != null ? aliases : Collections.emptyList();
      this.number = number;
      this.body = body;
      this.childDerived = childDerived;
      this.where = where;
    }

    static Node root(Node previous) {
      Node root = new Node(null, null, null, null, null, Collections.emptyMap(), null);
      root.match = previous;
      return root;
    }

    /**
     * The previous version's members of this node's match at {@code step}, of one kind.
     */
    List<Node> previous(String step, Kind kind) {
      List<Node> group = match != null ? match.groups.get(step) : null;
      return group != null && group.get(0).kind == kind ? group : Collections.emptyList();
    }
  }

  /**
   * A position in the walk: the inlined path, the native names so far (null once an edge recorded
   * none; see {@link Schema#getNativeEntryNames}), and the entry steps of the node we stand on,
   * spelled only if the walk goes further.
   */
  private static final class Where {

    private final List<Integer> path;
    private final List<String> names;
    private final List<String> pending;

    Where(List<Integer> path, List<String> names, List<String> pending) {
      this.path = path;
      this.names = names;
      this.pending = pending;
    }

    static Where root(Schema root) {
      return new Where(Collections.emptyList(), Collections.emptyList(), entryOf(root));
    }

    /**
     * One step down, to a member or through a collection, spelled by {@code steps}.
     */
    Where descend(Schema type, int step, List<String> steps) {
      List<Integer> extended = new ArrayList<>(path.size() + 1);
      extended.addAll(path);
      extended.add(step);
      return new Where(Collections.unmodifiableList(extended), spell(names, pending, steps),
          entryOf(type));
    }

    /**
     * Through a reference to {@code named}: no step, but the type's own entry steps.
     */
    Where through(Schema named) {
      return new Where(path, names, spell(pending, Collections.emptyList(), entryOf(named)));
    }

    private static List<String> entryOf(Schema type) {
      return type != null ? type.getNativeEntryNames() : Collections.emptyList();
    }

    /**
     * {@code names}, then {@code pending}, then {@code steps}; null if any part is unknown.
     */
    private static List<String> spell(List<String> names, List<String> pending,
        List<String> steps) {
      if (names == null || pending == null || steps == null) {
        return null;
      }
      List<String> spelled = new ArrayList<>(names.size() + pending.size() + steps.size());
      spelled.addAll(names);
      spelled.addAll(pending);
      spelled.addAll(steps);
      return Collections.unmodifiableList(spelled);
    }
  }

  // -----------------------------------------------------------------------------------------
  // Walking one version
  // -----------------------------------------------------------------------------------------

  /**
   * Walks one version top-down, matching each peer group against the previous version's members
   * of its parent's match. Reads the previous version and never writes it.
   */
  private static final class Walk {

    private final int version;
    private final IdentityPolicy policy;
    private final LogicalType logicalType;
    private final Map<String, Schema> namedTypes;

    /** The members, fields and branches, in path order. */
    private final List<Node> members = new ArrayList<>();
    /** Named types being walked through; a repeat is a recursive type. */
    private final Set<String> inlining = new HashSet<>();

    Walk(int version, IdentityPolicy policy, LogicalType logicalType) {
      this.version = version;
      this.policy = policy;
      this.logicalType = logicalType;
      this.namedTypes = logicalType.getNamedTypes();
    }

    Node walk(Node previous) {
      Node root = Node.root(previous);
      Schema schema = logicalType.getRootSchema();
      if (schema == null) {
        return root;
      }
      Where where = Where.root(schema);
      if (policy != IdentityPolicy.JSON && schema.getType() == Schema.Type.NAMED_TYPE_REF) {
        // The converter keeps a root record or message as a reference while types are nested in
        // it or a peer uses it; it is still the root, its members the root's.
        String name = schema.getQualifiedName();
        Schema body = namedTypes.get(name);
        if (body != null && body.getType() == Schema.Type.STRUCT) {
          walkNamed(name, () -> processGroup(
              memberNodes(body, where.through(body), Collections.emptyMap()), root, ""));
          return root;
        }
      }
      processType(schema, root, "", where, Collections.emptyMap());
      return root;
    }

    /** Matches one peer group, then descends into each peer's own type. */
    private void processGroup(List<Node> peers, Node owner, String step) {
      if (peers.isEmpty()) {
        return;
      }
      owner.groups.put(step, peers);
      match(peers, owner.previous(step, peers.get(0).kind));
      for (Node peer : peers) {
        if (peer.kind != Kind.NAMED_TYPE) {
          members.add(peer);
        }
        if (isUseOfItsType(peer)) {
          // A named Avro branch is itself the use of its type: its members follow directly.
          String name = peer.body.getQualifiedName();
          Schema named = namedTypes.get(name);
          walkNamed(name, () -> processType(named, peer, "", peer.where.through(named),
              peer.childDerived));
        } else {
          processType(peer.body, peer, "", peer.where, peer.childDerived);
        }
      }
    }

    /**
     * Walks a type down to the next peer group. A {@code NAMED_TYPE_REF} is walked where it is
     * used: transparently under JSON, and otherwise as one use of the type, a location under the
     * one using it that scopes the type's members.
     */
    private void processType(Schema schema, Node owner, String step, Where where,
        Map<Object, Integer> derived) {
      if (schema == null) {
        return;
      }
      switch (schema.getType()) {
        case STRUCT:
          processGroup(memberNodes(schema, where, Collections.emptyMap()), owner, step);
          break;
        case UNION:
          // Branch numbers, when derived, come from the enclosing struct: a oneof's branches
          // continue that message's numbering.
          processGroup(memberNodes(schema, where, derived), owner, step);
          break;
        case ARRAY:
        case MULTISET:
          processType(schema.getElementType(), owner, step(step, "[]"),
              where.descend(schema.getElementType(), 0, schema.getElementNativeNames()),
              derived);
          break;
        case MAP:
          processType(schema.getKeyType(), owner, step(step, "{key}"),
              where.descend(schema.getKeyType(), 0, schema.getKeyNativeNames()), derived);
          processType(schema.getValueType(), owner, step(step, "{value}"),
              where.descend(schema.getValueType(), 1, schema.getValueNativeNames()), derived);
          break;
        case NAMED_TYPE_REF: {
          String name = schema.getQualifiedName();
          Schema named = namedTypes.get(name);
          if (policy == IdentityPolicy.JSON) {
            // Walked as if inlined, so a reference changes nothing.
            walkNamed(name, () -> processType(named, owner, step, where.through(named), derived));
          } else if (named != null) {
            walkNamed(name, () -> processGroup(Collections.singletonList(new Node(
                Kind.NAMED_TYPE, name, typeAliases(named), null, named, Collections.emptyMap(),
                where.through(named))), owner, step));
          }
          break;
        }
        default:
          // Primitives and enums have no members.
          break;
      }
    }

    private static String step(String step, String next) {
      return step.isEmpty() ? next : step + "/" + next;
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

    /**
     * The members of a STRUCT or UNION, carrying their effective numbers. A struct derives its own
     * numbering; a union's branches continue the enclosing struct's, since that is how a oneof is
     * numbered.
     */
    private List<Node> memberNodes(Schema container, Where where,
        Map<Object, Integer> enclosingDerived) {
      List<Node> nodes = new ArrayList<>();
      if (container.getType() == Schema.Type.STRUCT) {
        Map<Object, Integer> derived = deriveNumbers(container);
        List<Field> fields = container.getFields();
        for (int i = 0; i < fields.size(); i++) {
          Field field = fields.get(i);
          nodes.add(new Node(Kind.FIELD, field.getName(), field.getAliases(),
              numberOf(field.getFieldNumber(), field, derived), field.getSchema(), derived,
              where.descend(field.getSchema(), i, field.getNativeNames())));
        }
      } else {
        List<UnionBranch> branches = container.getBranches();
        for (int i = 0; i < branches.size(); i++) {
          UnionBranch branch = branches.get(i);
          nodes.add(new Node(Kind.BRANCH, branchName(branch), branchAliases(branch),
              numberOf(branch.getFieldNumber(), branch, enclosingDerived), branch.getSchema(),
              enclosingDerived, where.descend(branch.getSchema(), i, branch.getNativeNames())));
        }
      }
      for (Node node : nodes) {
        node.memberNumbers = memberNumbersOf(node);
        node.content = contentOf(node);
      }
      return nodes;
    }

    // -------------------------------------------------------------------------------------
    // Matching one peer group
    // -------------------------------------------------------------------------------------

    /**
     * Matches a whole peer group at once against {@code previous}, the previous version's members
     * of the parent's match: whether an alias or a name wins a previous location depends on every
     * peer in the group.
     */
    private void match(List<Node> peers, List<Node> previous) {
      Map<Node, Node> matched = new IdentityHashMap<>();
      List<Node> byName = new ArrayList<>();
      for (Node peer : peers) {
        if (peer.name == null) {
          throw new IllegalArgumentException(
              "Entity at " + peer.where.path + " has no name (version " + version + ")");
        }
        if (policy == IdentityPolicy.AVRO) {
          byName.add(peer);
        } else if (peer.content == null) {
          Node found = matchByFormat(peer, previous);
          if (found != null) {
            matched.put(peer, found);
          }
        }
      }
      matchJsonBranches(peers, previous, matched);
      settleOneofs(peers, matched);
      arbitrate(byName, previous, matched);

      Set<Node> taken = Collections.newSetFromMap(new IdentityHashMap<>());
      taken.addAll(matched.values());
      for (Node peer : byName) {
        if (matched.containsKey(peer)) {
          continue;
        }
        Node continued = isNamedAvroType(peer) ? shortNameContinuation(peer, peers, previous)
            : peer.kind == Kind.BRANCH ? familyContinuation(peer, peers, previous) : null;
        if (continued != null && taken.add(continued)) {
          matched.put(peer, continued);
        }
      }
      requireOneToOne(peers, matched);
      for (Node peer : peers) {
        peer.match = matched.get(peer);
      }
    }

    /** Two peers of one key, or continuing one previous location, determine no single match. */
    private void requireOneToOne(List<Node> peers, Map<Node, Node> matched) {
      Set<Object> keys = new HashSet<>();
      Set<Node> continued = Collections.newSetFromMap(new IdentityHashMap<>());
      for (Node peer : peers) {
        Node previous = matched.get(peer);
        Object key = peer.number != null ? peer.number : peer.name;
        if (!keys.add(key) || previous != null && !continued.add(previous)) {
          throw new AmbiguousProvenanceException("Multiple entities resolve to the same logical "
              + "identity at version " + version + ": " + key + " (at " + peer.where.path + ")");
        }
      }
    }

    /**
     * Where the format decides outright: a JSON name; a Protobuf field number, a message's name,
     * or a oneof's member numbers, which a renamed oneof keeps.
     */
    private static Node matchByFormat(Node peer, List<Node> previous) {
      if (peer.memberNumbers != null) {
        return sole(previous, p -> p.memberNumbers != null
            && !Collections.disjoint(p.memberNumbers, peer.memberNumbers));
      }
      if (peer.number != null) {
        return sole(previous, p -> peer.number.equals(p.number));
      }
      return sole(previous, p -> p.number == null && p.memberNumbers == null
          && peer.name.equals(p.name));
    }

    /**
     * The one previous location {@code test} accepts; null if none or several do.
     */
    private static Node sole(List<Node> previous, Predicate<Node> test) {
      Node found = null;
      for (Node p : previous) {
        if (test.test(p)) {
          if (found != null) {
            return null;
          }
          found = p;
        }
      }
      return found;
    }

    // -------------------------------------------------------------------------------------
    // Avro names and aliases
    // -------------------------------------------------------------------------------------

    /**
     * Avro rules. A peer continues the previous location of its own name, or the one an alias
     * names. An explicit alias wins: Avro renames a writer field to the reader field aliasing it
     * even when a reader field of that name exists. A peer continuing itself may carry its aliases
     * forward, but a new one naming another location would make one continue two. Each previous
     * location has at most one continuation, and each peer continues at most one.
     */
    private void arbitrate(List<Node> peers, List<Node> previous, Map<Node, Node> matched) {
      Map<String, Node> byPreviousName = new HashMap<>();
      for (Node p : previous) {
        byPreviousName.put(p.name, p);
      }
      Map<Node, Node> own = new IdentityHashMap<>();
      Map<Node, Node> ownClaimant = new LinkedHashMap<>();
      for (Node peer : peers) {
        for (String alias : peer.aliases) {
          if (alias == null) {
            throw new IllegalArgumentException("Null alias at " + peer.where.path);
          }
        }
        Node p = byPreviousName.get(peer.name);
        if (p != null) {
          if (ownClaimant.put(p, peer) != null) {
            throw new AmbiguousProvenanceException("Two entities named " + peer.name
                + " at version " + version + " at " + peer.where.path);
          }
          own.put(peer, p);
        }
      }

      Map<Node, Set<Node>> aliasClaimants = new LinkedHashMap<>();
      for (Node peer : peers) {
        Node mine = own.get(peer);
        for (String alias : peer.aliases) {
          Node aliased = byPreviousName.get(alias);
          if (aliased == null || aliased == mine) {
            continue;
          }
          if (mine != null) {
            if (mine.aliases.contains(alias)) {
              // Carried forward from the version that introduced it.
              continue;
            }
            throw new AmbiguousProvenanceException("Ambiguous identity resolution at version "
                + version + ": " + peer.where.path + " continues " + mine.name
                + " and names another entity by a new alias: " + aliased.name);
          }
          aliasClaimants.computeIfAbsent(aliased, k -> new LinkedHashSet<>()).add(peer);
        }
      }

      Set<Node> claimed = new LinkedHashSet<>(ownClaimant.keySet());
      claimed.addAll(aliasClaimants.keySet());
      for (Node p : claimed) {
        Set<Node> byAlias = aliasClaimants.getOrDefault(p, Collections.emptySet());
        if (byAlias.size() > 1) {
          // Avro's decoder gives it to whichever alias is declared last; its checker, to all.
          throw new AmbiguousProvenanceException("Ambiguous identity resolution at version "
              + version + ": multiple entities claim " + p.name + " via aliases");
        }
        Node winner = byAlias.isEmpty() ? ownClaimant.get(p) : byAlias.iterator().next();
        Node earlier = matched.put(winner, p);
        if (earlier != null) {
          throw new AmbiguousProvenanceException("Ambiguous identity resolution at version "
              + version + ": " + winner.where.path + " matches both " + earlier.name + " and "
              + p.name);
        }
      }
    }

    /**
     * The previous named type a named Avro type continues when neither its name nor an alias
     * does: the one with the same short name, as Avro's checker compares names. A namespace
     * changed without an alias — as nested types inheriting a renamed root's namespace are — then
     * keeps its members. Null where a peer shares the short name.
     */
    private Node shortNameContinuation(Node peer, List<Node> peers, List<Node> previous) {
      String shortName = shortName(peer.name);
      if (peers.stream().filter(p -> isNamedAvroType(p)
          && shortName(p.name).equals(shortName)).count() != 1) {
        return null;
      }
      return sole(previous, p -> shortName(p.name).equals(shortName));
    }

    /**
     * The previous branch an unnamed Avro branch promotes from, when the promotion is
     * unambiguous: this union has one branch of its family, and the previous one had one.
     */
    private static Node familyContinuation(Node peer, List<Node> peers, List<Node> previous) {
      Set<String> family = familyOf(peer.name);
      if (family == null || peer.body == null
          || peer.body.getType() == Schema.Type.NAMED_TYPE_REF
          || peers.stream().filter(p -> family.contains(p.name)).count() != 1) {
        return null;
      }
      return sole(previous, p -> family.contains(p.name));
    }

    /**
     * A named type's use, or a named union branch, which is its type's use. A fixed branch is a
     * binary in the logical type; its native step, a full name rather than a type name, marks it.
     */
    private boolean isNamedAvroType(Node peer) {
      return peer.kind == Kind.NAMED_TYPE || isUseOfItsType(peer)
          || peer.kind == Kind.BRANCH && !AVRO_UNNAMED.contains(peer.name);
    }

    private static String shortName(String fullName) {
      return fullName.substring(fullName.lastIndexOf('.') + 1);
    }

    private static Set<String> familyOf(String typeName) {
      for (Set<String> family : PROMOTION_FAMILIES) {
        if (family.contains(typeName)) {
          return family;
        }
      }
      return null;
    }

    /** True for a named Avro branch, whose name and aliases are its type's. */
    private boolean isUseOfItsType(Node peer) {
      return isNamedAvroBranch(peer.kind, peer.body);
    }

    private boolean isNamedAvroBranch(Kind kind, Schema body) {
      return policy == IdentityPolicy.AVRO && kind == Kind.BRANCH && body != null
          && body.getType() == Schema.Type.NAMED_TYPE_REF;
    }

    /**
     * A branch's name for matching. An Avro branch is named as Avro finds it — a named type's full
     * name, a primitive's type name — which the converter records as its native step. The logical
     * type's own name for it is shortened where it can be, lengthened where simple names collide,
     * and replaced by any hint, so a branch would change match with its siblings or its hint.
     */
    private String branchName(UnionBranch branch) {
      if (policy == IdentityPolicy.AVRO) {
        if (isNamedAvroBranch(Kind.BRANCH, branch.getSchema())) {
          return branch.getSchema().getQualifiedName();
        }
        List<String> steps = branch.getNativeNames();
        if (steps != null && steps.size() == 1 && steps.get(0) != null) {
          return steps.get(0);
        }
      }
      return branch.getName();
    }

    /** A named Avro branch's type aliases, as full names, so a renamed type keeps its branch. */
    private List<String> branchAliases(UnionBranch branch) {
      if (!isNamedAvroBranch(Kind.BRANCH, branch.getSchema())) {
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

    /** A named type's aliases, which only Avro has, as full names. */
    private List<String> typeAliases(Schema named) {
      if (policy != IdentityPolicy.AVRO || named.getAliases() == null) {
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

    // -------------------------------------------------------------------------------------
    // Protobuf oneofs
    // -------------------------------------------------------------------------------------

    /** A Protobuf oneof's member field numbers; null for anything that is not a oneof. */
    private Set<Integer> memberNumbersOf(Node node) {
      if (policy != IdentityPolicy.PROTOBUF || node.kind != Kind.FIELD
          || node.number != null || !isUnion(node.body)) {
        return null;
      }
      Set<Integer> numbers = new TreeSet<>();
      for (UnionBranch branch : node.body.getBranches()) {
        Integer number = numberOf(branch.getFieldNumber(), branch, node.childDerived);
        if (number != null) {
          numbers.add(number);
        }
      }
      return numbers.isEmpty() ? null : numbers;
    }

    /**
     * Gives a previous oneof to one of the peers continuing it by member numbers: a oneof split in
     * two has each part share numbers with it. The part sharing the most keeps it — ties to the
     * one holding the lowest shared number — and the others are new.
     */
    private static void settleOneofs(List<Node> peers, Map<Node, Node> matched) {
      Map<Node, Node> keeper = new IdentityHashMap<>();
      for (Node peer : peers) {
        Node previous = matched.get(peer);
        if (peer.memberNumbers == null || previous == null || previous.memberNumbers == null) {
          continue;
        }
        Node other = keeper.get(previous);
        Node kept = other == null
            || keepsOneof(peer, other, previous.memberNumbers) ? peer : other;
        keeper.put(previous, kept);
        if (other != null) {
          matched.remove(kept == peer ? other : peer);
        }
      }
    }

    private static boolean keepsOneof(Node peer, Node other, Set<Integer> previous) {
      Set<Integer> mine = shared(peer.memberNumbers, previous);
      Set<Integer> theirs = shared(other.memberNumbers, previous);
      return mine.size() != theirs.size()
          ? mine.size() > theirs.size()
          : Collections.min(mine) < Collections.min(theirs);
    }

    private static Set<Integer> shared(Set<Integer> members, Set<Integer> previous) {
      Set<Integer> shared = new TreeSet<>(members);
      shared.retainAll(previous);
      return shared;
    }

    // -------------------------------------------------------------------------------------
    // JSON union branches
    // -------------------------------------------------------------------------------------

    /**
     * JSON union branches, which V1 names by position unless a hint names them: a branch inserted
     * or reordered would otherwise take another's place. In turn: a hinted branch continues the
     * previous branch of its name; a branch continues the one previous branch of the same content,
     * where no peer shares it; the one previous branch it alone shares a member with, and no
     * conflicting discriminator, as when it moved and its members changed; one at the same position
     * sharing a member with it, where overlap alone cannot tell; else it is new. None continues
     * another across a discriminator a branch related to them has (see {@link #crosses}).
     */
    private static void matchJsonBranches(List<Node> peers, List<Node> previous,
        Map<Node, Node> matched) {
      List<Node> pending = new ArrayList<>();
      Map<Set<String>, Integer> shared = new HashMap<>();
      for (Node peer : peers) {
        if (peer.content != null) {
          pending.add(peer);
          shared.merge(peer.content, 1, Integer::sum);
        }
      }
      if (pending.isEmpty()) {
        return;
      }
      Set<Node> taken = Collections.newSetFromMap(new IdentityHashMap<>());
      taken.addAll(matched.values());
      for (int phase = 0; phase < 4; phase++) {
        for (Node peer : pending) {
          if (matched.containsKey(peer)) {
            continue;
          }
          Set<String> content = peer.content;
          Node found;
          if (phase == 0) {
            found = peer.name.startsWith(POSITIONAL_BRANCH)
                ? null : previousBranch(previous, taken, p -> peer.name.equals(p.name));
          } else if (phase == 1) {
            found = shared.get(content) == 1
                ? previousBranch(previous, taken, p -> content.equals(p.content)) : null;
          } else if (phase == 2) {
            found = soleOverlap(peer, pending, matched, previous, taken);
          } else {
            found = previousBranch(previous, taken, p -> peer.name.equals(p.name)
                && overlaps(content, p.content)
                && !crosses(peer, p, pending, matched, previous, taken));
          }
          if (found != null) {
            taken.add(found);
            matched.put(peer, found);
          }
        }
      }
    }

    /**
     * The one untaken previous branch {@code peer}'s content overlaps, where no other unresolved
     * peer overlaps it too: one branch sharing members with one other is its continuation,
     * wherever it sits.
     */
    private static Node soleOverlap(Node peer, List<Node> pending, Map<Node, Node> matched,
        List<Node> previous, Set<Node> taken) {
      Node found = previousBranch(previous, taken, p -> overlaps(peer.content, p.content)
          && !crosses(peer, p, pending, matched, previous, taken));
      if (found == null) {
        return null;
      }
      for (Node other : pending) {
        if (other != peer && !matched.containsKey(other)
            && overlaps(other.content, found.content)
            && !crosses(other, found, pending, matched, previous, taken)) {
          return null;
        }
      }
      return found;
    }

    /**
     * The one untaken previous JSON branch that {@code test} accepts.
     */
    private static Node previousBranch(List<Node> previous, Set<Node> taken,
        Predicate<Node> test) {
      return sole(previous, p -> !taken.contains(p) && p.content != null && test.test(p));
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
     * Whether a peer and a previous branch disagree on having a discriminator that another branch
     * related to them has: the peer's own, where an untaken previous branch has it, or the previous
     * branch's, where an unresolved peer has it. That branch, not this one, is the counterpart.
     */
    private static boolean crosses(Node peer, Node candidate, List<Node> pending,
        Map<Node, Node> matched, List<Node> previous, Set<Node> taken) {
      Set<String> mine = peer.content;
      Set<String> theirs = candidate.content;
      for (String key : discriminatorKeys(mine)) {
        if (discriminatorKeys(theirs).contains(key)) {
          continue;
        }
        for (Node p : previous) {
          boolean other = p != candidate && !taken.contains(p) && p.content != null;
          if (other && related(mine, p.content) && discriminatorKeys(p.content).contains(key)) {
            return true;
          }
        }
      }
      for (String key : discriminatorKeys(theirs)) {
        if (discriminatorKeys(mine).contains(key)) {
          continue;
        }
        for (Node other : pending) {
          boolean unresolved = other != peer && !matched.containsKey(other);
          if (unresolved && related(other.content, theirs)
              && discriminatorKeys(other.content).contains(key)) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * The paths of a branch's discriminators, each as {@code d:path=}.
     */
    private static Set<String> discriminatorKeys(Set<String> content) {
      Set<String> keys = new HashSet<>();
      for (String entry : content) {
        if (entry.startsWith(DISCRIMINATOR)) {
          keys.add(entry.substring(0, entry.indexOf('=', DISCRIMINATOR.length()) + 1));
        }
      }
      return keys;
    }

    /** Whether two branches share a member other than a discriminator, whatever their values. */
    private static boolean related(Set<String> mine, Set<String> theirs) {
      for (String entry : mine) {
        String key = DISCRIMINATOR + entry.substring(MEMBER.length()) + "=";
        if (entry.startsWith(MEMBER) && theirs.contains(entry)
            && !discriminatorKeys(mine).contains(key) && !discriminatorKeys(theirs).contains(key)) {
          return true;
        }
      }
      return false;
    }

    /**
     * A JSON union branch's content: its members' names, with the value of a one-value enum
     * (a discriminator), or its type for a branch with no members; null for anything else.
     */
    private Set<String> contentOf(Node node) {
      if (policy != IdentityPolicy.JSON || node.kind != Kind.BRANCH) {
        return null;
      }
      Set<String> content = new TreeSet<>();
      addContent(node.body, "", 0, new HashSet<>(), content);
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

    // -------------------------------------------------------------------------------------
    // Protobuf number derivation
    // -------------------------------------------------------------------------------------

    private Integer numberOf(Integer recorded, Object member, Map<Object, Integer> derived) {
      if (policy != IdentityPolicy.PROTOBUF) {
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
     * mirrors that rule exactly rather than guessing.
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
  }
}
