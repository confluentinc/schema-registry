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
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
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
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

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
 * {@code {key}}, {@code {value}}) are part of that scope. A location whose type changes kind —
 * a scalar, struct, union, array, multiset or map becoming another, or what a collection holds
 * doing so — is new, with everything under it, however it was matched: no SQL {@code ALTER}
 * expresses the change. Nothing absent from the
 * previous version is ever continued, so a range of versions pairs its versions exactly as the
 * whole history does.
 *
 * <p>The rules are format-specific and a {@code LogicalType} carries no format discriminator, so
 * each version comes with its {@link SchemaType}. Versions of different schema types match
 * nothing: a history that changes format starts every location afresh at the change.
 *
 * <h2>Named types</h2>
 *
 * <p>Named types are matched where they are used: each use — a field's, an element's, a union
 * branch's — is a location of its own, under the location using it, and it scopes the type's
 * members. A type merged, split or swapped by aliases therefore keeps every location's lineage,
 * and each use of a shared type has its own ids. Under JSON's rules a named type is
 * transparent, as if inlined. A recursive type has no finite set of uses, and is rejected. A root
 * that refers to a named type — as a converter keeps a root record or message naming types inside
 * it — is walked through. The root's name matters only for a Protobuf message: another message at
 * the root, as one placed first in its file, is a different entity, and everything under it
 * restarts.
 *
 * <p>The walk is the same for every format; only matching a peer group differs, by one matcher per
 * format. A match on evidence weaker than a name or number — an Avro short name or promotion
 * family, a JSON branch's content — needs exactly one candidate on each side.
 *
 * <h2>Names and aliases</h2>
 *
 * <p>Under Avro rules, as Avro's own decoder applies them, a peer group is matched as a whole. A
 * peer continues the previous location of its own name, or the one an alias names; an alias names
 * what the previous version called a location, never one of its aliases nor any older name. An
 * explicit alias wins, as Avro renames a writer field to the reader field aliasing it even when one
 * of that name exists. A peer continuing itself may carry its aliases forward, but not claim
 * another location with a new one, unless the peers' new aliases permute their names: a swap or
 * rotation, read as the aliases intend. Each previous location has at most one continuation and
 * each peer continues at most one; where Avro itself cannot say which, the history is ambiguous.
 *
 * <h2>Allocation</h2>
 *
 * <p>Ids are allocated walking versions in order and, within a version, members in path order —
 * the pre-order walk — taking the next integer for each member matched to nothing.
 */
public final class ProvenanceComputer {

  /** The most locations one version may have; past it, the history has no provenance. */
  public static final int MAX_LOCATIONS = 100_000;

  /** The most locations a whole report may hold, as every version's are kept at once. */
  public static final int MAX_REPORT_LOCATIONS = 500_000;

  // The name V1 gives an unhinted JSON union branch, followed by its position.
  private static final String POSITIONAL_BRANCH = "connect_union_field_";
  // A JSON branch's content entries: a member's path, a discriminator's value, a scalar's type.
  private static final String MEMBER = "m:";
  private static final String DISCRIMINATOR = "d:";
  private static final String TYPE = "t:";
  // JSON's numeric scalars, narrowest first: a primitive branch continues one it widens from.
  private static final List<String> JSON_NUMERIC =
      Arrays.asList("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE");
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
   * As {@link #report(List, List)}, every version of the one schema type.
   */
  public static ProvenanceReport report(SchemaType schemaType, List<LogicalType> versions) {
    Objects.requireNonNull(schemaType, "schemaType");
    Objects.requireNonNull(versions, "versions");
    return report(Collections.nCopies(versions.size(), schemaType), versions);
  }

  /**
   * Every version's members with their provenance ids — the form a provenance endpoint serves.
   *
   * @param schemaTypes each version's schema type, in the same order as {@code versions}
   * @param versions the schema versions in chronological order
   * @throws IllegalArgumentException if the lists differ in size, a version or schema type is
   *     null, or an entity has no name
   * @throws AmbiguousProvenanceException if names and aliases determine no single match
   * @throws RecursiveTypeException for a recursive type
   * @throws TooManyLocationsException if a version has more than {@link #MAX_LOCATIONS}, or the
   *     history more than {@link #MAX_REPORT_LOCATIONS}
   */
  public static ProvenanceReport report(List<SchemaType> schemaTypes,
      List<LogicalType> versions) {
    Objects.requireNonNull(schemaTypes, "schemaTypes");
    Objects.requireNonNull(versions, "versions");
    if (versions.size() != schemaTypes.size()) {
      throw new IllegalArgumentException("Expected one schema type per version, got "
          + schemaTypes.size() + " schema types for " + versions.size() + " versions");
    }
    List<ProvenanceReport.Version> reported = new ArrayList<>(versions.size());
    Node previous = null;
    SchemaType previousSchemaType = null;
    String previousRootMessage = null;
    int nextId = 1;
    int locations = 0;
    for (int version = 0; version < versions.size(); version++) {
      LogicalType logicalType = versions.get(version);
      if (logicalType == null) {
        throw new IllegalArgumentException("Null LogicalType at version " + version);
      }
      SchemaType schemaType = schemaTypes.get(version);
      if (schemaType == null) {
        throw new IllegalArgumentException("Null SchemaType at version " + version);
      }

      // A Protobuf root is a named message: another message at the root, as one placed first in
      // its file, is a different entity, so everything under it restarts.
      String rootMessage = schemaType == SchemaType.PROTOBUF
          ? ProtoToLogicalTypeConverter.rootMessage(logicalType) : null;
      boolean continues = schemaType == previousSchemaType
          && Objects.equals(rootMessage, previousRootMessage);
      Walk walk = new Walk(version, schemaType, logicalType);
      final Node root = walk.walk(continues ? previous : null);
      locations += walk.members.size();
      if (locations > MAX_REPORT_LOCATIONS) {
        throw new TooManyLocationsException(MAX_REPORT_LOCATIONS);
      }
      List<ProvenanceReport.Member> members = new ArrayList<>(walk.members.size());
      for (Node member : walk.members) {
        member.id = member.match != null ? member.match.id : nextId++;
        members.add(new ProvenanceReport.Member(
            member.where.path, member.where.names, spelled(member.kinds), member.id));
      }
      reported.add(new ProvenanceReport.Version(version, walk.rootKind(), members));
      previous = root;
      previousSchemaType = schemaType;
      previousRootMessage = rootMessage;
    }
    return new ProvenanceReport(reported, nextId - 1);
  }


  // -----------------------------------------------------------------------------------------
  // Locations
  // -----------------------------------------------------------------------------------------

  private enum Role {
    /** One use of a named type at one location: an Avro record, enum or fixed, or a message. */
    NAMED_TYPE,
    FIELD,
    BRANCH
  }

  /**
   * What a location's type is, references resolved. A change between kinds has no SQL
   * {@code ALTER} — Iceberg allows none, and Flink reads a multiset as neither a list nor a map —
   * so it is a drop and an add.
   */
  private enum Kind {
    /**
     * A type with no locations under it: a primitive, an enum, a fixed, or a variant, whose
     * nested values the logical type gives no members.
     */
    SCALAR,
    STRUCT,
    UNION,
    ARRAY,
    MULTISET,
    MAP
  }

  /**
   * {@code kinds}, as a walk's {@code kindsOf} lists them, spelled for the report:
   * {@code ARRAY<k>}, {@code MULTISET<k>} and {@code MAP<k, k>} name what they hold. Equal lists
   * spell alike.
   */
  private static String spelled(List<Kind> kinds) {
    StringBuilder spelled = new StringBuilder();
    spell(kinds, 0, spelled);
    return spelled.toString();
  }

  // Spells the kind at kinds[at] and what it holds; returns the index after them.
  private static int spell(List<Kind> kinds, int at, StringBuilder spelled) {
    if (at >= kinds.size()) {
      return at;
    }
    Kind kind = kinds.get(at);
    spelled.append(kind.name());
    int next = at + 1;
    if (kind == Kind.ARRAY || kind == Kind.MULTISET) {
      spelled.append('<');
      next = spell(kinds, next, spelled);
      spelled.append('>');
    } else if (kind == Kind.MAP) {
      spelled.append('<');
      next = spell(kinds, next, spelled);
      spelled.append(", ");
      next = spell(kinds, next, spelled);
      spelled.append('>');
    }
    return next;
  }

  /** One location of one version: what it is, where it is, and what it matched. */
  private static final class Node {

    private final Role role;
    private final String name;
    private final List<String> aliases;
    private final Integer number;
    private final Schema body;
    /** Derived Protobuf numbers to hand to this node's own members, if any. */
    private final Map<Object, Integer> childDerived;
    private final Where where;

    /**
     * A Protobuf oneof's member numbers, a JSON branch's content and title; null for anything
     * else.
     */
    private Set<Integer> memberNumbers;
    private Set<String> content;
    // The content's discriminator keys, each as d:path=, kept for the crossing test.
    private Set<String> keys;
    private String title;

    /** This node's member groups, keyed by the collection steps leading to each. */
    private final Map<String, List<Node>> groups = new HashMap<>();
    private List<Kind> kinds;
    private Node match;
    private int id;

    Node(Role role, String name, List<String> aliases, Integer number, Schema body,
        Map<Object, Integer> childDerived, Where where) {
      this.role = role;
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
     * The previous version's members of this node's match at {@code step}, of one role.
     */
    List<Node> previous(String step, Role role) {
      List<Node> group = match != null ? match.groups.get(step) : null;
      return group != null && group.get(0).role == role ? group : Collections.emptyList();
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
    private final SchemaType schemaType;
    private final LogicalType logicalType;
    private final Map<String, Schema> namedTypes;

    /** The members, fields and branches, in path order. */
    private final List<Node> members = new ArrayList<>();
    /** Named types being walked through; a repeat is a recursive type. */
    private final Set<String> inlining = new HashSet<>();

    Walk(int version, SchemaType schemaType, LogicalType logicalType) {
      this.version = version;
      this.schemaType = schemaType;
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
      if (schema.getType() == Schema.Type.NAMED_TYPE_REF) {
        // The converter keeps a root record or message as a reference while types are nested in
        // it or a peer uses it. It is still the root, walked through.
        String name = schema.getQualifiedName();
        Schema body = namedTypes.get(name);
        walkNamed(name, () -> processType(body, root, "", where.through(body),
            Collections.emptyMap()));
      } else {
        processType(schema, root, "", where, Collections.emptyMap());
      }
      return root;
    }

    /** Matches one peer group, then descends into each peer's own type. */
    private void processGroup(List<Node> peers, Node owner, String step) {
      if (peers.isEmpty()) {
        return;
      }
      owner.groups.put(step, peers);
      for (Node peer : peers) {
        peer.kinds = kindsOf(peer.body);
      }
      match(peers, owner.previous(step, peers.get(0).role));
      for (Node peer : peers) {
        if (peer.match != null && !peer.match.kinds.equals(peer.kinds)) {
          // Matched, but its type changed kind: it, and all under it, are new.
          peer.match = null;
        }
      }
      for (Node peer : peers) {
        if (peer.role != Role.NAMED_TYPE) {
          members.add(peer);
          // Each use of a shared type is a location, so they can multiply with depth.
          if (members.size() > MAX_LOCATIONS) {
            throw new TooManyLocationsException(version, MAX_LOCATIONS);
          }
        }
        processType(peer.body, peer, "", peer.where, peer.childDerived);
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
          if (schemaType == SchemaType.JSON) {
            // Walked as if inlined, so a reference changes nothing.
            walkNamed(name, () -> processType(named, owner, step, where.through(named), derived));
          } else if (named != null) {
            walkNamed(name, () -> processGroup(Collections.singletonList(new Node(
                Role.NAMED_TYPE, name, typeAliases(named), null, named, Collections.emptyMap(),
                where.through(named))), owner, step));
          }
          break;
        }
        default:
          // Primitives and enums have no members.
          break;
      }
    }

    /**
     * The kind of {@code schema}, then of what each collection holds: a list of structs becoming
     * a list of strings changes kind as a struct becoming a string does.
     */
    private List<Kind> kindsOf(Schema schema) {
      List<Kind> kinds = new ArrayList<>();
      addKinds(schema, kinds, Collections.newSetFromMap(new IdentityHashMap<>()));
      return kinds;
    }

    /**
     * The root's kind, spelled; null when the logical type has no root.
     */
    String rootKind() {
      Schema root = logicalType.getRootSchema();
      return root == null ? null : spelled(kindsOf(root));
    }

    private void addKinds(Schema schema, List<Kind> kinds, Set<Schema> seen) {
      Schema type = resolved(schema);
      if (type != null && !seen.add(type)) {
        // A collection holding itself: walkNamed reports the recursion.
        return;
      }
      Kind kind = kindOf(type);
      kinds.add(kind);
      if (kind == Kind.ARRAY || kind == Kind.MULTISET) {
        addKinds(type.getElementType(), kinds, seen);
      } else if (kind == Kind.MAP) {
        addKinds(type.getKeyType(), kinds, seen);
        addKinds(type.getValueType(), kinds, seen);
      }
      // Only what holds it: a type met again beside itself, as a map's key and value, is no cycle.
      if (type != null) {
        seen.remove(type);
      }
    }

    private static Kind kindOf(Schema type) {
      if (type == null) {
        return Kind.SCALAR;
      }
      switch (type.getType()) {
        case STRUCT:
          return Kind.STRUCT;
        case UNION:
          return Kind.UNION;
        case ARRAY:
          return Kind.ARRAY;
        case MULTISET:
          return Kind.MULTISET;
        case MAP:
          return Kind.MAP;
        default:
          return Kind.SCALAR;
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
          nodes.add(new Node(Role.FIELD, field.getName(), field.getAliases(),
              numberOf(field.getFieldNumber(), field, derived), field.getSchema(), derived,
              where.descend(field.getSchema(), i, field.getNativeNames())));
        }
      } else {
        List<UnionBranch> branches = container.getBranches();
        for (int i = 0; i < branches.size(); i++) {
          UnionBranch branch = branches.get(i);
          Node node = new Node(Role.BRANCH, branchName(branch), branchAliases(branch),
              numberOf(branch.getFieldNumber(), branch, enclosingDerived), branch.getSchema(),
              enclosingDerived, where.descend(branch.getSchema(), i, branch.getNativeNames()));
          if (schemaType == SchemaType.JSON) {
            node.title = branch.getNativeTitle();
          }
          nodes.add(node);
        }
      }
      for (Node node : nodes) {
        node.memberNumbers = memberNumbersOf(node);
        node.content = contentOf(node);
        node.keys = node.content != null ? discriminatorKeys(node.content) : null;
      }
      return nodes;
    }

    // -------------------------------------------------------------------------------------
    // Matching one peer group
    // -------------------------------------------------------------------------------------

    /**
     * Matches a whole peer group at once against {@code previous}, the previous version's members
     * of the parent's match, by the rules of the version's format.
     */
    private void match(List<Node> peers, List<Node> previous) {
      for (Node peer : peers) {
        if (peer.name == null) {
          throw new IllegalArgumentException(
              "Entity at " + peer.where.path + " has no name (version " + version + ")");
        }
      }
      Map<Node, Node> matched = new IdentityHashMap<>();
      switch (schemaType) {
        case AVRO:
          matchAvro(peers, previous, matched);
          break;
        case PROTOBUF:
          matchProtobuf(peers, previous, matched);
          break;
        default:
          matchJson(peers, previous, matched);
          break;
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
              + "identity at version ", version, ": " + key + " (at " + peer.where.path + ")");
        }
      }
    }

    /**
     * Avro: by name and alias, then by short name for a named type, and for a primitive branch by
     * its promotion family or, failing that, as Avro's own union resolution promotes it.
     */
    private void matchAvro(List<Node> peers, List<Node> previous, Map<Node, Node> matched) {
      arbitrate(peers, previous, matched);
      Set<Node> taken = Collections.newSetFromMap(new IdentityHashMap<>());
      taken.addAll(matched.values());
      for (Node peer : peers) {
        if (matched.containsKey(peer)) {
          continue;
        }
        Node continued = isNamedAvroType(peer) ? shortNameContinuation(peer, peers, previous)
            : peer.role == Role.BRANCH ? promotionContinuation(peer, peers, previous, taken)
            : null;
        if (continued != null && taken.add(continued)) {
          matched.put(peer, continued);
        }
      }
    }

    /**
     * Protobuf: a field by number, a message by name, and a oneof by its members' numbers, which a
     * renamed oneof keeps; a oneof split in two is kept by one part. A oneof sharing members with
     * several previous ones, as when two merge, then takes the one it shares the most with of
     * those no other oneof kept.
     */
    private static void matchProtobuf(List<Node> peers, List<Node> previous,
        Map<Node, Node> matched) {
      // By number, indexed: a message may hold tens of thousands of fields. A number held twice
      // matches neither, as sole decides.
      Map<Integer, Node> byNumber = new HashMap<>();
      Set<Integer> repeated = new HashSet<>();
      for (Node p : previous) {
        if (p.number != null && byNumber.put(p.number, p) != null) {
          repeated.add(p.number);
        }
      }
      List<Node> overlapping = new ArrayList<>();
      for (Node peer : peers) {
        Node found = peer.memberNumbers != null
            ? sole(previous, p -> sharesMembers(p, peer))
            : peer.number != null
            ? (repeated.contains(peer.number) ? null : byNumber.get(peer.number))
            : sole(previous, p -> p.number == null && p.memberNumbers == null
                && peer.name.equals(p.name));
        if (found != null) {
          matched.put(peer, found);
        } else if (peer.memberNumbers != null) {
          overlapping.add(peer);
        }
      }
      settleOneofs(peers, matched);
      // Until none is left to take: a peer losing its pick tries the next, and each round keeps
      // one more previous oneof, so it ends.
      boolean picked = true;
      while (picked) {
        picked = false;
        Set<Node> kept = Collections.newSetFromMap(new IdentityHashMap<>());
        kept.addAll(matched.values());
        for (Node peer : overlapping) {
          if (matched.containsKey(peer)) {
            continue;
          }
          Node best = null;
          for (Node p : previous) {
            if (!kept.contains(p) && sharesMembers(p, peer)
                && (best == null || keepsOneof(p, best, peer.memberNumbers))) {
              best = p;
            }
          }
          if (best != null) {
            matched.put(peer, best);
            picked = true;
          }
        }
        settleOneofs(peers, matched);
      }
    }

    private static boolean sharesMembers(Node previous, Node peer) {
      return previous.memberNumbers != null
          && !Collections.disjoint(previous.memberNumbers, peer.memberNumbers);
    }

    /** JSON: a property by name; a union branch by hint, discriminators, title, then content. */
    private static void matchJson(List<Node> peers, List<Node> previous,
        Map<Node, Node> matched) {
      if (peers.get(0).role == Role.BRANCH) {
        matchJsonBranches(peers, previous, matched);
        return;
      }
      // By name, indexed: an object may hold tens of thousands of properties. A name held twice
      // matches neither, as sole decides.
      Map<String, Node> byName = new HashMap<>();
      Set<String> repeated = new HashSet<>();
      for (Node p : previous) {
        if (byName.put(p.name, p) != null) {
          repeated.add(p.name);
        }
      }
      for (Node peer : peers) {
        Node found = repeated.contains(peer.name) ? null : byName.get(peer.name);
        if (found != null) {
          matched.put(peer, found);
        }
      }
    }

    /**
     * The one previous location {@code related} relates {@code peer} to, where no other of
     * {@code peers} is related to it too: a match on evidence weaker than a name or number needs
     * exactly one candidate on each side.
     */
    private static Node mutual(Node peer, List<Node> peers, List<Node> previous,
        BiPredicate<Node, Node> related) {
      Node found = sole(previous, p -> related.test(peer, p));
      if (found == null) {
        return null;
      }
      for (Node other : peers) {
        if (other != peer && related.test(other, found)) {
          return null;
        }
      }
      return found;
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
     * forward, but a new one naming another location would make one continue two, unless the
     * peers' new aliases permute their names: fields or types swapped or rotated, which Avro's
     * decoder reads by the aliases. Each previous location has at most one continuation, and each
     * peer continues at most one.
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
                + " at version ", version, " at " + peer.where.path);
          }
          own.put(peer, p);
        }
      }

      // A peer whose own name another peer newly aliases yields it. One yielding without naming
      // another by a new alias of its own fails below, so only whole permutations go through.
      Map<Node, Node> before = new IdentityHashMap<>(own);
      // The own-matched peers newly aliasing each name, indexed once: a group can be wide.
      Map<String, List<Node>> newClaims = new HashMap<>();
      for (Node other : peers) {
        Node theirs = before.get(other);
        if (theirs != null) {
          for (String alias : other.aliases) {
            if (!theirs.aliases.contains(alias)) {
              newClaims.computeIfAbsent(alias, k -> new ArrayList<>()).add(other);
            }
          }
        }
      }
      for (Node peer : peers) {
        Node mine = before.get(peer);
        List<Node> claimants = mine != null ? newClaims.get(mine.name) : null;
        if (claimants != null && claimants.stream().anyMatch(other -> other != peer)) {
          ownClaimant.remove(own.remove(peer));
        }
      }

      Map<Node, Set<Node>> aliasClaimants = new LinkedHashMap<>();
      for (Node peer : peers) {
        Node mine = own.get(peer);
        Node was = before.get(peer);
        for (String alias : peer.aliases) {
          Node aliased = byPreviousName.get(alias);
          if (aliased == null || aliased == was) {
            continue;
          }
          if (was != null && was.aliases.contains(alias)) {
            // Carried forward from the version that introduced it.
            continue;
          }
          if (mine != null) {
            throw new AmbiguousProvenanceException("Ambiguous identity resolution at version ",
                version, ": " + peer.where.path + " continues " + mine.name
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
          throw new AmbiguousProvenanceException("Ambiguous identity resolution at version ",
              version, ": multiple entities claim " + p.name + " via aliases");
        }
        Node winner = byAlias.isEmpty() ? ownClaimant.get(p) : byAlias.iterator().next();
        Node earlier = matched.put(winner, p);
        if (earlier != null) {
          throw new AmbiguousProvenanceException("Ambiguous identity resolution at version ",
              version, ": " + winner.where.path + " matches both " + earlier.name + " and "
              + p.name);
        }
      }
    }

    /**
     * The previous named type a named Avro type continues when neither its name nor an alias
     * does: the one with the same short name, as Avro's checker compares names. A namespace
     * changed without an alias — as nested types inheriting a renamed root's namespace are — then
     * keeps its members.
     */
    private static Node shortNameContinuation(Node peer, List<Node> peers, List<Node> previous) {
      return mutual(peer, peers, previous, (a, p) -> isNamedAvroType(a)
          && shortName(a.name).equals(shortName(p.name)));
    }

    /**
     * The previous branch an unnamed Avro branch promotes from: the one of its promotion family,
     * where each union has one; else the one Avro's own resolution reads into this branch.
     */
    private static Node promotionContinuation(Node peer, List<Node> peers, List<Node> previous,
        Set<Node> taken) {
      Node family = mutual(peer, peers, previous, (a, p) -> familyOf(a.name) != null
          && familyOf(a.name).contains(p.name));
      if (family != null || familyOf(peer.name) == null) {
        return family;
      }
      return sole(previous, w -> !taken.contains(w) && avroReaderBranch(w, peers) == peer);
    }

    /**
     * The branch of {@code peers} Avro reads a primitive {@code writer} branch into when none has
     * its type: the first, in union order, its type promotes to, as
     * {@code Resolver.ReaderUnion.firstMatchingBranch} chooses. A branch of its type continues it
     * by name.
     */
    private static Node avroReaderBranch(Node writer, List<Node> peers) {
      for (Node branch : peers) {
        if (promotes(writer.name, branch.name)) {
          return branch;
        }
      }
      return null;
    }

    /**
     * Avro's promotions: int to long, float or double; long to float or double; float to double;
     * string to bytes and back.
     */
    private static boolean promotes(String writer, String reader) {
      switch (writer) {
        case "int":
          return reader.equals("long") || reader.equals("float") || reader.equals("double");
        case "long":
          return reader.equals("float") || reader.equals("double");
        case "float":
          return reader.equals("double");
        case "string":
          return reader.equals("bytes");
        case "bytes":
          return reader.equals("string");
        default:
          return false;
      }
    }

    /**
     * A named type's use, or a union branch of a named type: a record, an enum or a fixed, whose
     * branch the converter names by its full name rather than a type name.
     */
    private static boolean isNamedAvroType(Node peer) {
      return peer.role == Role.NAMED_TYPE || peer.role == Role.BRANCH
          && (isReference(peer.body) || !AVRO_UNNAMED.contains(peer.name));
    }

    private static boolean isReference(Schema schema) {
      return schema != null && schema.getType() == Schema.Type.NAMED_TYPE_REF;
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

    /** True for an Avro branch holding a named type, whose name and aliases are its type's. */
    private boolean isNamedAvroBranch(Schema body) {
      return schemaType == SchemaType.AVRO && isReference(body);
    }

    /**
     * A branch's name for matching. An Avro branch is named as Avro finds it — a named type's full
     * name, a primitive's type name — which the converter records as its native step. The logical
     * type's own name for it is shortened where it can be, lengthened where simple names collide,
     * and replaced by any hint, so a branch would change match with its siblings or its hint.
     */
    private String branchName(UnionBranch branch) {
      if (schemaType == SchemaType.AVRO) {
        if (isNamedAvroBranch(branch.getSchema())) {
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
      if (!isNamedAvroBranch(branch.getSchema())) {
        // A fixed has no named type; the converter records its aliases on the branch.
        List<String> recorded = schemaType == SchemaType.AVRO ? branch.getNativeAliases() : null;
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
      if (schemaType != SchemaType.AVRO || named.getAliases() == null) {
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
      if (schemaType != SchemaType.PROTOBUF || node.role != Role.FIELD
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
     * previous branch of its name; a branch continues the one previous branch with the same
     * top-level discriminators, where no unpaired peer has them, as a tagged union's tag names
     * its branch; the one previous branch of its title, where no unpaired peer has it and no
     * discriminator conflicts — a title only documents in V1, so one changed only leaves the
     * branch to the phases below; the one previous branch of the same content, where no unpaired
     * peer shares it; the previous branch it shares strictly the most members with, and it with
     * that one, a member every untaken previous branch has (or, with one left, every previous
     * branch had) aside, else the last previous branch it alone overlaps, and no conflicting
     * discriminator, as when it moved and its members changed, repeated while it pairs any; for
     * a primitive numeric branch, the one previous primitive branch it widens from (integer to
     * number), each the other's only such pairing, as a property's scalar change continues; one
     * at the same position sharing a member with it, where overlap alone cannot tell; else it is
     * new. None continues another across a discriminator a branch related
     * to them has (see {@link #crosses}), nor across a hint: two branches hinted otherwise are
     * different branches, as an Avro type renamed without an alias is.
     */
    private static void matchJsonBranches(List<Node> peers, List<Node> previous,
        Map<Node, Node> matched) {
      Set<Node> taken = Collections.newSetFromMap(new IdentityHashMap<>());
      for (int phase = 0; phase < 7; phase++) {
        boolean progressed = false;
        for (Node peer : peers) {
          if (matched.containsKey(peer)) {
            continue;
          }
          Node found;
          if (phase == 0) {
            found = peer.name.startsWith(POSITIONAL_BRANCH)
                ? null : previousBranch(previous, taken, p -> peer.name.equals(p.name));
          } else if (phase == 1) {
            found = tags(peer.content).isEmpty() ? null : mutual(peer,
                unresolved(peers, matched), previous,
                (a, p) -> !taken.contains(p) && tags(a.content).equals(tags(p.content))
                    && !otherHints(a, p));
          } else if (phase == 2) {
            found = peer.title == null ? null : mutual(peer, unresolved(peers, matched),
                previous,
                (a, p) -> !taken.contains(p) && Objects.equals(a.title, p.title)
                    && !otherHints(a, p) && !namesOtherwise(a.content, p.content));
          } else if (phase == 3) {
            found = mutual(peer, unresolved(peers, matched), previous,
                (a, p) -> !taken.contains(p) && a.content.equals(p.content) && !otherHints(a, p));
          } else if (phase == 4) {
            List<Node> unresolved = unresolved(peers, matched);
            Set<String> envelope = envelope(previous, taken);
            found = mostShared(peer, unresolved, previous, envelope, (a, p) -> !taken.contains(p)
                && overlapsBeyond(a.content, p.content, envelope) && !otherHints(a, p)
                && !crosses(a, p, peers, matched, previous, taken));
            if (found == null && previous.stream().filter(p -> !taken.contains(p)).count() == 1) {
              // The last previous branch, and it alone relates to it: linked, if only by a member
              // every branch had.
              found = mutual(peer, unresolved, previous, (a, p) -> !taken.contains(p)
                  && overlaps(a.content, p.content) && !otherHints(a, p)
                  && !crosses(a, p, peers, matched, previous, taken));
            }
          } else if (phase == 5) {
            found = numericType(peer.content) == null ? null : mutual(peer,
                unresolved(peers, matched), previous,
                (a, p) -> !taken.contains(p) && widens(p.content, a.content) && !otherHints(a, p));
          } else {
            found = previousBranch(previous, taken, p -> peer.name.equals(p.name)
                && overlaps(peer.content, p.content)
                && !crosses(peer, p, peers, matched, previous, taken));
          }
          if (found != null) {
            taken.add(found);
            matched.put(peer, found);
            progressed = true;
          }
        }
        if (phase == 4 && progressed) {
          // A pairing made here may settle a peer passed over before it, as when it leaves one
          // previous branch: until none is made.
          phase--;
        }
      }
    }

    // Whether a primitive branch of content mine widens, as a JSON value, into one of theirs.
    private static boolean widens(Set<String> mine, Set<String> theirs) {
      String from = numericType(mine);
      String to = numericType(theirs);
      return from != null && to != null
          && JSON_NUMERIC.indexOf(from) < JSON_NUMERIC.indexOf(to);
    }

    // A primitive branch's numeric type, its content's one entry; null for any other branch.
    private static String numericType(Set<String> content) {
      if (content == null || content.size() != 1) {
        return null;
      }
      String entry = content.iterator().next();
      String type = entry.startsWith(TYPE) ? entry.substring(TYPE.length()) : null;
      return JSON_NUMERIC.contains(type) ? type : null;
    }

    // The peers not yet paired: one paired already cannot take another previous branch.
    private static List<Node> unresolved(List<Node> peers, Map<Node, Node> matched) {
      List<Node> unresolved = new ArrayList<>();
      for (Node peer : peers) {
        if (!matched.containsKey(peer)) {
          unresolved.add(peer);
        }
      }
      return unresolved;
    }

    /**
     * A branch's own discriminators, each as {@code d:name=value}: those of its members, not of
     * branches nested in them.
     */
    private static Set<String> tags(Set<String> content) {
      Set<String> tags = new TreeSet<>();
      for (String entry : content) {
        // A step before the value's "=": the value itself may hold a "/".
        if (entry.startsWith(DISCRIMINATOR) && unescaped(entry, '/', keyEnd(entry)) < 0) {
          tags.add(entry);
        }
      }
      return tags;
    }

    /** Whether two branches are both hinted, and hinted otherwise. */
    private static boolean otherHints(Node a, Node p) {
      return !a.name.startsWith(POSITIONAL_BRANCH) && !p.name.startsWith(POSITIONAL_BRANCH)
          && !a.name.equals(p.name);
    }

    /**
     * The previous branch sharing strictly the most members with {@code peer}, where {@code peer}
     * also shares strictly the most with it: a member every branch has tells nothing on its own.
     */
    private static Node mostShared(Node peer, List<Node> peers, List<Node> previous,
        Set<String> envelope, BiPredicate<Node, Node> related) {
      Node best = strictMax(previous,
          p -> related.test(peer, p) ? sharedMembers(peer, p, envelope) : -1);
      return best != null && strictMax(peers,
          a -> related.test(a, best) ? sharedMembers(a, best, envelope) : -1) == peer
          ? best : null;
    }

    // The one node scoring strictly highest of those scoring 0 or more; null on a tie or none.
    private static Node strictMax(List<Node> nodes, ToIntFunction<Node> score) {
      Node best = null;
      int top = -1;
      boolean tied = false;
      for (Node n : nodes) {
        int s = score.applyAsInt(n);
        if (s > top) {
          best = n;
          top = s;
          tied = false;
        } else if (s == top && s >= 0) {
          tied = true;
        }
      }
      return tied ? null : best;
    }

    private static int sharedMembers(Node a, Node p, Set<String> envelope) {
      int n = 0;
      for (String entry : a.content) {
        if (entry.startsWith(MEMBER) && p.content.contains(entry) && !envelope.contains(entry)) {
          n++;
        }
      }
      return n;
    }

    /**
     * The members every untaken previous branch has, where there are several, else every previous
     * branch had: shared by all, they tell none of them apart.
     */
    private static Set<String> envelope(List<Node> previous, Set<Node> taken) {
      Set<String> common = null;
      int untaken = 0;
      for (Node p : previous) {
        if (taken.contains(p)) {
          continue;
        }
        untaken++;
        Set<String> members = new HashSet<>();
        for (String entry : p.content) {
          if (entry.startsWith(MEMBER)) {
            members.add(entry);
          }
        }
        if (common == null) {
          common = members;
        } else {
          common.retainAll(members);
        }
      }
      if (untaken >= 2) {
        return common;
      }
      // One left, or none: what every previous branch had still tells nothing.
      return previous.size() < 2 || untaken == previous.size() ? Collections.emptySet()
          : envelope(previous, Collections.emptySet());
    }

    /**
     * As {@link #overlaps}, a member of {@code envelope} not counting.
     */
    private static boolean overlapsBeyond(Set<String> mine, Set<String> theirs,
        Set<String> envelope) {
      if (namesOtherwise(mine, theirs)) {
        return false;
      }
      if (mine.equals(theirs)) {
        return true;
      }
      return mine.stream().anyMatch(entry -> entry.startsWith(MEMBER) && theirs.contains(entry)
          && !envelope.contains(entry));
    }

    /**
     * The one untaken previous JSON branch that {@code test} accepts.
     */
    private static Node previousBranch(List<Node> previous, Set<Node> taken,
        Predicate<Node> test) {
      return sole(previous, p -> !taken.contains(p) && test.test(p));
    }

    /**
     * Whether two branches' contents share a member, with no discriminator of one named
     * differently by the other.
     */
    private static boolean overlaps(Set<String> mine, Set<String> theirs) {
      if (namesOtherwise(mine, theirs)) {
        return false;
      }
      if (mine.equals(theirs)) {
        // Nothing tells them apart, even with no members: as alike as they can be.
        return true;
      }
      return mine.stream().anyMatch(entry -> entry.startsWith(MEMBER) && theirs.contains(entry));
    }

    /** Whether a discriminator of one branch is named differently by the other. */
    private static boolean namesOtherwise(Set<String> mine, Set<String> theirs) {
      for (String entry : mine) {
        if (entry.startsWith(DISCRIMINATOR) && !theirs.contains(entry)) {
          String key = entry.substring(0, keyEnd(entry) + 1);
          if (theirs.stream().anyMatch(other -> other.startsWith(key))) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * Whether pairing a peer with a previous branch would cross a discriminator: one of the two
     * has a discriminator key the other lacks, and an alternative for it — another untaken previous
     * branch for the peer, another unresolved peer for the previous branch — shares a member with
     * it and has that key too. That alternative, not this pairing, is the counterpart.
     */
    private static boolean crosses(Node peer, Node candidate, List<Node> peers,
        Map<Node, Node> matched, List<Node> previous, Set<Node> taken) {
      if (candidate.keys.equals(peer.keys)) {
        // The same discriminator keys on both sides: neither has one the other lacks.
        return false;
      }
      List<Node> otherPrevious = new ArrayList<>();
      for (Node p : previous) {
        if (p != candidate && !taken.contains(p) && p.content != null) {
          otherPrevious.add(p);
        }
      }
      List<Node> otherPeers = new ArrayList<>();
      for (Node other : peers) {
        if (other != peer && !matched.containsKey(other)) {
          otherPeers.add(other);
        }
      }
      return hasCounterpart(peer, candidate, otherPrevious)
          || hasCounterpart(candidate, peer, otherPeers);
    }

    /**
     * Whether one of {@code alternatives} shares a member with {@code side} and has a
     * discriminator key {@code side} has and {@code other} lacks.
     */
    private static boolean hasCounterpart(Node side, Node other, List<Node> alternatives) {
      Set<String> missing = new HashSet<>(side.keys);
      missing.removeAll(other.keys);
      if (missing.isEmpty()) {
        // No key of side's that other lacks: no alternative can have one.
        return false;
      }
      for (Node alternative : alternatives) {
        // The key test first: it is the cheaper, and fails for most alternatives.
        if (!Collections.disjoint(alternative.keys, missing)
            && related(side.content, alternative.content)) {
          return true;
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
          keys.add(entry.substring(0, keyEnd(entry) + 1));
        }
      }
      return keys;
    }

    // A member name as a content step, escaped so that none reads as a path's structure.
    private static String contentStep(String name) {
      return name.replace("\\", "\\\\").replace("/", "\\/").replace("=", "\\=")
          .replace("[", "\\[").replace("{", "\\{");
    }

    // Where a discriminator's path ends: its first unescaped "=".
    private static int keyEnd(String entry) {
      return unescaped(entry, '=', entry.length());
    }

    // The first unescaped c in a content entry's path, before end; -1 if none.
    private static int unescaped(String entry, char c, int end) {
      for (int i = DISCRIMINATOR.length(); i < end; i++) {
        char at = entry.charAt(i);
        if (at == '\\') {
          i++;
        } else if (at == c) {
          return i;
        }
      }
      return -1;
    }

    /** Whether two branches share a member other than a discriminator, whatever their values. */
    private static boolean related(Set<String> mine, Set<String> theirs) {
      Set<String> myKeys = discriminatorKeys(mine);
      Set<String> theirKeys = discriminatorKeys(theirs);
      for (String entry : mine) {
        String key = DISCRIMINATOR + entry.substring(MEMBER.length()) + "=";
        if (entry.startsWith(MEMBER) && theirs.contains(entry)
            && !myKeys.contains(key) && !theirKeys.contains(key)) {
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
      if (schemaType != SchemaType.JSON || node.role != Role.BRANCH) {
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
            String path = prefix + contentStep(field.getName());
            content.add(MEMBER + path);
            if (isDiscriminator(type)) {
              content.add(DISCRIMINATOR + path + "=" + type.getEnumValues().get(0).getSymbol());
            }
            addContent(field.getSchema(), path + "/", depth + 1, new HashSet<>(naming), content);
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
      if (schemaType != SchemaType.PROTOBUF) {
        return null;
      }
      if (recorded != null) {
        return recorded;
      }
      return derived.get(member);
    }

    /**
     * The field numbers of a struct that records none, as the Protobuf converter implies them
     * ({@link ProtoToLogicalTypeConverter#impliedFieldNumbers}). Omission is itself the
     * information, so this mirrors that rule exactly rather than guessing.
     */
    private Map<Object, Integer> deriveNumbers(Schema struct) {
      return schemaType == SchemaType.PROTOBUF
          ? ProtoToLogicalTypeConverter.impliedFieldNumbers(struct) : Collections.emptyMap();
    }

    private static boolean isUnion(Schema schema) {
      return schema != null && schema.getType() == Schema.Type.UNION;
    }
  }
}
