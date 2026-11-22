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

package io.confluent.kafka.schemaregistry.type.logical.alter;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.provenance.AmbiguousProvenanceException;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceReport;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceReport.Member;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * The column ids Schema Registry's provenance gives the attributes of a version and of a new one
 * to follow it, computed here as the registry computes them. Ids compare only within one
 * computation.
 */
final class ColumnIds {

  /** A version as provenance reads it, and its members by path. */
  static final class Version {

    private final int number;
    private final LogicalType type;
    private final Map<List<Integer>, Member> members;

    Version(int number, LogicalType type, Map<List<Integer>, Member> members) {
      this.number = number;
      this.type = type;
      this.members = members;
    }

    int number() {
      return number;
    }

    LogicalType type() {
      return type;
    }

    Map<List<Integer>, Member> members() {
      return members;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Version)) {
        return false;
      }
      Version other = (Version) o;
      return Objects.equals(number, other.number)
          && Objects.equals(type, other.type)
          && Objects.equals(members, other.members);
    }

    @Override
    public int hashCode() {
      return Objects.hash(number, type, members);
    }


    Map<Integer, Member> byId() {
      Map<Integer, Member> byId = new HashMap<>();
      members.values().forEach(m -> byId.put(m.getId(), m));
      return byId;
    }

    /** The attribute at a path, its attribute and branch names joined by dots. */
    String name(List<Integer> path) {
      return nameFrom(steps(path), 0);
    }

    /**
     * The attribute at a path by its names in the deepest use on the path of a type known by
     * one of {@code names}, or null if it is in none.
     */
    String nameIn(List<Integer> path, Set<String> names) {
      List<Step> steps = steps(path);
      for (int i = steps.size() - 1; i >= 0; i--) {
        if (names.contains(steps.get(i).type())) {
          String name = nameFrom(steps, i);
          return name.isEmpty() ? null : name;
        }
      }
      return null;
    }

    /**
     * The deepest named type of {@code among} a path uses from step {@code from}, or null.
     */
    String deepest(List<Integer> path, int from, Set<String> among) {
      List<Step> steps = steps(path);
      for (int i = steps.size() - 1; i >= from; i--) {
        if (among.contains(steps.get(i).type())) {
          return steps.get(i).type();
        }
      }
      return null;
    }

    private static String nameFrom(List<Step> steps, int from) {
      return steps.subList(from, steps.size()).stream().map(Step::name)
          .filter(Objects::nonNull).collect(Collectors.joining("."));
    }

    // Each step of a path: the named type it is taken in, if any, and the name it takes.
    private List<Step> steps(List<Integer> path) {
      List<Step> steps = new ArrayList<>();
      Schema s = type.getRootSchema();
      for (int step : path) {
        final String named = s != null && s.getType() == Schema.Type.NAMED_TYPE_REF
            ? s.getQualifiedName() : null;
        s = resolved(s, type);
        if (s == null) {
          break;
        }
        String name = null;
        switch (s.getType()) {
          case STRUCT:
            name = s.getFields().get(step).getName();
            s = s.getFields().get(step).getSchema();
            break;
          case UNION:
            name = s.getBranches().get(step).getName();
            s = s.getBranches().get(step).getSchema();
            break;
          case MAP:
            s = step == 0 ? s.getKeyType() : s.getValueType();
            break;
          default:
            s = s.getElementType();
            break;
        }
        steps.add(new Step(named, name));
      }
      return steps;
    }
  }

  private static final class Step {

    private final String type;
    private final String name;

    Step(String type, String name) {
      this.type = type;
      this.name = name;
    }

    String type() {
      return type;
    }

    String name() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Step)) {
        return false;
      }
      Step other = (Step) o;
      return Objects.equals(type, other.type)
          && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, name);
    }
  }

  /**
   * The version edited and the new one, as the registry pairs them; and, for references the new
   * one moves to other versions, the version edited with them moved, the named types that
   * changed, and the new one's ids as paired through every version of those types between.
   */
  static final class Ids {

    private final Version base;
    private final Version next;
    private final LogicalType stepped;
    private final Map<String, Move> changed;
    private final Map<List<Integer>, Integer> followed;

    Ids(Version base,
        Version next,
        LogicalType stepped,
        Map<String, Move> changed,
        Map<List<Integer>, Integer> followed) {
      this.base = base;
      this.next = next;
      this.stepped = stepped;
      this.changed = changed;
      this.followed = followed;
    }

    Version base() {
      return base;
    }

    Version next() {
      return next;
    }

    LogicalType stepped() {
      return stepped;
    }

    Map<String, Move> changed() {
      return changed;
    }

    Map<List<Integer>, Integer> followed() {
      return followed;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Ids)) {
        return false;
      }
      Ids other = (Ids) o;
      return Objects.equals(base, other.base)
          && Objects.equals(next, other.next)
          && Objects.equals(stepped, other.stepped)
          && Objects.equals(changed, other.changed)
          && Objects.equals(followed, other.followed);
    }

    @Override
    public int hashCode() {
      return Objects.hash(base, next, stepped, changed, followed);
    }
  }

  /**
   * A reference moved from one version of a type to another, later or earlier, by its name in the
   * new version and in the one it follows, {@code was}.
   */
  static final class Move {

    private final String name;
    private final String subject;
    private final int from;
    private final int to;
    private final String was;

    Move(String name, String subject, int from, int to, String was) {
      this.name = name;
      this.subject = subject;
      this.from = from;
      this.to = to;
      this.was = was;
    }

    String name() {
      return name;
    }

    String subject() {
      return subject;
    }

    int from() {
      return from;
    }

    int to() {
      return to;
    }

    String was() {
      return was;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Move)) {
        return false;
      }
      Move other = (Move) o;
      return Objects.equals(name, other.name)
          && Objects.equals(subject, other.subject)
          && Objects.equals(from, other.from)
          && Objects.equals(to, other.to)
          && Objects.equals(was, other.was);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, subject, from, to, was);
    }
  }

  private ColumnIds() {
  }

  /**
   * The ids of a stored version and of a new one after it, as the registry would compute them
   * with it registered; null if the new one cannot be made as the registry makes it, left to it.
   * A refusal starts with {@code refused}, as "Type t cannot be changed so".
   */
  static Ids compute(TypeLookup lookup, String subject, SchemaMetadata base, SchemaType format,
             LogicalType next, int number, String refused) {
    ParsedSchema schema = nativeOf(subject, format, next);
    if (schema == null) {
      return null;
    }
    SchemaType baseType = schemaType(base.getSchemaType());
    LogicalType was = logicalType(refused, lookup.parse(base));
    LogicalType now = logicalType(refused, schema);
    Map<String, Move> changed = new HashMap<>();
    LogicalType edited = lookup.logicalType(subject, base);
    List<SchemaReference> refs = new ArrayList<>(edited.getReferences());
    List<Move> moves = moves(refs, next.getReferences());
    // References moved back: the version edited with them at the versions they move to, then
    // moved up one version at a time to its own, as their types moved.
    List<LogicalType> up = new ArrayList<>(List.of(was));
    List<Move> back = moves.stream().filter(m -> m.from() > m.to()).collect(Collectors.toList());
    if (!back.isEmpty()) {
      back.forEach(m -> at(refs, m, m.to()));
      up.set(0, stepped(lookup, subject, refused, format, edited, refs));
      back.forEach(m -> step(lookup, subject, refused, format, edited, refs, m, up, changed));
      up.set(up.size() - 1, was);
      back.forEach(m -> at(refs, m, m.to()));
    }
    // Then its references moved on one version at a time, as their types moved.
    List<LogicalType> lts = new ArrayList<>(List.of(up.get(0)));
    moves.stream().filter(m -> m.from() < m.to())
        .forEach(m -> step(lookup, subject, refused, format, edited, refs, m, lts, changed));
    lts.add(now);
    int last = lts.size() - 1;
    List<SchemaType> schemaTypes = new ArrayList<>();
    List<Integer> numbers = new ArrayList<>();
    for (int i = 0; i < last; i++) {
      schemaTypes.add(baseType);
      numbers.add(base.getVersion());
    }
    schemaTypes.add(schemaType(format.name()));
    numbers.add(number);
    ProvenanceReport report = report(refused, List.of(baseType, schemaTypes.get(last)),
        List.of(base.getVersion(), number), List.of(was, now));
    List<Map<List<Integer>, Member>> members = new ArrayList<>();
    for (ProvenanceReport.Version v : report.getVersions()) {
      members.add(byPath(v));
    }
    Map<List<Integer>, Integer> followed = new HashMap<>();
    if (!moves.isEmpty()) {
      // A new place follows the chain back to its first version, then that one's place up to
      // the version edited.
      List<ProvenanceReport.Version> chain = report(refused, schemaTypes, numbers, lts)
          .getVersions();
      Map<Integer, List<Integer>> first = new HashMap<>();
      chain.get(0).getMembers().forEach(m -> first.put(m.getId(), m.getPath()));
      Map<List<Integer>, List<Integer>> toBase = new HashMap<>();
      if (up.size() > 1) {
        List<ProvenanceReport.Version> rise = report(refused,
            Collections.nCopies(up.size(), baseType),
            Collections.nCopies(up.size(), base.getVersion()), up).getVersions();
        Map<Integer, List<Integer>> top = new HashMap<>();
        rise.get(rise.size() - 1).getMembers()
            .forEach(m -> top.put(m.getId(), m.getPath()));
        rise.get(0).getMembers().forEach(m -> toBase.put(m.getPath(), top.get(m.getId())));
      }
      chain.get(chain.size() - 1).getMembers().forEach(m -> {
        List<Integer> path = first.get(m.getId());
        Member own = members.get(0).get(up.size() > 1 ? toBase.get(path) : path);
        if (own != null) {
          followed.put(m.getPath(), own.getId());
        }
      });
    }
    return new Ids(new Version(base.getVersion(), was, members.get(0)),
        new Version(number, now, members.get(1)), lts.get(last - 1), changed, followed);
  }

  private static Map<List<Integer>, Member> byPath(ProvenanceReport.Version v) {
    Map<List<Integer>, Member> byPath = new LinkedHashMap<>();
    v.getMembers().forEach(m -> byPath.put(m.getPath(), m));
    return byPath;
  }

  // Adds to lts the version edited at each version a move passes through, noting the types it
  // changed.
  private static void step(TypeLookup lookup, String subject, String refused, SchemaType format,
                LogicalType edited, List<SchemaReference> refs, Move move,
                List<LogicalType> lts, Map<String, Move> changed) {
    Map<String, Schema> before = lts.get(lts.size() - 1).getNamedTypes();
    for (int version : versions(lookup, subject, move)) {
      at(refs, move, version);
      lts.add(stepped(lookup, subject, refused, format, edited, refs));
    }
    lts.get(lts.size() - 1).getNamedTypes().forEach((name, type) -> {
      if (!type.equals(before.get(name))) {
        changed.putIfAbsent(name, move);
      }
    });
  }

  private static void at(List<SchemaReference> refs, Move move, int version) {
    for (int i = 0; i < refs.size(); i++) {
      if (refs.get(i).getName().equals(move.was())) {
        refs.set(i, new SchemaReference(move.was(), move.subject(), version));
      }
    }
  }

  // The new version's native schema, null if its script is none the registry would parse, or
  // it cannot be made as the registry makes it.
  static ParsedSchema nativeOf(String subject, SchemaType format, LogicalType lt) {
    try {
      RegisterSchemaRequest request = Formats.request(format, lt);
      return LogicalTypeConversion.tryParse(request.getSchema())
          .map(script -> LogicalTypeConversion.toNative(script,
            new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject,
              request), lt.getResolvedReferences()))
          .orElse(null);
    } catch (ValidationException e) {
      return null;
    }
  }

  // The version edited with its references at the given versions, as provenance reads it.
  private static LogicalType stepped(TypeLookup lookup, String subject, String refused,
                     SchemaType format, LogicalType edited,
                     List<SchemaReference> refs) {
    LogicalType lt = new LogicalType(edited.getName(), edited.getNamespace(),
        edited.getRootSchema(), edited.getNamedTypes(), edited.getExternalTypes(),
        edited.getExternalImports(), refs, TypeReferences.resolve(lookup, subject, refs), Map.of());
    ParsedSchema schema = nativeOf(subject, format, lt);
    if (schema == null) {
      throw new TypeRefusedException(refused + ": Schema Registry's provenance cannot read it at "
          + "the versions of the types it refers to between those of its versions");
    }
    return logicalType(refused, schema);
  }

  /**
   * The references the new version moves to another version of their types. One continues that
   * of its subject and name, or else each of its subject under a name the new version drops: a
   * hand-written version may name a reference otherwise than its subject, or under two names.
   */
  static List<Move> moves(List<SchemaReference> was, List<SchemaReference> now) {
    List<Move> moves = new ArrayList<>();
    for (SchemaReference ref : now) {
      boolean named = was.stream().anyMatch(old -> same(old, ref));
      for (SchemaReference old : was) {
        if (old.getSubject().equals(ref.getSubject())
            && (named ? same(old, ref) : now.stream().noneMatch(r -> same(old, r)))
            && !old.getVersion().equals(ref.getVersion())) {
          moves.add(new Move(ref.getName(), ref.getSubject(), old.getVersion(),
              ref.getVersion(), old.getName()));
        }
      }
    }
    return moves;
  }

  private static boolean same(SchemaReference a, SchemaReference b) {
    return a.getSubject().equals(b.getSubject()) && a.getName().equals(b.getName());
  }

  // The versions a move passes through upwards, soft-deleted or not, the later end included.
  // The versions of a moved reference's type, in the referring subject's context.
  private static List<Integer> versions(TypeLookup lookup, String subject, Move move) {
    int low = Math.min(move.from(), move.to());
    int high = Math.max(move.from(), move.to());
    return lookup.versions(TypeReferences.referenced(subject, move.subject()), true).stream()
        .filter(v -> v > low && v <= high).sorted().collect(Collectors.toList());
  }

  private static ProvenanceReport report(String refused, List<SchemaType> schemaTypes,
                       List<Integer> numbers, List<LogicalType> lts) {
    int last = lts.size() - 1;
    try {
      return ProvenanceHistory.report(schemaTypes, numbers, lts::get,
          i -> i == 0 || i >= last - 1);
    } catch (AmbiguousProvenanceException e) {
      throw new TypeRefusedException(refused + ": Schema Registry's provenance could not tell "
          + "which attribute is which across its versions, so their records could not be "
          + "read: " + e.getMessage(), e);
    } catch (IllegalStateException | IllegalArgumentException | ValidationException e) {
      throw new TypeRefusedException(refused + ": Schema Registry's provenance could not pair the "
          + "attributes of its versions, so their records could not be read: "
          + e.getMessage(), e);
    }
  }

  /**
   * Each place of the new version that continues one of the version edited, by its path in
   * each: a top-level attribute by the name {@code origin} gives it there, null for none, and
   * below it a field or branch of the same name and the parts of a collection. A use of a type
   * whose reference moved, continuing one of the same type, goes in {@code uses} instead of its
   * parts, which pair as that type's versions pair them.
   */
  static Map<List<Integer>, List<Integer>> continued(Ids ids, UnaryOperator<String> origin,
                             Map<List<Integer>, Move> uses) {
    Map<List<Integer>, List<Integer>> places = new HashMap<>();
    new Walk(ids, origin, places, uses).continued(ids.next().type().getRootSchema(),
        List.of(), ids.base().type().getRootSchema(), List.of());
    return places;
  }

  private static final class Walk {

    private final Ids ids;
    private final UnaryOperator<String> origin;
    private final Map<List<Integer>, List<Integer>> places;
    private final Map<List<Integer>, Move> uses;

    Walk(Ids ids,
        UnaryOperator<String> origin,
        Map<List<Integer>, List<Integer>> places,
        Map<List<Integer>, Move> uses) {
      this.ids = ids;
      this.origin = origin;
      this.places = places;
      this.uses = uses;
    }

    Ids ids() {
      return ids;
    }

    UnaryOperator<String> origin() {
      return origin;
    }

    Map<List<Integer>, List<Integer>> places() {
      return places;
    }

    Map<List<Integer>, Move> uses() {
      return uses;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Walk)) {
        return false;
      }
      Walk other = (Walk) o;
      return Objects.equals(ids, other.ids)
          && Objects.equals(origin, other.origin)
          && Objects.equals(places, other.places)
          && Objects.equals(uses, other.uses);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ids, origin, places, uses);
    }


    void continued(Schema now, List<Integer> nowPath, Schema was, List<Integer> wasPath) {
      LogicalType nowType = ids.next().type();
      Move move = was.getType() == Schema.Type.NAMED_TYPE_REF
          ? ids.changed().get(was.getQualifiedName()) : null;
      Schema use = now;
      now = resolved(now, nowType);
      if (move != null && use.getType() == Schema.Type.NAMED_TYPE_REF && now != null
          && now.equals(ids.stepped().getNamedTypes().get(was.getQualifiedName()))) {
        uses.put(nowPath, move);
        return;
      }
      was = resolved(was, ids.base().type());
      if (now == null || was == null || now.getType() != was.getType()) {
        return;
      }
      switch (now.getType()) {
        case STRUCT:
          for (int i = 0; i < now.getFields().size(); i++) {
            Field f = now.getFields().get(i);
            String name = nowPath.isEmpty() ? origin.apply(f.getName())
                : f.getName();
            for (int j = 0; name != null && j < was.getFields().size(); j++) {
              Field w = was.getFields().get(j);
              if (w.getName().equals(name)) {
                step(f.getSchema(), nowPath, i, w.getSchema(), wasPath, j);
              }
            }
          }
          break;
        case UNION:
          for (int i = 0; i < now.getBranches().size(); i++) {
            UnionBranch b = now.getBranches().get(i);
            for (int j = 0; j < was.getBranches().size(); j++) {
              UnionBranch w = was.getBranches().get(j);
              if (w.getName().equals(b.getName())) {
                step(b.getSchema(), nowPath, i, w.getSchema(), wasPath, j);
              }
            }
          }
          break;
        case ARRAY:
        case MULTISET:
          continued(now.getElementType(), append(nowPath, 0), was.getElementType(),
              append(wasPath, 0));
          break;
        case MAP:
          continued(now.getKeyType(), append(nowPath, 0), was.getKeyType(),
              append(wasPath, 0));
          continued(now.getValueType(), append(nowPath, 1), was.getValueType(),
              append(wasPath, 1));
          break;
        default:
          break;
      }
    }

    private void step(Schema now, List<Integer> nowPath, int i, Schema was,
               List<Integer> wasPath, int j) {
      List<Integer> nowAt = append(nowPath, i);
      List<Integer> wasAt = append(wasPath, j);
      places.put(nowAt, wasAt);
      continued(now, nowAt, was, wasAt);
    }
  }

  private static List<Integer> append(List<Integer> path, int step) {
    List<Integer> extended = new ArrayList<>(path);
    extended.add(step);
    return extended;
  }

  // A type through the named types it refers to; null for one the version does not define.
  private static Schema resolved(Schema s, LogicalType lt) {
    Map<String, Schema> named = lt.getNamedTypes();
    for (int i = 0; s != null && s.getType() == Schema.Type.NAMED_TYPE_REF
        && i <= named.size(); i++) {
      s = named.get(s.getQualifiedName());
    }
    return s == null || s.getType() == Schema.Type.NAMED_TYPE_REF ? null : s;
  }

  // Schema Registry leaves an Avro schema's type unset.
  private static SchemaType schemaType(String schemaType) {
    return SchemaType.of(schemaType != null ? schemaType : "AVRO");
  }

  // A failure to read a version as provenance does refuses the change, as the registry would.
  private static LogicalType logicalType(String refused, ParsedSchema schema) {
    try {
      return ProvenanceHistory.logicalTypeOf(schema);
    } catch (ValidationException e) {
      throw new TypeRefusedException(refused + ": Schema Registry's provenance cannot read one "
          + "of its versions: " + e.getMessage(), e);
    }
  }
}
