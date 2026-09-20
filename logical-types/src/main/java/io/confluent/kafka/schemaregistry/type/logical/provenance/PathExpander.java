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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Re-keys provenance from definition sites onto fully inlined index paths.
 *
 * <p>{@link PathKey} addresses an entity where it is <em>defined</em> — a named type's members are
 * keyed once, under that type. A consumer that has inlined every reference, as Flink's
 * {@code RowType} does, addresses the same entity by the path it walked to reach it. This bridges
 * the two by walking the {@link LogicalType} from its root, following every
 * {@code NAMED_TYPE_REF}, and carrying both coordinates at once.
 *
 * <p>Inlining makes a shared named type appear more than once, so one {@link Provenance} may land
 * on several expanded paths. That is correct — they are distinct physical locations of one logical
 * entity — but it is why {@link #correspond} cannot simply join two expanded maps on provenance:
 * the use site is only determined by the prefix. It walks both versions in step instead.
 *
 * <p>A recursive type has no finite inlining, so a cycle throws rather than being truncated. Any
 * consumer that needs expanded paths cannot represent a cycle either.
 */
final class PathExpander {

  private PathExpander() {
  }

  /**
   * Every member of {@code logicalType}, keyed by its inlined index path and valued by the
   * definition site it resolves to. Pre-order, so the first path reaching a given definition comes
   * first. Named types themselves are absent: they are definitions, not locations.
   */
  static Map<List<Integer>, PathKey> expandPaths(LogicalType logicalType) {
    Map<List<Integer>, PathKey> sink = new LinkedHashMap<>();
    expandInto(Side.root(logicalType), sink);
    return Collections.unmodifiableMap(sink);
  }

  /** Every member's provenance, keyed by inlined index path. */
  static Map<List<Integer>, Provenance> expand(
      LogicalType logicalType, Map<PathKey, Provenance> byPath) {
    Map<List<Integer>, Provenance> sink = new LinkedHashMap<>();
    for (Map.Entry<List<Integer>, PathKey> entry : expandPaths(logicalType).entrySet()) {
      Provenance provenance = byPath.get(entry.getValue());
      if (provenance != null) {
        sink.put(entry.getKey(), provenance);
      }
    }
    return Collections.unmodifiableMap(sink);
  }

  /**
   * Declared default values, keyed by inlined index path.
   *
   * <p>{@code LogicalType.getDefaultValues()} keys a member of a named type by the path of that
   * type's <em>first</em> occurrence, because the readers convert a named type's body once. This
   * re-keys them onto every occurrence, which is what a consumer that inlined the type needs.
   *
   * <p>A declared {@code null} default is kept as a null value, so callers distinguishing it from
   * "no default" must use {@code containsKey}.
   */
  static Map<List<Integer>, Object> expandDefaults(LogicalType logicalType) {
    Map<List<Integer>, Object> declared = logicalType.getDefaultValues();
    if (declared == null || declared.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<List<Integer>, PathKey> paths = expandPaths(logicalType);

    // Pre-order, so the first path seen for a definition is the one the reader keyed it under.
    Map<PathKey, List<Integer>> firstOccurrence = new HashMap<>();
    for (Map.Entry<List<Integer>, PathKey> entry : paths.entrySet()) {
      firstOccurrence.putIfAbsent(entry.getValue(), entry.getKey());
    }

    Map<List<Integer>, Object> sink = new LinkedHashMap<>();
    for (Map.Entry<List<Integer>, PathKey> entry : paths.entrySet()) {
      List<Integer> declaredAt = firstOccurrence.get(entry.getValue());
      if (declared.containsKey(declaredAt)) {
        sink.put(entry.getKey(), declared.get(declaredAt));
      }
    }
    return Collections.unmodifiableMap(sink);
  }

  private static void expandInto(Side side, Map<List<Integer>, PathKey> sink) {
    if (side.type == null) {
      return;
    }
    switch (side.type.getType()) {
      case NAMED_TYPE_REF:
        side.dereferencing(dereferenced -> expandInto(dereferenced, sink));
        break;
      case STRUCT:
      case UNION: {
        int members = memberCount(side.type);
        for (int i = 0; i < members; i++) {
          Side member = side.member(memberType(side.type, i), i, i);
          sink.put(member.expanded, member.definition);
          expandInto(member, sink);
        }
        break;
      }
      case ARRAY:
      case MULTISET:
        expandInto(side.member(side.type.getElementType(), 0, 0), sink);
        break;
      case MAP:
        expandInto(side.member(side.type.getKeyType(), 0, 0), sink);
        expandInto(side.member(side.type.getValueType(), 1, 1), sink);
        break;
      default:
        break;
    }
  }

  /**
   * Walks {@code target} and {@code source} in step, recording where each shared member sits on
   * both sides, each container's correspondence as positions, and why anything unmatched is
   * unmatched.
   *
   * <p>A member absent from the source stops that subtree: its children have no source location to
   * name. So does a structural divergence — if the two sides disagree on their kind,
   * correspondence below that point is not expressible as a path pair.
   */
  static Correspondence correspond(
      LogicalType target, LogicalType source, Map<PathKey, PathKey> correspondence) {
    Correspondence result = new Correspondence();
    correspond(Side.root(target), Side.root(source), correspondence, result);
    return result;
  }

  private static void correspond(
      Side target, Side source, Map<PathKey, PathKey> correspondence, Correspondence result) {
    if (target.type == null) {
      return;
    }
    if (source.type == null) {
      markSubtree(target, Absence.TYPE_DIVERGED, result.absences);
      return;
    }
    // Dereference each side independently: one may be a named type where the other is inline.
    if (target.type.getType() == Schema.Type.NAMED_TYPE_REF) {
      target.dereferencing(d -> correspond(d, source, correspondence, result));
      return;
    }
    if (source.type.getType() == Schema.Type.NAMED_TYPE_REF) {
      source.dereferencing(d -> correspond(target, d, correspondence, result));
      return;
    }
    if (target.type.getType() != source.type.getType()) {
      markSubtree(target, Absence.TYPE_DIVERGED, result.absences);
      return;
    }

    switch (target.type.getType()) {
      case STRUCT:
      case UNION:
        correspondMembers(target, source, correspondence, result);
        break;
      case ARRAY:
      case MULTISET:
        correspond(target.member(target.type.getElementType(), 0, 0),
            source.member(source.type.getElementType(), 0, 0), correspondence, result);
        break;
      case MAP:
        correspond(target.member(target.type.getKeyType(), 0, 0),
            source.member(source.type.getKeyType(), 0, 0), correspondence, result);
        correspond(target.member(target.type.getValueType(), 1, 1),
            source.member(source.type.getValueType(), 1, 1), correspondence, result);
        break;
      default:
        break;
    }
  }

  private static void correspondMembers(
      Side target, Side source, Map<PathKey, PathKey> correspondence, Correspondence result) {
    int members = memberCount(target.type);
    int sourceMembers = memberCount(source.type);
    int[] positions = new int[members];
    Arrays.fill(positions, ProvenanceResult.ABSENT);

    for (int i = 0; i < members; i++) {
      Side targetMember = target.member(memberType(target.type, i), i, i);
      PathKey sourceDefinition = correspondence.get(targetMember.definition);
      if (sourceDefinition == null || sourceDefinition.isRoot()
          || sourceDefinition.position() >= sourceMembers) {
        result.absences.put(targetMember.expanded, Absence.NOT_IN_SOURCE);
        markSubtree(targetMember, Absence.PARENT_ABSENT, result.absences);
        continue;
      }
      int sourcePosition = sourceDefinition.position();
      positions[i] = sourcePosition;

      Side sourceMember = source.at(memberType(source.type, sourcePosition),
          append(source.expanded, sourcePosition), sourceDefinition);
      result.paths.put(targetMember.expanded, sourceMember.expanded);
      correspond(targetMember, sourceMember, correspondence, result);
    }
    result.containers.put(target.expanded, new PositionMapping(positions, sourceMembers));
  }

  /**
   * Records every member beneath {@code target} as unmatched, for the stated reason.
   */
  private static void markSubtree(
      Side target, Absence reason, Map<List<Integer>, Absence> sink) {
    if (target.type == null) {
      return;
    }
    switch (target.type.getType()) {
      case NAMED_TYPE_REF:
        target.dereferencing(d -> markSubtree(d, reason, sink));
        break;
      case STRUCT:
      case UNION: {
        int members = memberCount(target.type);
        for (int i = 0; i < members; i++) {
          Side member = target.member(memberType(target.type, i), i, i);
          sink.put(member.expanded, reason);
          markSubtree(member, reason, sink);
        }
        break;
      }
      case ARRAY:
      case MULTISET:
        markSubtree(target.member(target.type.getElementType(), 0, 0), reason, sink);
        break;
      case MAP:
        markSubtree(target.member(target.type.getKeyType(), 0, 0), reason, sink);
        markSubtree(target.member(target.type.getValueType(), 1, 1), reason, sink);
        break;
      default:
        break;
    }
  }

  /** Everything one paired walk produces. */
  static final class Correspondence {

    private final Map<List<Integer>, List<Integer>> paths = new LinkedHashMap<>();
    private final Map<List<Integer>, PositionMapping> containers = new LinkedHashMap<>();
    private final Map<List<Integer>, Absence> absences = new LinkedHashMap<>();

    Map<List<Integer>, List<Integer>> paths() {
      return Collections.unmodifiableMap(paths);
    }

    Map<List<Integer>, PositionMapping> containers() {
      return Collections.unmodifiableMap(containers);
    }

    Map<List<Integer>, Absence> absences() {
      return Collections.unmodifiableMap(absences);
    }
  }

  /** One version's position in the walk: where we are, and both coordinates for it. */
  private static final class Side {

    private final LogicalType logicalType;
    private final Schema type;
    private final List<Integer> expanded;
    private final PathKey definition;
    private final Set<String> inProgress;

    private Side(LogicalType logicalType, Schema type, List<Integer> expanded,
        PathKey definition, Set<String> inProgress) {
      this.logicalType = logicalType;
      this.type = type;
      this.expanded = expanded;
      this.definition = definition;
      this.inProgress = inProgress;
    }

    static Side root(LogicalType logicalType) {
      return new Side(logicalType, logicalType.getRootSchema(), Collections.emptyList(),
          PathKey.ofRoot(), new LinkedHashSet<>());
    }

    Side at(Schema newType, List<Integer> newExpanded, PathKey newDefinition) {
      return new Side(logicalType, newType, newExpanded, newDefinition, inProgress);
    }

    /** One step down: an expanded step, and the matching definition step. */
    Side member(Schema newType, int expandedStep, int definitionStep) {
      return at(newType, append(expanded, expandedStep), definition.child(definitionStep));
    }

    /**
     * Runs {@code body} on this side resolved through its reference. A reference adds no step to
     * either coordinate — the member that held it already did — but it does move the definition
     * root to the named type.
     */
    void dereferencing(Consumer<Side> body) {
      String name = type.getQualifiedName();
      if (!inProgress.add(name)) {
        throw new IllegalStateException("Cannot expand a recursive named type: " + name);
      }
      try {
        body.accept(at(logicalType.getNamedTypes().get(name), expanded, PathKey.ofNamedType(name)));
      } finally {
        inProgress.remove(name);
      }
    }
  }

  private static int memberCount(Schema type) {
    switch (type.getType()) {
      case STRUCT:
        return type.getFields().size();
      case UNION:
        return type.getBranches().size();
      default:
        return -1;
    }
  }

  private static Schema memberType(Schema type, int index) {
    return type.getType() == Schema.Type.STRUCT
        ? type.getFields().get(index).getSchema()
        : type.getBranches().get(index).getSchema();
  }

  private static List<Integer> append(List<Integer> path, int step) {
    List<Integer> extended = new ArrayList<>(path.size() + 1);
    extended.addAll(path);
    extended.add(step);
    return Collections.unmodifiableList(extended);
  }
}
