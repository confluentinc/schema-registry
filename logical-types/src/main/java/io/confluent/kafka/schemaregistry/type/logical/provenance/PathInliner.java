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
import java.util.Collections;
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
 * {@code RowType} and an Iceberg schema both have, addresses the same entity by the path it walked
 * to reach it, and needs to tell two uses of one shared type apart. This walks the
 * {@link LogicalType} from its root following every {@code NAMED_TYPE_REF}, carrying the inlined
 * path, the definition site and the chain of enclosing provenances at once.
 *
 * <p>A recursive type has no finite inlining, so a cycle throws rather than being truncated. Any
 * consumer that needs inlined paths cannot represent a cycle either.
 */
final class PathInliner {

  private PathInliner() {
  }

  /**
   * Every member of {@code logicalType}, keyed by its inlined index path and valued by the chain
   * of provenances that locates it. Named types themselves are absent: they are definitions, not
   * locations, and one unreachable from the root contributes nothing.
   */
  static Map<List<Integer>, LocatedProvenance> inline(
      LogicalType logicalType, Map<PathKey, Provenance> byPath) {
    Map<List<Integer>, LocatedProvenance> sink = new LinkedHashMap<>();
    inlineInto(Side.root(logicalType), Collections.emptyList(), byPath, sink);
    return Collections.unmodifiableMap(sink);
  }

  private static void inlineInto(Side side, List<Provenance> ancestors,
      Map<PathKey, Provenance> byPath, Map<List<Integer>, LocatedProvenance> sink) {
    if (side.type == null) {
      return;
    }
    switch (side.type.getType()) {
      case NAMED_TYPE_REF:
        side.dereferencing(dereferenced -> inlineInto(dereferenced, ancestors, byPath, sink));
        break;
      case STRUCT:
      case UNION: {
        int members = memberCount(side.type);
        for (int i = 0; i < members; i++) {
          Side member = side.member(memberType(side.type, i), i, i);
          Provenance provenance = byPath.get(member.definition);
          if (provenance == null) {
            // Not an entity the resolver emitted, so this location cannot be named. Should not
            // happen for a schema the resolver walked; skip rather than chain past the gap.
            continue;
          }
          LocatedProvenance located = LocatedProvenance.of(ancestors, provenance);
          sink.put(member.inlined, located);
          inlineInto(member, located.getChain(), byPath, sink);
        }
        break;
      }
      case ARRAY:
      case MULTISET:
        // A collection step is not an entity, so the chain does not grow.
        inlineInto(side.member(side.type.getElementType(), 0, 0), ancestors, byPath, sink);
        break;
      case MAP:
        inlineInto(side.member(side.type.getKeyType(), 0, 0), ancestors, byPath, sink);
        inlineInto(side.member(side.type.getValueType(), 1, 1), ancestors, byPath, sink);
        break;
      default:
        break;
    }
  }

  /** One version's position in the walk: where we are, and both coordinates for it. */
  private static final class Side {

    private final LogicalType logicalType;
    private final Schema type;
    private final List<Integer> inlined;
    private final PathKey definition;
    private final Set<String> inProgress;

    private Side(LogicalType logicalType, Schema type, List<Integer> inlined,
        PathKey definition, Set<String> inProgress) {
      this.logicalType = logicalType;
      this.type = type;
      this.inlined = inlined;
      this.definition = definition;
      this.inProgress = inProgress;
    }

    static Side root(LogicalType logicalType) {
      return new Side(logicalType, logicalType.getRootSchema(), Collections.emptyList(),
          PathKey.ofRoot(), new LinkedHashSet<>());
    }

    Side at(Schema newType, List<Integer> newInlined, PathKey newDefinition) {
      return new Side(logicalType, newType, newInlined, newDefinition, inProgress);
    }

    /** One step down: an inlined step, and the matching definition step. */
    Side member(Schema newType, int inlinedStep, int definitionStep) {
      return at(newType, append(inlined, inlinedStep), definition.child(definitionStep));
    }

    /**
     * Runs {@code body} on this side resolved through its reference. A reference adds no step to
     * either coordinate — the member that held it already did — but it does move the definition
     * root to the named type.
     */
    void dereferencing(Consumer<Side> body) {
      String name = type.getQualifiedName();
      if (!inProgress.add(name)) {
        throw new IllegalStateException("Cannot inline a recursive named type: " + name);
      }
      try {
        body.accept(at(logicalType.getNamedTypes().get(name), inlined, PathKey.ofNamedType(name)));
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
