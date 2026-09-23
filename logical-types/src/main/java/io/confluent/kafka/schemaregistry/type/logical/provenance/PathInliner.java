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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;

import java.util.ArrayList;
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
 * {@code RowType} and an Iceberg schema both have, addresses the same entity by the path it walked
 * to reach it, and needs to tell two uses of one shared type apart. This walks the
 * {@link LogicalType} from its root following every {@code NAMED_TYPE_REF}, carrying the inlined
 * path, the definition site and the chain of enclosing provenances at once.
 *
 * <p>A recursive type has no finite inlining, so a cycle throws rather than being truncated. Any
 * consumer that needs inlined paths cannot represent a cycle either.
 *
 * <p>Default values are picked up on the way past. {@link LogicalType#getDefaultValues()} keys a
 * named type's members by the path of that type's <em>first</em> occurrence, so the walk records
 * where it first dereferences each named type and resolves the key from there — which gives both
 * uses of a shared type the same default, as they should have.
 */
final class PathInliner {

  private PathInliner() {
  }

  /**
   * Every member of {@code logicalType}, keyed by its inlined index path and valued by the chain
   * of provenances that locates it. Named types themselves are absent: they are definitions, not
   * locations, and one unreachable from the root contributes nothing.
   */
  static List<InlinedMember> inline(LogicalType logicalType, Map<PathKey, Provenance> byPath) {
    List<InlinedMember> sink = new ArrayList<>();
    inlineInto(Side.root(logicalType), Collections.emptyList(), byPath, sink);
    return Collections.unmodifiableList(sink);
  }

  private static void inlineInto(Side side, List<Provenance> ancestors,
      Map<PathKey, Provenance> byPath, List<InlinedMember> sink) {
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
          Side member = side.descend(memberType(side.type, i), i, memberName(side.type, i));
          Provenance provenance = byPath.get(member.definition);
          if (provenance == null) {
            // Not an entity the resolver emitted, so this location cannot be named. Should not
            // happen for a schema the resolver walked; skip rather than chain past the gap.
            continue;
          }
          LocatedProvenance located = LocatedProvenance.of(ancestors, provenance);
          sink.add(new InlinedMember(
              member.inlined, member.names, located, member.defaultValue()));
          inlineInto(member, located.getChain(), byPath, sink);
        }
        break;
      }
      case ARRAY:
      case MULTISET:
        // A collection step is not an entity, so the chain does not grow.
        inlineInto(side.descend(side.type.getElementType(), 0, "[]"), ancestors, byPath, sink);
        break;
      case MAP:
        inlineInto(side.descend(side.type.getKeyType(), 0, "{key}"), ancestors, byPath, sink);
        inlineInto(side.descend(side.type.getValueType(), 1, "{value}"), ancestors, byPath, sink);
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
    private final List<String> names;
    private final PathKey definition;
    private final Set<String> inProgress;
    /** Where each named type was first inlined — shared across the walk, written once per type. */
    private final Map<String, List<Integer>> firstInlined;

    private Side(LogicalType logicalType, Schema type, List<Integer> inlined, List<String> names,
        PathKey definition, Set<String> inProgress, Map<String, List<Integer>> firstInlined) {
      this.logicalType = logicalType;
      this.type = type;
      this.inlined = inlined;
      this.names = names;
      this.definition = definition;
      this.inProgress = inProgress;
      this.firstInlined = firstInlined;
    }

    static Side root(LogicalType logicalType) {
      return new Side(logicalType, logicalType.getRootSchema(), Collections.emptyList(),
          Collections.emptyList(), PathKey.ofRoot(), new LinkedHashSet<>(), new HashMap<>());
    }

    /** One step down, whether to a member or through a collection. */
    Side descend(Schema newType, int step, String name) {
      return new Side(logicalType, newType, append(inlined, step), appendName(names, name),
          definition.child(step), inProgress, firstInlined);
    }

    /**
     * This member's declared default, or {@code null} where it has none.
     *
     * <p>A struct default is left out. The readers normalise scalars and collections into common
     * Java values but pass a struct default through in the source format's own shape, so there is
     * nothing here a consumer could read without knowing which format it came from.
     */
    Object defaultValue() {
      Schema target = resolved(type);
      if (target != null && target.getType() == Schema.Type.STRUCT) {
        return null;
      }
      List<Integer> root = definition.getTypeName() == null
          ? Collections.emptyList()
          : firstInlined.get(definition.getTypeName());
      if (root == null) {
        return null;
      }
      List<Integer> key = new ArrayList<>(root);
      key.addAll(definition.getIndexPath());
      return normalise(logicalType.getDefaultValues().get(key));
    }

    private Schema resolved(Schema schema) {
      return schema != null && schema.getType() == Schema.Type.NAMED_TYPE_REF
          ? logicalType.getNamedTypes().get(schema.getQualifiedName())
          : schema;
    }

    /**
     * Runs {@code body} on this side resolved through its reference. A reference adds no step to
     * any coordinate — the member that held it already did — but it does move the definition root
     * to the named type.
     */
    void dereferencing(Consumer<Side> body) {
      String name = type.getQualifiedName();
      if (!inProgress.add(name)) {
        throw new RecursiveTypeException(name);
      }
      firstInlined.putIfAbsent(name, inlined);
      try {
        body.accept(new Side(logicalType, logicalType.getNamedTypes().get(name), inlined, names,
            PathKey.ofNamedType(name), inProgress, firstInlined));
      } finally {
        inProgress.remove(name);
      }
    }
  }

  /**
   * A recorded default in the common Java form the report promises.
   *
   * <p>The Protobuf reader deliberately records its format's native default types, mirroring
   * Flink's catalog defaults: an enum as its {@link EnumValueDescriptor}, bytes as a {@link
   * ByteString}. Normalising here, at the provenance boundary, keeps that parity intact for the
   * readers' other callers while giving every provenance consumer plain values.
   */
  static Object normalise(Object value) {
    if (value instanceof EnumValueDescriptor) {
      return ((EnumValueDescriptor) value).getName();
    }
    if (value instanceof ByteString) {
      return ((ByteString) value).toByteArray();
    }
    if (value instanceof List) {
      List<Object> normalised = new ArrayList<>();
      for (Object element : (List<?>) value) {
        normalised.add(normalise(element));
      }
      return normalised;
    }
    if (value instanceof Map) {
      Map<Object, Object> normalised = new LinkedHashMap<>();
      ((Map<?, ?>) value).forEach((k, v) -> normalised.put(normalise(k), normalise(v)));
      return normalised;
    }
    return value;
  }

  private static String memberName(Schema type, int index) {
    return type.getType() == Schema.Type.STRUCT
        ? type.getFields().get(index).getName()
        : type.getBranches().get(index).getName();
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

  private static List<String> appendName(List<String> names, String name) {
    List<String> extended = new ArrayList<>(names.size() + 1);
    extended.addAll(names);
    extended.add(name);
    return Collections.unmodifiableList(extended);
  }

  private static List<Integer> append(List<Integer> path, int step) {
    List<Integer> extended = new ArrayList<>(path.size() + 1);
    extended.addAll(path);
    extended.add(step);
    return Collections.unmodifiableList(extended);
  }
}
