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

package io.confluent.kafka.schemaregistry.rules.cel.avro;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import dev.cel.common.types.CelType;
import dev.cel.common.types.CelTypeProvider;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

/**
 * A {@link CelTypeProvider} that surfaces Avro {@link Schema} record types as CEL
 * {@link StructType}s. Used by the compiler so {@code this.field} type-checks
 * against the actual Avro field types rather than against {@code dyn}.
 *
 * <p>The runtime continues to receive Avro values as {@code Map<String, Object>}
 * (via {@code CelUtils.toCelValue}); Google cel-java's runtime treats
 * {@code Map}s as natively selectable, so no {@link dev.cel.common.values.CelValueProvider}
 * counterpart is needed.
 *
 * <p>Walks the supplied schema graph, registering each reachable record
 * schema as a {@link StructType}. Field types map to CEL types per the standard
 * Avro→CEL mapping; nested records become {@link dev.cel.common.types.StructTypeReference}s
 * (resolved at type-check time against this same provider).
 *
 * <p>Schemas this provider can't cleanly represent (e.g., multi-branch unions
 * mixing record + scalar) yield {@code dyn} for the field; the surrounding
 * record type is still registered. If the surrounding record itself is
 * unrepresentable, callers fall back to declaring {@code this} as
 * {@code MapType(STRING, DYN)}.
 */
public final class AvroCelTypeProvider implements CelTypeProvider {

  private final ImmutableMap<String, StructType> typesByName;

  private AvroCelTypeProvider(Map<String, StructType> types) {
    this.typesByName = ImmutableMap.copyOf(types);
  }

  /**
   * Build a provider that registers all record schemas reachable from
   * {@code root}.
   */
  public static AvroCelTypeProvider forSchema(Schema root) {
    Map<String, StructType> registry = new HashMap<>();
    collect(root, registry, new HashSet<>());
    return new AvroCelTypeProvider(registry);
  }

  /** Returns true iff there's at least one record we can describe. */
  public boolean hasAnyType() {
    return !typesByName.isEmpty();
  }

  @Override
  public ImmutableCollection<CelType> types() {
    return ImmutableSet.copyOf(typesByName.values());
  }

  @Override
  public Optional<CelType> findType(String typeName) {
    StructType found = typesByName.get(typeName);
    return found != null ? Optional.of(found) : Optional.empty();
  }

  // ---- internals ----

  private static void collect(
      Schema schema, Map<String, StructType> registry, Set<String> visiting) {
    if (schema == null) {
      return;
    }
    Schema effective = stripNullableUnion(schema);
    if (effective == null) {
      return;
    }
    Type type = effective.getType();
    if (type == Type.RECORD) {
      String fullName = effective.getFullName();
      if (registry.containsKey(fullName) || visiting.contains(fullName)) {
        return;
      }
      visiting.add(fullName);
      // Register the struct first (with field resolver that closes over the
      // schema), then recurse into nested fields.
      registry.put(fullName, structTypeFor(effective));
      for (Field f : effective.getFields()) {
        collect(f.schema(), registry, visiting);
      }
      visiting.remove(fullName);
    } else if (type == Type.ARRAY) {
      collect(effective.getElementType(), registry, visiting);
    } else if (type == Type.MAP) {
      collect(effective.getValueType(), registry, visiting);
    }
  }

  /**
   * Build a {@link StructType} for an Avro record schema. The field resolver
   * closes over {@code schema} so field types are computed lazily at type-check
   * time.
   */
  private static StructType structTypeFor(Schema schema) {
    String fullName = schema.getFullName();
    ImmutableSet<String> fieldNames = ImmutableSet.copyOf(
        schema.getFields().stream()
            .map(Field::name)
            .collect(ImmutableList.toImmutableList()));
    StructType.FieldResolver resolver = name -> {
      Field f = schema.getField(name);
      if (f == null) {
        return Optional.empty();
      }
      return Optional.of(StructType.Field.of(name, fieldCelType(f.schema())).type());
    };
    return StructType.create(fullName, fieldNames, resolver);
  }

  /**
   * Map an Avro {@link Schema} to a {@link CelType} for use as a struct field
   * type. Nested records become {@link dev.cel.common.types.StructTypeReference}
   * (resolved by this same provider). Anything we can't cleanly represent
   * becomes {@code dyn} so the surrounding struct stays usable.
   */
  static CelType fieldCelType(Schema schema) {
    Schema effective = stripNullableUnion(schema);
    if (effective == null) {
      return SimpleType.DYN;
    }
    // Logical-typed fields (timestamp-millis on long, decimal on bytes/fixed,
    // date on int, uuid on string, etc.) carry runtime values whose Java type
    // diverges from the underlying Avro primitive — Instant / BigDecimal /
    // LocalDate / UUID. Declaring the underlying CEL type creates a
    // compile/runtime mismatch. Use DYN so the runtime can dispatch against
    // whatever the value actually is, mirroring AvroResultWriter's logical-
    // type bypass and CelUtils.findCelTypeForAvroSchema.
    if (effective.getLogicalType() != null) {
      return SimpleType.DYN;
    }
    switch (effective.getType()) {
      case BOOLEAN:
        return SimpleType.BOOL;
      case INT:
      case LONG:
        return SimpleType.INT;
      case BYTES:
      case FIXED:
        return SimpleType.BYTES;
      case FLOAT:
      case DOUBLE:
        return SimpleType.DOUBLE;
      case STRING:
      case ENUM:
        return SimpleType.STRING;
      case ARRAY:
        return ListType.create(fieldCelType(effective.getElementType()));
      case MAP:
        return MapType.create(SimpleType.STRING, fieldCelType(effective.getValueType()));
      case NULL:
        return SimpleType.NULL_TYPE;
      case RECORD:
        return dev.cel.common.types.StructTypeReference.create(effective.getFullName());
      case UNION:
      default:
        // Multi-branch unions and unknown types: fall back to dyn for the field.
        return SimpleType.DYN;
    }
  }

  /**
   * For unions that are exactly {@code [null, X]} (i.e., Avro's "nullable X"
   * idiom), return {@code X}. For all other unions return null (the caller
   * decides how to handle).
   */
  private static Schema stripNullableUnion(Schema schema) {
    if (schema.getType() != Type.UNION) {
      return schema;
    }
    List<Schema> branches = schema.getTypes();
    if (branches.size() == 2) {
      Schema first = branches.get(0);
      Schema second = branches.get(1);
      if (first.getType() == Type.NULL) {
        return second;
      }
      if (second.getType() == Type.NULL) {
        return first;
      }
    }
    // Unrepresentable union — caller falls back.
    return null;
  }
}
