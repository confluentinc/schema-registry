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

package io.confluent.kafka.schemaregistry.type.logical.constraint;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import dev.cel.common.types.CelType;
import dev.cel.common.types.CelTypeProvider;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructType;
import dev.cel.common.types.StructTypeReference;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.CelTypeLabels;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

/**
 * Google cel-java {@link CelTypeProvider} that surfaces a logical-type
 * {@link Schema}'s field shape to CEL's static type-checker.
 *
 * <p>Google cel-java's type checker resolves field accesses on
 * {@link StructTypeReference} types by consulting a {@link CelTypeProvider}.
 * For protobuf messages, cel-java ships descriptor-based providers. We don't
 * have proto descriptors — our schemas are declarative LT trees — so this
 * class provides equivalent answers from the LT side: every nested STRUCT
 * in the schema gets a synthetic CEL type name, and field-type lookups walk
 * the LT field map.
 *
 * <p>Type-checking-only — the strict-check path (see
 * {@link ConstraintCelChecker}) only parses + checks; never evaluates. So we
 * don't need a runtime {@code CelValueProvider} counterpart.
 *
 * <p>Field-type mapping mirrors the v1 Schema → CEL convention:
 * <ul>
 *   <li>integral → {@code int}, floating → {@code double},
 *       DECIMAL → {@code double} (CEL has no native decimal),</li>
 *   <li>CHAR/VARCHAR → {@code string}, BINARY/VARBINARY → {@code bytes},</li>
 *   <li>BOOLEAN → {@code bool}, DATE/TIME/TIMESTAMP/TIMESTAMP_LTZ
 *       → {@code timestamp},</li>
 *   <li>ARRAY/MULTISET → {@code list<T>}, MAP → {@code map<K, V>},</li>
 *   <li>STRUCT → synthetic struct type (registered lazily); recursive
 *       structs cycle safely via {@link StructTypeReference}.</li>
 *   <li>NAMED_TYPE_REF → resolved via the validation context; falls back
 *       to {@code dyn} when the named type is unknown.</li>
 *   <li>ENUM/UNION/VARIANT → {@code dyn} (cel-java has no first-class
 *       analog; defer to runtime).</li>
 * </ul>
 */
final class ConstraintTypeProvider implements CelTypeProvider {

  /**
   * Synthetic CEL object type name for the root {@code this} struct in a
   * table-level CHECK. Column-level CHECK doesn't use this — it declares
   * {@code this} with the field's primitive CEL type directly.
   */
  static final String ROOT_TYPE_NAME = "__lt_root__";

  /**
   * synthetic-type-name → Schema. Built lazily as {@link #celTypeFor}
   * descends into nested STRUCTs. Insertion-ordered for deterministic
   * naming.
   */
  private final Map<String, Schema> typeNames = new LinkedHashMap<>();

  /** identity → synthetic name (so the same Schema reuses a type name). */
  private final Map<Schema, String> nameByStruct = new IdentityHashMap<>();

  /**
   * synthetic-type-name → registered {@link StructType}. Built alongside
   * {@link #typeNames}; cel-java's type checker queries this via
   * {@link #findType(String)}.
   */
  private final Map<String, StructType> structTypes = new LinkedHashMap<>();

  private final ConstraintValidationContext vctx;

  ConstraintTypeProvider(ConstraintValidationContext vctx) {
    this.vctx = vctx;
  }

  /**
   * Register {@code rootStruct} as the type behind {@link #ROOT_TYPE_NAME}.
   * Call once per env build, before any field-type queries.
   */
  void registerRoot(Schema rootStruct) {
    if (rootStruct.getType() != Schema.Type.STRUCT) {
      throw new IllegalArgumentException(
          "Root must be STRUCT, got " + rootStruct.getType());
    }
    typeNames.put(ROOT_TYPE_NAME, rootStruct);
    nameByStruct.put(rootStruct, ROOT_TYPE_NAME);
    structTypes.put(ROOT_TYPE_NAME, buildStructType(ROOT_TYPE_NAME, rootStruct));
  }

  // ---------------------------------------------------------------------
  // CelTypeProvider — Google cel-java's two-method type-check surface
  // ---------------------------------------------------------------------

  @Override
  public ImmutableCollection<CelType> types() {
    return ImmutableSet.copyOf(structTypes.values());
  }

  @Override
  public Optional<CelType> findType(String typeName) {
    StructType found = structTypes.get(typeName);
    return Optional.ofNullable(found);
  }

  // ---------------------------------------------------------------------
  // Schema → CEL type mapping
  // ---------------------------------------------------------------------

  /**
   * Map an LT {@link Schema} to a {@link CelType}. Container types recurse;
   * STRUCT recurses via {@link #structTypeName} which lazily registers a
   * synthetic name.
   */
  CelType celTypeFor(Schema s) {
    if (s == null) {
      return SimpleType.DYN;
    }
    Schema resolved = s;
    if (s.getType() == Schema.Type.NAMED_TYPE_REF) {
      Schema r = vctx.resolveNamedType(s);
      if (r == null) {
        return SimpleType.DYN;
      }
      resolved = r;
    }
    switch (resolved.getType()) {
      case BOOLEAN:
        return SimpleType.BOOL;
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return SimpleType.INT;
      case FLOAT:
      case DOUBLE:
        return SimpleType.DOUBLE;
      case DECIMAL:
        // DECIMAL rides as the opaque confluent.type.Decimal CEL type (the same
        // label BuiltinDeclarations declares decimal()/decimals.* against), not
        // CEL double — so the emitter's decimals.* dispatch type-checks and we
        // keep exact-decimal semantics rather than degrading to double.
        return CelTypeLabels.DECIMAL;
      case CHAR:
      case VARCHAR:
        return SimpleType.STRING;
      case BINARY:
      case VARBINARY:
        return SimpleType.BYTES;
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return SimpleType.TIMESTAMP;
      case ARRAY:
      case MULTISET:
        return ListType.create(celTypeFor(resolved.getElementType()));
      case MAP:
        return MapType.create(
            celTypeFor(resolved.getKeyType()),
            celTypeFor(resolved.getValueType()));
      case STRUCT:
        return StructTypeReference.create(structTypeName(resolved));
      case ENUM:
      case UNION:
      case VARIANT:
      default:
        return SimpleType.DYN;
    }
  }

  private String structTypeName(Schema struct) {
    String existing = nameByStruct.get(struct);
    if (existing != null) {
      return existing;
    }
    String name = "__lt_struct_" + (typeNames.size()) + "__";
    typeNames.put(name, struct);
    nameByStruct.put(struct, name);
    structTypes.put(name, buildStructType(name, struct));
    return name;
  }

  /**
   * Build a Google cel-java {@link StructType} from an LT STRUCT
   * {@link Schema}. The field resolver closes over {@code struct} so field
   * types are computed lazily at type-check time (necessary for recursive
   * structs to terminate — {@link #celTypeFor} on a STRUCT goes through
   * {@link StructTypeReference}, not back into a direct StructType).
   */
  private StructType buildStructType(String name, Schema struct) {
    LinkedHashSet<String> fieldNames = new LinkedHashSet<>();
    for (Schema.Field f : struct.getFields()) {
      fieldNames.add(f.getName());
    }
    StructType.FieldResolver resolver = fieldName -> {
      Schema.Field f = struct.getField(fieldName);
      if (f == null) {
        return Optional.empty();
      }
      return Optional.of(celTypeFor(f.getSchema()));
    };
    return StructType.create(name, ImmutableSet.copyOf(fieldNames), resolver);
  }

  /**
   * Snapshot of the synthetic type names registered so far. Used by
   * {@link ConstraintCelChecker} for diagnostics and for callers that
   * want to enumerate the registered types.
   */
  Map<String, Schema> registeredTypes() {
    return Collections.unmodifiableMap(typeNames);
  }
}
