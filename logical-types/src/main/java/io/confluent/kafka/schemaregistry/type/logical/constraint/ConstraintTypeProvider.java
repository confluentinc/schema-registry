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

import com.google.api.expr.v1alpha1.Type;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.ref.FieldType;
import org.projectnessie.cel.common.types.ref.TypeRegistry;
import org.projectnessie.cel.common.types.ref.Val;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * cel-java {@link TypeRegistry} implementation that exposes a logical-type
 * {@link Schema}'s field shape to CEL's static type-checker.
 *
 * <p>nessie cel-java's type-checker resolves field accesses on
 * {@link Decls#newObjectType(String) object types} by consulting a
 * {@code TypeProvider}. For protobuf messages, the standard
 * {@code ProtoTypeRegistry} answers via descriptors. We don't have proto
 * descriptors — our schemas are declarative LT trees — so this class
 * provides equivalent answers from the LT side: every nested STRUCT in
 * the schema gets a synthetic CEL object type name, and field-type
 * lookups walk the LT field map.
 *
 * <p>Type-checking-only — runtime evaluation hooks ({@link #findIdent},
 * {@link #newValue}, {@link #enumValue}, {@link #nativeToValue}) return
 * {@code null}/no-ops. The strict-check path (see
 * {@link ConstraintCelChecker}) only parses + checks; never evaluates.
 *
 * <p>Field-type mapping mirrors the v1 Schema → CEL convention:
 * <ul>
 *   <li>integral → {@code int}, floating → {@code double},
 *       DECIMAL → {@code double} (CEL has no native decimal),</li>
 *   <li>CHAR/VARCHAR → {@code string}, BINARY/VARBINARY → {@code bytes},</li>
 *   <li>BOOLEAN → {@code bool}, DATE/TIME/TIMESTAMP/TIMESTAMP_LTZ
 *       → {@code timestamp},</li>
 *   <li>ARRAY/MULTISET → {@code list<T>}, MAP → {@code map<K, V>},</li>
 *   <li>STRUCT → synthetic object type (registered lazily in
 *       {@link #typeNames}); recursive structs cycle safely.</li>
 *   <li>NAMED_TYPE_REF → resolved via the validation context; falls back
 *       to {@code dyn} when the named type is unknown.</li>
 *   <li>ENUM/UNION/VARIANT → {@code dyn} (cel-java has no first-class
 *       analog; defer to runtime).</li>
 * </ul>
 */
final class ConstraintTypeProvider implements TypeRegistry {

  /**
   * Synthetic CEL object type name for the root {@code this} struct in a
   * table-level CHECK. Column-level CHECK doesn't use this — it declares
   * {@code this} with the field's primitive CEL type directly.
   */
  static final String ROOT_TYPE_NAME = "__lt_root__";

  /**
   * synthetic-type-name → Schema. Built lazily as {@code findFieldType}
   * descends into nested STRUCTs. Insertion-ordered for deterministic
   * naming.
   */
  private final Map<String, Schema> typeNames = new LinkedHashMap<>();

  /** identity → synthetic name (so the same Schema reuses a type name). */
  private final Map<Schema, String> nameByStruct = new java.util.IdentityHashMap<>();

  private final ConstraintValidationContext vctx;

  ConstraintTypeProvider(ConstraintValidationContext vctx) {
    this.vctx = vctx;
  }

  /**
   * Register {@code rootStruct} as the type behind {@link #ROOT_TYPE_NAME}.
   * Call once per env build, before {@link #findType}/{@link #findFieldType}
   * see queries from the checker.
   */
  void registerRoot(Schema rootStruct) {
    if (rootStruct.getType() != Schema.Type.STRUCT) {
      throw new IllegalArgumentException(
          "Root must be STRUCT, got " + rootStruct.getType());
    }
    typeNames.put(ROOT_TYPE_NAME, rootStruct);
    nameByStruct.put(rootStruct, ROOT_TYPE_NAME);
  }

  // ---------------------------------------------------------------------
  // TypeProvider — type-checking surface
  // ---------------------------------------------------------------------

  @Override
  public Type findType(String typeName) {
    if (typeNames.containsKey(typeName)) {
      return Decls.newObjectType(typeName);
    }
    return null;
  }

  @Override
  public FieldType findFieldType(String typeName, String fieldName) {
    Schema s = typeNames.get(typeName);
    if (s == null || s.getType() != Schema.Type.STRUCT) {
      return null;
    }
    Schema.Field field = s.getField(fieldName);
    if (field == null) {
      return null;
    }
    Type t = celTypeFor(field.getSchema());
    return new FieldType(t, target -> false, target -> null);
  }

  // ---------------------------------------------------------------------
  // No-op implementations — we only do type-checking, not evaluation.
  // ---------------------------------------------------------------------

  @Override
  public Val enumValue(String enumName) {
    // Mirror ProtoTypeRegistry: nessie's checker doesn't null-check this,
    // so we must return an Err Val (not null) for unknown enums. We have
    // no enum registry, so every lookup is "unknown".
    return org.projectnessie.cel.common.types.Err.newErr(
        "unknown enum name '%s'", enumName);
  }

  @Override
  public Val findIdent(String identName) {
    // Same null-safety contract — return null only when the checker
    // tolerates it. Built-in identifiers (true/false/null) are recognized
    // by the checker before consulting the provider.
    return null;
  }

  @Override
  public Val newValue(String typeName, Map<String, Val> fields) {
    // Runtime construction — strict-check path doesn't evaluate.
    return org.projectnessie.cel.common.types.Err.newErr(
        "newValue not supported by ConstraintTypeProvider");
  }

  @Override
  public Val nativeToValue(Object value) {
    // Runtime adapter — strict-check path doesn't evaluate.
    return null;
  }

  @Override
  public TypeRegistry copy() {
    ConstraintTypeProvider c = new ConstraintTypeProvider(vctx);
    c.typeNames.putAll(typeNames);
    c.nameByStruct.putAll(nameByStruct);
    return c;
  }

  @Override
  public void register(Object obj) {
    // No runtime registration — we drive types from the schema.
  }

  @Override
  public void registerType(org.projectnessie.cel.common.types.ref.Type... types) {
    // No-op — types come from the schema, not runtime registration.
  }

  // ---------------------------------------------------------------------
  // Schema → CEL type mapping
  // ---------------------------------------------------------------------

  /**
   * Map an LT {@link Schema} to a CEL declaration {@link Type}. Container
   * types recurse; STRUCT recurses via {@link #structTypeName} which
   * lazily registers a synthetic name.
   */
  Type celTypeFor(Schema s) {
    if (s == null) {
      return Decls.Dyn;
    }
    Schema resolved = s;
    if (s.getType() == Schema.Type.NAMED_TYPE_REF) {
      Schema r = vctx.resolveNamedType(s);
      if (r == null) {
        return Decls.Dyn;
      }
      resolved = r;
    }
    switch (resolved.getType()) {
      case BOOLEAN:
        return Decls.Bool;
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return Decls.Int;
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return Decls.Double;
      case CHAR:
      case VARCHAR:
        return Decls.String;
      case BINARY:
      case VARBINARY:
        return Decls.Bytes;
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return Decls.Timestamp;
      case ARRAY:
      case MULTISET:
        return Decls.newListType(celTypeFor(resolved.getElementType()));
      case MAP:
        return Decls.newMapType(
            celTypeFor(resolved.getKeyType()),
            celTypeFor(resolved.getValueType()));
      case STRUCT:
        return Decls.newObjectType(structTypeName(resolved));
      case ENUM:
      case UNION:
      case VARIANT:
      default:
        return Decls.Dyn;
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
    return name;
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
