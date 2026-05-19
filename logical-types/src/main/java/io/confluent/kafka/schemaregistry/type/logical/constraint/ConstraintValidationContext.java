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

import io.confluent.kafka.schemaregistry.type.logical.Schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Context passed to {@link ConstraintToCelTranslator} for parse-time validation
 * of CHECK constraint expressions.
 *
 * <p>Carries:
 * <ul>
 *   <li>The set of column names the expression may reference, mapped to
 *       their schemas. For column-level constraints this contains only the
 *       attached field; for table-level constraints, every field in the
 *       surrounding struct.</li>
 *   <li>Runtime-injected variables (currently only {@code now} for
 *       {@code CURRENT_TIMESTAMP}) — recognized for column-ref resolution
 *       but not type-checked since they're injected outside the LT's
 *       schema.</li>
 * </ul>
 *
 * <p>Used by the translator's strict validation pass (per design decision
 * 11) to reject unknown column refs, non-boolean root expressions, and
 * mismatched comparison types — all at parse time, with source-position
 * information in the error message.
 */
public final class ConstraintValidationContext {

  /** Built-in runtime-injected variables that don't resolve to a struct field. */
  private static final java.util.Set<String> RUNTIME_VARS =
      Collections.unmodifiableSet(new java.util.HashSet<>(
          java.util.Collections.singletonList("now")));

  /**
   * True if {@code name} is a runtime-injected variable (e.g. {@code now} for
   * {@code CURRENT_TIMESTAMP}). Used by macro arg validation to refuse iter-var
   * names that would shadow these runtime variables for the macro body's scope.
   */
  public static boolean isRuntimeVar(String name) {
    return RUNTIME_VARS.contains(name);
  }

  private final Map<String, Schema> columns;
  private final Map<String, Schema> bindings;
  /**
   * Map from qualified name → STRUCT/ENUM body for named types declared in
   * the surrounding script. Used by indirection validators to resolve
   * {@code NAMED_TYPE_REF} fields to their underlying STRUCT before
   * walking field access.
   */
  private final Map<String, Schema> namedTypes;
  /**
   * True for column-level CHECKs (a single attached field). False for
   * table-level CHECKs and the macro-derived contexts that inherit from
   * either. Drives the protovalidate {@code this} convention at emit time:
   * column-level column refs collapse to {@code this}; table-level refs
   * become {@code this.<col>}.
   */
  private final boolean isColumnLevel;

  private ConstraintValidationContext(
      Map<String, Schema> columns, Map<String, Schema> bindings,
      Map<String, Schema> namedTypes, boolean isColumnLevel) {
    this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    this.bindings = Collections.unmodifiableMap(new LinkedHashMap<>(bindings));
    this.namedTypes = namedTypes != null
        ? Collections.unmodifiableMap(new LinkedHashMap<>(namedTypes))
        : Collections.emptyMap();
    this.isColumnLevel = isColumnLevel;
  }

  /**
   * Returns a derived context with {@code name} added as a known
   * binding (e.g., a CEL macro iteration variable). Schema is unknown.
   * Type-aware passes will defer to runtime for refs to {@code name}.
   * Prefer {@link #withBinding(String, Schema)} when the type is known.
   */
  public ConstraintValidationContext withBinding(String name) {
    return withBinding(name, null);
  }

  /**
   * Returns a derived context with {@code name} bound to {@code type} —
   * e.g., a CEL macro iteration variable typed as the collection's
   * element type. A null {@code type} registers the name without type
   * info (validators can still resolve presence but not type).
   * Inherits {@link #isColumnLevel()} from the outer context.
   */
  public ConstraintValidationContext withBinding(String name, Schema type) {
    Map<String, Schema> next = new LinkedHashMap<>(bindings);
    next.put(name, type);
    return new ConstraintValidationContext(columns, next, namedTypes, isColumnLevel);
  }

  /**
   * Returns a derived context with {@code namedTypes} attached. Used by the
   * DDL visitor to make previously-declared named-type bodies available for
   * {@code NAMED_TYPE_REF} resolution during indirection validation.
   */
  public ConstraintValidationContext withNamedTypes(Map<String, Schema> types) {
    return new ConstraintValidationContext(columns, bindings, types, isColumnLevel);
  }

  /**
   * Resolve a {@code NAMED_TYPE_REF}'s qualified name to its underlying
   * STRUCT/ENUM body, or null if not registered. Returns {@code schema}
   * unchanged when {@code schema} is not a {@code NAMED_TYPE_REF}.
   */
  public Schema resolveNamedType(Schema schema) {
    if (schema == null || schema.getType() != Schema.Type.NAMED_TYPE_REF) {
      return schema;
    }
    return namedTypes.get(schema.getQualifiedName());
  }

  /**
   * Context for a column-level CHECK: only the attached field is in scope.
   */
  public static ConstraintValidationContext columnLevel(String fieldName, Schema fieldSchema) {
    Map<String, Schema> map = new LinkedHashMap<>();
    map.put(fieldName, fieldSchema);
    return new ConstraintValidationContext(
        map, java.util.Collections.emptyMap(),
        java.util.Collections.emptyMap(), true);
  }

  /**
   * Context for a table-level CHECK: all fields of the surrounding struct.
   */
  public static ConstraintValidationContext tableLevel(java.util.List<Schema.Field> fields) {
    Map<String, Schema> map = new LinkedHashMap<>();
    for (Schema.Field f : fields) {
      map.put(f.getName(), f.getSchema());
    }
    return new ConstraintValidationContext(
        map, java.util.Collections.emptyMap(),
        java.util.Collections.emptyMap(), false);
  }

  /**
   * True if this context corresponds to a column-level CHECK (the attached
   * field is the only thing in scope). False for table-level. Macro
   * derivations inherit the outer context's value.
   */
  public boolean isColumnLevel() {
    return isColumnLevel;
  }

  /**
   * True when {@code name} resolves to the column-level CHECK's constrained
   * column — i.e., the placement is column-level, the name appears in the
   * schema column map, AND it is not shadowed by a macro iter-var binding.
   * Used by the skip-on-null contract: only the constrained column gets the
   * "this is non-null at evaluation" guarantee; iter-var bindings inside
   * macro bodies retain normal null semantics.
   */
  public boolean isConstrainedColumn(String name) {
    return isColumnLevel
        && columns.containsKey(name)
        && !bindings.containsKey(name);
  }

  /**
   * True if {@code name} resolves to a known schema column in this scope.
   * Bindings shadow columns: an iter-var named the same as a column is
   * NOT a column, so the protovalidate {@code this}/{@code this.x} prefix
   * must NOT be applied to it. Used at emit time by {@code visitColumnRef}.
   */
  public boolean isColumn(String name) {
    return !bindings.containsKey(name) && columns.containsKey(name);
  }

  /**
   * Read-only view of the registered column → schema map. Used by the
   * strict CEL checker to build the synthetic {@code this} struct type.
   */
  Map<String, Schema> columns() {
    return columns;
  }

  /**
   * True if {@code name} is a valid root identifier in this context — a
   * known column, an in-scope binding (e.g., macro iteration var), or a
   * runtime-injected variable.
   */
  public boolean isKnown(String name) {
    return columns.containsKey(name)
        || bindings.containsKey(name)
        || RUNTIME_VARS.contains(name);
  }

  /**
   * Schema for a known column or typed binding. Bindings shadow columns
   * (an iter-var declaration overrides the outer column with the same
   * name for the duration of the macro body). Returns null when
   * {@code name} is a runtime-injected variable, an untyped binding, or
   * unknown.
   */
  public Schema schemaOf(String name) {
    if (bindings.containsKey(name)) {
      return bindings.get(name);  // null for untyped bindings — that's intentional
    }
    return columns.get(name);
  }

  /**
   * Sorted list of valid column names for error message reporting. Excludes
   * runtime variables.
   */
  public String allowedNames() {
    if (columns.isEmpty()) {
      return "(none)";
    }
    return String.join(", ", columns.keySet());
  }
}
