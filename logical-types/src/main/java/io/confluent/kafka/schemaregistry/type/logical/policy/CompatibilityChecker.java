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

package io.confluent.kafka.schemaregistry.type.logical.policy;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;

import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker.Mode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks whether one {@link LogicalType} can evolve into another for a given downstream consumer.
 *
 * <p>Direction is BACKWARD: the {@code update} schema must be able to read data written with the
 * {@code original} schema. All violations are collected; see {@link CompatibilityResult}.
 *
 * <p>{@link LogicalTypeChecker} is the entry point. This class dispatches to one implementation
 * per mode — {@link IcebergComparison} and {@link FlinkComparison} — and holds the walk primitives
 * they share: {@link Kind}, the {@link FieldView} abstraction over struct fields and union
 * branches, named-type resolution and rendering.
 *
 * <p><b>The two implementations deliberately do not share a base class.</b> Each has to stay
 * diffable against the specification it implements, and a common walk would couple them and make
 * neither. What lives here belongs to neither reference: the impedance layer between their input
 * and SRLT, which is more expressive than either — and historically where this package's defects
 * have been, so it is worth testing directly rather than only through a full comparison.
 *
 * <p><b>This check is only half of the question.</b> It takes two schemas, so it says nothing about
 * the first schema registered on a subject, and nothing about whether either schema is usable by
 * the consumer on its own terms — a decimal whose precision exceeds what the consumer supports is
 * unusable whether or not it changed. {@link ValidityChecker} answers that half, and a caller
 * registering a schema should run both.
 */
final class CompatibilityChecker {

  /** The multiset-to-map encoding uses a non-null INT count as the map value. */
  private static final Schema MULTISET_COUNT_TYPE =
      Schema.create(Schema.Type.INT).setNullable(false);

  private CompatibilityChecker() {
  }

  /**
   * Compares {@code original} against {@code update} under {@code mode}.
   *
   * @param mode     which consumer's rules to apply
   * @param original the currently registered schema
   * @param update   the proposed schema
   * @return every violation found, or {@link CompatibilityResult#compatible()} if there are none
   * @throws UnsupportedOperationException if {@code mode} is not yet implemented
   */
  public static CompatibilityResult compare(
      Mode mode, LogicalType original, LogicalType update) {
    Objects.requireNonNull(mode, "mode");
    Objects.requireNonNull(original, "original");
    Objects.requireNonNull(update, "update");

    switch (mode) {
      case FLINK:
        return new FlinkComparison(original, update).run();
      case ICEBERG_V2:
        return new IcebergComparison(original, update, 2).run();
      case ICEBERG_V3:
        return new IcebergComparison(original, update, 3).run();
      default:
        throw new IllegalArgumentException("Unknown mode: " + mode);
    }
  }

  /**
   * The structural shapes a target type system models, after erasing the SRLT types that map onto
   * them. {@code MULTISET} is only ever produced by Flink mode — Iceberg has no multiset and lowers
   * one to a {@code MAP}.
   */
  enum Kind {
    STRUCT,
    LIST,
    MAP,
    MULTISET,
    PRIMITIVE
  }

  // ---------------------------------------------------------------------------------------------
  // Structural accessors (applying the UNION and MULTISET lowering)
  // ---------------------------------------------------------------------------------------------

  /**
   * A struct member, abstracting over STRUCT fields and UNION branches so the struct rules apply to
   * both without synthesising {@link Schema.Field} instances.
   */
  static final class FieldView {

    final String name;
    final Schema schema;
    final boolean hasDefault;

    /**
     * The declared default, or {@code null}. Distinct from {@link #hasDefault}: v3 requires a
     * non-null default when a required field is added, so presence alone is not enough.
     */
    final Object defaultValue;

    /** Union branches are always optional: at most one branch is populated per record. */
    final boolean forcedNullable;

    private FieldView(String name, Schema schema, boolean hasDefault, Object defaultValue,
        boolean forcedNullable) {
      this.name = name;
      this.schema = schema;
      this.hasDefault = hasDefault;
      this.defaultValue = defaultValue;
      this.forcedNullable = forcedNullable;
    }
  }

  /**
   * The members of a struct or union, <b>in list order</b>.
   *
   * <p>List order, deliberately — not {@link Schema.Field#getPosition()}, which is unreliable: a
   * Protobuf message containing a {@code oneof} reports duplicated and out-of-order positions,
   * because the {@code oneof} is hoisted to the end of the field list while positions stay put.
   * Field order is what {@code IcebergComparison} compares to detect a reorder, so switching would
   * report a spurious one on any message with a {@code oneof}.
   */
  static List<FieldView> fieldViews(Schema schema) {
    List<FieldView> views = new ArrayList<>();
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema.UnionBranch branch : schema.getBranches()) {
        views.add(new FieldView(branch.getName(), branch.getSchema(), false, null, true));
      }
      return views;
    }
    for (Schema.Field field : schema.getFields()) {
      views.add(new FieldView(field.getName(), field.getSchema(),
          field.hasDefaultValue(), field.getDefaultValue(), false));
    }
    return views;
  }

  static Schema elementOf(Schema schema) {
    return schema.getElementType();
  }

  static Schema keyOf(Schema schema) {
    // A MULTISET is a MAP from element to occurrence count.
    return schema.getType() == Schema.Type.MULTISET
        ? schema.getElementType()
        : schema.getKeyType();
  }

  static Schema valueOf(Schema schema) {
    return schema.getType() == Schema.Type.MULTISET
        ? MULTISET_COUNT_TYPE
        : schema.getValueType();
  }

  // ---------------------------------------------------------------------------------------------
  // Named type references
  // ---------------------------------------------------------------------------------------------

  static boolean isRef(Schema schema) {
    return schema.getType() == Schema.Type.NAMED_TYPE_REF;
  }

  static String refKey(Schema schema) {
    return isRef(schema) ? schema.getQualifiedName() : "";
  }

  /**
   * Follows named-type references to their definition. Returns the reference itself when it cannot
   * be resolved — an external type, which the caller then compares by qualified name.
   */
  static Schema resolve(Schema schema, Map<String, Schema> namedTypes) {
    Schema current = schema;
    Set<String> seen = new HashSet<>();
    while (isRef(current)) {
      String name = current.getQualifiedName();
      if (!seen.add(name)) {
        return current;
      }
      Schema target = namedTypes.get(name);
      if (target == null) {
        return current;
      }
      current = target;
    }
    return current;
  }

  // ---------------------------------------------------------------------------------------------
  // Rendering
  // ---------------------------------------------------------------------------------------------


  static String childPath(String parentPath, String fieldName) {
    return parentPath.isEmpty() ? fieldName : parentPath + '.' + fieldName;
  }

  static String describeChange(Schema original, Schema update) {
    return "type changed from " + render(original) + " to " + render(update);
  }

  /**
   * A DECIMAL's scale, with {@link Schema#NO_PARAM} read as {@code 0}.
   *
   * <p>SRLT preserves {@code NO_PARAM} so {@code DECIMAL(p)} round-trips through DDL, making the
   * sentinel legitimate input here, and SQL reads {@code DECIMAL(p)} as {@code DECIMAL(p, 0)}.
   * Comparing it raw rejects the no-op {@code DECIMAL(10) -> DECIMAL(10, 0)} and, worse, makes an
   * integer widening look safe: {@code precision - (-1)} overstates the integer digits by one, so
   * {@code BIGINT -> DECIMAL(18)} passes while {@code BIGINT -> DECIMAL(18, 0)} is rejected.
   */
  static int scaleOf(Schema decimal) {
    final int scale = decimal.getScale();
    return scale == Schema.NO_PARAM ? 0 : scale;
  }

  static String render(Schema schema) {
    switch (schema.getType()) {
      case DECIMAL:
        return "DECIMAL(" + schema.getPrecision() + ", " + scaleOf(schema) + ")";
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        return schema.getType() + "(" + schema.getLength() + ")";
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return schema.getType() + "(" + schema.getPrecision() + ")";
      case NAMED_TYPE_REF:
        return "ref(" + schema.getQualifiedName() + ")";
      default:
        return schema.getType().toString();
    }
  }
}
