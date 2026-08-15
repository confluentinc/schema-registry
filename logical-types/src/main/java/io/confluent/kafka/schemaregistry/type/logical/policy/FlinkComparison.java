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

import io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.FieldView;
import io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.Kind;
import io.confluent.kafka.schemaregistry.type.logical.policy.Incompatibility.Rule;

import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.childPath;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.describeChange;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.elementOf;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.fieldViews;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.isRef;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.refKey;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.resolve;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.scaleOf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * One comparison run in Flink mode.
 *
 * <p>Mirrors {@link IcebergComparison} rather than sharing a base class with it. The traversal
 * skeleton is similar, but the three rules that matter all differ — which fields count as optional,
 * how leaves are compared, and whether a MULTISET is a MAP — and each class must stay readable
 * against the specification it implements. A common walk would couple them and make neither
 * diffable against its own reference.
 *
 * <h3>The governing criterion</h3>
 *
 * <p>A change is rejected when it is not a <b>legal Flink SQL table evolution</b> — semantically
 * valid at both DDL and DML time, <em>whatever the underlying storage</em>. That reduces to one
 * test:
 *
 * <blockquote>Does the change require a stored value to be <b>reinterpreted</b>, or to be
 * <b>invented</b>?</blockquote>
 *
 * <p>If neither, it is legal. Flink accepts almost any {@code ALTER TABLE}, so DDL legality decides
 * nothing on its own, and how an encoding locates a value belongs to the format-level checker. What
 * is left is whether every legitimately written value can still be read as itself.
 *
 * <p>The retained rules are exactly those that fail the test.
 * {@link Rule#REQUIRED_FIELD_ADDED} invents; {@link Rule#NULLABLE_TO_NON_NULLABLE} reinterprets a
 * written null; {@link Rule#TYPE_MISMATCH} and {@link Rule#UNSUPPORTED_TYPE_CHANGE} reinterpret at
 * the kind and leaf level. The parameter guards belong to the last of those, because a decimal
 * scale, a temporal precision and a CHAR length each select how stored bytes are read.
 *
 * <p><b>Do not reintroduce a rule that fails this test.</b> Drops, reordering, renames, map-key
 * changes and enum symbol removal were each rejected here once and removed on this argument: a drop
 * and a reorder touch no value, a rename's byte-location question belongs to the format, a map key
 * is an ordinary value, and an ENUM derives to VARCHAR so dropping a symbol does not change the
 * Flink type at all. Enum removal looked like a Flink problem only because Avro resolves an unknown
 * symbol to the enum's default — Avro's behaviour, and the Avro checker's to catch.
 */
final class FlinkComparison {

  private final Schema originalRoot;
  private final Schema updateRoot;
  private final Map<String, Schema> originalNamedTypes;
  private final Map<String, Schema> updateNamedTypes;


  private final List<Incompatibility> findings = new ArrayList<>();

  /**
   * See {@code IcebergComparison#comparedRefPairs}.
   */
  private final Set<String> comparedRefPairs = new HashSet<>();

  /**
   * See {@code IcebergComparison#comparedStructPairs}.
   */
  private final Map<Schema, Set<Schema>> comparedStructPairs = new IdentityHashMap<>();

  FlinkComparison(LogicalType original, LogicalType update) {
    this.originalRoot = original.getRootSchema();
    this.updateRoot = update.getRootSchema();
    this.originalNamedTypes = original.getNamedTypes();
    this.updateNamedTypes = update.getNamedTypes();
  }

  CompatibilityResult run() {
    compareTypes(originalRoot, updateRoot, "");
    return CompatibilityResult.of(findings);
  }

  private void add(Rule rule, String path, String message) {
    findings.add(new Incompatibility(rule, path, message));
  }

  private boolean claimStructPair(Schema original, Schema update) {
    return comparedStructPairs
        .computeIfAbsent(original, key -> Collections.newSetFromMap(new IdentityHashMap<>()))
        .add(update);
  }

  // -- dispatch --------------------------------------------------------------------------------

  private void compareTypes(
      Schema original, Schema update, String path) {
    if (isRef(original) || isRef(update)) {
      // Dedup only when BOTH sides are refs. refKey returns "" for a non-ref, so keying on a
      // one-sided ref collapses every inline counterpart onto the same key: the first
      // (inline, ref X) pair claims it and every later one returns without comparing anything,
      // silently dropping findings. Inline-to-$ref is ordinary JSON Schema evolution.
      if (isRef(original) && isRef(update)) {
        final String pairKey = refKey(original) + ' ' + refKey(update);
        if (!comparedRefPairs.add(pairKey)) {
          return;
        }
      }
      checkCompatibilityRecursive(
          resolve(original, originalNamedTypes),
          resolve(update, updateNamedTypes),
          path);
      return;
    }
    checkCompatibilityRecursive(original, update, path);
  }

  private void checkCompatibilityRecursive(
      Schema original, Schema update, String path) {
    if (isRef(original) || isRef(update)) {
      if (!isRef(original) || !isRef(update)
          || !original.getQualifiedName().equals(update.getQualifiedName())) {
        add(Rule.TYPE_MISMATCH, path, describeChange(original, update));
      }
      return;
    }

    Kind originalKind = flinkKindOf(original);
    Kind updateKind = flinkKindOf(update);
    if (originalKind != updateKind) {
      add(Rule.TYPE_MISMATCH, path, describeChange(original, update));
      return;
    }

    switch (originalKind) {
      case STRUCT:
        if (claimStructPair(original, update)) {
          validateStructs(fieldViews(original), fieldViews(update), path);
        }
        break;
      case LIST:
        validateLists(original, update, path);
        break;
      case MULTISET:
        validateMultisets(original, update, path);
        break;
      case MAP:
        validateMaps(original, update, path);
        break;
      case PRIMITIVE:
        validatePrimitives(original, update, path);
        break;
      default:
        throw new IllegalStateException("Unhandled kind: " + originalKind);
    }
  }

  // -- structs ---------------------------------------------------------------------------------

  /**
   * Field-level rules: added columns and nullability. Columns are matched by name, and one
   * present only in the original is simply not read.
   *
   * <p><b>Deliberately narrower than {@code IcebergComparison#validateStructs}</b>, which also
   * rejects drops and reordering. Under the reinterpret-or-invent test, a drop and a reorder touch
   * no value and are both supported Flink table changes, so both are legal here; Iceberg rejects
   * them only because it cannot identify a column without a stable field ID. An added column does
   * invent — a NOT NULL column with no written value cannot be satisfied by any storage — so it
   * stays rejected unless nullable or defaulted.
   *
   * <p>A <b>rename</b> therefore produces no finding here, intentionally: whether old bytes can be
   * located under the new name is a question about the encoding, which the format-level checker
   * owns.
   */
  private void validateStructs(
      List<FieldView> originalFields, List<FieldView> updateFields, String path) {

    final Map<String, FieldView> originalFieldMap = originalFields.stream()
        .collect(Collectors.toMap(field -> field.name, field -> field));

    int updatePosition = -1;
    for (FieldView updateField : updateFields) {
      updatePosition++;
      final String fieldPath = childPath(path, updateField.name);
      // Struct fields are the one place both converters agree on the index convention: the
      // field's position within its struct, appended to the parent's path.
      final FieldView originalField = originalFieldMap.get(updateField.name);

      // A new column must be readable for rows written before it existed.
      if (originalField == null) {
        if (!isNullableOrDefaulted(updateField)) {
          add(Rule.REQUIRED_FIELD_ADDED, fieldPath,
              "added column is neither nullable nor defaulted, so rows written before it existed "
                  + "have no value for it");
        }
        continue;
      }

      // Tightening nullability would reject rows that already hold nulls. Deliberately blind to
      // defaults: a default rescues an *absent* field, and says nothing about a field that is
      // present and holds null, which is exactly what a nullable column permits.
      if (isEffectivelyNullable(originalField) && !isEffectivelyNullable(updateField)) {
        add(Rule.NULLABLE_TO_NON_NULLABLE, fieldPath,
            "column was nullable and is now NOT NULL; pre-existing rows may hold nulls");
      }

      validateDefaultNotRemoved(originalField, updateField, fieldPath);

      compareTypes(originalField.schema, updateField.schema, fieldPath);
    }
  }

  /**
   * The added-column predicate. Unlike Iceberg mode there is no container restriction: a Flink
   * column may be NOT NULL and carry a default whatever its type.
   *
   * <p>The default must be <b>non-null</b>, for the same reason it must be under Iceberg v3 — a
   * NOT NULL column whose default is null supplies null to every row written before it existed,
   * precisely the value that column cannot hold.
   */
  private static boolean isNullableOrDefaulted(FieldView field) {
    return isEffectivelyNullable(field) || hasNonNullDefault(field);
  }

  /**
   * The other half of tightening an optional column, and the counterpart to
   * {@link Rule#NULLABLE_TO_NON_NULLABLE}.
   *
   * <p>A NOT NULL column carrying a non-null default means precisely "may be absent, read the
   * default". Rows may therefore legitimately lack it. Take the default away and those rows have
   * no value at all, and the runtime has to <b>invent</b> one — the same failure as adding a
   * required column, arrived at from the other direction.
   *
   * <p>Only when the column is <b>not nullable</b> in the update. A nullable column that loses
   * its default is fine: an absent field falls back to null, which it accepts.
   *
   * <p>Changing a default from one non-null value to another is <em>not</em> reported. It does
   * change what an absent field reads as, but no stored value is reinterpreted; whether that
   * deserves a rule of its own is open.
   *
   * <p><b>Iceberg modes deliberately excluded.</b> The reference checker has no such rule, and
   * the pairwise Iceberg rules are a faithful 1:1 port; adding one there would be the first
   * divergence on that side and is a separate decision.
   *
   * <p>Both sides read {@link Schema.Field}, so a field that still carries a derived default does
   * not trip this. A derived default cannot be removed by editing a schema without also changing
   * the type, which the type rules already catch.
   */
  private void validateDefaultNotRemoved(
      FieldView originalField, FieldView updateField, String fieldPath) {
    if (isEffectivelyNullable(originalField) || isEffectivelyNullable(updateField)) {
      return;
    }
    final boolean originalHadDefault =
        originalField.hasDefault && originalField.defaultValue != null;
    if (originalHadDefault && !hasNonNullDefault(updateField)) {
      add(Rule.NON_NULLABLE_DEFAULT_REMOVED, fieldPath,
          "NOT NULL column lost its default; rows that omitted the column have no value to read");
    }
  }

  /**
   * Whether the default is present <em>and</em> non-null. Mirrors
   * {@code IcebergComparison#hasNonNullDefault}; kept separate so each class stays diffable against
   * its own reference.
   *
   * <p>Both kinds count: a user-declared default, and one a converter derived from format semantics
   * (an absent {@code repeated} field is an empty list). Both live on {@link Schema.Field}; derived
   * ones are flagged so writers skip them, but they make a field just as readable.
   */
  private static boolean hasNonNullDefault(FieldView field) {
    return field.hasDefault && field.defaultValue != null;
  }

  /**
   * Compares a container's child — an array or multiset element, a map key or value — checking
   * nullability on the way in.
   *
   * <p>{@link #validatePrimitives} leaves nullability alone because {@link #validateStructs}
   * covers it, which holds for a struct field but not for a container child: that reaches the leaf
   * without passing through a field, so without this, {@code ARRAY<INT>} tightening to
   * {@code ARRAY<INT NOT NULL>} was accepted while the identical change on a column was rejected.
   *
   * <p><b>Not mirrored in {@link IcebergComparison}</b>, which has the same gap faithfully: there
   * element and value optionality lives on the container rather than the child type, and the
   * reference never reads it. Closing it would be a divergence from the port.
   */
  private void compareChild(
      Schema original, Schema update, String path) {
    if (original.isNullable() && !update.isNullable()) {
      add(Rule.NULLABLE_TO_NON_NULLABLE, path,
          "container child was nullable and is now NOT NULL; existing collections may hold "
              + "nulls");
    }
    compareTypes(original, update, path);
  }

  /** Container recursion for ARRAY. */
  private void validateLists(
      Schema original, Schema update, String path) {
    compareChild(elementOf(original), elementOf(update), path + "[]");
  }

  /**
   * Container recursion for MULTISET, which Flink models as its own type rather than a MAP.
   */
  private void validateMultisets(
      Schema original, Schema update, String path) {
    compareChild(elementOf(original), elementOf(update), path + "[]");
  }

  /**
   * Container recursion for MAP. Key and value both go through the ordinary type comparison, so
   * each may widen as far as a leaf elsewhere may.
   *
   * <p><b>Deliberately unlike {@code IcebergComparison#validateMaps}</b>, which freezes the key and
   * reports {@link Rule#MAP_KEY_TYPE_MISMATCH} for any change. The reason does not carry over:
   * Iceberg gives a map key its own field ID and identifies it by that, so re-typing redefines the
   * field, whereas Flink's MAP has no such identity and a key is just another value. An unsafe key
   * change is still rejected — as the leaf rule that applies, at the {@code {key}} path.
   */
  private void validateMaps(
      Schema original, Schema update, String path) {
    // Nullability is checked on the key too, because SRLT can express a nullable map key even
    // though no converter emits one.
    compareChild(original.getKeyType(), update.getKeyType(), path + "{key}");
    compareChild(original.getValueType(), update.getValueType(), path + "{}");
  }

  private static boolean isEffectivelyNullable(FieldView field) {
    return field.forcedNullable || field.schema.isNullable();
  }

  // -- primitives ------------------------------------------------------------------------------

  /**
   * Leaf type changes. Two independent parts, both required.
   *
   * <p>Part A takes the root relation from {@link FlinkLogicalTypeCasts}, so a safe type change is
   * Flink's own notion rather than a hand-picked one. Nullability is excluded, being checked on the
   * way in to every leaf instead — {@link #validateStructs} for a field, {@link #compareChild} for
   * a container child. <b>Both of those are needed</b>: a container child reaches here without
   * passing through a field.
   *
   * <p>Part B applies the parameter guards Part A structurally cannot: Flink's table is keyed by
   * type root and never reads a length, precision or scale, so alone it would admit
   * {@code VARCHAR(50) -> VARCHAR(10)}. A hole rather than a policy — see
   * {@link FlinkLogicalTypeCasts}. <b>Do not remove these as redundant.</b>
   */
  private void validatePrimitives(Schema original, Schema update, String path) {
    FlinkLogicalTypeCasts.Root originalRootType = flinkRootOf(original);
    FlinkLogicalTypeCasts.Root updateRootType = flinkRootOf(update);

    // Part A -- the root relation, straight from Flink's table.
    if (!FlinkLogicalTypeCasts.supportsImplicitCast(originalRootType, updateRootType)) {
      add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update));
      return;
    }

    // Part B -- the parameters Flink's table does not look at.
    ParamKind originalParams = paramKindOf(originalRootType);
    ParamKind updateParams = paramKindOf(updateRootType);

    // A fixed-length type may widen into a variable-length one if the bound still covers it.
    if (originalParams == ParamKind.FIXED_LENGTH
        && updateParams == ParamKind.VARIABLE_LENGTH) {
      if (flinkLengthOf(update) < flinkLengthOf(original)) {
        add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
            + " (length may not shrink)");
      }
      return;
    }

    if (originalParams == updateParams) {
      switch (originalParams) {
        case FIXED_LENGTH:
          // Not a bound: the length is the value's stored width. CHAR right-pads and BINARY is a
          // fixed byte count, so widening rewrites every historical value rather than admitting
          // wider ones. Avro resolution likewise requires an identical `fixed` size.
          if (flinkLengthOf(update) != flinkLengthOf(original)) {
            add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
                + " (a fixed-length type cannot change length)");
          }
          return;
        case VARIABLE_LENGTH:
          if (flinkLengthOf(update) < flinkLengthOf(original)) {
            add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
                + " (length may not shrink)");
          }
          return;
        case PRECISION:
          // Frozen, not merely non-shrinking, for the same reason decimal scale is: the precision
          // selects the unit of the stored integer. An Avro timestamp-millis field re-annotated
          // as timestamp-micros keeps its bytes and its column type but every value is then read
          // a thousandfold out.
          if (update.getPrecision() != original.getPrecision()) {
            add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
                + " (precision selects the unit of the stored value and cannot change)");
          }
          return;
        case PRECISION_AND_SCALE:
          if (scaleOf(update) != scaleOf(original)
              || update.getPrecision() < original.getPrecision()) {
            add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
                + " (decimal scale must be unchanged and precision may not shrink)");
          }
          return;
        default:
          return;
      }
    }

    // An integer widening into a DECIMAL must leave room for the integer's whole range.
    if (updateParams == ParamKind.PRECISION_AND_SCALE) {
      int digitsNeeded = decimalDigitsOf(originalRootType);
      if (digitsNeeded > 0
          && update.getPrecision() - scaleOf(update) < digitsNeeded) {
        add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
            + " (target cannot represent the full range of the source)");
      }
    }
  }

  /** Which parameters a Flink type root carries, and therefore which guard applies to it. */
  private enum ParamKind {
    NONE,
    /** CHAR and BINARY: the declared length is the stored length. */
    FIXED_LENGTH,
    /** VARCHAR and VARBINARY: the declared length is an upper bound. */
    VARIABLE_LENGTH,
    /**
     * TIME and TIMESTAMP. The precision is not merely a bound: for an Avro logical type it selects
     * the unit of the stored integer, so changing it reinterprets every historical value.
     */
    PRECISION,
    /** DECIMAL, where precision is a bound but scale selects the unit. */
    PRECISION_AND_SCALE
  }

  /**
   * The Flink shape of an SRLT type.
   *
   * <p>Differs from {@link #kindOf} in one respect that matters: Flink has a MULTISET type of its
   * own, so a MULTISET and a MAP are different types here, where Iceberg lowers both to a map.
   */
  private static Kind flinkKindOf(Schema schema) {
    switch (schema.getType()) {
      case STRUCT:
      case UNION:
        return Kind.STRUCT;
      case ARRAY:
        return Kind.LIST;
      case MULTISET:
        return Kind.MULTISET;
      case MAP:
        return Kind.MAP;
      default:
        return Kind.PRIMITIVE;
    }
  }

  /**
   * Maps an SRLT primitive onto the Flink type root it derives to.
   *
   * <p>Only one erasure applies: Flink has no enum type, so an ENUM derives to an unbounded
   * VARCHAR. Adding or removing enum symbols therefore does not change the Flink type. Everything
   * else keeps its identity and its parameters, which is the whole reason this mode exists
   * alongside Iceberg's.
   */
  private static FlinkLogicalTypeCasts.Root flinkRootOf(Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return FlinkLogicalTypeCasts.Root.BOOLEAN;
      case TINYINT:
        return FlinkLogicalTypeCasts.Root.TINYINT;
      case SMALLINT:
        return FlinkLogicalTypeCasts.Root.SMALLINT;
      case INT:
        return FlinkLogicalTypeCasts.Root.INTEGER;
      case BIGINT:
        return FlinkLogicalTypeCasts.Root.BIGINT;
      case FLOAT:
        return FlinkLogicalTypeCasts.Root.FLOAT;
      case DOUBLE:
        return FlinkLogicalTypeCasts.Root.DOUBLE;
      case DECIMAL:
        return FlinkLogicalTypeCasts.Root.DECIMAL;
      case CHAR:
        return FlinkLogicalTypeCasts.Root.CHAR;
      case VARCHAR:
      case ENUM:
        return FlinkLogicalTypeCasts.Root.VARCHAR;
      case BINARY:
        return FlinkLogicalTypeCasts.Root.BINARY;
      case VARBINARY:
        return FlinkLogicalTypeCasts.Root.VARBINARY;
      case DATE:
        return FlinkLogicalTypeCasts.Root.DATE;
      case TIME:
        return FlinkLogicalTypeCasts.Root.TIME;
      case TIMESTAMP:
        return FlinkLogicalTypeCasts.Root.TIMESTAMP;
      case TIMESTAMP_LTZ:
        return FlinkLogicalTypeCasts.Root.TIMESTAMP_LTZ;
      case VARIANT:
        return FlinkLogicalTypeCasts.Root.VARIANT;
      default:
        throw new IllegalStateException("Not a Flink primitive: " + schema.getType());
    }
  }

  /**
   * The length of the Flink type an SRLT schema derives to.
   *
   * <p>Needed because the derivation is not always length-preserving: an ENUM derives to an
   * unbounded VARCHAR and carries no length of its own, so asking the SRLT schema directly throws.
   */
  private static int flinkLengthOf(Schema schema) {
    switch (schema.getType()) {
      case ENUM:
        // No enum type in Flink; an ENUM derives to an unbounded VARCHAR.
        return Schema.MAX_LENGTH;
      case VARCHAR:
      case VARBINARY: {
        // Mirror the SR-LT-to-Flink shim, which treats a non-positive length on a variable-length
        // type as "unbounded" rather than passing it through. Without this a VARCHAR(0) reads here
        // as length 0 while the derived Flink type is VARCHAR(MAX), inverting the verdict in both
        // directions.
        final int length = schema.getLength();
        return length > 0 ? length : Schema.MAX_LENGTH;
      }
      default:
        // CHAR and BINARY pass their length through in the shim too: it is the stored width, and a
        // zero-width fixed type is not reinterpreted as unbounded.
        return schema.getLength();
    }
  }

  private static ParamKind paramKindOf(FlinkLogicalTypeCasts.Root root) {
    switch (root) {
      case CHAR:
      case BINARY:
        return ParamKind.FIXED_LENGTH;
      case VARCHAR:
      case VARBINARY:
        return ParamKind.VARIABLE_LENGTH;
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return ParamKind.PRECISION;
      case DECIMAL:
        return ParamKind.PRECISION_AND_SCALE;
      default:
        return ParamKind.NONE;
    }
  }

  /**
   * Decimal digits needed to hold every value of an integer root, or {@code 0} if the root is not
   * an integer. Used to check that an integer widening into a DECIMAL leaves room for its whole
   * range.
   */
  private static int decimalDigitsOf(FlinkLogicalTypeCasts.Root root) {
    switch (root) {
      case TINYINT:
        return 3;
      case SMALLINT:
        return 5;
      case INTEGER:
        return 10;
      case BIGINT:
        return 19;
      default:
        return 0;
    }
  }
}
