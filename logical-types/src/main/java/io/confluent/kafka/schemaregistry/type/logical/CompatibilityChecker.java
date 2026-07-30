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

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.type.logical.Incompatibility.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Checks whether one {@link LogicalType} can evolve into another for a given downstream consumer.
 *
 * <p>Direction is BACKWARD: the {@code update} schema must be able to read data written with the
 * {@code original} schema. All violations are collected; see {@link CompatibilityResult}.
 *
 * <h2>Mode.ICEBERG</h2>
 *
 * <p>Applies the evolution rules that govern materialising a registered schema into an Apache
 * Iceberg table. An equivalent checker operating directly on {@code org.apache.iceberg.Schema} is
 * maintained elsewhere; this one is expected to reach the same verdicts from an SRLT input.
 *
 * <p><b>Both are live, so keep them in step.</b> The recursion shape and method names here mirror
 * that implementation's — {@code checkCompatibilityRecursive} dispatching to
 * {@code validateStructs}, {@code validateLists}, {@code validateMaps} and
 * {@code validatePrimitives} — so the two can be diffed side by side. Three things are folded in
 * here rather than kept separate: the container-nullability relaxation (in
 * {@code isEffectivelyOptional}), Iceberg's promotion table from
 * {@code TypeUtil#isPromotionAllowed} (in {@code validatePrimitives}), and the Flink-to-Iceberg
 * type mapping from Iceberg's own {@code FlinkTypeToType} (in {@link #icebergClassOf}).
 *
 * <p>Intentional departures, each noted at its site: findings accumulate rather than throwing
 * on the first violation; the nullability relaxation is a predicate rather than a rewrite; and
 * named-type references, with the cycle guards they require, have no counterpart at all because
 * Iceberg schemas cannot recurse.
 *
 * <p><b>These rules are deliberately stricter than the Iceberg spec.</b> The spec permits deleting,
 * renaming, and reordering struct fields, because Iceberg identifies fields by a stable field ID —
 * a rename keeps the ID and a reorder is only a position change. A {@link LogicalType} carries no
 * field IDs, so this checker compares two schemas <em>by name</em> and cannot distinguish a rename
 * from a delete-plus-add, nor tell which of two orderings is newer. It therefore rejects all three.
 * Do not "relax" these to match the spec without first solving field-ID continuity.
 *
 * <p>The one rule that <em>is</em> a genuine Iceberg constraint is
 * {@link Rule#REQUIRED_FIELD_ADDED}: {@code initial-default} and {@code write-default} arrived in
 * spec v3, and the target tables pin format-version 2, so a newly added required field has no
 * value for pre-existing rows. Adding a field to a nested struct is allowed by the spec and is
 * allowed here too (subject to the same optional-or-defaulted requirement at every level).
 *
 * <p>Type comparison erases the distinctions Iceberg does not model — see
 * {@link #icebergClassOf}. Rather than materialising a converted schema, types are compared through
 * equivalence classes, so {@code SMALLINT -> BIGINT} passes as {@code int -> long} and
 * {@code VARCHAR(50) -> VARCHAR(10)} passes because Iceberg has no string length. A Flink-level
 * checker is responsible for the distinctions erased here.
 *
 * <p>The equivalence classes are taken from Iceberg's own {@code FlinkTypeToType}, which is the
 * conversion applied before comparing there, so this checker agrees with it. Keep the two in
 * step. Notably: {@code BINARY(n)} becomes {@code fixed(n)} and so keeps its length, while
 * {@code CHAR} and {@code VARCHAR} lose theirs; {@code MULTISET<T>} becomes {@code map<T, int>};
 * {@code TIME(p)} erases precision at every value because Iceberg has no nanosecond time type; and
 * a timestamp with precision above {@value #MAX_ICEBERG_MICROS_PRECISION} becomes the nanosecond
 * timestamp type rather than being rejected.
 */
public final class CompatibilityChecker {

  /**
   * Precision above which a Flink timestamp maps to Iceberg's nanosecond timestamp rather than its
   * microsecond one. Mirrors the threshold in Iceberg's own {@code FlinkTypeToType}.
   */
  private static final int MAX_ICEBERG_MICROS_PRECISION = 6;

  /** Highest decimal precision Iceberg supports. */
  private static final int MAX_ICEBERG_DECIMAL_PRECISION = 38;

  /** The multiset-to-map encoding uses a non-null INT count as the map value. */
  private static final Schema MULTISET_COUNT_TYPE =
      Schema.create(Schema.Type.INT).setNullable(false);

  /** The downstream consumer whose evolution rules should be applied. */
  public enum Mode {
    /**
     * Materialization into an Apache Iceberg table. Stricter than the Iceberg spec; see the class
     * javadoc.
     */
    ICEBERG,

    /**
     * Flink SQL tables. Compares the Flink logical types the two schemas derive to; see the class
     * javadoc.
     */
    FLINK
  }

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
      case ICEBERG:
        return new IcebergComparison(original, update).run();
      case FLINK:
        return new FlinkComparison(original, update).run();
      default:
        throw new IllegalArgumentException("Unknown mode: " + mode);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // ICEBERG
  // ---------------------------------------------------------------------------------------------

  /**
   * One comparison run. Holds the accumulated findings and the two named-type tables needed to
   * resolve {@link Schema.Type#NAMED_TYPE_REF}, so the recursion itself stays parameter-light.
   */
  private static final class IcebergComparison {

    private final Schema originalRoot;
    private final Schema updateRoot;
    private final Map<String, Schema> originalNamedTypes;
    private final Map<String, Schema> updateNamedTypes;
    private final List<Incompatibility> findings = new ArrayList<>();

    /**
     * Reference pairs already compared. A cycle can only be formed by following a
     * {@link Schema.Type#NAMED_TYPE_REF}, so recording the (original, update) reference pair is
     * enough to guarantee termination.
     *
     * <p>Entries are never removed, which makes this a global set rather than a path-scoped one.
     * That is deliberate and does two things beyond termination:
     *
     * <ul>
     *   <li>A named type reached from several places is compared once, so one problem inside it
     *       yields one finding rather than one per reference site. The definition is shared, so the
     *       verdict cannot differ by path.
     *   <li>It bounds the walk. Path-scoped bookkeeping would revisit a shared type once per path,
     *       which is exponential for a chain of types that each reference the next twice.
     * </ul>
     *
     * <p>The cost is that a finding is reported at the first path that reaches the type rather than
     * at every such path.
     */
    private final Set<String> comparedRefPairs = new HashSet<>();

    /**
     * Struct definition pairs already compared, keyed by object identity.
     *
     * <p>{@link #comparedRefPairs} alone is not enough. A named type is usually also the root, and
     * the root is reached directly rather than through a reference, so the same definition would be
     * walked twice — once as the root and once via the self-reference — reporting every finding
     * inside it at two paths. Claiming the pair in the STRUCT branch means the root walk gets there
     * first, so findings are reported at the shallower, more useful path.
     *
     * <p>Identity rather than equality: {@link Schema} equality is structural, and a recursive
     * definition cannot be compared structurally without recursing forever.
     */
    private final Map<Schema, Set<Schema>> comparedStructPairs = new IdentityHashMap<>();

    IcebergComparison(LogicalType original, LogicalType update) {
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

    /**
     * Records that this pair of struct definitions is being compared.
     *
     * @return {@code true} if the caller should walk the pair, {@code false} if it was already
     *     compared at an earlier (and therefore shallower) path
     */
    private boolean claimStructPair(Schema original, Schema update) {
      return comparedStructPairs
          .computeIfAbsent(original, key -> Collections.newSetFromMap(new IdentityHashMap<>()))
          .add(update);
    }

    // -- dispatch --------------------------------------------------------------------------------

    private void compareTypes(Schema original, Schema update, String path) {
      if (isRef(original) || isRef(update)) {
        String pairKey = refKey(original) + ' ' + refKey(update);
        if (!comparedRefPairs.add(pairKey)) {
          return;
        }
        checkCompatibilityRecursive(
            resolve(original, originalNamedTypes),
            resolve(update, updateNamedTypes),
            path);
        return;
      }
      checkCompatibilityRecursive(original, update, path);
    }

    private void checkCompatibilityRecursive(Schema original, Schema update, String path) {
      // An unresolvable reference (e.g. an external type) can only be compared by name.
      if (isRef(original) || isRef(update)) {
        if (!isRef(original) || !isRef(update)
            || !original.getQualifiedName().equals(update.getQualifiedName())) {
          add(Rule.TYPE_MISMATCH, path, describeChange(original, update));
        }
        return;
      }

      Kind originalKind = kindOf(original);
      Kind updateKind = kindOf(update);
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
     * Mirrors the Iceberg-schema implementation's {@code validateStructs}, with two intentional
     * departures: findings accumulate instead of throwing, and the nullability relaxation is folded
     * in via {@link #isEffectivelyOptional} rather than applied as a pre-pass.
     */
    private void validateStructs(
        List<FieldView> originalFields, List<FieldView> updateFields, String path) {

      final Map<String, FieldView> originalFieldMap = originalFields.stream()
          .collect(Collectors.toMap(field -> field.name, field -> field));

      int lastSeenOriginalIndex = -1;
      final List<String> originalFieldOrder = originalFields.stream()
          .map(field -> field.name)
          .collect(Collectors.toList());

      final Set<String> updateFieldNames = updateFields.stream()
          .map(field -> field.name)
          .collect(Collectors.toSet());
      for (FieldView updateField : updateFields) {
        String fieldPath = childPath(path, updateField.name);
        final FieldView originalField = originalFieldMap.get(updateField.name);

        if (originalField == null) {
          if (!isEffectivelyOptional(updateField, null)) {
            add(Rule.REQUIRED_FIELD_ADDED, fieldPath,
                "added field is neither nullable nor defaulted; pre-existing rows have no value "
                    + "for it and Iceberg v2 cannot store a column default");
          }
          // Do not descend into a field the original schema never had.
          continue;
        }

        // Existing fields must keep their relative order. The position is advanced even when a
        // violation is reported, so a single swap yields one finding rather than cascading.
        final int originalIndex = originalFieldOrder.indexOf(updateField.name);
        if (originalIndex < lastSeenOriginalIndex) {
          add(Rule.FIELD_REORDERED, fieldPath,
              "field moved ahead of a field that preceded it in the original schema");
        }
        lastSeenOriginalIndex = originalIndex;

        if (isEffectivelyNullable(originalField)
            && !isEffectivelyOptional(updateField, originalField)) {
          add(Rule.NULLABLE_TO_NON_NULLABLE, fieldPath,
              "field was nullable and is now non-nullable; pre-existing rows may hold nulls");
        }

        compareTypes(originalField.schema, updateField.schema, fieldPath);
      }

      for (FieldView originalField : originalFields) {
        if (!updateFieldNames.contains(originalField.name)) {
          add(Rule.FIELD_DELETED, childPath(path, originalField.name),
              "field present in the original schema is missing from the update");
        }
      }
    }

    /**
     * Mirrors the Iceberg-schema implementation's {@code validateLists}.
     */
    private void validateLists(Schema original, Schema update, String path) {
      compareTypes(elementOf(original), elementOf(update), path + "[]");
    }

    /**
     * Mirrors the Iceberg-schema implementation's {@code validateMaps}.
     */
    private void validateMaps(Schema original, Schema update, String path) {
      if (!erasedEquals(keyOf(original), keyOf(update))) {
        add(Rule.MAP_KEY_TYPE_MISMATCH, path,
            "map key type changed from " + render(keyOf(original))
                + " to " + render(keyOf(update)));
      }
      compareTypes(valueOf(original), valueOf(update), path + "{}");
    }

    // -- primitives ------------------------------------------------------------------------------

    /**
     * Mirrors the Iceberg-schema implementation's {@code validatePrimitives}, with the promotion
     * table from {@code TypeUtil#isPromotionAllowed} inlined: identity, {@code int -> long},
     * {@code float -> double}, and {@code decimal(p,s) -> decimal(p',s)} with {@code p' >= p}.
     */
    private void validatePrimitives(Schema original, Schema update, String path) {
      if (!isIcebergRepresentable(update)) {
        add(Rule.UNREPRESENTABLE_TYPE, path,
            render(update) + " cannot be represented in Iceberg v2");
        return;
      }

      IcebergClass originalClass = icebergClassOf(original);
      IcebergClass updateClass = icebergClassOf(update);

      if (originalClass == updateClass) {
        switch (originalClass) {
          case DECIMAL:
            // Iceberg allows decimal precision to widen but never the scale to change: the stored
            // value is an unscaled integer, so re-scaling would reinterpret every existing row.
            if (update.getScale() != original.getScale()
                || update.getPrecision() < original.getPrecision()) {
              add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
                  + " (decimal scale must be unchanged and precision may not shrink)");
            }
            return;
          case FIXED:
            if (update.getLength() != original.getLength()) {
              add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
                  + " (fixed-length binary cannot change length)");
            }
            return;
          default:
            return;
        }
      }

      boolean promotable =
          (originalClass == IcebergClass.INT && updateClass == IcebergClass.LONG)
              || (originalClass == IcebergClass.FLOAT && updateClass == IcebergClass.DOUBLE);
      if (!promotable) {
        add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update));
      }
    }

    /** Structural equality under Iceberg erasure, with no promotion allowed. Used for map keys. */
    private boolean erasedEquals(Schema original, Schema update) {
      Schema left = resolve(original, originalNamedTypes);
      Schema right = resolve(update, updateNamedTypes);
      if (isRef(left) || isRef(right)) {
        return isRef(left) && isRef(right)
            && left.getQualifiedName().equals(right.getQualifiedName());
      }
      Kind kind = kindOf(left);
      if (kind != kindOf(right)) {
        return false;
      }
      switch (kind) {
        case STRUCT: {
          List<FieldView> leftFields = fieldViews(left);
          List<FieldView> rightFields = fieldViews(right);
          if (leftFields.size() != rightFields.size()) {
            return false;
          }
          for (int i = 0; i < leftFields.size(); i++) {
            if (!leftFields.get(i).name.equals(rightFields.get(i).name)
                || !erasedEquals(leftFields.get(i).schema, rightFields.get(i).schema)) {
              return false;
            }
          }
          return true;
        }
        case LIST:
          return erasedEquals(elementOf(left), elementOf(right));
        case MAP:
          return erasedEquals(keyOf(left), keyOf(right))
              && erasedEquals(valueOf(left), valueOf(right));
        case PRIMITIVE: {
          IcebergClass leftClass = icebergClassOf(left);
          if (leftClass != icebergClassOf(right)) {
            return false;
          }
          if (leftClass == IcebergClass.DECIMAL) {
            return left.getPrecision() == right.getPrecision()
                && left.getScale() == right.getScale();
          }
          if (leftClass == IcebergClass.FIXED) {
            return left.getLength() == right.getLength();
          }
          return true;
        }
        default:
          return false;
      }
    }

    // -- normalization ---------------------------------------------------------------------------

    /**
     * Whether {@code field} may be treated as optional, applying the container-nullability
     * relaxation inline.
     *
     * <p>Avro and Protobuf cannot encode a null container: an absent repeated field is an empty
     * list and an absent map is an empty map, so the converters mark such columns non-nullable and
     * record an empty-container default. Iceberg v2 cannot persist a column default, so the v2-safe
     * equivalent is to treat the field as optional — old rows read null, new rows write an empty
     * container, and for map and list types those are query-equivalent.
     *
     * <p>Deliberately narrow:
     * <ul>
     *   <li>containers only — relaxing a required scalar would substitute null for its configured
     *       default, a real semantic loss;
     *   <li>newly added or already-nullable fields only — flipping a pre-existing required field
     *       would silently break consumers relying on the NOT NULL contract.
     * </ul>
     *
     * <p>This rewrites nothing. {@link Schema#setNullable} mutates in place and the caller's schema
     * must not be modified, and since the relaxation is only ever needed to answer a question, a
     * predicate suffices.
     *
     * <p><b>The schema is the authoritative source of defaults.</b> The default is read from
     * {@link Schema.Field#hasDefaultValue()} on the field being examined, never from a path-keyed
     * side table supplied by the caller. That choice is deliberate: a separately-carried map of
     * defaults can be mis-keyed against the schema it describes, and reading the field
     * directly makes
     * that failure mode unrepresentable. A caller holding defaults out-of-band should
     * push them onto
     * the schema rather than expect this method to consult them.
     *
     * @param originalField the matching field in the original schema, or {@code null} if the field
     *                      is newly added
     */
    private boolean isEffectivelyOptional(FieldView field, FieldView originalField) {
      if (isEffectivelyNullable(field)) {
        return true;
      }
      if (!field.hasDefault) {
        return false;
      }
      Schema resolved = resolve(field.schema, updateNamedTypes);
      if (!isContainer(resolved)) {
        return false;
      }
      return originalField == null || isEffectivelyNullable(originalField);
    }

    private static boolean isEffectivelyNullable(FieldView field) {
      return field.forcedNullable || field.schema.isNullable();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Iceberg type model
  // ---------------------------------------------------------------------------------------------

  /**
   * The structural shapes a target type system models, after erasing the SRLT types that map onto
   * them. {@code MULTISET} is only ever produced by Flink mode — Iceberg has no multiset and lowers
   * one to a {@code MAP}.
   */
  private enum Kind {
    STRUCT,
    LIST,
    MAP,
    MULTISET,
    PRIMITIVE
  }

  /**
   * Iceberg's primitive type set. Members of the same class are indistinguishable to Iceberg, so
   * changes within a class are free and promotion edges apply to the whole class.
   */
  private enum IcebergClass {
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    TIMESTAMP_NANO,
    TIMESTAMPTZ_NANO,
    STRING,
    BINARY,
    FIXED,
    VARIANT
  }

  /**
   * The Iceberg shape of an SRLT type.
   *
   * <p>UNION becomes a STRUCT of its branches and MULTISET becomes a MAP, mirroring how the Flink
   * converters lower these before Iceberg conversion — so this checker sees the same shapes.
   */
  private static Kind kindOf(Schema schema) {
    switch (schema.getType()) {
      case STRUCT:
      case UNION:
        return Kind.STRUCT;
      case ARRAY:
        return Kind.LIST;
      case MAP:
      case MULTISET:
        return Kind.MAP;
      default:
        return Kind.PRIMITIVE;
    }
  }

  /**
   * Maps an SRLT primitive onto its Iceberg class, erasing what Iceberg does not model: TINYINT and
   * SMALLINT collapse into {@code int}; CHAR, VARCHAR and ENUM collapse into {@code string} with no
   * length; TIME and TIMESTAMP lose their fractional-second precision (Iceberg v2 stores
   * microseconds). DECIMAL keeps precision and scale, and BINARY keeps its length as {@code fixed},
   * because Iceberg models both.
   */
  private static IcebergClass icebergClassOf(Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return IcebergClass.BOOLEAN;
      case TINYINT:
      case SMALLINT:
      case INT:
        return IcebergClass.INT;
      case BIGINT:
        return IcebergClass.LONG;
      case FLOAT:
        return IcebergClass.FLOAT;
      case DOUBLE:
        return IcebergClass.DOUBLE;
      case DECIMAL:
        return IcebergClass.DECIMAL;
      case DATE:
        return IcebergClass.DATE;
      case TIME:
        // Iceberg has no nanosecond time type, so precision is erased at every value.
        return IcebergClass.TIME;
      case TIMESTAMP:
        return schema.getPrecision() > MAX_ICEBERG_MICROS_PRECISION
            ? IcebergClass.TIMESTAMP_NANO
            : IcebergClass.TIMESTAMP;
      case TIMESTAMP_LTZ:
        return schema.getPrecision() > MAX_ICEBERG_MICROS_PRECISION
            ? IcebergClass.TIMESTAMPTZ_NANO
            : IcebergClass.TIMESTAMPTZ;
      case CHAR:
      case VARCHAR:
      case ENUM:
        return IcebergClass.STRING;
      case VARBINARY:
        return IcebergClass.BINARY;
      case BINARY:
        return IcebergClass.FIXED;
      case VARIANT:
        return IcebergClass.VARIANT;
      default:
        throw new IllegalStateException(
            "Not an Iceberg primitive: " + schema.getType());
    }
  }

  /**
   * Whether Iceberg can represent the type at all, as distinct from erasing a detail of it.
   *
   * <p>Sub-microsecond timestamps are deliberately <em>not</em> listed here: Iceberg's own
   * {@code FlinkTypeToType} maps those to {@code timestamp_ns} rather than rejecting them, so they
   * are representable — just as a different type, which {@link #icebergClassOf} reflects. Whether a
   * given table can then store one depends on its format version, which is not a schema-comparison
   * question.
   */
  private static boolean isIcebergRepresentable(Schema schema) {
    if (schema.getType() == Schema.Type.DECIMAL) {
      return schema.getPrecision() <= MAX_ICEBERG_DECIMAL_PRECISION;
    }
    return true;
  }

  private static boolean isContainer(Schema schema) {
    Kind kind = kindOf(schema);
    return kind == Kind.LIST || kind == Kind.MAP;
  }

  // ---------------------------------------------------------------------------------------------
  // Structural accessors (applying the UNION and MULTISET lowering)
  // ---------------------------------------------------------------------------------------------

  /**
   * A struct member, abstracting over STRUCT fields and UNION branches so the struct rules apply to
   * both without synthesising {@link Schema.Field} instances.
   */
  private static final class FieldView {

    private final String name;
    private final Schema schema;
    private final boolean hasDefault;

    /** Union branches are always optional: at most one branch is populated per record. */
    private final boolean forcedNullable;

    private FieldView(String name, Schema schema, boolean hasDefault, boolean forcedNullable) {
      this.name = name;
      this.schema = schema;
      this.hasDefault = hasDefault;
      this.forcedNullable = forcedNullable;
    }
  }

  private static List<FieldView> fieldViews(Schema schema) {
    List<FieldView> views = new ArrayList<>();
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema.UnionBranch branch : schema.getBranches()) {
        views.add(new FieldView(branch.getName(), branch.getSchema(), false, true));
      }
      return views;
    }
    for (Schema.Field field : schema.getFields()) {
      views.add(new FieldView(
          field.getName(), field.getSchema(), field.hasDefaultValue(), false));
    }
    return views;
  }

  private static Schema elementOf(Schema schema) {
    return schema.getElementType();
  }

  private static Schema keyOf(Schema schema) {
    // A MULTISET is a MAP from element to occurrence count.
    return schema.getType() == Schema.Type.MULTISET
        ? schema.getElementType()
        : schema.getKeyType();
  }

  private static Schema valueOf(Schema schema) {
    return schema.getType() == Schema.Type.MULTISET
        ? MULTISET_COUNT_TYPE
        : schema.getValueType();
  }

  // ---------------------------------------------------------------------------------------------
  // Named type references
  // ---------------------------------------------------------------------------------------------

  private static boolean isRef(Schema schema) {
    return schema.getType() == Schema.Type.NAMED_TYPE_REF;
  }

  private static String refKey(Schema schema) {
    return isRef(schema) ? schema.getQualifiedName() : "";
  }

  /**
   * Follows named-type references to their definition. Returns the reference itself when it cannot
   * be resolved — an external type, which the caller then compares by qualified name.
   */
  private static Schema resolve(Schema schema, Map<String, Schema> namedTypes) {
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

  private static String childPath(String parentPath, String fieldName) {
    return parentPath.isEmpty() ? fieldName : parentPath + '.' + fieldName;
  }

  private static String describeChange(Schema original, Schema update) {
    return "type changed from " + render(original) + " to " + render(update);
  }

  private static String render(Schema schema) {
    switch (schema.getType()) {
      case DECIMAL:
        return "DECIMAL(" + schema.getPrecision() + ", " + schema.getScale() + ")";
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

  // ---------------------------------------------------------------------------------------------
  // FLINK
  // ---------------------------------------------------------------------------------------------

  /**
   * One comparison run in Flink mode.
   *
   * <p>Structured to mirror {@link IcebergComparison} rather than share a base class with it. The
   * traversal skeleton is similar, but the three rules that matter all differ — which fields count
   * as optional, how leaves are compared, and whether a MULTISET is a MAP — and each class must
   * stay independently readable against the specification it implements. Factoring out a common
   * walk would couple them and make neither diffable against its own reference.
   */
  private static final class FlinkComparison {

    private final Schema originalRoot;
    private final Schema updateRoot;
    private final Map<String, Schema> originalNamedTypes;
    private final Map<String, Schema> updateNamedTypes;
    private final List<Incompatibility> findings = new ArrayList<>();

    /**
     * See {@link IcebergComparison#comparedRefPairs}.
     */
    private final Set<String> comparedRefPairs = new HashSet<>();

    /**
     * See {@link IcebergComparison#comparedStructPairs}.
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

    private void compareTypes(Schema original, Schema update, String path) {
      if (isRef(original) || isRef(update)) {
        String pairKey = refKey(original) + ' ' + refKey(update);
        if (!comparedRefPairs.add(pairKey)) {
          return;
        }
        checkCompatibilityRecursive(
            resolve(original, originalNamedTypes),
            resolve(update, updateNamedTypes),
            path);
        return;
      }
      checkCompatibilityRecursive(original, update, path);
    }

    private void checkCompatibilityRecursive(Schema original, Schema update, String path) {
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
     * R1-R5 and R8. Deliberately the same shape as
     * {@link IcebergComparison#validateStructs(List, List, String)}, differing only in the
     * added-field predicate: Flink can represent a NOT NULL column that carries a default, so there
     * is no container-only relaxation here — a default is enough on its own.
     */
    private void validateStructs(
        List<FieldView> originalFields, List<FieldView> updateFields, String path) {

      final Map<String, FieldView> originalFieldMap = originalFields.stream()
          .collect(Collectors.toMap(field -> field.name, field -> field));

      int lastSeenOriginalIndex = -1;
      final List<String> originalFieldOrder = originalFields.stream()
          .map(field -> field.name)
          .collect(Collectors.toList());

      final Set<String> updateFieldNames = updateFields.stream()
          .map(field -> field.name)
          .collect(Collectors.toSet());
      for (FieldView updateField : updateFields) {
        String fieldPath = childPath(path, updateField.name);
        final FieldView originalField = originalFieldMap.get(updateField.name);

        // R1: a new column must be readable for rows written before it existed.
        if (originalField == null) {
          if (!isNullableOrDefaulted(updateField)) {
            add(Rule.REQUIRED_FIELD_ADDED, fieldPath,
                "added column is neither nullable nor defaulted, so rows written before it existed "
                    + "have no value for it");
          }
          continue;
        }

        // R4: Flink columns are positional, so existing columns may not be resequenced.
        final int originalIndex = originalFieldOrder.indexOf(updateField.name);
        if (originalIndex < lastSeenOriginalIndex) {
          add(Rule.FIELD_REORDERED, fieldPath,
              "column moved ahead of a column that preceded it in the original schema");
        }
        lastSeenOriginalIndex = originalIndex;

        // R5: tightening nullability would reject rows that already hold nulls.
        if (isEffectivelyNullable(originalField) && !isEffectivelyNullable(updateField)) {
          add(Rule.NULLABLE_TO_NON_NULLABLE, fieldPath,
              "column was nullable and is now NOT NULL; pre-existing rows may hold nulls");
        }

        compareTypes(originalField.schema, updateField.schema, fieldPath);
      }

      // R2, and R3 by consequence: a rename is indistinguishable from a drop plus an add.
      for (FieldView originalField : originalFields) {
        if (!updateFieldNames.contains(originalField.name)) {
          add(Rule.FIELD_DELETED, childPath(path, originalField.name),
              "column present in the original schema is missing from the update");
        }
      }
    }

    /**
     * R1's predicate. Unlike Iceberg mode there is no container restriction and no scalar
     * exclusion: a Flink column may be NOT NULL and still carry a default, so any default suffices.
     *
     * <p>As in Iceberg mode the schema is the authoritative source of defaults — read from the
     * field, never from a caller-supplied side table.
     */
    private static boolean isNullableOrDefaulted(FieldView field) {
      return isEffectivelyNullable(field) || field.hasDefault;
    }

    /** R7 for ARRAY. */
    private void validateLists(Schema original, Schema update, String path) {
      compareTypes(elementOf(original), elementOf(update), path + "[]");
    }

    /** R7 for MULTISET, which Flink models as its own type rather than as a MAP. */
    private void validateMultisets(Schema original, Schema update, String path) {
      compareTypes(elementOf(original), elementOf(update), path + "[]");
    }

    /** R7 for MAP. The key type is frozen; only the value may widen. */
    private void validateMaps(Schema original, Schema update, String path) {
      if (!flinkTypesEqual(original.getKeyType(), update.getKeyType())) {
        add(Rule.MAP_KEY_TYPE_MISMATCH, path,
            "map key type changed from " + render(original.getKeyType())
                + " to " + render(update.getKeyType()));
      }
      compareTypes(original.getValueType(), update.getValueType(), path + "{}");
    }

    private static boolean isEffectivelyNullable(FieldView field) {
      return field.forcedNullable || field.schema.isNullable();
    }

    // -- primitives ------------------------------------------------------------------------------

    /**
     * R6. Two independent parts, both required.
     *
     * <p>Part A delegates the root relation to {@link FlinkLogicalTypeCasts}, so this checker's
     * notion of a safe type change is Flink's own rather than a hand-picked one. Nullability is
     * excluded because R5 already covers it at field level, and reporting it twice would be noise.
     *
     * <p>Part B applies the parameter guards that Part A structurally cannot: Flink's table is
     * keyed by type root and never reads a length, precision, or scale, so on its own it would
     * admit {@code VARCHAR(50) -> VARCHAR(10)}. That is a hole rather than a policy — see {@link
     * FlinkLogicalTypeCasts}. <b>Do not remove these guards as redundant.</b>
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

      if (originalParams == updateParams) {
        switch (originalParams) {
          case LENGTH:
            if (flinkLengthOf(update) < flinkLengthOf(original)) {
              add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
                  + " (length may not shrink)");
            }
            return;
          case PRECISION:
            if (update.getPrecision() < original.getPrecision()) {
              add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
                  + " (precision may not shrink)");
            }
            return;
          case PRECISION_AND_SCALE:
            if (update.getScale() != original.getScale()
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
            && update.getPrecision() - update.getScale() < digitsNeeded) {
          add(Rule.UNSUPPORTED_TYPE_CHANGE, path, describeChange(original, update)
              + " (target cannot represent the full range of the source)");
        }
      }
    }

    /** Exact Flink-type equality, with no widening allowed. Used for map keys, which are frozen. */
    private boolean flinkTypesEqual(Schema original, Schema update) {
      Schema left = resolve(original, originalNamedTypes);
      Schema right = resolve(update, updateNamedTypes);
      if (isRef(left) || isRef(right)) {
        return isRef(left) && isRef(right)
            && left.getQualifiedName().equals(right.getQualifiedName());
      }
      Kind kind = flinkKindOf(left);
      if (kind != flinkKindOf(right)) {
        return false;
      }
      switch (kind) {
        case STRUCT: {
          List<FieldView> leftFields = fieldViews(left);
          List<FieldView> rightFields = fieldViews(right);
          if (leftFields.size() != rightFields.size()) {
            return false;
          }
          for (int i = 0; i < leftFields.size(); i++) {
            if (!leftFields.get(i).name.equals(rightFields.get(i).name)
                || !flinkTypesEqual(leftFields.get(i).schema, rightFields.get(i).schema)) {
              return false;
            }
          }
          return true;
        }
        case LIST:
        case MULTISET:
          return flinkTypesEqual(elementOf(left), elementOf(right));
        case MAP:
          return flinkTypesEqual(left.getKeyType(), right.getKeyType())
              && flinkTypesEqual(left.getValueType(), right.getValueType());
        case PRIMITIVE: {
          FlinkLogicalTypeCasts.Root leftRoot = flinkRootOf(left);
          if (leftRoot != flinkRootOf(right)) {
            return false;
          }
          switch (paramKindOf(leftRoot)) {
            case LENGTH:
              return flinkLengthOf(left) == flinkLengthOf(right);
            case PRECISION:
              return left.getPrecision() == right.getPrecision();
            case PRECISION_AND_SCALE:
              return left.getPrecision() == right.getPrecision()
                  && left.getScale() == right.getScale();
            default:
              return true;
          }
        }
        default:
          return false;
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Flink type model
  // ---------------------------------------------------------------------------------------------

  /** Which parameters a Flink type root carries, and therefore which guard applies to it. */
  private enum ParamKind {
    NONE,
    LENGTH,
    PRECISION,
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
    return schema.getType() == Schema.Type.ENUM ? Schema.MAX_LENGTH : schema.getLength();
  }

  private static ParamKind paramKindOf(FlinkLogicalTypeCasts.Root root) {
    switch (root) {
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        return ParamKind.LENGTH;
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
