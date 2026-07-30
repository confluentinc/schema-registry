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

package io.confluent.kafka.schemaregistry.type.logical.check;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;

import io.confluent.kafka.schemaregistry.type.logical.check.Incompatibility.Rule;
import io.confluent.kafka.schemaregistry.type.logical.check.LogicalTypeChecker.Mode;

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
 * <p>{@link LogicalTypeChecker} is the entry point; this class holds the rules.
 *
 * <p><b>This check is only half of the question.</b> It takes two schemas, so it says nothing about
 * the first schema registered on a subject, and nothing about whether either schema is usable by
 * the consumer on its own terms — a decimal whose precision exceeds what the consumer supports is
 * unusable whether or not it changed. {@link ValidityChecker} answers that half, and a caller
 * registering a schema should run both.
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
 * type mapping Iceberg itself applies (in {@link #icebergClassOf}).
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
 * <p>The equivalence classes are taken from the type mapping Iceberg itself applies, which is the
 * conversion applied before comparing there, so this checker agrees with it. Keep the two in
 * step. Notably: {@code BINARY(n)} becomes {@code fixed(n)} and so keeps its length, while
 * {@code CHAR} and {@code VARCHAR} lose theirs; {@code MULTISET<T>} becomes {@code map<T, int>};
 * {@code TIME(p)} erases precision at every value because Iceberg has no nanosecond time type; and
 * a timestamp with precision above {@value #MAX_ICEBERG_MICROS_PRECISION} becomes the nanosecond
 * timestamp type rather than being rejected.
 */
final class CompatibilityChecker {

  /**
   * Precision above which a Flink timestamp maps to Iceberg's nanosecond timestamp rather than its
   * microsecond one. Mirrors the threshold Iceberg's own type mapping uses.
   */
  private static final int MAX_ICEBERG_MICROS_PRECISION = 6;

  /**
   * Format version that added {@code initial-default} and {@code write-default}.
   */
  private static final int FORMAT_VERSION_WITH_COLUMN_DEFAULTS = 3;

  /**
   * Format version that widened the promotion table. v3 adds {@code date} to the without-timezone
   * timestamps, and {@code unknown} to any type — the latter unreachable here, since no
   * {@link Schema.Type} maps onto {@code unknown}.
   */
  private static final int FORMAT_VERSION_WITH_V3_PROMOTIONS = 3;

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

    /**
     * Derived defaults for the update schema, keyed by index path.
     *
     * <p>Read in preference to nothing at all: the converters record a container's implicit default
     * ({@code repeated} is an empty list, a proto map is an empty map) <em>only</em> here, never on
     * the {@link Schema.Field}. The field's own default is reserved for user-declared values so
     * that a DDL round-trip stays clean. Both are consulted; see {@code hasDefault}.
     */
    private final Map<List<Integer>, Object> updateDefaults;

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

    /** Iceberg table format version being targeted. */
    private final int formatVersion;

    IcebergComparison(LogicalType original, LogicalType update, int formatVersion) {
      this.formatVersion = formatVersion;
      this.originalRoot = original.getRootSchema();
      this.updateRoot = update.getRootSchema();
      this.originalNamedTypes = original.getNamedTypes();
      this.updateNamedTypes = update.getNamedTypes();
      this.updateDefaults = update.getDefaultValues();
    }

    CompatibilityResult run() {
      compareTypes(originalRoot, updateRoot, "", Collections.emptyList());
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

    private void compareTypes(
        Schema original, Schema update, String path, List<Integer> indexPath) {
      if (isRef(original) || isRef(update)) {
        String pairKey = refKey(original) + ' ' + refKey(update);
        if (!comparedRefPairs.add(pairKey)) {
          return;
        }
        checkCompatibilityRecursive(
            resolve(original, originalNamedTypes),
            resolve(update, updateNamedTypes),
            path, indexPath);
        return;
      }
      checkCompatibilityRecursive(original, update, path, indexPath);
    }

    private void checkCompatibilityRecursive(
        Schema original, Schema update, String path, List<Integer> indexPath) {
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
            validateStructs(fieldViews(original), fieldViews(update), path, indexPath);
          }
          break;
        case LIST:
          validateLists(original, update, path, indexPath);
          break;
        case MAP:
          validateMaps(original, update, path, indexPath);
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
        List<FieldView> originalFields, List<FieldView> updateFields, String path,
        List<Integer> indexPath) {

      final Map<String, FieldView> originalFieldMap = originalFields.stream()
          .collect(Collectors.toMap(field -> field.name, field -> field));

      int lastSeenOriginalIndex = -1;
      int updatePosition = -1;
      final List<String> originalFieldOrder = originalFields.stream()
          .map(field -> field.name)
          .collect(Collectors.toList());

      final Set<String> updateFieldNames = updateFields.stream()
          .map(field -> field.name)
          .collect(Collectors.toSet());
      for (FieldView updateField : updateFields) {
        updatePosition++;
        final String fieldPath = childPath(path, updateField.name);
        // Struct fields are the one place both converters agree on the index convention: the
        // field's position within its struct, appended to the parent's path.
        final List<Integer> fieldIndexPath = appendIndex(indexPath, updatePosition);
        final FieldView originalField = originalFieldMap.get(updateField.name);

        if (originalField == null) {
          if (!isEffectivelyOptional(updateField, fieldIndexPath, null)) {
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
            && !isEffectivelyOptional(updateField, fieldIndexPath, originalField)) {
          add(Rule.NULLABLE_TO_NON_NULLABLE, fieldPath,
              "field was nullable and is now non-nullable; pre-existing rows may hold nulls");
        }

        compareTypes(originalField.schema, updateField.schema, fieldPath, fieldIndexPath);
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
    private void validateLists(
        Schema original, Schema update, String path, List<Integer> indexPath) {
      // The element index path is not portable: the Avro converter appends 0 for an array element
      // while the Protobuf one appends nothing. Passing null marks the path unresolvable from here
      // down, which makes a default lookup below an array fail closed rather than guess.
      compareTypes(elementOf(original), elementOf(update), path + "[]", null);
    }

    /**
     * Mirrors the Iceberg-schema implementation's {@code validateMaps}.
     */
    private void validateMaps(
        Schema original, Schema update, String path, List<Integer> indexPath) {
      if (!erasedEquals(keyOf(original), keyOf(update))) {
        add(Rule.MAP_KEY_TYPE_MISMATCH, path,
            "map key type changed from " + render(keyOf(original))
                + " to " + render(keyOf(update)));
      }
      // Both converters agree on the map value index, unlike the array element.
      compareTypes(valueOf(original), valueOf(update), path + "{}", appendIndex(indexPath, 1));
    }

    // -- primitives ------------------------------------------------------------------------------

    /**
     * Mirrors the Iceberg-schema implementation's {@code validatePrimitives}, with the promotion
     * table from {@code TypeUtil#isPromotionAllowed} inlined: identity, {@code int -> long},
     * {@code float -> double}, and {@code decimal(p,s) -> decimal(p',s)} with {@code p' >= p}.
     */
    private void validatePrimitives(Schema original, Schema update, String path) {
      // Whether Iceberg can represent the update's types at all is deliberately not asked here.
      // It is a property of one schema rather than of the change, so a first registration would
      // escape it entirely; ValidityChecker owns it for both schemas instead.
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

      if (!isPromotionAllowed(originalClass, updateClass)) {
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
    private boolean isEffectivelyOptional(
        FieldView field, List<Integer> fieldIndexPath, FieldView originalField) {
      if (isEffectivelyNullable(field)) {
        return true;
      }

      // v3 only. With initial-default and write-default available, a newly added required field is
      // readable for rows written before it existed, so no relaxation is needed -- the default is
      // simply stored. Restricted to newly added fields because initial-default "is set only when a
      // field is added to an existing schema": it cannot be attached to an existing column
      // retroactively, so tightening one is still unrecoverable.
      if (supportsColumnDefaults()
          && originalField == null
          && typeAllowsNonNullDefault(field.schema)
          && hasNonNullDefault(field, fieldIndexPath)) {
        return true;
      }

      // The container relaxation, in both versions. It addresses a derivation quirk rather than an
      // Iceberg capability -- proto and Avro containers are marked NOT NULL because those formats
      // cannot encode a null container -- so v3 does not retire it.
      if (!hasDefault(field, fieldIndexPath)) {
        return false;
      }
      Schema resolved = resolve(field.schema, updateNamedTypes);
      if (!isContainer(resolved)) {
        return false;
      }
      return originalField == null || isEffectivelyNullable(originalField);
    }

    private boolean supportsColumnDefaults() {
      return formatVersion >= FORMAT_VERSION_WITH_COLUMN_DEFAULTS;
    }

    private boolean supportsV3Promotions() {
      return formatVersion >= FORMAT_VERSION_WITH_V3_PROMOTIONS;
    }

    /** Iceberg's promotion table for the targeted format version. */
    private boolean isPromotionAllowed(IcebergClass from, IcebergClass to) {
      if (from == IcebergClass.INT && to == IcebergClass.LONG) {
        return true;
      }
      if (from == IcebergClass.FLOAT && to == IcebergClass.DOUBLE) {
        return true;
      }
      return supportsV3Promotions() && isDateToTimestampPromotion(from, to);
    }

    /**
     * v3 adds {@code date} to the without-timezone timestamps only. Promotion to
     * {@code timestamptz} or {@code timestamptz_ns} is explicitly forbidden: a date carries no
     * zone, and assigning one would invent information.
     */
    private static boolean isDateToTimestampPromotion(IcebergClass from, IcebergClass to) {
      if (from != IcebergClass.DATE) {
        return false;
      }
      return to == IcebergClass.TIMESTAMP || to == IcebergClass.TIMESTAMP_NANO;
    }

    /**
     * Whether the default is present <em>and</em> non-null.
     *
     * <p>v3 requires both defaults to be non-null when a required field is added, so mere presence
     * is not enough. A null default leaves pre-existing rows with nothing to read.
     */
    private boolean hasNonNullDefault(FieldView field, List<Integer> fieldIndexPath) {
      if (field.hasDefault && field.defaultValue != null) {
        return true;
      }
      return fieldIndexPath != null && updateDefaults.get(fieldIndexPath) != null;
    }

    private static boolean isEffectivelyNullable(FieldView field) {
      return field.forcedNullable || field.schema.isNullable();
    }

    /**
     * Whether {@code field} carries a default, from either channel.
     *
     * <p>Two channels exist and both count. A user-declared default lands on the
     * {@link Schema.Field}. A default the converter derived from format semantics — an absent
     * {@code repeated} field is an empty list, an absent proto map is an empty map — lands only in
     * the schema's path-keyed map. Reading just the field would miss every derived default, which
     * is the majority of them and the whole reason the container relaxation exists.
     *
     * <p>A {@code null} path means the walk crossed an array, where the two converters disagree on
     * the index convention. There the lookup is skipped rather than guessed, so an undecidable case
     * reads as "no default" and fails closed.
     */
    private boolean hasDefault(FieldView field, List<Integer> fieldIndexPath) {
      if (field.hasDefault) {
        return true;
      }
      return fieldIndexPath != null && updateDefaults.containsKey(fieldIndexPath);
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
   * Whether the type may carry a non-null default. The spec forbids it for {@code unknown},
   * {@code variant}, {@code geometry} and {@code geography}; of those only VARIANT is expressible
   * here.
   */
  private static boolean typeAllowsNonNullDefault(Schema schema) {
    return schema.getType() != Schema.Type.VARIANT;
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

    /**
     * The declared default, or {@code null}. Distinct from {@link #hasDefault}: v3 requires a
     * non-null default when a required field is added, so presence alone is not enough.
     */
    private final Object defaultValue;

    /** Union branches are always optional: at most one branch is populated per record. */
    private final boolean forcedNullable;

    private FieldView(String name, Schema schema, boolean hasDefault, Object defaultValue,
        boolean forcedNullable) {
      this.name = name;
      this.schema = schema;
      this.hasDefault = hasDefault;
      this.defaultValue = defaultValue;
      this.forcedNullable = forcedNullable;
    }
  }

  private static List<FieldView> fieldViews(Schema schema) {
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

  /**
   * Appends a child index, or returns {@code null} if the parent path was already unresolvable.
   * See {@code hasDefault} for what {@code null} means.
   */
  private static List<Integer> appendIndex(List<Integer> parentPath, int index) {
    if (parentPath == null) {
      return null;
    }
    final List<Integer> child = new ArrayList<>(parentPath.size() + 1);
    child.addAll(parentPath);
    child.add(index);
    return child;
  }

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
   *
   * <h3>The governing criterion</h3>
   *
   * <p>A change is rejected here when it is not a <b>legal Flink SQL table evolution</b> — legal
   * meaning semantically valid at both DDL and DML time, <em>whatever the underlying storage</em>.
   * That reduces to one test:
   *
   * <blockquote>Does the change require a stored value to be <b>reinterpreted</b>, or to be
   * <b>invented</b>?</blockquote>
   *
   * <p>If neither, it is legal. Flink accepts almost any {@code ALTER TABLE}, so DDL legality alone
   * decides nothing; and how a particular encoding locates or resolves a value is the format-level
   * checker's business, not this one's. What is left is whether every value that was legitimately
   * written can still be read as itself.
   *
   * <p>The retained rules are exactly the ones that fail that test.
   * {@link Rule#REQUIRED_FIELD_ADDED} invents — a NOT NULL column with no written value cannot be
   * satisfied by any storage. {@link Rule#NULLABLE_TO_NON_NULLABLE} reinterprets — a legitimately
   * written null has no non-null reading. {@link Rule#TYPE_MISMATCH} and
   * {@link Rule#UNSUPPORTED_TYPE_CHANGE} reinterpret, at the
   * kind and leaf level respectively; the parameter guards belong to the latter because a decimal
   * scale, a temporal precision and a CHAR length each select how stored bytes are read.
   *
   * <p><b>Do not reintroduce a rule that fails this test.</b> Drops, reordering, renames, map-key
   * changes and enum symbol removal were all rejected here once, and each was removed on this
   * argument: a drop and a reorder touch no value, a rename is
   * a supported table-evolution operation with the byte-location question belonging to the format,
   * a map key is an ordinary value, and an ENUM derives to VARCHAR so dropping a symbol changes the
   * Flink type not at all. What made enum removal look like a Flink problem was Avro resolving an
   * unknown symbol to the enum's default — Avro's behaviour, and the Avro checker's to catch.
   */
  private static final class FlinkComparison {

    private final Schema originalRoot;
    private final Schema updateRoot;
    private final Map<String, Schema> originalNamedTypes;
    private final Map<String, Schema> updateNamedTypes;

    /**
     * Derived defaults for the update schema, keyed by index path.
     *
     * <p>Read in preference to nothing at all: the converters record a container's implicit default
     * ({@code repeated} is an empty list, a proto map is an empty map) <em>only</em> here, never on
     * the {@link Schema.Field}. The field's own default is reserved for user-declared values so
     * that a DDL round-trip stays clean. Both are consulted; see {@code hasDefault}.
     */
    private final Map<List<Integer>, Object> updateDefaults;

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
      this.updateDefaults = update.getDefaultValues();
    }

    CompatibilityResult run() {
      compareTypes(originalRoot, updateRoot, "", Collections.emptyList());
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
        Schema original, Schema update, String path, List<Integer> indexPath) {
      if (isRef(original) || isRef(update)) {
        String pairKey = refKey(original) + ' ' + refKey(update);
        if (!comparedRefPairs.add(pairKey)) {
          return;
        }
        checkCompatibilityRecursive(
            resolve(original, originalNamedTypes),
            resolve(update, updateNamedTypes),
            path, indexPath);
        return;
      }
      checkCompatibilityRecursive(original, update, path, indexPath);
    }

    private void checkCompatibilityRecursive(
        Schema original, Schema update, String path, List<Integer> indexPath) {
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
            validateStructs(fieldViews(original), fieldViews(update), path, indexPath);
          }
          break;
        case LIST:
          validateLists(original, update, path, indexPath);
          break;
        case MULTISET:
          validateMultisets(original, update, path, indexPath);
          break;
        case MAP:
          validateMaps(original, update, path, indexPath);
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
     * Field-level rules: added columns and nullability. Columns are matched by name, and a column
     * present only in the original is simply not read.
     *
     * <p><b>Deliberately narrower than {@code IcebergComparison#validateStructs}, which also
     * rejects drops and reordering.</b> Applying the reinterpret-or-invent test from the class
     * javadoc:
     *
     * <ul>
     *   <li>A <b>drop</b> invents nothing and reinterprets nothing — the column stops being read.
     *       {@code ALTER TABLE ... DROP COLUMN} is a supported Flink table change
     *       (a supported table-evolution operation), so it is legal here. Iceberg still rejects it,
     *       because it cannot drop a column in place without a stable field ID.
     *   <li>A <b>reorder</b> likewise touches no value. Flink SQL identifies columns by name, and
     *       a supported table-evolution operation is supported.
     *   <li>An <b>added column</b> does invent: a NOT NULL column with no written value cannot be
     *       satisfied by any storage, so it stays rejected. A nullable or defaulted addition is
     *       fine, because null or the default is a legitimate value for it.
     * </ul>
     *
     * <p>A <b>rename</b> therefore no longer produces a finding here. That is intended: renaming is
     * a supported table-evolution operation, and whether old bytes can still be located under the
     * new name is a question about the encoding — Avro needs an alias, a Protobuf value rides its
     * tag — which the format-level checker owns.
     */
    private void validateStructs(
        List<FieldView> originalFields, List<FieldView> updateFields, String path,
        List<Integer> indexPath) {

      final Map<String, FieldView> originalFieldMap = originalFields.stream()
          .collect(Collectors.toMap(field -> field.name, field -> field));

      int updatePosition = -1;
      for (FieldView updateField : updateFields) {
        updatePosition++;
        final String fieldPath = childPath(path, updateField.name);
        // Struct fields are the one place both converters agree on the index convention: the
        // field's position within its struct, appended to the parent's path.
        final List<Integer> fieldIndexPath = appendIndex(indexPath, updatePosition);
        final FieldView originalField = originalFieldMap.get(updateField.name);

        // A new column must be readable for rows written before it existed.
        if (originalField == null) {
          if (!isNullableOrDefaulted(updateField, fieldIndexPath)) {
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

        validateDefaultNotRemoved(originalField, updateField, fieldPath, fieldIndexPath);

        compareTypes(originalField.schema, updateField.schema, fieldPath, fieldIndexPath);
      }
    }

    /**
     * The added-column predicate. Unlike Iceberg mode there is no container restriction and no
     * scalar exclusion: a Flink column may be NOT NULL and still carry a default, so a default on
     * any type suffices.
     *
     * <p>The default must be <b>non-null</b>, though — mere presence is not enough, for the same
     * reason it is not enough under Iceberg v3. A NOT NULL column whose default is null supplies
     * null to every row written before the column existed, which is precisely the value that column
     * cannot hold.
     *
     * <p>Defaults are read from <em>either</em> channel; see {@link #hasNonNullDefault}.
     */
    private boolean isNullableOrDefaulted(FieldView field, List<Integer> fieldIndexPath) {
      return isEffectivelyNullable(field) || hasNonNullDefault(field, fieldIndexPath);
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
     * <p>The original side reads only the field's own default, not the path-keyed map, because the
     * update-side index path is not valid for the original once reordering is permitted. That is
     * sound rather than a shortcut: the path-keyed channel carries defaults the converter
     * <em>derived</em> from format semantics — an absent {@code repeated} field is an empty list —
     * and those cannot be removed by editing a schema without also changing the type, which the
     * type rules already catch. The update side consults both channels, so a field that still has a
     * derived default does not trip this.
     */
    private void validateDefaultNotRemoved(
        FieldView originalField, FieldView updateField, String fieldPath,
        List<Integer> fieldIndexPath) {
      if (isEffectivelyNullable(originalField) || isEffectivelyNullable(updateField)) {
        return;
      }
      final boolean originalHadDefault =
          originalField.hasDefault && originalField.defaultValue != null;
      if (originalHadDefault && !hasNonNullDefault(updateField, fieldIndexPath)) {
        add(Rule.NON_NULLABLE_DEFAULT_REMOVED, fieldPath,
            "NOT NULL column lost its default; rows that omitted the column have no value to read");
      }
    }

    /**
     * Whether the default is present <em>and</em> non-null, from either channel. Mirrors
     * {@code IcebergComparison#hasNonNullDefault}; the two are kept separate so each class stays
     * diffable against its own reference.
     *
     * <p><b>Two channels exist and both count.</b> A user-declared default lands on the
     * {@link Schema.Field}. A default the converter derived from format semantics — an absent
     * {@code repeated} field is an empty list, an absent proto map is an empty map — lands only in
     * the schema's path-keyed map. Reading just the field would miss every derived default, which
     * is the majority of them.
     *
     * <p>A {@code null} path means the walk crossed an array, where the two converters disagree on
     * the index convention. There the lookup is skipped rather than guessed, so an undecidable case
     * reads as "no default" and fails closed.
     */
    private boolean hasNonNullDefault(FieldView field, List<Integer> fieldIndexPath) {
      if (field.hasDefault && field.defaultValue != null) {
        return true;
      }
      return fieldIndexPath != null && updateDefaults.get(fieldIndexPath) != null;
    }

    /** Container recursion for ARRAY. */
    private void validateLists(
        Schema original, Schema update, String path, List<Integer> indexPath) {
      // The element index path is not portable: the Avro converter appends 0 for an array element
      // while the Protobuf one appends nothing. Passing null marks the path unresolvable from here
      // down, which makes a default lookup below an array fail closed rather than guess.
      compareTypes(elementOf(original), elementOf(update), path + "[]", null);
    }

    /**
     * Container recursion for MULTISET, which Flink models as its own type rather than a MAP.
     */
    private void validateMultisets(
        Schema original, Schema update, String path, List<Integer> indexPath) {
      compareTypes(elementOf(original), elementOf(update), path + "[]", null);
    }

    /**
     * Container recursion for MAP. Both the key and the value are compared through the ordinary
     * type comparison, so each may widen exactly as far as a leaf elsewhere may.
     *
     * <p><b>Deliberately unlike {@link IcebergComparison#validateMaps}</b>, which freezes the key
     * outright and reports {@link Rule#MAP_KEY_TYPE_MISMATCH} for any change at all. That is right
     * there and was mirrored here for a while, but the reason does not carry over: Iceberg gives
     * a map key its own field ID and identifies it by that, so re-typing the key redefines the
     * field.
     * Flink's MAP has no such identity, and a key is just another value that has to remain readable
     * — so {@code MAP<INT, V> -> MAP<BIGINT, V>} is as safe as the same widening on an ordinary
     * column, and freezing it rejected a change nothing objects to.
     *
     * <p>An unsafe key change is still rejected; it simply surfaces as the leaf rule that actually
     * applies, at the {@code {key}} path, rather than as a single map-level finding.
     */
    private void validateMaps(
        Schema original, Schema update, String path, List<Integer> indexPath) {
      // No index appended for the key: neither converter records a default under one, and a map key
      // cannot be null or defaulted anyway.
      compareTypes(original.getKeyType(), update.getKeyType(), path + "{key}", null);
      // Both converters agree on the map value index, unlike the array element.
      compareTypes(original.getValueType(), update.getValueType(), path + "{}",
          appendIndex(indexPath, 1));
    }

    private static boolean isEffectivelyNullable(FieldView field) {
      return field.forcedNullable || field.schema.isNullable();
    }

    // -- primitives ------------------------------------------------------------------------------

    /**
     * Leaf type changes. Two independent parts, both required.
     *
     * <p>Part A delegates the root relation to {@link FlinkLogicalTypeCasts}, so this checker's
     * notion of a safe type change is Flink's own rather than a hand-picked one. Nullability is
     * excluded because the field-level nullability rule already covers it, and reporting it twice
     * would be noise.
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
            case FIXED_LENGTH:
            case VARIABLE_LENGTH:
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
