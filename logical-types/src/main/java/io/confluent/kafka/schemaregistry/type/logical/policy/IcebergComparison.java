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
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.keyOf;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.refKey;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.render;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.resolve;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.scaleOf;
import static io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.valueOf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * One comparison run under {@code Mode.ICEBERG_V2} or {@code Mode.ICEBERG_V3}. Holds the
 * accumulated findings and the two named-type tables needed to resolve
 * {@link Schema.Type#NAMED_TYPE_REF}, so the recursion stays parameter-light.
 *
 * <p>Applies the rules governing materialisation of a registered schema into an Apache Iceberg
 * table. An equivalent checker over {@code org.apache.iceberg.Schema} is maintained elsewhere and
 * is expected to reach the same verdicts from an SRLT input.
 *
 * <p><b>Both are live, so keep them in step.</b> The recursion shape and method names mirror that
 * implementation so the two can be diffed side by side. Departures, each noted at its site:
 * findings accumulate rather than throwing; the nullability relaxation is a predicate rather than a
 * rewrite; the promotion table and Iceberg's own type mapping are folded in rather than called out
 * to; and named-type references with their cycle guards have no counterpart at all, Iceberg schemas
 * being unable to recurse.
 *
 * <p><b>Deliberately stricter than the Iceberg spec.</b> The spec permits delete, rename and
 * reorder because Iceberg identifies fields by a stable ID. A {@link LogicalType} carries no field
 * IDs, so this compares two schemas <em>by name</em> and can distinguish neither a rename from a
 * delete-plus-add nor which of two orderings is newer. Do not relax these to match the spec without
 * first solving field-ID continuity.
 *
 * <p>{@link Rule#REQUIRED_FIELD_ADDED} is the one rule that <em>is</em> a genuine Iceberg
 * constraint: {@code initial-default} arrived in spec v3, so before that a newly added required
 * field has no value for pre-existing rows. Adding to a nested struct is allowed, subject to the
 * same optional-or-defaulted requirement at every level.
 *
 * <p><b>One rule exists in the reference and not here, conditionally.</b> It rejects <em>any</em>
 * field added below the root, but is unreachable from that implementation's entry point. Matching
 * its behaviour therefore contradicts a rule still written down there, whose exception two
 * downstream callers still handle — re-wiring one line there would split the two silently.
 *
 * <p>Types are compared through equivalence classes rather than by materialising a converted
 * schema, erasing what Iceberg does not model: {@code SMALLINT -> BIGINT} passes as
 * {@code int -> long}, {@code VARCHAR(50) -> VARCHAR(10)} because Iceberg has no string length. A
 * Flink-level checker owns the distinctions erased here. The classes come from Iceberg's own type
 * mapping, so keep {@link #icebergClassOf} in step with it.
 */
final class IcebergComparison {

  /**
   * Precision above which a Flink timestamp maps to Iceberg's nanosecond timestamp rather than its
   * microsecond one. Mirrors the threshold Iceberg's own type mapping uses.
   */
  private static final int MAX_ICEBERG_MICROS_PRECISION = 6;

  /**
   * Format version that added {@code initial-default} and {@code write-default}.
   */
  private static final int FORMAT_VERSION_WITH_COLUMN_DEFAULTS = 3;

  private final Schema originalRoot;
  private final Schema updateRoot;
  private final Map<String, Schema> originalNamedTypes;
  private final Map<String, Schema> updateNamedTypes;

  private final List<Incompatibility> findings = new ArrayList<>();

  /**
   * Reference pairs already compared. Only a {@link Schema.Type#NAMED_TYPE_REF} can form a cycle,
   * so recording the (original, update) pair guarantees termination.
   *
   * <p>Never emptied, deliberately: a named type reached from several places is compared once, so
   * one problem inside it yields one finding rather than one per reference site, and the walk stays
   * bounded — path-scoped bookkeeping would revisit a shared type once per path, exponential for a
   * chain of types that each reference the next twice. The cost is that a finding is reported at
   * the first path reaching the type rather than at every such path.
   */
  private final Set<String> comparedRefPairs = new HashSet<>();

  /**
   * Struct definition pairs already compared, keyed by object identity.
   *
   * <p>{@link #comparedRefPairs} alone is not enough: a named type is usually also the root, which
   * is reached directly rather than through a reference, so the definition would be walked twice
   * and every finding reported at two paths. Claiming in the STRUCT branch lets the root walk get
   * there first, so findings land at the shallower path. Identity rather than equality, because
   * {@link Schema} equality is structural and a recursive definition cannot be compared
   * structurally without recursing forever.
   *
   * <p>No rule is position-sensitive, so this affects only where a finding is reported, not whether
   * the verdict is right.
   */
  private final Map<Schema, Set<Schema>> comparedStructPairs = new IdentityHashMap<>();

  /**
   * In-progress ref pairs for {@link #erasedEquals}; see its javadoc.
   */
  private final Set<String> erasedRefPairsInProgress = new HashSet<>();

  /** Iceberg table format version being targeted. */
  private final int formatVersion;

  IcebergComparison(LogicalType original, LogicalType update, int formatVersion) {
    this.formatVersion = formatVersion;
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

  private void compareTypes(
      Schema original, Schema update, String path) {
    if (isRef(original) || isRef(update)) {
      // Dedup only when BOTH sides are refs. refKey returns "" for a non-ref, so a one-sided
      // ref would collapse every inline counterpart onto one key and silently drop findings after
      // the first. Inline-to-$ref is ordinary JSON Schema evolution.
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
      final FieldView originalField = originalFieldMap.get(updateField.name);

      if (originalField == null) {
        if (!isEffectivelyOptional(updateField, null)) {
          add(Rule.REQUIRED_FIELD_ADDED, fieldPath,
              "added field is neither nullable nor defaulted, so pre-existing rows have no "
                  + "value for it"
                  + (supportsColumnDefaults()
                      ? "; a non-null column default would make it readable"
                      : "; format-version 2 cannot store a column default"));
        }
        // Do not descend into a field the original schema never had.
        continue;
      }

      // Existing fields keep their relative order. The watermark advances even on a violation,
      // so a single swap yields one finding rather than cascading.
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
  private void validateLists(
      Schema original, Schema update, String path) {
    compareTypes(elementOf(original), elementOf(update), path + "[]");
  }

  /**
   * Mirrors the Iceberg-schema implementation's {@code validateMaps}.
   */
  private void validateMaps(
      Schema original, Schema update, String path) {
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
    // Representability is deliberately not asked here: it is a property of one schema rather
    // than of the change, so a first registration would escape it. ValidityChecker owns it.
    IcebergClass originalClass = icebergClassOf(original);
    IcebergClass updateClass = icebergClassOf(update);

    if (originalClass == updateClass) {
      switch (originalClass) {
        case DECIMAL:
          // Precision may widen, scale may not: the stored value is an unscaled integer, so
          // re-scaling reinterprets every existing row.
          if (scaleOf(update) != scaleOf(original)
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

  /**
   * Structural equality under Iceberg erasure, no promotion allowed. Used for map keys.
   *
   * <p>The main walk's cycle guards do not cover this path, so it carries its own. Without one a
   * recursive named type reached through a map key — or a MULTISET element, since Iceberg lowers
   * {@code MULTISET<T>} to {@code map<T, int>} — resolves forever and overflows the stack, and an
   * {@code Error} escaping here cannot be reported to the user as an incompatibility at all.
   *
   * <p>An in-progress stack rather than a memo: entries are removed on the way out, so a second,
   * unrelated occurrence of the same pair is still compared. Re-entering means a cycle, and a cycle
   * is structurally equal to itself, so the inner frame answers {@code true}.
   */
  private boolean erasedEquals(Schema original, Schema update) {
    if (isRef(original) && isRef(update)) {
      final String pairKey =
          original.getQualifiedName() + ' ' + update.getQualifiedName();
      if (!erasedRefPairsInProgress.add(pairKey)) {
        return true;
      }
      try {
        return erasedEqualsResolved(original, update);
      } finally {
        erasedRefPairsInProgress.remove(pairKey);
      }
    }
    return erasedEqualsResolved(original, update);
  }

  private boolean erasedEqualsResolved(Schema original, Schema update) {
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
              && scaleOf(left) == scaleOf(right);
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
   * Whether {@code field} may be treated as optional, folding in the container-nullability
   * relaxation.
   *
   * <p>Avro and Protobuf cannot encode a null container — an absent repeated field is an empty
   * list, an absent map an empty map — so the converters mark such columns non-nullable and record
   * an empty-container default. Iceberg v2 cannot persist a column default, so the v2-safe
   * equivalent is to treat the field as optional: old rows read null, new rows write empty, and for
   * lists and maps those are query-equivalent.
   *
   * <p>Deliberately narrow. <b>Containers only</b> — relaxing a required scalar would substitute
   * null for its configured default, a real semantic loss. <b>Newly added or already-nullable
   * fields only</b> — flipping a pre-existing required field would break consumers relying on
   * NOT NULL.
   *
   * <p>A predicate rather than a rewrite, because {@link Schema#setNullable} mutates in place and
   * the caller's schema must not be modified.
   *
   * @param originalField the matching field in the original schema, or {@code null} if newly added
   */
  private boolean isEffectivelyOptional(
      FieldView field, FieldView originalField) {
    if (isEffectivelyNullable(field)) {
      return true;
    }

    // v3 only: initial-default makes a newly added required field readable for older rows.
    // Newly added fields only -- initial-default "is set only when a field is added to an existing
    // schema", so it cannot rescue a column tightened after the fact.
    if (supportsColumnDefaults()
        && originalField == null
        && typeAllowsNonNullDefault(field.schema)
        && hasNonNullDefault(field)) {
      return true;
    }

    // The container relaxation, both versions: a derivation quirk rather than an Iceberg
    // capability, so v3 does not retire it.
    if (!field.hasDefault) {
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

  /**
   * Iceberg's promotion table, mirroring {@code TypeUtil#isPromotionAllowed}: {@code int -> long},
   * {@code float -> double}, and decimal precision widening at unchanged scale (handled by the
   * caller, which has the parameters). Identical at every format version.
   *
   * <p><b>{@code date -> timestamp} is deliberately absent, though spec v3 permits it</b>, because
   * no Iceberg implementation has that row: {@code TypeUtil#isPromotionAllowed} switches on
   * {@code INTEGER}, {@code FLOAT} and {@code DECIMAL} only, and {@code SchemaUpdate#updateColumn}
   * gates every type change on it, so the evolution throws whatever the format version says.
   * Accepting it would pass a schema at registration that then fails when the table is evolved.
   * Restore the edge, gated on format version, once upstream implements it.
   */
  private static boolean isPromotionAllowed(IcebergClass from, IcebergClass to) {
    if (from == IcebergClass.INT && to == IcebergClass.LONG) {
      return true;
    }
    return from == IcebergClass.FLOAT && to == IcebergClass.DOUBLE;
  }

  /**
   * Whether the default is present <em>and</em> non-null.
   *
   * <p>v3 requires both defaults to be non-null when a required field is added, so mere presence
   * is not enough. A null default leaves pre-existing rows with nothing to read.
   */
  private static boolean hasNonNullDefault(FieldView field) {
    return field.hasDefault && field.defaultValue != null;
  }

  private static boolean isEffectivelyNullable(FieldView field) {
    return field.forcedNullable || field.schema.isNullable();
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
}
