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

import io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.FieldView;
import io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.Kind;
import io.confluent.kafka.schemaregistry.type.logical.check.Incompatibility.Rule;

import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.childPath;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.describeChange;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.elementOf;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.fieldViews;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.isRef;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.keyOf;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.refKey;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.render;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.resolve;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.scaleOf;
import static io.confluent.kafka.schemaregistry.type.logical.check.CompatibilityChecker.valueOf;

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
 * {@link Schema.Type#NAMED_TYPE_REF}, so the recursion itself stays parameter-light.
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
 * <p><b>One rule exists in the reference and not here, and the divergence is conditional.</b> That
 * implementation carries a nested-field-added rule that rejects <em>any</em> field added below the
 * root, optional or not. It is unreachable: its entry point calls only the recursive compatibility
 * walk, and the nested-field cluster is entered from nowhere but itself. So matching its
 * <em>behaviour</em>, as above, means contradicting a rule still written down there — and two
 * callers downstream of it still catch and format that exception. Re-wiring one line there would
 * split the two checkers silently, which is worth knowing for anyone diffing them.
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
   * Reference pairs already compared. A cycle can only be formed by following a
   * {@link Schema.Type#NAMED_TYPE_REF}, so recording the (original, update) reference pair is
   * enough to guarantee termination.
   *
   * <p>Entries are never removed, which makes this a global set rather than a path-scoped one.
   * That is deliberate and does two things beyond termination:
   *
   * <ul>
   *   <li>A named type reached from several places is compared once, so one problem inside it
   *       yields one finding rather than one per reference site.
   *   <li>It bounds the walk. Path-scoped bookkeeping would revisit a shared type once per path,
   *       which is exponential for a chain of types that each reference the next twice.
   * </ul>
   *
   * <p>The cost is that a finding is reported at the first path that reaches the type rather than
   * at every such path.
   *
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
   *
   * <p>Now that no rule is position-sensitive, this guard is purely about where a finding is
   * reported and how often, not about whether the verdict is right.
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
      // Struct fields are the one place both converters agree on the index convention: the
      // field's position within its struct, appended to the parent's path.
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
    // Both converters agree on the map value index, unlike the array element.
    compareTypes(valueOf(original), valueOf(update), path + "{}");
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
   * Structural equality under Iceberg erasure, with no promotion allowed. Used for map keys.
   *
   * <p>The main walk's cycle guards do not cover this path, so it carries its own. Without one, a
   * recursive named type reached through a map key — or a MULTISET element, since Iceberg lowers
   * {@code MULTISET<T>} to {@code map<T, int>} — resolves to the same struct forever and
   * overflows the stack. An {@code Error} escaping here is worse than any finding, because it
   * cannot be reported to the user as an incompatibility at all.
   *
   * <p>The guard is an in-progress stack rather than a memo: entries are removed on the way out,
   * so a second, unrelated occurrence of the same pair is still compared properly. Re-entering a
   * pair means a cycle, and a cycle is structurally equal to itself, so the inner frame answers
   * {@code true} and lets the outer one decide.
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
   * <p>Defaults are read from {@link Schema.Field} alone. Both channels are represented there: a
   * user-declared default, and one a converter derived from format semantics and marked
   * {@code derived} so no writer emits it.
   *
   * @param originalField the matching field in the original schema, or {@code null} if the field
   *                      is newly added
   */
  private boolean isEffectivelyOptional(
      FieldView field, FieldView originalField) {
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
        && hasNonNullDefault(field)) {
      return true;
    }

    // The container relaxation, in both versions. It addresses a derivation quirk rather than an
    // Iceberg capability -- proto and Avro containers are marked NOT NULL because those formats
    // cannot encode a null container -- so v3 does not retire it.
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
   * caller, which has the parameters). Identical for every format version.
   *
   * <p><b>{@code date -> timestamp} is deliberately absent, though spec v3 permits it.</b> The
   * spec's v3 row allows promoting {@code date} to {@code timestamp} and {@code timestamp_ns}
   * (never to a {@code timestamptz}, since a date carries no zone). No Iceberg implementation has
   * that row: {@code TypeUtil#isPromotionAllowed} switches on {@code INTEGER}, {@code FLOAT} and
   * {@code DECIMAL} only, and {@code SchemaUpdate#updateColumn} gates every type change on that
   * same function, so the evolution throws regardless of format version. Accepting it here would
   * pass a schema at registration that then fails when the table is evolved — the failure this
   * checker exists to prevent, reached from the other side. It would also be the one type rule
   * where this port is more permissive than the checker it mirrors, which rejects it for the same
   * reason.
   *
   * <p>Restore the edge when upstream implements it, gated on format version.
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
