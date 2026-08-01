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

import io.confluent.kafka.schemaregistry.type.logical.check.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.check.Invalidity.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks whether a single {@link LogicalType} can be used by a given downstream consumer at all.
 * {@link LogicalTypeChecker} is the entry point; this class holds the rules. All violations are
 * collected; see {@link ValidityResult}.
 *
 * <p>The companion to {@link CompatibilityChecker}, which needs two schemas and so says nothing
 * about the first registered on a subject. Two rules live here rather than there — decimal
 * precision above {@value #MAX_DECIMAL_PRECISION}, and sub-microsecond timestamps under
 * {@link Mode#ICEBERG_V2} — precisely because a first registration escaped them.
 *
 * <p>Three rules are mode-specific: {@link Rule#EMPTY_STRUCT} and
 * {@link Rule#FIELD_NAME_CASE_COLLISION} under the Iceberg modes only,
 * {@link Rule#UNREPRESENTABLE_TYPE} under {@code ICEBERG_V2} only.
 *
 * <h2>Every rule must clear the constructibility bar</h2>
 *
 * <p>{@link Schema} validates a good deal in its own constructors, and anything it rejects can
 * never reach here, so a rule for it would be dead code and untestable. It already guarantees
 * fractional-second precision in 0..9, non-blank field and branch names, and no duplicate names
 * within a struct, union or enum.
 *
 * <p><b>Determine that empirically, not by reading.</b> The {@code create*} factory bodies contain
 * no validation — it lives in the constructors they delegate to — so surveying the factories
 * concludes, wrongly, that SRLT validates nothing. A related trap: {@link Schema#NO_PARAM} is
 * {@code -1}, an "unspecified" sentinel rather than invalid input, so a naive reject-negative rule
 * fires on legitimate schemas.
 *
 * <h2>What is deliberately not a rule</h2>
 *
 * <p>Each was considered and rejected on the merits. Do not add one without defeating the argument.
 *
 * <ul>
 *   <li><b>MULTISET</b> — Iceberg's own type mapping maps it to {@code map<T, int>}, so it is
 *       representable; a rejection elsewhere in the stack is policy, not a limit of the type
 *       system.
 *   <li><b>{@code TIME(p > 3)}</b> — the conversion to a Flink type retypes it to BIGINT rather
 *       than failing. Lossy, but deliberate, and not this checker's to override.
 *   <li><b>A DECIMAL with {@link Schema#NO_PARAM} scale</b> — SRLT's encoding of an omitted scale.
 *       {@code DECIMAL(p)} means {@code DECIMAL(p, 0)} in SQL, so rejecting it would reject valid
 *       DDL.
 *   <li><b>An ENUM with no symbols</b> — derives to an unbounded VARCHAR, which no consumer objects
 *       to.
 *   <li><b>A non-struct root</b> — whether a table may have a scalar at its root is a question
 *       about tables, not types.
 * </ul>
 */
final class ValidityChecker {

  /**
   * Lowest decimal precision the engine's decimal type permits.
   */
  private static final int MIN_DECIMAL_PRECISION = 1;

  /**
   * Highest decimal precision. The engine's decimal type and Iceberg's {@code decimal}
   * agree on this bound, and Iceberg's holds in every format version.
   */
  private static final int MAX_DECIMAL_PRECISION = 38;

  /**
   * Lowest length the engine's fixed-length character and binary types permit. The unbounded
   * variants additionally accept zero; see {@link #validateLength}.
   */
  private static final int MIN_FIXED_LENGTH = 1;

  /**
   * Precision above which a timestamp needs Iceberg's nanosecond timestamp types, which arrived in
   * format-version 3.
   */
  private static final int MAX_ICEBERG_MICROS_PRECISION = 6;

  private ValidityChecker() {
  }

  /**
   * Validates {@code type} against {@code mode}.
   *
   * @param mode which consumer's rules to apply
   * @param type the schema to validate
   * @return every violation found, or {@link ValidityResult#valid()} if there are none
   */
  public static ValidityResult validate(Mode mode, LogicalType type) {
    Objects.requireNonNull(mode, "mode");
    Objects.requireNonNull(type, "type");
    return new Walk(mode, type).run();
  }

  /** One traversal of one schema, accumulating findings. */
  private static final class Walk {

    private final Mode mode;
    private final LogicalType logicalType;
    private final Map<String, Schema> namedTypes;
    private final List<Invalidity> found = new ArrayList<>();

    /**
     * Named types whose body has already been walked. Serves two purposes: it terminates the walk
     * on a cyclic schema, and it keeps a named type used by several fields from being reported once
     * per use — problems surface at the first path that reaches it, not at every path that does.
     *
     * <p>Only types reachable from the root are walked at all. An unreferenced named type is never
     * inlined by the conversion to a Flink type, so its contents cannot break a consumer.
     */
    private final Set<String> walkedNamedTypes = new HashSet<>();

    Walk(Mode mode, LogicalType logicalType) {
      this.mode = mode;
      this.logicalType = logicalType;
      this.namedTypes = logicalType.getNamedTypes();
    }

    ValidityResult run() {
      checkInvalidTypeRecursive(logicalType.getRootSchema(), "");
      return ValidityResult.of(found);
    }

    /**
     * Mirrors the same-named walk in the validator on the Iceberg materialization path: empty-row
     * check before descending, dot-joined field paths, an element marker for arrays, key and value
     * both descended for maps. Kept in that shape so the two can be diffed.
     *
     * <p>Two departures. That validator holds only two type-level rules and inspects an
     * already-converted Flink type, so most rules here have no counterpart. And its map paths read
     * {@code [key]} / {@code [value]} where these read <code>{key}</code> / <code>{}</code> — the
     * two references disagree with each other, so this follows the Iceberg-schema comparison,
     * giving a caller one path syntax across both checks.
     */
    private void checkInvalidTypeRecursive(Schema schema, String path) {
      if (schema == null) {
        return;
      }
      switch (schema.getType()) {
        case STRUCT:
          validateNonEmpty(schema.getFields().size(), "struct", path);
          validateNoCaseCollision(namesOf(schema), path);
          for (Schema.Field field : schema.getFields()) {
            checkInvalidTypeRecursive(field.getSchema(), childPath(path, field.getName()));
          }
          return;
        case UNION:
          validateNonEmpty(schema.getBranches().size(), "union", path);
          validateNoCaseCollision(namesOf(schema), path);
          for (Schema.UnionBranch branch : schema.getBranches()) {
            checkInvalidTypeRecursive(branch.getSchema(), childPath(path, branch.getName()));
          }
          return;
        case ARRAY:
        case MULTISET:
          checkInvalidTypeRecursive(schema.getElementType(), path + "[]");
          return;
        case MAP:
          checkInvalidTypeRecursive(schema.getKeyType(), path + "{key}");
          checkInvalidTypeRecursive(schema.getValueType(), path + "{}");
          return;
        case NAMED_TYPE_REF:
          validateNamedTypeRef(schema, path);
          return;
        default:
          validatePrimitive(schema, path);
      }
    }

    /**
     * Resolves a reference and walks its body. Both failure modes are fatal at derivation time: an
     * unresolved name has nothing to inline, and a cycle cannot be inlined at all because neither
     * Flink nor Iceberg has a recursive type.
     */
    private void validateNamedTypeRef(Schema ref, String path) {
      final String name = ref.getQualifiedName();
      final Schema body = namedTypes.get(name);
      if (body == null) {
        add(Rule.UNRESOLVED_TYPE_REF, path,
            "named type '" + name + "' is referenced but not defined");
        return;
      }
      if (logicalType.isCyclic(name)) {
        add(Rule.CYCLIC_TYPE, path,
            "named type '" + name + "' refers to itself, and cannot be represented by a "
                + "non-recursive type system");
        return;
      }
      if (walkedNamedTypes.add(name)) {
        checkInvalidTypeRecursive(body, path);
      }
    }

    private void validatePrimitive(Schema schema, String path) {
      switch (schema.getType()) {
        case DECIMAL:
          validateDecimal(schema, path);
          return;
        case TIME:
          // Nothing to check: Schema pins the precision to 0..9 at construction, and Iceberg erases
          // it because it has no nanosecond time type.
          return;
        case TIMESTAMP:
        case TIMESTAMP_LTZ:
          validateTimestampRepresentable(schema, path);
          return;
        case CHAR:
        case BINARY:
          validateLength(schema, path, false);
          return;
        case VARCHAR:
        case VARBINARY:
          validateLength(schema, path, true);
          return;
        case VARIANT:
          if (mode == Mode.ICEBERG_V2) {
            add(Rule.UNREPRESENTABLE_TYPE, path,
                "VARIANT needs Iceberg's variant type, which arrived in format-version 3");
          }
          return;
        default:
          // Everything else carries no parameters and every consumer can represent it.
      }
    }

    private void validateDecimal(Schema schema, String path) {
      final int precision = schema.getPrecision();
      final int scale = schema.getScale();
      if (precision < MIN_DECIMAL_PRECISION || precision > MAX_DECIMAL_PRECISION) {
        add(Rule.PRECISION_OUT_OF_RANGE, path,
            "DECIMAL precision " + precision + " is outside the supported range "
                + MIN_DECIMAL_PRECISION + " to " + MAX_DECIMAL_PRECISION);
      }
      if (scale == Schema.NO_PARAM) {
        // SRLT's encoding of an omitted scale, which SQL reads as zero. A legitimate schema.
        return;
      }
      // Compared against the declared precision even when that is itself out of range: the scale
      // has to fit the precision that was written down, whatever else is wrong with it.
      if (scale < 0 || scale > precision) {
        add(Rule.SCALE_OUT_OF_RANGE, path,
            "DECIMAL scale " + scale + " is outside the supported range 0 to the precision "
                + precision);
      }
    }

    /**
     * Iceberg v2 stores timestamps as microseconds. Anything finer needs {@code timestamp_ns} or
     * {@code timestamptz_ns}, which arrived in format-version 3. Unlike the compatibility checker's
     * type mapping, which maps such a timestamp onto the nanosecond class to work out which
     * promotions apply, this is a flat question of whether the target table can hold it.
     */
    private void validateTimestampRepresentable(Schema schema, String path) {
      if (mode != Mode.ICEBERG_V2) {
        return;
      }
      if (schema.getPrecision() > MAX_ICEBERG_MICROS_PRECISION) {
        add(Rule.UNREPRESENTABLE_TYPE, path,
            schema.getType() + "(" + schema.getPrecision() + ") needs Iceberg's nanosecond "
                + "timestamp types, which arrived in format-version 3");
      }
    }

    /**
     * Length bounds. The bounded types pass their length straight through to Flink, which requires
     * at least one character or byte. The unbounded types additionally treat zero as "no bound
     * declared" and widen it to the maximum, so zero is accepted there but a negative length never
     * is.
     */
    private void validateLength(Schema schema, String path, boolean zeroMeansUnbounded) {
      final int length = schema.getLength();
      final int min = zeroMeansUnbounded ? 0 : MIN_FIXED_LENGTH;
      if (length < min) {
        add(Rule.LENGTH_OUT_OF_RANGE, path,
            schema.getType() + " length " + length + " is below the minimum of " + min);
      }
    }

    /**
     * Names that differ only in case: an Iceberg problem, not a Flink one.
     *
     * <p>Iceberg indexes field names case-insensitively (a lower-cased name-to-id map behind
     * {@code caseInsensitiveFindField}), so two names differing only in case collide there. Flink's
     * row-type duplicate check is case-sensitive, so {@code a} and {@code A} are two ordinary
     * columns. Hence Iceberg modes only.
     *
     * <p>An exact duplicate cannot reach here — {@link Schema} rejects those at construction — so
     * any collision found is necessarily a case-only one.
     */
    private void validateNoCaseCollision(List<String> names, String path) {
      if (!isIceberg()) {
        return;
      }
      final Map<String, String> byLowerCase = new LinkedHashMap<>();
      final Set<String> reported = new LinkedHashSet<>();
      for (String name : names) {
        final String key = name.toLowerCase(Locale.ROOT);
        final String first = byLowerCase.putIfAbsent(key, name);
        if (first != null && reported.add(key)) {
          add(Rule.FIELD_NAME_CASE_COLLISION, path,
              "field names '" + first + "' and '" + name + "' differ only in case, which Iceberg's "
                  + "case-insensitive name index cannot distinguish");
        }
      }
    }

    private boolean isIceberg() {
      return mode == Mode.ICEBERG_V2 || mode == Mode.ICEBERG_V3;
    }

    private static List<String> namesOf(Schema schema) {
      if (schema.getType() == Schema.Type.UNION) {
        final List<String> names = new ArrayList<>(schema.getBranches().size());
        for (Schema.UnionBranch branch : schema.getBranches()) {
          names.add(branch.getName());
        }
        return names;
      }
      final List<String> names = new ArrayList<>(schema.getFields().size());
      for (Schema.Field field : schema.getFields()) {
        names.add(field.getName());
      }
      return names;
    }

    /**
     * Iceberg modes only, mirroring the reference validator that this rule was ported from.
     *
     * <p>Not applied under {@link Mode#FLINK}, because nothing on the Flink side rejects it: a
     * a row type constructs with zero fields, the {@code CREATE TABLE} column list is optional
     * in the grammar, and no minimum-column check was found on a resolved schema. So an empty
     * struct neither reinterprets nor invents a value, and the criterion in
     * {@code FlinkComparison} admits it. It was applied to every mode for a while purely because
     * the walk is shared, which is not a reason.
     */
    private void validateNonEmpty(int childCount, String kind, String path) {
      if (!isIceberg()) {
        return;
      }
      if (childCount == 0) {
        add(Rule.EMPTY_STRUCT, path, "a " + kind + " must have at least one field");
      }
    }

    private static String childPath(String parentPath, String fieldName) {
      return parentPath.isEmpty() ? String.valueOf(fieldName) : parentPath + '.' + fieldName;
    }

    private void add(Rule rule, String path, String message) {
      found.add(new Invalidity(rule, path, message));
    }
  }
}
