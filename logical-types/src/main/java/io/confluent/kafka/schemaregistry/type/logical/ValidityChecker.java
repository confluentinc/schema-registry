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

import io.confluent.kafka.schemaregistry.type.logical.CompatibilityChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.Invalidity.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks whether a single {@link LogicalType} can be used by a given downstream consumer at all.
 *
 * <p>The companion to {@link CompatibilityChecker}, and the answer to a different question. A
 * compatibility check needs two schemas, so it has nothing to say about the <em>first</em> schema
 * registered on a subject — that one is compared against nothing and passes unconditionally. This
 * checker takes one schema and asks whether the consumer can consume it, which applies equally to a
 * first registration and to every later one. All violations are collected; see
 * {@link ValidityResult}.
 *
 * <p><b>Both checks are needed; neither subsumes the other.</b> A caller registering a schema
 * should validate the proposed schema and, when a previous version exists, also compare the pair.
 * Two rules used to live in {@link CompatibilityChecker} and now live only here — decimal precision
 * above {@value #MAX_DECIMAL_PRECISION}, and sub-microsecond timestamps under
 * {@link Mode#ICEBERG_V2} — precisely because a first registration escaped them.
 *
 * <h2>Structure</h2>
 *
 * <p>Unlike {@link CompatibilityChecker}, which keeps a separate comparison class per consumer,
 * this is a single walk with a handful of mode-conditional branches. That is a deliberate
 * difference and not an inconsistency: the compatibility rules genuinely diverge between consumers,
 * whereas almost every validity rule here is shared. Only two are mode-specific, both about what
 * Iceberg v2 can store.
 *
 * <h2>What is checked, and why the list is short</h2>
 *
 * <p>Every rule here has to clear one bar: the schema must be <em>constructible</em>. {@link
 * Schema} validates a good deal in its own constructors, and anything it rejects can never reach
 * this checker, so re-checking it would be dead code. Specifically, it already guarantees that
 * fractional-second precision on TIME and the timestamps lies in 0 to 9, that no field or union
 * branch name is empty or contains whitespace, and that no two fields of a struct — or two branches
 * of a union — share a name. None of those is a rule here, and adding one would be untestable.
 *
 * <p>What it does <em>not</em> validate, and so is checked here:
 *
 * <ul>
 *   <li>decimal precision and scale, which are entirely unchecked — Avro caps precision only at
 *       what the underlying {@code fixed} can hold, well above {@value #MAX_DECIMAL_PRECISION};
 *   <li>character- and binary-string length, where a zero is accepted by SRLT but rejected by
 *       Flink's {@code CharType} and {@code BinaryType};
 *   <li>a struct or union with no fields, which SRLT builds happily and which derives to a row type
 *       with no columns;
 *   <li>a named type that refers to itself, or one that resolves to nothing — neither Flink nor
 *       Iceberg has a recursive type, so a cycle cannot be inlined at all.
 * </ul>
 *
 * <p>Only the last group is structural; the rest are parameter ranges taken from the constructors
 * of the corresponding Flink types. Two further rules are mode-specific, both about what Iceberg v2
 * can store: sub-microsecond timestamps and VARIANT both need format-version 3.
 *
 * <h2>Reachability of named types</h2>
 *
 * <p>Only named types reachable from the root are walked, and each is walked once. An unreferenced
 * named type is never inlined by the conversion to a Flink type, so its contents cannot break a
 * consumer; and walking a shared named type once reports its problems at the first path that
 * reaches it rather than at every path that does.
 *
 * <h2>What is deliberately not a rule</h2>
 *
 * <p>MULTISET is accepted. Iceberg's own {@code FlinkTypeToType} maps it to {@code map<T, int>}, so
 * it is representable; a rejection elsewhere in the stack is policy rather than a limit of the type
 * system.
 *
 * <p>A TIME with precision above 3 is accepted. The conversion to a Flink type maps it to BIGINT
 * rather than failing, because Flink carries TIME as an int millis-of-day — a silent retyping, but
 * not an error, and not this checker's to invent.
 *
 * <p>A DECIMAL whose scale is {@link Schema#NO_PARAM} is accepted. That is SRLT's encoding of a
 * scale the author omitted, and {@code DECIMAL(p)} means {@code DECIMAL(p, 0)} in SQL, so the
 * schema is legitimate and rejecting it would reject valid DDL.
 *
 * <p>An ENUM with no symbols is accepted: it derives to an unbounded VARCHAR, which no consumer
 * objects to. A non-struct root is accepted: whether a table may have a scalar at its root is a
 * question about tables, not about types.
 */
public final class ValidityChecker {

  /**
   * Lowest decimal precision Flink's {@code DecimalType} permits.
   */
  private static final int MIN_DECIMAL_PRECISION = 1;

  /**
   * Highest decimal precision. Flink's {@code DecimalType} and Iceberg's {@code decimal} agree on
   * this bound, and Iceberg's holds in every format version.
   */
  private static final int MAX_DECIMAL_PRECISION = 38;

  /**
   * Lowest length Flink's {@code CharType} and {@code BinaryType} permit. The unbounded variants
   * additionally accept zero; see {@link #validateLength}.
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
     * on a cyclic schema, and it keeps a named type used by several fields from being reported
     * once per use.
     */
    private final Set<String> walkedNamedTypes = new HashSet<>();

    Walk(Mode mode, LogicalType logicalType) {
      this.mode = mode;
      this.logicalType = logicalType;
      this.namedTypes = logicalType.getNamedTypes();
    }

    ValidityResult run() {
      validateType(logicalType.getRootSchema(), "");
      return ValidityResult.of(found);
    }

    private void validateType(Schema schema, String path) {
      if (schema == null) {
        return;
      }
      switch (schema.getType()) {
        case STRUCT:
          validateNonEmpty(schema.getFields().size(), "struct", path);
          for (Schema.Field field : schema.getFields()) {
            validateType(field.getSchema(), childPath(path, field.getName()));
          }
          return;
        case UNION:
          validateNonEmpty(schema.getBranches().size(), "union", path);
          for (Schema.UnionBranch branch : schema.getBranches()) {
            validateType(branch.getSchema(), childPath(path, branch.getName()));
          }
          return;
        case ARRAY:
        case MULTISET:
          validateType(schema.getElementType(), path + "[]");
          return;
        case MAP:
          validateType(schema.getKeyType(), path + "{key}");
          validateType(schema.getValueType(), path + "{}");
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
        validateType(body, path);
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

    private void validateNonEmpty(int childCount, String kind, String path) {
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
