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

/**
 * Entry point for checking a {@link LogicalType} against a downstream consumer's rules.
 *
 * <p>Two questions, deliberately kept apart:
 *
 * <ul>
 *   <li>{@link #validate} takes <b>one</b> schema and asks whether the consumer can use it at all —
 *       is the decimal precision within range, is the struct non-empty, does a named type refer to
 *       itself. Applies to a first registration and to every later one.
 *   <li>{@link #compare} takes <b>two</b> and asks whether the change between them is safe — was a
 *       field dropped, reordered, tightened to non-null, or retyped in a way the consumer
 *       forbids.
 * </ul>
 *
 * <p><b>A caller registering a schema needs both.</b> Neither subsumes the other, and the gap is
 * not hypothetical: a schema whose decimal precision exceeds what the consumer supports is unusable
 * whether or not it changed, so a pairwise check alone lets it through on the subject's first
 * registration and on any later one that leaves the offending column untouched. The intended shape
 * is:
 *
 * <pre>{@code
 * ValidityResult validity = LogicalTypeChecker.validate(mode, update);
 * CompatibilityResult compatibility = previous == null
 *     ? CompatibilityResult.compatible()
 *     : LogicalTypeChecker.compare(mode, previous, update);
 * if (!validity.isValid() || !compatibility.isCompatible()) {
 *   // reject, reporting validity.describe() and compatibility.describe()
 * }
 * }</pre>
 *
 * <p>Both results collect every finding rather than stopping at the first, so one pass yields the
 * complete set of problems.
 *
 * <p>Note also that {@code io.confluent.kafka.schemaregistry.CompatibilityChecker} — the
 * format-level check this one is layered with — shares a simple name with {@link
 * CompatibilityChecker} here. A caller running both would have to fully qualify one of them; going
 * through this class avoids the collision entirely.
 *
 * <p>This class is a thin façade. The rules live in {@link CompatibilityChecker} and
 * {@link ValidityChecker}, which stay separate for a reason: the compatibility rules are maintained
 * against two external reference implementations and are structured to be diffed against them
 * line by line, and folding them together would destroy that. Read those classes for the rules;
 * call this one.
 */
public final class LogicalTypeChecker {

  private LogicalTypeChecker() {
  }

  /** The downstream consumer whose rules should be applied. */
  public enum Mode {
    /**
     * Flink SQL tables. Compares the Flink logical types the two schemas derive to; see
     * {@link CompatibilityChecker}.
     */
    FLINK,

    /**
     * Materialization into an Apache Iceberg table at format-version 2. Stricter than the Iceberg
     * spec; see {@link CompatibilityChecker}.
     */
    ICEBERG_V2,

    /**
     * Materialization into an Apache Iceberg table at format-version 3.
     *
     * <p>A relaxation of {@link #ICEBERG_V2} in the rules, and a tightening in what it will
     * represent. v3 adds {@code initial-default} and {@code write-default}, so a newly added
     * required field becomes legal when it carries a non-null default; and it adds the nanosecond
     * timestamp types and {@code variant}, which v2 cannot store at all.
     */
    ICEBERG_V3
  }

  /**
   * Whether {@code update} may replace {@code original} under {@code mode}.
   *
   * <p>Direction is BACKWARD: the {@code update} schema must be able to read data written with the
   * {@code original} schema. Says nothing about whether either schema is usable on its own terms —
   * pair that with {@link #validate}.
   *
   * @param mode     which consumer's rules to apply
   * @param original the currently registered schema
   * @param update   the proposed schema
   * @return every violation found, or {@link CompatibilityResult#compatible()} if there are none
   */
  public static CompatibilityResult compare(
      Mode mode, LogicalType original, LogicalType update) {
    return CompatibilityChecker.compare(mode, original, update);
  }

  /**
   * Whether {@code type} can be used by {@code mode}'s consumer at all.
   *
   * <p>Independent of any previous version, so this is the only one of the two checks that can
   * reject a subject's first registration.
   *
   * @param mode which consumer's rules to apply
   * @param type the schema to validate
   * @return every violation found, or {@link ValidityResult#valid()} if there are none
   */
  public static ValidityResult validate(Mode mode, LogicalType type) {
    return ValidityChecker.validate(mode, type);
  }
}
