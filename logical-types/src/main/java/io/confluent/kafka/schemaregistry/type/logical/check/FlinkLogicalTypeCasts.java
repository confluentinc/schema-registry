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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Apache Flink's implicit type-widening rules, ported.
 *
 * <p>This is a transcription of {@code implicitCastingRules} in Flink's
 * {@code org.apache.flink.table.types.logical.utils.LogicalTypeCasts}. Depending on
 * {@code flink-table-common} directly is not an option here — it would impose the whole Flink table
 * stack on every consumer of this module — so the table is mirrored instead.
 *
 * <p><b>Keep this in sync with the Flink original.</b> The enum names, the builder API, and the
 * order in which rules are declared all deliberately match it, so {@link #IMPLICIT_CASTING_RULES}'s
 * static initialiser can be diffed against Flink's line for line. Two things are intentionally
 * absent: the roots that no {@link Schema.Type} maps onto (structured, raw, symbol, interval, null,
 * timestamp-with-time-zone) and the explicit and injective cast tables, which no rule here
 * consults.
 *
 * <p><b>Seven edges are deliberately narrower than Flink's</b>, each marked
 * {@code DIVERGENCE FROM FLINK} at its declaration. Flink's table serves query planning, where an
 * inserted cast may legitimately truncate, so "implicit" there does not imply lossless. A stored
 * column has no such licence: the rows were written under the old type and nothing rewrites them.
 * The divergences are kept as edits to the table, rather than as a filter layered on top, so that
 * each is expressible as a patch against Flink should the same reasoning apply upstream.
 *
 * <p>Flink's own {@code supportsImplicitCast} additionally short-circuits on nullability and on
 * structural types before reaching this table. Neither applies here: nullability is a field-level
 * rule and structural recursion is the caller's job, so this class answers the root question only.
 *
 * <p><b>This table is deliberately parameter-blind</b>, exactly as Flink's is: it is keyed by type
 * root and never reads a length, precision, or scale. That is not an oversight to be tidied up —
 * Flink's rules serve query planning, where a cast executor may legitimately truncate. Callers must
 * apply their own parameter guards on top; see {@code CompatibilityChecker}'s Flink mode.
 */
final class FlinkLogicalTypeCasts {

  /**
   * Mirrors the subset of Flink's {@code LogicalTypeRoot} that a {@link Schema.Type} can produce.
   *
   * <p>{@code TIMESTAMP} and {@code TIMESTAMP_LTZ} correspond to Flink's
   * {@code TIMESTAMP_WITHOUT_TIME_ZONE} and {@code TIMESTAMP_WITH_LOCAL_TIME_ZONE}; the shorter
   * names match this module's {@link Schema.Type} spelling.
   */
  enum Root {
    CHAR,
    VARCHAR,
    BINARY,
    VARBINARY,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    DECIMAL,
    FLOAT,
    DOUBLE,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMP_LTZ,
    VARIANT
  }

  /**
   * Mirrors the subset of Flink's {@code LogicalTypeFamily} the implicit rules refer to.
   *
   * <p>{@code NUMERIC} is retained even though no rule below still uses it: Flink declares three of
   * its rules against it, and keeping it here makes those divergences legible as a narrowing from
   * {@code NUMERIC} to {@code EXACT_NUMERIC} rather than as an unexplained list.
   */
  private enum Family {
    CHARACTER_STRING(Root.CHAR, Root.VARCHAR),
    BINARY_STRING(Root.BINARY, Root.VARBINARY),
    EXACT_NUMERIC(Root.TINYINT, Root.SMALLINT, Root.INTEGER, Root.BIGINT, Root.DECIMAL),
    NUMERIC(Root.TINYINT, Root.SMALLINT, Root.INTEGER, Root.BIGINT, Root.DECIMAL,
        Root.FLOAT, Root.DOUBLE);

    private final Set<Root> roots;

    Family(Root... roots) {
      this.roots = EnumSet.copyOf(Arrays.asList(roots));
    }
  }

  private static final Map<Root, Set<Root>> IMPLICIT_CASTING_RULES = new EnumMap<>(Root.class);

  static {
    // Identity casts: all types can be implicitly cast to themselves.
    for (Root typeRoot : Root.values()) {
      castTo(typeRoot).implicitFrom(typeRoot).build();
    }

    // -------------------------------------------------------------------------------------------
    // Character string types
    // -------------------------------------------------------------------------------------------

    castTo(Root.CHAR)
        .implicitFrom(Root.CHAR)
        .build();

    castTo(Root.VARCHAR)
        .implicitFromFamily(Family.CHARACTER_STRING)
        .build();

    // -------------------------------------------------------------------------------------------
    // Binary string types
    // -------------------------------------------------------------------------------------------

    castTo(Root.BINARY)
        .implicitFrom(Root.BINARY)
        .build();

    castTo(Root.VARBINARY)
        .implicitFromFamily(Family.BINARY_STRING)
        .build();

    // -------------------------------------------------------------------------------------------
    // Exact numeric types
    // -------------------------------------------------------------------------------------------

    castTo(Root.TINYINT)
        .implicitFrom(Root.TINYINT)
        .build();

    castTo(Root.SMALLINT)
        .implicitFrom(Root.TINYINT, Root.SMALLINT)
        .build();

    castTo(Root.INTEGER)
        .implicitFrom(Root.TINYINT, Root.SMALLINT, Root.INTEGER)
        .build();

    castTo(Root.BIGINT)
        .implicitFrom(Root.TINYINT, Root.SMALLINT, Root.INTEGER, Root.BIGINT)
        .build();

    // DIVERGENCE FROM FLINK: Flink uses implicitFromFamily(NUMERIC), which admits FLOAT and DOUBLE.
    // Narrowed to EXACT_NUMERIC. No DECIMAL can hold DOUBLE's range -- the widest Flink permits,
    // DECIMAL(38,18), spans about 1e20 against DOUBLE's 1e308 -- so the conversion overflows rather
    // than rounds, truncates any fraction beyond the scale, and has no representation at all for
    // NaN or the infinities.
    castTo(Root.DECIMAL)
        .implicitFromFamily(Family.EXACT_NUMERIC)
        .build();

    // -------------------------------------------------------------------------------------------
    // Approximate numeric types
    // -------------------------------------------------------------------------------------------

    // DIVERGENCE FROM FLINK: Flink also admits INTEGER, BIGINT and DECIMAL. A FLOAT significand is
    // 24 bits, so it represents integers exactly only to 2^24; INTEGER and BIGINT exceed that and
    // would be silently rounded. TINYINT and SMALLINT fit and are kept. DECIMAL is dropped because
    // a binary significand cannot hold every decimal fraction.
    castTo(Root.FLOAT)
        .implicitFrom(Root.TINYINT, Root.SMALLINT, Root.FLOAT)
        .build();

    // DIVERGENCE FROM FLINK: Flink uses implicitFromFamily(NUMERIC), which admits BIGINT and
    // DECIMAL. A DOUBLE significand is 53 bits, so BIGINT exceeds it; INTEGER and narrower fit and
    // are kept, as is FLOAT, every value of which is exactly representable. DECIMAL is dropped for
    // the same reason as under FLOAT.
    castTo(Root.DOUBLE)
        .implicitFrom(Root.TINYINT, Root.SMALLINT, Root.INTEGER, Root.FLOAT, Root.DOUBLE)
        .build();

    // -------------------------------------------------------------------------------------------
    // Boolean type
    // -------------------------------------------------------------------------------------------

    castTo(Root.BOOLEAN)
        .implicitFrom(Root.BOOLEAN)
        .build();

    // -------------------------------------------------------------------------------------------
    // Date and time types
    // -------------------------------------------------------------------------------------------

    // DIVERGENCE FROM FLINK: Flink also admits TIMESTAMP, which discards the time-of-day.
    castTo(Root.DATE)
        .implicitFrom(Root.DATE)
        .build();

    // DIVERGENCE FROM FLINK: Flink also admits TIMESTAMP, which discards the date.
    castTo(Root.TIME)
        .implicitFrom(Root.TIME)
        .build();

    // DIVERGENCE FROM FLINK: Flink also admits TIMESTAMP_LTZ, in both directions. The two share a
    // representation but not a reference frame -- one is a local wall-clock reading, the other an
    // instant -- so re-annotating a field shifts every historical value by the local UTC offset
    // while the bytes and the column type stay put. Same class as the frozen decimal scale and the
    // frozen temporal precision: an annotation that selects how stored bytes are read cannot be
    // changed.
    castTo(Root.TIMESTAMP)
        .implicitFrom(Root.TIMESTAMP)
        .build();

    // DIVERGENCE FROM FLINK: as above, for the opposite direction.
    castTo(Root.TIMESTAMP_LTZ)
        .implicitFrom(Root.TIMESTAMP_LTZ)
        .build();
  }

  private FlinkLogicalTypeCasts() {
  }

  /**
   * Whether {@code source} widens to {@code target} without loss of information, by Flink's
   * definition.
   *
   * <p>Unlike Flink's, this relation really is lossless: the five edges where Flink's is not are
   * narrowed at their declarations. See the class javadoc.
   */
  static boolean supportsImplicitCast(Root source, Root target) {
    return IMPLICIT_CASTING_RULES.get(target).contains(source);
  }

  private static CastingRuleBuilder castTo(Root targetType) {
    return new CastingRuleBuilder(targetType);
  }

  /**
   * Mirrors Flink's {@code CastingRuleBuilder}, minus the explicit and injective tables.
   */
  private static final class CastingRuleBuilder {

    private final Root targetType;
    private final Set<Root> implicitSourceTypes = new HashSet<>();

    private CastingRuleBuilder(Root targetType) {
      this.targetType = targetType;
    }

    private CastingRuleBuilder implicitFrom(Root... sourceTypes) {
      implicitSourceTypes.addAll(Arrays.asList(sourceTypes));
      return this;
    }

    private CastingRuleBuilder implicitFromFamily(Family... sourceFamilies) {
      for (Family family : sourceFamilies) {
        implicitSourceTypes.addAll(family.roots);
      }
      return this;
    }

    private void build() {
      IMPLICIT_CASTING_RULES
          .computeIfAbsent(targetType, key -> EnumSet.noneOf(Root.class))
          .addAll(implicitSourceTypes);
    }
  }
}
