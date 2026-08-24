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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * The runtime Java representation of the CEL {@code confluent.type.Decimal} type
 * ({@link CelTypeLabels#DECIMAL}): a {@link BigDecimal} whose {@code equals} is numeric.
 *
 * <p><b>Must not extend {@link Number}.</b> cel-java's {@code RuntimeEquality.objectEquals}
 * short-circuits on {@code instanceof Number} into {@code ComparisonFunctions.numericEquals},
 * which knows only Double / Long / UnsignedLong and answers {@code false} for any BigDecimal pair.
 */
public final class CelDecimal implements Comparable<CelDecimal> {

  private final BigDecimal value;

  private CelDecimal(BigDecimal value) {
    this.value = value;
  }

  public static CelDecimal of(BigDecimal value) {
    return new CelDecimal(Objects.requireNonNull(value, "value"));
  }

  /**
   * From unscaled two's-complement big-endian bytes plus a scale — the encoding an Avro
   * {@code decimal} logical type and a {@code confluent.type.Decimal} message both use.
   */
  public static CelDecimal ofUnscaled(byte[] unscaled, int scale) {
    return of(DecimalUtils.toBigDecimal(unscaled, scale));
  }

  /** The wrapped {@link BigDecimal} — the logical-type Java rep. */
  public BigDecimal value() {
    return value;
  }

  /** Numeric, scale-insensitive — the same answer {@code decimals.eq} gives. */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CelDecimal)) {
      return false;
    }
    return value.compareTo(((CelDecimal) o).value) == 0;
  }

  @Override
  public int hashCode() {
    // stripTrailingZeros collapses the scale differences equals ignores, except for zero:
    // BigDecimal zeros of differing scale don't all reduce to one representation, so pin them.
    return value.signum() == 0 ? 0 : value.stripTrailingZeros().hashCode();
  }

  @Override
  public int compareTo(CelDecimal other) {
    return value.compareTo(other.value);
  }

  @Override
  public String toString() {
    return value.toPlainString();
  }
}
