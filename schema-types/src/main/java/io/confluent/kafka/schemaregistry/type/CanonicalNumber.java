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

package io.confluent.kafka.schemaregistry.type;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * The canonical decimal rendering of a binary floating-point value, shared by all seven
 * Schema Registry clients so that they produce byte-identical JSON.
 *
 * <p>Digit selection and layout are separate stages and the dependency runs one way only:
 * layout is a pure function of the selected triple and performs no floating-point arithmetic.
 * That is what stops a layout obligation - such as the trailing {@code .0} on a whole number -
 * from influencing which digits get chosen.
 *
 * <p><b>Digit selection.</b> A decimal is {@code s} &times; 10<sup>i</sup> for integers
 * {@code s > 0}, {@code i}, with {@code s} not a multiple of ten; its length is the unique
 * {@code n} with 10<sup>n-1</sup> &le; {@code s} &lt; 10<sup>n</sup>. For a finite non-zero
 * magnitude, let {@code R} be the decimals that read back as it, {@code p} the minimal length
 * over {@code R}, and {@code T} the decimals in {@code R} of length {@code p}; the selected
 * decimal is the one in {@code T} nearest the exact value, and on an exact tie the one with the
 * even significand. In short: shortest, then nearest, then ties-to-even.
 *
 * <p>This is JDK 21 {@code Double.toString} semantics with one clause removed. The JDK widens
 * the candidate set to lengths 1 and 2 when {@code p} is 1, which makes it emit two digits where
 * one suffices on 34 subnormal bit patterns. Those are the only inputs where this class and
 * {@code Double.toString} disagree, and both renderings read back identically.
 */
final class CanonicalNumber {

  /** Digits needed to distinguish any two values of each type. */
  private static final int MAX_DOUBLE_DIGITS = 17;
  private static final int MAX_FLOAT_DIGITS = 9;

  private CanonicalNumber() {
  }

  /** A canonical decimal: significand digits with no trailing zero, and its leading exponent. */
  static final class Decimal {
    final boolean negative;
    final String digits;
    final int exponent;

    Decimal(boolean negative, String digits, int exponent) {
      this.negative = negative;
      this.digits = digits;
      this.exponent = exponent;
    }
  }

  /**
   * Digit selection for a double.
   *
   * <p>Neither of Java's obvious tools can stand in here. {@code Double.toString} emits more
   * digits than needed before JDK 19, so its output depends on the JDK the caller runs.
   * {@code String.format("%.*e")} rounds halves up rather than to even, and worse, pads with
   * zeros beyond the shortest representation instead of emitting true digits. {@code BigDecimal}
   * is exact, which makes it the cheapest correct route in this language.
   */
  static Decimal digitsOf(double value) {
    BigDecimal exact = new BigDecimal(Math.abs(value));
    long wanted = Double.doubleToLongBits(Math.abs(value));
    for (int length = 1; length <= MAX_DOUBLE_DIGITS; length++) {
      BigDecimal candidate = exact.round(new MathContext(length, RoundingMode.HALF_EVEN));
      if (Double.doubleToLongBits(candidate.doubleValue()) == wanted) {
        return canonicalise(value < 0, candidate);
      }
    }
    // Unreachable: 17 digits distinguish every double.
    return canonicalise(value < 0, exact.round(
        new MathContext(MAX_DOUBLE_DIGITS, RoundingMode.HALF_EVEN)));
  }

  /** Digit selection for a float, rounding against float precision rather than double. */
  static Decimal digitsOf(float value) {
    BigDecimal exact = new BigDecimal((double) Math.abs(value));
    int wanted = Float.floatToIntBits(Math.abs(value));
    for (int length = 1; length <= MAX_FLOAT_DIGITS; length++) {
      BigDecimal candidate = exact.round(new MathContext(length, RoundingMode.HALF_EVEN));
      if (Float.floatToIntBits(candidate.floatValue()) == wanted) {
        return canonicalise(value < 0, candidate);
      }
    }
    // Unreachable: 9 digits distinguish every float.
    return canonicalise(value < 0, exact.round(
        new MathContext(MAX_FLOAT_DIGITS, RoundingMode.HALF_EVEN)));
  }

  private static Decimal canonicalise(boolean negative, BigDecimal candidate) {
    BigDecimal trimmed = candidate.stripTrailingZeros();
    return new Decimal(negative, trimmed.unscaledValue().abs().toString(),
        trimmed.precision() - trimmed.scale() - 1);
  }

  /**
   * Layout: plain when the leading exponent is in [-3, 7), scientific otherwise, always with a
   * digit either side of the point, an uppercase {@code E}, and no sign or zero-padding on a
   * positive exponent. These are the JDK's own rules, unchanged.
   *
   * <p>Pure string work over the triple. No floating-point arithmetic occurs here, by design.
   */
  static String layout(Decimal decimal) {
    String digits = decimal.digits;
    int exponent = decimal.exponent;
    StringBuilder out = new StringBuilder();
    if (decimal.negative) {
      out.append('-');
    }
    if (exponent < -3 || exponent > 6) {
      out.append(digits.charAt(0)).append('.')
          .append(digits.length() > 1 ? digits.substring(1) : "0")
          .append('E').append(exponent);
    } else if (exponent < 0) {
      out.append("0.");
      for (int i = -exponent - 1; i > 0; i--) {
        out.append('0');
      }
      out.append(digits);
    } else if (digits.length() > exponent + 1) {
      out.append(digits, 0, exponent + 1).append('.')
          .append(digits, exponent + 1, digits.length());
    } else {
      out.append(digits);
      for (int i = digits.length(); i <= exponent; i++) {
        out.append('0');
      }
      out.append(".0");
    }
    return out.toString();
  }

  /**
   * The complete rendering, including the values that never enter digit selection: zero, which
   * has no canonical significand, and the non-finite values, which render as the barewords
   * {@code NaN}/{@code Infinity}/{@code -Infinity} rather than Spark's quoted spelling.
   */
  static String render(double value) {
    if (Double.isNaN(value)) {
      return "NaN";
    }
    if (Double.isInfinite(value)) {
      return value > 0 ? "Infinity" : "-Infinity";
    }
    if (value == 0.0) {
      return isNegativeZero(value) ? "-0.0" : "0.0";
    }
    return layout(digitsOf(value));
  }

  /**
   * As {@link #render(double)}, at float precision.
   */
  static String render(float value) {
    if (Float.isNaN(value)) {
      return "NaN";
    }
    if (Float.isInfinite(value)) {
      return value > 0 ? "Infinity" : "-Infinity";
    }
    if (value == 0.0f) {
      return isNegativeZero(value) ? "-0.0" : "0.0";
    }
    return layout(digitsOf(value));
  }

  /**
   * Negative zero, by bit inspection. A float widens to a double exactly, so one helper serves
   * both, and {@code value == 0.0} cannot distinguish the two signs on its own.
   */
  private static boolean isNegativeZero(double value) {
    return value == 0.0 && Double.doubleToLongBits(value) != 0L;
  }
}
