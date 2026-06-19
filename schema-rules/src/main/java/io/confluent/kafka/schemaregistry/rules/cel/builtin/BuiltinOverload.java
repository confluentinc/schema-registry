/*
 * Copyright 2023 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import dev.cel.runtime.CelFunctionBinding;
import io.confluent.kafka.schemaregistry.type.Variant;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.InetAddressValidator;

final class BuiltinOverload {

  /**
   * Decimal division uses 38-digit precision with HALF_UP rounding — matches
   * Flink SQL's MC_DIVIDE and PostgreSQL's NUMERIC division. Add/sub/mul use
   * BigDecimal's exact defaults (scale = scale_a + scale_b for mul, max for
   * add/sub); we deliberately do <i>not</i> cap the result scale at 38, so a
   * deeply chained operation on very-high-scale inputs can produce a result
   * with a longer fractional than Flink SQL would store (Flink derives the
   * same scale but caps precision at 38 by rounding the scale down). For real
   * decimal data (currency, percentages, basis points) this divergence does
   * not occur in practice — Flink's cap only kicks in once precision exceeds
   * 38. Users wanting bounded scale can apply {@code decimals.round(x, n)}
   * explicitly.
   */
  private static final MathContext DIV_MC = new MathContext(38, RoundingMode.HALF_UP);

  private BuiltinOverload() {
  }

  static ImmutableList<CelFunctionBinding> create() {
    List<CelFunctionBinding> out = new ArrayList<>();

    // existing validators
    out.add(unaryString("is_email", BuiltinOverload::validateEmail));
    out.add(unaryString("is_hostname", BuiltinOverload::validateHostname));
    out.add(unaryString("is_ipv4", BuiltinOverload::validateIpv4));
    out.add(unaryString("is_ipv6", BuiltinOverload::validateIpv6));
    out.add(unaryString("is_uri", BuiltinOverload::validateUri));
    out.add(unaryString("is_uri_ref", BuiltinOverload::validateUriRef));
    out.add(unaryString("is_uuid", BuiltinOverload::validateUuid));

    addDecimal(out);
    addTimestamp(out);
    addVariant(out);

    return ImmutableList.copyOf(out);
  }

  // ---- existing string validators ----

  private static CelFunctionBinding unaryString(
      String overloadId, Predicate<String> predicate) {
    return CelFunctionBinding.from(
        overloadId,
        String.class,
        (String input) -> !input.isEmpty() && predicate.test(input));
  }

  static boolean validateEmail(String input) {
    return EmailValidator.getInstance(false, true).isValid(input);
  }

  static boolean validateHostname(String input) {
    return DomainValidator.getInstance(true).isValid(input) && !input.contains("_");
  }

  static boolean validateIpv4(String input) {
    return InetAddressValidator.getInstance().isValidInet4Address(input);
  }

  static boolean validateIpv6(String input) {
    return InetAddressValidator.getInstance().isValidInet6Address(input);
  }

  static boolean validateUri(String input) {
    try {
      URI uri = new URI(input);
      return uri.isAbsolute();
    } catch (URISyntaxException e) {
      return false;
    }
  }

  static boolean validateUriRef(String input) {
    try {
      new URI(input);
      return true;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  static boolean validateUuid(String input) {
    try {
      UUID.fromString(input);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  // ---- Decimal ----

  private static void addDecimal(List<CelFunctionBinding> out) {
    // Constructor overloads — only (dyn) and (bytes, int) at the decl level;
    // String is handled by toBigDecimal(Object)'s String arm. See the comment
    // in BuiltinDeclarations.addDecimal for why we omit (string).
    out.add(CelFunctionBinding.from(
        "dyn_to_decimal", Object.class, DecimalUtils::toBigDecimal));
    // Bytes args arrive as CelByteString under CelOptions.DEFAULT
    // (evaluateCanonicalTypesToNativeValues=true converts proto ByteString and
    // bytes-field reads into CelByteString).
    out.add(CelFunctionBinding.from(
        "bytes_int_to_decimal",
        CelByteString.class, Long.class,
        (CelByteString bytes, Long scale) ->
            DecimalUtils.toBigDecimal(bytes.toByteArray(),
                requireIntScale(scale, "decimal(bytes, scale)"))));

    // Comparison
    out.add(decimalsBinary("decimals_eq_decimal_decimal", (a, b) -> a.compareTo(b) == 0));
    out.add(decimalsBinary("decimals_lt_decimal_decimal", (a, b) -> a.compareTo(b) < 0));
    out.add(decimalsBinary("decimals_le_decimal_decimal", (a, b) -> a.compareTo(b) <= 0));
    out.add(decimalsBinary("decimals_gt_decimal_decimal", (a, b) -> a.compareTo(b) > 0));
    out.add(decimalsBinary("decimals_ge_decimal_decimal", (a, b) -> a.compareTo(b) >= 0));

    // Arithmetic
    out.add(decimalsBinary("decimals_add_decimal_decimal", BigDecimal::add));
    out.add(decimalsBinary("decimals_sub_decimal_decimal", BigDecimal::subtract));
    out.add(decimalsBinary("decimals_mul_decimal_decimal", BigDecimal::multiply));
    // Division uses MathContext(38, HALF_UP) — see DIV_MC.
    out.add(decimalsBinary("decimals_div_decimal_decimal", (a, b) -> {
      try {
        return a.divide(b, DIV_MC);
      } catch (ArithmeticException e) {
        if (b.signum() == 0) {
          throw new IllegalArgumentException("decimals.div: division by zero", e);
        }
        throw new IllegalArgumentException(
            e.getMessage() != null ? e.getMessage() : "decimals.div: arithmetic error", e);
      }
    }));
    // Modulo: BigDecimal.remainder — remainder has the sign of the dividend,
    // matching SQL MOD. Throws on a zero divisor.
    out.add(decimalsBinary("decimals_mod_decimal_decimal", (a, b) -> {
      if (b.signum() == 0) {
        throw new IllegalArgumentException("decimals.mod: division by zero");
      }
      return a.remainder(b);
    }));

    // Square root — MathContext(38, HALF_UP), same precision/rounding as div.
    // BigDecimal.sqrt throws ArithmeticException on a negative value; re-emit
    // the canonical "decimals.sqrt: square root of negative number" message so
    // the user-visible cause is clear and stable across refactors.
    out.add(decimalsUnary("decimals_sqrt_decimal", d -> {
      if (d.signum() < 0) {
        throw new IllegalArgumentException("decimals.sqrt: square root of negative number");
      }
      return d.sqrt(DIV_MC);
    }));

    // Selection: decimals.greatest/least return the larger/smaller operand
    // (BigDecimal.max/min — the receiver on a numeric tie).
    out.add(decimalsBinary("decimals_greatest_decimal_decimal", BigDecimal::max));
    out.add(decimalsBinary("decimals_least_decimal_decimal", BigDecimal::min));

    // Unary
    out.add(decimalsUnary("decimals_neg_decimal", BigDecimal::negate));
    out.add(decimalsUnary("decimals_abs_decimal", BigDecimal::abs));
    out.add(CelFunctionBinding.from(
        "decimals_sign_decimal", BigDecimal.class,
        (BigDecimal d) -> (long) d.signum()));
    // string(Decimal) — extension overload on stdlib `string(...)`.
    out.add(CelFunctionBinding.from(
        "decimal_to_string", BigDecimal.class,
        BigDecimal::toPlainString));
    // double(Decimal) — extension overload on stdlib `double(...)`. Narrowing:
    // BigDecimal.doubleValue() returns the closest double (±Infinity if the
    // magnitude is out of range).
    out.add(CelFunctionBinding.from(
        "decimal_to_double", BigDecimal.class,
        BigDecimal::doubleValue));

    // Rounding family — Flink-aligned. Negative scale rounds left of the decimal.
    out.add(decimalsUnary(
        "decimals_round_unary", d -> d.setScale(0, RoundingMode.HALF_UP)));
    out.add(CelFunctionBinding.from(
        "decimals_round_scale", BigDecimal.class, Long.class,
        (BigDecimal d, Long scale) ->
            d.setScale(requireIntScale(scale, "decimals.round"), RoundingMode.HALF_UP)));
    // Flink's TRUNCATE early-returns when the target scale is at-or-finer than
    // the current scale — it's a no-op there, so the result keeps the input's
    // representation. Without this guard, setScale(n>=cur, DOWN) would zero-pad
    // and string(trunc(x, n>=cur)) would diverge from Flink.
    out.add(decimalsUnary(
        "decimals_trunc_unary",
        d -> d.scale() <= 0 ? d : d.setScale(0, RoundingMode.DOWN)));
    out.add(CelFunctionBinding.from(
        "decimals_trunc_scale", BigDecimal.class, Long.class,
        (BigDecimal d, Long scale) -> {
          int intScale = requireIntScale(scale, "decimals.trunc");
          return intScale >= d.scale() ? d : d.setScale(intScale, RoundingMode.DOWN);
        }));
    out.add(decimalsUnary(
        "decimals_floor_decimal", d -> d.setScale(0, RoundingMode.FLOOR)));
    out.add(decimalsUnary(
        "decimals_ceil_decimal", d -> d.setScale(0, RoundingMode.CEILING)));
  }

  private static <R> CelFunctionBinding decimalsBinary(
      String overloadId, BiFunction<BigDecimal, BigDecimal, R> fn) {
    return CelFunctionBinding.from(
        overloadId, BigDecimal.class, BigDecimal.class, fn::apply);
  }

  private static CelFunctionBinding decimalsUnary(
      String overloadId, Function<BigDecimal, BigDecimal> fn) {
    return CelFunctionBinding.from(overloadId, BigDecimal.class, fn::apply);
  }

  // ---- Timestamp ----

  private static void addTimestamp(List<CelFunctionBinding> out) {
    out.add(CelFunctionBinding.from(
        "timestamp_of_dyn", Object.class, TimestampUtils::toTimestamp));
    out.add(CelFunctionBinding.from(
        "timestamp_of_int_string", Long.class, String.class, TimestampUtils::fromEpoch));
  }

  // ---- Variant ----
  //
  // Null model (per Spark Variant semantics):
  //   - CEL null      = "path missed" / "no value at this location" (analog of
  //                     Spark's SQL NULL from variant_get).
  //   - variant-null  = "the value at this location is a Variant whose top
  //                     type is NULL" (analog of Spark's Variant(NULL)).
  //
  // Detection idioms:
  //   - `result == null`            -> missing path
  //   - `variants.isNull(result)`   -> explicit JSON null (Spark is_variant_null)
  //   - `result == null || variants.isNull(result)` -> absent in either sense
  //
  // All Variant-receiving bindings accept Object first arg so navigation chains
  // compose past a CEL-null intermediate result without runtime "no matching
  // overload" errors. Null propagates: f(null, ...) -> null.

  private static void addVariant(List<CelFunctionBinding> out) {
    // variant(null) -> CEL null. variant(string) -> rejected (use parseJson).
    out.add(CelFunctionBinding.from(
        "dyn_to_variant", Object.class, BuiltinOverload::variantConstructor));
    out.add(CelFunctionBinding.from(
        "bytes_bytes_to_variant", CelByteString.class, CelByteString.class,
        (CelByteString value, CelByteString metadata) ->
            VariantUtils.fromBytes(value.toByteArray(), metadata.toByteArray())));

    // variants.parseJson strict / variants.tryParseJson soft (Spark
    // parse_json / try_parse_json analogs).
    out.add(CelFunctionBinding.from(
        "variants_parsejson_string", String.class, VariantUtils::fromJson));
    out.add(CelFunctionBinding.from(
        "variants_tryparsejson_string", String.class,
        BuiltinOverload::variantTryParseJson));

    // variants.type propagates CEL null. variants.isNull is strict
    // Spark-equivalent: true iff input is a Variant with type=NULL (false for
    // non-Variant inputs — matches Spark is_variant_null on SQL NULL etc.).
    out.add(CelFunctionBinding.from(
        "variants_type_variant", Variant.class,
        (Variant v) -> variantTypeName(v.getType())));
    out.add(CelFunctionBinding.from(
        "variants_isnull_dyn", Object.class,
        (Object o) -> (o instanceof Variant)
            && ((Variant) o).getType() == Variant.Type.NULL));

    // Navigation. Each function returns sub-Variant or CEL null on miss.
    // Accessor Java null -> CEL null; explicit JSON null at path -> Variant
    // with type=NULL (detect via variants.isNull). Wrong-type receivers also
    // produce a miss. Malformed JSONPath still throws. For typed extraction,
    // compose with variants.as / variants.tryAs.
    out.add(CelFunctionBinding.from(
        "variants_path_dyn_string", Object.class, String.class,
        (Object o, String path) -> {
          Variant v = requireVariantOrNull(o, "variants.path");
          if (v == null) {
            return NullValue.NULL_VALUE;
          }
          Variant result = VariantPath.walk(v, path);
          return result == null ? NullValue.NULL_VALUE : result;
        }));
    out.add(CelFunctionBinding.from(
        "variants_field_dyn_string", Object.class, String.class,
        (Object o, String key) -> {
          Variant v = requireVariantOrNull(o, "variants.field");
          if (v == null || v.getType() != Variant.Type.OBJECT) {
            return NullValue.NULL_VALUE;
          }
          Variant result = v.getFieldByKey(key);
          return result == null ? NullValue.NULL_VALUE : result;
        }));
    out.add(CelFunctionBinding.from(
        "variants_index_dyn_int", Object.class, Long.class,
        (Object o, Long idx) -> {
          Variant v = requireVariantOrNull(o, "variants.index");
          if (v == null || v.getType() != Variant.Type.ARRAY
              || idx < 0 || idx > Integer.MAX_VALUE) {
            return NullValue.NULL_VALUE;
          }
          Variant result = v.getElementAtIndex(idx.intValue());
          return result == null ? NullValue.NULL_VALUE : result;
        }));

    // Standalone parameterized typed extraction. variants.as throws on type
    // mismatch (Spark variant_get root-path analog). variants.tryAs returns
    // CEL null on type mismatch (Spark try_variant_get root-path analog).
    // Both propagate CEL-null input.
    out.add(CelFunctionBinding.from(
        "variants_as_dyn_string", Object.class, String.class,
        (Object o, String typeStr) -> {
          Variant v = requireVariantOrNull(o, "variants.as");
          return v == null ? NullValue.NULL_VALUE
              : variantAs(v, typeStr, /*nullOnError=*/ false);
        }));
    out.add(CelFunctionBinding.from(
        "variants_tryas_dyn_string", Object.class, String.class,
        (Object o, String typeStr) -> {
          Variant v = requireVariantOrNull(o, "variants.tryAs");
          return v == null ? NullValue.NULL_VALUE
              : variantAs(v, typeStr, /*nullOnError=*/ true);
        }));

    // variants.toJson(Variant) — serialize a Variant to its JSON string form.
    out.add(CelFunctionBinding.from(
        "variants_tojson_variant", Variant.class, VariantUtils::toJsonString));
  }

  /** True iff {@code o} represents "null" in the CEL sense — Java null or
   *  cel-java's {@link NullValue#NULL_VALUE} sentinel. */
  private static boolean isCelNull(Object o) {
    return o == null || o instanceof NullValue;
  }

  /** Narrow a CEL int (Java {@code long}) into a Java {@code int} for use as
   *  a BigDecimal scale, throwing a clear IAE on out-of-range values. CEL int
   *  is i64; BigDecimal scale is i32. Using {@code Long.intValue()} directly
   *  would silently take the lower 32 bits (e.g., {@code 2^32 → 0}), yielding
   *  a wildly wrong Decimal. Mirrors the range-check pattern in
   *  {@code variants.index}. */
  private static int requireIntScale(long scale, String functionName) {
    try {
      return Math.toIntExact(scale);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          functionName + ": scale out of int range: " + scale, e);
    }
  }

  /** Common short-circuit for variants.* bindings whose first arg is declared
   *  as DYN. Returns null if the input is CEL null (signaling the binding
   *  should produce {@link NullValue#NULL_VALUE}). Returns the cast Variant
   *  otherwise. Throws {@link IllegalArgumentException} with a clear
   *  "expected Variant, got X" message if the input is neither CEL null nor
   *  a Variant — the DYN signature lets such inputs reach the binding (e.g.,
   *  a rule that accidentally passes a string instead of a Variant), and a
   *  helpful error beats a raw {@link ClassCastException}. */
  private static Variant requireVariantOrNull(Object o, String functionName) {
    if (isCelNull(o)) {
      return null;
    }
    if (!(o instanceof Variant)) {
      throw new IllegalArgumentException(
          functionName + ": expected Variant, got " + o.getClass().getName());
    }
    return (Variant) o;
  }

  /** {@code variant(dyn)} binding body. Propagates CEL null; rejects strings
   *  with a redirect to {@code variants.parseJson}; otherwise delegates to
   *  {@link VariantUtils#toVariant(Object)} for proto-Variant decoding and
   *  primitive wrapping. */
  private static Object variantConstructor(Object o) {
    if (isCelNull(o)) {
      return NullValue.NULL_VALUE;
    }
    if (o instanceof String) {
      throw new IllegalArgumentException(
          "variant(string) is not supported; use variants.parseJson(s) for"
              + " strict JSON parsing or variants.tryParseJson(s) for soft mode"
              + " that returns null on parse failure");
    }
    return VariantUtils.toVariant(o);
  }

  /** {@code variants.tryParseJson(s)} binding body — returns CEL null on
   *  parse failure (Spark try_parse_json analog). */
  private static Object variantTryParseJson(String s) {
    try {
      return VariantUtils.fromJson(s);
    } catch (IllegalArgumentException e) {
      return NullValue.NULL_VALUE;
    }
  }

  /**
   * Runtime dispatch for {@code variants.as(v, typeStr[, nullOnError])} and the
   * 3-arg / 4-arg navigation+extraction overloads ({@code variants.path/field/index}).
   *
   * <p>Accepted type strings match the {@code variants.type(v)} output for
   * extractable scalar types: {@code "string"}, {@code "int"}, {@code "double"},
   * {@code "boolean"}, {@code "decimal"}, {@code "timestamp"}, {@code "bytes"}.
   * The container/sentinel/v1-out-of-scope labels — {@code "object"}, {@code
   * "array"}, {@code "null"}, {@code "date"}, {@code "time"}, {@code "uuid"} —
   * are rejected (no concrete typed extractor exists for them).
   *
   * <p>When {@code nullOnError} is true, all rejections (recognized-but-mismatched
   * types, unknown type strings) return {@link
   * dev.cel.common.values.NullValue#NULL_VALUE}. When false, mismatches throw
   * {@link IllegalArgumentException}. Path/navigation misses are handled by the
   * caller before reaching this helper.
   */
  private static Object variantAs(Variant v, String typeStr, boolean nullOnError) {
    Variant.Type t = v.getType();
    switch (typeStr) {
      case "string":
        if (t == Variant.Type.STRING) {
          return v.getString();
        }
        break;
      case "int":
        if (t == Variant.Type.BYTE || t == Variant.Type.SHORT
            || t == Variant.Type.INT || t == Variant.Type.LONG) {
          return v.getLong();
        }
        break;
      case "double":
        if (t == Variant.Type.FLOAT) {
          // Widen FLOAT to double so users see a uniform double extraction.
          return (double) v.getFloat();
        }
        if (t == Variant.Type.DOUBLE) {
          return v.getDouble();
        }
        break;
      case "boolean":
        if (t == Variant.Type.BOOLEAN) {
          return v.getBoolean();
        }
        break;
      case "decimal":
        if (t == Variant.Type.DECIMAL4 || t == Variant.Type.DECIMAL8
            || t == Variant.Type.DECIMAL16) {
          return v.getDecimal();
        }
        break;
      case "timestamp":
        if (t == Variant.Type.TIMESTAMP_TZ || t == Variant.Type.TIMESTAMP_NTZ
            || t == Variant.Type.TIMESTAMP_NANOS_TZ
            || t == Variant.Type.TIMESTAMP_NANOS_NTZ) {
          return variantGetTimestamp(v);
        }
        break;
      case "bytes":
        if (t == Variant.Type.BINARY) {
          return CelByteString.of(variantGetBytes(v));
        }
        break;
      case "object":
      case "array":
      case "null":
      case "date":
      case "time":
      case "uuid":
        throw new IllegalArgumentException(
            "variants.as: type '" + typeStr + "' is not supported for extraction"
                + " (use variants.type/variants.path/variants.field/variants.index instead)");
      default:
        if (nullOnError) {
          return NullValue.NULL_VALUE;
        }
        throw new IllegalArgumentException(
            "variants.as: unknown type '" + typeStr + "'"
                + " (expected one of: string, int, double, boolean, decimal, timestamp, bytes)");
    }
    // Recognized typeStr but actual variant type doesn't match.
    if (nullOnError) {
      return NullValue.NULL_VALUE;
    }
    throw new IllegalArgumentException(
        "variants.as: variant is not " + typeStr + "-typed (type=" + t + ")");
  }

  private static Timestamp variantGetTimestamp(Variant v) {
    switch (v.getType()) {
      case TIMESTAMP_TZ:
      case TIMESTAMP_NTZ:
        return TimestampUtils.fromEpochMicros(v.getLong());
      case TIMESTAMP_NANOS_TZ:
      case TIMESTAMP_NANOS_NTZ:
        return TimestampUtils.fromEpochNanos(v.getLong());
      default:
        // Unreachable: callers (variantAs) verify the type before invoking.
        throw new IllegalStateException(
            "variantGetTimestamp called on non-timestamp variant (type="
                + v.getType() + ")");
    }
  }

  private static byte[] variantGetBytes(Variant v) {
    // Variant.getBinary() throws unexpectedType on non-BINARY variants; we
    // don't pre-check the type or null-check the return value because
    // neither path is reachable.
    java.nio.ByteBuffer buf = v.getBinary();
    java.nio.ByteBuffer dup = buf.duplicate();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return out;
  }

  private static String variantTypeName(Variant.Type t) {
    switch (t) {
      case OBJECT: return "object";
      case ARRAY:  return "array";
      case NULL:   return "null";
      case BOOLEAN: return "boolean";
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return "int";
      case FLOAT:
      case DOUBLE:
        return "double";
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return "decimal";
      case DATE: return "date";
      case TIME: return "time";
      case TIMESTAMP_TZ:
      case TIMESTAMP_NTZ:
      case TIMESTAMP_NANOS_TZ:
      case TIMESTAMP_NANOS_NTZ:
        return "timestamp";
      case STRING: return "string";
      case BINARY: return "bytes";
      case UUID:   return "uuid";
      default:
        throw new IllegalStateException("Unknown Variant.Type: " + t);
    }
  }
}
