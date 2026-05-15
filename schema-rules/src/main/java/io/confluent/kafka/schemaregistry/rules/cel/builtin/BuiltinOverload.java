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
   * add/sub) — no MathContext needed since those operations are always
   * representable.
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
            DecimalUtils.toBigDecimal(bytes.toByteArray(), scale.intValue())));

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

    // Rounding family — Flink-aligned. Negative scale rounds left of the decimal.
    out.add(decimalsUnary(
        "decimals_round_unary", d -> d.setScale(0, RoundingMode.HALF_UP)));
    out.add(CelFunctionBinding.from(
        "decimals_round_scale", BigDecimal.class, Long.class,
        (BigDecimal d, Long scale) -> d.setScale(scale.intValue(), RoundingMode.HALF_UP)));
    // Flink's TRUNCATE early-returns when the target scale is at-or-finer than
    // the current scale — it's a no-op there, so the result keeps the input's
    // representation. Without this guard, setScale(n>=cur, DOWN) would zero-pad
    // and string(trunc(x, n>=cur)) would diverge from Flink.
    out.add(decimalsUnary(
        "decimals_trunc_unary",
        d -> d.scale() <= 0 ? d : d.setScale(0, RoundingMode.DOWN)));
    out.add(CelFunctionBinding.from(
        "decimals_trunc_scale", BigDecimal.class, Long.class,
        (BigDecimal d, Long scale) ->
            scale.intValue() >= d.scale() ? d : d.setScale(scale.intValue(), RoundingMode.DOWN)));
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
    // Constructors. variant(null) propagates to CEL null (Spark parse_json(NULL)
    // returns SQL NULL).
    out.add(CelFunctionBinding.from(
        "dyn_to_variant", Object.class,
        (Object o) -> isCelNull(o) ? NullValue.NULL_VALUE : VariantUtils.toVariant(o)));
    out.add(CelFunctionBinding.from(
        "string_bool_to_variant", String.class, Boolean.class,
        (String s, Boolean nullOnError) -> {
          try {
            return VariantUtils.toVariant(s);
          } catch (RuntimeException e) {
            if (Boolean.TRUE.equals(nullOnError)) {
              return NullValue.NULL_VALUE;
            }
            throw e;
          }
        }));
    out.add(CelFunctionBinding.from(
        "bytes_bytes_to_variant",
        CelByteString.class,
        CelByteString.class,
        (CelByteString value,
         CelByteString metadata) ->
            VariantUtils.fromBytes(value.toByteArray(), metadata.toByteArray())));

    // Type inspection. variants.type propagates CEL null. variants.isNull is
    // strict Spark-equivalent: true iff the input is a Variant whose top type
    // is NULL (returns false for CEL null and for non-Variant inputs).
    out.add(CelFunctionBinding.from(
        "variants_type_dyn", Object.class,
        (Object o) -> isCelNull(o)
            ? NullValue.NULL_VALUE
            : variantTypeName(((Variant) o).getType())));
    out.add(CelFunctionBinding.from(
        "variants_isnull_dyn", Object.class,
        (Object o) -> (o instanceof Variant)
            && ((Variant) o).getType() == Variant.Type.NULL));

    // Navigation. Three overload families per function (path/field/elem):
    //   2-arg: sub-Variant or CEL null on miss
    //   3-arg: sub-Variant + typed extraction (strict, throws on cast failure)
    //   4-arg: 3-arg + nullOnError flag (true = return null on cast failure)
    //
    // Per binding, two distinct null outcomes:
    //   - Accessor returns Java null (missing field, OOB index, walk stopped
    //     early): we return CEL null.
    //   - Accessor returns a Variant whose top type happens to be NULL (the
    //     value at that location is explicit JSON null): we return that Variant
    //     unchanged — caller can detect via variants.isNull(...).
    // Wrong-type receivers also produce a miss (CEL null) rather than a throw,
    // preserving the existing "navigate returns null on any miss" contract.
    // Malformed JSONPath in variants.path still throws (constraint-registration
    // failure, not a silent runtime no-op).
    out.add(CelFunctionBinding.from(
        "variants_path_dyn_string", Object.class, String.class,
        (Object o, String path) -> {
          if (isCelNull(o)) {
            return NullValue.NULL_VALUE;
          }
          Variant result = VariantPath.walk((Variant) o, path);
          return result == null ? NullValue.NULL_VALUE : result;
        }));
    out.add(CelFunctionBinding.from(
        "variants_path_dyn_string_string",
        ImmutableList.of(Object.class, String.class, String.class),
        args -> navigateThenAs(walkPath(args[0], (String) args[1]),
            (String) args[2], /*nullOnError=*/ false)));
    out.add(CelFunctionBinding.from(
        "variants_path_dyn_string_string_bool",
        ImmutableList.of(Object.class, String.class, String.class, Boolean.class),
        args -> navigateThenAs(walkPath(args[0], (String) args[1]),
            (String) args[2], Boolean.TRUE.equals(args[3]))));
    out.add(CelFunctionBinding.from(
        "variants_field_dyn_string", Object.class, String.class,
        (Object o, String key) -> {
          if (isCelNull(o)) {
            return NullValue.NULL_VALUE;
          }
          Variant v = (Variant) o;
          if (v.getType() != Variant.Type.OBJECT) {
            return NullValue.NULL_VALUE;
          }
          Variant result = v.getFieldByKey(key);
          return result == null ? NullValue.NULL_VALUE : result;
        }));
    out.add(CelFunctionBinding.from(
        "variants_field_dyn_string_string",
        ImmutableList.of(Object.class, String.class, String.class),
        args -> navigateThenAs(walkField(args[0], (String) args[1]),
            (String) args[2], /*nullOnError=*/ false)));
    out.add(CelFunctionBinding.from(
        "variants_field_dyn_string_string_bool",
        ImmutableList.of(Object.class, String.class, String.class, Boolean.class),
        args -> navigateThenAs(walkField(args[0], (String) args[1]),
            (String) args[2], Boolean.TRUE.equals(args[3]))));
    out.add(CelFunctionBinding.from(
        "variants_elem_dyn_int", Object.class, Long.class,
        (Object o, Long idx) -> {
          if (isCelNull(o)) {
            return NullValue.NULL_VALUE;
          }
          Variant v = (Variant) o;
          if (v.getType() != Variant.Type.ARRAY
              || idx < Integer.MIN_VALUE || idx > Integer.MAX_VALUE) {
            return NullValue.NULL_VALUE;
          }
          Variant result = v.getElementAtIndex(idx.intValue());
          return result == null ? NullValue.NULL_VALUE : result;
        }));
    out.add(CelFunctionBinding.from(
        "variants_elem_dyn_int_string",
        ImmutableList.of(Object.class, Long.class, String.class),
        args -> navigateThenAs(walkElem(args[0], (Long) args[1]),
            (String) args[2], /*nullOnError=*/ false)));
    out.add(CelFunctionBinding.from(
        "variants_elem_dyn_int_string_bool",
        ImmutableList.of(Object.class, Long.class, String.class, Boolean.class),
        args -> navigateThenAs(walkElem(args[0], (Long) args[1]),
            (String) args[2], Boolean.TRUE.equals(args[3]))));

    // Standalone parameterized typed extraction.
    //   variants.as(v, t)              — strict, throws on type mismatch
    //   variants.as(v, t, nullOnError) — soft if true (Spark try_variant_get)
    // Both propagate CEL-null input (matches Spark variant_get(NULL, ...) -> NULL).
    out.add(CelFunctionBinding.from(
        "variants_as_dyn_string", Object.class, String.class,
        (Object o, String typeStr) -> isCelNull(o)
            ? NullValue.NULL_VALUE
            : variantAs((Variant) o, typeStr, /*nullOnError=*/ false)));
    out.add(CelFunctionBinding.from(
        "variants_as_dyn_string_bool",
        ImmutableList.of(Object.class, String.class, Boolean.class),
        args -> isCelNull(args[0])
            ? NullValue.NULL_VALUE
            : variantAs((Variant) args[0], (String) args[1],
                Boolean.TRUE.equals(args[2]))));

    // variants.toJson(Variant) — serialize a Variant to its JSON string form.
    // Replaces the prior string(Variant) extension overload; namespaced for
    // discoverability. For nullable navigation results, guard via
    // `... == null` upstream (this binding requires a non-null Variant).
    out.add(CelFunctionBinding.from(
        "variants_tojson_variant", Variant.class, VariantUtils::toJsonString));
  }

  /** Walk a JSONPath from {@code o} (which may be CEL null). Returns the
   *  navigated Variant, or Java null on miss / CEL-null input. */
  private static Variant walkPath(Object o, String path) {
    if (isCelNull(o)) {
      return null;
    }
    return VariantPath.walk((Variant) o, path);
  }

  /** Look up a field on {@code o}. Returns the value Variant, or Java null on
   *  miss / wrong-type-receiver / CEL-null input. */
  private static Variant walkField(Object o, String key) {
    if (isCelNull(o)) {
      return null;
    }
    Variant v = (Variant) o;
    if (v.getType() != Variant.Type.OBJECT) {
      return null;
    }
    return v.getFieldByKey(key);
  }

  /** Look up an array element. Returns the element Variant, or Java null on
   *  miss / wrong-type-receiver / OOB index / int-overflow / CEL-null input. */
  private static Variant walkElem(Object o, Long idx) {
    if (isCelNull(o)) {
      return null;
    }
    Variant v = (Variant) o;
    if (v.getType() != Variant.Type.ARRAY
        || idx < 0 || idx > Integer.MAX_VALUE) {
      return null;
    }
    return v.getElementAtIndex(idx.intValue());
  }

  /** Shared post-navigation step for the 3/4-arg variants.path/field/elem
   *  overloads: if the navigation missed, return CEL null; otherwise extract
   *  the requested type via {@link #variantAs}. */
  private static Object navigateThenAs(Variant navigated, String typeStr,
                                       boolean nullOnError) {
    if (navigated == null) {
      return NullValue.NULL_VALUE;
    }
    return variantAs(navigated, typeStr, nullOnError);
  }

  /** True iff {@code o} represents "null" in the CEL sense — Java null or
   *  cel-java's {@link NullValue#NULL_VALUE} sentinel. */
  private static boolean isCelNull(Object o) {
    return o == null || o instanceof NullValue;
  }

  /**
   * Runtime dispatch for {@code variants.as(v, typeStr[, nullOnError])} and the
   * 3-arg / 4-arg navigation+extraction overloads ({@code variants.path/field/elem}).
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
                + " (use variants.type/variants.path/variants.field/variants.elem instead)");
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
