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

  /**
   * Pre-built Variant whose top-level type is NULL. Returned by navigation
   * functions ({@code variants.at}, {@code variants.field}, {@code variants.elem})
   * on miss. Rules detect it via {@code variants.type(v) == "null"}.
   *
   * <p>The metadata is a minimal header (version=1, 0 dictionary entries); the
   * value is a single primitive-header byte for the NULL type.
   */
  private static final Variant NULL_VARIANT = new Variant(
      new byte[] {0},
      new byte[] {0x01, 0x00, 0x00});

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

  protected static boolean validateEmail(String input) {
    return EmailValidator.getInstance(false, true).isValid(input);
  }

  protected static boolean validateHostname(String input) {
    return DomainValidator.getInstance(true).isValid(input) && !input.contains("_");
  }

  protected static boolean validateIpv4(String input) {
    return InetAddressValidator.getInstance().isValidInet4Address(input);
  }

  protected static boolean validateIpv6(String input) {
    return InetAddressValidator.getInstance().isValidInet6Address(input);
  }

  protected static boolean validateUri(String input) {
    try {
      URI uri = new URI(input);
      return uri.isAbsolute();
    } catch (URISyntaxException e) {
      return false;
    }
  }

  protected static boolean validateUriRef(String input) {
    try {
      new URI(input);
      return true;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  protected static boolean validateUuid(String input) {
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
    out.add(decimalsBoolBinary("decimals_eq_decimal_decimal", (a, b) -> a.compareTo(b) == 0));
    out.add(decimalsBoolBinary("decimals_lt_decimal_decimal", (a, b) -> a.compareTo(b) < 0));
    out.add(decimalsBoolBinary("decimals_le_decimal_decimal", (a, b) -> a.compareTo(b) <= 0));
    out.add(decimalsBoolBinary("decimals_gt_decimal_decimal", (a, b) -> a.compareTo(b) > 0));
    out.add(decimalsBoolBinary("decimals_ge_decimal_decimal", (a, b) -> a.compareTo(b) >= 0));

    // Arithmetic
    out.add(decimalsBinary("decimals_add_decimal_decimal", BigDecimal::add));
    out.add(decimalsBinary("decimals_sub_decimal_decimal", BigDecimal::subtract));
    out.add(decimalsBinary("decimals_mul_decimal_decimal", BigDecimal::multiply));
    // Division uses MathContext(38, HALF_UP) — see DIV_MC.
    out.add(decimalsBinary("decimals_div_decimal_decimal", (a, b) -> {
      try {
        return a.divide(b, DIV_MC);
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException("decimals.div: division by zero", e);
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
        (BigDecimal d) -> d.toPlainString()));

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

  private static CelFunctionBinding decimalsBinary(
      String overloadId, BiFunction<BigDecimal, BigDecimal, BigDecimal> fn) {
    return CelFunctionBinding.from(
        overloadId, BigDecimal.class, BigDecimal.class, fn::apply);
  }

  private static CelFunctionBinding decimalsBoolBinary(
      String overloadId, BiFunction<BigDecimal, BigDecimal, Boolean> fn) {
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
        "timestamp_of_int_string", Long.class, String.class,
        (Long value, String unit) -> TimestampUtils.fromEpoch(value, unit)));
  }

  // ---- Variant ----

  private static void addVariant(List<CelFunctionBinding> out) {
    // Constructors
    out.add(CelFunctionBinding.from(
        "dyn_to_variant", Object.class, VariantUtils::toVariant));
    out.add(CelFunctionBinding.from(
        "bytes_bytes_to_variant",
        CelByteString.class,
        CelByteString.class,
        (CelByteString value,
         CelByteString metadata) ->
            VariantUtils.fromBytes(value.toByteArray(), metadata.toByteArray())));

    // Type inspection
    out.add(CelFunctionBinding.from(
        "variants_type_variant", Variant.class,
        (Variant v) -> variantTypeName(v.getType())));

    // Navigation. Missing field/index → variant-null sentinel. Wrong-type
    // receiver (e.g., variants.field on an INT) also returns variant-null:
    // Variant.getFieldByKey / getElementAtIndex throw IllegalArgumentException
    // for type mismatch by design, but the rule-level contract is "navigate
    // returns variant-null on any miss" — type mismatch is one kind of miss.
    // Malformed JSONPath in variants.at still throws (constraint-registration
    // failure, not a silent runtime no-op).
    out.add(CelFunctionBinding.from(
        "variants_at_variant_string", Variant.class, String.class,
        (Variant v, String path) -> nullToVariantNull(VariantPath.walk(v, path))));
    out.add(CelFunctionBinding.from(
        "variants_field_variant_string", Variant.class, String.class,
        (Variant v, String key) -> v.getType() == Variant.Type.OBJECT
            ? nullToVariantNull(v.getFieldByKey(key))
            : NULL_VARIANT));
    out.add(CelFunctionBinding.from(
        "variants_elem_variant_int", Variant.class, Long.class,
        (Variant v, Long idx) -> v.getType() == Variant.Type.ARRAY
            ? nullToVariantNull(v.getElementAtIndex(idx.intValue()))
            : NULL_VARIANT));

    // Parameterized typed extraction. variants.as throws on mismatch;
    // variants.tryAs returns CEL null. The second-arg type string selects
    // the target type (see variantAs for the accepted vocabulary).
    out.add(CelFunctionBinding.from(
        "variants_as_variant_string", Variant.class, String.class,
        (Variant v, String typeStr) -> variantAs(v, typeStr, /*tryMode=*/ false)));
    out.add(CelFunctionBinding.from(
        "variants_tryas_variant_string", Variant.class, String.class,
        (Variant v, String typeStr) -> variantAs(v, typeStr, /*tryMode=*/ true)));

    // string(Variant) — JSON serialization extension on stdlib string(...).
    out.add(CelFunctionBinding.from(
        "variant_to_string", Variant.class, VariantUtils::toJsonString));
  }

  private static Variant nullToVariantNull(Variant v) {
    return v == null ? NULL_VARIANT : v;
  }

  /**
   * Runtime dispatch for {@code variants.as(v, typeStr)} / {@code variants.tryAs}.
   *
   * <p>Accepted type strings match the {@code variants.type(v)} output for
   * extractable scalar types: {@code "string"}, {@code "int"}, {@code "double"},
   * {@code "boolean"}, {@code "decimal"}, {@code "timestamp"}, {@code "bytes"}.
   * The container/sentinel/v1-out-of-scope labels — {@code "object"}, {@code
   * "array"}, {@code "null"}, {@code "date"}, {@code "time"}, {@code "uuid"} —
   * are rejected (no concrete typed extractor exists for them).
   *
   * <p>In {@code tryMode}, all rejections return {@link
   * dev.cel.common.values.NullValue#NULL_VALUE}. Outside {@code tryMode},
   * mismatches throw {@link IllegalArgumentException}.
   */
  private static Object variantAs(Variant v, String typeStr, boolean tryMode) {
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
        if (tryMode) {
          return NullValue.NULL_VALUE;
        }
        throw new IllegalArgumentException(
            "variants.as: type '" + typeStr + "' is not supported for extraction"
                + " (use variants.type/variants.at/variants.field/variants.elem instead)");
      default:
        if (tryMode) {
          return NullValue.NULL_VALUE;
        }
        throw new IllegalArgumentException(
            "variants.as: unknown type '" + typeStr + "'"
                + " (expected one of: string, int, double, boolean, decimal, timestamp, bytes)");
    }
    // Recognized typeStr but actual variant type doesn't match.
    if (tryMode) {
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
