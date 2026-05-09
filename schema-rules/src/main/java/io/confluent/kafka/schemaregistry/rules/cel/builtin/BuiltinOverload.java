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

import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.type.Variant;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.projectnessie.cel.common.types.BoolT;
import org.projectnessie.cel.common.types.BytesT;
import org.projectnessie.cel.common.types.DoubleT;
import org.projectnessie.cel.common.types.Err;
import org.projectnessie.cel.common.types.IntT;
import org.projectnessie.cel.common.types.StringT;
import org.projectnessie.cel.common.types.TimestampT;
import org.projectnessie.cel.common.types.Types;
import org.projectnessie.cel.common.types.ref.TypeEnum;
import org.projectnessie.cel.common.types.ref.Val;
import org.projectnessie.cel.interpreter.functions.BinaryOp;
import org.projectnessie.cel.interpreter.functions.Overload;
import org.projectnessie.cel.interpreter.functions.UnaryOp;

final class BuiltinOverload {

  private static final String OVERLOAD_IS_EMAIL = "isEmail";
  private static final String OVERLOAD_IS_HOSTNAME = "isHostname";
  private static final String OVERLOAD_IS_IPV4 = "isIpv4";
  private static final String OVERLOAD_IS_IPV6 = "isIpv6";
  private static final String OVERLOAD_IS_URI = "isUri";
  private static final String OVERLOAD_IS_URI_REF = "isUriRef";
  private static final String OVERLOAD_IS_UUID = "isUuid";

  static Overload[] create() {
    return new Overload[] {
      isEmail(),
      isHostname(),
      isIpv4(),
      isIpv6(),
      isUri(),
      isUriRef(),
      isUuid(),
      // to_decimal: registered under the bare function name with both unary (dyn)
      // and binary (bytes, int) handlers. Nessie's planner falls back to function-name
      // lookup, so multi-arity overloads must be coalesced into a single entry.
      safeOverload(
          "to_decimal",
          v -> decimalOf(DecimalUtils.toBigDecimal(v.value())),
          (b, s) -> decimalOf(DecimalUtils.toBigDecimal(
              VariantUtils.toBytes(b.value()), (int) s.intValue()))),
      // decimal_* operators
      decimalCmp("decimal_eq", (a, b) -> a.compareTo(b) == 0),
      decimalCmp("decimal_ne", (a, b) -> a.compareTo(b) != 0),
      decimalCmp("decimal_lt", (a, b) -> a.compareTo(b) < 0),
      decimalCmp("decimal_le", (a, b) -> a.compareTo(b) <= 0),
      decimalCmp("decimal_gt", (a, b) -> a.compareTo(b) > 0),
      decimalCmp("decimal_ge", (a, b) -> a.compareTo(b) >= 0),
      decimalArith("decimal_add", BigDecimal::add),
      decimalArith("decimal_sub", BigDecimal::subtract),
      decimalArith("decimal_mul", BigDecimal::multiply),
      // BigDecimal.divide(other) throws on non-terminating decimals; supply an
      // explicit MathContext. Use Flink's MC_DIVIDE: 38-digit precision with
      // HALF_UP rounding, matching Flink's DecimalDataUtils.MC_DIVIDE exactly.
      // This keeps the rounding mode consistent with decimal_round (also HALF_UP)
      // and aligns with PostgreSQL's NUMERIC division semantics.
      decimalArith("decimal_div", (a, b) ->
          a.divide(b, new java.math.MathContext(38, java.math.RoundingMode.HALF_UP))),
      decimalUnary("decimal_neg", BigDecimal::negate),
      decimalUnary("decimal_abs", BigDecimal::abs),
      safeUnary("decimal_scale",
          v -> IntT.intOf(((BigDecimal) v.value()).scale())),
      safeUnary("decimal_precision",
          v -> IntT.intOf(((BigDecimal) v.value()).precision())),
      safeUnary("decimal_to_string",
          v -> StringT.stringOf(((BigDecimal) v.value()).toPlainString())),
      // Flink-aligned rounding family — see decimalRoundingFamily() for impls.
      decimalRound(),
      decimalTruncate(),
      decimalSetScale("decimal_floor", java.math.RoundingMode.FLOOR),
      decimalSetScale("decimal_ceil", java.math.RoundingMode.CEILING),
      safeUnary("decimal_sign",
          v -> IntT.intOf(((BigDecimal) v.value()).signum())),
      // to_timestamp: same coalesced shape as to_decimal.
      safeOverload(
          "to_timestamp",
          v -> TimestampT.timestampOf(TimestampUtils.toTimestamp(v.value())),
          (n, u) -> TimestampT.timestampOf(
              TimestampUtils.fromEpoch(n.intValue(), (String) u.value()))),
      // to_variant: same coalesced shape.
      safeOverload(
          "to_variant",
          v -> variantOf(VariantUtils.toVariant(v.value())),
          (val, meta) -> variantOf(VariantUtils.fromBytes(
              VariantUtils.toBytes(val.value()), VariantUtils.toBytes(meta.value())))),
      // variant_* accessors
      safeUnary("variant_type",
          v -> StringT.stringOf(variantTypeName(((Variant) v.value()).getType()))),
      safeUnary("is_variant_null",
          v -> Types.boolOf(((Variant) v.value()).getType() == Variant.Type.NULL)),
      safeBinary("variant_get",
          (v, p) -> variantOf(VariantPath.walk((Variant) v.value(), (String) p.value()))),
      safeBinary("variant_get_field",
          (v, k) -> variantOf(((Variant) v.value()).getFieldByKey((String) k.value()))),
      safeBinary("variant_get_element",
          (v, i) -> variantOf(((Variant) v.value()).getElementAtIndex((int) i.intValue()))),
      safeUnary("variant_get_string",
          v -> StringT.stringOf(((Variant) v.value()).getString())),
      safeUnary("variant_get_int",
          v -> IntT.intOf(variantGetInt((Variant) v.value()))),
      safeUnary("variant_get_double",
          v -> DoubleT.doubleOf(variantGetDouble((Variant) v.value()))),
      safeUnary("variant_get_bool",
          v -> Types.boolOf(((Variant) v.value()).getBoolean())),
      safeUnary("variant_get_decimal",
          v -> decimalOf(((Variant) v.value()).getDecimal())),
      safeUnary("variant_get_timestamp",
          v -> TimestampT.timestampOf(variantGetTimestamp((Variant) v.value()))),
      safeUnary("variant_get_binary",
          v -> BytesT.bytesOf(variantGetBinary((Variant) v.value()))),
      safeUnary("variant_get_uuid",
          v -> StringT.stringOf(((Variant) v.value()).getUUID().toString())),
      safeUnary("variant_to_json",
          v -> StringT.stringOf(VariantUtils.toJsonString((Variant) v.value()))),
      // try_variant_get_* family — return input Variant if type matches, else NULL.
      safeUnary("try_variant_get_string",
          v -> tryTypeFilter((Variant) v.value(),
              t -> t == Variant.Type.STRING)),
      safeUnary("try_variant_get_int",
          v -> tryTypeFilter((Variant) v.value(),
              t -> t == Variant.Type.BYTE || t == Variant.Type.SHORT
                  || t == Variant.Type.INT || t == Variant.Type.LONG)),
      safeUnary("try_variant_get_double",
          v -> tryTypeFilter((Variant) v.value(),
              t -> t == Variant.Type.FLOAT || t == Variant.Type.DOUBLE)),
      safeUnary("try_variant_get_bool",
          v -> tryTypeFilter((Variant) v.value(),
              t -> t == Variant.Type.BOOLEAN)),
      safeUnary("try_variant_get_decimal",
          v -> tryTypeFilter((Variant) v.value(),
              t -> t == Variant.Type.DECIMAL4 || t == Variant.Type.DECIMAL8
                  || t == Variant.Type.DECIMAL16)),
      safeUnary("try_variant_get_timestamp",
          v -> tryTypeFilter((Variant) v.value(),
              t -> t == Variant.Type.TIMESTAMP_TZ || t == Variant.Type.TIMESTAMP_NTZ
                  || t == Variant.Type.TIMESTAMP_NANOS_TZ
                  || t == Variant.Type.TIMESTAMP_NANOS_NTZ)),
      safeUnary("try_variant_get_binary",
          v -> tryTypeFilter((Variant) v.value(),
              t -> t == Variant.Type.BINARY)),
      safeUnary("try_variant_get_uuid",
          v -> tryTypeFilter((Variant) v.value(),
              t -> t == Variant.Type.UUID)),
      // Path / field / element try_* — already null-safe in the non-try versions
      // (getFieldByKey returns null, getElementAtIndex returns null on miss; we wrap).
      // Provided as aliases for API consistency with FLIP-521. The path variant
      // suppresses parse errors too — try_variant_get returns NULL on any failure.
      Overload.binary("try_variant_get",
          (v, p) -> {
            try {
              return variantOf(VariantPath.walk((Variant) v.value(), (String) p.value()));
            } catch (RuntimeException e) {
              return variantOf(null);
            }
          }),
      safeBinary("try_variant_get_field",
          (v, k) -> variantOf(((Variant) v.value()).getFieldByKey((String) k.value()))),
      safeBinary("try_variant_get_element",
          (v, i) -> variantOf(((Variant) v.value()).getElementAtIndex((int) i.intValue()))),
    };
  }

  private static Val tryTypeFilter(Variant v, java.util.function.Predicate<Variant.Type> pred) {
    return pred.test(v.getType()) ? variantOf(v) : variantOf(null);
  }

  private static Overload decimalCmp(String name, BiFunction<BigDecimal, BigDecimal, Boolean> fn) {
    return safeBinary(name, (a, b) ->
        Types.boolOf(fn.apply((BigDecimal) a.value(), (BigDecimal) b.value())));
  }

  private static Overload decimalArith(String name,
      BiFunction<BigDecimal, BigDecimal, BigDecimal> fn) {
    return safeBinary(name, (a, b) ->
        decimalOf(fn.apply((BigDecimal) a.value(), (BigDecimal) b.value())));
  }

  private static Overload decimalUnary(String name, Function<BigDecimal, BigDecimal> fn) {
    return safeUnary(name, v -> decimalOf(fn.apply((BigDecimal) v.value())));
  }

  /**
   * ROUND with HALF_UP — matches Flink {@code sround} and PostgreSQL
   * {@code round(numeric, int)}.
   */
  private static Overload decimalRound() {
    return safeOverload(
        "decimal_round",
        v -> decimalOf(((BigDecimal) v.value())
            .setScale(0, java.math.RoundingMode.HALF_UP)),
        (v, scale) -> decimalOf(((BigDecimal) v.value())
            .setScale((int) scale.intValue(), java.math.RoundingMode.HALF_UP)));
  }

  /**
   * TRUNCATE with RoundingMode.DOWN (toward zero) — matches Flink TRUNCATE.
   */
  private static Overload decimalTruncate() {
    return safeOverload(
        "decimal_truncate",
        v -> decimalOf(((BigDecimal) v.value())
            .setScale(0, java.math.RoundingMode.DOWN)),
        (v, scale) -> decimalOf(((BigDecimal) v.value())
            .setScale((int) scale.intValue(), java.math.RoundingMode.DOWN)));
  }

  /**
   * Single-arg setScale-to-zero variant used for FLOOR / CEIL.
   */
  private static Overload decimalSetScale(String name, java.math.RoundingMode mode) {
    return safeUnary(name, v -> decimalOf(((BigDecimal) v.value()).setScale(0, mode)));
  }

  /**
   * Wrap a unary lambda so that any {@link RuntimeException} thrown by the body is
   * converted to a CEL {@link Err} value rather than propagating up. This is the
   * mechanism by which rule-evaluation failures (e.g. {@code to_decimal("not a
   * number")}) flow out as a clean {@code ScriptExecutionException} (a
   * {@link org.projectnessie.cel.tools.ScriptException} subclass) and ultimately a
   * {@code ValidationRuleError}, instead of escaping {@code validateMessage} as
   * a generic {@code RuntimeException}.
   */
  private static Overload safeUnary(String name, UnaryOp impl) {
    return Overload.unary(name, v -> {
      try {
        return impl.invoke(v);
      } catch (RuntimeException e) {
        return Err.newErr(e, "%s: %s", name, e.getMessage());
      }
    });
  }

  /**
   * Binary equivalent of {@link #safeUnary(String, UnaryOp)}.
   */
  private static Overload safeBinary(String name, BinaryOp impl) {
    return Overload.binary(name, (a, b) -> {
      try {
        return impl.invoke(a, b);
      } catch (RuntimeException e) {
        return Err.newErr(e, "%s: %s", name, e.getMessage());
      }
    });
  }

  /**
   * Multi-arity equivalent for functions registered via
   * {@link Overload#overload(String, org.projectnessie.cel.common.types.traits.Trait,
   * UnaryOp, BinaryOp,
   * org.projectnessie.cel.interpreter.functions.FunctionOp)}.
   */
  private static Overload safeOverload(String name, UnaryOp unary, BinaryOp binary) {
    UnaryOp safeU = v -> {
      try {
        return unary.invoke(v);
      } catch (RuntimeException e) {
        return Err.newErr(e, "%s: %s", name, e.getMessage());
      }
    };
    BinaryOp safeB = (a, b) -> {
      try {
        return binary.invoke(a, b);
      } catch (RuntimeException e) {
        return Err.newErr(e, "%s: %s", name, e.getMessage());
      }
    };
    return Overload.overload(name, null, safeU, safeB, null);
  }

  static Val decimalOf(BigDecimal bd) {
    return OpaqueVal.of(CelTypes.DECIMAL, bd);
  }

  /**
   * Wrap a {@link Variant}; null becomes a Variant whose type is NULL.
   */
  static Val variantOf(Variant v) {
    if (v == null) {
      // Spark variant has a NULL singleton metadata; we synthesize a NULL-typed Variant
      // via a one-byte value buffer (the NULL type primitive header).
      return OpaqueVal.of(CelTypes.VARIANT, NULL_VARIANT);
    }
    return OpaqueVal.of(CelTypes.VARIANT, v);
  }

  /**
   * Pre-built Variant whose top-level type is NULL. Backs missing-field returns from
   * {@code variant_get_*} so rules can guard with {@code is_variant_null}.
   */
  private static final Variant NULL_VARIANT = new Variant(
      // single-byte value buffer: primitive header, type=NULL (0)
      new byte[] {0},
      // minimal metadata: header byte 0x01 (version=1), 0 dictionary entries
      new byte[] {0x01, 0x00, 0x00});

  private static String variantTypeName(Variant.Type t) {
    switch (t) {
      case OBJECT:
        return "object";
      case ARRAY:
        return "array";
      case NULL:
        return "null";
      case BOOLEAN:
        return "boolean";
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
      case DATE:
        return "date";
      case TIME:
        return "time";
      case TIMESTAMP_TZ:
      case TIMESTAMP_NTZ:
      case TIMESTAMP_NANOS_TZ:
      case TIMESTAMP_NANOS_NTZ:
        return "timestamp";
      case STRING:
        return "string";
      case BINARY:
        return "binary";
      case UUID:
        return "uuid";
      default:
        throw new IllegalStateException("Unknown variant type: " + t);
    }
  }

  private static long variantGetInt(Variant v) {
    switch (v.getType()) {
      case BYTE:  return v.getByte();
      case SHORT: return v.getShort();
      case INT:   return v.getInt();
      case LONG:  return v.getLong();
      default:
        throw new IllegalArgumentException(
            "variant_get_int: not an integer-typed variant (" + v.getType() + ")");
    }
  }

  private static double variantGetDouble(Variant v) {
    switch (v.getType()) {
      case FLOAT:  return v.getFloat();
      case DOUBLE: return v.getDouble();
      default:
        throw new IllegalArgumentException(
            "variant_get_double: not a floating-point variant (" + v.getType() + ")");
    }
  }

  private static byte[] variantGetBinary(Variant v) {
    if (v.getType() != Variant.Type.BINARY) {
      throw new IllegalArgumentException(
          "variant_get_binary: not a binary variant (" + v.getType() + ")");
    }
    java.nio.ByteBuffer buf = v.getBinary();
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
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
        throw new IllegalArgumentException(
            "variant_get_timestamp: not a timestamp variant (" + v.getType() + ")");
    }
  }

  private static Overload isEmail() {
    return Overload.unary(
        OVERLOAD_IS_EMAIL,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_EMAIL, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateEmail(input));
        });
  }

  private static Overload isHostname() {
    return Overload.unary(
        OVERLOAD_IS_HOSTNAME,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_HOSTNAME, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateHostname(input));
        });
  }

  private static Overload isIpv4() {
    return Overload.unary(
        OVERLOAD_IS_IPV4,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_IPV4, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateIpv4(input));
        });
  }

  private static Overload isIpv6() {
    return Overload.unary(
        OVERLOAD_IS_IPV6,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_IPV6, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateIpv6(input));
        });
  }

  private static Overload isUri() {
    return Overload.unary(
        OVERLOAD_IS_URI,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_URI, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateUri(input));
        });
  }

  private static Overload isUriRef() {
    return Overload.unary(
        OVERLOAD_IS_URI_REF,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_URI_REF, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateUriRef(input));
        });
  }

  private static Overload isUuid() {
    return Overload.unary(
        OVERLOAD_IS_UUID,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_UUID, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateUuid(input));
        });
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

}
