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
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import dev.cel.common.CelOptions;
import dev.cel.common.Operator;
import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import dev.cel.runtime.CelFunctionBinding;
import dev.cel.runtime.CelStandardFunctions.StandardFunction;
import dev.cel.runtime.RuntimeEquality;
import dev.cel.runtime.standard.CelStandardFunction;
import dev.cel.runtime.standard.DoubleFunction;
import dev.cel.runtime.standard.InOperator;
import dev.cel.runtime.standard.StringFunction;
import dev.cel.runtime.standard.TimestampFunction;
import io.confluent.kafka.schemaregistry.type.Variant;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
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

  /**
   * The standard functions {@link #standardOverrides} takes over. A caller must exclude these
   * from the runtime's standard functions, or registering the replacement bindings collides with
   * the standard ones under the same overload id.
   */
  static final ImmutableSet<StandardFunction> OVERRIDDEN_STANDARD_FUNCTIONS =
      ImmutableSet.of(StandardFunction.TIMESTAMP, StandardFunction.STRING,
          StandardFunction.DOUBLE, StandardFunction.EQUALS, StandardFunction.NOT_EQUALS,
          StandardFunction.IN);

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
        "dyn_to_decimal", Object.class,
        (Object o) -> CelDecimal.of(DecimalUtils.toBigDecimal(o))));
    // Bytes args arrive as CelByteString under CelOptions.DEFAULT
    // (evaluateCanonicalTypesToNativeValues=true converts proto ByteString and
    // bytes-field reads into CelByteString).
    out.add(CelFunctionBinding.from(
        "bytes_int_to_decimal",
        CelByteString.class, Long.class,
        (CelByteString bytes, Long scale) ->
            CelDecimal.of(DecimalUtils.toBigDecimal(bytes.toByteArray(),
                requireIntScale(scale, "decimal(bytes, scale)")))));

    // Comparison
    out.add(decimalsCompare("decimals_eq_decimal_decimal", (a, b) -> a.compareTo(b) == 0));
    out.add(decimalsCompare("decimals_lt_decimal_decimal", (a, b) -> a.compareTo(b) < 0));
    out.add(decimalsCompare("decimals_le_decimal_decimal", (a, b) -> a.compareTo(b) <= 0));
    out.add(decimalsCompare("decimals_gt_decimal_decimal", (a, b) -> a.compareTo(b) > 0));
    out.add(decimalsCompare("decimals_ge_decimal_decimal", (a, b) -> a.compareTo(b) >= 0));

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
        "decimals_sign_decimal", Object.class,
        (Object d) -> (long) DecimalUtils.toBigDecimal(d).signum()));
    // Rounding family — Flink-aligned. Negative scale rounds left of the decimal.
    out.add(decimalsUnary(
        "decimals_round_unary", d -> d.setScale(0, RoundingMode.HALF_UP)));
    out.add(CelFunctionBinding.from(
        "decimals_round_scale", Object.class, Long.class,
        (Object d, Long scale) -> CelDecimal.of(
            DecimalUtils.toBigDecimal(d).setScale(
                requireIntScale(scale, "decimals.round"), RoundingMode.HALF_UP))));
    // Flink's TRUNCATE early-returns when the target scale is at-or-finer than
    // the current scale — it's a no-op there, so the result keeps the input's
    // representation. Without this guard, setScale(n>=cur, DOWN) would zero-pad
    // and string(trunc(x, n>=cur)) would diverge from Flink.
    out.add(decimalsUnary(
        "decimals_trunc_unary",
        d -> d.scale() <= 0 ? d : d.setScale(0, RoundingMode.DOWN)));
    out.add(CelFunctionBinding.from(
        "decimals_trunc_scale", Object.class, Long.class,
        (Object d, Long scale) -> {
          int intScale = requireIntScale(scale, "decimals.trunc");
          BigDecimal v = DecimalUtils.toBigDecimal(d);
          return intScale >= v.scale()
              ? CelDecimal.of(v) : CelDecimal.of(v.setScale(intScale, RoundingMode.DOWN));
        }));
    out.add(decimalsUnary(
        "decimals_floor_decimal", d -> d.setScale(0, RoundingMode.FLOOR)));
    out.add(decimalsUnary(
        "decimals_ceil_decimal", d -> d.setScale(0, RoundingMode.CEILING)));
  }

  /**
   * Every {@code decimals.*} binding takes {@code Object} and coerces, rather than binding
   * {@link CelDecimal} directly. A value carrying {@link CelTypeLabels#DECIMAL} is a
   * {@code CelDecimal} when it came from an Avro boundary conversion, the {@code decimal()}
   * constructor, or another {@code decimals.*} result — but a plain proto {@code Decimal} message
   * when it came from protobuf field selection, which happens inside the runtime where no boundary
   * can reach it. {@link DecimalUtils#toBigDecimal(Object)} accepts either, so binding
   * {@code CelDecimal} would reject exactly the bare-protobuf case these declarations now admit.
   *
   * <p>Safe here because {@code decimals.*} are our own namespaced functions with a single binding
   * per overload id — unlike the extensions in {@link #standardOverrides}, where an
   * {@code Object}-bound binding sitting in a dispatch group with the standard ones would claim
   * every call.
   */
  private static CelFunctionBinding decimalsBinary(
      String overloadId, BiFunction<BigDecimal, BigDecimal, BigDecimal> fn) {
    return CelFunctionBinding.from(
        overloadId, Object.class, Object.class,
        (Object a, Object b) -> CelDecimal.of(
            fn.apply(DecimalUtils.toBigDecimal(a), DecimalUtils.toBigDecimal(b))));
  }

  /** Comparison family: the CEL bool result passes through, only Decimal results get wrapped. */
  private static CelFunctionBinding decimalsCompare(
      String overloadId, BiPredicate<BigDecimal, BigDecimal> fn) {
    return CelFunctionBinding.from(
        overloadId, Object.class, Object.class,
        (Object a, Object b) -> fn.test(
            DecimalUtils.toBigDecimal(a), DecimalUtils.toBigDecimal(b)));
  }

  private static CelFunctionBinding decimalsUnary(
      String overloadId, Function<BigDecimal, BigDecimal> fn) {
    return CelFunctionBinding.from(overloadId, Object.class,
        (Object d) -> CelDecimal.of(fn.apply(DecimalUtils.toBigDecimal(d))));
  }

  // ---- Timestamp ----

  /**
   * The overloads we add to <em>standard</em> function names, regrouped together with the
   * standard library's own bindings for those names.
   *
   * <p>Regrouping is required, not cosmetic. The planner runtime resolves a call to a single
   * overload id at plan time and, when the checker could not narrow it that far — which is
   * every call whose argument is {@code dyn}, i.e. every Avro logical-typed field — falls back
   * to a <em>name</em>-keyed lookup ({@code ProgramPlanner}, "Parsed-only function dispatch").
   * That name-keyed entry is a dynamic-dispatch group built from the bindings registered
   * together under the name, so an overload merely added alongside the standard ones is invisible
   * to it: {@code timestamp(this.ts)} fails with "No matching overload ... candidates:
   * string_to_timestamp, timestamp_to_timestamp, int64_to_timestamp".
   *
   * <p>Callers must therefore exclude {@link #OVERRIDDEN_STANDARD_FUNCTIONS} from the runtime's
   * standard functions and register these instead — the standard behavior is preserved because
   * the standard bindings are re-registered here verbatim, not reimplemented.
   */
  static ImmutableList<CelFunctionBinding> standardOverrides(
      CelOptions celOptions, RuntimeEquality runtimeEquality) {
    List<CelFunctionBinding> out = new ArrayList<>();

    // timestamp: the standard string / int / identity overloads, plus a Temporal binding
    // (stdlib's identity overload binds Instant alone, leaving every other java.time shape an
    // Avro or Proto decoder yields with no overload) and the (int, int) precision form.
    out.addAll(regroup("timestamp", TimestampFunction.create(), celOptions, runtimeEquality,
        CelFunctionBinding.from(
            "timestamp_to_timestamp_temporal", Temporal.class, TimestampUtils::toInstant),
        CelFunctionBinding.from(
            "timestamp_int_int", Long.class, Long.class, TimestampUtils::fromEpochPrecision)));

    // string(Decimal) / double(Decimal). Object-bound, not CelDecimal-bound: a decimal value is
    // a CelDecimal or a proto Decimal message depending on where it came from, and the checker
    // resolves the single declared string(Decimal) overload to one id, so that id has to accept
    // both. Safe despite the width because regroup appends extras last and dispatch is
    // first-match over insertion order (FunctionBindingImpl.DynamicDispatchOverload) — every
    // standard overload gets first refusal, and these see only what none of them handled.
    // The double form is narrowing: BigDecimal.doubleValue() returns the closest double
    // (±Infinity when out of range).
    out.addAll(regroup("string", StringFunction.create(), celOptions, runtimeEquality,
        CelFunctionBinding.from("decimal_to_string", Object.class,
            (Object d) -> requireDecimal(d, "string").toPlainString())));
    out.addAll(regroup("double", DoubleFunction.create(), celOptions, runtimeEquality,
        CelFunctionBinding.from("decimal_to_double", Object.class,
            (Object d) -> requireDecimal(d, "double").doubleValue())));

    // == and != . Unlike the three above, these are replaced rather than regrouped: stdlib
    // binds each as a single generic (Object, Object) overload whose id equals the function
    // name (EqualsOperator: CelFunctionBinding.from("equals", Object, Object,
    // runtimeEquality::objectEquals)), so there is no dispatch group to extend and nothing to
    // preserve alongside. Adding a narrower (Decimal, Decimal) declaration instead does not
    // work: stdlib's (A, A) always matches too, so the checker cannot narrow to one overload id
    // and the name fallback lands back on the standard binding.
    out.add(CelFunctionBinding.from("equals", Object.class, Object.class,
        (Object x, Object y) -> decimalEquals(x, y, runtimeEquality)));
    out.add(CelFunctionBinding.from("not_equals", Object.class, Object.class,
        (Object x, Object y) -> !decimalEquals(x, y, runtimeEquality)));

    // `in` over a list has to follow ==: RuntimeEquality.inList uses List.contains, which is
    // Java equality on the raw elements, so `this.subtotal in [this.total]` answered false for
    // 1.50 against 1.5 while `==` on the same operands answered true. Regrouped rather than
    // replaced, because `in` has two overloads and only the list one is affected — a CEL map key
    // can only be int, uint, bool or string, so a decimal can never be one.
    out.addAll(regroup(Operator.IN.getFunction(), InOperator.create(), celOptions, runtimeEquality,
        CelFunctionBinding.from("in_list", Object.class, List.class,
            // Raw List, matching the standard binding's own signature so the delegation below
            // type-checks against RuntimeEquality.inList.
            (Object value, List list) -> {
              if (!hasDecimal(value) && !hasDecimal(list)) {
                return runtimeEquality.inList(list, value);
              }
              for (Object element : list) {
                if (decimalEquals(value, element, runtimeEquality)) {
                  return true;
                }
              }
              return false;
            })));

    return ImmutableList.copyOf(out);
  }

  /**
   * CEL {@code ==} with decimals made numeric. A decimal operand may be a {@link CelDecimal} or
   * a proto {@code Decimal} message, and proto equality is the wrong answer for both: it compares
   * unscaled bytes and scale field-by-field, so {@code 1.50} and {@code 1.5} — the same number in
   * two encodings — come out unequal.
   *
   * <p>A thin pre-filter, deliberately. When neither side is a decimal this delegates to the
   * standard implementation verbatim, so every other {@code ==} in the language keeps stdlib
   * semantics (numeric cross-type rules, list and map recursion, proto message comparison,
   * the {@code disableCelStandardEquality} option).
   */
  private static boolean decimalEquals(Object x, Object y, RuntimeEquality runtimeEquality) {
    BigDecimal a = asDecimalOrNull(x);
    BigDecimal b = asDecimalOrNull(y);
    if (a != null && b != null) {
      return a.compareTo(b) == 0;
    }
    if (a != null || b != null) {
      // A decimal is never equal to a non-decimal. Returning stdlib's answer here would be
      // false anyway, but only by accident of the shapes not matching; say it outright.
      return false;
    }
    // Containers, but only when a decimal is actually inside one of them. The standard
    // implementation recurses with its own equality, so a Decimal nested in a list or map was
    // compared by protobuf encoding and `[this.subtotal] == [this.total]` answered false while
    // `this.subtotal == this.total` answered true — the same values, disagreeing on nesting
    // alone. Gating on hasDecimal keeps every decimal-free comparison on the standard path
    // exactly as it was, and each element pair recurses back through here, so a non-decimal
    // element inside a decimal-bearing container still gets standard semantics.
    if (x instanceof List && y instanceof List && (hasDecimal(x) || hasDecimal(y))) {
      List<?> xs = (List<?>) x;
      List<?> ys = (List<?>) y;
      if (xs.size() != ys.size()) {
        return false;
      }
      for (int i = 0; i < xs.size(); i++) {
        if (!decimalEquals(xs.get(i), ys.get(i), runtimeEquality)) {
          return false;
        }
      }
      return true;
    }
    if (x instanceof Map && y instanceof Map && (hasDecimal(x) || hasDecimal(y))) {
      Map<?, ?> xm = (Map<?, ?>) x;
      Map<?, ?> ym = (Map<?, ?>) y;
      if (xm.size() != ym.size()) {
        return false;
      }
      for (Map.Entry<?, ?> e : xm.entrySet()) {
        if (!ym.containsKey(e.getKey())
            || !decimalEquals(e.getValue(), ym.get(e.getKey()), runtimeEquality)) {
          return false;
        }
      }
      return true;
    }
    return runtimeEquality.objectEquals(x, y);
  }

  /**
   * Whether {@code o} is a decimal or holds one, at any depth. Only consulted once both operands
   * are containers, so it never runs on the scalar comparison path.
   */
  private static boolean hasDecimal(Object o) {
    if (asDecimalOrNull(o) != null) {
      return true;
    }
    if (o instanceof List) {
      for (Object element : (List<?>) o) {
        if (hasDecimal(element)) {
          return true;
        }
      }
      return false;
    }
    if (o instanceof Map) {
      for (Object value : ((Map<?, ?>) o).values()) {
        if (hasDecimal(value)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * {@code o} as a {@link BigDecimal} if it carries {@link CelTypeLabels#DECIMAL}, else null.
   * Narrow on purpose — unlike {@link DecimalUtils#toBigDecimal(Object)}, which coerces numbers
   * and strings too. This runs on every {@code ==} in every rule, so it must not turn
   * {@code 1 == "1"} into a decimal comparison, and it must stay cheap for the common case.
   */
  private static BigDecimal asDecimalOrNull(Object o) {
    if (o instanceof CelDecimal) {
      return ((CelDecimal) o).value();
    }
    if (o instanceof Message
        && CelTypeLabels.DECIMAL_NAME.equals(
            ((Message) o).getDescriptorForType().getFullName())) {
      return DecimalUtils.toBigDecimal(o);
    }
    return null;
  }

  /**
   * {@code o} as a {@link BigDecimal}, or a clear failure. Uses the narrow
   * {@link #asDecimalOrNull} rather than {@link DecimalUtils#toBigDecimal(Object)} because this
   * backs the last-resort binding in the {@code string} / {@code double} dispatch groups: a value
   * that reaches it is one no standard overload could handle, and reporting that plainly beats
   * coercing, say, a list into a number.
   */
  private static BigDecimal requireDecimal(Object o, String functionName) {
    BigDecimal d = asDecimalOrNull(o);
    if (d == null) {
      throw new IllegalArgumentException(
          functionName + ": expected " + CelTypeLabels.DECIMAL_NAME + ", got "
              + (o instanceof Message
                 ? ((Message) o).getDescriptorForType().getFullName()
                 : o.getClass().getName()));
    }
    return d;
  }

  /**
   * One standard function's bindings plus {@code extras}, grouped under {@code functionName} so
   * the name-keyed dynamic-dispatch entry covers all of them. The standard function's own
   * name-keyed entry is dropped from the input, since {@code fromOverloads} rebuilds it.
   */
  private static ImmutableSet<CelFunctionBinding> regroup(
      String functionName,
      CelStandardFunction standardFunction,
      CelOptions celOptions,
      RuntimeEquality runtimeEquality,
      CelFunctionBinding... extras) {
    List<CelFunctionBinding> bindings = new ArrayList<>();
    for (CelFunctionBinding binding :
        standardFunction.newFunctionBindings(celOptions, runtimeEquality)) {
      if (!binding.getOverloadId().equals(functionName)) {
        bindings.add(binding);
      }
    }
    for (CelFunctionBinding extra : extras) {
      // An extra sharing a standard overload's id replaces it; a new id is added alongside.
      // timestamp_to_timestamp_temporal is a new id, so stdlib's Instant-bound
      // timestamp_to_timestamp stays in the group. Harmless: the declaration side shadows it
      // (same signature), and for an Instant both bindings do the same thing.
      bindings.removeIf(b -> b.getOverloadId().equals(extra.getOverloadId()));
      bindings.add(extra);
    }
    return CelFunctionBinding.fromOverloads(functionName, bindings);
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
        (CelByteString value, CelByteString metadata) -> {
          Variant result = VariantUtils.fromBytes(value.toByteArray(), metadata.toByteArray());
          if (result == null) {
            // A variant's metadata carries the key dictionary and opens with a version byte, so
            // empty metadata cannot be decoded.
            throw new IllegalArgumentException(
                "variant(value, metadata): metadata is empty, so there is no variant to read");
          }
          return result;
        }));

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
        "variants_type_variant", Object.class,
        (Object o) -> {
          Variant v = requireVariantOrNull(o, "variants.type");
          return v == null ? NullValue.NULL_VALUE : variantTypeName(v.getType());
        }));
    out.add(CelFunctionBinding.from(
        "variants_isnull_dyn", Object.class,
        // Coerces like every other accessor. A raw `o instanceof Variant` would answer false for
        // the shapes a variant-typed *field* decodes to — a confluent.type.Variant message, or
        // the map an Avro variant record yields — which the dyn declaration now admits: a bare
        // variant holding an explicit JSON null reported false instead of true.
        (Object o) -> {
          Variant v = requireVariantOrNull(o, "variants.isNull");
          return v != null && v.getType() == Variant.Type.NULL;
        }));

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
        "variants_tojson_variant", Object.class,
        (Object o) -> {
          Variant v = requireVariantOrNull(o, "variants.toJson");
          return v == null ? NullValue.NULL_VALUE : VariantUtils.toJsonString(v);
        }));
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
    if (o instanceof Variant) {
      return (Variant) o;
    }
    // Otherwise accept the shapes a variant-typed *field* decodes to — a proto
    // confluent.type.Variant message, or the Map an Avro variant record is converted to — so
    // such a field can be used without a variant(...) call, as on the other clients. A shape
    // toVariant doesn't recognize still yields a clear message rather than a ClassCastException.
    try {
      return VariantUtils.toVariant(o);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          functionName + ": expected Variant, got " + o.getClass().getName(), e);
    } catch (RuntimeException e) {
      // A malformed value — the Variant constructor rejects an unreadable metadata version — is
      // reported against the function that read it rather than surfacing bare.
      throw new IllegalArgumentException(
          functionName + ": malformed Variant: " + e.getMessage(), e);
    }
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
    // toVariant returns null for an absent variant — an unset proto field, or an Avro variant
    // record with empty byte fields. Report that as CEL null, the same as a null input above,
    // rather than handing a Java null back to the runtime.
    Variant v = VariantUtils.toVariant(o);
    return v == null ? NullValue.NULL_VALUE : v;
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
          // A bare BigDecimal here would make decimals.eq(variants.as(v, 'decimal'), ...) fail
          // with "no matching overload" at runtime; the compiler can't catch it, since both
          // sides are the DECIMAL OpaqueType at the CEL level.
          return CelDecimal.of(v.getDecimal());
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
