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
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.types.OpaqueType;
import dev.cel.common.types.SimpleType;
import java.util.ArrayList;
import java.util.List;

final class BuiltinDeclarations {

  private static final OpaqueType DECIMAL = CelTypeLabels.DECIMAL;
  private static final OpaqueType VARIANT = CelTypeLabels.VARIANT;

  private BuiltinDeclarations() {
  }

  static List<CelFunctionDecl> create() {
    List<CelFunctionDecl> decls = new ArrayList<>();
    decls.add(member("isEmail", "is_email"));
    decls.add(member("isHostname", "is_hostname"));
    decls.add(member("isIpv4", "is_ipv4"));
    decls.add(member("isIpv6", "is_ipv6"));
    decls.add(member("isUriRef", "is_uri_ref"));
    decls.add(member("isUri", "is_uri"));
    decls.add(member("isUuid", "is_uuid"));
    addDecimal(decls);
    addTimestamp(decls);
    addVariant(decls);
    return ImmutableList.copyOf(decls);
  }

  /**
   * Build a member-style function declaration: {@code STRING.isFoo() -> bool}.
   */
  private static CelFunctionDecl member(String functionName, String overloadId) {
    return CelFunctionDecl.newFunctionDeclaration(
        functionName,
        CelOverloadDecl.newMemberOverload(
            overloadId, SimpleType.BOOL, SimpleType.STRING));
  }

  // ---- Decimal ----

  private static void addDecimal(List<CelFunctionDecl> decls) {
    // decimal(...) constructor overloads. No separate (string) overload —
    // cel-java's checker rejects it as overlapping with (dyn) (string is
    // assignable to dyn). The (dyn) overload's runtime dispatch handles
    // String via DecimalUtils.toBigDecimal(Object)'s String arm.
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "decimal",
        CelOverloadDecl.newGlobalOverload(
            "dyn_to_decimal",
            "Convert a value to Decimal (runtime dispatches on actual type)",
            DECIMAL, ImmutableList.of(SimpleType.DYN)),
        CelOverloadDecl.newGlobalOverload(
            "bytes_int_to_decimal",
            "Construct from unscaled two's-complement bytes + scale",
            DECIMAL, ImmutableList.of(SimpleType.BYTES, SimpleType.INT))));

    // Comparison: decimals.eq/ne/lt/le/gt/ge
    decls.add(binaryDecimal("decimals.eq", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.ne", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.lt", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.le", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.gt", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.ge", SimpleType.BOOL));

    // Arithmetic: decimals.add/sub/mul/div
    decls.add(binaryDecimal("decimals.add", DECIMAL));
    decls.add(binaryDecimal("decimals.sub", DECIMAL));
    decls.add(binaryDecimal("decimals.mul", DECIMAL));
    decls.add(binaryDecimal("decimals.div", DECIMAL));

    // Unary numeric: decimals.neg/abs/sign/scale/precision
    decls.add(unaryDecimal("decimals.neg", DECIMAL));
    decls.add(unaryDecimal("decimals.abs", DECIMAL));
    decls.add(unaryDecimal("decimals.sign", SimpleType.INT));
    decls.add(unaryDecimal("decimals.scale", SimpleType.INT));
    decls.add(unaryDecimal("decimals.precision", SimpleType.INT));

    // String coercion via CEL built-in: extend stdlib `string(...)` with a
    // (Decimal) -> string overload. No overlap with any existing
    // string(int/uint/double/bytes/timestamp/duration/string) overload because
    // our opaque Decimal type isn't assignable to any of them.
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "string",
        CelOverloadDecl.newGlobalOverload(
            "decimal_to_string",
            "Convert a Decimal to its plain-string form (no scientific notation)",
            SimpleType.STRING, ImmutableList.of(DECIMAL))));

    // Rounding family: decimals.round/trunc accept either 1 or 2 args; floor/ceil unary.
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "decimals.round",
        CelOverloadDecl.newGlobalOverload(
            "decimals_round_unary",
            "Round to integer (HALF_UP, away from zero on ties)",
            DECIMAL, ImmutableList.of(DECIMAL)),
        CelOverloadDecl.newGlobalOverload(
            "decimals_round_scale",
            "Round to the given scale (HALF_UP). Negative scale rounds left of the decimal.",
            DECIMAL, ImmutableList.of(DECIMAL, SimpleType.INT))));
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "decimals.trunc",
        CelOverloadDecl.newGlobalOverload(
            "decimals_trunc_unary",
            "Truncate to integer (toward zero)",
            DECIMAL, ImmutableList.of(DECIMAL)),
        CelOverloadDecl.newGlobalOverload(
            "decimals_trunc_scale",
            "Truncate to the given scale (toward zero)",
            DECIMAL, ImmutableList.of(DECIMAL, SimpleType.INT))));
    decls.add(unaryDecimal("decimals.floor", DECIMAL));
    decls.add(unaryDecimal("decimals.ceil", DECIMAL));
  }

  private static CelFunctionDecl binaryDecimal(String name, dev.cel.common.types.CelType result) {
    return CelFunctionDecl.newFunctionDeclaration(
        name,
        CelOverloadDecl.newGlobalOverload(
            overloadId(name, "decimal_decimal"),
            result, ImmutableList.of(DECIMAL, DECIMAL)));
  }

  private static CelFunctionDecl unaryDecimal(String name, dev.cel.common.types.CelType result) {
    return CelFunctionDecl.newFunctionDeclaration(
        name,
        CelOverloadDecl.newGlobalOverload(
            overloadId(name, "decimal"),
            result, ImmutableList.of(DECIMAL)));
  }

  // ---- Timestamp ----

  private static void addTimestamp(List<CelFunctionDecl> decls) {
    // timestamp.of — two overloads (matches the decimal(...) / variant(...)
    // pattern: (dyn) runtime-dispatch + explicit (int, string) epoch + unit).
    // The arity check in SignaturesOverlap short-circuits, so the overlap rule
    // doesn't trigger.
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "timestamp.of",
        CelOverloadDecl.newGlobalOverload(
            "timestamp_of_dyn",
            "Convert a value to a Timestamp (runtime dispatches on actual type)",
            SimpleType.TIMESTAMP, ImmutableList.of(SimpleType.DYN)),
        CelOverloadDecl.newGlobalOverload(
            "timestamp_of_int_string",
            "Construct from epoch numeric + unit (millis, micros, nanos, seconds)",
            SimpleType.TIMESTAMP, ImmutableList.of(SimpleType.INT, SimpleType.STRING))));
  }

  // ---- Variant ----

  private static void addVariant(List<CelFunctionDecl> decls) {
    // variant(...) constructor overloads
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variant",
        CelOverloadDecl.newGlobalOverload(
            "dyn_to_variant",
            "Convert a value to a Variant (runtime dispatches on actual type)",
            VARIANT, ImmutableList.of(SimpleType.DYN)),
        CelOverloadDecl.newGlobalOverload(
            "bytes_bytes_to_variant",
            "Construct from value + metadata byte arrays",
            VARIANT, ImmutableList.of(SimpleType.BYTES, SimpleType.BYTES))));

    // Type inspection
    decls.add(unaryVariant("variants.type", SimpleType.STRING));
    decls.add(unaryVariant("variants.isNull", SimpleType.BOOL));

    // Path / field / element navigation
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.get",
        CelOverloadDecl.newGlobalOverload(
            "variants_get_variant_string",
            "JSONPath subset navigation; missing path → variant-null",
            VARIANT, ImmutableList.of(VARIANT, SimpleType.STRING))));
    decls.add(binaryVariant("variants.getField", SimpleType.STRING, VARIANT));
    decls.add(binaryVariant("variants.getElement", SimpleType.INT, VARIANT));

    // Typed extraction
    decls.add(unaryVariant("variants.getString", SimpleType.STRING));
    decls.add(unaryVariant("variants.getInt", SimpleType.INT));
    decls.add(unaryVariant("variants.getDouble", SimpleType.DOUBLE));
    decls.add(unaryVariant("variants.getBool", SimpleType.BOOL));
    decls.add(unaryVariant("variants.getDecimal", DECIMAL));
    decls.add(unaryVariant("variants.getTimestamp", SimpleType.TIMESTAMP));
    decls.add(unaryVariant("variants.getBinary", SimpleType.BYTES));
    decls.add(unaryVariant("variants.toJson", SimpleType.STRING));

    // Try-typed extraction (returns input Variant on type match, else NULL Variant)
    decls.add(unaryVariant("variants.tryGetString", VARIANT));
    decls.add(unaryVariant("variants.tryGetInt", VARIANT));
    decls.add(unaryVariant("variants.tryGetDouble", VARIANT));
    decls.add(unaryVariant("variants.tryGetBool", VARIANT));
    decls.add(unaryVariant("variants.tryGetDecimal", VARIANT));
    decls.add(unaryVariant("variants.tryGetTimestamp", VARIANT));
    decls.add(unaryVariant("variants.tryGetBinary", VARIANT));

    // Try path / field / element (aliases — the non-try forms are already null-safe
    // for missing fields/indices; tryGet additionally suppresses path parse errors).
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.tryGet",
        CelOverloadDecl.newGlobalOverload(
            "variants_try_get_variant_string",
            "JSONPath; variant-null on miss or parse error",
            VARIANT, ImmutableList.of(VARIANT, SimpleType.STRING))));
    decls.add(binaryVariant("variants.tryGetField", SimpleType.STRING, VARIANT));
    decls.add(binaryVariant("variants.tryGetElement", SimpleType.INT, VARIANT));
  }

  private static CelFunctionDecl unaryVariant(String name, dev.cel.common.types.CelType result) {
    return CelFunctionDecl.newFunctionDeclaration(
        name,
        CelOverloadDecl.newGlobalOverload(
            overloadId(name, "variant"),
            result, ImmutableList.of(VARIANT)));
  }

  private static CelFunctionDecl binaryVariant(
      String name, dev.cel.common.types.CelType arg2,
      dev.cel.common.types.CelType result) {
    return CelFunctionDecl.newFunctionDeclaration(
        name,
        CelOverloadDecl.newGlobalOverload(
            overloadId(name, "variant_" + typeSuffix(arg2)),
            result, ImmutableList.of(VARIANT, arg2)));
  }

  private static String overloadId(String functionName, String suffix) {
    // Overload IDs are lowercase by cel-java convention (string_char_at_int,
    // list_sets_contains_list, etc.) and we lowercase here so the binding-side
    // IDs in BuiltinOverload (also lowercase) match the decl-side IDs exactly.
    return (functionName + "_" + suffix)
        .replace('.', '_')
        .toLowerCase(java.util.Locale.ROOT);
  }

  private static String typeSuffix(dev.cel.common.types.CelType t) {
    return t.name().toLowerCase(java.util.Locale.ROOT)
        .replace("!error!", "err");
  }
}
