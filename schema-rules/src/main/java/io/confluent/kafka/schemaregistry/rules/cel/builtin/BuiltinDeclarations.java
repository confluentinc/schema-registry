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
import dev.cel.common.types.CelType;
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

    // Comparison: decimals.eq/lt/le/gt/ge.
    decls.add(binaryDecimal("decimals.eq", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.lt", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.le", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.gt", SimpleType.BOOL));
    decls.add(binaryDecimal("decimals.ge", SimpleType.BOOL));

    // Arithmetic: decimals.add/sub/mul/div
    decls.add(binaryDecimal("decimals.add", DECIMAL));
    decls.add(binaryDecimal("decimals.sub", DECIMAL));
    decls.add(binaryDecimal("decimals.mul", DECIMAL));
    decls.add(binaryDecimal("decimals.div", DECIMAL));

    // Unary numeric: decimals.neg/abs/sign.
    decls.add(unaryDecimal("decimals.neg", DECIMAL));
    decls.add(unaryDecimal("decimals.abs", DECIMAL));
    decls.add(unaryDecimal("decimals.sign", SimpleType.INT));

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

  private static CelFunctionDecl binaryDecimal(String name, CelType result) {
    return CelFunctionDecl.newFunctionDeclaration(
        name,
        CelOverloadDecl.newGlobalOverload(
            overloadId(name, "decimal_decimal"),
            result, ImmutableList.of(DECIMAL, DECIMAL)));
  }

  private static CelFunctionDecl unaryDecimal(String name, CelType result) {
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
    // variant(dyn) — runtime dispatch; null propagates (Spark parse_json(NULL)
    // returning SQL NULL). variant(string, bool) — JSON parse with explicit
    // nullOnError flag (Spark try_parse_json analog). variant(bytes, bytes) —
    // construct from binary. Arity-aware overlap check lets arity-1 and arity-2
    // coexist; variant(string) on its own goes through variant(dyn).
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variant",
        CelOverloadDecl.newGlobalOverload(
            "dyn_to_variant",
            "Convert a value to a Variant (runtime dispatches on actual type);"
                + " null propagates",
            SimpleType.DYN, ImmutableList.of(SimpleType.DYN)),
        CelOverloadDecl.newGlobalOverload(
            "string_bool_to_variant",
            "Parse JSON string; nullOnError=true returns CEL null on parse"
                + " failure (Spark try_parse_json analog); false throws.",
            SimpleType.DYN, ImmutableList.of(SimpleType.STRING, SimpleType.BOOL)),
        CelOverloadDecl.newGlobalOverload(
            "bytes_bytes_to_variant",
            "Construct from value + metadata byte arrays",
            VARIANT, ImmutableList.of(SimpleType.BYTES, SimpleType.BYTES))));

    // Type inspection. Both accept DYN first arg so they can be invoked on the
    // tail of a navigation chain (which may produce either a Variant or CEL
    // null). variants.type propagates CEL null; variants.isNull is strict
    // Spark-equivalent (true iff the argument is a Variant whose type tag is
    // NULL — false for CEL null, which represents "path missed").
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.type",
        CelOverloadDecl.newGlobalOverload(
            "variants_type_dyn",
            "Variant type label (string), or null if input is null",
            SimpleType.DYN, ImmutableList.of(SimpleType.DYN))));
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.isNull",
        CelOverloadDecl.newGlobalOverload(
            "variants_isnull_dyn",
            "True iff input is a Variant whose top type is NULL; false for CEL null",
            SimpleType.BOOL, ImmutableList.of(SimpleType.DYN))));

    // Navigation. Three overload arities per function (path/field/elem):
    //   2-arg: returns sub-Variant or CEL null on miss
    //   3-arg: navigate + typed extraction, strict (Spark variant_get(v, p, t))
    //   4-arg: 3-arg + nullOnError flag (true → Spark try_variant_get)
    // Miss always → CEL null (regardless of nullOnError). Detect via `... == null`.
    // Explicit JSON null at path → variant with type=NULL; detect via variants.isNull.
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.path",
        CelOverloadDecl.newGlobalOverload(
            "variants_path_dyn_string",
            "JSONPath subset navigation; missing path → CEL null;"
                + " explicit JSON null → variant-null; malformed path throws.",
            SimpleType.DYN, ImmutableList.of(SimpleType.DYN, SimpleType.STRING)),
        CelOverloadDecl.newGlobalOverload(
            "variants_path_dyn_string_string",
            "Navigate + extract typed value (strict, throws on cast failure).",
            SimpleType.DYN,
            ImmutableList.of(SimpleType.DYN, SimpleType.STRING, SimpleType.STRING)),
        CelOverloadDecl.newGlobalOverload(
            "variants_path_dyn_string_string_bool",
            "Navigate + extract typed value; nullOnError=true returns CEL null"
                + " on cast failure (Spark try_variant_get analog).",
            SimpleType.DYN,
            ImmutableList.of(SimpleType.DYN, SimpleType.STRING,
                SimpleType.STRING, SimpleType.BOOL))));
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.field",
        CelOverloadDecl.newGlobalOverload(
            "variants_field_dyn_string",
            "Object field by name; missing → CEL null; explicit JSON null →"
                + " variant-null.",
            SimpleType.DYN, ImmutableList.of(SimpleType.DYN, SimpleType.STRING)),
        CelOverloadDecl.newGlobalOverload(
            "variants_field_dyn_string_string",
            "Field + extract typed value (strict, throws on cast failure).",
            SimpleType.DYN,
            ImmutableList.of(SimpleType.DYN, SimpleType.STRING, SimpleType.STRING)),
        CelOverloadDecl.newGlobalOverload(
            "variants_field_dyn_string_string_bool",
            "Field + extract typed value; nullOnError=true returns CEL null"
                + " on cast failure.",
            SimpleType.DYN,
            ImmutableList.of(SimpleType.DYN, SimpleType.STRING,
                SimpleType.STRING, SimpleType.BOOL))));
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.elem",
        CelOverloadDecl.newGlobalOverload(
            "variants_elem_dyn_int",
            "Array element by index; out-of-bounds → CEL null; explicit JSON"
                + " null at index → variant-null.",
            SimpleType.DYN, ImmutableList.of(SimpleType.DYN, SimpleType.INT)),
        CelOverloadDecl.newGlobalOverload(
            "variants_elem_dyn_int_string",
            "Element + extract typed value (strict, throws on cast failure).",
            SimpleType.DYN,
            ImmutableList.of(SimpleType.DYN, SimpleType.INT, SimpleType.STRING)),
        CelOverloadDecl.newGlobalOverload(
            "variants_elem_dyn_int_string_bool",
            "Element + extract typed value; nullOnError=true returns CEL null"
                + " on cast failure.",
            SimpleType.DYN,
            ImmutableList.of(SimpleType.DYN, SimpleType.INT,
                SimpleType.STRING, SimpleType.BOOL))));

    // Standalone typed extraction (when you already have a Variant value).
    //   variants.as(v, t)         — strict, throws on cast failure
    //   variants.as(v, t, nullOnError) — soft if true (Spark try_variant_get
    //                                    on root path)
    // CEL-null input propagates to CEL null in both forms.
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.as",
        CelOverloadDecl.newGlobalOverload(
            "variants_as_dyn_string",
            "Extract a typed value from a Variant; throws on type mismatch;"
                + " null in → null out.",
            SimpleType.DYN, ImmutableList.of(SimpleType.DYN, SimpleType.STRING)),
        CelOverloadDecl.newGlobalOverload(
            "variants_as_dyn_string_bool",
            "Extract a typed value; nullOnError=true returns CEL null on type"
                + " mismatch (Spark try_variant_get).",
            SimpleType.DYN,
            ImmutableList.of(SimpleType.DYN, SimpleType.STRING, SimpleType.BOOL))));

    // variants.toJson(Variant) — serialize a Variant to its JSON string form.
    // Replaces the prior string(Variant) extension overload; namespaced for
    // discoverability and to avoid extending stdlib string(...).
    decls.add(CelFunctionDecl.newFunctionDeclaration(
        "variants.toJson",
        CelOverloadDecl.newGlobalOverload(
            "variants_tojson_variant",
            "Serialize a Variant to its JSON string form",
            SimpleType.STRING, ImmutableList.of(VARIANT))));
  }

  private static String overloadId(String functionName, String suffix) {
    // Overload IDs are lowercase by cel-java convention (string_char_at_int,
    // list_sets_contains_list, etc.) and we lowercase here so the binding-side
    // IDs in BuiltinOverload (also lowercase) match the decl-side IDs exactly.
    return (functionName + "_" + suffix)
        .replace('.', '_')
        .toLowerCase(java.util.Locale.ROOT);
  }
}
