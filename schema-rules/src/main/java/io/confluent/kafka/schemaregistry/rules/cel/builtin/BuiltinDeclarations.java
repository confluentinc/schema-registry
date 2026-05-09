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

import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.pb.Checked;

final class BuiltinDeclarations {

  private static final Type DECIMAL = Decls.newObjectType(CelTypes.DECIMAL);
  private static final Type TIMESTAMP = Checked.checkedTimestamp;
  private static final Type VARIANT = Decls.newObjectType(CelTypes.VARIANT);

  static List<Decl> create() {
    List<Decl> decls = new ArrayList<>();

    decls.add(
        Decls.newFunction(
            "isEmail",
            Decls.newInstanceOverload(
                "is_email", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isHostname",
            Decls.newInstanceOverload(
                "is_hostname", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isIpv4",
            Decls.newInstanceOverload(
                "is_ipv4", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isIpv6",
            Decls.newInstanceOverload(
                "is_ipv6", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isUriRef",
            Decls.newInstanceOverload(
                "is_uri_ref", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isUri",
            Decls.newInstanceOverload(
                "is_uri", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isUuid",
            Decls.newInstanceOverload(
                "is_uuid", Collections.singletonList(Decls.String), Decls.Bool)));

    addDecimal(decls);
    addTimestamp(decls);
    addVariant(decls);

    return Collections.unmodifiableList(decls);
  }

  private static void addDecimal(List<Decl> decls) {
    decls.add(
        Decls.newFunction(
            "to_decimal",
            // Single-arg overload accepts dyn; the runtime dispatches on actual type
            // (String → parse, BigDecimal → identity, proto Decimal → decode, etc.).
            // Nessie's checker treats (dyn) and (string) as overlapping, so we don't
            // declare a separate string overload — the runtime handles it.
            Decls.newOverload(
                "to_decimal_dyn", Collections.singletonList(Decls.Dyn), DECIMAL),
            Decls.newOverload(
                "to_decimal_bytes", Arrays.asList(Decls.Bytes, Decls.Int), DECIMAL)));

    binaryDecimal(decls, "decimal_eq", Decls.Bool);
    binaryDecimal(decls, "decimal_ne", Decls.Bool);
    binaryDecimal(decls, "decimal_lt", Decls.Bool);
    binaryDecimal(decls, "decimal_le", Decls.Bool);
    binaryDecimal(decls, "decimal_gt", Decls.Bool);
    binaryDecimal(decls, "decimal_ge", Decls.Bool);
    binaryDecimal(decls, "decimal_add", DECIMAL);
    binaryDecimal(decls, "decimal_sub", DECIMAL);
    binaryDecimal(decls, "decimal_mul", DECIMAL);
    binaryDecimal(decls, "decimal_div", DECIMAL);

    unaryDecimal(decls, "decimal_neg", DECIMAL);
    unaryDecimal(decls, "decimal_abs", DECIMAL);
    unaryDecimal(decls, "decimal_scale", Decls.Int);
    unaryDecimal(decls, "decimal_precision", Decls.Int);
    unaryDecimal(decls, "decimal_to_string", Decls.String);

    // Flink-aligned rounding family. ROUND and TRUNCATE accept either 1 arg
    // (scale 0) or 2 args (specific scale), matching Flink SQL exactly. The
    // multi-arity overloads must coalesce under the bare function name due to
    // the Nessie planner quirk — see "Nessie cel-java workarounds" section.
    decls.add(
        Decls.newFunction(
            "decimal_round",
            Decls.newOverload(
                "decimal_round_unary",
                Collections.singletonList(DECIMAL),
                DECIMAL),
            Decls.newOverload(
                "decimal_round_binary",
                Arrays.asList(DECIMAL, Decls.Int),
                DECIMAL)));
    decls.add(
        Decls.newFunction(
            "decimal_truncate",
            Decls.newOverload(
                "decimal_truncate_unary",
                Collections.singletonList(DECIMAL),
                DECIMAL),
            Decls.newOverload(
                "decimal_truncate_binary",
                Arrays.asList(DECIMAL, Decls.Int),
                DECIMAL)));
    unaryDecimal(decls, "decimal_floor", DECIMAL);
    unaryDecimal(decls, "decimal_ceil", DECIMAL);
    unaryDecimal(decls, "decimal_sign", Decls.Int);
  }

  private static void addTimestamp(List<Decl> decls) {
    decls.add(
        Decls.newFunction(
            "to_timestamp",
            // Single-arg overload accepts dyn; runtime dispatches on String / Instant /
            // Timestamp / etc. Same Nessie-checker constraint as to_decimal.
            Decls.newOverload(
                "to_timestamp_dyn", Collections.singletonList(Decls.Dyn), TIMESTAMP),
            Decls.newOverload(
                "to_timestamp_int_unit",
                Arrays.asList(Decls.Int, Decls.String),
                TIMESTAMP)));
  }

  private static void addVariant(List<Decl> decls) {
    decls.add(
        Decls.newFunction(
            "to_variant",
            Decls.newOverload(
                "to_variant_dyn", Collections.singletonList(Decls.Dyn), VARIANT),
            Decls.newOverload(
                "to_variant_bytes", Arrays.asList(Decls.Bytes, Decls.Bytes), VARIANT)));

    decls.add(
        Decls.newFunction(
            "variant_type",
            Decls.newOverload(
                "variant_type", Collections.singletonList(VARIANT), Decls.String)));
    decls.add(
        Decls.newFunction(
            "is_variant_null",
            Decls.newOverload(
                "is_variant_null", Collections.singletonList(VARIANT), Decls.Bool)));
    decls.add(
        Decls.newFunction(
            "variant_get",
            Decls.newOverload(
                "variant_get", Arrays.asList(VARIANT, Decls.String), VARIANT)));
    decls.add(
        Decls.newFunction(
            "variant_get_field",
            Decls.newOverload(
                "variant_get_field", Arrays.asList(VARIANT, Decls.String), VARIANT)));
    decls.add(
        Decls.newFunction(
            "variant_get_element",
            Decls.newOverload(
                "variant_get_element", Arrays.asList(VARIANT, Decls.Int), VARIANT)));
    decls.add(
        Decls.newFunction(
            "variant_get_string",
            Decls.newOverload(
                "variant_get_string", Collections.singletonList(VARIANT), Decls.String)));
    decls.add(
        Decls.newFunction(
            "variant_get_int",
            Decls.newOverload(
                "variant_get_int", Collections.singletonList(VARIANT), Decls.Int)));
    decls.add(
        Decls.newFunction(
            "variant_get_double",
            Decls.newOverload(
                "variant_get_double", Collections.singletonList(VARIANT), Decls.Double)));
    decls.add(
        Decls.newFunction(
            "variant_get_bool",
            Decls.newOverload(
                "variant_get_bool", Collections.singletonList(VARIANT), Decls.Bool)));
    decls.add(
        Decls.newFunction(
            "variant_get_decimal",
            Decls.newOverload(
                "variant_get_decimal", Collections.singletonList(VARIANT), DECIMAL)));
    decls.add(
        Decls.newFunction(
            "variant_get_timestamp",
            Decls.newOverload(
                "variant_get_timestamp",
                Collections.singletonList(VARIANT),
                TIMESTAMP)));
    decls.add(
        Decls.newFunction(
            "variant_get_binary",
            Decls.newOverload(
                "variant_get_binary", Collections.singletonList(VARIANT), Decls.Bytes)));
    decls.add(
        Decls.newFunction(
            "variant_get_uuid",
            Decls.newOverload(
                "variant_get_uuid", Collections.singletonList(VARIANT), Decls.String)));
    decls.add(
        Decls.newFunction(
            "variant_to_json",
            Decls.newOverload(
                "variant_to_json", Collections.singletonList(VARIANT), Decls.String)));

    // try_variant_get_* family: type-check filters that return the input Variant
    // when the type matches, else a NULL Variant. Callers compose with
    // is_variant_null to handle missing/mistyped paths without throwing.
    tryVariantUnary(decls, "try_variant_get_string");
    tryVariantUnary(decls, "try_variant_get_int");
    tryVariantUnary(decls, "try_variant_get_double");
    tryVariantUnary(decls, "try_variant_get_bool");
    tryVariantUnary(decls, "try_variant_get_decimal");
    tryVariantUnary(decls, "try_variant_get_timestamp");
    tryVariantUnary(decls, "try_variant_get_binary");
    tryVariantUnary(decls, "try_variant_get_uuid");
    decls.add(
        Decls.newFunction(
            "try_variant_get",
            Decls.newOverload(
                "try_variant_get", Arrays.asList(VARIANT, Decls.String), VARIANT)));
    decls.add(
        Decls.newFunction(
            "try_variant_get_field",
            Decls.newOverload(
                "try_variant_get_field",
                Arrays.asList(VARIANT, Decls.String),
                VARIANT)));
    decls.add(
        Decls.newFunction(
            "try_variant_get_element",
            Decls.newOverload(
                "try_variant_get_element",
                Arrays.asList(VARIANT, Decls.Int),
                VARIANT)));
  }

  private static void tryVariantUnary(List<Decl> decls, String name) {
    decls.add(
        Decls.newFunction(
            name,
            Decls.newOverload(name, Collections.singletonList(VARIANT), VARIANT)));
  }

  private static void binaryDecimal(List<Decl> decls, String name, Type result) {
    decls.add(
        Decls.newFunction(
            name,
            Decls.newOverload(name, Arrays.asList(DECIMAL, DECIMAL), result)));
  }

  private static void unaryDecimal(List<Decl> decls, String name, Type result) {
    decls.add(
        Decls.newFunction(
            name,
            Decls.newOverload(name, Collections.singletonList(DECIMAL), result)));
  }
}
