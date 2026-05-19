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

package io.confluent.kafka.schemaregistry.type.logical.constraint;

import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Type;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import org.projectnessie.cel.Env;
import org.projectnessie.cel.EnvOption;
import org.projectnessie.cel.Library;
import org.projectnessie.cel.checker.Decls;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Strict cel-java type-checker for emitted CEL expressions. Used as the
 * post-emit safety net in {@link ConstraintToCelTranslator#translate}: every
 * constraint translation runs the resulting CEL through cel-java's checker
 * with the schema's actual column types declared (not {@code dyn}), so any
 * type/overload mismatch the hand-coded validator passes missed becomes a
 * parse-time {@link ValidationException} instead of a runtime CEL error
 * downstream.
 *
 * <p>The strict checker uses {@link ConstraintTypeProvider} to resolve
 * {@code this.<field>} accesses against the LT schema. CEL stdlib functions
 * are loaded via {@link Library#StdLib()}; extension methods our emit uses
 * (upperAscii, lowerAscii, trim, substring, indexOf, replace) are declared
 * here since nessie's stdlib doesn't ship them. Protovalidate-style format
 * validators (isEmail, isHostname, isIpv4, isIpv6, isUri, isUriRef, isUuid)
 * are also declared.
 *
 * <p>{@code now} is declared as {@code timestamp} for the
 * CURRENT_TIMESTAMP runtime variable our emit references.
 *
 * <p>This class is reachable from production code; the test-side
 * {@code CelValidator} delegates to it for symmetry.
 */
final class ConstraintCelChecker {

  private ConstraintCelChecker() {
    // static utility
  }

  /**
   * Strict-check {@code cel} against the given validation context. Throws
   * {@link ValidationException} when cel-java's checker rejects the
   * expression — message includes the CEL error and (when provided) the
   * source SQL for context. No-op for {@code vctx == null} (the public
   * translate() entry rejects null vctx anyway, but this guards programmatic
   * callers).
   *
   * @param cel emitted CEL expression to check
   * @param vctx validation context — used for type lookups and to determine
   *     whether {@code this} is a struct (table-level) or a primitive value
   *     (column-level)
   * @param sourceSql original SQL CHECK expression, included in error
   *     messages for user context; may be null
   */
  static void strictCheck(
      String cel, ConstraintValidationContext vctx, String sourceSql) {
    if (vctx == null) {
      return;
    }
    Env env = newStrictEnv(vctx);
    Env.AstIssuesTuple parsed = env.parse(cel);
    if (parsed.hasIssues()) {
      throw new ValidationException(
          "Emitted CEL fails parsing — internal translator bug. "
              + sourceContext(sourceSql, cel) + parsed.getIssues());
    }
    Env.AstIssuesTuple checked;
    try {
      checked = env.check(parsed.getAst());
    } catch (org.projectnessie.cel.common.types.Err.ErrException e) {
      throw new ValidationException(
          "Emitted CEL fails strict type-check. "
              + sourceContext(sourceSql, cel) + e.getMessage());
    }
    if (checked.hasIssues()) {
      throw new ValidationException(
          "Emitted CEL fails strict type-check. "
              + sourceContext(sourceSql, cel) + checked.getIssues());
    }
  }

  private static String sourceContext(String sourceSql, String cel) {
    StringBuilder sb = new StringBuilder();
    if (sourceSql != null) {
      sb.append("SQL: ").append(sourceSql).append("\n  ");
    }
    sb.append("CEL: ").append(cel).append("\n  ");
    return sb.toString();
  }

  /**
   * Build a strict env from {@code vctx}. {@code this} is declared with
   * the appropriate type:
   * <ul>
   *   <li>Column-level CHECK: {@code this} is the field's CEL primitive
   *       type (or a synthetic object type if the column is a STRUCT).</li>
   *   <li>Table-level CHECK: {@code this} is a synthetic object type
   *       resolved by {@link ConstraintTypeProvider} to the column-table
   *       fields.</li>
   * </ul>
   */
  private static Env newStrictEnv(ConstraintValidationContext vctx) {
    ConstraintTypeProvider provider = new ConstraintTypeProvider(vctx);
    Type thisType;
    if (vctx.isColumnLevel()) {
      // Column-level: `this` is the single column's value. Walk the
      // context's column map (it has exactly one entry) to find it.
      Schema columnSchema = singleColumnSchema(vctx);
      thisType = provider.celTypeFor(columnSchema);
    } else {
      // Table-level: `this` is the surrounding struct. Build a synthetic
      // root with the column-table fields.
      Schema rootStruct = buildRootStruct(vctx);
      provider.registerRoot(rootStruct);
      thisType = Decls.newObjectType(ConstraintTypeProvider.ROOT_TYPE_NAME);
    }
    List<Decl> decls = new ArrayList<>();
    decls.add(Decls.newVar("this", thisType));
    decls.addAll(commonDeclarations());
    // Bracket-access overload: emit uses `this["fieldname"]` for fields
    // whose names are CEL-reserved words (`in`, `null`, etc.). nessie's
    // stdlib `_[_]` is defined for map and list, not custom object types,
    // so we declare a permissive overload that accepts our synthetic root
    // type indexed by string and returns dyn (the field type).
    if (!vctx.isColumnLevel()) {
      decls.add(Decls.newFunction("_[_]",
          Decls.newOverload("index_lt_root_string",
              Arrays.asList(Decls.newObjectType(
                  ConstraintTypeProvider.ROOT_TYPE_NAME), Decls.String),
              Decls.Dyn)));
    }
    return Env.newCustomEnv(provider, Arrays.asList(
        Library.StdLib(),
        EnvOption.declarations(decls.toArray(new Decl[0]))));
  }

  /**
   * Common variable + function declarations layered on top of CEL stdlib:
   * <ul>
   *   <li>{@code now} (Timestamp) for {@code CURRENT_TIMESTAMP} emit</li>
   *   <li>String extension methods our emit uses but nessie's stdlib
   *       doesn't ship (upperAscii, lowerAscii, trim, substring, indexOf,
   *       replace)</li>
   *   <li>Protovalidate-style format validators (isEmail, isHostname,
   *       isIpv4, isIpv6, isUri, isUriRef, isUuid)</li>
   * </ul>
   * Nessie stdlib already declares {@code contains}, {@code endsWith},
   * {@code matches}, {@code startsWith}, {@code size} and the timestamp
   * accessors ({@code getFullYear} etc. via {@code timestamp_to_*}); we
   * skip those to avoid overlapping-overload errors.
   */
  static List<Decl> commonDeclarations() {
    List<Decl> decls = new ArrayList<>();
    decls.add(Decls.newVar("now", Decls.Timestamp));
    for (String name : Arrays.asList(
        "isEmail", "isHostname", "isIpv4", "isIpv6",
        "isUri", "isUriRef", "isUuid")) {
      decls.add(Decls.newFunction(name, Decls.newInstanceOverload(
          name + "_string",
          Collections.singletonList(Decls.String), Decls.Bool)));
    }
    decls.add(Decls.newFunction("upperAscii", Decls.newInstanceOverload(
        "upperAscii_string",
        Collections.singletonList(Decls.String), Decls.String)));
    decls.add(Decls.newFunction("lowerAscii", Decls.newInstanceOverload(
        "lowerAscii_string",
        Collections.singletonList(Decls.String), Decls.String)));
    decls.add(Decls.newFunction("trim", Decls.newInstanceOverload(
        "trim_string",
        Collections.singletonList(Decls.String), Decls.String)));
    decls.add(Decls.newFunction("substring",
        Decls.newInstanceOverload("substring_string_int",
            Arrays.asList(Decls.String, Decls.Int), Decls.String),
        Decls.newInstanceOverload("substring_string_int_int",
            Arrays.asList(Decls.String, Decls.Int, Decls.Int), Decls.String)));
    decls.add(Decls.newFunction("indexOf", Decls.newInstanceOverload(
        "indexOf_string_string",
        Arrays.asList(Decls.String, Decls.String), Decls.Int)));
    decls.add(Decls.newFunction("replace", Decls.newInstanceOverload(
        "replace_string_string_string",
        Arrays.asList(Decls.String, Decls.String, Decls.String), Decls.String)));
    return decls;
  }

  private static Schema singleColumnSchema(ConstraintValidationContext vctx) {
    // Column-level vctx has exactly one schema column registered.
    for (java.util.Map.Entry<String, Schema> e : vctx.columns().entrySet()) {
      Schema s = e.getValue();
      if (s != null) {
        return s;
      }
    }
    throw new IllegalStateException(
        "Column-level vctx has no schema column — internal bug.");
  }

  private static Schema buildRootStruct(ConstraintValidationContext vctx) {
    List<Schema.Field> fields = new ArrayList<>();
    int pos = 0;
    for (java.util.Map.Entry<String, Schema> e : vctx.columns().entrySet()) {
      Schema s = e.getValue();
      if (s == null) {
        continue;
      }
      fields.add(new Schema.Field(e.getKey(), s, pos++));
    }
    return Schema.createStruct(fields);
  }
}
