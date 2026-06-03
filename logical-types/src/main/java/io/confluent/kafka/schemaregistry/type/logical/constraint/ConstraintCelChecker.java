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

import com.google.common.collect.ImmutableList;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOptions;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.CelValidationException;
import dev.cel.common.CelVarDecl;
import dev.cel.common.types.CelType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.extensions.CelExtensions;
import dev.cel.parser.CelStandardMacro;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.BuiltinDeclarations;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.util.ArrayList;
import java.util.List;

/**
 * Strict Google cel-java type-checker for emitted CEL expressions. Used as
 * the post-emit safety net in {@link ConstraintToCelTranslator#translate}:
 * every constraint translation runs the resulting CEL through cel-java's
 * checker with the schema's actual column types declared (not {@code dyn}),
 * so any type/overload mismatch the hand-coded validator passed missed
 * becomes a parse-time {@link ValidationException} instead of a runtime CEL
 * error downstream.
 *
 * <p>The strict checker uses {@link ConstraintTypeProvider} to resolve
 * {@code this.<field>} accesses against the LT schema. CEL standard
 * functions are loaded via {@link CelCompilerFactory#standardCelCompilerBuilder()}.
 * Extension libraries:
 * <ul>
 *   <li>{@link CelExtensions#strings()} — upperAscii, lowerAscii, replace,
 *       substring, indexOf, trim, etc.</li>
 *   <li>{@link CelExtensions#math(CelOptions)} — math.abs, math.round,
 *       math.trunc, math.floor, math.ceil, math.bitAnd, math.bitOr, math.sqrt,
 *       etc.</li>
 * </ul>
 *
 * <p>Extended function-family decls (Decimal / Timestamp.of / Variant /
 * isEmail / isHostname / isIpv4 / isIpv6 / isUri / isUriRef / isUuid) come from
 * schema-rules' {@link BuiltinDeclarations#create()} — a single source of
 * truth shared with schema-rules' runtime compiler. Decls-only on this side
 * (no runtime bindings); emitted CEL is consumed by downstream runtimes
 * (cel-go etc.) which register their own bindings.
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
    CelCompiler compiler = newStrictCompiler(vctx);
    try {
      compiler.compile(cel).getAst();
    } catch (CelValidationException e) {
      throw new ValidationException(
          "Emitted expression fails strict type-check. "
              + sourceContext(sourceSql, cel) + e.getMessage());
    }
  }

  private static String sourceContext(String sourceSql, String cel) {
    StringBuilder sb = new StringBuilder();
    if (sourceSql != null) {
      sb.append("SQL: ").append(sourceSql).append("\n  ");
    }
    sb.append("Expr: ").append(cel).append("\n  ");
    return sb.toString();
  }

  /**
   * Build a strict compiler from {@code vctx}. {@code this} is declared with
   * the appropriate type:
   * <ul>
   *   <li>Column-level CHECK: {@code this} is the field's CEL primitive
   *       type (or a synthetic struct type if the column is a STRUCT).</li>
   *   <li>Table-level CHECK: {@code this} is a synthetic struct type
   *       resolved by {@link ConstraintTypeProvider} to the column-table
   *       fields.</li>
   * </ul>
   */
  private static CelCompiler newStrictCompiler(ConstraintValidationContext vctx) {
    ConstraintTypeProvider provider = new ConstraintTypeProvider(vctx);
    CelType thisType;
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
      thisType = StructTypeReference.create(ConstraintTypeProvider.ROOT_TYPE_NAME);
    }
    List<CelVarDecl> varDecls = new ArrayList<>();
    varDecls.add(CelVarDecl.newVarDeclaration("this", thisType));
    varDecls.add(CelVarDecl.newVarDeclaration("now", SimpleType.TIMESTAMP));
    List<CelFunctionDecl> funcDecls = new ArrayList<>(BuiltinDeclarations.create());
    // Bracket-access overload: emit uses `this["fieldname"]` for fields whose
    // names are CEL-reserved words (`in`, `null`, etc.). Standard `_[_]` is
    // declared for map and list only; declare a permissive overload that
    // accepts our synthetic root type indexed by string and returns dyn.
    // Strict-check only — no runtime binding (production evaluates against
    // the actual data value, which supports bracket-access natively for
    // maps/messages in cel-go).
    if (!vctx.isColumnLevel()) {
      funcDecls.add(CelFunctionDecl.newFunctionDeclaration(
          "_[_]",
          CelOverloadDecl.newGlobalOverload(
              "index_lt_root_string",
              SimpleType.DYN,
              ImmutableList.of(
                  StructTypeReference.create(ConstraintTypeProvider.ROOT_TYPE_NAME),
                  SimpleType.STRING))));
    }
    // STANDARD_MACROS enables has(), all(), exists(), exists_one(), map(),
    // filter() — without it, has(this.x) fails as an undeclared reference.
    // Google cel-java's standardCelCompilerBuilder() omits these by default.
    return CelCompilerFactory.standardCelCompilerBuilder()
        .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
        .addLibraries(
            CelExtensions.strings(),
            CelExtensions.math(CelOptions.DEFAULT))
        .addVarDeclarations(varDecls)
        .addFunctionDeclarations(funcDecls)
        .setTypeProvider(provider)
        .build();
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
