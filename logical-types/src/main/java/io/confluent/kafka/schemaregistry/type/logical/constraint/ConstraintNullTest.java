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

import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator.EMIT_VCTX;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * The test that a CHECK expression is NULL, from its inputs rather than its value. Almost every
 * SQL operator and function is NULL exactly when an input is, so such an expression is NULL when
 * one of its nullable columns is, and need not be evaluated (which, on a NULL input, fails) to
 * tell. COALESCE, GREATEST and LEAST are NULL when all their arguments are.
 *
 * <p>{@code "false"} means the expression is never NULL. Null means it can't be told without
 * evaluating it: NULLIF, CASE, AND, OR, BETWEEN, an IN with an element that may be NULL, a
 * subscript, a quantifier, or a VARIANT function or cast.
 */
final class ConstraintNullTest {

  static final String NEVER = "false";

  // NULL exactly when an argument is.
  private static final Set<String> STRICT_FUNCS = Set.of(
      "LENGTH", "UPPER", "LOWER", "REPLACE", "STARTS_WITH", "ENDS_WITH", "CONTAINS", "MATCHES",
      "IS_EMAIL", "IS_HOSTNAME", "IS_IPV4", "IS_IPV6", "IS_URI", "IS_URI_REF", "IS_UUID",
      "ABS", "SIGN", "FLOOR", "CEIL", "CEILING", "SQRT", "ROUND", "TRUNCATE");

  // NULL exactly when every argument is.
  private static final Set<String> NULL_IF_ALL_FUNCS = Set.of("COALESCE", "GREATEST", "LEAST");

  private ConstraintNullTest() {
  }

  static String of(LogicalTypesParser.Check_exprContext ctx) {
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_unary_notContext not = and.check_expr_unary_not(0);
    while (not instanceof LogicalTypesParser.CheckExprNotContext) {
      not = ((LogicalTypesParser.CheckExprNotContext) not).check_expr_unary_not();
    }
    LogicalTypesParser.Check_expr_isnullContext is =
        ((LogicalTypesParser.CheckExprNotPassContext) not).check_expr_isnull();
    return is.IS() != null ? NEVER : of(is.check_expr_compare());
  }

  static String of(LogicalTypesParser.Check_expr_compareContext ctx) {
    List<String> tests = new ArrayList<>();
    for (LogicalTypesParser.Check_expr_betweenContext between : ctx.check_expr_between()) {
      if (between.BETWEEN() != null) {
        return null;
      }
      LogicalTypesParser.Check_expr_inContext in = between.check_expr_in(0);
      if (in.IN() != null && !elementsNeverNull(in)) {
        return null;
      }
      // An IN over elements never NULL is NULL exactly when its subject is.
      tests.add(of(in.check_expr_like()));
    }
    return any(tests);
  }

  static String of(LogicalTypesParser.Check_expr_likeContext like) {
    // A LIKE pattern is a literal.
    List<String> tests = new ArrayList<>();
    for (LogicalTypesParser.Check_expr_mulContext mul : like.check_expr_add().check_expr_mul()) {
      for (LogicalTypesParser.Check_expr_unary_signContext sign : mul.check_expr_unary_sign()) {
        tests.add(of(sign.c_expr()));
      }
    }
    return any(tests);
  }

  private static String of(LogicalTypesParser.C_exprContext c) {
    if (c instanceof LogicalTypesParser.CheckLiteralContext) {
      return ((LogicalTypesParser.CheckLiteralContext) c).literal().NULL() != null
          ? "true" : NEVER;
    }
    if (c instanceof LogicalTypesParser.CheckCaseContext) {
      return null;
    }
    if (c instanceof LogicalTypesParser.CheckFuncContext) {
      return of(((LogicalTypesParser.CheckFuncContext) c).func_expr());
    }
    if (c instanceof LogicalTypesParser.CheckParenContext
        && ((LogicalTypesParser.CheckParenContext) c).indirection() == null) {
      return of(((LogicalTypesParser.CheckParenContext) c).check_expr());
    }
    return ofSelection(c);
  }

  private static String of(LogicalTypesParser.Func_exprContext f) {
    if (f.func_application() != null) {
      String name = f.func_application().identifier().getText().toUpperCase(Locale.ROOT);
      boolean strict = STRICT_FUNCS.contains(name);
      if (!strict && !NULL_IF_ALL_FUNCS.contains(name)) {
        return null;
      }
      List<String> tests = new ArrayList<>();
      if (f.func_application().check_expr_list() != null) {
        for (LogicalTypesParser.Check_exprContext arg
            : f.func_application().check_expr_list().check_expr()) {
          tests.add(of(arg));
        }
      }
      return strict ? any(tests) : all(tests);
    }
    LogicalTypesParser.Func_expr_common_subexprContext sub = f.func_expr_common_subexpr();
    if (sub instanceof LogicalTypesParser.FuncCurrentTimestampContext) {
      return NEVER;
    }
    if (sub instanceof LogicalTypesParser.FuncVariantGetContext
        || sub instanceof LogicalTypesParser.FuncTryVariantGetContext
        || sub instanceof LogicalTypesParser.FuncCastContext && isVariant(
            ((LogicalTypesParser.FuncCastContext) sub).check_expr())) {
      return null;
    }
    // CAST, EXTRACT, SUBSTRING, POSITION and TRIM are NULL when an operand is.
    List<String> tests = new ArrayList<>();
    for (int i = 0; i < sub.getChildCount(); i++) {
      ParseTree child = sub.getChild(i);
      if (child instanceof LogicalTypesParser.Check_exprContext) {
        tests.add(of((LogicalTypesParser.Check_exprContext) child));
      } else if (child instanceof LogicalTypesParser.C_exprContext) {
        tests.add(of((LogicalTypesParser.C_exprContext) child));
      }
    }
    return any(tests);
  }

  /**
   * {@link #of} outside an emission, as during validation.
   */
  static String of(LogicalTypesParser.Check_expr_compareContext ctx,
      ConstraintValidationContext vctx) {
    ConstraintValidationContext emitting = EMIT_VCTX.get();
    EMIT_VCTX.set(vctx);
    try {
      return of(ctx);
    } finally {
      if (emitting == null) {
        EMIT_VCTX.remove();
      } else {
        EMIT_VCTX.set(emitting);
      }
    }
  }

  /**
   * The test that {@code test}'s expression is not NULL.
   */
  static String not(String test) {
    if (NEVER.equals(test)) {
      return "true";
    }
    if (test.startsWith("dyn(") && test.endsWith(") == null")
        && closingParen(test, 3) == test.length() - 9) {
      return test.substring(0, test.length() - 8) + " != null";
    }
    return test.startsWith("(") ? "!" + test : "!(" + test + ")";
  }

  /**
   * The test that either of two expressions is NULL.
   */
  static String either(String a, String b) {
    return any(Arrays.asList(a, b));
  }

  /**
   * True if {@code arg} is a COALESCE, GREATEST or LEAST, whose CEL gives NULL as null.
   */
  static boolean givesNullAsNull(LogicalTypesParser.Check_exprContext arg) {
    LogicalTypesParser.C_exprContext c = ConstraintResolver.leafPrimary(arg);
    while (c instanceof LogicalTypesParser.CheckParenContext
        && ((LogicalTypesParser.CheckParenContext) c).indirection() == null) {
      c = ConstraintResolver.leafPrimary(((LogicalTypesParser.CheckParenContext) c).check_expr());
    }
    if (!(c instanceof LogicalTypesParser.CheckFuncContext)) {
      return false;
    }
    LogicalTypesParser.Func_applicationContext app =
        ((LogicalTypesParser.CheckFuncContext) c).func_expr().func_application();
    return app != null
        && NULL_IF_ALL_FUNCS.contains(app.identifier().getText().toUpperCase(Locale.ROOT));
  }

  // The index of the parenthesis closing the one at open.
  private static int closingParen(String s, int open) {
    int depth = 0;
    for (int i = open; i < s.length(); i++) {
      if (s.charAt(i) == '(') {
        depth++;
      } else if (s.charAt(i) == ')' && --depth == 0) {
        return i;
      }
    }
    return -1;
  }

  // A column, a macro variable or a field of either, by its nullable steps.
  private static String ofSelection(LogicalTypesParser.C_exprContext c) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    List<String> path = ConstraintResolver.selectionPathCExpr(c);
    List<Integer> steps = path == null ? null : ConstraintResolver.nullableSteps(path, vctx);
    if (steps == null) {
      return null;
    }
    if (steps.isEmpty()) {
      return NEVER;
    }
    StringBuilder emitted = new StringBuilder();
    ConstraintEmitter.visitCExpr(c, emitted);
    return ConstraintResolver.isHasCompatibleCExpr(c, vctx)
        ? ConstraintEmitter.nullTest(path, emitted.toString(), true)
        : "dyn(" + emitted + ") == null";
  }

  private static boolean isVariant(LogicalTypesParser.Check_exprContext operand) {
    Schema type = ConstraintResolver.tryResolveCheckExprType(operand, EMIT_VCTX.get());
    return type == null || type.getType() == Schema.Type.VARIANT;
  }

  private static boolean elementsNeverNull(LogicalTypesParser.Check_expr_inContext in) {
    if (!(in.in_target() instanceof LogicalTypesParser.InTargetParenListContext)) {
      return false;
    }
    for (LogicalTypesParser.Check_exprContext item
        : ((LogicalTypesParser.InTargetParenListContext) in.in_target())
            .check_expr_list().check_expr()) {
      if (!NEVER.equals(of(item))) {
        return false;
      }
    }
    return true;
  }

  // NULL when any input is.
  static String any(List<String> tests) {
    if (tests.contains(null)) {
      return null;
    }
    if (tests.contains("true")) {
      return "true";
    }
    Set<String> nullable = new LinkedHashSet<>(tests);
    nullable.remove(NEVER);
    if (nullable.isEmpty()) {
      return NEVER;
    }
    return nullable.size() == 1 ? nullable.iterator().next()
        : "(" + String.join(" || ", nullable) + ")";
  }

  // NULL when every input is.
  private static String all(List<String> tests) {
    if (tests.contains(null)) {
      return null;
    }
    if (tests.isEmpty() || tests.contains(NEVER)) {
      return NEVER;
    }
    Set<String> nullable = new LinkedHashSet<>(tests);
    nullable.remove("true");
    if (nullable.isEmpty()) {
      return "true";
    }
    return nullable.size() == 1 ? nullable.iterator().next()
        : "(" + String.join(" && ", nullable) + ")";
  }
}
