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
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * The test that a CHECK expression is NULL, from its inputs rather than its value. Almost every
 * SQL operator and function is NULL exactly when an input is, so such an expression is NULL when
 * one of its nullable columns is, and need not be evaluated (which, on a NULL input, fails) to
 * tell. COALESCE is NULL when all its arguments are.
 *
 * <p>CASE, NULLIF and an IN with an element that may be NULL give NULL as null, so are tested by
 * their value; an IN over an array of elements never NULL is NULL when its subject or array is.
 * AND, OR, BETWEEN, EVERY and ANY are NULL when neither definitely TRUE nor definitely FALSE. A
 * subscript is NULL when a step, its index or its element is, or its key or index is missing.
 * VARIANT_GET, TRY_VARIANT_GET and TRY_PARSE_JSON are NULL when an input is or their CEL gives
 * null. {@link #NEVER} means the expression is never NULL, {@link #ALWAYS} always. Null means it
 * can't be told without evaluating it: ONE, VARIANT_IS_NULL, a CAST of a VARIANT, an IN whose
 * inputs or array type don't tell, or any of these over one.
 */
final class ConstraintNullTest {

  static final CelNode NEVER = CelNode.FALSE;
  static final CelNode ALWAYS = CelNode.TRUE;

  // NULL exactly when an argument is.
  private static final Set<String> STRICT_FUNCS = Set.of(
      "LENGTH", "UPPER", "LOWER", "REPLACE", "STARTS_WITH", "ENDS_WITH", "CONTAINS", "MATCHES",
      "IS_EMAIL", "IS_HOSTNAME", "IS_IPV4", "IS_IPV6", "IS_URI", "IS_URI_REF", "IS_UUID",
      "ABS", "SIGN", "FLOOR", "CEIL", "CEILING", "SQRT", "ROUND", "TRUNCATE",
      "GREATEST", "LEAST", "PARSE_JSON", "TO_JSON", "TYPE_OF_VARIANT");

  // Their CEL gives NULL as null.
  private static final Set<String> NULL_AS_NULL_FUNCS = Set.of("COALESCE", "GREATEST", "LEAST");

  private ConstraintNullTest() {
  }

  static CelNode of(LogicalTypesParser.Check_exprContext ctx) {
    return ConstraintEmitter.cachedNullTest(ctx, () -> ofExpr(ctx));
  }

  static CelNode of(LogicalTypesParser.Check_expr_compareContext ctx) {
    List<CelNode> tests = new ArrayList<>();
    for (LogicalTypesParser.Check_expr_betweenContext between : ctx.check_expr_between()) {
      if (between.BETWEEN() != null) {
        tests.add(ofBetween(between));
        continue;
      }
      LogicalTypesParser.Check_expr_inContext in = between.check_expr_in(0);
      CelNode test = in.IN() == null ? of(in.check_expr_like()) : ofIn(in);
      if (test == null && in.IN() != null) {
        if (!ConstraintEmitter.givesNullAsNull(in)) {
          return null;
        }
        tests.add(CelNode.isNull(ConstraintEmitter.visitIn(in)));
        continue;
      }
      tests.add(test);
    }
    return any(tests);
  }

  static CelNode of(LogicalTypesParser.Check_expr_inContext in) {
    return in.IN() == null ? of(in.check_expr_like()) : null;
  }

  static CelNode of(LogicalTypesParser.Check_expr_likeContext like) {
    // A LIKE pattern is a literal.
    List<CelNode> tests = new ArrayList<>();
    for (LogicalTypesParser.Check_expr_mulContext mul : like.check_expr_add().check_expr_mul()) {
      for (LogicalTypesParser.Check_expr_unary_signContext sign : mul.check_expr_unary_sign()) {
        tests.add(of(sign.c_expr()));
      }
    }
    return any(tests);
  }

  private static CelNode of(LogicalTypesParser.C_exprContext c) {
    if (c instanceof LogicalTypesParser.CheckLiteralContext) {
      return ((LogicalTypesParser.CheckLiteralContext) c).literal().NULL() != null
          ? ALWAYS : NEVER;
    }
    if (c instanceof LogicalTypesParser.CheckCaseContext
        && neverNull(((LogicalTypesParser.CheckCaseContext) c).case_expr())) {
      return NEVER;
    }
    if (isTestedByValue(c)) {
      return CelNode.isNull(ConstraintEmitter.visitCExpr(c));
    }
    if (c instanceof LogicalTypesParser.CheckFuncContext) {
      LogicalTypesParser.CheckFuncContext func = (LogicalTypesParser.CheckFuncContext) c;
      if (isQuantifier(func)) {
        // Cached, as each nested quantifier's would otherwise be emitted twice per level.
        return ConstraintEmitter.cachedNullTest(func, () ->
            neither(ConstraintEmitter.emitQuantifierTruth(func, true),
                ConstraintEmitter.emitQuantifierTruth(func, false)));
      }
      List<LogicalTypesParser.Check_exprContext> inputs = valueTestedInputs(func.func_expr());
      if (inputs != null) {
        // NULL when an input is, or when there is no value, which its CEL gives as null.
        List<CelNode> tests = new ArrayList<>();
        for (LogicalTypesParser.Check_exprContext input : inputs) {
          tests.add(of(input));
        }
        tests.add(CelNode.isNull(ConstraintEmitter.visitCExpr(c)));
        return any(tests);
      }
      return of(func.func_expr());
    }
    if (c instanceof LogicalTypesParser.CheckParenContext
        && ((LogicalTypesParser.CheckParenContext) c).indirection() == null) {
      return of(((LogicalTypesParser.CheckParenContext) c).check_expr());
    }
    return ofSelection(c);
  }

  private static CelNode of(LogicalTypesParser.Func_exprContext f) {
    if (f.func_application() != null) {
      String name = f.func_application().identifier().getText().toUpperCase(Locale.ROOT);
      boolean strict = STRICT_FUNCS.contains(name);
      if (!strict && !"COALESCE".equals(name)) {
        return null;
      }
      List<CelNode> tests = new ArrayList<>();
      if (f.func_application().check_expr_list() != null) {
        for (LogicalTypesParser.Check_exprContext arg
            : f.func_application().check_expr_list().check_expr()) {
          tests.add(of(arg));
        }
      }
      return strict ? any(tests) : all(tests);
    }
    return of(f.func_expr_common_subexpr());
  }

  static CelNode of(LogicalTypesParser.Func_expr_common_subexprContext sub) {
    if (sub instanceof LogicalTypesParser.FuncCurrentTimestampContext) {
      return NEVER;
    }
    if (sub instanceof LogicalTypesParser.FuncVariantGetContext
        || sub instanceof LogicalTypesParser.FuncTryVariantGetContext
        || sub instanceof LogicalTypesParser.FuncCastContext && mayBeVariant(
            ((LogicalTypesParser.FuncCastContext) sub).check_expr())) {
      return null;
    }
    // CAST, EXTRACT, SUBSTRING, POSITION and TRIM are NULL when an operand is.
    List<CelNode> tests = new ArrayList<>();
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
  static CelNode of(LogicalTypesParser.Check_expr_compareContext ctx,
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

  private static CelNode ofExpr(LogicalTypesParser.Check_exprContext ctx) {
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1
        || or.check_expr_and(0).check_expr_unary_not().size() > 1) {
      return neither(ConstraintEmitter.emitTruth(or, true),
          ConstraintEmitter.emitTruth(or, false));
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    LogicalTypesParser.Check_expr_unary_notContext not = and.check_expr_unary_not(0);
    while (not instanceof LogicalTypesParser.CheckExprNotContext) {
      not = ((LogicalTypesParser.CheckExprNotContext) not).check_expr_unary_not();
    }
    LogicalTypesParser.Check_expr_isnullContext is =
        ((LogicalTypesParser.CheckExprNotPassContext) not).check_expr_isnull();
    return is.IS() != null ? NEVER : of(is.check_expr_compare());
  }

  /**
   * The test that {@code test}'s expression is not NULL.
   */
  static CelNode not(CelNode test) {
    if (NEVER.equals(test)) {
      return CelNode.TRUE;
    }
    CelNode valueOf = CelNode.isNullOf(test);
    return valueOf != null ? CelNode.isNotNull(valueOf) : CelNode.not(test);
  }

  /**
   * The test that either of two expressions is NULL.
   */
  static CelNode either(CelNode a, CelNode b) {
    return any(Arrays.asList(a, b));
  }

  /**
   * True if {@code ctx} is a CASE or NULLIF, which the input test tells by its value: a caller
   * testing one as a whole tests its value in its own type instead.
   */
  static boolean isTestedByValue(LogicalTypesParser.Check_expr_compareContext ctx) {
    return isTestedByValue(unparen(ConstraintResolver.leafPrimary(ctx)));
  }

  /**
   * {@link #isTestedByValue(LogicalTypesParser.Check_expr_compareContext)} of an argument.
   */
  static boolean isTestedByValue(LogicalTypesParser.Check_exprContext arg) {
    return isTestedByValue(unparen(ConstraintResolver.leafPrimary(arg)));
  }

  private static boolean isTestedByValue(LogicalTypesParser.C_exprContext c) {
    if (c instanceof LogicalTypesParser.CheckCaseContext) {
      return !neverNull(((LogicalTypesParser.CheckCaseContext) c).case_expr());
    }
    if (!(c instanceof LogicalTypesParser.CheckFuncContext)) {
      return false;
    }
    LogicalTypesParser.Func_applicationContext app =
        ((LogicalTypesParser.CheckFuncContext) c).func_expr().func_application();
    return app != null && "NULLIF".equalsIgnoreCase(app.identifier().getText());
  }

  /**
   * True if {@code arg} is a COALESCE, GREATEST or LEAST, whose CEL gives NULL as null.
   */
  static boolean givesNullAsNull(LogicalTypesParser.Check_exprContext arg) {
    LogicalTypesParser.C_exprContext c = unparen(ConstraintResolver.leafPrimary(arg));
    if (!(c instanceof LogicalTypesParser.CheckFuncContext)) {
      return false;
    }
    LogicalTypesParser.Func_applicationContext app =
        ((LogicalTypesParser.CheckFuncContext) c).func_expr().func_application();
    return app != null
        && NULL_AS_NULL_FUNCS.contains(app.identifier().getText().toUpperCase(Locale.ROOT));
  }

  // A CASE with an ELSE whose results are never NULL.
  private static boolean neverNull(LogicalTypesParser.Case_exprContext ctx) {
    boolean afterElse = false;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode && "ELSE".equalsIgnoreCase(child.getText())) {
        afterElse = true;
      } else if (afterElse && child instanceof LogicalTypesParser.Check_exprContext) {
        if (!NEVER.equals(of((LogicalTypesParser.Check_exprContext) child))) {
          return false;
        }
        for (LogicalTypesParser.When_clauseContext when : ctx.when_clause()) {
          if (!NEVER.equals(of(when.check_expr(1)))) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  // c without the parentheses around it.
  private static LogicalTypesParser.C_exprContext unparen(LogicalTypesParser.C_exprContext c) {
    while (c instanceof LogicalTypesParser.CheckParenContext
        && ((LogicalTypesParser.CheckParenContext) c).indirection() == null) {
      c = ConstraintResolver.leafPrimary(((LogicalTypesParser.CheckParenContext) c).check_expr());
    }
    return c;
  }

  // A column, a macro variable or a field of either, by its nullable steps.
  private static CelNode ofSelection(LogicalTypesParser.C_exprContext c) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    // A path has() can't take, (e).x, (arr[1]).x or r.`in`, is tested step by step from its
    // root, so a NULL step above isn't read through.
    if (hasSubscript(c)
        || indirectionOf(c) != null && !ConstraintResolver.isHasCompatibleCExpr(c, vctx)) {
      return ConstraintEmitter.subscriptNullTest(c);
    }
    List<String> path = ConstraintResolver.selectionPathCExpr(c);
    List<Integer> steps = path == null ? null : ConstraintResolver.nullableSteps(path, vctx);
    if (steps == null) {
      return null;
    }
    if (steps.isEmpty()) {
      return NEVER;
    }
    CelNode emitted = ConstraintEmitter.visitCExpr(c);
    return ConstraintResolver.isHasCompatibleCExpr(c, vctx)
        ? ConstraintEmitter.nullTest(path, emitted, true) : CelNode.isNull(emitted);
  }

  private static boolean hasSubscript(LogicalTypesParser.C_exprContext c) {
    LogicalTypesParser.IndirectionContext indirection = indirectionOf(c);
    return indirection != null && indirection.indirection_el().stream()
        .anyMatch(el -> el.check_expr() != null);
  }

  private static LogicalTypesParser.IndirectionContext indirectionOf(
      LogicalTypesParser.C_exprContext c) {
    return c instanceof LogicalTypesParser.CheckColumnRefContext
        ? ((LogicalTypesParser.CheckColumnRefContext) c).columnref().indirection()
        : c instanceof LogicalTypesParser.CheckParenContext
            ? ((LogicalTypesParser.CheckParenContext) c).indirection() : null;
  }

  /**
   * True if {@code func} is EVERY, ANY or ONE, whose three values emitTruth folds over the
   * elements.
   */
  static boolean isQuantifier(LogicalTypesParser.CheckFuncContext func) {
    LogicalTypesParser.Func_applicationContext app = func.func_expr().func_application();
    if (app == null || app.check_expr_list() == null
        || app.check_expr_list().check_expr().size() != 3) {
      return false;
    }
    String name = app.identifier().getText().toUpperCase(Locale.ROOT);
    return "EVERY".equals(name) || "ANY".equals(name) || "ONE".equals(name);
  }

  // The inputs of a function NULL either when one is or when its value is missing, which its
  // CEL gives as null: VARIANT_GET, TRY_VARIANT_GET and TRY_PARSE_JSON.
  private static List<LogicalTypesParser.Check_exprContext> valueTestedInputs(
      LogicalTypesParser.Func_exprContext f) {
    LogicalTypesParser.Func_expr_common_subexprContext sub = f.func_expr_common_subexpr();
    if (sub instanceof LogicalTypesParser.FuncVariantGetContext) {
      return ((LogicalTypesParser.FuncVariantGetContext) sub).check_expr();
    }
    if (sub instanceof LogicalTypesParser.FuncTryVariantGetContext) {
      return ((LogicalTypesParser.FuncTryVariantGetContext) sub).check_expr();
    }
    LogicalTypesParser.Func_applicationContext app = f.func_application();
    return app != null && app.check_expr_list() != null
        && "TRY_PARSE_JSON".equalsIgnoreCase(app.identifier().getText())
        ? app.check_expr_list().check_expr() : null;
  }

  /**
   * {@code test}, refusing an expression whose NULL can't be told when an input may be NULL:
   * evaluated instead, it could answer as if its NULL were a value.
   */
  static CelNode told(CelNode test, org.antlr.v4.runtime.ParserRuleContext node) {
    if (test == null && mayBeNullByInput(node)) {
      throw ConstraintToCelTranslator.locatedError(node, "can't tell when '" + node.getText()
          + "' is NULL, so it can't be tested for NULL here; test its inputs instead.");
    }
    return test;
  }

  /**
   * {@link #mayBeNullByInput(ParseTree)} outside an emission, as during validation.
   */
  static boolean mayBeNullByInput(ParseTree node, ConstraintValidationContext vctx) {
    ConstraintValidationContext emitting = EMIT_VCTX.get();
    EMIT_VCTX.set(vctx);
    try {
      return mayBeNullByInput(node);
    } finally {
      if (emitting == null) {
        EMIT_VCTX.remove();
      } else {
        EMIT_VCTX.set(emitting);
      }
    }
  }

  /**
   * True if a NULL can reach {@code node}: a nullable or unresolved column or field, a
   * subscript, a literal NULL, or a function that can give NULL on its own.
   */
  static boolean mayBeNullByInput(ParseTree node) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (node instanceof LogicalTypesParser.CheckLiteralContext) {
      return ((LogicalTypesParser.CheckLiteralContext) node).literal().NULL() != null;
    }
    if (node instanceof LogicalTypesParser.C_exprContext && hasSubscript(
        (LogicalTypesParser.C_exprContext) node)) {
      return true;
    }
    if (node instanceof LogicalTypesParser.CheckColumnRefContext
        || node instanceof LogicalTypesParser.CheckParenContext
            && ((LogicalTypesParser.CheckParenContext) node).indirection() != null) {
      List<String> path =
          ConstraintResolver.selectionPathCExpr((LogicalTypesParser.C_exprContext) node);
      List<Integer> steps = path == null ? null : ConstraintResolver.nullableSteps(path, vctx);
      if (steps == null || !steps.isEmpty()) {
        return true;
      }
    }
    if (node instanceof LogicalTypesParser.CheckFuncContext) {
      LogicalTypesParser.CheckFuncContext func = (LogicalTypesParser.CheckFuncContext) node;
      if (valueTestedInputs(func.func_expr()) != null || isTestedByValue(func)) {
        return true;
      }
      LogicalTypesParser.Func_applicationContext app = func.func_expr().func_application();
      if (app != null && app.check_expr_list() != null
          && app.check_expr_list().check_expr().size() == 3 && vctx != null) {
        // A quantifier's body sees its variable bound to the collection's element.
        List<LogicalTypesParser.Check_exprContext> args = app.check_expr_list().check_expr();
        String name = app.identifier().getText().toUpperCase(Locale.ROOT);
        if ("EVERY".equals(name) || "ANY".equals(name) || "ONE".equals(name)) {
          if (mayBeNullByInput(args.get(0))) {
            return true;
          }
          ConstraintValidationContext body = ConstraintEmitter.bodyContext(app, vctx,
              () -> ConstraintEmitter.iteration(vctx, args.get(1).getText(), args.get(0)));
          EMIT_VCTX.set(body);
          try {
            return mayBeNullByInput(args.get(2));
          } finally {
            EMIT_VCTX.set(vctx);
          }
        }
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (mayBeNullByInput(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  // True if operand may be a VARIANT, whose CAST can be NULL though it isn't: its type is one, or,
  // its type not known, a VARIANT is read or made in it.
  private static boolean mayBeVariant(LogicalTypesParser.Check_exprContext operand) {
    // NULLIF's value is its first argument, whatever the second's type.
    if ("NULLIF".equals(ConstraintResolver.tryExtractLeafFuncName(operand))) {
      LogicalTypesParser.Func_applicationContext call = outermostCall(operand);
      if (call != null && call.check_expr_list() != null) {
        return mayBeVariant(call.check_expr_list().check_expr(0));
      }
    }
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    Schema type = ConstraintResolver.tryResolveCheckExprType(operand, vctx);
    return type != null ? type.getType() == Schema.Type.VARIANT : readsVariant(operand, vctx);
  }

  private static LogicalTypesParser.Func_applicationContext outermostCall(ParseTree node) {
    if (node instanceof LogicalTypesParser.Func_applicationContext) {
      return (LogicalTypesParser.Func_applicationContext) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      LogicalTypesParser.Func_applicationContext call = outermostCall(node.getChild(i));
      if (call != null) {
        return call;
      }
    }
    return null;
  }

  // A part whose type is known is a VARIANT just when that type is one, so a condition, a
  // macro or a comparison in it, whose value doesn't flow out, isn't entered.
  private static boolean readsVariant(ParseTree node, ConstraintValidationContext vctx) {
    Schema type = null;
    if (node instanceof LogicalTypesParser.Check_exprContext) {
      type = ConstraintResolver.tryResolveCheckExprType(
          (LogicalTypesParser.Check_exprContext) node, vctx);
    } else if (node instanceof LogicalTypesParser.Func_exprContext) {
      type = ConstraintResolver.tryResolveFuncReturnType(
          (LogicalTypesParser.Func_exprContext) node, vctx);
    }
    if (type != null) {
      return type.getType() == Schema.Type.VARIANT;
    }
    if (node instanceof LogicalTypesParser.ColumnrefContext) {
      return ConstraintResolver.mayReadVariant((LogicalTypesParser.ColumnrefContext) node, vctx);
    }
    // A step off a bracketed value, as a path's, may reach one where its type doesn't resolve.
    if (node instanceof LogicalTypesParser.CheckParenContext
        && ((LogicalTypesParser.CheckParenContext) node).indirection() != null) {
      Schema read = ConstraintResolver.resolveParenType(
          (LogicalTypesParser.CheckParenContext) node, vctx);
      return read == null || read.getType() == Schema.Type.VARIANT;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (readsVariant(node.getChild(i), vctx)) {
        return true;
      }
    }
    return false;
  }

  // SYMMETRIC, or with bounds never NULL, NULL exactly when an operand is; else by Kleene's AND.
  private static CelNode ofBetween(LogicalTypesParser.Check_expr_betweenContext between) {
    List<LogicalTypesParser.Check_expr_inContext> ins = between.check_expr_in();
    CelNode lo = of(ins.get(1));
    CelNode hi = of(ins.get(2));
    if (between.SYMMETRIC() != null || NEVER.equals(lo) && NEVER.equals(hi)) {
      return any(Arrays.asList(of(ins.get(0)), lo, hi));
    }
    return neither(ConstraintEmitter.emitBetweenTruth(between, true),
        ConstraintEmitter.emitBetweenTruth(between, false));
  }

  // NULL when neither definitely TRUE nor definitely FALSE, from emitTruth's flags.
  private static CelNode neither(ConstraintEmitter.Truth whenTrue,
      ConstraintEmitter.Truth whenFalse) {
    int flags = whenTrue.flags | whenFalse.flags;
    if ((flags & ConstraintEmitter.UNTOLD) != 0) {
      return null;
    }
    return (flags & ConstraintEmitter.MAY_BE_NULL) == 0 ? NEVER
        : CelNode.and(CelNode.not(whenTrue.cel), CelNode.not(whenFalse.cel));
  }

  // An IN's test when its elements are never NULL; null when one may be.
  private static CelNode ofIn(LogicalTypesParser.Check_expr_inContext in) {
    CelNode subject = of(in.check_expr_like());
    if (in.in_target() instanceof LogicalTypesParser.InTargetExprContext) {
      ConstraintValidationContext vctx = EMIT_VCTX.get();
      Schema type = vctx == null ? null : ConstraintResolver.inTargetType(in, vctx);
      return type != null && holdsNoNull(type) ? ofCollectionIn(in, subject) : null;
    }
    for (LogicalTypesParser.Check_exprContext item
        : ((LogicalTypesParser.InTargetParenListContext) in.in_target())
            .check_expr_list().check_expr()) {
      if (!NEVER.equals(of(item))) {
        return null;
      }
    }
    return subject;
  }

  // An array of elements never NULL, or a MULTISET of strings or a MAP with string keys, held
  // as a map whose keys never are.
  private static boolean holdsNoNull(Schema type) {
    switch (type.getType()) {
      case ARRAY:
        return !type.getElementType().isNullable();
      case MULTISET:
        return isString(type.getElementType());
      case MAP:
        return isString(type.getKeyType());
      default:
        return false;
    }
  }

  private static boolean isString(Schema type) {
    return type.getType() == Schema.Type.VARCHAR || type.getType() == Schema.Type.CHAR;
  }

  /**
   * The test of an IN over a collection: NULL when the collection is, or when the subject is
   * and the collection isn't empty, as SQL's = ANY over no elements is FALSE whatever it is.
   */
  static CelNode ofCollectionIn(LogicalTypesParser.Check_expr_inContext in, CelNode subject) {
    LogicalTypesParser.Check_expr_likeContext target =
        ((LogicalTypesParser.InTargetExprContext) in.in_target()).check_expr_like();
    CelNode collection = of(target);
    if (subject == null || collection == null) {
      return null;
    }
    return either(collection, NEVER.equals(subject) ? NEVER : CelNode.and(subject,
        CelNode.binary(ConstraintEmitter.collectionSize(target), ">", CelNode.literal("0"))));
  }

  // NULL when any input is.
  static CelNode any(List<CelNode> tests) {
    if (tests.contains(null)) {
      return null;
    }
    if (tests.contains(ALWAYS)) {
      return ALWAYS;
    }
    Set<CelNode> nullable = new LinkedHashSet<>(tests);
    nullable.remove(NEVER);
    return nullable.isEmpty() ? NEVER : CelNode.or(new ArrayList<>(nullable));
  }

  // NULL when every input is.
  private static CelNode all(List<CelNode> tests) {
    if (tests.isEmpty() || tests.contains(NEVER)) {
      return NEVER;
    }
    if (tests.contains(null)) {
      return null;
    }
    Set<CelNode> nullable = new LinkedHashSet<>(tests);
    nullable.remove(ALWAYS);
    return nullable.isEmpty() ? ALWAYS : CelNode.and(new ArrayList<>(nullable));
  }
}
