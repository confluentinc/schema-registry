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

import io.confluent.kafka.schemaregistry.rules.cel.builtin.VariantPaths;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;

import java.util.ArrayList;
import java.util.List;

import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintEmitter.visitCExpr;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintEmitter.visitCheckExpr;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintResolver.resolveCollectionElementType;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintResolver.tryAsLiteral;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintResolver.tryLiteralIntValue;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator.EMIT_VCTX;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator.locatedError;

/**
 * Function dispatch + special-syntax form emit for the CHECK→CEL translator.
 * Splits {@link ConstraintToCelTranslator}'s ~600 lines of function handling
 * out so the main file stays focused on cascade orchestration.
 *
 * <p>Two SQL function shapes:
 * <ul>
 *   <li>{@code func_application} — regular {@code f(args)} form. Name is any
 *       identifier; {@link #visitFuncApplication} dispatches against the
 *       closed whitelist and emits the appropriate CEL form.</li>
 *   <li>{@code func_expr_common_subexpr} — SQL-spec keyword forms whose
 *       syntax differs from {@code f(args)}: CAST, EXTRACT, SUBSTRING,
 *       POSITION, TRIM, plus the no-parens CURRENT_TIMESTAMP runtime variable.
 *       {@link #visitFuncCommonSubexpr} dispatches by parse-tree alternative.</li>
 * </ul>
 *
 * <p>This class delegates back to {@link ConstraintEmitter} and
 * {@link ConstraintToCelTranslator} for the cross-cutting helpers
 * ({@code visitCheckExpr}, {@code locatedError}) and ThreadLocal state
 * ({@code EMIT_VCTX}).
 */
final class ConstraintFunctions {

  private ConstraintFunctions() {
    // static utility
  }

  static CelNode visitFuncExpr(LogicalTypesParser.Func_exprContext ctx) {
    if (ctx.func_application() != null) {
      return visitFuncApplication(ctx.func_application());
    }
    if (ctx.func_expr_common_subexpr() != null) {
      return visitFuncCommonSubexpr(ctx.func_expr_common_subexpr());
    }
    throw new ValidationException("Internal: empty func_expr");
  }

  // -------------------------------------------------------------------------
  // Regular form: func_application
  // -------------------------------------------------------------------------

  private static CelNode visitFuncApplication(LogicalTypesParser.Func_applicationContext ctx) {
    String name = ctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    List<LogicalTypesParser.Check_exprContext> args = ctx.check_expr_list() != null
        ? ctx.check_expr_list().check_expr()
        : java.util.Collections.emptyList();
    switch (name) {
      // size() — polymorphic over strings, lists, maps, bytes
      case "LENGTH":
        if (args.size() == 1) {
          return CelNode.call("size",
              ConstraintEmitter.typedSubscript(visitCheckExpr(args.get(0))));
        }
        return emitMethodCall(ctx, args, "size", -1, name);

      // String predicates → CEL string methods
      case "UPPER":
        return emitInstanceMethod(ctx, args, "upperAscii", 0, 1, name);
      case "LOWER":
        return emitInstanceMethod(ctx, args, "lowerAscii", 0, 1, name);
      case "STARTS_WITH":
        return emitInstanceMethod(ctx, args, "startsWith", 0, 2, name);
      case "ENDS_WITH":
        return emitInstanceMethod(ctx, args, "endsWith", 0, 2, name);
      case "CONTAINS":
        return emitInstanceMethod(ctx, args, "contains", 0, 2, name);
      case "REPLACE":
        return emitInstanceMethod(ctx, args, "replace", 0, 3, name);
      case "MATCHES":
        validateMatchesPattern(ctx, args);
        return emitInstanceMethod(ctx, args, "matches", 0, 2, name);

      // Format validators (require SR BuiltinOverload extension at runtime)
      case "IS_EMAIL":
        return emitInstanceMethod(ctx, args, "isEmail", 0, 1, name);
      case "IS_HOSTNAME":
        return emitInstanceMethod(ctx, args, "isHostname", 0, 1, name);
      case "IS_IPV4":
        return emitInstanceMethod(ctx, args, "isIpv4", 0, 1, name);
      case "IS_IPV6":
        return emitInstanceMethod(ctx, args, "isIpv6", 0, 1, name);
      case "IS_URI":
        return emitInstanceMethod(ctx, args, "isUri", 0, 1, name);
      case "IS_URI_REF":
        return emitInstanceMethod(ctx, args, "isUriRef", 0, 1, name);
      case "IS_UUID":
        return emitInstanceMethod(ctx, args, "isUuid", 0, 1, name);

      // Conditional helpers — n-ary or fixed
      case "COALESCE":
        return emitCoalesce(ctx, args);
      case "NULLIF":
        return emitNullIf(ctx, args);
      case "GREATEST":
        return emitGreatestLeast(ctx, args, ">", name);
      case "LEAST":
        return emitGreatestLeast(ctx, args, "<", name);

      // Numeric math — dispatch on argument type: DECIMAL operands route to the
      // opaque-Decimal decimals.* surface (exact), all other numerics to the
      // CEL math.* extension. All single-arg except ROUND/TRUNCATE, which also
      // accept a 2-arg scale form on DECIMAL only.
      case "ABS":
        return emitNumericFunc(ctx, args, "abs", name);
      case "SIGN":
        return emitNumericFunc(ctx, args, "sign", name);
      case "FLOOR":
        return emitNumericFunc(ctx, args, "floor", name);
      case "CEIL":
      case "CEILING":
        return emitNumericFunc(ctx, args, "ceil", name);
      case "SQRT":
        return emitNumericFunc(ctx, args, "sqrt", name);
      case "ROUND":
        return emitRoundTrunc(ctx, args, "round", name);
      case "TRUNCATE":
        return emitRoundTrunc(ctx, args, "trunc", name);

      // Variant: parse / serialize / inspect. (Navigation+extraction is the
      // VARIANT_GET / TRY_VARIANT_GET special form, see visitFuncCommonSubexpr.)
      case "PARSE_JSON":
        return emitVariantUnary(ctx, args, "variants.parseJson", name);
      case "TRY_PARSE_JSON":
        return emitVariantUnary(ctx, args, "variants.tryParseJson", name);
      case "TO_JSON":
        return emitVariantUnary(ctx, args, "variants.toJson", name);
      case "TYPE_OF_VARIANT":
        return emitVariantUnary(ctx, args, "variants.type", name);
      case "VARIANT_IS_NULL":
        return emitVariantUnary(ctx, args, "variants.isNull", name);

      // CEL macros (sugar)
      case "EVERY":
        return emitMacro(ctx, args, "all", name);
      case "ANY":
        return emitMacro(ctx, args, "exists", name);
      case "ONE":
        return ConstraintEmitter.emitOne(ctx, () -> emitMacro(ctx, args, "exists_one", name));

      default:
        throw locatedError(ctx,
            "Unknown function in CHECK: '" + ctx.identifier().getText()
                + "'. Allowed functions: LENGTH, UPPER, LOWER, STARTS_WITH, "
                + "ENDS_WITH, CONTAINS, REPLACE, MATCHES, COALESCE, NULLIF, "
                + "GREATEST, LEAST, EVERY, ANY, ONE, IS_EMAIL, IS_HOSTNAME, "
                + "IS_IPV4, IS_IPV6, IS_URI, IS_URI_REF, IS_UUID, ABS, SIGN, "
                + "FLOOR, CEIL/CEILING, ROUND, TRUNCATE, SQRT, PARSE_JSON, "
                + "TRY_PARSE_JSON, TO_JSON, TYPE_OF_VARIANT, VARIANT_IS_NULL, "
                + "plus the special-syntax forms CAST, EXTRACT, SUBSTRING, "
                + "POSITION, TRIM, CURRENT_TIMESTAMP, VARIANT_GET, "
                + "TRY_VARIANT_GET.");
    }
  }

  /**
   * If {@code MATCHES(s, '<regex>')}'s second arg is a literal string,
   * compile it with Google's RE2J at parse time. RE2J matches CEL's
   * {@code matches()} runtime engine — Java's {@code java.util.regex} is
   * more permissive (named groups, possessive quantifiers, backreferences)
   * and would silently accept patterns that fail at CEL evaluation.
   */
  private static void validateMatchesPattern(
      LogicalTypesParser.Func_applicationContext ctx,
      List<LogicalTypesParser.Check_exprContext> args) {
    if (args.size() != 2) {
      return;  // arity error caught downstream by emitInstanceMethod
    }
    ConstraintResolver.LiteralMatch m = tryAsLiteral(args.get(1));
    if (m == null || m.lit.stringLiteral() == null) {
      return;
    }
    String pattern = ConstraintPatterns.extractSqlStringContent(
        m.lit.stringLiteral().getText());
    try {
      com.google.re2j.Pattern.compile(pattern);
    } catch (com.google.re2j.PatternSyntaxException e) {
      throw locatedError(args.get(1),
          "MATCHES() regex is invalid (RE2 syntax): " + e.getDescription()
              + " in pattern '" + pattern + "'. Note: matches() uses "
              + "Google RE2 syntax — named groups (?<name>...), possessive "
              + "quantifiers (*+, ++, ?+), and backreferences (\\1) are not "
              + "supported.");
    }
  }

  /**
   * CEL-macro sugar: {@code EVERY/ANY/ONE(arr, x, predicate)} →
   * {@code arr.all(x, predicate)} / {@code .exists(x, predicate)} /
   * {@code .exists_one(x, predicate)}.
   */
  private static CelNode emitMacro(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String celMacro, String sqlName) {
    if (args.size() != 3) {
      throw locatedError(fctx,
          sqlName + "() expects 3 arguments (list, iteration variable, predicate), "
              + "got " + args.size());
    }
    String iterVar = extractIterationVar(args.get(1), sqlName);
    CelNode collection = visitCheckExpr(args.get(0));
    // Push the iter-var binding onto EMIT_VCTX so visitColumnRef and
    // visitIndirection see it during the body emit.
    ConstraintValidationContext outer = EMIT_VCTX.get();
    ConstraintValidationContext bodyCtx = outer == null ? null : ConstraintEmitter.bodyContext(
        fctx, outer, () -> outer.withBinding(iterVar,
            resolveCollectionElementType(args.get(0), outer)));
    EMIT_VCTX.set(bodyCtx);
    try {
      return CelNode.method(collection, celMacro, CelNode.ident(iterVar),
          visitCheckExpr(args.get(2)));
    } finally {
      EMIT_VCTX.set(outer);
    }
  }

  /**
   * Validate that the iter-var arg is a bare identifier and not a reserved
   * word.
   */
  static String extractIterationVar(
      LogicalTypesParser.Check_exprContext ctx, String sqlName) {
    String text = ctx.getText();
    if (!text.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw locatedError(ctx,
          "Second argument to " + sqlName + "() must be a simple identifier "
              + "naming the iteration variable, got: '" + text + "'. "
              + "Example: " + sqlName + "(tags, t, LENGTH(t) > 0)");
    }
    if ("this".equals(text)) {
      throw locatedError(ctx,
          sqlName + "() iteration variable name 'this' shadows the implicit "
              + "row/root reference (the macro binds it as the loop variable, "
              + "hiding the row so the body can no longer reach its columns); "
              + "choose a different identifier.");
    }
    if (ConstraintValidationContext.isRuntimeVar(text)) {
      throw locatedError(ctx,
          sqlName + "() iteration variable name '" + text + "' shadows a "
              + "runtime-injected variable; choose a different name.");
    }
    if (Schema.isCelReservedName(text)) {
      throw locatedError(ctx,
          sqlName + "() iteration variable name '" + text + "' is a "
              + "reserved word; choose a different identifier (the macro "
              + "binding emits as a bare identifier).");
    }
    return text;
  }

  /**
   * Emit {@code methodName(arg)} (CEL function-style).
   */
  private static CelNode emitMethodCall(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String methodName,
      int expectedArity, String sqlName) {
    if (expectedArity != -1 && args.size() != expectedArity) {
      throw locatedError(fctx,
          sqlName + "() expects " + expectedArity + " argument(s), got " + args.size());
    }
    if (expectedArity == -1 && args.size() != 1) {
      throw locatedError(fctx,
          sqlName + "() expects exactly 1 argument, got " + args.size());
    }
    List<CelNode> emitted = new ArrayList<>();
    for (LogicalTypesParser.Check_exprContext arg : args) {
      emitted.add(visitCheckExpr(arg));
    }
    return CelNode.call(methodName, emitted);
  }

  /**
   * Single-argument numeric function: {@code ABS/SIGN/FLOOR/CEILING/SQRT}.
   * DECIMAL argument → {@code decimals.<fn>(...)} (the argument emitted via the
   * decimal cascade); any other numeric → {@code math.<fn>(...)}.
   *
   * <p>For non-decimal operands, {@code abs}/{@code sign}/{@code sqrt} have
   * int/uint/double overloads in the CEL math extension, but
   * {@code ceil}/{@code floor} are double-only — applying them to an INT
   * column surfaces as a "no matching overload" error from the strict checker
   * (rather than a silent no-op), which is the intended signal.
   */
  private static CelNode emitNumericFunc(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String shortName, String sqlName) {
    if (args.size() != 1) {
      throw locatedError(fctx,
          sqlName + "() expects exactly 1 argument, got " + args.size());
    }
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    Schema t = vctx == null ? null
        : ConstraintResolver.tryResolveCheckExprType(args.get(0), vctx);
    return ConstraintResolver.isDecimal(t)
        ? CelNode.call("decimals." + shortName, ConstraintEmitter.emitDecimalValue(args.get(0)))
        : CelNode.call("math." + shortName, visitCheckExpr(args.get(0)));
  }

  /**
   * {@code ROUND}/{@code TRUNCATE}: 1-arg for any numeric ({@code math.round}/
   * {@code math.trunc} or {@code decimals.round}/{@code decimals.trunc}); the
   * 2-arg scale form is DECIMAL-only ({@code decimals.round(d, n)} /
   * {@code decimals.trunc(d, n)}) — CEL {@code math.round}/{@code math.trunc}
   * have no scale parameter, so 2-arg on a non-decimal operand is rejected.
   */
  private static CelNode emitRoundTrunc(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String shortName, String sqlName) {
    if (args.size() != 1 && args.size() != 2) {
      throw locatedError(fctx,
          sqlName + "() expects 1 or 2 arguments, got " + args.size());
    }
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    Schema t = vctx == null ? null
        : ConstraintResolver.tryResolveCheckExprType(args.get(0), vctx);
    boolean decimal = ConstraintResolver.isDecimal(t);
    if (args.size() == 2 && !decimal) {
      throw locatedError(fctx,
          sqlName + "() with a scale argument is only supported on DECIMAL "
              + "operands; for INT/DOUBLE only the single-argument form is "
              + "available (math." + shortName + " has no scale parameter).");
    }
    if (args.size() == 2) {
      refuseNonIntScale(args.get(1), vctx, sqlName);
    }
    if (!decimal) {
      return CelNode.call("math." + shortName, visitCheckExpr(args.get(0)));
    }
    CelNode value = ConstraintEmitter.emitDecimalValue(args.get(0));
    if (args.size() == 1) {
      return CelNode.call("decimals." + shortName, value);
    }
    return CelNode.call("decimals." + shortName, value, "round".equals(shortName)
        ? emitRoundScale(args.get(1)) : visitCheckExpr(args.get(1)));
  }

  /**
   * ROUND's scale, at most DECIMAL's 38: a larger one gives the value back, as in Flink, where
   * decimals.round would pad it, taking time that grows with the scale. The declared scale
   * doesn't bound it, as a JSON or Protobuf value carries its own, which may be larger.
   */
  private static CelNode emitRoundScale(LogicalTypesParser.Check_exprContext scale) {
    Long literal = tryLiteralIntValue(scale);
    if (literal != null) {
      return CelNode.integer(Math.min(literal, 38));
    }
    CelNode emitted = visitCheckExpr(scale);
    CelNode max = CelNode.literal("38");
    return CelNode.ternary(CelNode.binary(emitted, "<", max), emitted, max);
  }

  // A column, field, variable or subscript of one, parenthesized or not, whose declared type is
  // exact, unlike an expression's.
  private static boolean isPath(LogicalTypesParser.Check_exprContext e) {
    LogicalTypesParser.C_exprContext c = ConstraintResolver.leafPrimary(e);
    if (c instanceof LogicalTypesParser.CheckColumnRefContext) {
      return true;
    }
    return c instanceof LogicalTypesParser.CheckParenContext
        && isPath(((LogicalTypesParser.CheckParenContext) c).check_expr());
  }

  // Flink takes ROUND's and TRUNCATE's scale as an INT, refusing a BIGINT one or a literal out
  // of range. Only a path's BIGINT is told: an expression's integer resolves as CEL's int64
  // whatever its width, as CAST(x AS INT) or LENGTH(s) does.
  private static void refuseNonIntScale(LogicalTypesParser.Check_exprContext scale,
      ConstraintValidationContext vctx, String sqlName) {
    Long literal = tryLiteralIntValue(scale);
    Schema type = vctx == null || literal != null || !isPath(scale)
        ? null : ConstraintResolver.tryResolveCheckExprType(scale, vctx);
    boolean outOfRange = literal != null
        && (literal < Integer.MIN_VALUE || literal > Integer.MAX_VALUE);
    if (outOfRange || type != null && type.getType() == Schema.Type.BIGINT) {
      throw locatedError(scale, sqlName + "() takes an INT scale, as in Flink: '"
          + scale.getText() + "' is " + (outOfRange ? "out of its range." : "a BIGINT."));
    }
  }

  /**
   * Emit CEL receiver-style call: arg[receiverIdx].method(other args).
   */
  private static CelNode emitInstanceMethod(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String methodName,
      int receiverIdx, int expectedArity, String sqlName) {
    if (args.size() != expectedArity) {
      throw locatedError(fctx,
          sqlName + "() expects " + expectedArity + " argument(s), got " + args.size());
    }
    List<CelNode> rest = new ArrayList<>();
    for (int i = 0; i < args.size(); i++) {
      if (i != receiverIdx) {
        rest.add(visitCheckExpr(args.get(i)));
      }
    }
    return CelNode.method(visitCheckExpr(args.get(receiverIdx)), methodName, rest);
  }

  /**
   * COALESCE(a, b, c, ...) → {@code (a != null ? a : (b != null ? b : c))}.
   * Each per-arg null check uses {@link #notNullTestOf}, a {@code has()} guard
   * when the arg is a CEL field-selection chain.
   */
  private static CelNode emitCoalesce(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args) {
    if (args.size() < 2) {
      throw locatedError(fctx,
          "COALESCE() expects at least 2 arguments, got " + args.size());
    }
    int n = args.size();
    // The COALESCE branches unify to their common type. Numeric branches
    // coerce to the common numeric type (DECIMAL → Decimal values, DOUBLE →
    // double-cast values) so the ternary branches type-unify under the strict
    // checker. Temporal branches normalize to CEL timestamps. Null checks still
    // use the native emit so the has() guard sees a field selection.
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    String common = (vctx != null) ? ConstraintEmitter.numericCommonOf(args, vctx) : null;
    boolean decimal = "decimal".equals(common);
    boolean dbl = "double".equals(common);
    boolean temporal = !decimal && !dbl && argsHaveTemporal(args);
    CelNode[] nativeEmit = new CelNode[n];
    CelNode[] valueEmit = new CelNode[n];
    for (int i = 0; i < n; i++) {
      nativeEmit[i] = visitCheckExpr(args.get(i));
      if (decimal) {
        valueEmit[i] = ConstraintEmitter.emitDecimalValue(args.get(i));
      } else if (dbl) {
        valueEmit[i] = ConstraintEmitter.emitNumericAs(args.get(i), "double");
      } else if (temporal) {
        // Normalize each branch so the COALESCE result is a CEL timestamp.
        valueEmit[i] = ConstraintEmitter.emitTimestampValueCheckExpr(args.get(i));
      } else {
        valueEmit[i] = nativeEmit[i];
      }
    }
    // A last argument that may be NULL gives NULL, not a widened NULL or, on
    // JSON, which leaves a NULL field out, a missing key. (The validator checks
    // the arguments share a category, as the dyn branch no longer makes the
    // strict check unify them.)
    LogicalTypesParser.Check_exprContext last = args.get(n - 1);
    CelNode cel = tryAsLiteral(last) != null ? valueEmit[n - 1]
        : emitNullOrValue(last, nativeEmit[n - 1], valueEmit[n - 1]);
    for (int i = n - 2; i >= 0; i--) {
      cel = CelNode.ternary(notNullTestOf(args.get(i), nativeEmit[i], valueEmit[i]),
          valueEmit[i], cel);
    }
    return cel;
  }

  /**
   * {@code value}, {@code arg} converted to the common type, or NULL when {@code arg} is NULL:
   * tested before the conversion, in its own type or by has(), as converting a NULL fails and
   * JSON leaves a NULL field out.
   */
  static CelNode emitNullOrValue(LogicalTypesParser.Check_exprContext arg, CelNode nativeEmit,
      CelNode value) {
    // COALESCE, GREATEST and LEAST give NULL as null, so one not converted is not tested again.
    return value.equals(nativeEmit) && ConstraintNullTest.givesNullAsNull(arg) ? value
        : emitNullOr(nullTestOf(arg, nativeEmit, value), true, value);
  }

  /**
   * The test that {@code arg} is NULL, before it is converted to {@code value}'s type: by has()
   * for a selection, by its inputs ({@link ConstraintNullTest}) if they tell, else of its value in
   * its own type; {@code false} when it is never NULL.
   */
  private static CelNode nullTestOf(LogicalTypesParser.Check_exprContext arg,
      CelNode nativeEmit, CelNode value) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (ConstraintResolver.isHasCompatible(arg, vctx)) {
      return ConstraintEmitter.nullTest(ConstraintResolver.selectionPath(arg), nativeEmit, true);
    }
    CelNode byInputs = ConstraintNullTest.isTestedByValue(arg) ? null
        : ConstraintNullTest.told(ConstraintNullTest.of(arg), arg);
    return byInputs != null ? byInputs
        : CelNode.isNull(ownTypeValue(arg, nativeEmit, value, vctx));
  }

  /**
   * The test that {@code arg} is not NULL, as {@link #nullTestOf} tells it.
   */
  private static CelNode notNullTestOf(LogicalTypesParser.Check_exprContext arg,
      CelNode nativeEmit, CelNode value) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (ConstraintResolver.isHasCompatible(arg, vctx)) {
      return ConstraintEmitter.nullTest(ConstraintResolver.selectionPath(arg), nativeEmit, false);
    }
    CelNode byInputs = ConstraintNullTest.isTestedByValue(arg) ? null
        : ConstraintNullTest.told(ConstraintNullTest.of(arg), arg);
    return byInputs != null ? ConstraintNullTest.not(byInputs)
        : CelNode.isNotNull(ownTypeValue(arg, nativeEmit, value, vctx));
  }

  /**
   * {@code (nullTest ? dyn(null) : value)}, or just {@code value} when the test is false or only
   * asks whether it is null. The strict check, which the dyn branch would pass, types
   * {@code (nullTest ? value : value)}, or just {@code value} when the test reads nothing else.
   */
  private static CelNode emitNullOr(CelNode nullTest, boolean testsOnlyValue, CelNode value) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    boolean typeOnly = vctx != null && vctx.isTypeOnly();
    if (ConstraintNullTest.NEVER.equals(nullTest) || nullTest.equals(CelNode.isNull(value))
        || typeOnly && testsOnlyValue) {
      return value;
    }
    return CelNode.ternary(nullTest, typeOnly ? value : CelNode.dyn(CelNode.NULL), value);
  }

  /**
   * NULLIF(x, y) → {@code (x == y ? dyn(null) : x)}, after a NULL x gives NULL and a NULL y
   * gives x, as SQL's {@code CASE WHEN x = y THEN NULL ELSE x END} does. The {@code dyn(null)}
   * makes the ternary branches type-unify under CEL's strict checker
   * — a bare {@code (cond ? null : typed)} fails with "no matching
   * overload for '_?_:_' applied to '(bool, null, type)'" because CEL
   * ternary requires both branches to share a type.
   */
  private static CelNode emitNullIf(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args) {
    if (args.size() != 2) {
      throw locatedError(fctx,
          "NULLIF() expects 2 arguments, got " + args.size());
    }
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    String common = (vctx != null)
        ? ConstraintEmitter.numericCommonOf(args.get(0), args.get(1), vctx) : null;
    boolean temporal = !ConstraintResolver.isNumericCategory(common)
        && (argIsTemporal(args.get(0)) || argIsTemporal(args.get(1)));
    CelNode equal;
    if ("decimal".equals(common)) {
      // Opaque Decimal has no native `==`.
      equal = CelNode.call("decimals.eq", ConstraintEmitter.emitDecimalValue(args.get(0)),
          ConstraintEmitter.emitDecimalValue(args.get(1)));
    } else if ("double".equals(common)) {
      // Native `==`, operands cast to double or timestamp-normalized when they need it.
      equal = CelNode.binary(ConstraintEmitter.emitNumericAs(args.get(0), "double"), "==",
          ConstraintEmitter.emitNumericAs(args.get(1), "double"));
    } else if (temporal) {
      equal = CelNode.binary(ConstraintEmitter.emitTimestampValueCheckExpr(args.get(0)), "==",
          ConstraintEmitter.emitTimestampValueCheckExpr(args.get(1)));
    } else {
      equal = CelNode.binary(visitCheckExpr(args.get(0)), "==", visitCheckExpr(args.get(1)));
    }
    // Normalize the returned value too (so a temporal NULLIF result is a CEL
    // timestamp), keeping the decimal/native paths unchanged.
    CelNode value = temporal ? ConstraintEmitter.emitTimestampValueCheckExpr(args.get(0))
        : visitCheckExpr(args.get(0));
    // Each argument is tested before it is compared or converted.
    CelNode nativeA = visitCheckExpr(args.get(0));
    CelNode nativeB = visitCheckExpr(args.get(1));
    CelNode nullB = nullTestOf(args.get(1), nativeB, nativeB);
    CelNode unlessEqual = emitNullOr(equal, false, value);
    CelNode unlessNullA = ConstraintNullTest.NEVER.equals(nullB) ? unlessEqual
        : CelNode.ternary(nullB, value, unlessEqual);
    return emitNullOr(nullTestOf(args.get(0), nativeA, value), true, unlessNullA);
  }

  /**
   * True if {@code arg} statically resolves to the DECIMAL category.
   */
  private static boolean argIsDecimal(LogicalTypesParser.Check_exprContext arg) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    return vctx != null
        && ConstraintResolver.isDecimal(
            ConstraintResolver.tryResolveCheckExprType(arg, vctx));
  }

  /**
   * True if {@code arg}'s subtree involves a timestamp column or a
   * TIMESTAMP/INTERVAL literal.
   */
  private static boolean argIsTemporal(LogicalTypesParser.Check_exprContext arg) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    return vctx != null && ConstraintResolver.subtreeHasTemporal(arg, vctx);
  }

  /** True if any argument's subtree is temporal. */
  private static boolean argsHaveTemporal(List<LogicalTypesParser.Check_exprContext> args) {
    for (LogicalTypesParser.Check_exprContext arg : args) {
      if (argIsTemporal(arg)) {
        return true;
      }
    }
    return false;
  }

  /**
   * GREATEST(a, b) / LEAST(a, b) — NULL when either argument is, as in Flink, else the
   * comparison. Limited to 2 arguments in v1. Per-arg null guards use
   * {@link #nullTestOf}, a {@code has()} guard when the arg is a CEL
   * field-selection chain.
   */
  private static CelNode emitGreatestLeast(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String op, String sqlName) {
    if (args.size() != 2) {
      throw locatedError(fctx,
          sqlName + "() supports exactly 2 arguments in v1, got " + args.size()
              + ". For more, nest calls: " + sqlName + "(a, " + sqlName + "(b, c))");
    }
    // Null checks use the native emit (so the has() guard sees a field
    // selection). The inner min/max picks by common type: DECIMAL uses
    // decimals.greatest/least; INT/DOUBLE use the math extension's
    // greatest/least (operands cast to the common type); temporal/other use a
    // native compare ternary. The NULL guard means greatest/least only ever
    // see non-null operands.
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    String common = (vctx != null)
        ? ConstraintEmitter.numericCommonOf(args.get(0), args.get(1), vctx) : null;
    boolean decimal = "decimal".equals(common);
    boolean dbl = "double".equals(common);
    boolean temporal = !decimal && !dbl
        && (argIsTemporal(args.get(0)) || argIsTemporal(args.get(1)));
    CelNode nativeA = visitCheckExpr(args.get(0));
    CelNode nativeB = visitCheckExpr(args.get(1));
    CelNode a = nativeA;
    CelNode b = nativeB;
    if (decimal) {
      a = ConstraintEmitter.emitDecimalValue(args.get(0));
      b = ConstraintEmitter.emitDecimalValue(args.get(1));
    } else if (dbl) {
      a = ConstraintEmitter.emitNumericAs(args.get(0), "double");
      b = ConstraintEmitter.emitNumericAs(args.get(1), "double");
    } else if (temporal) {
      // Native >/< on timestamps; operands and returned values normalized.
      a = ConstraintEmitter.emitTimestampValueCheckExpr(args.get(0));
      b = ConstraintEmitter.emitTimestampValueCheckExpr(args.get(1));
    }
    CelNode pick;
    if (decimal) {
      pick = CelNode.call(">".equals(op) ? "decimals.greatest" : "decimals.least", a, b);
    } else if (dbl || "int".equals(common)) {
      pick = CelNode.call(">".equals(op) ? "math.greatest" : "math.least", a, b);
    } else {
      // Temporal / string / other comparable — native min/max via ternary.
      pick = CelNode.ternary(CelNode.binary(a, op, b), a, b);
    }
    // Each operand is tested before it is converted or, as JSON leaves a NULL field out, read.
    return emitNullOr(ConstraintNullTest.either(nullTestOf(args.get(0), nativeA, a),
        nullTestOf(args.get(1), nativeB, b)), true, pick);
  }

  /**
   * An operand's value in its own numeric type, to test it for NULL before it is
   * converted to the common type: INT natively, DOUBLE and DECIMAL by their
   * cascades. Any other operand is tested as {@code converted}.
   */
  private static CelNode ownTypeValue(
      LogicalTypesParser.Check_exprContext arg, CelNode nativeEmit, CelNode converted,
      ConstraintValidationContext vctx) {
    Schema t = vctx == null ? null : ConstraintResolver.tryResolveCheckExprType(arg, vctx);
    String cat = t == null ? null : ConstraintResolver.categoryOf(t.getType());
    if ("int".equals(cat)) {
      return nativeEmit;
    }
    if ("double".equals(cat)) {
      return ConstraintEmitter.emitNumericAs(arg, "double");
    }
    if ("decimal".equals(cat)) {
      return ConstraintEmitter.emitDecimalValue(arg);
    }
    return converted;
  }

  // -------------------------------------------------------------------------
  // Special-syntax forms: func_expr_common_subexpr
  // -------------------------------------------------------------------------

  private static CelNode visitFuncCommonSubexpr(
      LogicalTypesParser.Func_expr_common_subexprContext ctx) {
    if (ctx instanceof LogicalTypesParser.FuncCastContext) {
      return visitCast((LogicalTypesParser.FuncCastContext) ctx);
    }
    if (ctx instanceof LogicalTypesParser.FuncExtractContext) {
      return visitExtract((LogicalTypesParser.FuncExtractContext) ctx);
    }
    if (ctx instanceof LogicalTypesParser.FuncSubstringFromForContext) {
      return visitSubstringFromFor((LogicalTypesParser.FuncSubstringFromForContext) ctx);
    }
    if (ctx instanceof LogicalTypesParser.FuncSubstringCommasContext) {
      return visitSubstringCommas((LogicalTypesParser.FuncSubstringCommasContext) ctx);
    }
    if (ctx instanceof LogicalTypesParser.FuncPositionContext) {
      return visitPosition((LogicalTypesParser.FuncPositionContext) ctx);
    }
    if (ctx instanceof LogicalTypesParser.FuncTrimContext) {
      return visitTrim((LogicalTypesParser.FuncTrimContext) ctx);
    }
    if (ctx instanceof LogicalTypesParser.FuncCurrentTimestampContext) {
      return CelNode.ident("now");
    }
    if (ctx instanceof LogicalTypesParser.FuncVariantGetContext) {
      return visitVariantGet((LogicalTypesParser.FuncVariantGetContext) ctx);
    }
    if (ctx instanceof LogicalTypesParser.FuncTryVariantGetContext) {
      return visitTryVariantGet((LogicalTypesParser.FuncTryVariantGetContext) ctx);
    }
    throw new ValidationException(
        "Unrecognized func_expr_common_subexpr subtype: "
            + ctx.getClass().getSimpleName());
  }

  /**
   * Single-arg variant function: {@code celFn(<arg>)}.
   */
  private static CelNode emitVariantUnary(
      LogicalTypesParser.Func_applicationContext ctx,
      List<LogicalTypesParser.Check_exprContext> args, String celFn, String sqlName) {
    if (args.size() != 1) {
      throw locatedError(ctx, sqlName + "() expects 1 argument, got " + args.size());
    }
    return CelNode.call(celFn, visitCheckExpr(args.get(0)));
  }

  /**
   * VARIANT_GET(v, path [RETURNING T]) → {@code variants.path(v, path)} (returns
   * the sub-variant) when no RETURNING; {@code variants.as(variants.path(v,
   * path), "label")} (extract + cast) when a return type is given.
   */
  private static CelNode visitVariantGet(LogicalTypesParser.FuncVariantGetContext ctx) {
    checkLiteralPath(ctx.check_expr(1));
    return ctx.castType() == null ? variantPath(ctx.check_expr(0), ctx.check_expr(1))
        : emitVariantPathAs("variants.as", ctx.check_expr(0), ctx.check_expr(1), ctx.castType());
  }

  /**
   * TRY_VARIANT_GET(v, path RETURNING T) →
   * {@code variants.tryAs(variants.path(v, path), "label")} (null on type
   * mismatch).
   */
  private static CelNode visitTryVariantGet(LogicalTypesParser.FuncTryVariantGetContext ctx) {
    checkLiteralPath(ctx.check_expr(1));
    return emitVariantPathAs("variants.tryAs", ctx.check_expr(0), ctx.check_expr(1),
        ctx.castType());
  }

  // A literal path is parsed as the variants functions parse it, so a malformed one is refused
  // here rather than failing every record.
  private static void checkLiteralPath(LogicalTypesParser.Check_exprContext path) {
    ConstraintResolver.LiteralMatch m = tryAsLiteral(path);
    if (m == null || m.lit.stringLiteral() == null) {
      return;
    }
    String value = ConstraintPatterns.extractSqlStringContent(m.lit.stringLiteral().getText());
    try {
      VariantPaths.check(value);
    } catch (IllegalArgumentException e) {
      throw locatedError(path, "variant path '" + value + "' is malformed: " + e.getMessage());
    }
  }

  private static CelNode emitVariantPathAs(
      String asFn, LogicalTypesParser.Check_exprContext v,
      LogicalTypesParser.Check_exprContext path,
      LogicalTypesParser.CastTypeContext castType) {
    return CelNode.call(asFn, variantPath(v, path),
        CelNode.string(variantTypeLabel(castType.primitiveType())));
  }

  private static CelNode variantPath(LogicalTypesParser.Check_exprContext v,
      LogicalTypesParser.Check_exprContext path) {
    return CelNode.call("variants.path", visitCheckExpr(v), visitCheckExpr(path));
  }

  /**
   * Map a VARIANT_GET RETURNING type to the {@code variants.as} extraction label.
   */
  private static String variantTypeLabel(LogicalTypesParser.PrimitiveTypeContext pt) {
    String text = pt.getText().toUpperCase(java.util.Locale.ROOT);
    int paren = text.indexOf('(');
    String head = paren >= 0 ? text.substring(0, paren) : text;
    switch (head) {
      case "INT":
      case "INTEGER":
      case "TINYINT":
      case "SMALLINT":
      case "BIGINT":
        return "int";
      case "FLOAT":
      case "REAL":
      case "DOUBLE":
      case "DOUBLEPRECISION":
        return "double";
      case "DECIMAL":
      case "DEC":
      case "NUMERIC":
        return "decimal";
      case "STRING":
      case "VARCHAR":
      case "CHAR":
      case "CHARACTER":
      case "CHARACTERVARYING":
        return "string";
      case "BOOLEAN":
        return "boolean";
      case "BYTES":
      case "BINARY":
      case "VARBINARY":
      case "BINARYVARYING":
        return "bytes";
      case "TIMESTAMP":
      case "TIMESTAMP_LTZ":
        return "timestamp";
      default:
        throw locatedError(pt,
            "VARIANT_GET RETURNING type '" + text + "' is not supported. "
                + "Extractable types: INT/INTEGER/TINYINT/SMALLINT/BIGINT, "
                + "FLOAT/REAL/DOUBLE, DECIMAL, STRING/VARCHAR/CHAR, BOOLEAN, "
                + "BYTES/BINARY/VARBINARY, TIMESTAMP.");
    }
  }

  /**
   * CAST(x AS T) → int(x) / double(x) / string(x) / bool(x) / bytes(x), with
   * two decimal-specific forms:
   * <ul>
   *   <li><b>To decimal:</b> {@code CAST(x AS DECIMAL(p,s))} →
   *       {@code decimals.round(decimal(x), s)} — applies the scale (HALF_UP),
   *       faithful to SQL CAST; scale defaults to 0 when absent. Precision
   *       {@code p} is not enforced.</li>
   *   <li><b>From decimal to string/double:</b> {@code CAST(decimalExpr AS STRING)}
   *       → {@code string(<decimal value>)} and {@code CAST(decimalExpr AS DOUBLE)}
   *       → {@code double(<decimal value>)} (narrowing; may lose precision) via the
   *       {@code (Decimal) -> string} / {@code (Decimal) -> double} overloads. Other
   *       CAST targets from a decimal source (INT/BOOLEAN/BYTES) have no conversion
   *       and are rejected.</li>
   * </ul>
   */
  private static CelNode visitCast(LogicalTypesParser.FuncCastContext ctx) {
    LogicalTypesParser.PrimitiveTypeContext pt = ctx.castType().primitiveType();
    LogicalTypesParser.Check_exprContext arg = ctx.check_expr();
    // A NULL's CAST is null, as int(null) would fail; the strict check still types it.
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    boolean nullArg = ConstraintResolver.isLiteralNull(arg);
    if (nullArg && vctx != null && !vctx.isTypeOnly()) {
      return CelNode.dyn(CelNode.NULL);
    }
    boolean argDecimal = argIsDecimal(arg);

    if (isDecimalCastTarget(pt)) {
      int scale = ConstraintResolver.parseDecimalCastParams(pt.getText())[1];
      return CelNode.call("decimals.round", argDecimal ? ConstraintEmitter.emitDecimalValue(arg)
          : CelNode.call("decimal", emitCastArg(arg, nullArg)),
          CelNode.integer(scale));
    }

    String celFn = celTypeFor(pt);
    if (argDecimal) {
      if ("string".equals(celFn)) {
        // (Decimal) -> string overload; the arg must be emitted as a Decimal.
        return CelNode.call("string", ConstraintEmitter.emitDecimalValue(arg));
      }
      if ("double".equals(celFn)) {
        // (Decimal) -> double overload (FLOAT/REAL/DOUBLE target); narrowing,
        // may lose precision (out-of-range magnitudes become ±Infinity).
        return CelNode.call("double", ConstraintEmitter.emitDecimalValue(arg));
      }
      throw locatedError(ctx,
          "CAST from DECIMAL to " + pt.getText().toUpperCase(java.util.Locale.ROOT)
              + " is not supported. From a DECIMAL source only CAST(... AS STRING), "
              + "CAST(... AS DOUBLE), and CAST(... AS DECIMAL(p,s)) are available.");
    }
    return CelNode.call(celFn, emitCastArg(arg, nullArg));
  }

  // A NULL as dyn, as bool(null) and bytes(null) have no overload, while the cast types it.
  private static CelNode emitCastArg(LogicalTypesParser.Check_exprContext arg, boolean nullArg) {
    return nullArg ? CelNode.dyn(CelNode.NULL) : visitCheckExpr(arg);
  }

  /** True if the CAST target type is DECIMAL / DEC / NUMERIC. */
  private static boolean isDecimalCastTarget(
      LogicalTypesParser.PrimitiveTypeContext pt) {
    String head = pt.getText().toUpperCase(java.util.Locale.ROOT);
    int paren = head.indexOf('(');
    if (paren >= 0) {
      head = head.substring(0, paren);
    }
    return "DECIMAL".equals(head) || "DEC".equals(head) || "NUMERIC".equals(head);
  }

  static String celTypeFor(LogicalTypesParser.PrimitiveTypeContext pt) {
    String text = pt.getText().toUpperCase(java.util.Locale.ROOT);
    int paren = text.indexOf('(');
    String head = paren >= 0 ? text.substring(0, paren) : text;
    switch (head) {
      case "INT":
      case "INTEGER":
      case "TINYINT":
      case "SMALLINT":
      case "BIGINT":
        return "int";
      case "FLOAT":
      case "REAL":
      case "DOUBLE":
      case "DOUBLEPRECISION":
      case "DECIMAL":
      case "DEC":
      case "NUMERIC":
        return "double";
      case "STRING":
      case "VARCHAR":
      case "CHAR":
      case "CHARACTER":
      case "CHARACTERVARYING":
        return "string";
      case "BOOLEAN":
        return "bool";
      case "BYTES":
      case "BINARY":
      case "VARBINARY":
      case "BINARYVARYING":
        return "bytes";
      default:
        throw locatedError(pt,
            "CAST to type '" + text + "' is not supported in CHECK. "
                + "Supported types: INT/INTEGER/TINYINT/SMALLINT/BIGINT, "
                + "FLOAT/REAL/DOUBLE/DECIMAL, STRING/VARCHAR/CHAR, BOOLEAN, "
                + "BYTES/BINARY/VARBINARY. Date/time casts are not supported "
                + "in v1.");
    }
  }

  /**
   * EXTRACT(field FROM ts) → ts.getXxx(). The receiver is normalized via
   * {@code timestamp(...)} when it's an instant-timestamp column so the
   * runtime value (Instant / RFC-3339 string) coerces to a CEL timestamp;
   * {@code now} (CURRENT_TIMESTAMP) is left unwrapped.
   */
  private static CelNode visitExtract(LogicalTypesParser.FuncExtractContext ctx) {
    String field = ctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    switch (field) {
      case "YEAR":
        return CelNode.method(ConstraintEmitter.emitTimestampReceiver(ctx.check_expr()),
            "getFullYear");
      case "MONTH":
        return CelNode.binary(CelNode.method(
            ConstraintEmitter.emitTimestampReceiver(ctx.check_expr()), "getMonth"), "+",
            CelNode.literal("1"));
      case "DAY":
        return CelNode.method(ConstraintEmitter.emitTimestampReceiver(ctx.check_expr()),
            "getDate");
      case "HOUR":
        return CelNode.method(ConstraintEmitter.emitTimestampReceiver(ctx.check_expr()),
            "getHours");
      case "MINUTE":
        return CelNode.method(ConstraintEmitter.emitTimestampReceiver(ctx.check_expr()),
            "getMinutes");
      case "SECOND":
        return CelNode.method(ConstraintEmitter.emitTimestampReceiver(ctx.check_expr()),
            "getSeconds");
      case "EPOCH":
        return CelNode.call("int", ConstraintEmitter.emitTimestampReceiver(ctx.check_expr()));
      default:
        throw locatedError(ctx,
            "EXTRACT field '" + ctx.identifier().getText() + "' is not supported. "
                + "Supported fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, EPOCH.");
    }
  }

  /** SUBSTRING(s FROM i FOR n) → s.substring(i-1, i-1+n). */
  private static CelNode visitSubstringFromFor(
      LogicalTypesParser.FuncSubstringFromForContext ctx) {
    List<LogicalTypesParser.Check_exprContext> exprs = ctx.check_expr();
    rejectNonPositiveSubstringStart(exprs.get(1));
    if (exprs.size() > 2) {
      rejectNonPositiveSubstringLength(exprs.get(2));
    }
    return emitSubstring(exprs);
  }

  /** SUBSTRING(s, i, n) (comma form) — same translation as the FROM/FOR form. */
  private static CelNode visitSubstringCommas(
      LogicalTypesParser.FuncSubstringCommasContext ctx) {
    List<LogicalTypesParser.Check_exprContext> exprs = ctx.check_expr();
    rejectNonPositiveSubstringStart(exprs.get(1));
    if (exprs.size() > 2) {
      rejectNonPositiveSubstringLength(exprs.get(2));
    }
    return emitSubstring(exprs);
  }

  private static CelNode emitSubstring(List<LogicalTypesParser.Check_exprContext> exprs) {
    CelNode start = CelNode.binary(visitCheckExpr(exprs.get(1)), "-", CelNode.literal("1"));
    CelNode receiver = visitCheckExpr(exprs.get(0));
    return exprs.size() > 2
        ? CelNode.method(receiver, "substring", start,
            CelNode.binary(start, "+", visitCheckExpr(exprs.get(2))))
        : CelNode.method(receiver, "substring", start);
  }

  /** Reject SUBSTRING(... FROM i ...) when i is a literal value ≤ 0. */
  private static void rejectNonPositiveSubstringStart(
      LogicalTypesParser.Check_exprContext startExpr) {
    Long literal = tryLiteralIntValue(startExpr);
    if (literal != null && literal <= 0L) {
      throw locatedError(startExpr,
          "SUBSTRING start index must be ≥ 1 (SQL is 1-indexed); got "
              + literal + ". Postgres clamps non-positive starts to 1, but "
              + "the generated expression performs no clamping; rewrite "
              + "the expression to use a positive start.");
    }
  }

  /** Reject SUBSTRING(... FOR n ...) when n is a literal value < 0. */
  private static void rejectNonPositiveSubstringLength(
      LogicalTypesParser.Check_exprContext lengthExpr) {
    Long literal = tryLiteralIntValue(lengthExpr);
    if (literal != null && literal < 0L) {
      throw locatedError(lengthExpr,
          "SUBSTRING length must be ≥ 0; got " + literal
              + ". Postgres returns an empty string for negative lengths, "
              + "but the generated expression would throw at runtime; rewrite "
              + "the expression to use a non-negative length.");
    }
  }

  /**
   * POSITION(needle IN haystack) → (haystack.indexOf(needle) + 1). With FROM, the
   * guards give Flink's answers (an empty needle is 1, a start past the end 0) and
   * never pass indexOf an offset out of range, where the CEL runtimes disagree.
   */
  private static CelNode visitPosition(LogicalTypesParser.FuncPositionContext ctx) {
    CelNode haystack = visitCheckExpr(ctx.check_expr(0));
    CelNode needle = visitCExpr(ctx.c_expr());
    CelNode one = CelNode.literal("1");
    if (ctx.FROM() == null) {
      return CelNode.binary(CelNode.method(haystack, "indexOf", needle), "+", one);
    }
    CelNode start = visitCheckExpr(ctx.check_expr(1));
    CelNode zero = CelNode.literal("0");
    CelNode offset = CelNode.ternary(CelNode.binary(start, "<=", one), zero,
        CelNode.binary(start, "-", one));
    CelNode value = CelNode.ternary(CelNode.binary(needle, "==", CelNode.literal("''")), one,
        CelNode.ternary(CelNode.binary(start, ">", CelNode.call("size", haystack)), zero,
            CelNode.binary(CelNode.method(haystack, "indexOf", needle, offset), "+", one)));
    // NULL when an operand is: an empty needle or a start past the end skips the others.
    CelNode isNull = ConstraintNullTest.of(ctx);
    return isNull == null ? value : emitNullOr(isNull, true, value);
  }


  /** TRIM(x) → x.trim() (whitespace, both ends). */
  private static CelNode visitTrim(LogicalTypesParser.FuncTrimContext ctx) {
    if (ctx.LEADING() != null || ctx.TRAILING() != null) {
      String mode = ctx.LEADING() != null ? "LEADING" : "TRAILING";
      throw locatedError(ctx,
          "TRIM " + mode + " is not supported. There is no directional trim; "
              + "use TRIM(x) (whitespace, both ends).");
    }
    List<LogicalTypesParser.Check_exprContext> exprs = ctx.check_expr();
    if (exprs.size() != 1) {
      throw locatedError(ctx,
          "TRIM with a character set is not supported. The generated trim() removes "
              + "leading/trailing whitespace only and has no chars overload. "
              + "Use TRIM(x) (whitespace, both ends), or for character-set "
              + "trimming, use MATCHES(x, '<regex>') with an anchored pattern.");
    }
    return CelNode.method(visitCheckExpr(exprs.get(0)), "trim");
  }

  // -------------------------------------------------------------------------
  // Bytes literal — translated at literal-emit time but lives here since it
  // mirrors the special-syntax functional layer (validation + emit shape).
  // -------------------------------------------------------------------------

  /**
   * SQL bytes literal {@code x'AABB'} → CEL bytes literal {@code b"\xAA\xBB"}.
   * Validates hex content (non-hex char or odd length → ValidationException).
   */
  static String translateBytesLiteral(
      LogicalTypesParser.BytesLiteralContext ctx, String sqlText) {
    if (!isWellFormedBytesLiteral(sqlText)) {
      throw new ValidationException(
          "Internal: malformed bytes literal token: " + sqlText);
    }
    String hex = sqlText.substring(2, sqlText.length() - 1);
    if ((hex.length() & 1) != 0) {
      throw locatedError(ctx,
          "Bytes literal must have an even number of hex digits, got: "
              + sqlText);
    }
    StringBuilder out = new StringBuilder("b\"");
    for (int i = 0; i < hex.length(); i += 2) {
      char hi = hex.charAt(i);
      char lo = hex.charAt(i + 1);
      if (!isHexDigit(hi) || !isHexDigit(lo)) {
        throw locatedError(ctx,
            "Bytes literal contains non-hex character in: " + sqlText);
      }
      out.append("\\x").append(Character.toUpperCase(hi))
          .append(Character.toUpperCase(lo));
    }
    out.append('"');
    return out.toString();
  }

  private static boolean isWellFormedBytesLiteral(String sqlText) {
    if (sqlText.length() < 3) {
      return false;
    }
    char head = sqlText.charAt(0);
    if (head != 'x' && head != 'X') {
      return false;
    }
    return sqlText.charAt(1) == '\''
        && sqlText.charAt(sqlText.length() - 1) == '\'';
  }

  private static boolean isHexDigit(char c) {
    if (c >= '0' && c <= '9') {
      return true;
    }
    if (c >= 'a' && c <= 'f') {
      return true;
    }
    return c >= 'A' && c <= 'F';
  }
}
