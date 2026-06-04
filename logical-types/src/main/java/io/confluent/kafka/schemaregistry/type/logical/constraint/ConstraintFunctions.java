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

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;

import java.util.List;

import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintEmitter.emitWrappedReceiver;
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
 * <p>This class delegates back to {@link ConstraintToCelTranslator} for the
 * cross-cutting helpers ({@code visitCheckExpr}, {@code emitWrappedReceiver},
 * {@code locatedError}) and ThreadLocal state ({@code EMIT_VCTX}).
 */
final class ConstraintFunctions {

  private ConstraintFunctions() {
    // static utility
  }

  static void visitFuncExpr(
      LogicalTypesParser.Func_exprContext ctx, StringBuilder sb) {
    if (ctx.func_application() != null) {
      visitFuncApplication(ctx.func_application(), sb);
    } else if (ctx.func_expr_common_subexpr() != null) {
      visitFuncCommonSubexpr(ctx.func_expr_common_subexpr(), sb);
    } else {
      throw new ValidationException("Internal: empty func_expr");
    }
  }

  // -------------------------------------------------------------------------
  // Regular form: func_application
  // -------------------------------------------------------------------------

  private static void visitFuncApplication(
      LogicalTypesParser.Func_applicationContext ctx, StringBuilder sb) {
    String name = ctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    List<LogicalTypesParser.Check_exprContext> args = ctx.check_expr_list() != null
        ? ctx.check_expr_list().check_expr()
        : java.util.Collections.emptyList();
    switch (name) {
      // size() — polymorphic over strings, lists, maps, bytes
      case "LENGTH":
        emitMethodCall(ctx, args, "size", -1, sb, name);
        break;

      // String predicates → CEL string methods
      case "UPPER":
        emitInstanceMethod(ctx, args, "upperAscii", 0, 1, sb, name);
        break;
      case "LOWER":
        emitInstanceMethod(ctx, args, "lowerAscii", 0, 1, sb, name);
        break;
      case "STARTS_WITH":
        emitInstanceMethod(ctx, args, "startsWith", 0, 2, sb, name);
        break;
      case "ENDS_WITH":
        emitInstanceMethod(ctx, args, "endsWith", 0, 2, sb, name);
        break;
      case "CONTAINS":
        emitInstanceMethod(ctx, args, "contains", 0, 2, sb, name);
        break;
      case "REPLACE":
        emitInstanceMethod(ctx, args, "replace", 0, 3, sb, name);
        break;
      case "MATCHES":
        validateMatchesPattern(ctx, args);
        emitInstanceMethod(ctx, args, "matches", 0, 2, sb, name);
        break;

      // Format validators (require SR BuiltinOverload extension at runtime)
      case "IS_EMAIL":
        emitInstanceMethod(ctx, args, "isEmail", 0, 1, sb, name);
        break;
      case "IS_HOSTNAME":
        emitInstanceMethod(ctx, args, "isHostname", 0, 1, sb, name);
        break;
      case "IS_IPV4":
        emitInstanceMethod(ctx, args, "isIpv4", 0, 1, sb, name);
        break;
      case "IS_IPV6":
        emitInstanceMethod(ctx, args, "isIpv6", 0, 1, sb, name);
        break;
      case "IS_URI":
        emitInstanceMethod(ctx, args, "isUri", 0, 1, sb, name);
        break;
      case "IS_URI_REF":
        emitInstanceMethod(ctx, args, "isUriRef", 0, 1, sb, name);
        break;
      case "IS_UUID":
        emitInstanceMethod(ctx, args, "isUuid", 0, 1, sb, name);
        break;

      // Conditional helpers — n-ary or fixed
      case "COALESCE":
        emitCoalesce(ctx, args, sb);
        break;
      case "NULLIF":
        emitNullIf(ctx, args, sb);
        break;
      case "GREATEST":
        emitGreatestLeast(ctx, args, ">", sb, name);
        break;
      case "LEAST":
        emitGreatestLeast(ctx, args, "<", sb, name);
        break;

      // Numeric math — dispatch on argument type: DECIMAL operands route to the
      // opaque-Decimal decimals.* surface (exact), all other numerics to the
      // CEL math.* extension. All single-arg except ROUND/TRUNCATE, which also
      // accept a 2-arg scale form on DECIMAL only.
      case "ABS":
        emitNumericFunc(ctx, args, "abs", sb, name);
        break;
      case "SIGN":
        emitNumericFunc(ctx, args, "sign", sb, name);
        break;
      case "FLOOR":
        emitNumericFunc(ctx, args, "floor", sb, name);
        break;
      case "CEIL":
      case "CEILING":
        emitNumericFunc(ctx, args, "ceil", sb, name);
        break;
      case "SQRT":
        emitNumericFunc(ctx, args, "sqrt", sb, name);
        break;
      case "ROUND":
        emitRoundTrunc(ctx, args, "round", sb, name);
        break;
      case "TRUNCATE":
        emitRoundTrunc(ctx, args, "trunc", sb, name);
        break;

      // Variant: parse / serialize / inspect. (Navigation+extraction is the
      // VARIANT_GET / TRY_VARIANT_GET special form, see visitFuncCommonSubexpr.)
      case "PARSE_JSON":
        emitVariantUnary(ctx, args, "variants.parseJson", sb, name);
        break;
      case "TRY_PARSE_JSON":
        emitVariantUnary(ctx, args, "variants.tryParseJson", sb, name);
        break;
      case "TO_JSON":
        emitVariantUnary(ctx, args, "variants.toJson", sb, name);
        break;
      case "TYPE_OF_VARIANT":
        emitVariantUnary(ctx, args, "variants.type", sb, name);
        break;
      case "VARIANT_IS_NULL":
        emitVariantUnary(ctx, args, "variants.isNull", sb, name);
        break;

      // CEL macros (sugar)
      case "EVERY":
        emitMacro(ctx, args, "all", sb, name);
        break;
      case "ANY":
        emitMacro(ctx, args, "exists", sb, name);
        break;
      case "ONE":
        emitMacro(ctx, args, "exists_one", sb, name);
        break;

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
  private static void emitMacro(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String celMacro,
      StringBuilder sb, String sqlName) {
    if (args.size() != 3) {
      throw locatedError(fctx,
          sqlName + "() expects 3 arguments (list, iteration variable, predicate), "
              + "got " + args.size());
    }
    String iterVar = extractIterationVar(args.get(1), sqlName);
    emitWrappedReceiver(args.get(0), sb);
    sb.append('.').append(celMacro).append('(').append(iterVar).append(", ");
    // Push the iter-var binding onto EMIT_VCTX so visitColumnRef and
    // visitIndirection see it during the body emit.
    ConstraintValidationContext outer = EMIT_VCTX.get();
    ConstraintValidationContext bodyCtx = outer;
    if (outer != null) {
      Schema elemType = resolveCollectionElementType(args.get(0), outer);
      bodyCtx = outer.withBinding(iterVar, elemType);
    }
    EMIT_VCTX.set(bodyCtx);
    try {
      visitCheckExpr(args.get(2), sb);
    } finally {
      EMIT_VCTX.set(outer);
    }
    sb.append(')');
  }

  /**
   * Validate that the iter-var arg is a bare identifier and not a reserved
   * word.
   */
  private static String extractIterationVar(
      LogicalTypesParser.Check_exprContext ctx, String sqlName) {
    String text = ctx.getText();
    if (!text.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw locatedError(ctx,
          "Second argument to " + sqlName + "() must be a simple identifier "
              + "naming the iteration variable, got: '" + text + "'. "
              + "Example: " + sqlName + "(tags, t, LENGTH(t) > 0)");
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
  private static void emitMethodCall(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String methodName,
      int expectedArity, StringBuilder sb, String sqlName) {
    if (expectedArity != -1 && args.size() != expectedArity) {
      throw locatedError(fctx,
          sqlName + "() expects " + expectedArity + " argument(s), got " + args.size());
    }
    if (expectedArity == -1 && args.size() != 1) {
      throw locatedError(fctx,
          sqlName + "() expects exactly 1 argument, got " + args.size());
    }
    sb.append(methodName).append('(');
    for (int i = 0; i < args.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      visitCheckExpr(args.get(i), sb);
    }
    sb.append(')');
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
  private static void emitNumericFunc(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String shortName,
      StringBuilder sb, String sqlName) {
    if (args.size() != 1) {
      throw locatedError(fctx,
          sqlName + "() expects exactly 1 argument, got " + args.size());
    }
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    Schema t = vctx == null ? null
        : ConstraintResolver.tryResolveCheckExprType(args.get(0), vctx);
    if (ConstraintResolver.isDecimal(t)) {
      sb.append("decimals.").append(shortName).append('(');
      ConstraintEmitter.emitDecimalValue(args.get(0), sb);
      sb.append(')');
    } else {
      sb.append("math.").append(shortName).append('(');
      visitCheckExpr(args.get(0), sb);
      sb.append(')');
    }
  }

  /**
   * {@code ROUND}/{@code TRUNCATE}: 1-arg for any numeric ({@code math.round}/
   * {@code math.trunc} or {@code decimals.round}/{@code decimals.trunc}); the
   * 2-arg scale form is DECIMAL-only ({@code decimals.round(d, n)} /
   * {@code decimals.trunc(d, n)}) — CEL {@code math.round}/{@code math.trunc}
   * have no scale parameter, so 2-arg on a non-decimal operand is rejected.
   */
  private static void emitRoundTrunc(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String shortName,
      StringBuilder sb, String sqlName) {
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
    if (decimal) {
      sb.append("decimals.").append(shortName).append('(');
      ConstraintEmitter.emitDecimalValue(args.get(0), sb);
      if (args.size() == 2) {
        sb.append(", ");
        visitCheckExpr(args.get(1), sb);
      }
      sb.append(')');
    } else {
      sb.append("math.").append(shortName).append('(');
      visitCheckExpr(args.get(0), sb);
      sb.append(')');
    }
  }

  /**
   * Emit CEL receiver-style call: arg[receiverIdx].method(other args). Wraps
   * the receiver in parens only when needed — compound receivers like
   * {@code a || b} or {@code -x} would otherwise mis-bind under CEL's tight
   * {@code .method()} precedence, but simple primaries (column refs,
   * literals, function calls, already-parenthesized expressions) emit
   * unwrapped. Delegates to {@link ConstraintEmitter#emitWrappedReceiver}.
   */
  private static void emitInstanceMethod(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String methodName,
      int receiverIdx, int expectedArity, StringBuilder sb, String sqlName) {
    if (args.size() != expectedArity) {
      throw locatedError(fctx,
          sqlName + "() expects " + expectedArity + " argument(s), got " + args.size());
    }
    emitWrappedReceiver(args.get(receiverIdx), sb);
    sb.append('.').append(methodName).append('(');
    boolean first = true;
    for (int i = 0; i < args.size(); i++) {
      if (i == receiverIdx) {
        continue;
      }
      if (!first) {
        sb.append(", ");
      }
      visitCheckExpr(args.get(i), sb);
      first = false;
    }
    sb.append(')');
  }

  /**
   * COALESCE(a, b, c, ...) → {@code (a != null ? a : (b != null ? b : c))}.
   * Each per-arg null check uses {@link #emitIsNotNullCheck}, which wraps
   * with a {@code has()} guard when the arg is a CEL field-selection chain.
   */
  private static void emitCoalesce(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, StringBuilder sb) {
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
    String[] nativeEmit = new String[n];
    String[] valueEmit = new String[n];
    for (int i = 0; i < n; i++) {
      StringBuilder buf = new StringBuilder();
      visitCheckExpr(args.get(i), buf);
      nativeEmit[i] = buf.toString();
      if (decimal) {
        StringBuilder dec = new StringBuilder();
        ConstraintEmitter.emitDecimalValue(args.get(i), dec);
        valueEmit[i] = dec.toString();
      } else if (dbl) {
        StringBuilder d = new StringBuilder();
        ConstraintEmitter.emitNumericAs(args.get(i), "double", d);
        valueEmit[i] = d.toString();
      } else if (temporal) {
        // Normalize each branch so the COALESCE result is a CEL timestamp.
        StringBuilder ts = new StringBuilder();
        ConstraintEmitter.emitTimestampValueCheckExpr(args.get(i), ts);
        valueEmit[i] = ts.toString();
      } else {
        valueEmit[i] = nativeEmit[i];
      }
    }
    for (int i = 0; i < n - 1; i++) {
      sb.append('(');
      emitIsNotNullCheck(args.get(i), nativeEmit[i], sb);
      sb.append(" ? ").append(valueEmit[i]).append(" : ");
    }
    sb.append(valueEmit[n - 1]);
    for (int i = 0; i < n - 1; i++) {
      sb.append(')');
    }
  }

  /**
   * Emit {@code dyn(<arg>) != null}, optionally wrapped with a presence
   * check ({@code (has(<arg>) && dyn(<arg>) != null)}) when the arg is a
   * CEL field-selection chain. {@code emitted} is the cached emit of the
   * arg so we don't re-visit the parse tree per use site.
   *
   * <p>The {@code dyn(...)} wrapper makes the null comparison portable
   * across CEL checkers that would otherwise reject {@code <typed> != null}
   * (e.g., consumers compiling against concrete proto types). {@code dyn}
   * is a no-op at runtime. {@code has()}'s argument stays a bare selection
   * per CEL spec.
   *
   * <p>The has-rewrite output is paren-wrapped so the helper is safe to
   * splice into any context. Today both call sites (COALESCE,
   * GREATEST/LEAST) use it as a ternary condition where CEL's grammar
   * naturally groups the {@code &&}/{@code ||} chain — but a future call
   * site that splices into an {@code ||}/{@code &&} expression would
   * silently mis-bind without the parens (same root cause as
   * {@link ConstraintEmitter#visitIs}).
   */
  private static void emitIsNotNullCheck(
      LogicalTypesParser.Check_exprContext arg, String emitted, StringBuilder sb) {
    if (ConstraintResolver.isHasCompatible(arg, EMIT_VCTX.get())) {
      sb.append("(has(").append(emitted).append(") && dyn(")
          .append(emitted).append(") != null)");
    } else {
      sb.append("dyn(").append(emitted).append(") != null");
    }
  }

  /**
   * Emit {@code dyn(<arg>) == null}, optionally wrapped with a presence
   * check ({@code (!has(<arg>) || dyn(<arg>) == null)}) when the arg is a
   * CEL field-selection chain. Companion to {@link #emitIsNotNullCheck};
   * same paren-wrap and dyn-wrap rationale.
   */
  private static void emitIsNullCheck(
      LogicalTypesParser.Check_exprContext arg, String emitted, StringBuilder sb) {
    if (ConstraintResolver.isHasCompatible(arg, EMIT_VCTX.get())) {
      sb.append("(!has(").append(emitted).append(") || dyn(")
          .append(emitted).append(") == null)");
    } else {
      sb.append("dyn(").append(emitted).append(") == null");
    }
  }

  /**
   * NULLIF(x, y) → {@code (x == y ? dyn(null) : dyn(x))}. The {@code dyn(...)}
   * wraps make the ternary branches type-unify under CEL's strict checker
   * — a bare {@code (cond ? null : typed)} fails with "no matching
   * overload for '_?_:_' applied to '(bool, null, type)'" because CEL
   * ternary requires both branches to share a type.
   */
  private static void emitNullIf(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, StringBuilder sb) {
    if (args.size() != 2) {
      throw locatedError(fctx,
          "NULLIF() expects 2 arguments, got " + args.size());
    }
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    String common = (vctx != null)
        ? ConstraintEmitter.numericCommonOf(args.get(0), args.get(1), vctx) : null;
    boolean temporal = !ConstraintResolver.isNumericCategory(common)
        && (argIsTemporal(args.get(0)) || argIsTemporal(args.get(1)));
    sb.append('(');
    if ("decimal".equals(common)) {
      // Opaque Decimal has no native `==`.
      sb.append("decimals.eq(");
      ConstraintEmitter.emitDecimalValue(args.get(0), sb);
      sb.append(", ");
      ConstraintEmitter.emitDecimalValue(args.get(1), sb);
      sb.append(')');
    } else if ("double".equals(common)) {
      ConstraintEmitter.emitNumericAs(args.get(0), "double", sb);
      sb.append(" == ");
      ConstraintEmitter.emitNumericAs(args.get(1), "double", sb);
    } else if (temporal) {
      // Native `==`, operands timestamp-normalized.
      ConstraintEmitter.emitTimestampValueCheckExpr(args.get(0), sb);
      sb.append(" == ");
      ConstraintEmitter.emitTimestampValueCheckExpr(args.get(1), sb);
    } else {
      visitCheckExpr(args.get(0), sb);
      sb.append(" == ");
      visitCheckExpr(args.get(1), sb);
    }
    sb.append(" ? dyn(null) : dyn(");
    // Normalize the returned value too (so a temporal NULLIF result is a CEL
    // timestamp), keeping the decimal/native paths unchanged.
    if (temporal) {
      ConstraintEmitter.emitTimestampValueCheckExpr(args.get(0), sb);
    } else {
      visitCheckExpr(args.get(0), sb);
    }
    sb.append("))");
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
   * GREATEST(a, b) / LEAST(a, b) — emit a NULL-skipping comparison.
   * Limited to 2 arguments in v1. Per-arg null guards use
   * {@link #emitIsNullCheck}, which wraps with a {@code has()} guard when
   * the arg is a CEL field-selection chain.
   */
  private static void emitGreatestLeast(
      LogicalTypesParser.Func_applicationContext fctx,
      List<LogicalTypesParser.Check_exprContext> args, String op,
      StringBuilder sb, String sqlName) {
    if (args.size() != 2) {
      throw locatedError(fctx,
          sqlName + "() supports exactly 2 arguments in v1, got " + args.size()
              + ". For more, nest calls: " + sqlName + "(a, " + sqlName + "(b, c))");
    }
    // Null checks use the native emit (so the has() guard sees a field
    // selection). The inner min/max picks by common type: DECIMAL uses
    // decimals.greatest/least; INT/DOUBLE use the math extension's
    // greatest/least (operands cast to the common type); temporal/other use a
    // native compare ternary. The NULL-skipping wrapper means greatest/least
    // only ever see non-null operands.
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    String common = (vctx != null)
        ? ConstraintEmitter.numericCommonOf(args.get(0), args.get(1), vctx) : null;
    boolean decimal = "decimal".equals(common);
    boolean dbl = "double".equals(common);
    boolean temporal = !decimal && !dbl
        && (argIsTemporal(args.get(0)) || argIsTemporal(args.get(1)));
    StringBuilder bufA = new StringBuilder();
    StringBuilder bufB = new StringBuilder();
    visitCheckExpr(args.get(0), bufA);
    visitCheckExpr(args.get(1), bufB);
    String nativeA = bufA.toString();
    String nativeB = bufB.toString();
    String a = nativeA;
    String b = nativeB;
    if (decimal) {
      StringBuilder decA = new StringBuilder();
      StringBuilder decB = new StringBuilder();
      ConstraintEmitter.emitDecimalValue(args.get(0), decA);
      ConstraintEmitter.emitDecimalValue(args.get(1), decB);
      a = decA.toString();
      b = decB.toString();
    } else if (dbl) {
      StringBuilder dblA = new StringBuilder();
      StringBuilder dblB = new StringBuilder();
      ConstraintEmitter.emitNumericAs(args.get(0), "double", dblA);
      ConstraintEmitter.emitNumericAs(args.get(1), "double", dblB);
      a = dblA.toString();
      b = dblB.toString();
    } else if (temporal) {
      // Native >/< on timestamps; operands and returned values normalized.
      StringBuilder tsA = new StringBuilder();
      StringBuilder tsB = new StringBuilder();
      ConstraintEmitter.emitTimestampValueCheckExpr(args.get(0), tsA);
      ConstraintEmitter.emitTimestampValueCheckExpr(args.get(1), tsB);
      a = tsA.toString();
      b = tsB.toString();
    }
    sb.append('(');
    emitIsNullCheck(args.get(0), nativeA, sb);
    sb.append(" ? ").append(b).append(" : (");
    emitIsNullCheck(args.get(1), nativeB, sb);
    sb.append(" ? ").append(a).append(" : ");
    if (decimal) {
      sb.append(">".equals(op) ? "decimals.greatest" : "decimals.least")
          .append('(').append(a).append(", ").append(b).append(')');
    } else if (dbl || "int".equals(common)) {
      sb.append(">".equals(op) ? "math.greatest" : "math.least")
          .append('(').append(a).append(", ").append(b).append(')');
    } else {
      // Temporal / string / other comparable — native min/max via ternary.
      sb.append('(').append(a).append(' ').append(op).append(' ').append(b)
          .append(" ? ").append(a).append(" : ").append(b).append(')');
    }
    sb.append("))");
  }

  // -------------------------------------------------------------------------
  // Special-syntax forms: func_expr_common_subexpr
  // -------------------------------------------------------------------------

  private static void visitFuncCommonSubexpr(
      LogicalTypesParser.Func_expr_common_subexprContext ctx, StringBuilder sb) {
    if (ctx instanceof LogicalTypesParser.FuncCastContext) {
      visitCast((LogicalTypesParser.FuncCastContext) ctx, sb);
    } else if (ctx instanceof LogicalTypesParser.FuncExtractContext) {
      visitExtract((LogicalTypesParser.FuncExtractContext) ctx, sb);
    } else if (ctx instanceof LogicalTypesParser.FuncSubstringFromForContext) {
      visitSubstringFromFor((LogicalTypesParser.FuncSubstringFromForContext) ctx, sb);
    } else if (ctx instanceof LogicalTypesParser.FuncSubstringCommasContext) {
      visitSubstringCommas((LogicalTypesParser.FuncSubstringCommasContext) ctx, sb);
    } else if (ctx instanceof LogicalTypesParser.FuncPositionContext) {
      visitPosition((LogicalTypesParser.FuncPositionContext) ctx, sb);
    } else if (ctx instanceof LogicalTypesParser.FuncTrimContext) {
      visitTrim((LogicalTypesParser.FuncTrimContext) ctx, sb);
    } else if (ctx instanceof LogicalTypesParser.FuncCurrentTimestampContext) {
      sb.append("now");
    } else if (ctx instanceof LogicalTypesParser.FuncVariantGetContext) {
      visitVariantGet((LogicalTypesParser.FuncVariantGetContext) ctx, sb);
    } else if (ctx instanceof LogicalTypesParser.FuncTryVariantGetContext) {
      visitTryVariantGet((LogicalTypesParser.FuncTryVariantGetContext) ctx, sb);
    } else {
      throw new ValidationException(
          "Unrecognized func_expr_common_subexpr subtype: "
              + ctx.getClass().getSimpleName());
    }
  }

  /**
   * Single-arg variant function: {@code celFn(<arg>)}.
   */
  private static void emitVariantUnary(
      LogicalTypesParser.Func_applicationContext ctx,
      List<LogicalTypesParser.Check_exprContext> args, String celFn,
      StringBuilder sb, String sqlName) {
    if (args.size() != 1) {
      throw locatedError(ctx, sqlName + "() expects 1 argument, got " + args.size());
    }
    sb.append(celFn).append('(');
    visitCheckExpr(args.get(0), sb);
    sb.append(')');
  }

  /**
   * VARIANT_GET(v, path [RETURNING T]) → {@code variants.path(v, path)} (returns
   * the sub-variant) when no RETURNING; {@code variants.as(variants.path(v,
   * path), "label")} (extract + cast) when a return type is given.
   */
  private static void visitVariantGet(
      LogicalTypesParser.FuncVariantGetContext ctx, StringBuilder sb) {
    if (ctx.castType() == null) {
      sb.append("variants.path(");
      visitCheckExpr(ctx.check_expr(0), sb);
      sb.append(", ");
      visitCheckExpr(ctx.check_expr(1), sb);
      sb.append(')');
    } else {
      emitVariantPathAs("variants.as", ctx.check_expr(0), ctx.check_expr(1),
          ctx.castType(), sb);
    }
  }

  /**
   * TRY_VARIANT_GET(v, path RETURNING T) →
   * {@code variants.tryAs(variants.path(v, path), "label")} (null on type
   * mismatch).
   */
  private static void visitTryVariantGet(
      LogicalTypesParser.FuncTryVariantGetContext ctx, StringBuilder sb) {
    emitVariantPathAs("variants.tryAs", ctx.check_expr(0), ctx.check_expr(1),
        ctx.castType(), sb);
  }

  private static void emitVariantPathAs(
      String asFn, LogicalTypesParser.Check_exprContext v,
      LogicalTypesParser.Check_exprContext path,
      LogicalTypesParser.CastTypeContext castType, StringBuilder sb) {
    final String label = variantTypeLabel(castType.primitiveType());
    sb.append(asFn).append("(variants.path(");
    visitCheckExpr(v, sb);
    sb.append(", ");
    visitCheckExpr(path, sb);
    sb.append("), \"").append(label).append("\")");
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
  private static void visitCast(
      LogicalTypesParser.FuncCastContext ctx, StringBuilder sb) {
    LogicalTypesParser.PrimitiveTypeContext pt = ctx.castType().primitiveType();
    LogicalTypesParser.Check_exprContext arg = ctx.check_expr();
    boolean argDecimal = argIsDecimal(arg);

    if (isDecimalCastTarget(pt)) {
      int scale = ConstraintResolver.parseDecimalCastParams(pt.getText())[1];
      sb.append("decimals.round(");
      if (argDecimal) {
        ConstraintEmitter.emitDecimalValue(arg, sb);
      } else {
        sb.append("decimal(");
        visitCheckExpr(arg, sb);
        sb.append(')');
      }
      sb.append(", ").append(scale).append(')');
      return;
    }

    String celFn = celTypeFor(pt);
    if (argDecimal) {
      if ("string".equals(celFn)) {
        // (Decimal) -> string overload; the arg must be emitted as a Decimal.
        sb.append("string(");
        ConstraintEmitter.emitDecimalValue(arg, sb);
        sb.append(')');
        return;
      }
      if ("double".equals(celFn)) {
        // (Decimal) -> double overload (FLOAT/REAL/DOUBLE target); narrowing,
        // may lose precision (out-of-range magnitudes become ±Infinity).
        sb.append("double(");
        ConstraintEmitter.emitDecimalValue(arg, sb);
        sb.append(')');
        return;
      }
      throw locatedError(ctx,
          "CAST from DECIMAL to " + pt.getText().toUpperCase(java.util.Locale.ROOT)
              + " is not supported. From a DECIMAL source only CAST(... AS STRING), "
              + "CAST(... AS DOUBLE), and CAST(... AS DECIMAL(p,s)) are available.");
    }
    sb.append(celFn).append('(');
    visitCheckExpr(arg, sb);
    sb.append(')');
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
   * {@code timestamp.of(...)} when it's an instant-timestamp column so the
   * runtime value (Instant / RFC-3339 string) coerces to a CEL timestamp;
   * {@code now} (CURRENT_TIMESTAMP) is left unwrapped.
   */
  private static void visitExtract(
      LogicalTypesParser.FuncExtractContext ctx, StringBuilder sb) {
    String field = ctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    switch (field) {
      case "YEAR":
        ConstraintEmitter.emitTimestampReceiver(ctx.check_expr(), sb);
        sb.append(".getFullYear()");
        break;
      case "MONTH":
        sb.append('(');
        ConstraintEmitter.emitTimestampReceiver(ctx.check_expr(), sb);
        sb.append(".getMonth() + 1)");
        break;
      case "DAY":
        ConstraintEmitter.emitTimestampReceiver(ctx.check_expr(), sb);
        sb.append(".getDate()");
        break;
      case "HOUR":
        ConstraintEmitter.emitTimestampReceiver(ctx.check_expr(), sb);
        sb.append(".getHours()");
        break;
      case "MINUTE":
        ConstraintEmitter.emitTimestampReceiver(ctx.check_expr(), sb);
        sb.append(".getMinutes()");
        break;
      case "SECOND":
        ConstraintEmitter.emitTimestampReceiver(ctx.check_expr(), sb);
        sb.append(".getSeconds()");
        break;
      case "EPOCH":
        sb.append("int(");
        ConstraintEmitter.emitTimestampReceiver(ctx.check_expr(), sb);
        sb.append(')');
        break;
      default:
        throw locatedError(ctx,
            "EXTRACT field '" + ctx.identifier().getText() + "' is not supported. "
                + "Supported fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, EPOCH.");
    }
  }

  /** SUBSTRING(s FROM i FOR n) → s.substring(i-1, i-1+n). */
  private static void visitSubstringFromFor(
      LogicalTypesParser.FuncSubstringFromForContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_exprContext> exprs = ctx.check_expr();
    rejectNonPositiveSubstringStart(exprs.get(1));
    if (exprs.size() > 2) {
      rejectNonPositiveSubstringLength(exprs.get(2));
    }
    emitSubstring(exprs, sb);
  }

  /** SUBSTRING(s, i, n) (comma form) — same translation as the FROM/FOR form. */
  private static void visitSubstringCommas(
      LogicalTypesParser.FuncSubstringCommasContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_exprContext> exprs = ctx.check_expr();
    rejectNonPositiveSubstringStart(exprs.get(1));
    if (exprs.size() > 2) {
      rejectNonPositiveSubstringLength(exprs.get(2));
    }
    emitSubstring(exprs, sb);
  }

  private static void emitSubstring(
      List<LogicalTypesParser.Check_exprContext> exprs, StringBuilder sb) {
    emitWrappedReceiver(exprs.get(0), sb);
    sb.append(".substring((");
    visitCheckExpr(exprs.get(1), sb);
    sb.append(") - 1");
    if (exprs.size() > 2) {
      sb.append(", (");
      visitCheckExpr(exprs.get(1), sb);
      sb.append(") - 1 + (");
      visitCheckExpr(exprs.get(2), sb);
      sb.append(')');
    }
    sb.append(')');
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

  /** POSITION(needle IN haystack) → (haystack.indexOf(needle) + 1). */
  private static void visitPosition(
      LogicalTypesParser.FuncPositionContext ctx, StringBuilder sb) {
    sb.append('(');
    emitWrappedReceiver(ctx.check_expr(1), sb);
    sb.append(".indexOf(");
    visitCheckExpr(ctx.check_expr(0), sb);
    sb.append(") + 1)");
  }

  /** TRIM(x) → x.trim() (whitespace, both ends). */
  private static void visitTrim(
      LogicalTypesParser.FuncTrimContext ctx, StringBuilder sb) {
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
    emitWrappedReceiver(exprs.get(0), sb);
    sb.append(".trim()");
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
