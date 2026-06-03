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

import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator.colidName;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

/**
 * Type-resolution + literal-detection helpers extracted from
 * {@link ConstraintToCelTranslator}. Stateless cascade walkers that try to
 * derive a {@link Schema} or literal-shape from a parse-tree node, returning
 * null when the node is too compound to resolve at our coarse granularity
 * (the surrounding validators always defer to runtime in that case).
 *
 * <p>Two flavors of helpers live here:
 * <ul>
 *   <li><b>tryResolve*</b> — best-effort type inference at a given cascade
 *       level. Returns null for compounds; never throws.</li>
 *   <li><b>is*</b> / <b>tryAsLiteral</b> / <b>tryLiteralIntValue</b> — shape
 *       detection used by emit decisions (paren wrapping, NULL guards) and
 *       by validators rejecting NULL in operator positions.</li>
 * </ul>
 *
 * <p>Type-category helpers ({@link #categoryOf}, {@link #areComparable},
 * {@link #isOrderable}, {@link #isIntegral}, {@link #isStringOrBytes}) sit
 * here too because they're consumed alongside the resolvers.
 */
final class ConstraintResolver {

  private ConstraintResolver() {
    // static utility
  }

  // ---------------------------------------------------------------------
  // Cascade-level type resolvers
  // ---------------------------------------------------------------------

  static Schema resolveCollectionElementType(
      LogicalTypesParser.Check_exprContext ctx, ConstraintValidationContext vctx) {
    Schema collType = tryResolveCheckExprType(ctx, vctx);
    if (collType == null) {
      return null;
    }
    if (collType.getType() == Schema.Type.ARRAY
        || collType.getType() == Schema.Type.MULTISET) {
      return collType.getElementType();
    }
    if (collType.getType() == Schema.Type.MAP) {
      return collType.getValueType();
    }
    return null;
  }

  /**
   * Best-effort type resolution for a {@code check_expr_mul}. For a single
   * {@code unary_sign} leaf, delegates to the c_expr resolver below. For a
   * multi-operator chain ({@code a * b}, {@code a / b}, etc.), unifies the
   * operand types: returns the shared type when all operands resolve to
   * the same category; returns null otherwise (mixed → defers, validator
   * catches).
   */
  static Schema tryResolveMulType(
      LogicalTypesParser.Check_expr_mulContext mul, ConstraintValidationContext vctx) {
    return unifyOperandTypes(mul.check_expr_unary_sign(),
        sign -> resolveSingleSignType(sign, vctx));
  }

  /**
   * Best-effort type resolution for a {@code check_expr_add}. Single
   * {@code mul} leaf delegates to {@link #tryResolveMulType}; multi-mul
   * (additive chain {@code a + b - c}) unifies operand categories.
   */
  static Schema tryResolveAddType(
      LogicalTypesParser.Check_expr_addContext add, ConstraintValidationContext vctx) {
    return unifyOperandTypes(add.check_expr_mul(),
        mul -> tryResolveMulType(mul, vctx));
  }

  /**
   * Unify a list of operand types: if all resolve and share a category,
   * return the first type; if any is unresolvable or categories differ,
   * return null. Pure helper used by the arithmetic/concat/multiplicative
   * cascade resolvers.
   */
  private static <T> Schema unifyOperandTypes(
      List<T> operands, java.util.function.Function<T, Schema> resolveOne) {
    if (operands.size() == 1) {
      return resolveOne.apply(operands.get(0));
    }
    Schema first = null;
    String firstCategory = null;
    for (T operand : operands) {
      Schema t = resolveOne.apply(operand);
      if (t == null) {
        return null;  // unresolvable operand — defer
      }
      String cat = categoryOf(t.getType());
      if (cat == null) {
        return null;
      }
      if (first == null) {
        first = t;
        firstCategory = cat;
      } else if (!firstCategory.equals(cat)) {
        return null;  // mixed categories — validator catches
      }
    }
    return first;
  }

  /**
   * Resolve a single c_expr ({@code check_expr_unary_sign} leaf): column
   * ref, literal, paren-wrapped check_expr, or function call. The unary
   * sign itself is irrelevant to the type (negating a numeric stays
   * numeric).
   */
  private static Schema resolveSingleSignType(
      LogicalTypesParser.Check_expr_unary_signContext unarySign,
      ConstraintValidationContext vctx) {
    LogicalTypesParser.C_exprContext c = unarySign.c_expr();
    if (c instanceof LogicalTypesParser.CheckColumnRefContext) {
      return resolveColumnRefType(
          ((LogicalTypesParser.CheckColumnRefContext) c).columnref(), vctx);
    }
    if (c instanceof LogicalTypesParser.CheckLiteralContext) {
      LogicalTypesParser.LiteralContext lit =
          ((LogicalTypesParser.CheckLiteralContext) c).literal();
      if (lit.intLiteral() != null) {
        return Schema.create(Schema.Type.BIGINT);
      }
      if (lit.floatLiteral() != null) {
        return Schema.create(Schema.Type.DOUBLE);
      }
      // TIMESTAMP/INTERVAL literals also carry a stringLiteral child, so they
      // must be checked before the stringLiteral arm below.
      if (lit.TIMESTAMP() != null) {
        return Schema.createTimestampLtz(Schema.NO_PARAM);
      }
      if (lit.INTERVAL() != null) {
        // Duration has no LT Schema type; emit is duration(...) and the strict
        // CEL checker validates duration usage (e.g. timestamp ± interval).
        return null;
      }
      if (lit.stringLiteral() != null) {
        return Schema.createString();
      }
      if (lit.bytesLiteral() != null) {
        return Schema.createBytes();
      }
      if (lit.boolLiteral() != null) {
        return Schema.create(Schema.Type.BOOLEAN);
      }
      // NULL literal — no usable type information.
      return null;
    }
    if (c instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) c;
      // Indirection (e.g. `(arr)[0]`) requires receiver-aware resolution
      // we don't do here — defer.
      if (paren.indirection() != null) {
        return null;
      }
      return tryResolveCheckExprType(paren.check_expr(), vctx);
    }
    if (c instanceof LogicalTypesParser.CheckFuncContext) {
      return tryResolveFuncReturnType(
          ((LogicalTypesParser.CheckFuncContext) c).func_expr(), vctx);
    }
    if (c instanceof LogicalTypesParser.CheckCaseContext) {
      return tryResolveCaseExprType(
          ((LogicalTypesParser.CheckCaseContext) c).case_expr(), vctx);
    }
    return null;
  }

  /**
   * Resolve the result type of a CASE expression by unifying the THEN and
   * ELSE branch types. Returns the first branch's type when all resolvable
   * branches share its category; returns null when branches differ
   * (validator catches separately) or when no branch resolves.
   */
  static Schema tryResolveCaseExprType(
      LogicalTypesParser.Case_exprContext ctx, ConstraintValidationContext vctx) {
    Schema first = null;
    String firstCategory = null;
    for (LogicalTypesParser.When_clauseContext when : ctx.when_clause()) {
      // when_clause has two check_expr children: condition (or value for
      // simple-CASE) and the THEN result. Result is index 1.
      Schema t = tryResolveCheckExprType(when.check_expr(1), vctx);
      if (t == null) {
        continue;
      }
      String cat = categoryOf(t.getType());
      if (cat == null) {
        continue;
      }
      if (first == null) {
        first = t;
        firstCategory = cat;
      } else if (!firstCategory.equals(cat)) {
        return null;  // mixed — validator catches
      }
    }
    // Walk the ELSE expression too. Grammar exposes it as a check_expr that
    // appears after the ELSE keyword as an untagged child of case_expr.
    boolean afterElse = false;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      org.antlr.v4.runtime.tree.ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode
          && "ELSE".equalsIgnoreCase(child.getText())) {
        afterElse = true;
      } else if (afterElse
          && child instanceof LogicalTypesParser.Check_exprContext) {
        Schema t = tryResolveCheckExprType(
            (LogicalTypesParser.Check_exprContext) child, vctx);
        if (t != null) {
          String cat = categoryOf(t.getType());
          if (cat != null) {
            if (first == null) {
              first = t;
            } else if (!firstCategory.equals(cat)) {
              return null;
            }
          }
        }
        break;
      }
    }
    return first;
  }

  /**
   * Resolve the return type of a {@code func_expr}. Mirrors the function
   * whitelist in {@code emitFuncApplication} / the common-subexpr emit
   * methods. Returns null for functions whose return type depends on input
   * we can't resolve.
   */
  static Schema tryResolveFuncReturnType(
      LogicalTypesParser.Func_exprContext fctx, ConstraintValidationContext vctx) {
    if (fctx.func_expr_common_subexpr() != null) {
      return tryResolveCommonSubexprReturnType(fctx.func_expr_common_subexpr(), vctx);
    }
    if (fctx.func_application() == null) {
      return null;
    }
    LogicalTypesParser.Func_applicationContext fa = fctx.func_application();
    String name = fa.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    List<LogicalTypesParser.Check_exprContext> args =
        fa.check_expr_list() != null ? fa.check_expr_list().check_expr() : null;
    switch (name) {
      case "LENGTH":
        return Schema.create(Schema.Type.BIGINT);
      case "UPPER":
      case "LOWER":
      case "REPLACE":
        return Schema.createString();
      case "STARTS_WITH":
      case "ENDS_WITH":
      case "CONTAINS":
      case "MATCHES":
      case "IS_EMAIL":
      case "IS_HOSTNAME":
      case "IS_IPV4":
      case "IS_IPV6":
      case "IS_URI":
      case "IS_URI_REF":
      case "IS_UUID":
      case "EVERY":
      case "ANY":
      case "ONE":
        return Schema.create(Schema.Type.BOOLEAN);
      case "GREATEST":
      case "LEAST":
      case "COALESCE":
      case "NULLIF":
        // Result type matches the (first non-null-resolvable) argument type.
        if (args == null) {
          return null;
        }
        for (LogicalTypesParser.Check_exprContext arg : args) {
          Schema t = tryResolveCheckExprType(arg, vctx);
          if (t != null) {
            return t;
          }
        }
        return null;
      case "ABS":
      case "FLOOR":
      case "CEIL":
      case "CEILING":
      case "ROUND":
      case "TRUNCATE":
        // Magnitude/rounding preserve the operand's numeric type (decimal stays
        // decimal via decimals.*, double stays double via math.*).
        return (args == null || args.isEmpty())
            ? null : tryResolveCheckExprType(args.get(0), vctx);
      case "SQRT": {
        // decimals.sqrt → Decimal; math.sqrt → double.
        if (args == null || args.isEmpty()) {
          return null;
        }
        Schema at = tryResolveCheckExprType(args.get(0), vctx);
        return isDecimal(at) ? at : Schema.create(Schema.Type.DOUBLE);
      }
      case "SIGN": {
        // decimals.sign → int; math.sign preserves the operand type.
        if (args == null || args.isEmpty()) {
          return null;
        }
        Schema at = tryResolveCheckExprType(args.get(0), vctx);
        return isDecimal(at) ? Schema.create(Schema.Type.BIGINT) : at;
      }
      default:
        return null;
    }
  }

  static Schema tryResolveCommonSubexprReturnType(
      LogicalTypesParser.Func_expr_common_subexprContext fctx,
      ConstraintValidationContext vctx) {
    if (fctx instanceof LogicalTypesParser.FuncCastContext) {
      return celTypeFromCastType(
          ((LogicalTypesParser.FuncCastContext) fctx).castType());
    }
    if (fctx instanceof LogicalTypesParser.FuncExtractContext) {
      // EXTRACT returns INT (we emit `int(ts.getX())`).
      return Schema.create(Schema.Type.BIGINT);
    }
    if (fctx instanceof LogicalTypesParser.FuncSubstringFromForContext
        || fctx instanceof LogicalTypesParser.FuncSubstringCommasContext) {
      return Schema.createString();
    }
    if (fctx instanceof LogicalTypesParser.FuncPositionContext) {
      return Schema.create(Schema.Type.BIGINT);
    }
    if (fctx instanceof LogicalTypesParser.FuncTrimContext) {
      return Schema.createString();
    }
    if (fctx instanceof LogicalTypesParser.FuncCurrentTimestampContext) {
      return Schema.createTimestampLtz(Schema.NO_PARAM);
    }
    return null;
  }

  /**
   * Best-effort: derive a schema from a {@code castType} parse-tree node.
   * Mirrors the CAST emit path's recognized targets. Returns null for
   * types our cast emit doesn't support.
   */
  static Schema celTypeFromCastType(
      LogicalTypesParser.CastTypeContext castType) {
    if (castType == null) {
      return null;
    }
    String text = castType.getText().toUpperCase(java.util.Locale.ROOT);
    if (text.startsWith("INT") || text.startsWith("BIGINT")
        || text.startsWith("SMALLINT") || text.startsWith("TINYINT")) {
      return Schema.create(Schema.Type.BIGINT);
    }
    if (text.startsWith("FLOAT") || text.startsWith("DOUBLE")) {
      return Schema.create(Schema.Type.DOUBLE);
    }
    if (text.startsWith("STRING") || text.startsWith("VARCHAR")
        || text.startsWith("CHAR")) {
      return Schema.createString();
    }
    if (text.startsWith("BYTES") || text.startsWith("BINARY")
        || text.startsWith("VARBINARY")) {
      return Schema.createBytes();
    }
    if (text.startsWith("BOOLEAN")) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    if (text.startsWith("DEC") || text.startsWith("NUMERIC")) {
      // CAST(... AS DECIMAL(p,s)) yields a Decimal (the emit applies the scale
      // via decimals.round). startsWith("DEC") covers both DEC and DECIMAL.
      int[] ps = parseDecimalCastParams(text);
      return Schema.createDecimal(ps[0], ps[1]);
    }
    return null;
  }

  /**
   * Parse {@code (precision, scale)} from a DECIMAL cast-type text such as
   * {@code DECIMAL(10,2)} / {@code DEC(10)} / {@code NUMERIC} (any case).
   * Absent precision → {@link Schema#NO_PARAM} (factory applies the default);
   * absent scale → {@link Schema#DEFAULT_DECIMAL_SCALE} (0), matching SQL.
   */
  static int[] parseDecimalCastParams(String castTypeText) {
    int precision = Schema.NO_PARAM;
    int scale = Schema.DEFAULT_DECIMAL_SCALE;
    int lp = castTypeText.indexOf('(');
    if (lp >= 0) {
      int rp = castTypeText.indexOf(')', lp);
      String inner = castTypeText.substring(lp + 1, rp < 0 ? castTypeText.length() : rp);
      String[] parts = inner.split(",");
      try {
        if (parts.length >= 1 && !parts[0].trim().isEmpty()) {
          precision = Integer.parseInt(parts[0].trim());
        }
        if (parts.length >= 2) {
          scale = Integer.parseInt(parts[1].trim());
        }
      } catch (NumberFormatException e) {
        // Malformed params — fall back to defaults; the grammar normally
        // prevents this from being reached.
      }
    }
    return new int[]{precision, scale};
  }

  /**
   * Resolve the type of a {@code check_expr_isnull} that's a pass-through
   * to a single leaf. Mirrors the cascade-walking pattern used elsewhere
   * starting from this specific level.
   */
  static Schema tryResolveIsnullChild(
      LogicalTypesParser.Check_expr_isnullContext is, ConstraintValidationContext vctx) {
    // IS NULL / IS NOT NULL emit a boolean expression. Returning the
    // resolved BOOLEAN lets type-aware validators (concat operands,
    // string-func args, COALESCE/GREATEST/LEAST/NULLIF unification, CASE
    // branches, IN list elements) reject IS NULL operands at parse time
    // instead of letting `bool` slip through to a CEL type-check failure.
    if (is.IS() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_compareContext compare = is.check_expr_compare();
    // `<x> <op> <y>` (size 2) yields boolean. Same goes for LIKE below.
    if (compare.check_expr_like().size() > 1) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_likeContext like = compare.check_expr_like(0);
    if (like.LIKE() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    return tryResolveCheckExprConcatType(like.check_expr_concat(), vctx);
  }

  static Schema tryResolveUnarySignType(
      LogicalTypesParser.Check_expr_unary_signContext sign,
      ConstraintValidationContext vctx) {
    LogicalTypesParser.C_exprContext c = sign.c_expr();
    if (c instanceof LogicalTypesParser.CheckColumnRefContext) {
      return resolveColumnRefType(
          ((LogicalTypesParser.CheckColumnRefContext) c).columnref(), vctx);
    }
    if (c instanceof LogicalTypesParser.CheckLiteralContext) {
      LogicalTypesParser.LiteralContext lit =
          ((LogicalTypesParser.CheckLiteralContext) c).literal();
      if (lit.intLiteral() != null) {
        return Schema.create(Schema.Type.BIGINT);
      }
      if (lit.floatLiteral() != null) {
        return Schema.create(Schema.Type.DOUBLE);
      }
      // TIMESTAMP/INTERVAL literals carry a stringLiteral child — check first.
      if (lit.TIMESTAMP() != null) {
        return Schema.createTimestampLtz(Schema.NO_PARAM);
      }
      if (lit.INTERVAL() != null) {
        return null;  // duration: no LT type; strict CEL checker validates
      }
      if (lit.stringLiteral() != null) {
        return Schema.createString();
      }
      if (lit.bytesLiteral() != null) {
        return Schema.createBytes();
      }
      if (lit.boolLiteral() != null) {
        return Schema.create(Schema.Type.BOOLEAN);
      }
    }
    if (c instanceof LogicalTypesParser.CheckParenContext) {
      // Descend into `(expr)` so wrapped expressions like `(doubleCol + 0)`
      // get resolved (and the modulo-validator can catch them) instead of
      // silently deferring to runtime.
      LogicalTypesParser.CheckParenContext paren =
          (LogicalTypesParser.CheckParenContext) c;
      // Indirection on a paren-wrapped expression (e.g. `(expr).field`)
      // isn't tracked; defer those.
      if (paren.indirection() == null) {
        return tryResolveCheckExprType(paren.check_expr(), vctx);
      }
    }
    return null;
  }

  /**
   * Best-effort type resolution for a {@code check_expr_in} that's a
   * pass-through to a single leaf (column ref or literal). Reuses
   * {@link #tryResolveCheckExprType}'s logic by reconstructing the
   * higher-level cascade — we drop into the same {@code check_expr_isnull}
   * level that {@code check_expr_in} wraps when no IN is present.
   */
  static Schema tryResolveInType(
      LogicalTypesParser.Check_expr_inContext ctx, ConstraintValidationContext vctx) {
    // Same protocol as tryResolveIsnullChild: relational/logical operators
    // yield BOOLEAN; expose that to type-aware callers (BETWEEN bound check,
    // etc.) instead of silently deferring with null.
    if (ctx.IN() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_isnullContext is = ctx.check_expr_isnull();
    if (is.IS() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_compareContext compare = is.check_expr_compare();
    if (compare.check_expr_like().size() > 1) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_likeContext like = compare.check_expr_like(0);
    if (like.LIKE() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    return tryResolveCheckExprConcatType(like.check_expr_concat(), vctx);
  }

  /**
   * Try to resolve the type of a {@code check_expr} that's a pass-through
   * cascade to a single leaf (column ref or literal). Mirrors
   * {@link #tryResolveMulType}'s coarseness for upstream callers that need
   * to peer down through the full {@code check_expr} ladder.
   */
  static Schema tryResolveCheckExprType(
      LogicalTypesParser.Check_exprContext ctx, ConstraintValidationContext vctx) {
    // Logical (OR/AND/NOT), relational (BETWEEN/IN/IS NULL/comparison/LIKE)
    // operators all yield BOOLEAN. Returning the resolved boolean lets
    // type-aware callers (concat operands, function args, CASE branches,
    // IN list elements, etc.) reject bool operands at parse time.
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() > 1) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_unary_notContext unaryNot = and.check_expr_unary_not(0);
    if (unaryNot instanceof LogicalTypesParser.CheckExprNotContext) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_betweenContext between =
        ((LogicalTypesParser.CheckExprNotPassContext) unaryNot).check_expr_between();
    if (between.BETWEEN() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_inContext in = between.check_expr_in(0);
    if (in.IN() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_isnullContext is = in.check_expr_isnull();
    if (is.IS() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_compareContext compare = is.check_expr_compare();
    if (compare.check_expr_like().size() > 1) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    LogicalTypesParser.Check_expr_likeContext like = compare.check_expr_like(0);
    if (like.LIKE() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    return tryResolveCheckExprConcatType(like.check_expr_concat(), vctx);
  }

  /**
   * Resolve the type of a {@code check_expr_like} for the comparison and
   * LIKE-receiver validators' use. Returns:
   * <ul>
   *   <li>{@code BOOLEAN} when the cascade is a {@code LIKE} test (the
   *       result of LIKE is bool).</li>
   *   <li>For pass-through cascades down to a single c_expr leaf,
   *       delegates to {@link #tryResolveMulType} — which covers column
   *       refs, literals (int/float/string/bytes/bool), paren-wrapped
   *       expressions, and function calls (via
   *       {@link #tryResolveFuncReturnType}).</li>
   *   <li>{@code null} for compound expressions with operators at the
   *       concat/additive/multiplicative levels (we defer those to
   *       runtime).</li>
   * </ul>
   *
   * <p>Despite the historical name, this helper now resolves much more
   * than just simple column refs — it's the comparison validator's
   * primary type resolver for a {@code check_expr_like} operand.
   */
  static Schema tryResolveSimpleColumnRefType(
      LogicalTypesParser.Check_expr_likeContext like, ConstraintValidationContext vctx) {
    if (like.LIKE() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    return tryResolveCheckExprConcatType(like.check_expr_concat(), vctx);
  }

  /**
   * Resolve the type of a {@code check_expr_concat} cascade. For a single
   * {@code add} operand, delegates to {@link #tryResolveAddType}. For a
   * multi-{@code ||} chain, unifies operand categories — returns the
   * shared category type when all operands resolve consistently, null
   * otherwise. Used by the LIKE-receiver validator and the comparison
   * validator's resolver chain.
   */
  static Schema tryResolveCheckExprConcatType(
      LogicalTypesParser.Check_expr_concatContext concat,
      ConstraintValidationContext vctx) {
    return unifyOperandTypes(concat.check_expr_add(),
        add -> tryResolveAddType(add, vctx));
  }

  // -------------------------------------------------------------------------
  // Decimal detection for the emitter's decimals.* dispatch.
  //
  // Unlike the unify-or-null resolvers above (which return null on mixed
  // categories), these answer "does this node have ANY decimal operand?" —
  // because a mixed chain like `decimalCol + intCol` must still dispatch to
  // decimals.* (the non-decimal operand is coerced with decimal(...) at emit).
  // -------------------------------------------------------------------------

  /**
   * True if {@code s} resolves to the DECIMAL category.
   */
  static boolean isDecimal(Schema s) {
    return s != null && "decimal".equals(categoryOf(s.getType()));
  }

  /**
   * True if {@code s} is an instant-bearing timestamp ({@code TIMESTAMP} or
   * {@code TIMESTAMP_LTZ}). These are the temporal types the emitter normalizes
   * with {@code timestamp.of(...)} (DATE/TIME are partial temporals and out of
   * scope). The runtime surfaces these as Instant/proto Timestamp/RFC-3339
   * string, none of which CEL's native timestamp ops accept un-normalized.
   */
  static boolean isInstantTimestamp(Schema s) {
    if (s == null) {
      return false;
    }
    Schema.Type t = s.getType();
    return t == Schema.Type.TIMESTAMP || t == Schema.Type.TIMESTAMP_LTZ;
  }

  static boolean likeHasDecimal(
      LogicalTypesParser.Check_expr_likeContext like, ConstraintValidationContext vctx) {
    if (like.LIKE() != null) {
      return false;  // LIKE yields bool, never decimal
    }
    return concatHasDecimal(like.check_expr_concat(), vctx);
  }

  static boolean concatHasDecimal(
      LogicalTypesParser.Check_expr_concatContext concat, ConstraintValidationContext vctx) {
    for (LogicalTypesParser.Check_expr_addContext add : concat.check_expr_add()) {
      if (addHasDecimal(add, vctx)) {
        return true;
      }
    }
    return false;
  }

  static boolean addHasDecimal(
      LogicalTypesParser.Check_expr_addContext add, ConstraintValidationContext vctx) {
    for (LogicalTypesParser.Check_expr_mulContext mul : add.check_expr_mul()) {
      if (mulHasDecimal(mul, vctx)) {
        return true;
      }
    }
    return false;
  }

  static boolean mulHasDecimal(
      LogicalTypesParser.Check_expr_mulContext mul, ConstraintValidationContext vctx) {
    for (LogicalTypesParser.Check_expr_unary_signContext sign : mul.check_expr_unary_sign()) {
      if (isDecimal(resolveSingleSignType(sign, vctx))) {
        return true;
      }
    }
    return false;
  }

  /**
   * True if {@code node}'s subtree contains an instant-timestamp column
   * reference or a TIMESTAMP/INTERVAL literal — i.e. the expression needs the
   * timestamp-value emit (column leaves wrapped in {@code timestamp.of(...)},
   * literals as {@code timestamp("…")}/{@code duration("…")}).
   *
   * <p>A generic parse-tree walk rather than a per-level cascade: over-detection
   * is harmless because the timestamp-value emitter reproduces the native output
   * exactly except at timestamp-column leaves (which are always correct to wrap).
   */
  static boolean subtreeHasTemporal(ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.CheckColumnRefContext) {
      return isInstantTimestamp(resolveColumnRefType(
          ((LogicalTypesParser.CheckColumnRefContext) node).columnref(), vctx));
    }
    if (node instanceof LogicalTypesParser.LiteralContext) {
      LogicalTypesParser.LiteralContext lit = (LogicalTypesParser.LiteralContext) node;
      return lit.TIMESTAMP() != null || lit.INTERVAL() != null;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (subtreeHasTemporal(node.getChild(i), vctx)) {
        return true;
      }
    }
    return false;
  }

  /**
   * If {@code s} is a {@code NAMED_TYPE_REF}, resolve it via the validation
   * context's named-type table. Returns null if the ref can't be resolved
   * (caller defers to runtime). Returns {@code s} unchanged for non-refs.
   */
  static Schema resolveIfNamedRef(Schema s, ConstraintValidationContext vctx) {
    if (s == null || s.getType() != Schema.Type.NAMED_TYPE_REF) {
      return s;
    }
    return vctx.resolveNamedType(s);
  }

  /**
   * Resolve the type of {@code col}, walking through any indirection. Returns
   * null if the root name isn't a known column (validation will catch that
   * separately) or if the indirection can't be resolved at our coarse
   * granularity (we follow {@code .field} into structs but stop at array/map
   * indexing where the type changes per element).
   */
  static Schema resolveColumnRefType(
      LogicalTypesParser.ColumnrefContext col, ConstraintValidationContext vctx) {
    String rootName = colidName(col.colid());
    Schema current = resolveIfNamedRef(vctx.schemaOf(rootName), vctx);
    if (current == null) {
      return null;
    }
    if (col.indirection() == null) {
      return current;
    }
    for (LogicalTypesParser.Indirection_elContext el : col.indirection().indirection_el()) {
      if (el.colid() != null && current.getType() == Schema.Type.STRUCT) {
        Schema.Field nested = current.getField(colidName(el));
        if (nested == null) {
          return null;
        }
        current = resolveIfNamedRef(nested.getSchema(), vctx);
      } else if (el.check_expr() != null && current.getType() == Schema.Type.ARRAY) {
        current = current.getElementType();
      } else if (el.check_expr() != null && current.getType() == Schema.Type.MAP) {
        current = current.getValueType();
      } else {
        return null;
      }
    }
    return current;
  }

  // ---------------------------------------------------------------------
  // Type-category helpers
  // ---------------------------------------------------------------------

  static boolean isStringOrBytes(Schema.Type t) {
    switch (t) {
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        return true;
      default:
        return false;
    }
  }

  static boolean isIntegral(Schema.Type t) {
    switch (t) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return true;
      default:
        return false;
    }
  }

  static boolean isOrderable(Schema.Type t) {
    switch (t) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return true;
      default:
        return false;
    }
  }

  /**
   * True if two LT types are CEL-comparable. Categories must match exactly:
   * CEL has no implicit int↔double promotion in {@code _>_}/{@code _==_}
   * etc. (cel-java's strict checker rejects {@code int > double}). Same for
   * decimal, which isn't a built-in CEL type and rides in its own category.
   */
  static boolean areComparable(Schema.Type a, Schema.Type b) {
    if (a == b) {
      return true;
    }
    String ca = categoryOf(a);
    String cb = categoryOf(b);
    if (ca == null || cb == null) {
      return false;
    }
    if (ca.equals(cb)) {
      return true;
    }
    // DECIMAL coerces a numeric counterpart (int/double) via decimal(...) at
    // emit, so decimal-vs-int and decimal-vs-double are comparable. int-vs-double
    // stays incomparable — CEL has no implicit int↔double promotion and the
    // emitted native operators would fail the strict checker.
    return ("decimal".equals(ca) && isNumericCategory(cb))
        || ("decimal".equals(cb) && isNumericCategory(ca));
  }

  private static boolean isNumericCategory(String category) {
    return "int".equals(category) || "double".equals(category) || "decimal".equals(category);
  }

  static String categoryOf(Schema.Type t) {
    switch (t) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return "int";
      case FLOAT:
      case DOUBLE:
        return "double";
      case DECIMAL:
        return "decimal";
      case CHAR:
      case VARCHAR:
        return "string";
      case BINARY:
      case VARBINARY:
        return "bytes";
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return "datetime";
      case BOOLEAN:
        return "boolean";
      default:
        return null;
    }
  }

  // ---------------------------------------------------------------------
  // Literal/primary detection
  // ---------------------------------------------------------------------

  /**
   * True if {@code c} is a self-delimited c_expr alternative — column ref,
   * literal, function call, paren expression, or CASE block. These all emit
   * as a single tightly-bound token sequence that doesn't need defensive
   * paren wrapping.
   */
  static boolean isPrimaryCExpr(LogicalTypesParser.C_exprContext c) {
    if (c instanceof LogicalTypesParser.CheckColumnRefContext) {
      return true;
    }
    if (c instanceof LogicalTypesParser.CheckLiteralContext) {
      return true;
    }
    if (c instanceof LogicalTypesParser.CheckFuncContext) {
      return true;
    }
    if (c instanceof LogicalTypesParser.CheckParenContext) {
      return true;
    }
    return c instanceof LogicalTypesParser.CheckCaseContext;
  }

  /**
   * {@link #isSimplePrimary} starting from a {@code check_expr_in} level.
   */
  static boolean isSimplePrimaryIn(
      LogicalTypesParser.Check_expr_inContext ctx) {
    if (ctx.IN() != null) {
      return false;
    }
    LogicalTypesParser.Check_expr_isnullContext is = ctx.check_expr_isnull();
    if (is.IS() != null) {
      return false;
    }
    LogicalTypesParser.Check_expr_compareContext compare = is.check_expr_compare();
    if (compare.check_expr_like().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_likeContext like = compare.check_expr_like(0);
    if (like.LIKE() != null) {
      return false;
    }
    return isSimplePrimaryConcat(like.check_expr_concat());
  }

  /**
   * If {@code ctx} is a pass-through cascade ending at a single literal
   * (descending through paren wrappers and unary {@code -}/{@code +} signs),
   * return the literal context plus the cumulative negation. Returns null
   * for compound expressions — callers defer to runtime in that case.
   *
   * <p>Each cascade level has a dedicated entry point used when the caller
   * already holds a sub-context: {@link #tryAsLiteralIn} (BETWEEN bound),
   * {@link #tryAsLiteralLike} (comparison operand). Each one delegates
   * down to the next level so the cascade descent isn't duplicated.
   */
  static LiteralMatch tryAsLiteral(
      LogicalTypesParser.Check_exprContext ctx) {
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_unary_notContext unaryNot = and.check_expr_unary_not(0);
    if (unaryNot instanceof LogicalTypesParser.CheckExprNotContext) {
      return null;
    }
    LogicalTypesParser.Check_expr_betweenContext between =
        ((LogicalTypesParser.CheckExprNotPassContext) unaryNot).check_expr_between();
    if (between.BETWEEN() != null) {
      return null;
    }
    return tryAsLiteralIn(between.check_expr_in(0));
  }

  /**
   * {@link #tryAsLiteral} starting from a {@code check_expr_in} level.
   */
  static LiteralMatch tryAsLiteralIn(
      LogicalTypesParser.Check_expr_inContext in) {
    if (in.IN() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_isnullContext is = in.check_expr_isnull();
    if (is.IS() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_compareContext compare = is.check_expr_compare();
    if (compare.check_expr_like().size() > 1) {
      return null;
    }
    return tryAsLiteralLike(compare.check_expr_like(0));
  }

  /**
   * {@link #tryAsLiteral} starting from a {@code check_expr_like} level.
   */
  static LiteralMatch tryAsLiteralLike(
      LogicalTypesParser.Check_expr_likeContext like) {
    if (like.LIKE() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_concatContext concat = like.check_expr_concat();
    if (concat.check_expr_add().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_addContext add = concat.check_expr_add(0);
    if (add.check_expr_mul().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_mulContext mul = add.check_expr_mul(0);
    if (mul.check_expr_unary_sign().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_unary_signContext sign = mul.check_expr_unary_sign(0);
    boolean negate = false;
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode && "-".equals(child.getText())) {
        negate = true;
      }
    }
    LogicalTypesParser.C_exprContext c = sign.c_expr();
    if (c instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) c;
      // Indirection (e.g. `(arr)[1]`) breaks the literal-only invariant.
      if (paren.indirection() != null) {
        return null;
      }
      LiteralMatch inner = tryAsLiteral(paren.check_expr());
      if (inner == null) {
        return null;
      }
      return new LiteralMatch(inner.lit, negate ^ inner.negate);
    }
    if (!(c instanceof LogicalTypesParser.CheckLiteralContext)) {
      return null;
    }
    return new LiteralMatch(
        ((LogicalTypesParser.CheckLiteralContext) c).literal(), negate);
  }

  /**
   * True if {@code ctx} emits a single tightly-bound primary that doesn't
   * need defensive paren-wrapping when used as a BETWEEN operand, as a
   * receiver before a CEL {@code .method()} / {@code [index]}, as the
   * operand of {@code !}, or as either side of CASE simple-form's implicit
   * {@code ==}. A primary is a column ref, literal, function call (already
   * parenthesized), CASE block (self-delimited by END), or already-
   * parenthesized expression. Compound expressions (any operator at any
   * cascade level, including unary sign) need wrapping because the
   * surrounding CEL operator could pull them apart by precedence.
   *
   * <p>Each cascade level has a dedicated entry point used when the
   * caller already holds a sub-context: {@link #isSimplePrimaryUnaryNot}
   * (NOT operand), {@link #isSimplePrimaryBetween} (NOT body),
   * {@link #isSimplePrimaryIn} (BETWEEN bound), {@link #isSimplePrimaryConcat}
   * (LIKE/method receiver). Each one delegates down to the next level so
   * the cascade descent isn't duplicated across helpers.
   */
  static boolean isSimplePrimary(
      LogicalTypesParser.Check_exprContext ctx) {
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() > 1) {
      return false;
    }
    return isSimplePrimaryUnaryNot(and.check_expr_unary_not(0));
  }

  /**
   * {@link #isSimplePrimary} starting from a {@code check_expr_unary_not}
   * level — used by NOT emit, which inspects its inner operand.
   *
   * <p>Chained NOT (e.g. {@code NOT NOT x}) recurses through the inner
   * NOT, so {@code !!x} can emit unwrapped if the leaf is a primary.
   */
  static boolean isSimplePrimaryUnaryNot(
      LogicalTypesParser.Check_expr_unary_notContext ctx) {
    if (ctx instanceof LogicalTypesParser.CheckExprNotContext) {
      return isSimplePrimaryUnaryNot(
          ((LogicalTypesParser.CheckExprNotContext) ctx).check_expr_unary_not());
    }
    return isSimplePrimaryBetween(
        ((LogicalTypesParser.CheckExprNotPassContext) ctx).check_expr_between());
  }

  /**
   * {@link #isSimplePrimary} starting from a {@code check_expr_between}
   * level.
   */
  static boolean isSimplePrimaryBetween(
      LogicalTypesParser.Check_expr_betweenContext between) {
    if (between.BETWEEN() != null) {
      return false;
    }
    return isSimplePrimaryIn(between.check_expr_in(0));
  }

  /**
   * {@link #isSimplePrimary} starting from a {@code check_expr_concat}
   * level — used by emit sites whose receiver is exposed at the concat
   * level (LIKE → matches).
   */
  static boolean isSimplePrimaryConcat(
      LogicalTypesParser.Check_expr_concatContext concat) {
    if (concat.check_expr_add().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_addContext add = concat.check_expr_add(0);
    if (add.check_expr_mul().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_mulContext mul = add.check_expr_mul(0);
    if (mul.check_expr_unary_sign().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_unary_signContext sign = mul.check_expr_unary_sign(0);
    // A unary sign on the receiver would be wrong (e.g. `-x.method()` parses
    // as `-(x.method())` in CEL). For BETWEEN it'd be safe but rare; keep
    // the check conservative so one helper covers all call sites.
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode
          && ("-".equals(child.getText()) || "+".equals(child.getText()))) {
        return false;
      }
    }
    return isPrimaryCExpr(sign.c_expr());
  }

  /**
   * Detect if {@code ctx} is a bare {@code NULL} literal (possibly wrapped
   * in parens). Used by validators that reject {@code NULL} in operator
   * positions where it would emit unsound CEL (e.g. {@code x = NULL} or
   * {@code BETWEEN NULL AND ...}).
   */
  static boolean isLiteralNull(
      LogicalTypesParser.Check_exprContext ctx) {
    return isLiteralNullMatch(tryAsLiteral(ctx));
  }

  /**
   * Like {@link #isLiteralNull(LogicalTypesParser.Check_exprContext)} but
   * starting from a {@code check_expr_like} (one cascade level below
   * {@code check_expr}, no LIKE operator), used by the comparison validator.
   */
  static boolean isLiteralNullLikeChild(
      LogicalTypesParser.Check_expr_likeContext like) {
    return isLiteralNullMatch(tryAsLiteralLike(like));
  }

  private static boolean isLiteralNullMatch(LiteralMatch m) {
    return m != null && m.lit.NULL() != null;
  }

  /**
   * If {@code ctx} is a pass-through cascade ending at a single int literal
   * (with optional unary {@code -}/{@code +} sign and paren wrapping), return
   * its long value. Returns null for any compound expression.
   */
  static Long tryLiteralIntValue(
      LogicalTypesParser.Check_exprContext ctx) {
    LiteralMatch m = tryAsLiteral(ctx);
    if (m == null || m.lit.intLiteral() == null) {
      return null;
    }
    String text = m.lit.intLiteral().getText();
    try {
      long value = Long.parseLong(text);
      return m.negate ? -value : value;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * True if a {@code check_expr_in} bound resolves (after pass-through
   * cascade descent and through paren wrappers) to a bare NULL literal,
   * with optional unary {@code +}/{@code -} signs. Used by BETWEEN's
   * null-bound rejection — structural so it catches {@code NULL},
   * {@code (NULL)}, {@code +NULL}, {@code -NULL}, {@code -(NULL)} etc.,
   * which the previous text-based check missed.
   */
  static boolean isBoundLiteralNull(
      LogicalTypesParser.Check_expr_inContext bound) {
    return isLiteralNullMatch(tryAsLiteralIn(bound));
  }

  // ---------------------------------------------------------------------
  // has() compatibility detection (selection-shape rule)
  // ---------------------------------------------------------------------

  /**
   * True if {@code ctx} would emit as a CEL field-selection chain that's a
   * valid argument to the {@code has()} macro. Used by IS NULL / IS NOT NULL
   * and the COALESCE / GREATEST / LEAST null-guard emits to wrap the null
   * comparison with a presence check (protovalidate convention) so absent
   * proto fields, which CEL surfaces as their typed defaults rather than
   * null, still test correctly.
   *
   * <p>Selection-shape rule: {@code has()} requires a field-selection
   * expression {@code e.f}. The chain is valid iff:
   * <ul>
   *   <li>The expression is a pass-through cascade ending at a single
   *       {@link LogicalTypesParser.CheckColumnRefContext} (no operators,
   *       no unary sign).</li>
   *   <li>The emit produces a dotted form ending in {@code .field} — see
   *       {@link #isHasCompatibleColumnRef}.</li>
   * </ul>
   */
  static boolean isHasCompatible(
      LogicalTypesParser.Check_exprContext ctx, ConstraintValidationContext vctx) {
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_unary_notContext unaryNot = and.check_expr_unary_not(0);
    if (unaryNot instanceof LogicalTypesParser.CheckExprNotContext) {
      return false;
    }
    LogicalTypesParser.Check_expr_betweenContext between =
        ((LogicalTypesParser.CheckExprNotPassContext) unaryNot).check_expr_between();
    if (between.BETWEEN() != null) {
      return false;
    }
    LogicalTypesParser.Check_expr_inContext in = between.check_expr_in(0);
    if (in.IN() != null) {
      return false;
    }
    LogicalTypesParser.Check_expr_isnullContext is = in.check_expr_isnull();
    if (is.IS() != null) {
      return false;
    }
    return isHasCompatible(is.check_expr_compare(), vctx);
  }

  /**
   * {@link #isHasCompatible(LogicalTypesParser.Check_exprContext,
   * ConstraintValidationContext)} starting from a {@code check_expr_compare}
   * level — used by IS NULL / IS NOT NULL emit (which descends one level
   * into the cascade for its LHS).
   */
  static boolean isHasCompatible(
      LogicalTypesParser.Check_expr_compareContext ctx,
      ConstraintValidationContext vctx) {
    if (ctx.check_expr_like().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_likeContext like = ctx.check_expr_like(0);
    if (like.LIKE() != null) {
      return false;
    }
    LogicalTypesParser.Check_expr_concatContext concat = like.check_expr_concat();
    if (concat.check_expr_add().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_addContext add = concat.check_expr_add(0);
    if (add.check_expr_mul().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_mulContext mul = add.check_expr_mul(0);
    if (mul.check_expr_unary_sign().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_unary_signContext sign = mul.check_expr_unary_sign(0);
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode
          && ("-".equals(child.getText()) || "+".equals(child.getText()))) {
        return false;
      }
    }
    return isHasCompatibleCExpr(sign.c_expr(), vctx);
  }

  /**
   * True if {@code c} resolves (through paren wrappers, with or without
   * indirection) to an expression whose emit is a CEL selection chain
   * usable as a {@code has()} argument.
   *
   * <p>Three cases:
   * <ul>
   *   <li>Bare column ref: delegate to
   *       {@link #isHasCompatibleColumnRef}.</li>
   *   <li>Paren with no indirection ({@code ((x))}): recurse through the
   *       wrapped expression — paren-only wrapping doesn't change the
   *       emit's selection shape, so {@code ((x)) IS NULL} gets the same
   *       has-rewrite as bare {@code x IS NULL}.</li>
   *   <li>Paren with indirection ({@code (x).y}): the indirection's
   *       {@code .field} steps make the chain end in a selection. The
   *       wrapped expression must itself be has-compatible (so the
   *       receiver before {@code .field} is a valid selection chain), and
   *       the indirection chain must be pure dotted (no {@code [...]} or
   *       CEL-reserved names).</li>
   * </ul>
   */
  private static boolean isHasCompatibleCExpr(
      LogicalTypesParser.C_exprContext c, ConstraintValidationContext vctx) {
    if (c instanceof LogicalTypesParser.CheckColumnRefContext) {
      return isHasCompatibleColumnRef(
          ((LogicalTypesParser.CheckColumnRefContext) c).columnref(), vctx);
    }
    if (c instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) c;
      if (paren.indirection() == null) {
        return isHasCompatible(paren.check_expr(), vctx);
      }
      // Paren-with-indirection: emit is `(<wrapped>).<field>...`. Both the
      // wrapped expression must be has-compatible (otherwise the receiver
      // isn't a valid selection chain) and the indirection chain must be
      // pure dotted (no brackets, no CEL-reserved names).
      if (!isHasCompatible(paren.check_expr(), vctx)) {
        return false;
      }
      return isPureDottedIndirection(paren.indirection());
    }
    return false;
  }

  private static boolean isPureDottedIndirection(
      LogicalTypesParser.IndirectionContext ind) {
    for (LogicalTypesParser.Indirection_elContext el : ind.indirection_el()) {
      if (el.colid() == null) {
        return false;  // [...] step
      }
      if (Schema.isCelReservedName(colidName(el))) {
        return false;
      }
    }
    return true;
  }

  /**
   * True if {@code col} would emit as a CEL field-selection chain. Mirrors
   * {@link ConstraintEmitter}'s emit decisions:
   * <ul>
   *   <li>Schema column in table-level CHECK → emits {@code this.<name>} —
   *       a selection (so the chain is selection-shaped even with no
   *       indirection). Reject when the name is CEL-reserved (the emit uses
   *       {@code this["name"]} bracket form, which {@code has()} rejects).</li>
   *   <li>Schema column in column-level CHECK → emits bare {@code this} —
   *       NOT a selection. Need at least one {@code .field} indirection step
   *       to make the chain end in {@code .field}.</li>
   *   <li>Macro binding / runtime variable → emits bare {@code <name>} —
   *       NOT a selection. Same: need at least one {@code .field} step.</li>
   * </ul>
   *
   * <p>Indirection chain restrictions (apply in all cases):
   * {@code [...]} steps disqualify (CEL has() rejects index expressions
   * like {@code has(arr[0])}); a CEL-reserved field name disqualifies
   * (the emit uses bracket syntax {@code ["name"]}).
   */
  private static boolean isHasCompatibleColumnRef(
      LogicalTypesParser.ColumnrefContext col, ConstraintValidationContext vctx) {
    String rootName = colidName(col.colid());
    boolean rootEmitsAsSelection =
        vctx.isColumn(rootName) && !vctx.isColumnLevel();
    if (rootEmitsAsSelection && Schema.isCelReservedName(rootName)) {
      // table-level emit collides → `this["name"]` (bracket form, has-invalid)
      return false;
    }
    int fieldSteps = 0;
    if (col.indirection() != null) {
      for (LogicalTypesParser.Indirection_elContext el
          : col.indirection().indirection_el()) {
        if (el.colid() != null) {
          if (Schema.isCelReservedName(colidName(el))) {
            return false;
          }
          fieldSteps++;
        } else {
          // [...] step — has() rejects index expressions
          return false;
        }
      }
    }
    // Note on ARRAY/MULTISET/MAP roots: we INTENTIONALLY apply the has()
    // rewrite to collection columns. CEL spec: `has(repeated_field)`
    // returns false for empty collections (proto3 has no presence for
    // repeated). So `arr IS NULL` emits `(!has(this.arr) ||
    // dyn(this.arr) == null)` and fires for both empty and absent —
    // matching the protovalidate convention where empty/absent are
    // equivalent for repeated fields. SQL purists may expect strict
    // NULL-only semantics, but for proto3 repeated fields the strict
    // interpretation has no meaningful runtime value (the field can never
    // actually be NULL). See visitIs javadoc for details.
    return rootEmitsAsSelection || fieldSteps > 0;
  }

  static final class LiteralMatch {
    final LogicalTypesParser.LiteralContext lit;
    final boolean negate;

    LiteralMatch(LogicalTypesParser.LiteralContext lit, boolean negate) {
      this.lit = lit;
      this.negate = negate;
    }
  }

  // ---------------------------------------------------------------------
  // Function-call leaf extraction + null-return classification
  // ---------------------------------------------------------------------

  /**
   * Functions and special-syntax forms whose return type is statically
   * non-nullable. Used by the IS NULL validator to reject dead-code
   * constraints like {@code LENGTH(name) IS NULL} — the emit produces
   * {@code dyn(size(this.name)) == null} which is well-typed but
   * unconditionally false at runtime.
   *
   * <p>NULLIF is NOT in this set — it returns null when its two args
   * compare equal. COALESCE is also not in this set: it returns the first
   * non-null arg (and the all-NULL-args case is already rejected by the
   * literal-NULL pass).
   */
  static final java.util.Set<String> NEVER_NULL_FUNCS =
      java.util.Collections.unmodifiableSet(new java.util.HashSet<>(
          java.util.Arrays.asList(
              "LENGTH", "UPPER", "LOWER", "REPLACE",
              "STARTS_WITH", "ENDS_WITH", "CONTAINS", "MATCHES",
              "IS_EMAIL", "IS_HOSTNAME", "IS_IPV4", "IS_IPV6",
              "IS_URI", "IS_URI_REF", "IS_UUID",
              "EVERY", "ANY", "ONE",
              "GREATEST", "LEAST",
              // Special-syntax forms:
              "CAST", "EXTRACT", "SUBSTRING", "POSITION", "TRIM",
              "CURRENT_TIMESTAMP")));

  /**
   * Functions that CAN return null. Used by the BETWEEN-bound validator
   * to reject {@code x BETWEEN NULLIF(a,b) AND 10} — the emit produces a
   * valid CEL ternary but a runtime null result hits a comparison op
   * with no null overload.
   */
  static final java.util.Set<String> NULL_RETURNING_FUNCS =
      java.util.Collections.unmodifiableSet(new java.util.HashSet<>(
          java.util.Arrays.asList("NULLIF")));

  /**
   * If {@code ctx} is a pass-through cascade ending in a single function-
   * call c_expr (no operators, no unary sign, no indirection, no paren
   * wrapping a non-func), return the function's uppercase name. Otherwise
   * return null. Common-syntax forms (CAST, EXTRACT, SUBSTRING, POSITION,
   * TRIM, CURRENT_TIMESTAMP) are also detected.
   *
   * <p>Each cascade level has a dedicated entry point used when the
   * caller already holds a sub-context: {@link #tryExtractLeafFuncNameIn}
   * (BETWEEN bound), {@link #tryExtractLeafFuncNameCompare} (IS NULL
   * operand). Each one delegates down to the next level so the cascade
   * descent isn't duplicated.
   */
  static String tryExtractLeafFuncName(
      LogicalTypesParser.Check_exprContext ctx) {
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_unary_notContext unaryNot = and.check_expr_unary_not(0);
    if (unaryNot instanceof LogicalTypesParser.CheckExprNotContext) {
      return null;
    }
    LogicalTypesParser.Check_expr_betweenContext between =
        ((LogicalTypesParser.CheckExprNotPassContext) unaryNot).check_expr_between();
    if (between.BETWEEN() != null) {
      return null;
    }
    return tryExtractLeafFuncNameIn(between.check_expr_in(0));
  }

  /**
   * {@link #tryExtractLeafFuncName} starting from a {@code check_expr_in}
   * level — used by the BETWEEN-bound validator (H-2).
   */
  static String tryExtractLeafFuncNameIn(
      LogicalTypesParser.Check_expr_inContext ctx) {
    if (ctx.IN() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_isnullContext is = ctx.check_expr_isnull();
    if (is.IS() != null) {
      return null;
    }
    return tryExtractLeafFuncNameCompare(is.check_expr_compare());
  }

  /**
   * {@link #tryExtractLeafFuncName} starting from a {@code check_expr_compare}
   * level — used by the IS NULL operand validator (H-3).
   */
  static String tryExtractLeafFuncNameCompare(
      LogicalTypesParser.Check_expr_compareContext ctx) {
    if (ctx.check_expr_like().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_likeContext like = ctx.check_expr_like(0);
    if (like.LIKE() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_concatContext concat = like.check_expr_concat();
    if (concat.check_expr_add().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_addContext add = concat.check_expr_add(0);
    if (add.check_expr_mul().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_mulContext mul = add.check_expr_mul(0);
    if (mul.check_expr_unary_sign().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_unary_signContext sign = mul.check_expr_unary_sign(0);
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode
          && ("-".equals(child.getText()) || "+".equals(child.getText()))) {
        return null;
      }
    }
    return funcNameOfCExpr(sign.c_expr());
  }

  /**
   * Walk through paren-only wrapping ({@code ((expr))}) to a leaf
   * c_expr, returning the function name if the leaf is a function call.
   * Returns null otherwise. Paren-with-indirection ({@code (f(...)).x})
   * isn't a function call any more — return null.
   */
  private static String funcNameOfCExpr(LogicalTypesParser.C_exprContext c) {
    if (c instanceof LogicalTypesParser.CheckFuncContext) {
      return funcNameOfFuncExpr(((LogicalTypesParser.CheckFuncContext) c).func_expr());
    }
    if (c instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) c;
      if (paren.indirection() != null) {
        return null;
      }
      return tryExtractLeafFuncName(paren.check_expr());
    }
    return null;
  }

  /**
   * Map a {@code func_expr} to the function name our NEVER_NULL_FUNCS /
   * NULL_RETURNING_FUNCS sets recognize. Function-application form
   * returns the identifier; common-subexpr forms return their canonical
   * name (CAST, EXTRACT, SUBSTRING, etc.).
   */
  private static String funcNameOfFuncExpr(
      LogicalTypesParser.Func_exprContext fctx) {
    if (fctx.func_application() != null) {
      return fctx.func_application().identifier().getText()
          .toUpperCase(java.util.Locale.ROOT);
    }
    LogicalTypesParser.Func_expr_common_subexprContext sub =
        fctx.func_expr_common_subexpr();
    if (sub instanceof LogicalTypesParser.FuncCastContext) {
      return "CAST";
    }
    if (sub instanceof LogicalTypesParser.FuncExtractContext) {
      return "EXTRACT";
    }
    if (sub instanceof LogicalTypesParser.FuncSubstringFromForContext
        || sub instanceof LogicalTypesParser.FuncSubstringCommasContext) {
      return "SUBSTRING";
    }
    if (sub instanceof LogicalTypesParser.FuncPositionContext) {
      return "POSITION";
    }
    if (sub instanceof LogicalTypesParser.FuncTrimContext) {
      return "TRIM";
    }
    if (sub instanceof LogicalTypesParser.FuncCurrentTimestampContext) {
      return "CURRENT_TIMESTAMP";
    }
    return null;
  }

  // ---------------------------------------------------------------------
  // Column-ref leaf extraction + nullability resolution
  // ---------------------------------------------------------------------

  /**
   * If {@code ctx} is a pass-through cascade ending in a single column-ref
   * leaf (no operators, no unary sign, paren-only wrappers OK), return the
   * underlying {@link LogicalTypesParser.ColumnrefContext}. Otherwise null.
   *
   * <p>Each cascade level has a dedicated entry point used when the
   * caller already holds a sub-context: {@link #tryExtractLeafColumnRefCompare}
   * (IS NULL operand). Each one delegates down to the next level so the
   * cascade descent isn't duplicated.
   */
  static LogicalTypesParser.ColumnrefContext tryExtractLeafColumnRef(
      LogicalTypesParser.Check_exprContext ctx) {
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_unary_notContext unaryNot = and.check_expr_unary_not(0);
    if (unaryNot instanceof LogicalTypesParser.CheckExprNotContext) {
      return null;
    }
    LogicalTypesParser.Check_expr_betweenContext between =
        ((LogicalTypesParser.CheckExprNotPassContext) unaryNot).check_expr_between();
    if (between.BETWEEN() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_inContext in = between.check_expr_in(0);
    if (in.IN() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_isnullContext is = in.check_expr_isnull();
    if (is.IS() != null) {
      return null;
    }
    return tryExtractLeafColumnRefCompare(is.check_expr_compare());
  }

  /**
   * {@link #tryExtractLeafColumnRef} starting from a {@code check_expr_compare}
   * level — used by the IS NULL operand validator (H-3 / B-8).
   */
  static LogicalTypesParser.ColumnrefContext tryExtractLeafColumnRefCompare(
      LogicalTypesParser.Check_expr_compareContext ctx) {
    if (ctx.check_expr_like().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_likeContext like = ctx.check_expr_like(0);
    if (like.LIKE() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_concatContext concat = like.check_expr_concat();
    if (concat.check_expr_add().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_addContext add = concat.check_expr_add(0);
    if (add.check_expr_mul().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_mulContext mul = add.check_expr_mul(0);
    if (mul.check_expr_unary_sign().size() > 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_unary_signContext sign = mul.check_expr_unary_sign(0);
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode
          && ("-".equals(child.getText()) || "+".equals(child.getText()))) {
        return null;
      }
    }
    return columnRefOfCExpr(sign.c_expr());
  }

  private static LogicalTypesParser.ColumnrefContext columnRefOfCExpr(
      LogicalTypesParser.C_exprContext c) {
    if (c instanceof LogicalTypesParser.CheckColumnRefContext) {
      return ((LogicalTypesParser.CheckColumnRefContext) c).columnref();
    }
    if (c instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) c;
      if (paren.indirection() != null) {
        return null;
      }
      return tryExtractLeafColumnRef(paren.check_expr());
    }
    return null;
  }

  /**
   * True if every step in {@code col}'s indirection chain (root included)
   * is declared non-nullable, indicating the path can never produce null.
   * Returns false for any nullable step, unknown root, indirection through
   * ARRAY/MULTISET/MAP indexing (the element/value could be missing), or
   * any unresolvable step — i.e. the conservative "defer to runtime" answer.
   */
  static boolean isWhollyNonNullableColumnRef(
      LogicalTypesParser.ColumnrefContext col, ConstraintValidationContext vctx) {
    String rootName = colidName(col.colid());
    Schema rootField = vctx.schemaOf(rootName);
    if (rootField == null || rootField.isNullable()) {
      return false;
    }
    Schema current = resolveIfNamedRef(rootField, vctx);
    if (col.indirection() == null) {
      return current != null;
    }
    for (LogicalTypesParser.Indirection_elContext el : col.indirection().indirection_el()) {
      if (el.colid() != null && current != null
          && current.getType() == Schema.Type.STRUCT) {
        Schema.Field nested = current.getField(colidName(el));
        if (nested == null || nested.getSchema().isNullable()) {
          return false;
        }
        current = resolveIfNamedRef(nested.getSchema(), vctx);
      } else {
        // Indexing into a collection, or an unresolvable step — element /
        // value access can produce missing/default values; defer.
        return false;
      }
    }
    return current != null;
  }
}
