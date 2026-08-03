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

import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;

/**
 * Public entry point for translating a parsed SQL CHECK expression
 * (postgres-derived subset) to a CEL expression string.
 *
 * <p>The work is split across siblings in this package:
 * <ul>
 *   <li>{@link ConstraintValidator} — pre-emit validation passes (boolean
 *       root, column-ref resolution, type-aware checks).</li>
 *   <li>{@link ConstraintEmitter} — cascade walkers that turn the parse tree
 *       into CEL fragments.</li>
 *   <li>{@link ConstraintFunctions} — function dispatch and special-syntax
 *       forms (CAST, EXTRACT, SUBSTRING, etc.).</li>
 *   <li>{@link ConstraintResolver} — best-effort type/literal resolution
 *       used by both validators and emitters.</li>
 *   <li>{@link ConstraintPatterns} — pure SQL→CEL string/regex helpers.</li>
 * </ul>
 *
 * <p>This file holds: the public {@link #translate} entry, the per-translation
 * {@link #EMIT_VCTX} ThreadLocal, the located-error builder, and the colid
 * unquoting helpers. Everyone in the constraint package shares those —
 * keeping them here avoids an extra utility file for three small helpers.
 *
 * <p>The translator is purely text-emitting — no CEL evaluator is bundled or
 * required. The output is a well-formed CEL expression string that consumers
 * (the SR rules engine, Flink runtime, etc.) can evaluate with their own CEL
 * evaluator.
 */
public final class ConstraintToCelTranslator {

  /**
   * Per-translation validation context, set by {@link #translate} and read by
   * emit-time helpers in {@link ConstraintEmitter} that need vctx access:
   * indirection emit (receiver type → ARRAY index conversion vs MAP keying)
   * and column-ref emit (protovalidate {@code this}/{@code this.x} prefix
   * placement). ThreadLocal so concurrent translations on different threads
   * don't race; cleared in a finally block.
   */
  static final ThreadLocal<ConstraintValidationContext> EMIT_VCTX =
      new ThreadLocal<>();

  private ConstraintToCelTranslator() {
  }

  /**
   * Translate a {@code check_expr} parse tree to CEL. Runs strict parse-time
   * validation passes (boolean root, column-ref resolution, ordered-type and
   * cross-type checks, macro arity, etc.) before emitting; errors include
   * source position. Column refs are emitted using the protovalidate
   * {@code this} convention based on the placement signaled by
   * {@code validationContext}: column-level CHECK refs collapse to
   * {@code this}; table-level refs become {@code this.<col>}.
   */
  public static String translate(
      LogicalTypesParser.Check_exprContext ctx,
      ConstraintValidationContext validationContext) {
    if (validationContext == null) {
      throw new IllegalArgumentException(
          "validationContext is required — the translator emits a "
              + "validation-context-aware shape (this/this.x prefixing) and "
              + "cannot run without one.");
    }
    ConstraintValidator.enforceMaxParseDepth(ctx);
    ConstraintValidator.validateBooleanRoot(ctx, validationContext);
    ConstraintValidator.validateColumnRefs(ctx, validationContext);
    ConstraintValidator.validateLiterals(ctx);
    ConstraintValidator.validateLikePatterns(ctx, validationContext);
    ConstraintValidator.validateComparisons(ctx, validationContext);
    ConstraintValidator.validateConcatOperands(ctx, validationContext);
    ConstraintValidator.validateArithmeticOperands(ctx, validationContext);
    ConstraintValidator.validateCaseBranches(ctx, validationContext);
    ConstraintValidator.validateBetweenOperands(ctx, validationContext);
    ConstraintValidator.validateInList(ctx, validationContext);
    ConstraintValidator.validateFunctionArgs(ctx, validationContext);
    ConstraintValidator.validateIsNullOperand(ctx, validationContext);
    ConstraintValidator.validateMacroBodies(ctx, validationContext);
    EMIT_VCTX.set(validationContext);
    try {
      StringBuilder sb = new StringBuilder();
      ConstraintEmitter.visitCheckExpr(ctx, sb);
      String cel = sb.toString();
      // Post-emit safety net: strict cel-java type-check the result. Catches
      // type/overload mismatches the hand-coded validator passes don't cover
      // (compound expressions, function-result types, has() argument shape,
      // etc.). Throws ValidationException on failure.
      ConstraintCelChecker.strictCheck(cel, validationContext, ctx.getText());
      return cel;
    } finally {
      EMIT_VCTX.remove();
    }
  }

  /**
   * Build a {@link ValidationException} carrying the source line/col of the
   * offending parser context.
   */
  static ValidationException locatedError(
      org.antlr.v4.runtime.ParserRuleContext ctx, String message) {
    int line = ctx.getStart().getLine();
    int col = ctx.getStart().getCharPositionInLine() + 1;
    return new ValidationException("CHECK constraint at line " + line
        + ", col " + col + ": " + message);
  }

  /**
   * Strip backticks (and unescape doubled backticks) from a {@code colid}
   * token's source text. The grammar's {@code colid} rule is
   * {@code identifier | nonReservedKeyword}, where {@code identifier}
   * accepts {@code QUOTED_ID} (backtick-wrapped). Without unquoting,
   * {@code colid().getText()} returns the raw {@code `name`} form, which
   * mismatches schema field names (stored unquoted) at lookup time and
   * produces invalid CEL at emit time. Mirrors
   * {@code LogicalTypesSchemaVisitor#buildIdentifier}.
   */
  static String colidName(LogicalTypesParser.ColidContext ctx) {
    return unquoteId(ctx.getText());
  }

  /**
   * Same unquoting for the indirection element's {@code colid} (the
   * {@code .field} step in {@code col.field}).
   */
  static String colidName(LogicalTypesParser.Indirection_elContext el) {
    return unquoteId(el.colid().getText());
  }

  static String unquoteId(String raw) {
    if (raw.length() >= 2 && raw.charAt(0) == '`'
        && raw.charAt(raw.length() - 1) == '`') {
      return raw.substring(1, raw.length() - 1).replace("``", "`");
    }
    return raw;
  }
}
