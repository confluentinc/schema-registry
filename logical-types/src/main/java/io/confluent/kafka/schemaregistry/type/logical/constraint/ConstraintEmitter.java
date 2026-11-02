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
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator.colidName;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator.locatedError;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Cascade visitors for the CHECK→CEL translator. Each visitor walks one
 * level of the postgres-derived CHECK grammar and emits the equivalent CEL
 * fragment into the caller's {@link StringBuilder}. The cascade reflects
 * SQL operator precedence — top to bottom: OR, AND, NOT, IS NULL,
 * comparison, BETWEEN, IN, LIKE, additive, multiplicative ({@code ||}
 * concat lives here), unary sign, c_expr (column ref / literal / paren /
 * case / func).
 *
 * <p>Visitors run after the validator suite has accepted the parse tree,
 * so they can assume well-formed input — no {@code null} guards, no
 * shape-checking past what's needed for the emit decision (e.g. wrap vs.
 * pass-through). Cross-cascade jumps (e.g. value-cascade emitters called
 * directly from {@link ConstraintFunctions}'s function-arg emit) keep
 * package-private visibility.
 *
 * <p>{@link #emitWrappedReceiver} and {@link #emitParenIn} are the two
 * defensive paren-wrappers shared by macro-call emit, BETWEEN bound emit,
 * and instance-method emit. Both check for "simple primary" shape (column
 * ref, literal, function call, paren, case) and skip the wrap when the
 * operand is already self-delimited.
 */
final class ConstraintEmitter {

  private ConstraintEmitter() {
    // static utility
  }

  /**
   * The CEL of each subexpression emitted during one translation, by context and form. A NULL
   * test or a common type emits a subexpression again, which, nested, would multiply the work
   * at each level.
   */
  static final class EmitCache {
    private final String sourceSql;
    private final Map<ConstraintValidationContext, Map<ParseTree, String[]>> emitted =
        new IdentityHashMap<>();
    private final Map<ConstraintValidationContext, Map<ParseTree, String[]>> nullTests =
        new IdentityHashMap<>();
    private final Map<ConstraintValidationContext, Map<ParseTree, ConstraintValidationContext>>
        bodies = new IdentityHashMap<>();
    private final Map<ConstraintValidationContext, Map<ParseTree, Truth[]>> truths =
        new IdentityHashMap<>();

    EmitCache(String sourceSql) {
      this.sourceSql = sourceSql;
    }
  }

  // A quantifier body's emitTruth in one truth, with its flags.
  private static final class Truth {
    private final String cel;
    private final int flags;

    Truth(String cel, int flags) {
      this.cel = cel;
      this.flags = flags;
    }
  }

  static final ThreadLocal<EmitCache> EMIT_CACHE = new ThreadLocal<>();

  private enum Form { NATIVE, DECIMAL, DOUBLE, TIMESTAMP }

  // ctx's CEL in a form, emitted once per translation and context. One over CEL's size limit is
  // refused at once, as nesting may multiply it further.
  private static void emitCached(ParseTree ctx, Form form,
      StringBuilder sb, Consumer<StringBuilder> emit) {
    EmitCache cache = EMIT_CACHE.get();
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (cache == null || vctx == null) {
      emit.accept(sb);
      return;
    }
    String[] forms = cache.emitted.computeIfAbsent(vctx, k -> new IdentityHashMap<>())
        .computeIfAbsent(ctx, k -> new String[Form.values().length]);
    if (forms[form.ordinal()] == null) {
      StringBuilder cel = new StringBuilder();
      emit.accept(cel);
      forms[form.ordinal()] = cel.toString();
      ConstraintCelChecker.checkSize(forms[form.ordinal()], true, cache.sourceSql);
    }
    sb.append(forms[form.ordinal()]);
  }

  static void visitCheckExpr(
      LogicalTypesParser.Check_exprContext ctx, StringBuilder sb) {
    emitCached(ctx, Form.NATIVE, sb, out -> visitOr(ctx.check_expr_or(), out));
  }

  /**
   * A node's NULL test, computed once per translation and context, and refused at once over
   * CEL's size limit; null when it can't be told.
   */
  static String cachedNullTest(ParseTree node, Supplier<String> test) {
    EmitCache cache = EMIT_CACHE.get();
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (cache == null || vctx == null) {
      return test.get();
    }
    Map<ParseTree, String[]> tests =
        cache.nullTests.computeIfAbsent(vctx, k -> new IdentityHashMap<>());
    String[] held = tests.get(node);
    if (held == null) {
      held = new String[] {test.get()};
      if (held[0] != null) {
        ConstraintCelChecker.checkSize(held[0], true, cache.sourceSql);
      }
      tests.put(node, held);
    }
    return held[0];
  }

  /**
   * The context a quantifier's body is emitted in, made once per translation and outer context,
   * so the body's emissions are cached across its quantifier's.
   */
  static ConstraintValidationContext bodyContext(ParseTree quantifier,
      ConstraintValidationContext outer, Supplier<ConstraintValidationContext> make) {
    EmitCache cache = EMIT_CACHE.get();
    if (cache == null) {
      return make.get();
    }
    return cache.bodies.computeIfAbsent(outer, k -> new IdentityHashMap<>())
        .computeIfAbsent(quantifier, k -> make.get());
  }

  private static void visitOr(
      LogicalTypesParser.Check_expr_orContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_andContext> ands = ctx.check_expr_and();
    for (int i = 0; i < ands.size(); i++) {
      if (i > 0) {
        sb.append(" || ");
      }
      visitAnd(ands.get(i), sb);
    }
  }

  private static void visitAnd(
      LogicalTypesParser.Check_expr_andContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_unary_notContext> nots = ctx.check_expr_unary_not();
    for (int i = 0; i < nots.size(); i++) {
      if (i > 0) {
        sb.append(" && ");
      }
      visitUnaryNot(nots.get(i), sb);
    }
  }

  /**
   * BETWEEN translation:
   * <ul>
   *   <li>{@code a BETWEEN b AND c} → {@code b <= a && a <= c}</li>
   *   <li>{@code a NOT BETWEEN b AND c} → {@code !(b <= a && a <= c)}</li>
   *   <li>{@code a BETWEEN SYMMETRIC b AND c} →
   *       {@code (b <= c ? (b <= a && a <= c) : (c <= a && a <= b))}</li>
   *   <li>{@code a NOT BETWEEN SYMMETRIC b AND c} → {@code !(...)}</li>
   * </ul>
   *
   * <p>The bounds are themselves {@code check_expr_in}, which can't contain a
   * top-level AND — so the inner AND between bounds parses unambiguously.
   */
  private static void visitBetween(
      LogicalTypesParser.Check_expr_betweenContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_inContext> ins = ctx.check_expr_in();
    if (ins.size() == 1) {
      visitIn(ins.get(0), sb);
      return;
    }
    // Three operands: subject, lower, upper
    boolean negated = ctx.NOT() != null;
    boolean symmetric = ctx.SYMMETRIC() != null;
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    // Numeric coercion: route by the common type over all three operands.
    // DECIMAL → decimals.le cascade; DOUBLE → native <= with operands cast to
    // double; INT (or non-numeric) → fall through to temporal/native below.
    String common = (vctx != null) ? numericCommonOf(ins, vctx) : null;
    if ("decimal".equals(common)) {
      emitBetweenDecimal(ins, negated, symmetric, sb);
      return;
    }
    if ("double".equals(common)) {
      emitBetweenDouble(ins, negated, symmetric, sb);
      return;
    }
    if (vctx != null
        && (ConstraintResolver.subtreeHasTemporal(ins.get(0), vctx)
            || ConstraintResolver.subtreeHasTemporal(ins.get(1), vctx)
            || ConstraintResolver.subtreeHasTemporal(ins.get(2), vctx))) {
      emitBetweenTimestamp(ins, negated, symmetric, sb);
      return;
    }
    if (negated) {
      sb.append("!(");
    }
    // Bounds and subject are individually wrapped in parens so the rewrite
    // is precedence-safe regardless of what the user wrote inside. The
    // grammar admits `check_expr_in` for each operand, which can descend
    // through IS NULL / NOT / comparison / additive — without parens, e.g.
    // `x BETWEEN y IS NULL AND z` would emit `y == null <= x && x <= z`
    // which CEL parses as `((y == null) <= x) && ...` (a type error).
    if (symmetric) {
      // (lo <= hi ? (lo <= a && a <= hi) : (hi <= a && a <= lo))
      sb.append("(");
      emitParenIn(ins.get(1), sb);
      sb.append(" <= ");
      emitParenIn(ins.get(2), sb);
      sb.append(" ? (");
      emitParenIn(ins.get(1), sb);
      sb.append(" <= ");
      emitParenIn(ins.get(0), sb);
      sb.append(" && ");
      emitParenIn(ins.get(0), sb);
      sb.append(" <= ");
      emitParenIn(ins.get(2), sb);
      sb.append(") : (");
      emitParenIn(ins.get(2), sb);
      sb.append(" <= ");
      emitParenIn(ins.get(0), sb);
      sb.append(" && ");
      emitParenIn(ins.get(0), sb);
      sb.append(" <= ");
      emitParenIn(ins.get(1), sb);
      sb.append("))");
    } else {
      // lo <= a && a <= hi
      emitParenIn(ins.get(1), sb);
      sb.append(" <= ");
      emitParenIn(ins.get(0), sb);
      sb.append(" && ");
      emitParenIn(ins.get(0), sb);
      sb.append(" <= ");
      emitParenIn(ins.get(2), sb);
    }
    if (negated) {
      sb.append(")");
    }
  }

  /**
   * Decimal BETWEEN: {@code lo <= a && a <= hi} becomes
   * {@code decimals.le(lo, a) && decimals.le(a, hi)} (SYMMETRIC orders the
   * bounds with a decimals.le test). Operands emitted via the decimal cascade.
   */
  private static void emitBetweenDecimal(
      List<LogicalTypesParser.Check_expr_inContext> ins,
      boolean negated, boolean symmetric, StringBuilder sb) {
    if (negated) {
      sb.append("!(");
    }
    if (symmetric) {
      sb.append('(');
      emitDecimalLe(ins.get(1), ins.get(2), sb);   // lo <= hi ?
      sb.append(" ? (");
      emitDecimalLe(ins.get(1), ins.get(0), sb);
      sb.append(" && ");
      emitDecimalLe(ins.get(0), ins.get(2), sb);
      sb.append(") : (");
      emitDecimalLe(ins.get(2), ins.get(0), sb);
      sb.append(" && ");
      emitDecimalLe(ins.get(0), ins.get(1), sb);
      sb.append("))");
    } else {
      emitDecimalLe(ins.get(1), ins.get(0), sb);
      sb.append(" && ");
      emitDecimalLe(ins.get(0), ins.get(2), sb);
    }
    if (negated) {
      sb.append(")");
    }
  }

  /**
   * {@code decimals.le(a, b)} over two BETWEEN operands.
   */
  private static void emitDecimalLe(
      LogicalTypesParser.Check_expr_inContext a,
      LogicalTypesParser.Check_expr_inContext b, StringBuilder sb) {
    sb.append("decimals.le(");
    emitInAsDecimal(a, sb);
    sb.append(", ");
    emitInAsDecimal(b, sb);
    sb.append(')');
  }

  /**
   * Double BETWEEN: {@code lo <= a && a <= hi} with native {@code <=}, each
   * operand cast to double (int → {@code double(x)}, decimal → {@code double(x)}
   * / plain double literal). Mirrors {@link #emitBetweenDecimal}'s structure.
   */
  private static void emitBetweenDouble(
      List<LogicalTypesParser.Check_expr_inContext> ins,
      boolean negated, boolean symmetric, StringBuilder sb) {
    if (negated) {
      sb.append("!(");
    }
    if (symmetric) {
      sb.append('(');
      emitDoubleLe(ins.get(1), ins.get(2), sb);   // lo <= hi ?
      sb.append(" ? (");
      emitDoubleLe(ins.get(1), ins.get(0), sb);
      sb.append(" && ");
      emitDoubleLe(ins.get(0), ins.get(2), sb);
      sb.append(") : (");
      emitDoubleLe(ins.get(2), ins.get(0), sb);
      sb.append(" && ");
      emitDoubleLe(ins.get(0), ins.get(1), sb);
      sb.append("))");
    } else {
      emitDoubleLe(ins.get(1), ins.get(0), sb);
      sb.append(" && ");
      emitDoubleLe(ins.get(0), ins.get(2), sb);
    }
    if (negated) {
      sb.append(")");
    }
  }

  /**
   * {@code a <= b} over two BETWEEN operands, both cast to double.
   */
  private static void emitDoubleLe(
      LogicalTypesParser.Check_expr_inContext a,
      LogicalTypesParser.Check_expr_inContext b, StringBuilder sb) {
    emitNumericInAs(a, "double", sb);
    sb.append(" <= ");
    emitNumericInAs(b, "double", sb);
  }

  /**
   * Emit a {@code check_expr_in} wrapped in {@code (...)}. Used by
   * {@link #visitBetween} so each BETWEEN operand is precedence-safe in the
   * surrounding {@code <=} chain. Skips the wrap when the operand is a
   * simple primary (column ref, literal, function call, paren, case) since
   * those are already self-delimited and don't need defensive parens.
   */
  static void emitParenIn(
      LogicalTypesParser.Check_expr_inContext ctx, StringBuilder sb) {
    if (ConstraintResolver.isSimplePrimaryIn(ctx)) {
      visitIn(ctx, sb);
      return;
    }
    sb.append('(');
    visitIn(ctx, sb);
    sb.append(')');
  }

  /**
   * IN translation:
   * <ul>
   *   <li>{@code a IN (x, y, z)} → {@code a in [x, y, z]}</li>
   *   <li>{@code a IN list_field} → {@code a in list_field}</li>
   *   <li>{@code a NOT IN ...} → {@code !(a in ...)}</li>
   *   <li>a value list with an element that may be NULL → three values, see
   *       {@link #emitInMayBeNull}</li>
   * </ul>
   */
  private static void visitIn(
      LogicalTypesParser.Check_expr_inContext ctx, StringBuilder sb) {
    if (ctx.IN() == null) {
      visitLike(ctx.check_expr_like(), sb);
      return;
    }
    boolean negated = ctx.NOT() != null;
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (inElementMayBeNull(ctx)) {
      emitInMayBeNull(ctx, negated, sb);
      return;
    }
    if (inArrayElementMayBeNull(ctx)) {
      emitInArrayMayBeNull(ctx, negated, sb);
      return;
    }
    if (vctx != null) {
      LogicalTypesParser.In_targetContext target = ctx.in_target();
      if (target instanceof LogicalTypesParser.InTargetParenListContext) {
        // Numeric coercion over LHS + value-list elements. DECIMAL: CEL's
        // `in` has no Decimal overload, so emit an OR of decimals.eq. DOUBLE:
        // `double(lhs) in [double(e), …]`. INT/non-numeric falls through.
        String common = inListCommon(ctx, vctx);
        if ("decimal".equals(common)) {
          emitInDecimal(ctx, negated, sb);
          return;
        }
        if ("double".equals(common)) {
          emitInDouble(ctx, negated, sb);
          return;
        }
      } else if (likeHasDecimal(ctx.check_expr_like(), vctx)) {
        // Decimal LHS against a list-typed operand: emitInDecimal raises the
        // (unsupported) located error, preserving the prior clear message.
        emitInDecimal(ctx, negated, sb);
        return;
      }
    }
    // Timestamp: CEL's native `in` works on timestamps once the column LHS and
    // any column elements are normalized — emit the same `x in [...]` shape via
    // the timestamp-value cascade (no per-element rewrite needed, unlike decimal).
    String guard = emptyGuard(ctx);
    boolean wrapped = negated || !guard.isEmpty();
    if (wrapped) {
      sb.append(negated ? "!(" : "(");
    }
    sb.append(guard);
    if (vctx != null && inTargetHasTemporal(ctx, vctx)) {
      emitTimestampValueLike(ctx.check_expr_like(), sb);
      sb.append(" in ");
      emitTimestampInTarget(ctx.in_target(), sb);
    } else {
      visitLike(ctx.check_expr_like(), sb);
      sb.append(" in ");
      visitInTarget(ctx.in_target(), sb);
    }
    if (wrapped) {
      sb.append(")");
    }
  }

  // Before a match against a collection: false over an empty one, without evaluating a subject
  // that may be NULL, or absent on JSON, as SQL's = ANY over no elements is FALSE.
  private static String emptyGuard(LogicalTypesParser.Check_expr_inContext ctx) {
    if (EMIT_VCTX.get() == null
        || !(ctx.in_target() instanceof LogicalTypesParser.InTargetExprContext)
        || ConstraintNullTest.NEVER.equals(ConstraintNullTest.of(ctx.check_expr_like()))) {
      return "";
    }
    return collectionSize(((LogicalTypesParser.InTargetExprContext) ctx.in_target())
        .check_expr_like()) + " == 0 ? false : ";
  }

  // The size of the collection an IN tests.
  static String collectionSize(LogicalTypesParser.Check_expr_likeContext target) {
    StringBuilder cel = new StringBuilder();
    visitLike(target, cel);
    return "size(" + typedCollection(cel) + ")";
  }

  /**
   * True if an element of an IN's value list may be NULL, and the inputs of each value tell.
   * One whose elements never are is NULL exactly when its subject is, so is emitted as before.
   */
  static boolean inElementMayBeNull(LogicalTypesParser.Check_expr_inContext ctx) {
    if (EMIT_VCTX.get() == null || ctx.IN() == null
        || !(ctx.in_target() instanceof LogicalTypesParser.InTargetParenListContext)
        || ConstraintNullTest.of(ctx.check_expr_like()) == null) {
      return false;
    }
    boolean mayBeNull = false;
    for (LogicalTypesParser.Check_exprContext item : inItems(ctx)) {
      String test = ConstraintNullTest.of(item);
      if (test == null) {
        return false;
      }
      mayBeNull |= !ConstraintNullTest.NEVER.equals(test);
    }
    return mayBeNull;
  }

  /**
   * SQL's {@code x IN (a, b)}, {@code x = a OR x = b} in three values: NULL when {@code x} is,
   * else TRUE on a match, else NULL when an element is NULL, else FALSE; NOT IN negates it,
   * keeping NULL. Each value is tested before it is compared or converted to the common type.
   */
  private static void emitInMayBeNull(
      LogicalTypesParser.Check_expr_inContext ctx, boolean negated, StringBuilder sb) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    List<LogicalTypesParser.Check_exprContext> items = inItems(ctx);
    String common = inListCommon(ctx, vctx);
    boolean temporal = !"decimal".equals(common) && !"double".equals(common)
        && inTargetHasTemporal(ctx, vctx);
    StringBuilder subject = new StringBuilder();
    if ("decimal".equals(common)) {
      emitLikeAsDecimalValue(ctx.check_expr_like(), subject);
    } else if ("double".equals(common)) {
      emitDoubleValueLike(ctx.check_expr_like(), subject);
    } else if (temporal) {
      emitTimestampValueLike(ctx.check_expr_like(), subject);
    } else {
      visitLike(ctx.check_expr_like(), subject);
    }
    List<String> elementNulls = new ArrayList<>();
    StringBuilder match = new StringBuilder("(");
    for (int i = 0; i < items.size(); i++) {
      String elementNull = ConstraintNullTest.of(items.get(i));
      elementNulls.add(elementNull);
      if (i > 0) {
        match.append(" || ");
      }
      if (!ConstraintNullTest.NEVER.equals(elementNull)) {
        match.append('(').append(ConstraintNullTest.not(elementNull)).append(" && ");
      }
      if ("decimal".equals(common)) {
        match.append("decimals.eq(").append(subject).append(", ");
        emitDecimalValue(items.get(i), match);
        match.append(')');
      } else {
        match.append(subject).append(" == ");
        if ("double".equals(common)) {
          emitDoubleValueCheckExpr(items.get(i), match);
        } else if (temporal) {
          emitTimestampValueCheckExpr(items.get(i), match);
        } else {
          emitWrappedReceiver(items.get(i), match);
        }
      }
      if (!ConstraintNullTest.NEVER.equals(elementNull)) {
        match.append(')');
      }
    }
    match.append(')');
    String subjectNull = ConstraintNullTest.of(ctx.check_expr_like());
    String anyElementNull = ConstraintNullTest.any(elementNulls);
    if (vctx != null && vctx.isTypeOnly()) {
      sb.append(negated ? "!" : "").append(match);
      return;
    }
    StringBuilder noMatch = new StringBuilder();
    if (ConstraintNullTest.NEVER.equals(anyElementNull)) {
      noMatch.append(negated ? "true" : "false");
    } else {
      noMatch.append('(').append(anyElementNull).append(" ? dyn(null) : ")
          .append(negated ? "true" : "false").append(')');
    }
    String unlessSubjectNull = "(" + match + " ? " + (negated ? "false" : "true") + " : "
        + noMatch + ")";
    if (ConstraintNullTest.NEVER.equals(subjectNull)) {
      sb.append(unlessSubjectNull);
    } else {
      sb.append('(').append(subjectNull).append(" ? dyn(null) : ").append(unlessSubjectNull)
          .append(')');
    }
  }

  /**
   * True if an element of the array an IN tests may be NULL, and the inputs of its subject and
   * array tell. One whose elements never are is emitted as before, NULL as its inputs tell.
   */
  static boolean inArrayElementMayBeNull(LogicalTypesParser.Check_expr_inContext ctx) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (vctx == null || ctx.IN() == null
        || !(ctx.in_target() instanceof LogicalTypesParser.InTargetExprContext)
        || likeHasDecimal(ctx.check_expr_like(), vctx)) {
      return false;
    }
    String subject = ConstraintNullTest.of(ctx.check_expr_like());
    String array = ConstraintNullTest.of(
        ((LogicalTypesParser.InTargetExprContext) ctx.in_target()).check_expr_like());
    Schema element = ConstraintResolver.inArrayElementType(ctx, vctx);
    return subject != null && array != null && element != null && element.isNullable();
  }

  /**
   * SQL's {@code x IN arr}: NULL when {@code arr} is, or {@code x} is and {@code arr} isn't
   * empty, else TRUE on a match, else NULL when an element is NULL, else FALSE; NOT IN negates
   * it, keeping NULL.
   */
  private static void emitInArrayMayBeNull(
      LogicalTypesParser.Check_expr_inContext ctx, boolean negated, StringBuilder sb) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    LogicalTypesParser.Check_expr_likeContext array =
        ((LogicalTypesParser.InTargetExprContext) ctx.in_target()).check_expr_like();
    StringBuilder match = new StringBuilder(emptyGuard(ctx));
    if (inTargetHasTemporal(ctx, vctx)) {
      emitTimestampValueLike(ctx.check_expr_like(), match);
      match.append(" in ");
      emitTimestampInTarget(ctx.in_target(), match);
    } else {
      visitLike(ctx.check_expr_like(), match);
      match.append(" in ");
      visitInTarget(ctx.in_target(), match);
    }
    if (vctx.isTypeOnly()) {
      sb.append(negated ? "!(" : "(").append(match).append(')');
      return;
    }
    StringBuilder noMatch = new StringBuilder("((");
    visitLike(array, noMatch);
    noMatch.append(").exists(v, dyn(v) == null) ? dyn(null) : ")
        .append(negated ? "true" : "false").append(')');
    String unlessNull = "((" + match + ") ? " + (negated ? "false" : "true") + " : "
        + noMatch + ")";
    String isNull = ConstraintNullTest.ofCollectionIn(ctx,
        ConstraintNullTest.of(ctx.check_expr_like()));
    if (ConstraintNullTest.NEVER.equals(isNull)) {
      sb.append(unlessNull);
    } else {
      sb.append('(').append(isNull).append(" ? dyn(null) : ").append(unlessNull).append(')');
    }
  }

  private static List<LogicalTypesParser.Check_exprContext> inItems(
      LogicalTypesParser.Check_expr_inContext ctx) {
    return ((LogicalTypesParser.InTargetParenListContext) ctx.in_target())
        .check_expr_list().check_expr();
  }

  /** True if the IN LHS or any value-list element is temporal. */
  private static boolean inTargetHasTemporal(
      LogicalTypesParser.Check_expr_inContext ctx, ConstraintValidationContext vctx) {
    if (ConstraintResolver.subtreeHasTemporal(ctx.check_expr_like(), vctx)) {
      return true;
    }
    LogicalTypesParser.In_targetContext target = ctx.in_target();
    return target instanceof LogicalTypesParser.InTargetParenListContext
        && ConstraintResolver.subtreeHasTemporal(target, vctx);
  }

  /** Emit an IN value list (or list-field) with each element timestamp-normalized. */
  private static void emitTimestampInTarget(
      LogicalTypesParser.In_targetContext ctx, StringBuilder sb) {
    if (ctx instanceof LogicalTypesParser.InTargetParenListContext) {
      List<LogicalTypesParser.Check_exprContext> items =
          ((LogicalTypesParser.InTargetParenListContext) ctx).check_expr_list().check_expr();
      sb.append('[');
      for (int i = 0; i < items.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        emitTimestampValueCheckExpr(items.get(i), sb);
      }
      sb.append(']');
    } else {
      // IN <list-field>: the field is a list of timestamps; CEL `in` handles it
      // natively. The receiver still normalizes via the cascade (field ref).
      StringBuilder target = new StringBuilder();
      emitTimestampValueLike(
          ((LogicalTypesParser.InTargetExprContext) ctx).check_expr_like(), target);
      sb.append(typedSubscript(target));
    }
  }

  static void emitTimestampValueLike(
      LogicalTypesParser.Check_expr_likeContext like, StringBuilder sb) {
    if (like.LIKE() == null) {
      emitTimestampValueAdd(like.check_expr_add(), sb);
    } else {
      visitLike(like, sb);  // not a bare value (defensive); emit native
    }
  }

  /**
   * Decimal {@code a IN (x, y, …)} → {@code (decimals.eq(a, x) || decimals.eq(a, y) || …)}
   * (negated wraps with {@code !}). The {@code IN <list-field>} form has no
   * Decimal-aware equivalent and is rejected.
   */
  private static void emitInDecimal(
      LogicalTypesParser.Check_expr_inContext ctx, boolean negated, StringBuilder sb) {
    LogicalTypesParser.In_targetContext target = ctx.in_target();
    if (!(target instanceof LogicalTypesParser.InTargetParenListContext)) {
      throw locatedError(ctx,
          "IN against a list-typed operand is not supported for DECIMAL; use an "
              + "explicit value list, e.g. amount IN (1.0, 2.5, 9.99).");
    }
    final List<LogicalTypesParser.Check_exprContext> items =
        ((LogicalTypesParser.InTargetParenListContext) target).check_expr_list().check_expr();
    StringBuilder lhsBuf = new StringBuilder();
    emitLikeAsDecimalValue(ctx.check_expr_like(), lhsBuf);
    String lhs = lhsBuf.toString();
    if (negated) {
      sb.append("!(");
    }
    sb.append('(');
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(" || ");
      }
      sb.append("decimals.eq(").append(lhs).append(", ");
      emitDecimalValue(items.get(i), sb);
      sb.append(')');
    }
    sb.append(')');
    if (negated) {
      sb.append(")");
    }
  }

  /**
   * Double IN: {@code double(lhs) in [double(e1), double(e2), …]}. CEL's native
   * {@code in} does element equality, which works for double once every element
   * is cast to double.
   */
  private static void emitInDouble(
      LogicalTypesParser.Check_expr_inContext ctx, boolean negated, StringBuilder sb) {
    final List<LogicalTypesParser.Check_exprContext> items =
        ((LogicalTypesParser.InTargetParenListContext) ctx.in_target())
            .check_expr_list().check_expr();
    if (negated) {
      sb.append("!(");
    }
    emitDoubleValueLike(ctx.check_expr_like(), sb);
    sb.append(" in [");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      emitDoubleValueCheckExpr(items.get(i), sb);
    }
    sb.append("]");
    if (negated) {
      sb.append(")");
    }
  }

  private static void visitInTarget(
      LogicalTypesParser.In_targetContext ctx, StringBuilder sb) {
    if (ctx instanceof LogicalTypesParser.InTargetParenListContext) {
      LogicalTypesParser.Check_expr_listContext list =
          ((LogicalTypesParser.InTargetParenListContext) ctx).check_expr_list();
      sb.append("[");
      List<LogicalTypesParser.Check_exprContext> items = list.check_expr();
      for (int i = 0; i < items.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        visitCheckExpr(items.get(i), sb);
      }
      sb.append("]");
    } else if (ctx instanceof LogicalTypesParser.InTargetExprContext) {
      StringBuilder target = new StringBuilder();
      visitLike(((LogicalTypesParser.InTargetExprContext) ctx).check_expr_like(), target);
      sb.append(typedSubscript(target));
    } else {
      throw new ValidationException(
          "Unrecognized in_target subtype: " + ctx.getClass().getSimpleName());
    }
  }

  /**
   * SQL {@code NOT} has lower precedence than IS/comparison/BETWEEN/IN/LIKE,
   * so {@code NOT x > 0} means {@code NOT (x > 0)}. CEL {@code !} binds tighter
   * than {@code >}, so we must parenthesize the operand: {@code !(x > 0)}.
   * Emitting bare {@code !} would miscompile to {@code (!x) > 0} — a runtime
   * type error.
   *
   * <p>The grammar's NOT alternative is recursive (matches real PG), so
   * {@code NOT NOT x} chains cleanly here.
   */
  private static void visitUnaryNot(
      LogicalTypesParser.Check_expr_unary_notContext ctx, StringBuilder sb) {
    if (ctx instanceof LogicalTypesParser.CheckExprNotContext) {
      // CEL `!` binds tighter than comparison/logical operators. For
      // simple primaries (column refs, literals, function calls,
      // already-paren'd) the operand emits without parens; compound
      // operands need the wrap so `!a > b` doesn't parse as `(!a) > b`.
      LogicalTypesParser.Check_expr_unary_notContext inner =
          ((LogicalTypesParser.CheckExprNotContext) ctx).check_expr_unary_not();
      boolean wrap = !ConstraintResolver.isSimplePrimaryUnaryNot(inner);
      sb.append('!');
      if (wrap) {
        sb.append('(');
      }
      visitUnaryNot(inner, sb);
      if (wrap) {
        sb.append(')');
      }
    } else if (ctx instanceof LogicalTypesParser.CheckExprNotPassContext) {
      visitIs(
          ((LogicalTypesParser.CheckExprNotPassContext) ctx).check_expr_isnull(), sb);
    } else {
      throw new ValidationException(
          "Unrecognized check_expr_unary_not subtype: " + ctx.getClass().getSimpleName());
    }
  }

  /**
   * IS NULL translation:
   * <ul>
   *   <li>has-compatible LHS: {@code x IS NULL} →
   *       {@code (!has(x) || dyn(x) == null)};
   *       {@code x IS NOT NULL} → {@code (has(x) && dyn(x) != null)}.</li>
   *   <li>Other LHS: {@code <expr> IS NULL} →
   *       {@code dyn(<expr>) == null};
   *       {@code <expr> IS NOT NULL} → {@code dyn(<expr>) != null}.</li>
   * </ul>
   *
   * <p>The {@code has()} guard is the protovalidate convention for absent
   * proto field detection — required because CEL surfaces absent proto
   * scalar fields as their typed defaults (0, "", false) rather than null.
   *
   * <p>The {@code dyn(...)} wrapper around the {@code ==}/{@code !=}
   * operand makes the null comparison portable across CEL checkers.
   * Without it, consumers compiling against concrete proto types would
   * see checker errors like {@code no matching overload for '_!=_'
   * applied to '(int, null)'}. Wrapping with {@code dyn} forces the
   * type-checker to accept any operand against null. {@code dyn} is a
   * no-op at runtime (proto3 scalars stay non-null; wrapper types and
   * message fields keep their actual null-ness). {@code has()}'s argument
   * stays a bare field selection per CEL spec — only the value-side null
   * comparison gets the dyn wrap. The LHS is emitted twice in the
   * has-compatible branch (CEL expressions are pure).
   *
   * <p><b>Outer parens on the has-compatible branch are mandatory.</b>
   * CEL's {@code &&} binds tighter than {@code ||}, so an unwrapped IS
   * NULL rewrite ({@code !has(x) || dyn(x) == null}) ANDed with another
   * operand mis-binds: a surrounding {@code AND} would absorb the right
   * half. The IS NOT NULL rewrite is paren-wrapped too for symmetry. The
   * non-has-compatible fallback ({@code dyn(<expr>) == null}) doesn't
   * need outer parens — {@code dyn(...)} is a single tightly-bound call.
   *
   * <p><b>Note on collection (ARRAY/MULTISET/MAP) IS NULL.</b> CEL spec:
   * {@code has(repeated_field)} returns false for non-null but EMPTY
   * collections (proto3 has no presence bit for repeated fields, so empty
   * and absent are wire-equivalent). So {@code arr IS NULL} on a
   * collection column emits {@code (!has(this.arr) || dyn(this.arr) ==
   * null)} which fires for empty collections too. This matches the
   * protovalidate convention — {@code !has(field)} is the standard
   * "field is unset" idiom for proto3 repeated. Strict SQL semantics
   * (NULL only, not empty) has no meaningful runtime equivalent for
   * proto3 repeated fields (which can never actually be NULL).
   */
  private static void visitIs(
      LogicalTypesParser.Check_expr_isnullContext ctx, StringBuilder sb) {
    if (ctx.IS() == null) {
      visitCompare(ctx.check_expr_compare(), sb);
      return;
    }
    boolean negated = ctx.NOT() != null;
    if (ConstraintResolver.isHasCompatible(ctx.check_expr_compare(), EMIT_VCTX.get())) {
      StringBuilder emitted = new StringBuilder();
      visitCompare(ctx.check_expr_compare(), emitted);
      sb.append(nullTest(ConstraintResolver.selectionPathCompare(ctx.check_expr_compare()),
          emitted.toString(), !negated));
      return;
    }
    // An expression is tested by its inputs, as evaluating it on a NULL one fails, and is still
    // evaluated otherwise, so an error such as bad JSON fails as SQL's does. The strict check
    // types it in its own numeric type, as DECIMAL arithmetic has no native form.
    LogicalTypesParser.Check_expr_compareContext operand = ctx.check_expr_compare();
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    String byInputs = ConstraintNullTest.isTestedByValue(operand) ? null
        : ConstraintNullTest.told(ConstraintNullTest.of(operand), operand);
    String category = operand.check_expr_between().size() == 1 && vctx != null
        ? ConstraintResolver.coercedNumericCategory(operand.check_expr_between(0), vctx) : null;
    StringBuilder emitted = new StringBuilder();
    if ("decimal".equals(category) || "double".equals(category)) {
      emitNumericBetweenAs(operand.check_expr_between(0), category, emitted);
    } else {
      visitCompare(operand, emitted);
    }
    String value = "dyn(" + emitted + ") == null";
    if (byInputs == null || vctx != null && vctx.isTypeOnly()) {
      sb.append(negated ? ConstraintNullTest.not(value) : value);
    } else if (byInputs.equals(value)) {
      sb.append(negated ? ConstraintNullTest.not(byInputs) : byInputs);
    } else if (byInputs.contains(value)) {
      // The test evaluates it already, as a VARIANT_GET's or a subscript's does. Ternaries, as
      // cel-java's || and && hide an error once their result is compared.
      sb.append('(').append(byInputs).append(negated ? " ? false : true)" : " ? true : false)");
    } else {
      // A predicate's CEL may be || or && itself, so its value is a ternary's condition too:
      // its inputs tell it isn't NULL there.
      Schema type = vctx == null ? null
          : ConstraintResolver.tryResolveCompareValueType(operand, vctx);
      String ofValue = type != null && type.getType() == Schema.Type.BOOLEAN
          ? "(" + emitted + (negated ? " ? true : true)" : " ? false : false)")
          : negated ? ConstraintNullTest.not(value) : value;
      sb.append('(').append(byInputs).append(negated ? " ? false : " : " ? true : ")
          .append(ofValue).append(')');
    }
  }

  private static void visitCompare(
      LogicalTypesParser.Check_expr_compareContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_betweenContext> betweens = ctx.check_expr_between();
    if (betweens.size() == 2) {
      // The comparison operator is the second child of the rule (between the
      // two check_expr_between). Find it as the only TerminalNode child.
      String celOp = translateCompareOp(findComparisonOp(ctx));
      ConstraintValidationContext vctx = EMIT_VCTX.get();
      // Numeric coercion: if both sides are pure numeric, compute the
      // common type and emit at it. common=DECIMAL → decimals.* (no native
      // overload on the opaque type); common=DOUBLE/INT → native operator with
      // operands cast to the common type (double()/double literal as needed).
      //
      // coercedNumericCategory classifies a boolean-shaped operand (BETWEEN/
      // IN/LIKE, even parenthesized) as non-numeric, so this path only sees
      // bare numeric values — the assumption emitNumericBetweenAs relies on.
      // Boolean predicate operands fall through to the wrapped native path.
      if (vctx != null) {
        String lc = ConstraintResolver.coercedNumericCategory(betweens.get(0), vctx);
        String rc = ConstraintResolver.coercedNumericCategory(betweens.get(1), vctx);
        if (lc != null && rc != null) {
          String common = ConstraintResolver.numericCommon(lc, rc);
          if ("decimal".equals(common)) {
            emitDecimalCompare(betweens.get(0), celOp, betweens.get(1), sb);
          } else {
            emitNumericBetweenAs(betweens.get(0), common, sb);
            sb.append(' ').append(celOp).append(' ');
            emitNumericBetweenAs(betweens.get(1), common, sb);
          }
          return;
        }
      }
      // Non-numeric: timestamp normalization (instant-timestamp operands wrapped
      // in timestamp(...); CURRENT_TIMESTAMP stays `now`) or plain native.
      emitCompareOperandWrapped(betweens.get(0), vctx, sb);
      sb.append(' ').append(celOp).append(' ');
      emitCompareOperandWrapped(betweens.get(1), vctx, sb);
      return;
    }
    visitBetween(betweens.get(0), sb);
  }

  // -------------------------------------------------------------------------
  // Timestamp normalization — wrap instant-timestamp operands in
  // timestamp(...) so Avro (Instant) / JSON (RFC-3339 string) values coerce
  // to a CEL timestamp; proto Timestamp is identity. The comparison/BETWEEN
  // operators stay native (CEL has first-class timestamp comparison).
  // CURRENT_TIMESTAMP (→ now) is already a CEL timestamp and is left unwrapped.
  // -------------------------------------------------------------------------

  /**
   * A comparison operand. If it involves a timestamp column or a TIMESTAMP/
   * INTERVAL literal, emit it via the timestamp-value cascade (column leaves
   * wrapped in {@code timestamp(...)}, literals as {@code timestamp(...)}/
   * {@code duration(...)}, operators native); otherwise emit natively.
   */
  /**
   * Emit a comparison operand, parenthesizing it when it is itself a compound
   * predicate. Since BETWEEN/IN now bind tighter than comparison, an operand
   * like {@code x BETWEEN 0 AND 10} (emits {@code lo <= a && a <= hi}) or
   * {@code x IN (...)} (emits {@code a in [...]}) would otherwise mis-bind
   * under CEL, where {@code ==} binds tighter than {@code &&} and {@code in}
   * shares precedence with {@code ==}.
   */
  private static void emitCompareOperandWrapped(
      LogicalTypesParser.Check_expr_betweenContext between,
      ConstraintValidationContext vctx, StringBuilder sb) {
    boolean wrap = compareOperandNeedsParens(between);
    if (wrap) {
      sb.append('(');
    }
    emitCompareOperand(between, vctx, sb);
    if (wrap) {
      sb.append(')');
    }
  }

  /**
   * True if a comparison operand emits a BETWEEN ({@code &&}) or IN ({@code in}).
   */
  private static boolean compareOperandNeedsParens(
      LogicalTypesParser.Check_expr_betweenContext between) {
    if (between.BETWEEN() != null) {
      return true;
    }
    return between.check_expr_in().size() == 1
        && between.check_expr_in(0).IN() != null;
  }

  private static void emitCompareOperand(
      LogicalTypesParser.Check_expr_betweenContext between,
      ConstraintValidationContext vctx, StringBuilder sb) {
    // Guard LIKE: the cascade descends straight to the add value root, so a LIKE
    // clause would be dropped. A LIKE operand is boolean (and invalid on a
    // timestamp), so emit it natively and let the strict checker reject the type
    // error. Only a bare value (no BETWEEN/IN/LIKE) goes through the timestamp
    // cascade; anything else falls back to the native between emit.
    LogicalTypesParser.Check_expr_addContext add =
        between.BETWEEN() == null && between.check_expr_in().size() == 1
            ? bareAddOfIn(between.check_expr_in(0)) : null;
    if (vctx != null && add != null
        && ConstraintResolver.subtreeHasTemporal(between, vctx)) {
      emitTimestampValueAdd(add, sb);
    } else {
      visitBetween(between, sb);
    }
  }

  /**
   * Emit an EXTRACT receiver. Temporal receivers go through the timestamp-value
   * cascade (so {@code ts} → {@code timestamp(this.ts)}); {@code now} and
   * non-temporal receivers emit natively. Compound receivers are parenthesized
   * so the trailing {@code .getXxx()} binds to the whole receiver.
   */
  static void emitTimestampReceiver(
      LogicalTypesParser.Check_exprContext receiver, StringBuilder sb) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (vctx != null && ConstraintResolver.subtreeHasTemporal(receiver, vctx)) {
      if (ConstraintResolver.isSimplePrimary(receiver)) {
        emitTimestampValueCheckExpr(receiver, sb);
      } else {
        sb.append('(');
        emitTimestampValueCheckExpr(receiver, sb);
        sb.append(')');
      }
    } else {
      emitWrappedReceiver(receiver, sb);
    }
  }

  /**
   * Timestamp BETWEEN: native {@code <=} chain with each operand emitted via
   * the timestamp-value cascade.
   */
  private static void emitBetweenTimestamp(
      List<LogicalTypesParser.Check_expr_inContext> ins,
      boolean negated, boolean symmetric, StringBuilder sb) {
    if (negated) {
      sb.append("!(");
    }
    if (symmetric) {
      sb.append('(');
      emitTimestampLe(ins.get(1), ins.get(2), sb);   // lo <= hi ?
      sb.append(" ? (");
      emitTimestampLe(ins.get(1), ins.get(0), sb);
      sb.append(" && ");
      emitTimestampLe(ins.get(0), ins.get(2), sb);
      sb.append(") : (");
      emitTimestampLe(ins.get(2), ins.get(0), sb);
      sb.append(" && ");
      emitTimestampLe(ins.get(0), ins.get(1), sb);
      sb.append("))");
    } else {
      emitTimestampLe(ins.get(1), ins.get(0), sb);
      sb.append(" && ");
      emitTimestampLe(ins.get(0), ins.get(2), sb);
    }
    if (negated) {
      sb.append(")");
    }
  }

  private static void emitTimestampLe(
      LogicalTypesParser.Check_expr_inContext a,
      LogicalTypesParser.Check_expr_inContext b, StringBuilder sb) {
    emitTimestampValueIn(a, sb);
    sb.append(" <= ");
    emitTimestampValueIn(b, sb);
  }

  // ---- timestamp-value cascade: native emit, but instant-timestamp column
  // leaves wrapped in timestamp(...). Reused by comparison, BETWEEN, EXTRACT,
  // and the Phase B constructs. ----

  static void emitTimestampValueIn(
      LogicalTypesParser.Check_expr_inContext in, StringBuilder sb) {
    LogicalTypesParser.Check_expr_addContext add = bareAddOfIn(in);
    if (add != null) {
      emitTimestampValueAdd(add, sb);
    } else {
      visitIn(in, sb);  // not a bare value (defensive); emit native
    }
  }

  static void emitTimestampValueCheckExpr(
      LogicalTypesParser.Check_exprContext ctx, StringBuilder sb) {
    emitCached(ctx, Form.TIMESTAMP, sb, out -> {
      LogicalTypesParser.Check_expr_addContext add = bareAddOfOr(ctx.check_expr_or());
      if (add != null) {
        emitTimestampValueAdd(add, out);
      } else {
        visitCheckExpr(ctx, out);  // not a bare value (defensive)
      }
    });
  }

  private static void emitTimestampValueAdd(
      LogicalTypesParser.Check_expr_addContext add, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_mulContext> muls = add.check_expr_mul();
    emitTimestampValueMul(muls.get(0), sb);
    int mulIdx = 1;
    for (int i = 0; i < add.getChildCount(); i++) {
      ParseTree child = add.getChild(i);
      if (child instanceof TerminalNode) {
        sb.append(' ').append(child.getText()).append(' ');
        emitTimestampValueMul(muls.get(mulIdx++), sb);
      }
    }
  }

  private static void emitTimestampValueMul(
      LogicalTypesParser.Check_expr_mulContext mul, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = mul.check_expr_unary_sign();
    emitTimestampValueSign(signs.get(0), sb);
    int signIdx = 1;
    for (int i = 0; i < mul.getChildCount(); i++) {
      ParseTree child = mul.getChild(i);
      if (child instanceof TerminalNode) {
        String op = child.getText();
        // `||` (string concat) → CEL `+`; not expected for temporal values.
        sb.append(' ').append("||".equals(op) ? "+" : op).append(' ');
        emitTimestampValueSign(signs.get(signIdx++), sb);
      }
    }
  }

  private static void emitTimestampValueSign(
      LogicalTypesParser.Check_expr_unary_signContext sign, StringBuilder sb) {
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode && "-".equals(child.getText())) {
        sb.append('-');  // '+' dropped, matching native visitUnarySign
      }
    }
    emitTimestampValueCExpr(sign.c_expr(), sb);
  }

  private static void emitTimestampValueCExpr(
      LogicalTypesParser.C_exprContext ctx, StringBuilder sb) {
    if (ctx instanceof LogicalTypesParser.CheckColumnRefContext) {
      LogicalTypesParser.ColumnrefContext cr =
          ((LogicalTypesParser.CheckColumnRefContext) ctx).columnref();
      if (ConstraintResolver.isInstantTimestamp(
          ConstraintResolver.resolveColumnRefType(cr, EMIT_VCTX.get()))) {
        sb.append("timestamp(");
        visitColumnRef(cr, sb);
        sb.append(')');
      } else {
        visitColumnRef(cr, sb);
      }
      return;
    }
    if (ctx instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) ctx;
      if (paren.indirection() == null) {
        sb.append('(');
        emitTimestampValueCheckExpr(paren.check_expr(), sb);
        sb.append(')');
        return;
      }
    }
    // Literal (TIMESTAMP/INTERVAL → timestamp(...)/duration(...)), function
    // (self-normalizing, incl. Phase B constructs), case, or paren+indirection:
    // emit natively.
    visitCExpr(ctx, sb);
  }

  // -------------------------------------------------------------------------
  // Decimal emit cascade — parallels the native add/mul/sign/c_expr cascade
  // but emits the opaque-Decimal function forms (decimals.add/sub/mul/div/neg)
  // and coerces leaves to Decimal. Entered only when a comparison (or, in
  // Phase C, BETWEEN/IN/CASE/COALESCE/NULLIF/GREATEST/LEAST) has a decimal
  // operand; the native cascade is left untouched for non-decimal numerics.
  //
  // decimal(dyn) is idempotent on Decimal values, so coercion is only emitted
  // where the operand is *not* already Decimal-typed (keeps output clean:
  // `decimals.lt(this.x, decimal("5"))`, not `decimals.lt(decimal(this.x), …)`).
  // -------------------------------------------------------------------------

  /**
   * Emit a {@code check_expr} known to resolve to decimal as a Decimal CEL
   * value (column ref as-is, literal as {@code decimal("…")}, arithmetic as
   * folded {@code decimals.*} calls). Used by {@link ConstraintFunctions} to
   * emit the argument of {@code decimals.abs/floor/ceil/round/trunc/sqrt/...}.
   */
  static void emitDecimalValue(
      LogicalTypesParser.Check_exprContext ctx, StringBuilder sb) {
    emitCheckExprAsDecimal(ctx, sb);
  }

  /**
   * {@code a <op> b} where a/b are decimal → {@code decimals.<fn>(a, b)}.
   */
  private static void emitDecimalCompare(
      LogicalTypesParser.Check_expr_betweenContext lhs, String celOp,
      LogicalTypesParser.Check_expr_betweenContext rhs, StringBuilder sb) {
    String fn;
    boolean negate = false;
    switch (celOp) {
      case "==":
        fn = "decimals.eq";
        break;
      case "!=":
        fn = "decimals.eq";
        negate = true;
        break;
      case "<":
        fn = "decimals.lt";
        break;
      case "<=":
        fn = "decimals.le";
        break;
      case ">":
        fn = "decimals.gt";
        break;
      case ">=":
        fn = "decimals.ge";
        break;
      default:
        throw new ValidationException("Internal: unexpected decimal compare op " + celOp);
    }
    if (negate) {
      sb.append('!');
    }
    sb.append(fn).append('(');
    emitBetweenAsDecimal(lhs, sb);
    sb.append(", ");
    emitBetweenAsDecimal(rhs, sb);
    sb.append(')');
  }

  /**
   * Emit a {@code check_expr_between} comparison operand as a Decimal value. A
   * decimal operand is a bare value (no BETWEEN/IN/LIKE), so it descends
   * between → in → like → add.
   */
  private static void emitBetweenAsDecimal(
      LogicalTypesParser.Check_expr_betweenContext between, StringBuilder sb) {
    emitLikeAsDecimalValue(between.check_expr_in(0).check_expr_like(), sb);
  }

  /**
   * Emit a {@code check_expr_like} (an IN left operand or comparison operand) as
   * a Decimal value. A decimal operand never carries LIKE (that yields bool);
   * detection guarantees the add value root in normal use, with a defensive
   * {@code decimal((...))} coercion fallback if a LIKE somehow reaches here.
   */
  private static void emitLikeAsDecimalValue(
      LogicalTypesParser.Check_expr_likeContext like, StringBuilder sb) {
    if (like.LIKE() == null) {
      emitAddAsDecimal(like.check_expr_add(), sb);
    } else {
      sb.append("decimal((");
      visitLike(like, sb);
      sb.append("))");
    }
  }

  /** Fold an additive chain left-associatively into decimals.add/sub calls. */
  private static void emitAddAsDecimal(
      LogicalTypesParser.Check_expr_addContext add, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_mulContext> muls = add.check_expr_mul();
    emitAddRangeAsDecimal(muls, operators(add), muls.size(), sb);
  }

  /**
   * Fold the first {@code count} operands of an additive chain into decimals.add/sub calls.
   * Leading integer operands are added natively and widened once, as SQL types each operator
   * by its own operands.
   */
  private static void emitAddRangeAsDecimal(
      List<LogicalTypesParser.Check_expr_mulContext> muls, List<String> ops, int count,
      StringBuilder sb) {
    StringBuilder acc = new StringBuilder();
    int run = Math.min(narrowerRun(muls, "decimal"), count);
    int next = 1;
    if (run >= 2) {
      acc.append("decimal(");
      emitNativeRun(muls, ops, run, acc);
      acc.append(')');
      next = run;
    } else {
      emitMulAsDecimal(muls.get(0), acc);
    }
    for (int i = next; i < count; i++) {
      String fn = "-".equals(ops.get(i - 1)) ? "decimals.sub" : "decimals.add";
      StringBuilder rhs = new StringBuilder();
      emitMulAsDecimal(muls.get(i), rhs);
      String folded = fn + "(" + acc + ", " + rhs + ")";
      acc.setLength(0);
      acc.append(folded);
    }
    sb.append(acc);
  }

  /** Fold a multiplicative chain left-associatively into decimals.mul/div/mod calls. */
  private static void emitMulAsDecimal(
      LogicalTypesParser.Check_expr_mulContext mul, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = mul.check_expr_unary_sign();
    emitMulRangeAsDecimal(mul, signs, operators(mul), signs.size(), sb);
  }

  /**
   * Fold the first {@code count} operands of a multiplicative chain into decimals.mul/div/mod
   * calls. Leading integer operands are combined natively (so `/` truncates) and widened once.
   */
  private static void emitMulRangeAsDecimal(
      LogicalTypesParser.Check_expr_mulContext mul,
      List<LogicalTypesParser.Check_expr_unary_signContext> signs, List<String> ops, int count,
      StringBuilder sb) {
    StringBuilder acc = new StringBuilder();
    int run = Math.min(narrowerRun(signs, "decimal"), count);
    int next = 1;
    if (run >= 2) {
      acc.append("decimal(");
      emitNativeRun(signs, ops, run, acc);
      acc.append(')');
      next = run;
    } else {
      emitSignAsDecimal(signs.get(0), acc);
    }
    for (int i = next; i < count; i++) {
      String op = ops.get(i - 1);
      String fn;
      if ("*".equals(op)) {
        fn = "decimals.mul";
      } else if ("/".equals(op)) {
        fn = "decimals.div";
      } else if ("%".equals(op)) {
        // Modulo → decimals.mod (remainder with the sign of the dividend).
        fn = "decimals.mod";
      } else {
        // `||` is string/bytes concat — never valid on a DECIMAL value.
        throw locatedError(mul,
            "Operator '||' (string concat) is not supported on DECIMAL operands.");
      }
      StringBuilder rhs = new StringBuilder();
      emitSignAsDecimal(signs.get(i), rhs);
      String folded = fn + "(" + acc + ", " + rhs + ")";
      acc.setLength(0);
      acc.append(folded);
    }
    sb.append(acc);
  }

  private static void emitSignAsDecimal(
      LogicalTypesParser.Check_expr_unary_signContext sign, StringBuilder sb) {
    if (sign.getChildCount() > 1 && narrowerThan(numericCategory(sign), "decimal")) {
      // A negated integer is negated natively, then widened.
      sb.append("decimal(");
      visitUnarySign(sign, sb);
      sb.append(')');
      return;
    }
    boolean negate = false;
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode && "-".equals(child.getText())) {
        negate = !negate;  // parity: `--x` cancels, matching the native path; '+' dropped
      }
    }
    if (negate) {
      sb.append("decimals.neg(");
      emitCExprAsDecimal(sign.c_expr(), sb);
      sb.append(')');
    } else {
      emitCExprAsDecimal(sign.c_expr(), sb);
    }
  }

  /** Emit a leaf as a Decimal value, coercing non-Decimal operands. */
  private static void emitCExprAsDecimal(
      LogicalTypesParser.C_exprContext ctx, StringBuilder sb) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (ctx instanceof LogicalTypesParser.CheckLiteralContext) {
      LogicalTypesParser.LiteralContext lit =
          ((LogicalTypesParser.CheckLiteralContext) ctx).literal();
      // Numeric literal → decimal("<exact original text>"): preserve the
      // written value exactly rather than routing through a double literal,
      // which would bake in binary-float imprecision.
      if (lit.intLiteral() != null) {
        sb.append("decimal(\"").append(lit.intLiteral().getText()).append("\")");
        return;
      }
      if (lit.decimalLiteral() != null) {
        sb.append("decimal(\"").append(lit.decimalLiteral().getText()).append("\")");
        return;
      }
      if (lit.doubleLiteral() != null) {
        sb.append("decimal(\"").append(lit.doubleLiteral().getText()).append("\")");
        return;
      }
      // Non-numeric literal in a decimal context — let decimal(dyn) reject it
      // at runtime / the checker reject it; emit the coercion form.
      sb.append("decimal(");
      visitLiteral(lit, sb);
      sb.append(')');
      return;
    }
    if (ctx instanceof LogicalTypesParser.CheckColumnRefContext) {
      LogicalTypesParser.ColumnrefContext cr =
          ((LogicalTypesParser.CheckColumnRefContext) ctx).columnref();
      if (ConstraintResolver.isDecimal(ConstraintResolver.resolveColumnRefType(cr, vctx))) {
        visitColumnRef(cr, sb);
      } else {
        sb.append("decimal(");
        visitColumnRef(cr, sb);
        sb.append(')');
      }
      return;
    }
    if (ctx instanceof LogicalTypesParser.CheckFuncContext) {
      LogicalTypesParser.Func_exprContext fe =
          ((LogicalTypesParser.CheckFuncContext) ctx).func_expr();
      if (ConstraintResolver.isDecimal(
          ConstraintResolver.tryResolveFuncReturnType(fe, vctx))) {
        ConstraintFunctions.visitFuncExpr(fe, sb);
      } else {
        sb.append("decimal(");
        ConstraintFunctions.visitFuncExpr(fe, sb);
        sb.append(')');
      }
      return;
    }
    if (ctx instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) ctx;
      if (paren.indirection() == null
          && ConstraintResolver.isDecimal(
              ConstraintResolver.tryResolveCheckExprType(paren.check_expr(), vctx))) {
        // Decimal sub-expression: re-enter the decimal cascade (no SQL parens
        // needed — we're already inside a function-call argument position).
        emitCheckExprAsDecimal(paren.check_expr(), sb);
      } else {
        // Non-decimal numeric (or unresolved): coerce the native emit.
        sb.append("decimal(");
        visitCExpr(ctx, sb);
        sb.append(')');
      }
      return;
    }
    if (ctx instanceof LogicalTypesParser.CheckCaseContext) {
      LogicalTypesParser.Case_exprContext ce =
          ((LogicalTypesParser.CheckCaseContext) ctx).case_expr();
      if (ConstraintResolver.isDecimal(
          ConstraintResolver.tryResolveCaseExprType(ce, vctx))) {
        visitCase(ce, sb);
      } else {
        sb.append("decimal(");
        visitCase(ce, sb);
        sb.append(')');
      }
      return;
    }
    // Fallback: coerce whatever the native path emits.
    sb.append("decimal(");
    visitCExpr(ctx, sb);
    sb.append(')');
  }

  /**
   * Re-enter the decimal cascade from a {@code check_expr} known to resolve to
   * decimal (used for paren-wrapped decimal sub-expressions). The boolean
   * cascade levels are all single-child pass-throughs for a decimal value;
   * if any branches unexpectedly, fall back to coercing the native emit.
   */
  private static void emitCheckExprAsDecimal(
      LogicalTypesParser.Check_exprContext ctx, StringBuilder sb) {
    emitCached(ctx, Form.DECIMAL, sb, out -> {
      LogicalTypesParser.Check_expr_addContext add = bareAddOfOr(ctx.check_expr_or());
      if (add != null) {
        emitAddAsDecimal(add, out);
      } else {
        out.append("decimal((");
        visitCheckExpr(ctx, out);
        out.append("))");
      }
    });
  }

  /**
   * Emit a {@code check_expr_in} (a BETWEEN operand) as a Decimal value.
   */
  private static void emitInAsDecimal(
      LogicalTypesParser.Check_expr_inContext in, StringBuilder sb) {
    LogicalTypesParser.Check_expr_addContext add = bareAddOfIn(in);
    if (add != null) {
      emitAddAsDecimal(add, sb);
    } else {
      sb.append("decimal((");
      visitIn(in, sb);
      sb.append("))");
    }
  }

  private static boolean likeHasDecimal(
      LogicalTypesParser.Check_expr_likeContext like, ConstraintValidationContext vctx) {
    if (like.LIKE() != null) {
      return false;
    }
    return ConstraintResolver.addHasDecimal(like.check_expr_add(), vctx);
  }

  // Walk the single-child cascade down to the check_expr_add value root; return
  // null at any level that branches (operator/keyword present), meaning the node
  // is a boolean predicate rather than a bare arithmetic/decimal value. Follows
  // the precedence chain or → and → not → isnull → compare → between → in → like.

  static LogicalTypesParser.Check_expr_addContext bareAddOfOr(
      LogicalTypesParser.Check_expr_orContext or) {
    if (or.check_expr_and().size() != 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() != 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_unary_notContext notNode = and.check_expr_unary_not(0);
    if (!(notNode instanceof LogicalTypesParser.CheckExprNotPassContext)) {
      return null;
    }
    LogicalTypesParser.Check_expr_isnullContext isnull =
        ((LogicalTypesParser.CheckExprNotPassContext) notNode).check_expr_isnull();
    return bareAddOfIsnull(isnull);
  }

  private static LogicalTypesParser.Check_expr_addContext bareAddOfIsnull(
      LogicalTypesParser.Check_expr_isnullContext is) {
    if (is.IS() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_compareContext compare = is.check_expr_compare();
    if (compare.check_expr_between().size() != 1) {
      return null;
    }
    LogicalTypesParser.Check_expr_betweenContext between = compare.check_expr_between(0);
    if (between.BETWEEN() != null || between.check_expr_in().size() != 1) {
      return null;
    }
    return bareAddOfIn(between.check_expr_in(0));
  }

  private static LogicalTypesParser.Check_expr_addContext bareAddOfIn(
      LogicalTypesParser.Check_expr_inContext in) {
    if (in.IN() != null) {
      return null;
    }
    LogicalTypesParser.Check_expr_likeContext like = in.check_expr_like();
    if (like.LIKE() != null) {
      return null;
    }
    return like.check_expr_add();
  }

  // -------------------------------------------------------------------------
  // Numeric coercion emit: emit a numeric expression cast to a common
  // type. DECIMAL → the decimal cascade (decimals.* + decimal()); DOUBLE → the
  // double cascade below (native operators + double()/double-literal casts);
  // INT → native (no casts, both operands already int).
  // -------------------------------------------------------------------------

  /**
   * Emit a comparison/operand value cast to {@code common} ("int"/"double"/"decimal").
   */
  static void emitNumericAs(
      LogicalTypesParser.Check_exprContext ctx, String common, StringBuilder sb) {
    if ("decimal".equals(common)) {
      emitDecimalValue(ctx, sb);
    } else if ("double".equals(common)) {
      emitDoubleValueCheckExpr(ctx, sb);
    } else {
      visitCheckExpr(ctx, sb);
    }
  }

  private static void emitNumericBetweenAs(
      LogicalTypesParser.Check_expr_betweenContext between, String common, StringBuilder sb) {
    // A pure-numeric comparison operand is a bare value (no BETWEEN/IN/LIKE), so
    // it descends between → in → like → add.
    LogicalTypesParser.Check_expr_likeContext like = between.check_expr_in(0).check_expr_like();
    if ("decimal".equals(common)) {
      emitAddAsDecimal(like.check_expr_add(), sb);
    } else if ("double".equals(common)) {
      emitDoubleValueAdd(like.check_expr_add(), sb);
    } else {
      visitBetween(between, sb);
    }
  }

  private static void emitNumericInAs(
      LogicalTypesParser.Check_expr_inContext in, String common, StringBuilder sb) {
    LogicalTypesParser.Check_expr_addContext add = bareAddOfIn(in);
    if (add == null) {
      // not a bare value (defensive) — native
      emitParenIn(in, sb);
      return;
    }
    if ("decimal".equals(common)) {
      emitAddAsDecimal(add, sb);
    } else if ("double".equals(common)) {
      emitDoubleValueAdd(add, sb);
    } else {
      emitParenIn(in, sb);
    }
  }

  /**
   * Common numeric type ("int"/"double"/"decimal") over a set of operand
   * subtrees, or {@code null} if any operand is non-numeric (so the construct
   * isn't a pure numeric one and should defer to its temporal/native path).
   */
  static String numericCommonOf(
      List<? extends ParseTree> nodes, ConstraintValidationContext vctx) {
    String acc = null;
    for (ParseTree n : nodes) {
      String c = ConstraintResolver.coercedNumericCategory(n, vctx);
      if (c == null) {
        return null;
      }
      acc = acc == null ? c : ConstraintResolver.numericCommon(acc, c);
    }
    return acc;
  }

  /**
   * Two-operand {@link #numericCommonOf}: common type of {@code a} and {@code b}.
   */
  static String numericCommonOf(ParseTree a, ParseTree b, ConstraintValidationContext vctx) {
    String ca = ConstraintResolver.coercedNumericCategory(a, vctx);
    String cb = ConstraintResolver.coercedNumericCategory(b, vctx);
    if (ca == null || cb == null) {
      return null;
    }
    return ConstraintResolver.numericCommon(ca, cb);
  }

  /**
   * Common numeric type over an IN's left operand and its value-list elements
   * (paren-list form only), or {@code null} if any is non-numeric.
   */
  private static String inListCommon(
      LogicalTypesParser.Check_expr_inContext ctx, ConstraintValidationContext vctx) {
    String acc = ConstraintResolver.coercedNumericCategory(ctx.check_expr_like(), vctx);
    if (acc == null) {
      return null;
    }
    List<LogicalTypesParser.Check_exprContext> items =
        ((LogicalTypesParser.InTargetParenListContext) ctx.in_target())
            .check_expr_list().check_expr();
    for (LogicalTypesParser.Check_exprContext item : items) {
      String c = ConstraintResolver.coercedNumericCategory(item, vctx);
      if (c == null) {
        return null;
      }
      acc = ConstraintResolver.numericCommon(acc, c);
    }
    return acc;
  }

  // ---- double cascade: native operators, int/decimal leaves cast to double ----

  /**
   * Emit a {@code check_expr_like} (an IN left operand) as a double value.
   */
  private static void emitDoubleValueLike(
      LogicalTypesParser.Check_expr_likeContext like, StringBuilder sb) {
    if (like.LIKE() == null) {
      emitDoubleValueAdd(like.check_expr_add(), sb);
    } else {
      sb.append("double((");
      visitLike(like, sb);
      sb.append("))");
    }
  }

  private static void emitDoubleValueCheckExpr(
      LogicalTypesParser.Check_exprContext ctx, StringBuilder sb) {
    emitCached(ctx, Form.DOUBLE, sb, out -> {
      LogicalTypesParser.Check_expr_addContext add = bareAddOfOr(ctx.check_expr_or());
      if (add != null) {
        emitDoubleValueAdd(add, out);
      } else {
        out.append("double((");
        visitCheckExpr(ctx, out);
        out.append("))");
      }
    });
  }

  // Leading integer or decimal operands are combined in their own type and converted once:
  // SQL types each operator by its own operands, so `a / 4` truncates and `0.10 + 0.20` is exact.
  private static void emitDoubleValueAdd(
      LogicalTypesParser.Check_expr_addContext add, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_mulContext> muls = add.check_expr_mul();
    List<String> ops = operators(add);
    int run = narrowerRun(muls, "double");
    int next = 1;
    if (run >= 2) {
      sb.append("double(");
      if ("int".equals(runCategory(muls, run))) {
        emitNativeRun(muls, ops, run, sb);
      } else {
        emitAddRangeAsDecimal(muls, ops, run, sb);
      }
      sb.append(')');
      next = run;
    } else {
      emitDoubleValueMul(muls.get(0), sb);
    }
    for (int i = next; i < muls.size(); i++) {
      sb.append(' ').append(ops.get(i - 1)).append(' ');
      emitDoubleValueMul(muls.get(i), sb);
    }
  }

  private static void emitDoubleValueMul(
      LogicalTypesParser.Check_expr_mulContext mul, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = mul.check_expr_unary_sign();
    List<String> ops = operators(mul);
    int run = narrowerRun(signs, "double");
    int next = 1;
    if (run >= 2) {
      sb.append("double(");
      if ("int".equals(runCategory(signs, run))) {
        emitNativeRun(signs, ops, run, sb);
      } else {
        emitMulRangeAsDecimal(mul, signs, ops, run, sb);
      }
      sb.append(')');
      next = run;
    } else {
      emitDoubleValueSign(signs.get(0), sb);
    }
    for (int i = next; i < signs.size(); i++) {
      String op = ops.get(i - 1);
      if ("%".equals(op)) {
        // No double-modulo operator is available; modulo needs integer or
        // decimal operands.
        throw locatedError(mul,
            "Operator '%' (modulo) is not supported on DOUBLE operands; "
                + "use integer or decimal operands.");
      }
      // `||` (string concat) → `+`; not expected for double values.
      sb.append(' ').append("||".equals(op) ? "+" : op).append(' ');
      emitDoubleValueSign(signs.get(i), sb);
    }
  }

  private static void emitDoubleValueSign(
      LogicalTypesParser.Check_expr_unary_signContext sign, StringBuilder sb) {
    String category = numericCategory(sign);
    if (sign.getChildCount() > 1 && narrowerThan(category, "double")) {
      // A negated integer or decimal is negated in its own type, then converted.
      sb.append("double(");
      if ("int".equals(category)) {
        visitUnarySign(sign, sb);
      } else {
        emitSignAsDecimal(sign, sb);
      }
      sb.append(')');
      return;
    }
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode && "-".equals(child.getText())) {
        sb.append('-');
      }
    }
    emitDoubleValueCExpr(sign.c_expr(), sb);
  }

  private static void emitDoubleValueCExpr(
      LogicalTypesParser.C_exprContext ctx, StringBuilder sb) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (ctx instanceof LogicalTypesParser.CheckColumnRefContext) {
      LogicalTypesParser.ColumnrefContext cr =
          ((LogicalTypesParser.CheckColumnRefContext) ctx).columnref();
      if (isDoubleCategory(ConstraintResolver.resolveColumnRefType(cr, vctx))) {
        visitColumnRef(cr, sb);
      } else {
        sb.append("double(");
        visitColumnRef(cr, sb);
        sb.append(')');
      }
      return;
    }
    if (ctx instanceof LogicalTypesParser.CheckLiteralContext) {
      LogicalTypesParser.LiteralContext lit =
          ((LogicalTypesParser.CheckLiteralContext) ctx).literal();
      if (lit.intLiteral() != null) {
        // int literal → double literal (5 → 5.0).
        sb.append(lit.intLiteral().getText()).append(".0");
        return;
      }
      if (lit.decimalLiteral() != null) {
        sb.append(ConstraintPatterns.normalizeNumericLiteral(lit.decimalLiteral().getText()));
        return;
      }
      if (lit.doubleLiteral() != null) {
        sb.append(ConstraintPatterns.normalizeNumericLiteral(lit.doubleLiteral().getText()));
        return;
      }
      sb.append("double(");
      visitLiteral(lit, sb);
      sb.append(')');
      return;
    }
    if (ctx instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) ctx;
      if (paren.indirection() == null) {
        sb.append('(');
        emitDoubleValueCheckExpr(paren.check_expr(), sb);
        sb.append(')');
        return;
      }
    }
    if (ctx instanceof LogicalTypesParser.CheckFuncContext) {
      LogicalTypesParser.Func_exprContext fe =
          ((LogicalTypesParser.CheckFuncContext) ctx).func_expr();
      if (isDoubleCategory(ConstraintResolver.tryResolveFuncReturnType(fe, vctx))) {
        ConstraintFunctions.visitFuncExpr(fe, sb);
      } else {
        sb.append("double(");
        ConstraintFunctions.visitFuncExpr(fe, sb);
        sb.append(')');
      }
      return;
    }
    if (ctx instanceof LogicalTypesParser.CheckCaseContext) {
      LogicalTypesParser.Case_exprContext ce =
          ((LogicalTypesParser.CheckCaseContext) ctx).case_expr();
      if (isDoubleCategory(ConstraintResolver.tryResolveCaseExprType(ce, vctx))) {
        visitCase(ce, sb);
      } else {
        sb.append("double(");
        visitCase(ce, sb);
        sb.append(')');
      }
      return;
    }
    sb.append("double(");
    visitCExpr(ctx, sb);
    sb.append(')');
  }

  /** The numeric category ("int"/"decimal"/"double") of a subtree, or null when not numeric. */
  private static String numericCategory(ParseTree node) {
    return ConstraintResolver.coercedNumericCategory(node, EMIT_VCTX.get());
  }

  /**
   * Whether {@code category} widens to {@code target}: int to decimal or double, decimal to
   * double.
   */
  private static boolean narrowerThan(String category, String target) {
    return category != null && !category.equals(target)
        && target.equals(ConstraintResolver.numericCommon(category, target));
  }

  /**
   * How many leading operands combine to a category narrower than {@code target}.
   */
  private static int narrowerRun(List<? extends ParseTree> operands, String target) {
    String prefix = null;
    int run = 0;
    for (ParseTree operand : operands) {
      String category = numericCategory(operand);
      prefix = prefix == null || category == null
          ? category : ConstraintResolver.numericCommon(prefix, category);
      if (!narrowerThan(prefix, target)) {
        break;
      }
      run++;
    }
    return run;
  }

  /**
   * The category the first {@code count} operands combine to.
   */
  private static String runCategory(List<? extends ParseTree> operands, int count) {
    return numericCommonOf(operands.subList(0, count), EMIT_VCTX.get());
  }

  /** The operators between a chain's operands, in order. */
  private static List<String> operators(ParseTree chain) {
    List<String> ops = new ArrayList<>();
    for (int i = 0; i < chain.getChildCount(); i++) {
      if (chain.getChild(i) instanceof TerminalNode) {
        ops.add(chain.getChild(i).getText());
      }
    }
    return ops;
  }

  /**
   * The first {@code count} operands of an additive or multiplicative chain, emitted natively.
   */
  private static void emitNativeRun(
      List<? extends ParseTree> operands, List<String> ops, int count, StringBuilder sb) {
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        sb.append(' ').append(ops.get(i - 1)).append(' ');
      }
      ParseTree operand = operands.get(i);
      if (operand instanceof LogicalTypesParser.Check_expr_mulContext) {
        visitMul((LogicalTypesParser.Check_expr_mulContext) operand, sb);
      } else {
        visitUnarySign((LogicalTypesParser.Check_expr_unary_signContext) operand, sb);
      }
    }
  }

  private static boolean isDoubleCategory(io.confluent.kafka.schemaregistry.type.logical.Schema s) {
    return s != null && "double".equals(ConstraintResolver.categoryOf(s.getType()));
  }

  /**
   * LIKE translation:
   * <ul>
   *   <li>{@code x LIKE 'pat'} → {@code x.matches('^…$')} with {@code %}→{@code .*},
   *       {@code _}→{@code .}, regex specials escaped, default escape char {@code \}.</li>
   *   <li>{@code x LIKE 'pat' ESCAPE 'c'} — char {@code c} neutralizes the next
   *       metacharacter in the pattern.</li>
   *   <li>{@code x NOT LIKE 'pat'} → {@code !(x.matches('^…$'))}</li>
   * </ul>
   *
   * <p>SQL LIKE is whole-string match by default; CEL {@code matches()} is
   * unanchored — we always wrap with {@code ^…$} to preserve SQL semantics.
   */
  private static void visitLike(
      LogicalTypesParser.Check_expr_likeContext ctx, StringBuilder sb) {
    if (ctx.LIKE() == null) {
      visitAdd(ctx.check_expr_add(), sb);
      return;
    }
    boolean negated = ctx.NOT() != null;
    if (negated) {
      sb.append("!(");
    }
    // Wrap the receiver in parens only when needed. CEL `.matches()` binds
    // tighter than any arithmetic/concat operator, so a compound receiver
    // like `a || b` or `a + b` would mis-bind: `a + b.matches(...)` parses
    // as `a + (b.matches(...))`. Simple primaries (column refs, literals,
    // function calls, already-paren'd expressions) emit unwrapped.
    boolean wrap = !ConstraintResolver.isSimplePrimaryAdd(ctx.check_expr_add());
    if (wrap) {
      sb.append('(');
    }
    visitAdd(ctx.check_expr_add(), sb);
    if (wrap) {
      sb.append(')');
    }
    sb.append(".matches('");
    String patternRaw = ConstraintPatterns.extractSqlStringContent(ctx.stringLiteral().getText());
    char escapeChar = '\\';
    if (ctx.escape_clause() != null) {
      String escRaw = ConstraintPatterns.extractSqlStringContent(
          ctx.escape_clause().stringLiteral().getText());
      if (escRaw.length() != 1) {
        throw locatedError(ctx.escape_clause(),
            "ESCAPE clause must specify exactly one character, got: '"
                + escRaw + "'");
      }
      escapeChar = escRaw.charAt(0);
    }
    String regex = ConstraintPatterns.likePatternToRegex(patternRaw, escapeChar);
    sb.append(ConstraintPatterns.escapeForCelStringLiteral(regex));
    sb.append("')");
    if (negated) {
      sb.append(")");
    }
  }

  /**
   * SQL comparison ops to CEL: {@code =} → {@code ==}, {@code <>} → {@code !=}.
   * Others pass through unchanged.
   */
  private static String translateCompareOp(String sqlOp) {
    switch (sqlOp) {
      case "=":  return "==";
      case "<>": return "!=";
      default:   return sqlOp;  // !=, <, <=, >, >=
    }
  }

  private static String findComparisonOp(
      LogicalTypesParser.Check_expr_compareContext ctx) {
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode) {
        return child.getText();
      }
    }
    throw new ValidationException(
        "Internal: comparison op not found in check_expr_compare");
  }

  private static void visitAdd(
      LogicalTypesParser.Check_expr_addContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_mulContext> muls = ctx.check_expr_mul();
    visitMul(muls.get(0), sb);
    // Operators interleave between operands as terminal children.
    int mulIdx = 1;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode) {
        String op = child.getText();
        sb.append(' ').append(op).append(' ');
        visitMul(muls.get(mulIdx++), sb);
      }
    }
  }

  /**
   * Multiplicative level: {@code * / %} plus SQL {@code ||} (string/bytes
   * concat). {@code ||} maps to CEL {@code +}; the others pass through
   * unchanged.
   */
  private static void visitMul(
      LogicalTypesParser.Check_expr_mulContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = ctx.check_expr_unary_sign();
    visitUnarySign(signs.get(0), sb);
    int signIdx = 1;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode) {
        String op = child.getText();
        // SQL `||` is string/bytes concat → CEL `+`; `* / %` emit verbatim.
        sb.append(' ').append("||".equals(op) ? "+" : op).append(' ');
        visitUnarySign(signs.get(signIdx++), sb);
      }
    }
  }

  private static void visitUnarySign(
      LogicalTypesParser.Check_expr_unary_signContext ctx, StringBuilder sb) {
    // Optional leading '+' or '-' shows up as a TerminalNode child.
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode) {
        String op = child.getText();
        if ("-".equals(op)) {
          sb.append('-');
        }
        // Drop unary '+'; CEL doesn't need it.
      }
    }
    visitCExpr(ctx.c_expr(), sb);
  }

  static void visitCExpr(
      LogicalTypesParser.C_exprContext ctx, StringBuilder sb) {
    if (ctx instanceof LogicalTypesParser.CheckColumnRefContext) {
      visitColumnRef(
          ((LogicalTypesParser.CheckColumnRefContext) ctx).columnref(), sb);
    } else if (ctx instanceof LogicalTypesParser.CheckLiteralContext) {
      visitLiteral(
          ((LogicalTypesParser.CheckLiteralContext) ctx).literal(), sb);
    } else if (ctx instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) ctx;
      sb.append('(');
      visitCheckExpr(paren.check_expr(), sb);
      sb.append(')');
      if (paren.indirection() != null) {
        visitIndirection(paren.indirection(), sb);
      }
    } else if (ctx instanceof LogicalTypesParser.CheckCaseContext) {
      visitCase(
          ((LogicalTypesParser.CheckCaseContext) ctx).case_expr(), sb);
    } else if (ctx instanceof LogicalTypesParser.CheckFuncContext) {
      ConstraintFunctions.visitFuncExpr(
          ((LogicalTypesParser.CheckFuncContext) ctx).func_expr(), sb);
    } else {
      throw new ValidationException(
          "Unrecognized c_expr subtype: " + ctx.getClass().getSimpleName());
    }
  }

  /**
   * Emit a receiver expression wrapped in {@code (...)} so a subsequent
   * CEL {@code .method()} or {@code [index]} binds against the whole
   * receiver and not just its rightmost token. Used by every translator
   * site that emits {@code <expr>.method(...)} — wrapping is uniform and
   * always-on rather than gated by a "needs wrapping" heuristic.
   */
  static void emitWrappedReceiver(
      LogicalTypesParser.Check_exprContext ctx, StringBuilder sb) {
    if (ConstraintResolver.isSimplePrimary(ctx)) {
      visitCheckExpr(ctx, sb);
      return;
    }
    sb.append('(');
    visitCheckExpr(ctx, sb);
    sb.append(')');
  }

  private static void visitColumnRef(
      LogicalTypesParser.ColumnrefContext ctx, StringBuilder sb) {
    // Apply protovalidate's `this` convention. The column root is rewritten
    // based on what kind of identifier it is (resolved via vctx) and where
    // the CHECK clause is placed:
    //   - Schema column in a column-level CHECK   → `this`
    //     (the attached field is the only thing in scope; the field name
    //     itself collapses into `this`. Indirection chains follow `this`
    //     unchanged: e.g., `addr.zip` → `this.zip`.)
    //   - Schema column in a table-level CHECK    → `this.<col>`
    //     (the surrounding struct is `this`; access fields off it.)
    //   - Macro iter-var (e.g., `t` in EVERY)      → bare `<name>`
    //     (CEL macro bindings are not message fields.)
    //   - Runtime variable (e.g., `now`)           → bare `<name>`
    //
    // The vctx is non-null at this point — the public translate() entry
    // requires it.
    emitColumnRoot(colidName(ctx.colid()), sb);
    if (ctx.indirection() != null) {
      visitIndirection(ctx.indirection(), sb);
    }
  }

  private static void emitColumnRoot(String name, StringBuilder sb) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (vctx != null && vctx.isColumn(name)) {
      if (vctx.isColumnLevel()) {
        sb.append("this");
      } else if (Schema.isCelReservedName(name)) {
        // Field name collides with a CEL reserved word (e.g. `null`, `in`).
        // Dot-syntax `this.in` would fail to parse; emit index syntax
        // `this["in"]` so existing wire schemas with such names work.
        sb.append("this[\"").append(name).append("\"]");
      } else {
        sb.append("this.").append(name);
      }
    } else {
      sb.append(name);
    }
  }

  /**
   * The null (or not-null) test of a has()-compatible reference: each nullable step of its path
   * ({@link ConstraintResolver#nullableSteps}), outermost first, so a null struct decides it
   * before anything is selected from it; a constant for a path that is never null; or a test of
   * {@code emitted}, the whole reference, when the steps can't be told.
   */
  static String nullTest(List<String> path, String emitted, boolean isNull) {
    List<Integer> steps =
        path == null ? null : ConstraintResolver.nullableSteps(path, EMIT_VCTX.get());
    if (steps == null) {
      return stepTest(emitted, isNull);
    }
    if (steps.isEmpty()) {
      return isNull ? "false" : "true";
    }
    List<String> tests = new ArrayList<>();
    for (int step : steps) {
      tests.add(stepTest(step == path.size() ? emitted : prefix(path, step), isNull));
    }
    return tests.size() == 1 ? tests.get(0)
        : "(" + String.join(isNull ? " || " : " && ", tests) + ")";
  }

  // The first steps of a selection path, emitted.
  private static String prefix(List<String> path, int steps) {
    StringBuilder sb = new StringBuilder();
    emitColumnRoot(path.get(0), sb);
    for (int i = 1; i < steps; i++) {
      sb.append('.').append(path.get(i));
    }
    return sb.toString();
  }

  // One step's test: has() of a selection, or a null comparison of a bare iteration variable,
  // which has() can't take.
  private static String stepTest(String selection, boolean isNull) {
    if (selection.indexOf('.') < 0) {
      return "dyn(" + selection + (isNull ? ") == null" : ") != null");
    }
    return isNull ? "(!has(" + selection + ") || dyn(" + selection + ") == null)"
        : "(has(" + selection + ") && dyn(" + selection + ") != null)";
  }

  private static void visitIndirection(
      LogicalTypesParser.IndirectionContext ctx, StringBuilder sb) {
    // Receiver type tracks what type each indirection element is applied to,
    // walking from the column root down. We need this to discriminate ARRAY/
    // MULTISET indexing (1-based at SQL → emit `[expr - 1]`) from MAP keying
    // (no index conversion → emit `[expr]` verbatim). Without it we'd emit
    // `m['k'] - 1` for any map lookup, a CEL type error.
    //
    // Type info comes from the per-translation EMIT_VCTX (set by the
    // validating translate() entry). For paren-form indirection
    // ({@code (expr).field}) and the no-validation translate() entry, the
    // receiver type is unknown and we fall back to the array convention.
    Schema receiver = resolveIndirectionReceiverType(ctx);
    for (LogicalTypesParser.Indirection_elContext el : ctx.indirection_el()) {
      if (el.colid() != null) {
        String fieldName = colidName(el);
        if (Schema.isCelReservedName(fieldName)) {
          // CEL-reserved field name in nested access — escape via index
          // syntax (`["in"]` instead of `.in`) so existing wire schemas
          // with such names produce parseable CEL.
          sb.append("[\"").append(fieldName).append("\"]");
        } else {
          sb.append('.').append(fieldName);
        }
        receiver = stepStructField(receiver, fieldName);
      } else {
        emitBracketIndex(el, receiver, sb);
        receiver = stepBracketElement(receiver);
      }
    }
  }

  /**
   * Look up the type that this indirection chain is being applied to. For
   * {@code col.x[i]}, the receiver of the chain is the column's schema
   * (resolved via the validation context). For paren-form indirection
   * ({@code (expr).field}) and the no-validation entry point, returns null
   * — callers fall back to the array convention.
   */
  private static Schema resolveIndirectionReceiverType(
      LogicalTypesParser.IndirectionContext ctx) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (vctx == null) {
      return null;
    }
    if (ctx.getParent() instanceof LogicalTypesParser.ColumnrefContext) {
      LogicalTypesParser.ColumnrefContext col =
          (LogicalTypesParser.ColumnrefContext) ctx.getParent();
      return vctx.schemaOf(colidName(col.colid()));
    }
    if (ctx.getParent() instanceof LogicalTypesParser.CheckParenContext) {
      // Paren-form indirection: `(expr).field` or `(expr)[idx]`. Resolve the
      // wrapped expression's type so MAP indexing emits `map[key]` (not
      // `map[(key) - 1]`) and STRUCT field access can be type-validated.
      LogicalTypesParser.CheckParenContext paren =
          (LogicalTypesParser.CheckParenContext) ctx.getParent();
      return ConstraintResolver.tryResolveCheckExprType(paren.check_expr(), vctx);
    }
    return null;
  }

  /**
   * Resolve a {@code NAMED_TYPE_REF} receiver to its underlying schema via the
   * per-translation context, so the indirection walk discriminates STRUCT
   * field access and MAP-vs-ARRAY indexing through named-type chains the same
   * way {@link ConstraintResolver}/{@link ConstraintValidator} do. A null
   * context (the no-validation translate entry) or a non-ref schema is
   * returned unchanged.
   */
  private static Schema resolveRef(Schema s) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    return vctx == null ? s : ConstraintResolver.resolveIfNamedRef(s, vctx);
  }

  /**
   * Emit a bracket-form indirection. MAP → pass the key expression through
   * verbatim ({@code [expr]}). Otherwise — ARRAY/MULTISET, or unknown
   * receiver (paren-form, no validation context) — apply the SQL→CEL index
   * conversion: 1-based SQL becomes 0-based CEL ({@code [(expr) - 1]}).
   * Wrap the index in parens so the subtraction is precedence-safe
   * regardless of what the user wrote inside.
   */
  private static void emitBracketIndex(
      LogicalTypesParser.Indirection_elContext el,
      Schema receiver, StringBuilder sb) {
    receiver = resolveRef(receiver);
    boolean isMap = receiver != null && receiver.getType() == Schema.Type.MAP;
    if (isMap) {
      sb.append('[');
      visitCheckExpr(el.check_expr(), sb);
      sb.append(']');
      return;
    }
    sb.append("[(");
    visitCheckExpr(el.check_expr(), sb);
    sb.append(") - 1]");
  }

  private static Schema stepStructField(Schema receiver, String fieldName) {
    receiver = resolveRef(receiver);
    if (receiver == null || receiver.getType() != Schema.Type.STRUCT) {
      return null;
    }
    Schema.Field f = receiver.getField(fieldName);
    return f != null ? f.getSchema() : null;
  }

  private static Schema stepBracketElement(Schema receiver) {
    receiver = resolveRef(receiver);
    if (receiver == null) {
      return null;
    }
    switch (receiver.getType()) {
      case ARRAY:
      case MULTISET:
        return receiver.getElementType();
      case MAP:
        return receiver.getValueType();
      default:
        return null;
    }
  }

  private static void visitLiteral(
      LogicalTypesParser.LiteralContext ctx, StringBuilder sb) {
    if (ctx.NULL() != null) {
      sb.append("null");
    } else if (ctx.boolLiteral() != null) {
      sb.append(ctx.boolLiteral().getText().toLowerCase(java.util.Locale.ROOT));
    } else if (ctx.intLiteral() != null) {
      sb.append(ctx.intLiteral().getText());
    } else if (ctx.decimalLiteral() != null) {
      sb.append(ConstraintPatterns.normalizeNumericLiteral(ctx.decimalLiteral().getText()));
    } else if (ctx.doubleLiteral() != null) {
      sb.append(ConstraintPatterns.normalizeNumericLiteral(ctx.doubleLiteral().getText()));
    } else if (ctx.TIMESTAMP() != null) {
      // TIMESTAMP '2020-01-01 00:00:00' → timestamp("2020-01-01T00:00:00Z")
      // (CEL stdlib RFC-3339 constructor; zoneless literal assumed UTC).
      // Checked before stringLiteral: this literal also has a stringLiteral child.
      sb.append("timestamp(\"")
          .append(normalizeSqlTimestamp(ctx))
          .append("\")");
    } else if (ctx.INTERVAL() != null) {
      // INTERVAL '7' DAY → duration("604800s") (CEL stdlib duration; reject
      // calendar units YEAR/MONTH which have no fixed length). Also checked
      // before stringLiteral.
      sb.append("duration(\"")
          .append(intervalSeconds(ctx))
          .append("s\")");
    } else if (ctx.stringLiteral() != null) {
      sb.append(ConstraintPatterns.translateStringLiteral(ctx.stringLiteral().getText()));
    } else if (ctx.bytesLiteral() != null) {
      sb.append(ConstraintFunctions.translateBytesLiteral(
          ctx.bytesLiteral(), ctx.bytesLiteral().getText()));
    } else {
      throw new ValidationException(
          "Unrecognized literal: " + ctx.getText());
    }
  }

  /**
   * Normalize a SQL {@code TIMESTAMP '…'} literal body to an RFC-3339 string for
   * CEL's {@code timestamp(...)} constructor: replace the date/time space with
   * {@code T} and, when the literal carries no zone offset, append {@code Z}
   * (UTC) — a zoneless value is treated as UTC.
   * Rejects malformed datetimes at parse time.
   */
  private static String normalizeSqlTimestamp(LogicalTypesParser.LiteralContext ctx) {
    String content = ConstraintPatterns.extractSqlStringContent(
        ctx.stringLiteral().getText()).trim().replace(' ', 'T');
    if (content.length() == 10) {
      // date-only "YYYY-MM-DD" → midnight
      content = content + "T00:00:00";
    }
    try {
      // Explicit offset (…+02:00 / …Z) → valid RFC-3339 as-is.
      java.time.OffsetDateTime.parse(content);
      return content;
    } catch (java.time.format.DateTimeParseException withOffset) {
      try {
        // Zoneless local datetime → assume UTC.
        java.time.LocalDateTime.parse(content);
        return content + "Z";
      } catch (java.time.format.DateTimeParseException zoneless) {
        throw locatedError(ctx,
            "Invalid TIMESTAMP literal " + ctx.stringLiteral().getText()
                + ". Expected 'YYYY-MM-DD HH:MM:SS[.fff]' (optionally with a zone "
                + "offset); zoneless values are treated as UTC.");
      }
    }
  }

  /**
   * Convert a SQL {@code INTERVAL '<n>' <unit>} literal to whole seconds for
   * CEL's {@code duration(...)} constructor. Supports the fixed-length units
   * DAY / HOUR / MINUTE / SECOND; rejects YEAR / MONTH (no fixed length in CEL
   * duration) and non-integer values.
   */
  private static long intervalSeconds(LogicalTypesParser.LiteralContext ctx) {
    String unit = ctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    long multiplier;
    switch (unit) {
      case "DAY":
      case "DAYS":
        multiplier = 86400L;
        break;
      case "HOUR":
      case "HOURS":
        multiplier = 3600L;
        break;
      case "MINUTE":
      case "MINUTES":
        multiplier = 60L;
        break;
      case "SECOND":
      case "SECONDS":
        multiplier = 1L;
        break;
      case "YEAR":
      case "YEARS":
      case "MONTH":
      case "MONTHS":
        throw locatedError(ctx,
            "INTERVAL " + unit + " is not supported: durations have no fixed "
                + "calendar length. Use DAY, HOUR, MINUTE, or SECOND.");
      default:
        throw locatedError(ctx,
            "Unsupported INTERVAL unit '" + ctx.identifier().getText()
                + "'. Supported units: DAY, HOUR, MINUTE, SECOND.");
    }
    String valueStr = ConstraintPatterns.extractSqlStringContent(
        ctx.stringLiteral().getText()).trim();
    long value;
    try {
      value = Long.parseLong(valueStr);
    } catch (NumberFormatException e) {
      throw locatedError(ctx,
          "INTERVAL value '" + valueStr + "' must be an integer.");
    }
    try {
      return Math.multiplyExact(value, multiplier);
    } catch (ArithmeticException overflow) {
      throw locatedError(ctx, "INTERVAL value '" + valueStr + " " + unit + "' overflows.");
    }
  }

  private static void visitCase(
      LogicalTypesParser.Case_exprContext ctx, StringBuilder sb) {
    emitCached(ctx, Form.NATIVE, sb, out -> emitCase(ctx, out));
  }

  private static void emitCase(
      LogicalTypesParser.Case_exprContext ctx, StringBuilder sb) {
    // Two flavors:
    //   searched: CASE WHEN c1 THEN r1 WHEN c2 THEN r2 ELSE d END
    //   simple:   CASE x WHEN v1 THEN r1 WHEN v2 THEN r2 ELSE d END
    // Grammar:  CASE check_expr? when_clause+ ( ELSE check_expr )? END
    // The optional case-arg and the optional ELSE expression both populate
    // ctx.check_expr() as a list — disambiguate via child position.
    LogicalTypesParser.Check_exprContext caseArg = null;
    LogicalTypesParser.Check_exprContext elseExpr = null;
    boolean sawWhen = false;
    boolean afterElse = false;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode
          && "ELSE".equalsIgnoreCase(child.getText())) {
        afterElse = true;
      } else if (child instanceof LogicalTypesParser.When_clauseContext) {
        sawWhen = true;
      } else if (child instanceof LogicalTypesParser.Check_exprContext) {
        if (afterElse) {
          elseExpr = (LogicalTypesParser.Check_exprContext) child;
        } else if (!sawWhen) {
          caseArg = (LogicalTypesParser.Check_exprContext) child;
        }
      }
    }

    List<LogicalTypesParser.When_clauseContext> whens = ctx.when_clause();

    // The THEN/ELSE result branches unify to their common type, exactly like
    // COALESCE: numeric branches coerce to the common numeric type (DECIMAL →
    // Decimal values, DOUBLE → double-cast values) and temporal branches
    // normalize to CEL timestamps, so the emitted ternary's branches type-check
    // under the strict checker. Without this, `CASE WHEN c THEN intCol ELSE
    // decimalCol END` would emit `c ? int : Decimal`, which has no common CEL
    // type. (Non-numeric/mixed-incompatible branches yield common=null and emit
    // natively, leaving genuine mismatches for the strict checker to reject.)
    ConstraintValidationContext resultVctx = EMIT_VCTX.get();
    List<LogicalTypesParser.Check_exprContext> resultBranches = new ArrayList<>();
    for (LogicalTypesParser.When_clauseContext when : whens) {
      resultBranches.add(when.check_expr(1));
    }
    if (elseExpr != null) {
      resultBranches.add(elseExpr);
    }
    String resultCommon =
        (resultVctx != null) ? numericCommonOf(resultBranches, resultVctx) : null;
    boolean resultTemporal = resultVctx != null
        && !"decimal".equals(resultCommon) && !"double".equals(resultCommon)
        && resultBranchesHaveTemporal(resultBranches, resultVctx);

    sb.append("(");
    for (int i = 0; i < whens.size(); i++) {
      LogicalTypesParser.When_clauseContext when = whens.get(i);
      // Searched form: WHEN's first expr is the condition directly.
      // Simple form: "caseArg == WHEN's first expr" is the condition.
      // Both sides of the implicit `==` are wrapped in parens for compound
      // operands so a low-precedence operator inside either side
      // (e.g. `CASE x WHEN a OR b THEN ...`) doesn't mis-bind under CEL
      // (`==` binds tighter than `||`/`&&`). Simple primaries (column
      // refs, literals, function calls, already-paren'd) emit unwrapped.
      if (caseArg != null) {
        ConstraintValidationContext vctx = EMIT_VCTX.get();
        // SQL's `=` matches no NULL, so a subject or WHEN value that may be NULL is tested first.
        String eitherNull = ConstraintNullTest.either(
            ConstraintNullTest.told(ConstraintNullTest.of(caseArg), caseArg),
            ConstraintNullTest.told(ConstraintNullTest.of(when.check_expr(0)), when.check_expr(0)));
        boolean guarded = eitherNull != null && !ConstraintNullTest.NEVER.equals(eitherNull);
        if (guarded) {
          sb.append('(').append(eitherNull).append(" ? false : ");
        }
        // The implicit `caseArg == whenValue` comparison coerces to the
        // common numeric type of the two operands.
        String common = (vctx != null)
            ? numericCommonOf(caseArg, when.check_expr(0), vctx) : null;
        if ("decimal".equals(common)) {
          // Opaque Decimal has no native `==`; use decimals.eq for the implicit
          // simple-CASE subject comparison.
          sb.append("decimals.eq(");
          emitDecimalValue(caseArg, sb);
          sb.append(", ");
          emitDecimalValue(when.check_expr(0), sb);
          sb.append(')');
        } else if ("double".equals(common)) {
          emitNumericAs(caseArg, "double", sb);
          sb.append(" == ");
          emitNumericAs(when.check_expr(0), "double", sb);
        } else if (vctx != null
            && (ConstraintResolver.subtreeHasTemporal(caseArg, vctx)
                || ConstraintResolver.subtreeHasTemporal(when.check_expr(0), vctx))) {
          // Native `==`, operands timestamp-normalized and grouped, as a BETWEEN's && would
          // otherwise bind to the last comparison.
          sb.append('(');
          emitTimestampValueCheckExpr(caseArg, sb);
          sb.append(") == (");
          emitTimestampValueCheckExpr(when.check_expr(0), sb);
          sb.append(')');
        } else {
          emitWrappedReceiver(caseArg, sb);
          sb.append(" == ");
          emitWrappedReceiver(when.check_expr(0), sb);
        }
        if (guarded) {
          sb.append(')');
        }
      } else {
        emitCondition(when.check_expr(0), sb);
      }
      sb.append(" ? ");
      emitCaseResult(when.check_expr(1), resultCommon, resultTemporal, sb);
      sb.append(" : ");
      if (i < whens.size() - 1) {
        sb.append("(");
      }
    }
    if (elseExpr != null) {
      emitCaseResult(elseExpr, resultCommon, resultTemporal, sb);
    } else {
      // Postgres CASE without ELSE returns NULL on no match.
      sb.append("null");
    }
    // Close one paren per nested ternary (whens.size() - 1) plus the outer one.
    for (int i = 0; i < whens.size() - 1; i++) {
      sb.append(")");
    }
    sb.append(")");
  }

  /**
   * A searched CASE's condition, taken only when SQL's is TRUE, not FALSE or NULL. One with
   * nothing that may be NULL is emitted as before.
   */
  private static void emitCondition(
      LogicalTypesParser.Check_exprContext cond, StringBuilder sb) {
    StringBuilder whenTrue = new StringBuilder();
    if ((emitTruth(cond.check_expr_or(), true, whenTrue) & MAY_BE_NULL) != 0) {
      sb.append(whenTrue);
    } else {
      emitWrappedReceiver(cond, sb);
    }
  }

  /**
   * A CHECK's CEL, failing only when SQL's result is FALSE, so a NULL one passes, as in SQL.
   * One with nothing that may be NULL is emitted as is.
   */
  static void emitCheck(LogicalTypesParser.Check_exprContext ctx, StringBuilder sb) {
    StringBuilder whenFalse = new StringBuilder();
    if ((emitTruth(ctx.check_expr_or(), false, whenFalse) & MAY_BE_NULL) != 0) {
      sb.append('!').append(whenFalse);
    } else {
      visitCheckExpr(ctx, sb);
    }
  }

  // emitTruth's result: a predicate may be NULL; a predicate's NULL can't be told.
  static final int MAY_BE_NULL = 1;
  static final int UNTOLD = 2;

  /**
   * A condition true only when SQL's is {@code truth} (TRUE, or with {@code truth} false,
   * FALSE): a predicate that may be NULL is false then, while NOT, AND and OR keep SQL's three
   * values. Returns {@link #MAY_BE_NULL} and {@link #UNTOLD} flags; a predicate whose inputs
   * don't tell is emitted as is, as a NULL or missing input fails it either way.
   */
  static int emitTruth(
      LogicalTypesParser.Check_expr_orContext or, boolean truth, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_andContext> ands = or.check_expr_and();
    if (ands.size() == 1) {
      return emitTruth(ands.get(0), truth, sb);
    }
    return emitJunction(or, ands.size(), (i, t, out) -> emitTruth(ands.get(i), t, out),
        !truth, truth, sb);
  }

  private static int emitTruth(
      LogicalTypesParser.Check_expr_andContext and, boolean truth, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_unary_notContext> nots = and.check_expr_unary_not();
    if (nots.size() == 1) {
      return emitTruth(nots.get(0), truth, sb);
    }
    return emitJunction(and, nots.size(), (i, t, out) -> emitTruth(nots.get(i), t, out),
        truth, truth, sb);
  }

  // True if evaluating node can raise an error SQL raises too: a division, CAST, PARSE_JSON or
  // VARIANT_GET, or TRY_VARIANT_GET with a path that isn't a literal, so isn't checked
  // beforehand. Flink raises for neither a bad regex nor ROUND's or TRUNCATE's scale.
  static boolean mayRaise(ParseTree node) {
    if (node instanceof LogicalTypesParser.FuncCastContext
        || node instanceof LogicalTypesParser.FuncVariantGetContext) {
      return true;
    }
    if (node instanceof LogicalTypesParser.FuncTryVariantGetContext && ConstraintResolver
        .tryAsLiteral(((LogicalTypesParser.FuncTryVariantGetContext) node).check_expr(1)) == null) {
      return true;
    }
    if (node instanceof LogicalTypesParser.Func_applicationContext
        && raises((LogicalTypesParser.Func_applicationContext) node)) {
      return true;
    }
    if (node instanceof TerminalNode
        && node.getParent() instanceof LogicalTypesParser.Check_expr_mulContext
        && ("/".equals(node.getText()) || "%".equals(node.getText()))) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (mayRaise(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  private static boolean raises(LogicalTypesParser.Func_applicationContext app) {
    return "PARSE_JSON".equalsIgnoreCase(app.identifier().getText());
  }

  // emitTruth of operand i in a truth.
  private interface OperandTruth {
    int emit(int i, boolean truth, StringBuilder sb);
  }

  /**
   * The operands' truths joined by && when {@code all}, else ||. A NULL operand is false there,
   * which cel-java's && lets swallow another's error, so where one may raise an error they're
   * joined only once no operand decides it the other way.
   */
  private static int emitJunction(ParseTree node, int count, OperandTruth operand,
      boolean all, boolean truth, StringBuilder sb) {
    StringBuilder joined = new StringBuilder("(");
    int flags = 0;
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        joined.append(all ? " && " : " || ");
      }
      flags |= operand.emit(i, truth, joined);
    }
    joined.append(')');
    if (!all || (flags & MAY_BE_NULL) == 0 || !mayRaise(node)) {
      sb.append(joined);
      return flags;
    }
    sb.append("((");
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        sb.append(" || ");
      }
      operand.emit(i, !truth, sb);
    }
    sb.append(") ? false : ").append(joined).append(')');
    return flags;
  }

  private static int emitTruth(
      LogicalTypesParser.Check_expr_unary_notContext not, boolean truth, StringBuilder sb) {
    if (not instanceof LogicalTypesParser.CheckExprNotContext) {
      return emitTruth(
          ((LogicalTypesParser.CheckExprNotContext) not).check_expr_unary_not(), !truth, sb);
    }
    LogicalTypesParser.Check_expr_isnullContext is =
        ((LogicalTypesParser.CheckExprNotPassContext) not).check_expr_isnull();
    if (is.IS() == null) {
      LogicalTypesParser.C_exprContext c = ConstraintResolver.leafPrimary(is.check_expr_compare());
      if (c instanceof LogicalTypesParser.CheckParenContext
          && ((LogicalTypesParser.CheckParenContext) c).indirection() == null) {
        // Cached and size-checked, as nested AND and OR may repeat their operands.
        return cachedTruth(
            ((LogicalTypesParser.CheckParenContext) c).check_expr().check_expr_or(), truth, sb);
      }
    }
    if (is.IS() == null && is.check_expr_compare().check_expr_between().size() == 1
        && is.check_expr_compare().check_expr_between(0).BETWEEN() == null
        && givesNullAsNull(is.check_expr_compare().check_expr_between(0).check_expr_in(0))) {
      // Its CEL gives NULL as null, which equals neither true nor false.
      sb.append("(dyn(");
      visitIs(is, sb);
      sb.append(") == ").append(truth).append(')');
      return MAY_BE_NULL;
    }
    if (is.IS() == null && is.check_expr_compare().check_expr_between().size() == 1
        && is.check_expr_compare().check_expr_between(0).BETWEEN() != null) {
      return emitBetweenTruth(is.check_expr_compare().check_expr_between(0), truth, sb);
    }
    LogicalTypesParser.C_exprContext leaf =
        is.IS() == null ? ConstraintResolver.leafPrimary(is.check_expr_compare()) : null;
    if (leaf instanceof LogicalTypesParser.CheckFuncContext
        && ConstraintNullTest.isQuantifier((LogicalTypesParser.CheckFuncContext) leaf)) {
      return emitQuantifierTruth((LogicalTypesParser.CheckFuncContext) leaf, truth, sb);
    }
    String isNull = is.IS() != null ? null : ConstraintNullTest.of(is.check_expr_compare());
    boolean guarded = isNull != null && !ConstraintNullTest.NEVER.equals(isNull);
    sb.append('(');
    if (guarded) {
      sb.append(isNull).append(" ? false : ");
    }
    sb.append(truth ? "(" : "!(");
    visitIs(is, sb);
    sb.append("))");
    return guarded ? MAY_BE_NULL : is.IS() == null && isNull == null ? UNTOLD : 0;
  }

  /**
   * {@link #emitTruth} of EVERY, ANY or ONE: EVERY is TRUE when its body is TRUE for every element
   * and FALSE when it is FALSE for any, ANY the other way round, ONE TRUE for exactly one, and
   * all are NULL over a NULL collection. One with nothing that may be NULL is emitted as before.
   */
  static int emitQuantifierTruth(
      LogicalTypesParser.CheckFuncContext func, boolean truth, StringBuilder sb) {
    int flags = quantifierFold(func.func_expr().func_application(), truth, sb);
    if ((flags & NATIVE) != 0) {
      sb.append(truth ? "(" : "!(");
      visitCExpr(func, sb);
      sb.append(')');
    }
    return flags & ~NATIVE;
  }

  // quantifierFold's flag for one emitted as is: nothing may be NULL, or its NULL can't be told.
  private static final int NATIVE = 4;

  /**
   * ONE's own CEL: TRUE, FALSE or null from its three-valued forms, as cel-java's exists_one,
   * unlike all and exists, doesn't absorb a NULL body's error once the answer is decided; as is
   * when nothing may be NULL.
   */
  static void emitOne(
      LogicalTypesParser.Func_applicationContext app, StringBuilder sb, Runnable asIs) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    boolean folds = vctx != null && !vctx.isTypeOnly() && app.check_expr_list() != null
        && app.check_expr_list().check_expr().size() == 3;
    StringBuilder whenTrue = new StringBuilder();
    if (!folds || (quantifierFold(app, true, whenTrue) & NATIVE) != 0) {
      asIs.run();
      return;
    }
    StringBuilder whenFalse = new StringBuilder();
    quantifierFold(app, false, whenFalse);
    sb.append('(').append(whenTrue).append(" ? true : (").append(whenFalse)
        .append(" ? false : dyn(null)))");
  }

  // The three-valued fold of EVERY, ANY or ONE in a truth, or NATIVE with nothing emitted.
  private static int quantifierFold(
      LogicalTypesParser.Func_applicationContext app, boolean truth, StringBuilder sb) {
    List<LogicalTypesParser.Check_exprContext> args = app.check_expr_list().check_expr();
    String name = app.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    ConstraintValidationContext outer = EMIT_VCTX.get();
    String collectionNull = ConstraintNullTest.of(args.get(0));
    if (outer == null || collectionNull == null) {
      return NATIVE | UNTOLD;
    }
    String iterVar = ConstraintFunctions.extractIterationVar(args.get(1), name);
    ConstraintValidationContext body = bodyContext(app, outer, () -> outer.withBinding(iterVar,
        ConstraintResolver.resolveCollectionElementType(args.get(0), outer)));
    // ONE takes both of its body's truths.
    boolean one = "ONE".equals(name);
    StringBuilder bodyTruth = new StringBuilder();
    StringBuilder bodyFalse = new StringBuilder();
    int flags;
    EMIT_VCTX.set(body);
    try {
      flags = cachedTruth(args.get(2).check_expr_or(), truth || one, bodyTruth);
      if (one) {
        flags |= cachedTruth(args.get(2).check_expr_or(), false, bodyFalse);
      }
    } finally {
      EMIT_VCTX.set(outer);
    }
    if (flags == 0 && ConstraintNullTest.NEVER.equals(collectionNull)) {
      return NATIVE;
    }
    StringBuilder coll = new StringBuilder();
    emitWrappedReceiver(args.get(0), coll);
    String fold;
    if (one) {
      // TRUE when exactly one body is TRUE and every other FALSE; FALSE when two or more are
      // TRUE, or all are FALSE.
      // A ternary, as && would let a NULL body, false there, swallow exists_one's error.
      String exactlyOne = coll + ".exists_one(" + iterVar + ", " + bodyTruth + ")";
      fold = truth
          ? "(" + exactlyOne + " ? " + coll + ".all(" + iterVar + ", " + bodyTruth + " || "
              + bodyFalse + ") : false)"
          : "((" + coll + ".exists(" + iterVar + ", " + bodyTruth + ") && !" + exactlyOne
              + ") || " + coll + ".all(" + iterVar + ", " + bodyFalse + "))";
    } else if ("EVERY".equals(name) == truth && (flags & MAY_BE_NULL) != 0
        && mayRaise(args.get(2))) {
      // all() lets a NULL element, false there, swallow another's error, so it's taken only
      // once no element decides it the other way.
      StringBuilder decides = new StringBuilder();
      EMIT_VCTX.set(body);
      try {
        cachedTruth(args.get(2).check_expr_or(), !truth, decides);
      } finally {
        EMIT_VCTX.set(outer);
      }
      fold = "(" + coll + ".exists(" + iterVar + ", " + decides + ") ? false : " + coll
          + ".all(" + iterVar + ", " + bodyTruth + "))";
    } else {
      fold = coll + ("EVERY".equals(name) == truth ? ".all(" : ".exists(") + iterVar + ", "
          + bodyTruth + ")";
    }
    falseIfNull(collectionNull, fold, sb);
    return flags | (ConstraintNullTest.NEVER.equals(collectionNull) ? 0 : MAY_BE_NULL);
  }

  // A parenthesized or quantifier body's emitTruth, once per translation, context and truth.
  // Nesting may repeat it, so one over CEL's size limit is refused at once.
  private static int cachedTruth(
      LogicalTypesParser.Check_expr_orContext body, boolean truth, StringBuilder sb) {
    EmitCache cache = EMIT_CACHE.get();
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (cache == null || vctx == null) {
      return emitTruth(body, truth, sb);
    }
    Truth[] held = cache.truths.computeIfAbsent(vctx, k -> new IdentityHashMap<>())
        .computeIfAbsent(body, k -> new Truth[2]);
    int at = truth ? 0 : 1;
    if (held[at] == null) {
      StringBuilder cel = new StringBuilder();
      int flags = emitTruth(body, truth, cel);
      held[at] = new Truth(cel.toString(), flags);
      ConstraintCelChecker.checkSize(held[at].cel, true, cache.sourceSql);
    }
    sb.append(held[at].cel);
    return held[at].flags;
  }

  /**
   * The NULL test of a selection with a subscript, NULL as SQL gives it: when a step, an index
   * or the element is NULL, or a key or index is missing; null when a step's type doesn't
   * resolve.
   */
  static String subscriptNullTest(LogicalTypesParser.C_exprContext c) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (vctx == null) {
      return null;
    }
    List<String> tests = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    Schema type;
    LogicalTypesParser.IndirectionContext indirection;
    if (c instanceof LogicalTypesParser.CheckColumnRefContext) {
      LogicalTypesParser.ColumnrefContext col =
          ((LogicalTypesParser.CheckColumnRefContext) c).columnref();
      String root = colidName(col.colid());
      Schema rootSchema = vctx.schemaOf(root);
      if (rootSchema == null) {
        return null;
      }
      emitColumnRoot(root, prefix);
      // A column-level CHECK is skipped when its column is null.
      if (rootSchema.isNullable() && !(vctx.isColumnLevel() && vctx.isColumn(root))) {
        tests.add(stepTest(prefix.toString(), true));
      }
      type = resolveRef(rootSchema);
      indirection = col.indirection();
    } else {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) c;
      tests.add(ConstraintNullTest.of(paren.check_expr()));
      prefix.append('(');
      visitCheckExpr(paren.check_expr(), prefix);
      prefix.append(')');
      type = resolveRef(ConstraintResolver.tryResolveCheckExprType(paren.check_expr(), vctx));
      indirection = paren.indirection();
    }
    for (LogicalTypesParser.Indirection_elContext el : indirection.indirection_el()) {
      if (type == null) {
        return null;
      }
      Schema next;
      // has() takes a field's selection, but not a reserved name's bracket form.
      boolean selected = false;
      if (el.colid() != null) {
        Schema.Field f = type.getType() == Schema.Type.STRUCT ? type.getField(colidName(el)) : null;
        if (f == null) {
          return null;
        }
        String fieldName = colidName(el);
        selected = !Schema.isCelReservedName(fieldName);
        prefix.append(selected ? "." + fieldName : "[\"" + fieldName + "\"]");
        next = f.getSchema();
      } else {
        tests.add(ConstraintNullTest.of(el.check_expr()));
        StringBuilder index = new StringBuilder();
        visitCheckExpr(el.check_expr(), index);
        String collection = typedCollection(prefix);
        if (type.getType() == Schema.Type.MAP) {
          tests.add("!((" + index + ") in " + collection + ")");
          prefix.append('[').append(index).append(']');
          next = type.getValueType();
        } else if (type.getType() == Schema.Type.ARRAY) {
          tests.add(index.toString().matches("\\d+") ? "size(" + collection + ") < " + index
              : "((" + index + ") < 1 || (" + index + ") > size(" + collection + "))");
          prefix.append("[(").append(index).append(") - 1]");
          next = type.getElementType();
        } else {
          return null;
        }
      }
      if (next.isNullable()) {
        tests.add(selected ? stepTest(prefix.toString(), true) : "dyn(" + prefix + ") == null");
      }
      type = resolveRef(next);
    }
    return ConstraintNullTest.any(tests);
  }

  // A collection for size() or in: cel-java types a subscript of dyn as the first overload's
  // argument (size(m['k']) as a string's), unless it's dyn() itself.
  private static String typedCollection(CharSequence prefix) {
    char last = prefix.charAt(prefix.length() - 1);
    return last == ']' || last == ')' ? "dyn(" + prefix + ")" : prefix.toString();
  }

  // An operand of size() or in, given as typedCollection gives one, parenthesized or not;
  // the type-only pass keeps its type for the strict check.
  static String typedSubscript(CharSequence cel) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    return vctx != null && vctx.isTypeOnly() ? cel.toString() : typedCollection(cel);
  }

  // An IN whose three-valued CEL gives NULL as null.
  static boolean givesNullAsNull(LogicalTypesParser.Check_expr_inContext in) {
    return inElementMayBeNull(in) || inArrayElementMayBeNull(in);
  }

  /**
   * {@link #emitTruth} of a BETWEEN, SQL's {@code lo <= a AND a <= hi}: TRUE when both hold,
   * FALSE when either fails, whatever the other is. SYMMETRIC is NULL when any operand is.
   */
  static int emitBetweenTruth(
      LogicalTypesParser.Check_expr_betweenContext between, boolean truth, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_inContext> ins = between.check_expr_in();
    String a = ConstraintNullTest.of(ins.get(0));
    String lo = ConstraintNullTest.of(ins.get(1));
    String hi = ConstraintNullTest.of(ins.get(2));
    String anyNull = a == null || lo == null || hi == null ? null
        : ConstraintNullTest.any(Arrays.asList(a, lo, hi));
    if (anyNull == null || ConstraintNullTest.NEVER.equals(anyNull)) {
      sb.append(truth ? "(" : "!(");
      visitBetween(between, sb);
      sb.append(')');
      return anyNull == null ? UNTOLD : 0;
    }
    StringBuilder lower = new StringBuilder();
    emitBetweenLe(ins, 1, 0, lower);
    StringBuilder upper = new StringBuilder();
    emitBetweenLe(ins, 0, 2, upper);
    boolean holds = truth != (between.NOT() != null);
    if (between.SYMMETRIC() != null) {
      StringBuilder ordered = new StringBuilder("(");
      emitBetweenLe(ins, 1, 2, ordered);
      ordered.append(" ? (").append(lower).append(" && ").append(upper).append(") : (");
      emitBetweenLe(ins, 2, 0, ordered);
      ordered.append(" && ");
      emitBetweenLe(ins, 0, 1, ordered);
      ordered.append("))");
      sb.append('(').append(anyNull).append(" ? false : ").append(holds ? "" : "!")
          .append(ordered).append(')');
    } else if (holds && !mayRaise(between)) {
      sb.append('(').append(anyNull).append(" ? false : (").append(lower).append(" && ")
          .append(upper).append("))");
    } else if (holds) {
      // TRUE when both comparisons are, taken only once neither fails, so a NULL operand
      // doesn't hide another's error.
      sb.append("((");
      falseIfNull(ConstraintNullTest.either(lo, a), "!(" + lower + ")", sb);
      sb.append(" || ");
      falseIfNull(ConstraintNullTest.either(a, hi), "!(" + upper + ")", sb);
      sb.append(") ? false : (");
      falseIfNull(ConstraintNullTest.either(lo, a), lower.toString(), sb);
      sb.append(" && ");
      falseIfNull(ConstraintNullTest.either(a, hi), upper.toString(), sb);
      sb.append("))");
    } else {
      sb.append('(');
      falseIfNull(ConstraintNullTest.either(lo, a), "!(" + lower + ")", sb);
      sb.append(" || ");
      falseIfNull(ConstraintNullTest.either(a, hi), "!(" + upper + ")", sb);
      sb.append(')');
    }
    return MAY_BE_NULL;
  }

  // p, or false when isNull.
  private static void falseIfNull(String isNull, String p, StringBuilder sb) {
    if (ConstraintNullTest.NEVER.equals(isNull)) {
      sb.append(p);
    } else {
      sb.append('(').append(isNull).append(" ? false : ").append(p).append(')');
    }
  }

  // ins[x] <= ins[y] for BETWEEN operands, compared in the type visitBetween compares them in.
  private static void emitBetweenLe(
      List<LogicalTypesParser.Check_expr_inContext> ins, int x, int y, StringBuilder sb) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    String common = (vctx != null) ? numericCommonOf(ins, vctx) : null;
    if ("decimal".equals(common)) {
      emitDecimalLe(ins.get(x), ins.get(y), sb);
    } else if ("double".equals(common)) {
      emitDoubleLe(ins.get(x), ins.get(y), sb);
    } else if (vctx != null
        && (ConstraintResolver.subtreeHasTemporal(ins.get(0), vctx)
            || ConstraintResolver.subtreeHasTemporal(ins.get(1), vctx)
            || ConstraintResolver.subtreeHasTemporal(ins.get(2), vctx))) {
      emitTimestampLe(ins.get(x), ins.get(y), sb);
    } else {
      emitParenIn(ins.get(x), sb);
      sb.append(" <= ");
      emitParenIn(ins.get(y), sb);
    }
  }

  /**
   * Emit a CASE THEN/ELSE result coerced to the branches' common type, so the
   * ternary's branches type-unify. Mirrors {@code emitCoalesce}'s per-branch
   * coercion: DECIMAL common → Decimal value, DOUBLE common → double-cast,
   * temporal → CEL-timestamp normalization, otherwise native.
   */
  private static void emitCaseResult(
      LogicalTypesParser.Check_exprContext branch, String common,
      boolean temporal, StringBuilder sb) {
    StringBuilder nativeEmit = new StringBuilder();
    visitCheckExpr(branch, nativeEmit);
    StringBuilder value = new StringBuilder();
    if ("decimal".equals(common)) {
      emitDecimalValue(branch, value);
    } else if ("double".equals(common)) {
      emitNumericAs(branch, "double", value);
    } else if (temporal) {
      emitTimestampValueCheckExpr(branch, value);
    } else {
      value = nativeEmit;
    }
    // A NULL result is NULL, as a COALESCE argument is, not converted or read.
    if (ConstraintResolver.tryAsLiteral(branch) != null) {
      sb.append(value);
    } else {
      ConstraintFunctions.emitNullOrValue(branch, nativeEmit.toString(), value.toString(), sb);
    }
  }

  /** True if any of the CASE result branches has a temporal subtree. */
  private static boolean resultBranchesHaveTemporal(
      List<LogicalTypesParser.Check_exprContext> branches,
      ConstraintValidationContext vctx) {
    for (LogicalTypesParser.Check_exprContext b : branches) {
      if (ConstraintResolver.subtreeHasTemporal(b, vctx)) {
        return true;
      }
    }
    return false;
  }
}
