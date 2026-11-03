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
import java.util.function.Supplier;

/**
 * Cascade visitors for the CHECK→CEL translator. Each visitor walks one
 * level of the postgres-derived CHECK grammar and returns the equivalent CEL
 * as a {@link CelNode}, which brackets its operands as CEL's precedence needs
 * when printed. The cascade reflects SQL operator precedence — top to bottom:
 * OR, AND, NOT, IS NULL, comparison, BETWEEN, IN, LIKE, additive,
 * multiplicative ({@code ||} concat lives here), unary sign, c_expr (column
 * ref / literal / paren / case / func).
 *
 * <p>Visitors run after the validator suite has accepted the parse tree,
 * so they can assume well-formed input — no {@code null} guards, no
 * shape-checking past what's needed for the emit decision. Cross-cascade
 * jumps (e.g. value-cascade emitters called directly from
 * {@link ConstraintFunctions}'s function-arg emit) keep package-private
 * visibility.
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
    private final Map<ConstraintValidationContext, Map<ParseTree, CelNode[]>> emitted =
        new IdentityHashMap<>();
    private final Map<ConstraintValidationContext, Map<ParseTree, CelNode[]>> nullTests =
        new IdentityHashMap<>();
    private final Map<ConstraintValidationContext, Map<ParseTree, ConstraintValidationContext>>
        bodies = new IdentityHashMap<>();
    private final Map<ConstraintValidationContext, Map<ParseTree, Truth[]>> truths =
        new IdentityHashMap<>();

    EmitCache(String sourceSql) {
      this.sourceSql = sourceSql;
    }
  }

  /**
   * An emitTruth result: its CEL, null when nothing is emitted, and its flags.
   */
  static final class Truth {
    final CelNode cel;
    final int flags;

    Truth(CelNode cel, int flags) {
      this.cel = cel;
      this.flags = flags;
    }
  }

  static final ThreadLocal<EmitCache> EMIT_CACHE = new ThreadLocal<>();

  private enum Form { NATIVE, DECIMAL, DOUBLE, TIMESTAMP }

  // ctx's CEL in a form, emitted once per translation and context. One over CEL's size limit is
  // refused at once, as nesting may multiply it further.
  private static CelNode emitCached(ParseTree ctx, Form form, Supplier<CelNode> emit) {
    EmitCache cache = EMIT_CACHE.get();
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (cache == null || vctx == null) {
      return emit.get();
    }
    CelNode[] forms = cache.emitted.computeIfAbsent(vctx, k -> new IdentityHashMap<>())
        .computeIfAbsent(ctx, k -> new CelNode[Form.values().length]);
    if (forms[form.ordinal()] == null) {
      forms[form.ordinal()] = emit.get();
      ConstraintCelChecker.checkSize(forms[form.ordinal()], true, cache.sourceSql);
    }
    return forms[form.ordinal()];
  }

  static CelNode visitCheckExpr(LogicalTypesParser.Check_exprContext ctx) {
    return emitCached(ctx, Form.NATIVE, () -> visitOr(ctx.check_expr_or()));
  }

  /**
   * A node's NULL test, computed once per translation and context, and refused at once over
   * CEL's size limit; null when it can't be told.
   */
  static CelNode cachedNullTest(ParseTree node, Supplier<CelNode> test) {
    EmitCache cache = EMIT_CACHE.get();
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (cache == null || vctx == null) {
      return test.get();
    }
    Map<ParseTree, CelNode[]> tests =
        cache.nullTests.computeIfAbsent(vctx, k -> new IdentityHashMap<>());
    CelNode[] held = tests.get(node);
    if (held == null) {
      held = new CelNode[] {test.get()};
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

  private static CelNode visitOr(LogicalTypesParser.Check_expr_orContext ctx) {
    List<CelNode> ands = new ArrayList<>();
    for (LogicalTypesParser.Check_expr_andContext and : ctx.check_expr_and()) {
      ands.add(visitAnd(and));
    }
    return CelNode.or(ands);
  }

  private static CelNode visitAnd(LogicalTypesParser.Check_expr_andContext ctx) {
    List<CelNode> nots = new ArrayList<>();
    for (LogicalTypesParser.Check_expr_unary_notContext not : ctx.check_expr_unary_not()) {
      nots.add(visitUnaryNot(not));
    }
    return CelNode.and(nots);
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
   * <p>Each {@code <=} compares its operands in their common type, as {@link #emitBetweenLe}
   * does: {@code decimals.le} for DECIMAL, native {@code <=} on doubles for DOUBLE, on
   * normalized timestamps for a temporal operand, else native.
   */
  private static CelNode visitBetween(LogicalTypesParser.Check_expr_betweenContext ctx) {
    List<LogicalTypesParser.Check_expr_inContext> ins = ctx.check_expr_in();
    if (ins.size() == 1) {
      return visitIn(ins.get(0));
    }
    // Three operands: subject, lower, upper
    CelNode holds = ctx.SYMMETRIC() != null
        ? CelNode.ternary(emitBetweenLe(ins, 1, 2),
            CelNode.and(emitBetweenLe(ins, 1, 0), emitBetweenLe(ins, 0, 2)),
            CelNode.and(emitBetweenLe(ins, 2, 0), emitBetweenLe(ins, 0, 1)))
        : CelNode.and(emitBetweenLe(ins, 1, 0), emitBetweenLe(ins, 0, 2));
    return ctx.NOT() != null ? CelNode.not(holds) : holds;
  }

  /**
   * {@code decimals.le(a, b)} over two BETWEEN operands.
   */
  private static CelNode emitDecimalLe(
      LogicalTypesParser.Check_expr_inContext a, LogicalTypesParser.Check_expr_inContext b) {
    return CelNode.call("decimals.le", emitInAsDecimal(a), emitInAsDecimal(b));
  }

  /**
   * {@code a <= b} over two BETWEEN operands, both cast to double.
   */
  private static CelNode emitDoubleLe(
      LogicalTypesParser.Check_expr_inContext a, LogicalTypesParser.Check_expr_inContext b) {
    return CelNode.binary(emitNumericInAs(a, "double"), "<=", emitNumericInAs(b, "double"));
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
  static CelNode visitIn(LogicalTypesParser.Check_expr_inContext ctx) {
    if (ctx.IN() == null) {
      return visitLike(ctx.check_expr_like());
    }
    boolean negated = ctx.NOT() != null;
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (inElementMayBeNull(ctx)) {
      return emitInMayBeNull(ctx, negated);
    }
    if (inArrayElementMayBeNull(ctx)) {
      return emitInArrayMayBeNull(ctx, negated);
    }
    if (vctx != null) {
      LogicalTypesParser.In_targetContext target = ctx.in_target();
      if (target instanceof LogicalTypesParser.InTargetParenListContext) {
        // Numeric coercion over LHS + value-list elements. DECIMAL: CEL's
        // `in` has no Decimal overload, so emit an OR of decimals.eq. DOUBLE:
        // `double(lhs) in [double(e), …]`. INT/non-numeric falls through.
        String common = inListCommon(ctx, vctx);
        if ("decimal".equals(common)) {
          return emitInDecimal(ctx, negated);
        }
        if ("double".equals(common)) {
          return emitInDouble(ctx, negated);
        }
      } else if (likeHasDecimal(ctx.check_expr_like(), vctx)) {
        // Decimal LHS against a list-typed operand: emitInDecimal raises the
        // (unsupported) located error, preserving the prior clear message.
        return emitInDecimal(ctx, negated);
      }
    }
    // Timestamp: CEL's native `in` works on timestamps once the column LHS and
    // any column elements are normalized — emit the same `x in [...]` shape via
    // the timestamp-value cascade (no per-element rewrite needed, unlike decimal).
    CelNode match = vctx != null && inTargetHasTemporal(ctx, vctx)
        ? CelNode.binary(emitTimestampValueLike(ctx.check_expr_like()), "in",
            emitTimestampInTarget(ctx.in_target()))
        : CelNode.binary(visitLike(ctx.check_expr_like()), "in", visitInTarget(ctx.in_target()));
    CelNode guarded = unlessEmpty(ctx, match);
    return negated ? CelNode.not(guarded) : guarded;
  }

  // A match against a collection, false over an empty one without evaluating a subject that
  // may be NULL, or absent on JSON, as SQL's = ANY over no elements is FALSE.
  private static CelNode unlessEmpty(LogicalTypesParser.Check_expr_inContext ctx,
      CelNode match) {
    if (EMIT_VCTX.get() == null
        || !(ctx.in_target() instanceof LogicalTypesParser.InTargetExprContext)
        || ConstraintNullTest.NEVER.equals(ConstraintNullTest.of(ctx.check_expr_like()))) {
      return match;
    }
    CelNode empty = CelNode.binary(collectionSize(((LogicalTypesParser.InTargetExprContext)
        ctx.in_target()).check_expr_like()), "==", CelNode.literal("0"));
    return CelNode.ternary(empty, CelNode.FALSE, match);
  }

  // The size of the collection an IN tests.
  static CelNode collectionSize(LogicalTypesParser.Check_expr_likeContext target) {
    return CelNode.call("size", typedCollection(visitLike(target), false));
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
      CelNode test = ConstraintNullTest.of(item);
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
  private static CelNode emitInMayBeNull(
      LogicalTypesParser.Check_expr_inContext ctx, boolean negated) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    List<LogicalTypesParser.Check_exprContext> items = inItems(ctx);
    String common = inListCommon(ctx, vctx);
    boolean temporal = !"decimal".equals(common) && !"double".equals(common)
        && inTargetHasTemporal(ctx, vctx);
    CelNode subject;
    if ("decimal".equals(common)) {
      subject = emitLikeAsDecimalValue(ctx.check_expr_like());
    } else if ("double".equals(common)) {
      subject = emitDoubleValueLike(ctx.check_expr_like());
    } else if (temporal) {
      subject = emitTimestampValueLike(ctx.check_expr_like());
    } else {
      subject = visitLike(ctx.check_expr_like());
    }
    List<CelNode> elementNulls = new ArrayList<>();
    List<CelNode> matches = new ArrayList<>();
    for (LogicalTypesParser.Check_exprContext item : items) {
      CelNode elementNull = ConstraintNullTest.of(item);
      elementNulls.add(elementNull);
      CelNode equal;
      if ("decimal".equals(common)) {
        equal = CelNode.call("decimals.eq", subject, emitDecimalValue(item));
      } else if ("double".equals(common)) {
        equal = CelNode.binary(subject, "==", emitDoubleValueCheckExpr(item));
      } else if (temporal) {
        equal = CelNode.binary(subject, "==", emitTimestampValueCheckExpr(item));
      } else {
        equal = CelNode.binary(subject, "==", visitCheckExpr(item));
      }
      matches.add(ConstraintNullTest.NEVER.equals(elementNull) ? equal
          : CelNode.and(ConstraintNullTest.not(elementNull), equal));
    }
    CelNode match = CelNode.or(matches);
    CelNode subjectNull = ConstraintNullTest.of(ctx.check_expr_like());
    CelNode anyElementNull = ConstraintNullTest.any(elementNulls);
    if (vctx != null && vctx.isTypeOnly()) {
      return negated ? CelNode.not(match) : match;
    }
    CelNode noMatch = ConstraintNullTest.NEVER.equals(anyElementNull) ? CelNode.bool(negated)
        : CelNode.ternary(anyElementNull, CelNode.dyn(CelNode.NULL), CelNode.bool(negated));
    CelNode unlessSubjectNull = CelNode.ternary(match, CelNode.bool(!negated), noMatch);
    return ConstraintNullTest.NEVER.equals(subjectNull) ? unlessSubjectNull
        : CelNode.ternary(subjectNull, CelNode.dyn(CelNode.NULL), unlessSubjectNull);
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
    CelNode subject = ConstraintNullTest.of(ctx.check_expr_like());
    CelNode array = ConstraintNullTest.of(
        ((LogicalTypesParser.InTargetExprContext) ctx.in_target()).check_expr_like());
    Schema element = ConstraintResolver.inArrayElementType(ctx, vctx);
    return subject != null && array != null && element != null && element.isNullable();
  }

  /**
   * SQL's {@code x IN arr}: NULL when {@code arr} is, or {@code x} is and {@code arr} isn't
   * empty, else TRUE on a match, else NULL when an element is NULL, else FALSE; NOT IN negates
   * it, keeping NULL.
   */
  private static CelNode emitInArrayMayBeNull(
      LogicalTypesParser.Check_expr_inContext ctx, boolean negated) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    LogicalTypesParser.Check_expr_likeContext array =
        ((LogicalTypesParser.InTargetExprContext) ctx.in_target()).check_expr_like();
    CelNode match = unlessEmpty(ctx, inTargetHasTemporal(ctx, vctx)
        ? CelNode.binary(emitTimestampValueLike(ctx.check_expr_like()), "in",
            emitTimestampInTarget(ctx.in_target()))
        : CelNode.binary(visitLike(ctx.check_expr_like()), "in", visitInTarget(ctx.in_target())));
    if (vctx.isTypeOnly()) {
      return negated ? CelNode.not(match) : match;
    }
    CelNode v = CelNode.ident("v");
    CelNode noMatch = CelNode.ternary(
        CelNode.method(visitLike(array), "exists", v, CelNode.isNull(v)),
        CelNode.dyn(CelNode.NULL), CelNode.bool(negated));
    CelNode unlessNull = CelNode.ternary(match, CelNode.bool(!negated), noMatch);
    CelNode isNull = ConstraintNullTest.ofCollectionIn(ctx,
        ConstraintNullTest.of(ctx.check_expr_like()));
    return ConstraintNullTest.NEVER.equals(isNull) ? unlessNull
        : CelNode.ternary(isNull, CelNode.dyn(CelNode.NULL), unlessNull);
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
  private static CelNode emitTimestampInTarget(LogicalTypesParser.In_targetContext ctx) {
    if (ctx instanceof LogicalTypesParser.InTargetParenListContext) {
      List<CelNode> elements = new ArrayList<>();
      for (LogicalTypesParser.Check_exprContext item
          : ((LogicalTypesParser.InTargetParenListContext) ctx).check_expr_list().check_expr()) {
        elements.add(emitTimestampValueCheckExpr(item));
      }
      return CelNode.list(elements);
    }
    // IN <list-field>: the field is a list of timestamps; CEL `in` handles it
    // natively. The receiver still normalizes via the cascade (field ref).
    return typedSubscript(emitTimestampValueLike(
        ((LogicalTypesParser.InTargetExprContext) ctx).check_expr_like()));
  }

  static CelNode emitTimestampValueLike(LogicalTypesParser.Check_expr_likeContext like) {
    return like.LIKE() == null ? emitTimestampValueAdd(like.check_expr_add())
        : visitLike(like);  // not a bare value (defensive); emit native
  }

  /**
   * Decimal {@code a IN (x, y, …)} → {@code (decimals.eq(a, x) || decimals.eq(a, y) || …)}
   * (negated wraps with {@code !}). The {@code IN <list-field>} form has no
   * Decimal-aware equivalent and is rejected.
   */
  private static CelNode emitInDecimal(
      LogicalTypesParser.Check_expr_inContext ctx, boolean negated) {
    LogicalTypesParser.In_targetContext target = ctx.in_target();
    if (!(target instanceof LogicalTypesParser.InTargetParenListContext)) {
      throw locatedError(ctx,
          "IN against a list-typed operand is not supported for DECIMAL; use an "
              + "explicit value list, e.g. amount IN (1.0, 2.5, 9.99).");
    }
    CelNode lhs = emitLikeAsDecimalValue(ctx.check_expr_like());
    List<CelNode> equals = new ArrayList<>();
    for (LogicalTypesParser.Check_exprContext item
        : ((LogicalTypesParser.InTargetParenListContext) target).check_expr_list().check_expr()) {
      equals.add(CelNode.call("decimals.eq", lhs, emitDecimalValue(item)));
    }
    CelNode any = CelNode.or(equals);
    return negated ? CelNode.not(any) : any;
  }

  /**
   * Double IN: {@code double(lhs) in [double(e1), double(e2), …]}. CEL's native
   * {@code in} does element equality, which works for double once every element
   * is cast to double.
   */
  private static CelNode emitInDouble(
      LogicalTypesParser.Check_expr_inContext ctx, boolean negated) {
    List<CelNode> elements = new ArrayList<>();
    for (LogicalTypesParser.Check_exprContext item : inItems(ctx)) {
      elements.add(emitDoubleValueCheckExpr(item));
    }
    CelNode in = CelNode.binary(emitDoubleValueLike(ctx.check_expr_like()), "in",
        CelNode.list(elements));
    return negated ? CelNode.not(in) : in;
  }

  private static CelNode visitInTarget(LogicalTypesParser.In_targetContext ctx) {
    if (ctx instanceof LogicalTypesParser.InTargetParenListContext) {
      List<CelNode> elements = new ArrayList<>();
      for (LogicalTypesParser.Check_exprContext item
          : ((LogicalTypesParser.InTargetParenListContext) ctx).check_expr_list().check_expr()) {
        elements.add(visitCheckExpr(item));
      }
      return CelNode.list(elements);
    }
    if (ctx instanceof LogicalTypesParser.InTargetExprContext) {
      return typedSubscript(
          visitLike(((LogicalTypesParser.InTargetExprContext) ctx).check_expr_like()));
    }
    throw new ValidationException(
        "Unrecognized in_target subtype: " + ctx.getClass().getSimpleName());
  }

  /**
   * SQL {@code NOT} has lower precedence than IS/comparison/BETWEEN/IN/LIKE,
   * so {@code NOT x > 0} means {@code NOT (x > 0)}; the printed {@code !}
   * brackets its operand where CEL's tighter {@code !} needs it.
   *
   * <p>The grammar's NOT alternative is recursive (matches real PG), so
   * {@code NOT NOT x} chains cleanly here.
   */
  private static CelNode visitUnaryNot(LogicalTypesParser.Check_expr_unary_notContext ctx) {
    if (ctx instanceof LogicalTypesParser.CheckExprNotContext) {
      return CelNode.not(
          visitUnaryNot(((LogicalTypesParser.CheckExprNotContext) ctx).check_expr_unary_not()));
    }
    if (ctx instanceof LogicalTypesParser.CheckExprNotPassContext) {
      return visitIs(((LogicalTypesParser.CheckExprNotPassContext) ctx).check_expr_isnull());
    }
    throw new ValidationException(
        "Unrecognized check_expr_unary_not subtype: " + ctx.getClass().getSimpleName());
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
  private static CelNode visitIs(LogicalTypesParser.Check_expr_isnullContext ctx) {
    if (ctx.IS() == null) {
      return visitCompare(ctx.check_expr_compare());
    }
    boolean negated = ctx.NOT() != null;
    if (ConstraintResolver.isHasCompatible(ctx.check_expr_compare(), EMIT_VCTX.get())) {
      return nullTest(ConstraintResolver.selectionPathCompare(ctx.check_expr_compare()),
          visitCompare(ctx.check_expr_compare()), !negated);
    }
    // An expression is tested by its inputs, as evaluating it on a NULL one fails, and is still
    // evaluated otherwise, so an error such as bad JSON fails as SQL's does. The strict check
    // types it in its own numeric type, as DECIMAL arithmetic has no native form.
    LogicalTypesParser.Check_expr_compareContext operand = ctx.check_expr_compare();
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    CelNode byInputs = ConstraintNullTest.isTestedByValue(operand) ? null
        : ConstraintNullTest.told(ConstraintNullTest.of(operand), operand);
    String category = operand.check_expr_between().size() == 1 && vctx != null
        ? ConstraintResolver.coercedNumericCategory(operand.check_expr_between(0), vctx) : null;
    CelNode emitted = "decimal".equals(category) || "double".equals(category)
        ? emitNumericBetweenAs(operand.check_expr_between(0), category) : visitCompare(operand);
    CelNode value = CelNode.isNull(emitted);
    if (byInputs == null || vctx != null && vctx.isTypeOnly()) {
      return negated ? ConstraintNullTest.not(value) : value;
    }
    if (byInputs.equals(value)) {
      return negated ? ConstraintNullTest.not(byInputs) : byInputs;
    }
    if (byInputs.contains(value)) {
      // The test evaluates it already, as a VARIANT_GET's or a subscript's does. Ternaries, as
      // cel-java's || and && hide an error once their result is compared.
      return CelNode.ternary(byInputs, CelNode.bool(!negated), CelNode.bool(negated));
    }
    // A predicate's CEL may be || or && itself, so its value is a ternary's condition too:
    // its inputs tell it isn't NULL there.
    Schema type = vctx == null ? null
        : ConstraintResolver.tryResolveCompareValueType(operand, vctx);
    CelNode ofValue = type != null && type.getType() == Schema.Type.BOOLEAN
        ? CelNode.ternary(emitted, CelNode.bool(negated), CelNode.bool(negated))
        : negated ? ConstraintNullTest.not(value) : value;
    if (ConstraintNullTest.NEVER.equals(byInputs)) {
      // Never NULL: evaluated only for its errors.
      return ofValue;
    }
    if (ConstraintNullTest.ALWAYS.equals(byInputs)) {
      // Always NULL: never evaluated.
      return CelNode.bool(!negated);
    }
    return CelNode.ternary(byInputs, CelNode.bool(!negated), ofValue);
  }

  private static CelNode visitCompare(LogicalTypesParser.Check_expr_compareContext ctx) {
    List<LogicalTypesParser.Check_expr_betweenContext> betweens = ctx.check_expr_between();
    if (betweens.size() == 2) {
      // The comparison operator is the second child of the rule (between the
      // two check_expr_between). Find it as the only TerminalNode child.
      String celOp = translateCompareOp(findComparisonOp(ctx));
      ConstraintValidationContext vctx = EMIT_VCTX.get();
      CelNode literalCompare = emitBooleanLiteralCompare(betweens, celOp, vctx);
      if (literalCompare != null) {
        return literalCompare;
      }
      // Numeric coercion: if both sides are pure numeric, compute the
      // common type and emit at it. common=DECIMAL → decimals.* (no native
      // overload on the opaque type); common=DOUBLE/INT → native operator with
      // operands cast to the common type (double()/double literal as needed).
      //
      // coercedNumericCategory classifies a boolean-shaped operand (BETWEEN/
      // IN/LIKE, even parenthesized) as non-numeric, so this path only sees
      // bare numeric values — the assumption emitNumericBetweenAs relies on.
      // Boolean predicate operands fall through to the native path.
      if (vctx != null) {
        String lc = ConstraintResolver.coercedNumericCategory(betweens.get(0), vctx);
        String rc = ConstraintResolver.coercedNumericCategory(betweens.get(1), vctx);
        if (lc != null && rc != null) {
          String common = ConstraintResolver.numericCommon(lc, rc);
          if ("decimal".equals(common)) {
            return emitDecimalCompare(betweens.get(0), celOp, betweens.get(1));
          }
          return CelNode.binary(emitNumericBetweenAs(betweens.get(0), common), celOp,
              emitNumericBetweenAs(betweens.get(1), common));
        }
      }
      // Non-numeric: timestamp normalization (instant-timestamp operands wrapped
      // in timestamp(...); CURRENT_TIMESTAMP stays `now`) or plain native.
      return CelNode.binary(emitCompareOperand(betweens.get(0), vctx), celOp,
          emitCompareOperand(betweens.get(1), vctx));
    }
    return visitBetween(betweens.get(0));
  }

  /**
   * A BOOLEAN compared with = or <> to a TRUE or FALSE literal, as that side or its negation:
   * {@code t = TRUE} is {@code this.t}. Null for any other comparison.
   */
  private static CelNode emitBooleanLiteralCompare(
      List<LogicalTypesParser.Check_expr_betweenContext> sides, String celOp,
      ConstraintValidationContext vctx) {
    if (vctx == null || !"==".equals(celOp) && !"!=".equals(celOp)) {
      return null;
    }
    for (int i = 0; i < 2; i++) {
      LogicalTypesParser.BoolLiteralContext literal = boolLiteral(sides.get(i));
      LogicalTypesParser.Check_expr_betweenContext other = sides.get(1 - i);
      Schema type = literal == null ? null
          : ConstraintResolver.tryResolveCompareOperandType(other, vctx);
      if (type != null && type.getType() == Schema.Type.BOOLEAN) {
        boolean positive = "==".equals(celOp) == (literal.TRUE() != null);
        CelNode operand = emitCompareOperand(other, vctx);
        return positive ? operand : CelNode.not(operand);
      }
    }
    return null;
  }

  // A comparison operand's unsigned TRUE or FALSE literal, or null; a signed one, even one whose
  // signs cancel, is left to the strict check to refuse.
  private static LogicalTypesParser.BoolLiteralContext boolLiteral(
      LogicalTypesParser.Check_expr_betweenContext side) {
    if (side.BETWEEN() != null || side.check_expr_in().size() != 1) {
      return null;
    }
    ConstraintResolver.LiteralMatch m = ConstraintResolver.tryAsLiteralIn(side.check_expr_in(0));
    return m == null || hasSign(side) ? null : m.lit.boolLiteral();
  }

  // True if a parse tree holds a + or - token.
  private static boolean hasSign(ParseTree tree) {
    if (tree instanceof TerminalNode) {
      return "-".equals(tree.getText()) || "+".equals(tree.getText());
    }
    for (int i = 0; i < tree.getChildCount(); i++) {
      if (hasSign(tree.getChild(i))) {
        return true;
      }
    }
    return false;
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
  private static CelNode emitCompareOperand(
      LogicalTypesParser.Check_expr_betweenContext between, ConstraintValidationContext vctx) {
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
      return emitTimestampValueAdd(add);
    }
    return visitBetween(between);
  }

  /**
   * Emit an EXTRACT receiver. Temporal receivers go through the timestamp-value
   * cascade (so {@code ts} → {@code timestamp(this.ts)}); {@code now} and
   * non-temporal receivers emit natively.
   */
  static CelNode emitTimestampReceiver(LogicalTypesParser.Check_exprContext receiver) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    return vctx != null && ConstraintResolver.subtreeHasTemporal(receiver, vctx)
        ? emitTimestampValueCheckExpr(receiver) : visitCheckExpr(receiver);
  }

  private static CelNode emitTimestampLe(
      LogicalTypesParser.Check_expr_inContext a, LogicalTypesParser.Check_expr_inContext b) {
    return CelNode.binary(emitTimestampValueIn(a), "<=", emitTimestampValueIn(b));
  }

  // ---- timestamp-value cascade: native emit, but instant-timestamp column
  // leaves wrapped in timestamp(...). Reused by comparison, BETWEEN, EXTRACT,
  // and the Phase B constructs. ----

  static CelNode emitTimestampValueIn(LogicalTypesParser.Check_expr_inContext in) {
    LogicalTypesParser.Check_expr_addContext add = bareAddOfIn(in);
    return add != null ? emitTimestampValueAdd(add)
        : visitIn(in);  // not a bare value (defensive); emit native
  }

  static CelNode emitTimestampValueCheckExpr(LogicalTypesParser.Check_exprContext ctx) {
    return emitCached(ctx, Form.TIMESTAMP, () -> {
      LogicalTypesParser.Check_expr_addContext add = bareAddOfOr(ctx.check_expr_or());
      return add != null ? emitTimestampValueAdd(add)
          : visitCheckExpr(ctx);  // not a bare value (defensive)
    });
  }

  private static CelNode emitTimestampValueAdd(LogicalTypesParser.Check_expr_addContext add) {
    List<LogicalTypesParser.Check_expr_mulContext> muls = add.check_expr_mul();
    List<String> ops = operators(add);
    CelNode acc = emitTimestampValueMul(muls.get(0));
    for (int i = 1; i < muls.size(); i++) {
      acc = CelNode.binary(acc, ops.get(i - 1), emitTimestampValueMul(muls.get(i)));
    }
    return acc;
  }

  private static CelNode emitTimestampValueMul(LogicalTypesParser.Check_expr_mulContext mul) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = mul.check_expr_unary_sign();
    List<String> ops = operators(mul);
    CelNode acc = emitTimestampValueSign(signs.get(0));
    for (int i = 1; i < signs.size(); i++) {
      // `||` (string concat) → CEL `+`; not expected for temporal values.
      acc = CelNode.binary(acc, celMulOp(ops.get(i - 1)), emitTimestampValueSign(signs.get(i)));
    }
    return acc;
  }

  private static CelNode emitTimestampValueSign(
      LogicalTypesParser.Check_expr_unary_signContext sign) {
    // '+' dropped, matching native visitUnarySign
    return signed(sign, emitTimestampValueCExpr(sign.c_expr()));
  }

  private static CelNode emitTimestampValueCExpr(LogicalTypesParser.C_exprContext ctx) {
    if (ctx instanceof LogicalTypesParser.CheckColumnRefContext) {
      LogicalTypesParser.ColumnrefContext cr =
          ((LogicalTypesParser.CheckColumnRefContext) ctx).columnref();
      CelNode column = visitColumnRef(cr);
      return ConstraintResolver.isInstantTimestamp(
          ConstraintResolver.resolveColumnRefType(cr, EMIT_VCTX.get()))
          ? CelNode.call("timestamp", column) : column;
    }
    if (ctx instanceof LogicalTypesParser.CheckParenContext
        && ((LogicalTypesParser.CheckParenContext) ctx).indirection() == null) {
      return emitTimestampValueCheckExpr(((LogicalTypesParser.CheckParenContext) ctx).check_expr());
    }
    // Literal (TIMESTAMP/INTERVAL → timestamp(...)/duration(...)), function
    // (self-normalizing, incl. Phase B constructs), case, or paren+indirection:
    // emit natively.
    return visitCExpr(ctx);
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
  static CelNode emitDecimalValue(LogicalTypesParser.Check_exprContext ctx) {
    return emitCheckExprAsDecimal(ctx);
  }

  /**
   * {@code a <op> b} where a/b are decimal → {@code decimals.<fn>(a, b)}.
   */
  private static CelNode emitDecimalCompare(
      LogicalTypesParser.Check_expr_betweenContext lhs, String celOp,
      LogicalTypesParser.Check_expr_betweenContext rhs) {
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
    CelNode compare = CelNode.call(fn, emitBetweenAsDecimal(lhs), emitBetweenAsDecimal(rhs));
    return negate ? CelNode.not(compare) : compare;
  }

  /**
   * Emit a {@code check_expr_between} comparison operand as a Decimal value. A
   * decimal operand is a bare value (no BETWEEN/IN/LIKE), so it descends
   * between → in → like → add.
   */
  private static CelNode emitBetweenAsDecimal(
      LogicalTypesParser.Check_expr_betweenContext between) {
    return emitLikeAsDecimalValue(between.check_expr_in(0).check_expr_like());
  }

  /**
   * Emit a {@code check_expr_like} (an IN left operand or comparison operand) as
   * a Decimal value. A decimal operand never carries LIKE (that yields bool);
   * detection guarantees the add value root in normal use, with a defensive
   * {@code decimal(...)} coercion fallback if a LIKE somehow reaches here.
   */
  private static CelNode emitLikeAsDecimalValue(LogicalTypesParser.Check_expr_likeContext like) {
    return like.LIKE() == null ? emitAddAsDecimal(like.check_expr_add())
        : CelNode.call("decimal", visitLike(like));
  }

  /** Fold an additive chain left-associatively into decimals.add/sub calls. */
  private static CelNode emitAddAsDecimal(LogicalTypesParser.Check_expr_addContext add) {
    List<LogicalTypesParser.Check_expr_mulContext> muls = add.check_expr_mul();
    return emitAddRangeAsDecimal(muls, operators(add), muls.size());
  }

  /**
   * Fold the first {@code count} operands of an additive chain into decimals.add/sub calls.
   * Leading integer operands are added natively and widened once, as SQL types each operator
   * by its own operands.
   */
  private static CelNode emitAddRangeAsDecimal(
      List<LogicalTypesParser.Check_expr_mulContext> muls, List<String> ops, int count) {
    int run = Math.min(narrowerRun(muls, "decimal"), count);
    int next = 1;
    CelNode acc;
    if (run >= 2) {
      acc = CelNode.call("decimal", emitNativeRun(muls, ops, run));
      next = run;
    } else {
      acc = emitMulAsDecimal(muls.get(0));
    }
    for (int i = next; i < count; i++) {
      String fn = "-".equals(ops.get(i - 1)) ? "decimals.sub" : "decimals.add";
      acc = CelNode.call(fn, acc, emitMulAsDecimal(muls.get(i)));
    }
    return acc;
  }

  /** Fold a multiplicative chain left-associatively into decimals.mul/div/mod calls. */
  private static CelNode emitMulAsDecimal(LogicalTypesParser.Check_expr_mulContext mul) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = mul.check_expr_unary_sign();
    return emitMulRangeAsDecimal(mul, signs, operators(mul), signs.size());
  }

  /**
   * Fold the first {@code count} operands of a multiplicative chain into decimals.mul/div/mod
   * calls. Leading integer operands are combined natively (so `/` truncates) and widened once.
   */
  private static CelNode emitMulRangeAsDecimal(
      LogicalTypesParser.Check_expr_mulContext mul,
      List<LogicalTypesParser.Check_expr_unary_signContext> signs, List<String> ops,
      int count) {
    int run = Math.min(narrowerRun(signs, "decimal"), count);
    int next = 1;
    CelNode acc;
    if (run >= 2) {
      acc = CelNode.call("decimal", emitNativeRun(signs, ops, run));
      next = run;
    } else {
      acc = emitSignAsDecimal(signs.get(0));
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
      acc = CelNode.call(fn, acc, emitSignAsDecimal(signs.get(i)));
    }
    return acc;
  }

  private static CelNode emitSignAsDecimal(LogicalTypesParser.Check_expr_unary_signContext sign) {
    if (sign.getChildCount() > 1 && narrowerThan(numericCategory(sign), "decimal")) {
      // A negated integer is negated natively, then widened.
      return CelNode.call("decimal", visitUnarySign(sign));
    }
    // parity: `--x` cancels, matching the native path; '+' dropped
    CelNode value = emitCExprAsDecimal(sign.c_expr());
    return negated(sign) ? CelNode.call("decimals.neg", value) : value;
  }

  /** Emit a leaf as a Decimal value, coercing non-Decimal operands. */
  private static CelNode emitCExprAsDecimal(LogicalTypesParser.C_exprContext ctx) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (ctx instanceof LogicalTypesParser.CheckLiteralContext) {
      LogicalTypesParser.LiteralContext lit =
          ((LogicalTypesParser.CheckLiteralContext) ctx).literal();
      // Numeric literal → decimal("<exact original text>"): preserve the
      // written value exactly rather than routing through a double literal,
      // which would bake in binary-float imprecision.
      if (lit.intLiteral() != null) {
        return CelNode.call("decimal", CelNode.string(lit.intLiteral().getText()));
      }
      if (lit.decimalLiteral() != null) {
        return CelNode.call("decimal", CelNode.string(lit.decimalLiteral().getText()));
      }
      if (lit.doubleLiteral() != null) {
        return CelNode.call("decimal", CelNode.string(lit.doubleLiteral().getText()));
      }
      // Non-numeric literal in a decimal context — let decimal(dyn) reject it
      // at runtime / the checker reject it; emit the coercion form.
      return CelNode.call("decimal", visitLiteral(lit));
    }
    if (ctx instanceof LogicalTypesParser.CheckColumnRefContext) {
      LogicalTypesParser.ColumnrefContext cr =
          ((LogicalTypesParser.CheckColumnRefContext) ctx).columnref();
      CelNode column = visitColumnRef(cr);
      return ConstraintResolver.isDecimal(ConstraintResolver.resolveColumnRefType(cr, vctx))
          ? column : CelNode.call("decimal", column);
    }
    if (ctx instanceof LogicalTypesParser.CheckFuncContext) {
      LogicalTypesParser.Func_exprContext fe =
          ((LogicalTypesParser.CheckFuncContext) ctx).func_expr();
      CelNode func = ConstraintFunctions.visitFuncExpr(fe);
      return ConstraintResolver.isDecimal(ConstraintResolver.tryResolveFuncReturnType(fe, vctx))
          ? func : CelNode.call("decimal", func);
    }
    if (ctx instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) ctx;
      if (paren.indirection() == null
          && ConstraintResolver.isDecimal(
              ConstraintResolver.tryResolveCheckExprType(paren.check_expr(), vctx))) {
        // Decimal sub-expression: re-enter the decimal cascade.
        return emitCheckExprAsDecimal(paren.check_expr());
      }
      // Non-decimal numeric (or unresolved): coerce the native emit.
      return CelNode.call("decimal", visitCExpr(ctx));
    }
    if (ctx instanceof LogicalTypesParser.CheckCaseContext) {
      LogicalTypesParser.Case_exprContext ce =
          ((LogicalTypesParser.CheckCaseContext) ctx).case_expr();
      CelNode result = visitCase(ce);
      return ConstraintResolver.isDecimal(ConstraintResolver.tryResolveCaseExprType(ce, vctx))
          ? result : CelNode.call("decimal", result);
    }
    // Fallback: coerce whatever the native path emits.
    return CelNode.call("decimal", visitCExpr(ctx));
  }

  /**
   * Re-enter the decimal cascade from a {@code check_expr} known to resolve to
   * decimal (used for paren-wrapped decimal sub-expressions). The boolean
   * cascade levels are all single-child pass-throughs for a decimal value;
   * if any branches unexpectedly, fall back to coercing the native emit.
   */
  private static CelNode emitCheckExprAsDecimal(LogicalTypesParser.Check_exprContext ctx) {
    return emitCached(ctx, Form.DECIMAL, () -> {
      LogicalTypesParser.Check_expr_addContext add = bareAddOfOr(ctx.check_expr_or());
      return add != null ? emitAddAsDecimal(add) : CelNode.call("decimal", visitCheckExpr(ctx));
    });
  }

  /**
   * Emit a {@code check_expr_in} (a BETWEEN operand) as a Decimal value.
   */
  private static CelNode emitInAsDecimal(LogicalTypesParser.Check_expr_inContext in) {
    LogicalTypesParser.Check_expr_addContext add = bareAddOfIn(in);
    return add != null ? emitAddAsDecimal(add) : CelNode.call("decimal", visitIn(in));
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
  static CelNode emitNumericAs(LogicalTypesParser.Check_exprContext ctx, String common) {
    if ("decimal".equals(common)) {
      return emitDecimalValue(ctx);
    }
    if ("double".equals(common)) {
      return emitDoubleValueCheckExpr(ctx);
    }
    return visitCheckExpr(ctx);
  }

  private static CelNode emitNumericBetweenAs(
      LogicalTypesParser.Check_expr_betweenContext between, String common) {
    // A pure-numeric comparison operand is a bare value (no BETWEEN/IN/LIKE), so
    // it descends between → in → like → add.
    LogicalTypesParser.Check_expr_likeContext like = between.check_expr_in(0).check_expr_like();
    if ("decimal".equals(common)) {
      return emitAddAsDecimal(like.check_expr_add());
    }
    if ("double".equals(common)) {
      return emitDoubleValueAdd(like.check_expr_add());
    }
    return visitBetween(between);
  }

  private static CelNode emitNumericInAs(
      LogicalTypesParser.Check_expr_inContext in, String common) {
    LogicalTypesParser.Check_expr_addContext add = bareAddOfIn(in);
    if (add != null && "decimal".equals(common)) {
      return emitAddAsDecimal(add);
    }
    if (add != null && "double".equals(common)) {
      return emitDoubleValueAdd(add);
    }
    // not a bare value (defensive), or an int — native
    return visitIn(in);
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
    for (LogicalTypesParser.Check_exprContext item : inItems(ctx)) {
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
  private static CelNode emitDoubleValueLike(LogicalTypesParser.Check_expr_likeContext like) {
    return like.LIKE() == null ? emitDoubleValueAdd(like.check_expr_add())
        : CelNode.call("double", visitLike(like));
  }

  private static CelNode emitDoubleValueCheckExpr(LogicalTypesParser.Check_exprContext ctx) {
    return emitCached(ctx, Form.DOUBLE, () -> {
      LogicalTypesParser.Check_expr_addContext add = bareAddOfOr(ctx.check_expr_or());
      return add != null ? emitDoubleValueAdd(add) : CelNode.call("double", visitCheckExpr(ctx));
    });
  }

  // Leading integer or decimal operands are combined in their own type and converted once:
  // SQL types each operator by its own operands, so `a / 4` truncates and `0.10 + 0.20` is exact.
  private static CelNode emitDoubleValueAdd(LogicalTypesParser.Check_expr_addContext add) {
    List<LogicalTypesParser.Check_expr_mulContext> muls = add.check_expr_mul();
    List<String> ops = operators(add);
    int run = narrowerRun(muls, "double");
    int next = 1;
    CelNode acc;
    if (run >= 2) {
      acc = CelNode.call("double", "int".equals(runCategory(muls, run))
          ? emitNativeRun(muls, ops, run) : emitAddRangeAsDecimal(muls, ops, run));
      next = run;
    } else {
      acc = emitDoubleValueMul(muls.get(0));
    }
    for (int i = next; i < muls.size(); i++) {
      acc = CelNode.binary(acc, ops.get(i - 1), emitDoubleValueMul(muls.get(i)));
    }
    return acc;
  }

  private static CelNode emitDoubleValueMul(LogicalTypesParser.Check_expr_mulContext mul) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = mul.check_expr_unary_sign();
    List<String> ops = operators(mul);
    int run = narrowerRun(signs, "double");
    int next = 1;
    CelNode acc;
    if (run >= 2) {
      acc = CelNode.call("double", "int".equals(runCategory(signs, run))
          ? emitNativeRun(signs, ops, run) : emitMulRangeAsDecimal(mul, signs, ops, run));
      next = run;
    } else {
      acc = emitDoubleValueSign(signs.get(0));
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
      acc = CelNode.binary(acc, celMulOp(op), emitDoubleValueSign(signs.get(i)));
    }
    return acc;
  }

  private static CelNode emitDoubleValueSign(
      LogicalTypesParser.Check_expr_unary_signContext sign) {
    String category = numericCategory(sign);
    if (sign.getChildCount() > 1 && narrowerThan(category, "double")) {
      // A negated integer or decimal is negated in its own type, then converted.
      return CelNode.call("double", "int".equals(category)
          ? visitUnarySign(sign) : emitSignAsDecimal(sign));
    }
    return signed(sign, emitDoubleValueCExpr(sign.c_expr()));
  }

  private static CelNode emitDoubleValueCExpr(LogicalTypesParser.C_exprContext ctx) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (ctx instanceof LogicalTypesParser.CheckColumnRefContext) {
      LogicalTypesParser.ColumnrefContext cr =
          ((LogicalTypesParser.CheckColumnRefContext) ctx).columnref();
      CelNode column = visitColumnRef(cr);
      return isDoubleCategory(ConstraintResolver.resolveColumnRefType(cr, vctx))
          ? column : CelNode.call("double", column);
    }
    if (ctx instanceof LogicalTypesParser.CheckLiteralContext) {
      LogicalTypesParser.LiteralContext lit =
          ((LogicalTypesParser.CheckLiteralContext) ctx).literal();
      if (lit.intLiteral() != null) {
        // int literal → double literal (5 → 5.0).
        return CelNode.number(lit.intLiteral().getText() + ".0");
      }
      if (lit.decimalLiteral() != null) {
        return CelNode.number(
            ConstraintPatterns.normalizeNumericLiteral(lit.decimalLiteral().getText()));
      }
      if (lit.doubleLiteral() != null) {
        return CelNode.number(
            ConstraintPatterns.normalizeNumericLiteral(lit.doubleLiteral().getText()));
      }
      return CelNode.call("double", visitLiteral(lit));
    }
    if (ctx instanceof LogicalTypesParser.CheckParenContext
        && ((LogicalTypesParser.CheckParenContext) ctx).indirection() == null) {
      return emitDoubleValueCheckExpr(((LogicalTypesParser.CheckParenContext) ctx).check_expr());
    }
    if (ctx instanceof LogicalTypesParser.CheckFuncContext) {
      LogicalTypesParser.Func_exprContext fe =
          ((LogicalTypesParser.CheckFuncContext) ctx).func_expr();
      CelNode func = ConstraintFunctions.visitFuncExpr(fe);
      return isDoubleCategory(ConstraintResolver.tryResolveFuncReturnType(fe, vctx))
          ? func : CelNode.call("double", func);
    }
    if (ctx instanceof LogicalTypesParser.CheckCaseContext) {
      LogicalTypesParser.Case_exprContext ce =
          ((LogicalTypesParser.CheckCaseContext) ctx).case_expr();
      CelNode result = visitCase(ce);
      return isDoubleCategory(ConstraintResolver.tryResolveCaseExprType(ce, vctx))
          ? result : CelNode.call("double", result);
    }
    return CelNode.call("double", visitCExpr(ctx));
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

  // A multiplicative operator's CEL: SQL's `||` (string/bytes concat) is `+`.
  private static String celMulOp(String op) {
    return "||".equals(op) ? "+" : op;
  }

  /**
   * The first {@code count} operands of an additive or multiplicative chain, emitted natively.
   */
  private static CelNode emitNativeRun(
      List<? extends ParseTree> operands, List<String> ops, int count) {
    CelNode acc = null;
    for (int i = 0; i < count; i++) {
      ParseTree operand = operands.get(i);
      CelNode value = operand instanceof LogicalTypesParser.Check_expr_mulContext
          ? visitMul((LogicalTypesParser.Check_expr_mulContext) operand)
          : visitUnarySign((LogicalTypesParser.Check_expr_unary_signContext) operand);
      acc = acc == null ? value : CelNode.binary(acc, celMulOp(ops.get(i - 1)), value);
    }
    return acc;
  }

  private static boolean isDoubleCategory(Schema s) {
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
  private static CelNode visitLike(LogicalTypesParser.Check_expr_likeContext ctx) {
    if (ctx.LIKE() == null) {
      return visitAdd(ctx.check_expr_add());
    }
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
    CelNode matches = CelNode.method(visitAdd(ctx.check_expr_add()), "matches",
        CelNode.literal("'" + ConstraintPatterns.escapeForCelStringLiteral(regex) + "'"));
    return ctx.NOT() != null ? CelNode.not(matches) : matches;
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

  private static CelNode visitAdd(LogicalTypesParser.Check_expr_addContext ctx) {
    List<LogicalTypesParser.Check_expr_mulContext> muls = ctx.check_expr_mul();
    List<String> ops = operators(ctx);
    CelNode acc = visitMul(muls.get(0));
    for (int i = 1; i < muls.size(); i++) {
      acc = CelNode.binary(acc, ops.get(i - 1), visitMul(muls.get(i)));
    }
    return acc;
  }

  /**
   * Multiplicative level: {@code * / %} plus SQL {@code ||} (string/bytes
   * concat). {@code ||} maps to CEL {@code +}; the others pass through
   * unchanged.
   */
  private static CelNode visitMul(LogicalTypesParser.Check_expr_mulContext ctx) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = ctx.check_expr_unary_sign();
    List<String> ops = operators(ctx);
    CelNode acc = visitUnarySign(signs.get(0));
    for (int i = 1; i < signs.size(); i++) {
      acc = CelNode.binary(acc, celMulOp(ops.get(i - 1)), visitUnarySign(signs.get(i)));
    }
    return acc;
  }

  private static CelNode visitUnarySign(LogicalTypesParser.Check_expr_unary_signContext ctx) {
    return signed(ctx, visitCExpr(ctx.c_expr()));
  }

  // True if a unary sign's '-' tokens are odd in number; '+' is dropped, and CEL's parser drops
  // a pair of '-'.
  private static boolean negated(LogicalTypesParser.Check_expr_unary_signContext sign) {
    boolean negate = false;
    for (int i = 0; i < sign.getChildCount(); i++) {
      ParseTree child = sign.getChild(i);
      if (child instanceof TerminalNode && "-".equals(child.getText())) {
        negate = !negate;
      }
    }
    return negate;
  }

  private static CelNode signed(LogicalTypesParser.Check_expr_unary_signContext sign,
      CelNode value) {
    return negated(sign) ? CelNode.negate(value) : value;
  }

  static CelNode visitCExpr(LogicalTypesParser.C_exprContext ctx) {
    if (ctx instanceof LogicalTypesParser.CheckColumnRefContext) {
      return visitColumnRef(((LogicalTypesParser.CheckColumnRefContext) ctx).columnref());
    }
    if (ctx instanceof LogicalTypesParser.CheckLiteralContext) {
      return visitLiteral(((LogicalTypesParser.CheckLiteralContext) ctx).literal());
    }
    if (ctx instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) ctx;
      CelNode inner = visitCheckExpr(paren.check_expr());
      return paren.indirection() != null ? visitIndirection(paren.indirection(), inner) : inner;
    }
    if (ctx instanceof LogicalTypesParser.CheckCaseContext) {
      return visitCase(((LogicalTypesParser.CheckCaseContext) ctx).case_expr());
    }
    if (ctx instanceof LogicalTypesParser.CheckFuncContext) {
      return ConstraintFunctions.visitFuncExpr(
          ((LogicalTypesParser.CheckFuncContext) ctx).func_expr());
    }
    throw new ValidationException(
        "Unrecognized c_expr subtype: " + ctx.getClass().getSimpleName());
  }

  private static CelNode visitColumnRef(LogicalTypesParser.ColumnrefContext ctx) {
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
    CelNode root = emitColumnRoot(colidName(ctx.colid()));
    return ctx.indirection() != null ? visitIndirection(ctx.indirection(), root) : root;
  }

  private static CelNode emitColumnRoot(String name) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (vctx != null && vctx.isColumn(name)) {
      CelNode self = CelNode.ident("this");
      if (vctx.isColumnLevel()) {
        return self;
      }
      // A field name colliding with a CEL reserved word (e.g. `null`, `in`) takes index
      // syntax, `this["in"]`, as `this.in` would fail to parse.
      return Schema.isCelReservedName(name) ? CelNode.index(self, CelNode.string(name))
          : CelNode.select(self, name);
    }
    return CelNode.ident(name);
  }

  /**
   * The null (or not-null) test of a has()-compatible reference: each nullable step of its path
   * ({@link ConstraintResolver#nullableSteps}), outermost first, so a null struct decides it
   * before anything is selected from it; a constant for a path that is never null; or a test of
   * {@code emitted}, the whole reference, when the steps can't be told.
   */
  static CelNode nullTest(List<String> path, CelNode emitted, boolean isNull) {
    List<Integer> steps =
        path == null ? null : ConstraintResolver.nullableSteps(path, EMIT_VCTX.get());
    if (steps == null) {
      return stepTest(emitted, isNull);
    }
    if (steps.isEmpty()) {
      return CelNode.bool(!isNull);
    }
    List<CelNode> tests = new ArrayList<>();
    for (int step : steps) {
      tests.add(stepTest(step == path.size() ? emitted : prefix(path, step), isNull));
    }
    return isNull ? CelNode.or(tests) : CelNode.and(tests);
  }

  // The first steps of a selection path, emitted.
  private static CelNode prefix(List<String> path, int steps) {
    CelNode prefix = emitColumnRoot(path.get(0));
    for (int i = 1; i < steps; i++) {
      prefix = CelNode.select(prefix, path.get(i));
    }
    return prefix;
  }

  // One step's test: has() of a selection, or a null comparison of what has() can't take, as a
  // bare iteration variable.
  private static CelNode stepTest(CelNode selection, boolean isNull) {
    if (!(selection instanceof CelNode.Select)) {
      return isNull ? CelNode.isNull(selection) : CelNode.isNotNull(selection);
    }
    CelNode has = CelNode.call("has", selection);
    return isNull ? CelNode.or(CelNode.not(has), CelNode.isNull(selection))
        : CelNode.and(has, CelNode.isNotNull(selection));
  }

  private static CelNode visitIndirection(LogicalTypesParser.IndirectionContext ctx,
      CelNode operand) {
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
    CelNode selected = operand;
    for (LogicalTypesParser.Indirection_elContext el : ctx.indirection_el()) {
      if (el.colid() != null) {
        String fieldName = colidName(el);
        // CEL-reserved field name in nested access — escape via index
        // syntax (`["in"]` instead of `.in`) so existing wire schemas
        // with such names produce parseable CEL.
        selected = Schema.isCelReservedName(fieldName)
            ? CelNode.index(selected, CelNode.string(fieldName))
            : CelNode.select(selected, fieldName);
        receiver = stepStructField(receiver, fieldName);
      } else {
        selected = emitBracketIndex(el, receiver, selected);
        receiver = stepBracketElement(receiver);
      }
    }
    return selected;
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
   * conversion: 1-based SQL becomes 0-based CEL ({@code [expr - 1]}).
   */
  private static CelNode emitBracketIndex(
      LogicalTypesParser.Indirection_elContext el, Schema receiver, CelNode operand) {
    receiver = resolveRef(receiver);
    CelNode index = visitCheckExpr(el.check_expr());
    boolean isMap = receiver != null && receiver.getType() == Schema.Type.MAP;
    return CelNode.index(operand, isMap ? index : zeroBased(index));
  }

  // A 1-based SQL index as CEL's 0-based one.
  private static CelNode zeroBased(CelNode index) {
    return CelNode.binary(index, "-", CelNode.literal("1"));
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

  private static CelNode visitLiteral(LogicalTypesParser.LiteralContext ctx) {
    if (ctx.NULL() != null) {
      return CelNode.NULL;
    }
    if (ctx.boolLiteral() != null) {
      return CelNode.bool(ctx.boolLiteral().TRUE() != null);
    }
    // The grammar lets a literal carry its own sign, as in - -5.
    if (ctx.intLiteral() != null) {
      return CelNode.number(ctx.intLiteral().getText());
    }
    if (ctx.decimalLiteral() != null) {
      return CelNode.number(
          ConstraintPatterns.normalizeNumericLiteral(ctx.decimalLiteral().getText()));
    }
    if (ctx.doubleLiteral() != null) {
      return CelNode.number(
          ConstraintPatterns.normalizeNumericLiteral(ctx.doubleLiteral().getText()));
    }
    if (ctx.TIMESTAMP() != null) {
      // TIMESTAMP '2020-01-01 00:00:00' → timestamp("2020-01-01T00:00:00Z")
      // (CEL stdlib RFC-3339 constructor; zoneless literal assumed UTC).
      // Checked before stringLiteral: this literal also has a stringLiteral child.
      return CelNode.call("timestamp", CelNode.string(normalizeSqlTimestamp(ctx)));
    }
    if (ctx.INTERVAL() != null) {
      // INTERVAL '7' DAY → duration("604800s") (CEL stdlib duration; reject
      // calendar units YEAR/MONTH which have no fixed length). Also checked
      // before stringLiteral.
      return CelNode.call("duration", CelNode.string(intervalSeconds(ctx) + "s"));
    }
    if (ctx.stringLiteral() != null) {
      return CelNode.literal(
          ConstraintPatterns.translateStringLiteral(ctx.stringLiteral().getText()));
    }
    if (ctx.bytesLiteral() != null) {
      return CelNode.literal(ConstraintFunctions.translateBytesLiteral(
          ctx.bytesLiteral(), ctx.bytesLiteral().getText()));
    }
    throw new ValidationException("Unrecognized literal: " + ctx.getText());
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

  private static CelNode visitCase(LogicalTypesParser.Case_exprContext ctx) {
    return emitCached(ctx, Form.NATIVE, () -> emitCase(ctx));
  }

  private static CelNode emitCase(LogicalTypesParser.Case_exprContext ctx) {
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

    List<CelNode> conditions = new ArrayList<>();
    List<CelNode> results = new ArrayList<>();
    for (LogicalTypesParser.When_clauseContext when : whens) {
      // Searched form: WHEN's first expr is the condition directly.
      // Simple form: "caseArg == WHEN's first expr" is the condition.
      conditions.add(caseArg != null ? emitCaseMatch(caseArg, when.check_expr(0))
          : emitCondition(when.check_expr(0)));
      results.add(emitCaseResult(when.check_expr(1), resultCommon, resultTemporal));
    }
    // Postgres CASE without ELSE returns NULL on no match.
    CelNode cel = elseExpr != null ? emitCaseResult(elseExpr, resultCommon, resultTemporal)
        : CelNode.NULL;
    for (int i = whens.size() - 1; i >= 0; i--) {
      cel = CelNode.ternary(conditions.get(i), results.get(i), cel);
    }
    return cel;
  }

  // A simple CASE's implicit `caseArg = whenValue`, taken only when SQL's is TRUE.
  private static CelNode emitCaseMatch(LogicalTypesParser.Check_exprContext caseArg,
      LogicalTypesParser.Check_exprContext whenValue) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    // SQL's `=` matches no NULL, so a subject or WHEN value that may be NULL is tested first.
    CelNode eitherNull = ConstraintNullTest.either(
        ConstraintNullTest.told(ConstraintNullTest.of(caseArg), caseArg),
        ConstraintNullTest.told(ConstraintNullTest.of(whenValue), whenValue));
    // The comparison coerces to the common numeric type of the two operands.
    String common = (vctx != null) ? numericCommonOf(caseArg, whenValue, vctx) : null;
    CelNode equal;
    if ("decimal".equals(common)) {
      // Opaque Decimal has no native `==`.
      equal = CelNode.call("decimals.eq", emitDecimalValue(caseArg), emitDecimalValue(whenValue));
    } else if ("double".equals(common)) {
      equal = CelNode.binary(emitNumericAs(caseArg, "double"), "==",
          emitNumericAs(whenValue, "double"));
    } else if (vctx != null
        && (ConstraintResolver.subtreeHasTemporal(caseArg, vctx)
            || ConstraintResolver.subtreeHasTemporal(whenValue, vctx))) {
      // Native `==`, operands timestamp-normalized.
      equal = CelNode.binary(emitTimestampValueCheckExpr(caseArg), "==",
          emitTimestampValueCheckExpr(whenValue));
    } else {
      equal = CelNode.binary(visitCheckExpr(caseArg), "==", visitCheckExpr(whenValue));
    }
    return eitherNull == null || ConstraintNullTest.NEVER.equals(eitherNull) ? equal
        : CelNode.ternary(eitherNull, CelNode.FALSE, equal);
  }

  /**
   * A searched CASE's condition, taken only when SQL's is TRUE, not FALSE or NULL. One with
   * nothing that may be NULL is emitted as before.
   */
  private static CelNode emitCondition(LogicalTypesParser.Check_exprContext cond) {
    Truth whenTrue = emitTruth(cond.check_expr_or(), true);
    return (whenTrue.flags & MAY_BE_NULL) != 0 ? whenTrue.cel : visitCheckExpr(cond);
  }

  /**
   * A CHECK's CEL, failing only when SQL's result is FALSE, so a NULL one passes, as in SQL.
   * One with nothing that may be NULL is emitted as is.
   */
  static CelNode emitCheck(LogicalTypesParser.Check_exprContext ctx) {
    Truth notFalse = emitTruth(ctx.check_expr_or(), true, true);
    return (notFalse.flags & MAY_BE_NULL) != 0 ? notFalse.cel : visitCheckExpr(ctx);
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
  static Truth emitTruth(LogicalTypesParser.Check_expr_orContext or, boolean truth) {
    return emitTruth(or, truth, false);
  }

  /**
   * {@link #emitTruth}, or with {@code nullAs} its negation the other way round: true unless
   * SQL's is FALSE (with {@code truth}), or TRUE, so a predicate that may be NULL is true then.
   * A CHECK is "not FALSE", which reads as its NULL test or its predicate.
   */
  static Truth emitTruth(LogicalTypesParser.Check_expr_orContext or, boolean truth,
      boolean nullAs) {
    List<LogicalTypesParser.Check_expr_andContext> ands = or.check_expr_and();
    if (ands.size() == 1) {
      return emitTruth(ands.get(0), truth, nullAs);
    }
    return emitJunction(or, ands.size(), (i, t, n) -> emitTruth(ands.get(i), t, n),
        !truth, truth, nullAs);
  }

  private static Truth emitTruth(LogicalTypesParser.Check_expr_andContext and, boolean truth,
      boolean nullAs) {
    List<LogicalTypesParser.Check_expr_unary_notContext> nots = and.check_expr_unary_not();
    if (nots.size() == 1) {
      return emitTruth(nots.get(0), truth, nullAs);
    }
    return emitJunction(and, nots.size(), (i, t, n) -> emitTruth(nots.get(i), t, n),
        truth, truth, nullAs);
  }

  private static Truth emitTruth(LogicalTypesParser.Check_expr_unary_notContext not,
      boolean truth, boolean nullAs) {
    if (not instanceof LogicalTypesParser.CheckExprNotContext) {
      return emitTruth(((LogicalTypesParser.CheckExprNotContext) not).check_expr_unary_not(),
          !truth, nullAs);
    }
    LogicalTypesParser.Check_expr_isnullContext is =
        ((LogicalTypesParser.CheckExprNotPassContext) not).check_expr_isnull();
    if (is.IS() == null) {
      LogicalTypesParser.C_exprContext c = ConstraintResolver.leafPrimary(is.check_expr_compare());
      if (c instanceof LogicalTypesParser.CheckParenContext
          && ((LogicalTypesParser.CheckParenContext) c).indirection() == null) {
        // Cached and size-checked, as nested AND and OR may repeat their operands.
        return cachedTruth(((LogicalTypesParser.CheckParenContext) c).check_expr()
            .check_expr_or(), truth, nullAs);
      }
    }
    if (is.IS() == null && is.check_expr_compare().check_expr_between().size() == 1
        && is.check_expr_compare().check_expr_between(0).BETWEEN() == null
        && givesNullAsNull(is.check_expr_compare().check_expr_between(0).check_expr_in(0))) {
      // Its CEL gives NULL as null, which equals neither true nor false.
      return new Truth(CelNode.binary(CelNode.dyn(visitIs(is)), nullAs ? "!=" : "==",
          CelNode.bool(nullAs != truth)), MAY_BE_NULL);
    }
    if (is.IS() == null && is.check_expr_compare().check_expr_between().size() == 1
        && is.check_expr_compare().check_expr_between(0).BETWEEN() != null) {
      return emitBetweenTruth(is.check_expr_compare().check_expr_between(0), truth, nullAs);
    }
    LogicalTypesParser.C_exprContext leaf =
        is.IS() == null ? ConstraintResolver.leafPrimary(is.check_expr_compare()) : null;
    if (leaf instanceof LogicalTypesParser.CheckFuncContext
        && ConstraintNullTest.isQuantifier((LogicalTypesParser.CheckFuncContext) leaf)) {
      return emitQuantifierTruth((LogicalTypesParser.CheckFuncContext) leaf, truth, nullAs);
    }
    if (is.IS() != null && !truth
        && ConstraintResolver.isHasCompatible(is.check_expr_compare(), EMIT_VCTX.get())) {
      // NOT of a selection's IS [NOT] NULL is the opposite test, as the generator writes it.
      return new Truth(nullTest(ConstraintResolver.selectionPathCompare(is.check_expr_compare()),
          visitCompare(is.check_expr_compare()), is.NOT() != null), 0);
    }
    CelNode isNull = is.IS() != null ? null : ConstraintNullTest.of(is.check_expr_compare());
    boolean guarded = isNull != null && !ConstraintNullTest.NEVER.equals(isNull);
    int flags = guarded ? MAY_BE_NULL : is.IS() == null && isNull == null ? UNTOLD : 0;
    CelNode predicate = truth ? visitIs(is) : CelNode.not(visitIs(is));
    if (!guarded) {
      return new Truth(predicate, flags);
    }
    if (nullAs) {
      // Its NULL test first, so a runtime that only short-circuits never reads a NULL. One that
      // may raise, as a predicate that may raise has, is a ternary's condition: || would let a
      // true predicate, as CEL gives null == null, swallow its error.
      return new Truth(mayRaise(is) ? CelNode.ternary(isNull, CelNode.TRUE, predicate)
          : CelNode.or(isNull, predicate), flags);
    }
    return new Truth(CelNode.ternary(isNull, CelNode.FALSE, predicate), flags);
  }

  // True if evaluating node can raise an error SQL raises too: a division, CAST, PARSE_JSON or
  // VARIANT_GET, TRY_VARIANT_GET with a path that isn't a literal, so isn't checked beforehand,
  // or ROUND or TRUNCATE with a scale that isn't a literal at least 0, as Flink raises for a
  // very negative one. Flink gives FALSE for a bad regex.
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
    String name = app.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    if ("PARSE_JSON".equals(name)) {
      return true;
    }
    List<LogicalTypesParser.Check_exprContext> args = app.check_expr_list() == null
        ? java.util.Collections.emptyList() : app.check_expr_list().check_expr();
    if (("ROUND".equals(name) || "TRUNCATE".equals(name)) && args.size() == 2) {
      Long scale = ConstraintResolver.tryLiteralIntValue(args.get(1));
      return scale == null || scale < 0;
    }
    return false;
  }

  // emitTruth of operand i in a truth.
  private interface OperandTruth {
    Truth emit(int i, boolean truth, boolean nullAs);
  }

  /**
   * The operands' truths joined by && when {@code all}, else ||. A NULL operand is false there
   * (true with {@code nullAs}), which && (||) lets swallow another's error, so where one may
   * raise an error they're joined only once no operand decides it the other way; with
   * {@code nullAs}, as the negation of the other truth, which does that.
   */
  private static Truth emitJunction(ParseTree node, int count, OperandTruth operand,
      boolean all, boolean truth, boolean nullAs) {
    List<CelNode> operands = new ArrayList<>();
    int flags = 0;
    for (int i = 0; i < count; i++) {
      Truth op = operand.emit(i, truth, nullAs);
      flags |= op.flags;
      operands.add(op.cel);
    }
    CelNode joined = all ? CelNode.and(operands) : CelNode.or(operands);
    if (all == nullAs || (flags & MAY_BE_NULL) == 0 || !mayRaise(node)) {
      return new Truth(joined, flags);
    }
    if (nullAs) {
      Truth other = emitJunction(node, count, operand, !all, !truth, false);
      return new Truth(CelNode.not(other.cel), other.flags);
    }
    List<CelNode> decides = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      decides.add(operand.emit(i, !truth, false).cel);
    }
    return new Truth(CelNode.ternary(CelNode.or(decides), CelNode.FALSE, joined), flags);
  }

  /**
   * {@link #emitTruth} of EVERY, ANY or ONE: EVERY is TRUE when its body is TRUE for every element
   * and FALSE when it is FALSE for any, ANY the other way round, ONE TRUE for exactly one, and
   * all are NULL over a NULL collection. One with nothing that may be NULL is emitted as before.
   */
  static Truth emitQuantifierTruth(LogicalTypesParser.CheckFuncContext func, boolean truth) {
    return emitQuantifierTruth(func, truth, false);
  }

  private static Truth emitQuantifierTruth(LogicalTypesParser.CheckFuncContext func,
      boolean truth, boolean nullAs) {
    Truth fold = quantifierFold(func.func_expr().func_application(), truth, nullAs);
    if ((fold.flags & NATIVE) == 0) {
      return fold;
    }
    CelNode asIs = visitCExpr(func);
    return new Truth(truth ? asIs : CelNode.not(asIs), fold.flags & ~NATIVE);
  }

  // quantifierFold's flag for one emitted as is: nothing may be NULL, or its NULL can't be told.
  private static final int NATIVE = 4;

  /**
   * ONE's own CEL: TRUE, FALSE or null from its three-valued forms, as cel-java's exists_one,
   * unlike all and exists, doesn't absorb a NULL body's error once the answer is decided; as is
   * when nothing may be NULL.
   */
  static CelNode emitOne(LogicalTypesParser.Func_applicationContext app, Supplier<CelNode> asIs) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    boolean folds = vctx != null && !vctx.isTypeOnly() && app.check_expr_list() != null
        && app.check_expr_list().check_expr().size() == 3;
    Truth whenTrue = folds ? quantifierFold(app, true, false) : null;
    if (whenTrue == null || (whenTrue.flags & NATIVE) != 0) {
      return asIs.get();
    }
    Truth whenFalse = quantifierFold(app, false, false);
    return CelNode.ternary(whenTrue.cel, CelNode.TRUE,
        CelNode.ternary(whenFalse.cel, CelNode.FALSE, CelNode.dyn(CelNode.NULL)));
  }

  // The three-valued fold of EVERY, ANY or ONE in a truth, or NATIVE with nothing emitted.
  private static Truth quantifierFold(LogicalTypesParser.Func_applicationContext app,
      boolean truth, boolean nullAs) {
    List<LogicalTypesParser.Check_exprContext> args = app.check_expr_list().check_expr();
    String name = app.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    ConstraintValidationContext outer = EMIT_VCTX.get();
    CelNode collectionNull = ConstraintNullTest.of(args.get(0));
    if (outer == null || collectionNull == null) {
      return new Truth(null, NATIVE | UNTOLD);
    }
    String iterVar = ConstraintFunctions.extractIterationVar(args.get(1), name);
    ConstraintValidationContext body = bodyContext(app, outer, () -> outer.withBinding(iterVar,
        ConstraintResolver.resolveCollectionElementType(args.get(0), outer)));
    // ONE takes both of its body's truths.
    boolean one = "ONE".equals(name);
    // exists() with nullAs lets a NULL body, true there, swallow another's error.
    boolean every = "EVERY".equals(name) == truth;
    if (nullAs && (one || !every && mayRaise(args.get(2)))) {
      Truth negated = quantifierFold(app, !truth, false);
      return (negated.flags & NATIVE) != 0 ? negated
          : new Truth(CelNode.not(negated.cel), negated.flags);
    }
    Truth bodyTruth;
    Truth bodyFalse = null;
    EMIT_VCTX.set(body);
    try {
      bodyTruth = cachedTruth(args.get(2).check_expr_or(), truth || one, nullAs);
      if (one) {
        bodyFalse = cachedTruth(args.get(2).check_expr_or(), false, false);
      }
    } finally {
      EMIT_VCTX.set(outer);
    }
    int flags = bodyTruth.flags | (bodyFalse == null ? 0 : bodyFalse.flags);
    if (flags == 0 && ConstraintNullTest.NEVER.equals(collectionNull)) {
      return new Truth(null, NATIVE);
    }
    CelNode coll = visitCheckExpr(args.get(0));
    CelNode var = CelNode.ident(iterVar);
    boolean bodyMayBeNull = (flags & MAY_BE_NULL) != 0;
    CelNode fold;
    if (one) {
      // TRUE when exactly one body is TRUE and every other FALSE; FALSE when two or more are
      // TRUE, or all are FALSE.
      // A ternary, as && would let a NULL body, false there, swallow exists_one's error.
      CelNode exactlyOne = CelNode.method(coll, "exists_one", var, bodyTruth.cel);
      fold = truth
          ? CelNode.ternary(exactlyOne,
              CelNode.method(coll, "all", var, CelNode.or(bodyTruth.cel, bodyFalse.cel)),
              CelNode.FALSE)
          : CelNode.or(
              CelNode.and(CelNode.method(coll, "exists", var, bodyTruth.cel),
                  CelNode.not(exactlyOne)),
              CelNode.method(coll, "all", var, bodyFalse.cel));
    } else if (every && !nullAs && bodyMayBeNull && mayRaise(args.get(2))) {
      // all() lets a NULL element, false there, swallow another's error, so it's taken only
      // once no element decides it the other way.
      Truth decides;
      EMIT_VCTX.set(body);
      try {
        decides = cachedTruth(args.get(2).check_expr_or(), !truth, false);
      } finally {
        EMIT_VCTX.set(outer);
      }
      fold = CelNode.ternary(CelNode.method(coll, "exists", var, decides.cel), CelNode.FALSE,
          CelNode.method(coll, "all", var, bodyTruth.cel));
    } else {
      fold = CelNode.method(coll, every ? "all" : "exists", var, bodyTruth.cel);
    }
    boolean collectionMayBeNull = !ConstraintNullTest.NEVER.equals(collectionNull);
    if (nullAs && collectionMayBeNull && mayRaise(args.get(0))) {
      // A ternary, as || would let a true fold swallow the collection test's error.
      return new Truth(CelNode.ternary(collectionNull, CelNode.TRUE, fold),
          flags | MAY_BE_NULL);
    }
    if (nullAs && collectionMayBeNull) {
      return new Truth(CelNode.or(collectionNull, fold), flags | MAY_BE_NULL);
    }
    return new Truth(falseIfNull(collectionNull, fold),
        flags | (collectionMayBeNull ? MAY_BE_NULL : 0));
  }

  // A parenthesized or quantifier body's emitTruth, once per translation, context and truth.
  // Nesting may repeat it, so one over CEL's size limit is refused at once.
  private static Truth cachedTruth(LogicalTypesParser.Check_expr_orContext body, boolean truth,
      boolean nullAs) {
    EmitCache cache = EMIT_CACHE.get();
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (cache == null || vctx == null) {
      return emitTruth(body, truth, nullAs);
    }
    Truth[] held = cache.truths.computeIfAbsent(vctx, k -> new IdentityHashMap<>())
        .computeIfAbsent(body, k -> new Truth[4]);
    int at = (truth ? 0 : 1) + (nullAs ? 2 : 0);
    if (held[at] == null) {
      held[at] = emitTruth(body, truth, nullAs);
      ConstraintCelChecker.checkSize(held[at].cel, true, cache.sourceSql);
    }
    return held[at];
  }

  /**
   * The NULL test of a selection with a subscript, NULL as SQL gives it: when a step, an index
   * or the element is NULL, or a key or index is missing; null when a step's type doesn't
   * resolve.
   */
  static CelNode subscriptNullTest(LogicalTypesParser.C_exprContext c) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    if (vctx == null) {
      return null;
    }
    List<CelNode> tests = new ArrayList<>();
    CelNode prefix;
    // A parenthesized root, which typedCollection takes as a call's result.
    boolean bracketed;
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
      prefix = emitColumnRoot(root);
      bracketed = false;
      // A column-level CHECK is skipped when its column is null.
      if (rootSchema.isNullable() && !(vctx.isColumnLevel() && vctx.isColumn(root))) {
        tests.add(stepTest(prefix, true));
      }
      type = resolveRef(rootSchema);
      indirection = col.indirection();
    } else {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) c;
      tests.add(ConstraintNullTest.of(paren.check_expr()));
      prefix = visitCheckExpr(paren.check_expr());
      bracketed = true;
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
        prefix = selected ? CelNode.select(prefix, fieldName)
            : CelNode.index(prefix, CelNode.string(fieldName));
        next = f.getSchema();
      } else {
        tests.add(ConstraintNullTest.of(el.check_expr()));
        CelNode index = visitCheckExpr(el.check_expr());
        CelNode collection = typedCollection(prefix, bracketed);
        if (type.getType() == Schema.Type.MAP) {
          tests.add(CelNode.not(CelNode.binary(index, "in", collection)));
          prefix = CelNode.index(prefix, index);
          next = type.getValueType();
        } else if (type.getType() == Schema.Type.ARRAY) {
          CelNode size = CelNode.call("size", collection);
          tests.add(index instanceof CelNode.Literal && ((CelNode.Literal) index).isNatural()
              ? CelNode.binary(size, "<", index)
              : CelNode.or(CelNode.binary(index, "<", CelNode.literal("1")),
                  CelNode.binary(index, ">", size)));
          prefix = CelNode.index(prefix, zeroBased(index));
          next = type.getElementType();
        } else {
          return null;
        }
      }
      bracketed = false;
      if (next.isNullable()) {
        tests.add(selected ? stepTest(prefix, true) : CelNode.isNull(prefix));
      }
      type = resolveRef(next);
    }
    return ConstraintNullTest.any(tests);
  }

  // A collection for size() or in: cel-java types a subscript of dyn as the first overload's
  // argument (size(m['k']) as a string's), unless it's dyn() itself. A subscript, a call's
  // result, a ternary's or a bracketed one is taken as one that may be.
  private static CelNode typedCollection(CelNode collection, boolean bracketed) {
    return bracketed || collection instanceof CelNode.Index || collection instanceof CelNode.Call
        || collection instanceof CelNode.Ternary ? CelNode.dyn(collection) : collection;
  }

  // An operand of size() or in, given as typedCollection gives one; the type-only pass keeps its
  // type for the strict check.
  static CelNode typedSubscript(CelNode cel) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    return vctx != null && vctx.isTypeOnly() ? cel : typedCollection(cel, false);
  }

  // An IN whose three-valued CEL gives NULL as null.
  static boolean givesNullAsNull(LogicalTypesParser.Check_expr_inContext in) {
    return inElementMayBeNull(in) || inArrayElementMayBeNull(in);
  }

  /**
   * {@link #emitTruth} of a BETWEEN, SQL's {@code lo <= a AND a <= hi}: TRUE when both hold,
   * FALSE when either fails, whatever the other is. SYMMETRIC is NULL when any operand is.
   */
  static Truth emitBetweenTruth(LogicalTypesParser.Check_expr_betweenContext between,
      boolean truth) {
    return emitBetweenTruth(between, truth, false);
  }

  /**
   * {@link #emitBetweenTruth}, or with {@code nullAs} its negation the other way round. With
   * bounds that are never NULL, both comparisons share the operand's NULL test, taken once.
   */
  private static Truth emitBetweenTruth(LogicalTypesParser.Check_expr_betweenContext between,
      boolean truth, boolean nullAs) {
    List<LogicalTypesParser.Check_expr_inContext> ins = between.check_expr_in();
    CelNode a = ConstraintNullTest.of(ins.get(0));
    CelNode lo = ConstraintNullTest.of(ins.get(1));
    CelNode hi = ConstraintNullTest.of(ins.get(2));
    CelNode anyNull = a == null || lo == null || hi == null ? null
        : ConstraintNullTest.any(Arrays.asList(a, lo, hi));
    if (anyNull == null || ConstraintNullTest.NEVER.equals(anyNull)) {
      CelNode asIs = visitBetween(between);
      return new Truth(truth ? asIs : CelNode.not(asIs), anyNull == null ? UNTOLD : 0);
    }
    CelNode lower = emitBetweenLe(ins, 1, 0);
    CelNode upper = emitBetweenLe(ins, 0, 2);
    boolean holds = truth != (between.NOT() != null);
    if (nullAs && (between.SYMMETRIC() != null || mayRaise(between))) {
      // SYMMETRIC, and one that may raise, where || would let a NULL comparison or a true one
      // swallow an error, as the negation of the other truth.
      Truth other = emitBetweenTruth(between, !truth, false);
      return new Truth(CelNode.not(other.cel), other.flags);
    }
    if (nullAs) {
      if (ConstraintNullTest.NEVER.equals(lo) && ConstraintNullTest.NEVER.equals(hi)) {
        CelNode both = CelNode.and(lower, upper);
        return new Truth(CelNode.or(a, holds ? both : CelNode.not(both)), MAY_BE_NULL);
      }
      CelNode l = trueIfNull(ConstraintNullTest.either(lo, a), holds ? lower : CelNode.not(lower));
      CelNode u = trueIfNull(ConstraintNullTest.either(a, hi), holds ? upper : CelNode.not(upper));
      return new Truth(holds ? CelNode.and(l, u) : CelNode.or(l, u), MAY_BE_NULL);
    }
    CelNode cel;
    if (between.SYMMETRIC() != null) {
      CelNode ordered = CelNode.ternary(emitBetweenLe(ins, 1, 2), CelNode.and(lower, upper),
          CelNode.and(emitBetweenLe(ins, 2, 0), emitBetweenLe(ins, 0, 1)));
      cel = CelNode.ternary(anyNull, CelNode.FALSE, holds ? ordered : CelNode.not(ordered));
    } else if (holds && !mayRaise(between)) {
      cel = CelNode.ternary(anyNull, CelNode.FALSE, CelNode.and(lower, upper));
    } else {
      CelNode fails = CelNode.or(
          falseIfNull(ConstraintNullTest.either(lo, a), CelNode.not(lower)),
          falseIfNull(ConstraintNullTest.either(a, hi), CelNode.not(upper)));
      // TRUE when both comparisons are, taken only once neither fails, so a NULL operand
      // doesn't hide another's error.
      cel = !holds ? fails : CelNode.ternary(fails, CelNode.FALSE, CelNode.and(
          falseIfNull(ConstraintNullTest.either(lo, a), lower),
          falseIfNull(ConstraintNullTest.either(a, hi), upper)));
    }
    return new Truth(cel, MAY_BE_NULL);
  }

  // p, or true when isNull, which is tested first.
  private static CelNode trueIfNull(CelNode isNull, CelNode p) {
    return ConstraintNullTest.NEVER.equals(isNull) ? p : CelNode.or(isNull, p);
  }

  // p, or false when isNull.
  private static CelNode falseIfNull(CelNode isNull, CelNode p) {
    return ConstraintNullTest.NEVER.equals(isNull) ? p
        : CelNode.ternary(isNull, CelNode.FALSE, p);
  }

  // ins[x] <= ins[y] for BETWEEN operands, compared in their common type.
  private static CelNode emitBetweenLe(
      List<LogicalTypesParser.Check_expr_inContext> ins, int x, int y) {
    ConstraintValidationContext vctx = EMIT_VCTX.get();
    String common = (vctx != null) ? numericCommonOf(ins, vctx) : null;
    if ("decimal".equals(common)) {
      return emitDecimalLe(ins.get(x), ins.get(y));
    }
    if ("double".equals(common)) {
      return emitDoubleLe(ins.get(x), ins.get(y));
    }
    if (vctx != null
        && (ConstraintResolver.subtreeHasTemporal(ins.get(0), vctx)
            || ConstraintResolver.subtreeHasTemporal(ins.get(1), vctx)
            || ConstraintResolver.subtreeHasTemporal(ins.get(2), vctx))) {
      return emitTimestampLe(ins.get(x), ins.get(y));
    }
    return CelNode.binary(visitIn(ins.get(x)), "<=", visitIn(ins.get(y)));
  }

  /**
   * Emit a CASE THEN/ELSE result coerced to the branches' common type, so the
   * ternary's branches type-unify. Mirrors {@code emitCoalesce}'s per-branch
   * coercion: DECIMAL common → Decimal value, DOUBLE common → double-cast,
   * temporal → CEL-timestamp normalization, otherwise native.
   */
  private static CelNode emitCaseResult(
      LogicalTypesParser.Check_exprContext branch, String common, boolean temporal) {
    CelNode nativeEmit = visitCheckExpr(branch);
    CelNode value;
    if ("decimal".equals(common)) {
      value = emitDecimalValue(branch);
    } else if ("double".equals(common)) {
      value = emitNumericAs(branch, "double");
    } else if (temporal) {
      value = emitTimestampValueCheckExpr(branch);
    } else {
      value = nativeEmit;
    }
    // A NULL result is NULL, as a COALESCE argument is, not converted or read.
    return ConstraintResolver.tryAsLiteral(branch) != null ? value
        : ConstraintFunctions.emitNullOrValue(branch, nativeEmit, value);
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
