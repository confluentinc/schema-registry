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

import java.util.List;

/**
 * Cascade visitors for the CHECK→CEL translator. Each visitor walks one
 * level of the postgres-derived CHECK grammar and emits the equivalent CEL
 * fragment into the caller's {@link StringBuilder}. The cascade reflects
 * SQL operator precedence — top to bottom: OR, AND, NOT, BETWEEN, IN,
 * IS NULL, comparison, LIKE, concat, additive, multiplicative, unary sign,
 * c_expr (column ref / literal / paren / case / func).
 *
 * <p>Visitors run after the validator suite has accepted the parse tree,
 * so they can assume well-formed input — no {@code null} guards, no
 * shape-checking past what's needed for the emit decision (e.g. wrap vs.
 * pass-through). Cross-cascade jumps (e.g. {@code visitConcat} called
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

  static void visitCheckExpr(
      LogicalTypesParser.Check_exprContext ctx, StringBuilder sb) {
    visitOr(ctx.check_expr_or(), sb);
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
   * </ul>
   */
  private static void visitIn(
      LogicalTypesParser.Check_expr_inContext ctx, StringBuilder sb) {
    if (ctx.IN() == null) {
      visitIs(ctx.check_expr_isnull(), sb);
      return;
    }
    boolean negated = ctx.NOT() != null;
    if (negated) {
      sb.append("!(");
    }
    visitIs(ctx.check_expr_isnull(), sb);
    sb.append(" in ");
    visitInTarget(ctx.in_target(), sb);
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
      visitIs(
          ((LogicalTypesParser.InTargetExprContext) ctx).check_expr_isnull(), sb);
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
      visitBetween(
          ((LogicalTypesParser.CheckExprNotPassContext) ctx).check_expr_between(), sb);
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
      StringBuilder lhs = new StringBuilder();
      visitCompare(ctx.check_expr_compare(), lhs);
      if (negated) {
        sb.append("(has(").append(lhs).append(") && dyn(")
            .append(lhs).append(") != null)");
      } else {
        sb.append("(!has(").append(lhs).append(") || dyn(")
            .append(lhs).append(") == null)");
      }
      return;
    }
    sb.append("dyn(");
    visitCompare(ctx.check_expr_compare(), sb);
    sb.append(negated ? ") != null" : ") == null");
  }

  private static void visitCompare(
      LogicalTypesParser.Check_expr_compareContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_likeContext> likes = ctx.check_expr_like();
    visitLike(likes.get(0), sb);
    if (likes.size() == 2) {
      // The comparison operator is the second child of the rule (between the
      // two check_expr_like). Find it as the only TerminalNode child.
      String op = findComparisonOp(ctx);
      sb.append(' ').append(translateCompareOp(op)).append(' ');
      visitLike(likes.get(1), sb);
    }
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
      visitConcat(ctx.check_expr_concat(), sb);
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
    boolean wrap = !ConstraintResolver.isSimplePrimaryConcat(ctx.check_expr_concat());
    if (wrap) {
      sb.append('(');
    }
    visitConcat(ctx.check_expr_concat(), sb);
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

  /**
   * Concat level (between LIKE and additive). SQL {@code ||} maps to CEL
   * {@code +} — string and bytes concat. {@code ||} sits at its own
   * precedence level so it binds looser than {@code +}/{@code -} (matching
   * PG and antlr/grammars-v4); see grammar comment on
   * {@code check_expr_concat}.
   */
  static void visitConcat(
      LogicalTypesParser.Check_expr_concatContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_addContext> adds = ctx.check_expr_add();
    visitAdd(adds.get(0), sb);
    for (int i = 1; i < adds.size(); i++) {
      sb.append(" + ");
      visitAdd(adds.get(i), sb);
    }
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

  private static void visitMul(
      LogicalTypesParser.Check_expr_mulContext ctx, StringBuilder sb) {
    List<LogicalTypesParser.Check_expr_unary_signContext> signs = ctx.check_expr_unary_sign();
    visitUnarySign(signs.get(0), sb);
    int signIdx = 1;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode) {
        sb.append(' ').append(child.getText()).append(' ');
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

  private static void visitCExpr(
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
    String name = colidName(ctx.colid());
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
    if (ctx.indirection() != null) {
      visitIndirection(ctx.indirection(), sb);
    }
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
    if (receiver == null || receiver.getType() != Schema.Type.STRUCT) {
      return null;
    }
    Schema.Field f = receiver.getField(fieldName);
    return f != null ? f.getSchema() : null;
  }

  private static Schema stepBracketElement(Schema receiver) {
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
    } else if (ctx.floatLiteral() != null) {
      sb.append(ConstraintPatterns.normalizeFloatLiteral(ctx.floatLiteral().getText()));
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

  private static void visitCase(
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
        emitWrappedReceiver(caseArg, sb);
        sb.append(" == ");
        emitWrappedReceiver(when.check_expr(0), sb);
      } else {
        emitWrappedReceiver(when.check_expr(0), sb);
      }
      sb.append(" ? ");
      visitCheckExpr(when.check_expr(1), sb);
      sb.append(" : ");
      if (i < whens.size() - 1) {
        sb.append("(");
      }
    }
    if (elseExpr != null) {
      visitCheckExpr(elseExpr, sb);
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
}
