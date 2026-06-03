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
import static io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator.locatedError;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-emit validation passes for the CHECK→CEL translator. Each pass walks
 * the parse tree once and throws a located {@link
 * io.confluent.kafka.schemaregistry.type.logical.ValidationException} when it
 * finds a construct that would emit unsound CEL or fail at CEL evaluation.
 *
 * <p>The full validator suite runs from {@link
 * ConstraintToCelTranslator#translate}, in this order:
 * <ol>
 *   <li>{@link #enforceMaxParseDepth} — guard recursive walkers from stack
 *       overflow on pathological inputs.</li>
 *   <li>{@link #validateBooleanRoot} — top-level expression must yield bool.</li>
 *   <li>{@link #validateColumnRefs} — every column ref resolves to a known
 *       binding, with indirection chains type-checked.</li>
 *   <li>Type-aware passes: comparisons, concat, casts, CASE branches, BETWEEN
 *       bounds, IN list/target, function args, macro bodies, modulo.</li>
 * </ol>
 *
 * <p>All passes are best-effort: when a node's type can't be resolved at our
 * coarse granularity (function results, complex expressions), they defer to
 * runtime rather than reject. The same conservative policy keeps
 * false-positive rates low across the validator suite.
 *
 * <p>{@link #recurseIntoMacro} threads the iteration variable's binding into
 * macro predicate-body recursion so type-aware passes catch issues inside
 * {@code EVERY/ANY/ONE} bodies that depend on the bound element type.
 */
final class ConstraintValidator {

  private ConstraintValidator() {
    // static utility
  }

  /**
   * Maximum parse-tree depth we'll attempt to validate / emit. The validators
   * and cascade walkers are recursive (one stack frame per parse-tree level),
   * so a deeply-nested expression can otherwise blow the JVM stack. The
   * cascade adds ~13 parse-tree levels per c_expr, so 1024 admits ~80 nested
   * parens (well beyond legitimate use) while still catching pathological
   * inputs (~800 paren stress tests reliably overflow without this guard).
   */
  private static final int MAX_PARSE_DEPTH = 1024;

  /**
   * Reject deeply-nested CHECK expressions before any recursive validator runs.
   * Walks the parse tree iteratively, tracking max depth, and throws when
   * {@link #MAX_PARSE_DEPTH} is exceeded — turning a {@link StackOverflowError}
   * into an actionable parse-time error.
   */
  static void enforceMaxParseDepth(
      LogicalTypesParser.Check_exprContext ctx) {
    java.util.ArrayDeque<int[]> stack = new java.util.ArrayDeque<>();
    stack.push(new int[]{0, 0});  // [childIndex, depth]
    org.antlr.v4.runtime.tree.ParseTree node = ctx;
    while (true) {
      int[] frame = stack.peek();
      if (frame[0] >= node.getChildCount()) {
        stack.pop();
        if (stack.isEmpty()) {
          return;
        }
        node = node.getParent();
        continue;
      }
      org.antlr.v4.runtime.tree.ParseTree child = node.getChild(frame[0]++);
      int childDepth = frame[1] + 1;
      if (childDepth > MAX_PARSE_DEPTH) {
        throw locatedError(ctx,
            "CHECK expression nesting depth exceeds " + MAX_PARSE_DEPTH
                + " — simplify or break the constraint into smaller pieces. "
                + "Deep nesting risks a stack overflow during validation.");
      }
      if (child.getChildCount() > 0) {
        stack.push(new int[]{0, childDepth});
        node = child;
      }
    }
  }

  // ---------------------------------------------------------------------
  // Boolean-root validation
  // ---------------------------------------------------------------------

  /**
   * Verify the top-level expression yields boolean. Walks down the cascade
   * skipping pass-through levels; rejects when the bottom is reached without
   * encountering a boolean-producing construct.
   */
  static void validateBooleanRoot(
      LogicalTypesParser.Check_exprContext ctx, ConstraintValidationContext vctx) {
    if (!isBooleanShaped(ctx, vctx)) {
      throw locatedError(ctx,
          "CHECK expression must yield a boolean, got: '" + ctx.getText() + "'. "
              + "Use a comparison, logical operator, IS NULL, IN, BETWEEN, LIKE, "
              + "or a boolean-returning function (CONTAINS, STARTS_WITH, IS_EMAIL, "
              + "EVERY, ANY, ONE, etc.).");
    }
  }

  /** True if the expression's top operator/result is boolean-shaped. */
  private static boolean isBooleanShaped(
      LogicalTypesParser.Check_exprContext ctx, ConstraintValidationContext vctx) {
    LogicalTypesParser.Check_expr_orContext or = ctx.check_expr_or();
    if (or.check_expr_and().size() > 1) {
      return true;  // OR
    }
    LogicalTypesParser.Check_expr_andContext and = or.check_expr_and(0);
    if (and.check_expr_unary_not().size() > 1) {
      return true;  // AND
    }
    LogicalTypesParser.Check_expr_unary_notContext unaryNot = and.check_expr_unary_not(0);
    if (unaryNot instanceof LogicalTypesParser.CheckExprNotContext) {
      return true;  // NOT
    }
    LogicalTypesParser.Check_expr_betweenContext between =
        ((LogicalTypesParser.CheckExprNotPassContext) unaryNot).check_expr_between();
    if (between.BETWEEN() != null) {
      return true;
    }
    LogicalTypesParser.Check_expr_inContext in = between.check_expr_in(0);
    if (in.IN() != null) {
      return true;
    }
    LogicalTypesParser.Check_expr_isnullContext is = in.check_expr_isnull();
    if (is.IS() != null) {
      return true;
    }
    LogicalTypesParser.Check_expr_compareContext compare = is.check_expr_compare();
    if (compare.check_expr_like().size() > 1) {
      return true;  // comparison
    }
    LogicalTypesParser.Check_expr_likeContext like = compare.check_expr_like(0);
    if (like.LIKE() != null) {
      return true;
    }
    // Below the LIKE level we're in non-boolean territory — concat, additive,
    // multiplicative, unary, c_expr. The exception: bool literal, boolean
    // column ref, or call to a boolean-returning function/macro.
    LogicalTypesParser.Check_expr_concatContext concat = like.check_expr_concat();
    if (concat.check_expr_add().size() > 1) {
      return false;  // ||
    }
    LogicalTypesParser.Check_expr_addContext add = concat.check_expr_add(0);
    if (add.check_expr_mul().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_mulContext mul = add.check_expr_mul(0);
    if (mul.check_expr_unary_sign().size() > 1) {
      return false;
    }
    LogicalTypesParser.Check_expr_unary_signContext unarySign = mul.check_expr_unary_sign(0);
    LogicalTypesParser.C_exprContext c = unarySign.c_expr();

    if (c instanceof LogicalTypesParser.CheckLiteralContext) {
      LogicalTypesParser.LiteralContext lit =
          ((LogicalTypesParser.CheckLiteralContext) c).literal();
      return lit.boolLiteral() != null;
    }
    if (c instanceof LogicalTypesParser.CheckColumnRefContext) {
      LogicalTypesParser.ColumnrefContext colref =
          ((LogicalTypesParser.CheckColumnRefContext) c).columnref();
      Schema resolved = ConstraintResolver.resolveColumnRefType(colref, vctx);
      return resolved != null && resolved.getType() == Schema.Type.BOOLEAN;
    }
    if (c instanceof LogicalTypesParser.CheckParenContext) {
      LogicalTypesParser.CheckParenContext paren = (LogicalTypesParser.CheckParenContext) c;
      return paren.indirection() == null && isBooleanShaped(paren.check_expr(), vctx);
    }
    if (c instanceof LogicalTypesParser.CheckCaseContext) {
      // CASE branches' types are not tracked in v1; assume the user knows
      // what they're doing and accept (defer to runtime CEL type-check).
      return true;
    }
    if (c instanceof LogicalTypesParser.CheckFuncContext) {
      return isBooleanFunc(((LogicalTypesParser.CheckFuncContext) c).func_expr());
    }
    return false;
  }

  /**
   * True if {@code CAST(x AS T)}'s target type is BOOLEAN.
   */
  private static boolean isCastToBoolean(LogicalTypesParser.FuncCastContext ctx) {
    String typeText = ctx.castType().primitiveType().getText().toUpperCase(java.util.Locale.ROOT);
    int paren = typeText.indexOf('(');
    String head = paren >= 0 ? typeText.substring(0, paren) : typeText;
    return "BOOLEAN".equals(head);
  }

  /**
   * True if a function call returns boolean. Conservative — known boolean
   * functions only; everything else assumed non-boolean (CASE handled
   * separately via "assume true" above).
   */
  private static boolean isBooleanFunc(LogicalTypesParser.Func_exprContext fctx) {
    if (fctx.func_application() == null) {
      // Special-syntax forms: only CAST(x AS BOOLEAN) returns boolean;
      // EXTRACT, SUBSTRING, POSITION, TRIM, CURRENT_TIMESTAMP all return
      // non-boolean types.
      LogicalTypesParser.Func_expr_common_subexprContext sub =
          fctx.func_expr_common_subexpr();
      return sub instanceof LogicalTypesParser.FuncCastContext
          && isCastToBoolean((LogicalTypesParser.FuncCastContext) sub);
    }
    String name = fctx.func_application().identifier().getText().toUpperCase(java.util.Locale.ROOT);
    switch (name) {
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
        return true;
      default:
        return false;
    }
  }

  // ---------------------------------------------------------------------
  // Column-ref + indirection validation
  // ---------------------------------------------------------------------

  /**
   * Recursively walk the expression and verify every column-ref root
   * resolves to a known column (or a runtime-injected variable).
   */
  static void validateColumnRefs(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.ColumnrefContext) {
      LogicalTypesParser.ColumnrefContext col = (LogicalTypesParser.ColumnrefContext) node;
      String root = colidName(col.colid());
      if (!vctx.isKnown(root)) {
        throw locatedError(col,
            "Unknown column in CHECK: '" + root + "'. Allowed columns: "
                + vctx.allowedNames() + ".");
      }
      validateIndirectionChain(col, vctx);
    }
    if (node instanceof LogicalTypesParser.CheckParenContext) {
      // Paren-form indirection: (expr).field / (expr)[idx]. Validate the
      // chain against the inner expression's resolved type so misnamed
      // fields and non-collection indexing are caught at parse time.
      LogicalTypesParser.CheckParenContext paren =
          (LogicalTypesParser.CheckParenContext) node;
      if (paren.indirection() != null) {
        Schema receiver = ConstraintResolver.tryResolveCheckExprType(paren.check_expr(), vctx);
        if (receiver != null) {
          validateIndirectionFromReceiver(receiver, paren.indirection(),
              "(" + paren.check_expr().getText() + ")", vctx);
        }
      }
    }
    if (node instanceof LogicalTypesParser.Func_applicationContext) {
      // Function-application's identifier is the function name, not a
      // column ref. Skip its identifier child but recurse into args.
      LogicalTypesParser.Func_applicationContext fctx =
          (LogicalTypesParser.Func_applicationContext) node;
      String name = fctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
      boolean isMacro = "EVERY".equals(name) || "ANY".equals(name) || "ONE".equals(name);
      if (fctx.check_expr_list() != null) {
        java.util.List<LogicalTypesParser.Check_exprContext> args =
            fctx.check_expr_list().check_expr();
        if (isMacro) {
          // Check macro arity FIRST. Otherwise a 2-arg `EVERY(tags, t)` falls
          // through to the generic per-arg validator, which sees `t` as a
          // column ref and reports "Unknown column 't'" — masking the real
          // problem (wrong arity).
          if (args.size() != 3) {
            throw locatedError(fctx,
                name + "() expects 3 arguments (collection, iteration variable, "
                    + "predicate), got " + args.size() + ". "
                    + "Example: " + name + "(tags, t, LENGTH(t) > 0)");
          }
          // Validate the collection (arg 0) in the outer context.
          validateColumnRefs(args.get(0), vctx);
          // Skip arg 1 (the iteration var binding name itself); validate the
          // predicate body (arg 2) in a context that includes the binding,
          // typed as the collection's element type if we can resolve it.
          String iterVar = args.get(1).getText();
          if (iterVar.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            Schema elemType = ConstraintResolver.resolveCollectionElementType(args.get(0), vctx);
            validateColumnRefs(args.get(2), vctx.withBinding(iterVar, elemType));
          }
        } else {
          for (LogicalTypesParser.Check_exprContext arg : args) {
            validateColumnRefs(arg, vctx);
          }
        }
      }
      return;
    }
    if (node instanceof LogicalTypesParser.FuncExtractContext) {
      // EXTRACT(field FROM ts) — `field` is a keyword identifier, not a
      // column ref. Recurse only into the ts expression.
      LogicalTypesParser.FuncExtractContext ext = (LogicalTypesParser.FuncExtractContext) node;
      validateColumnRefs(ext.check_expr(), vctx);
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateColumnRefs(node.getChild(i), vctx);
    }
  }

  /**
   * Validate each step of {@code col}'s indirection chain against the
   * receiver's type at that point. {@code .field} requires STRUCT;
   * {@code [index]} requires ARRAY/MULTISET/MAP. Mismatches throw a
   * located error so the user gets a clear parse-time message instead of
   * an opaque CEL evaluation failure (e.g. {@code this.x.y} where x:INT
   * would emit `this.x.y` and CEL would reject member access on int).
   *
   * <p>Best-effort: if the receiver's type can't be resolved (e.g. the root
   * is a binding with unknown type), the check defers to runtime.
   */
  private static void validateIndirectionChain(
      LogicalTypesParser.ColumnrefContext col, ConstraintValidationContext vctx) {
    if (col.indirection() == null) {
      return;
    }
    String rootName = colidName(col.colid());
    Schema current = vctx.schemaOf(rootName);
    if (current == null) {
      return;  // unknown receiver type — defer
    }
    validateIndirectionFromReceiver(current, col.indirection(), rootName, vctx);
  }

  /**
   * Shared core: walk an indirection chain against a known receiver type.
   * Used by both column-ref form ({@code col.field}) and paren form
   * ({@code (expr).field}). Resolves {@code NAMED_TYPE_REF} steps via
   * the validation context's named-type table so chains like
   * {@code addr.zip} (where {@code addr} is typed as a named ref to a
   * STRUCT body) validate correctly.
   */
  private static void validateIndirectionFromReceiver(
      Schema initial, LogicalTypesParser.IndirectionContext indirection,
      String pathRoot, ConstraintValidationContext vctx) {
    Schema current = ConstraintResolver.resolveIfNamedRef(initial, vctx);
    String pathSoFar = pathRoot;
    for (LogicalTypesParser.Indirection_elContext el : indirection.indirection_el()) {
      if (el.colid() != null) {
        // .field step
        String fieldName = colidName(el);
        if (current == null || current.getType() != Schema.Type.STRUCT) {
          // current may be null if the named-ref resolver couldn't find the
          // underlying body — defer to runtime in that case rather than
          // mis-rejecting.
          if (current == null) {
            return;
          }
          throw locatedError(el,
              "Cannot access field '" + fieldName + "' on '" + pathSoFar
                  + "' (type " + current.getType() + ") — only STRUCT/ROW "
                  + "supports field access.");
        }
        Schema.Field nested = current.getField(fieldName);
        if (nested == null) {
          throw locatedError(el,
              "Unknown field '" + fieldName + "' on STRUCT '" + pathSoFar + "'.");
        }
        current = ConstraintResolver.resolveIfNamedRef(nested.getSchema(), vctx);
        pathSoFar = pathSoFar + "." + fieldName;
      } else if (el.check_expr() != null) {
        // [index] step
        if (current == null) {
          return;  // can't validate further
        }
        Schema.Type t = current.getType();
        if (t != Schema.Type.ARRAY && t != Schema.Type.MULTISET
            && t != Schema.Type.MAP) {
          throw locatedError(el,
              "Cannot index '" + pathSoFar + "' (type " + t + ") with [...] "
                  + "— only ARRAY, MULTISET, and MAP support index access.");
        }
        // ARRAY/MULTISET indices are 1-based in SQL; emit translates to
        // CEL's 0-based form via `[(i) - 1]`. A literal i ≤ 0 produces a
        // negative CEL index → runtime "index out of bounds" crash. Reject
        // at parse time. MAP keys can be any type, so this only applies
        // to ARRAY/MULTISET.
        if (t == Schema.Type.ARRAY || t == Schema.Type.MULTISET) {
          Long literal = ConstraintResolver.tryLiteralIntValue(el.check_expr());
          if (literal != null && literal <= 0L) {
            throw locatedError(el,
                "ARRAY/MULTISET indexing is 1-based in SQL; got literal "
                    + literal + " for '" + pathSoFar + "[...]' which would "
                    + "translate to a negative CEL index (`[" + (literal - 1)
                    + "]`) and crash at runtime. Use a positive index.");
          }
        }
        current = (t == Schema.Type.MAP) ? current.getValueType()
            : current.getElementType();
        current = ConstraintResolver.resolveIfNamedRef(current, vctx);
        pathSoFar = pathSoFar + "[...]";
      }
    }
  }

  // ---------------------------------------------------------------------
  // Macro recursion helper (used by every type-aware pass)
  // ---------------------------------------------------------------------

  /**
   * Functional interface for a validation pass — needed because Java method
   * references can't be inlined as polymorphic walkers.
   */
  @FunctionalInterface
  interface ValidationPass {
    void run(org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx);
  }

  /**
   * Shared macro-binding handler for validation passes. Returns true (and
   * fully visits the macro's args) when {@code node} is a 3-arg
   * {@code EVERY/ANY/ONE} macro call: validates the collection in the outer
   * context, then validates the predicate body in a derived context with
   * the iteration variable bound to the collection's element type. Returns
   * false otherwise — the caller should continue its generic recursion.
   *
   * <p>Without this, type-aware passes (concat, cast, comparison) would see
   * the iteration variable as untyped and silently defer to runtime — even
   * when the element type is statically known. See {@code concatRejects
   * NumericMacroBinding} for the originating regression.
   */
  static boolean recurseIntoMacro(
      org.antlr.v4.runtime.tree.ParseTree node,
      ConstraintValidationContext vctx,
      ValidationPass pass) {
    if (!(node instanceof LogicalTypesParser.Func_applicationContext)) {
      return false;
    }
    LogicalTypesParser.Func_applicationContext fctx =
        (LogicalTypesParser.Func_applicationContext) node;
    String name = fctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
    boolean isMacro = "EVERY".equals(name) || "ANY".equals(name) || "ONE".equals(name);
    if (!isMacro || fctx.check_expr_list() == null) {
      return false;
    }
    List<LogicalTypesParser.Check_exprContext> args =
        fctx.check_expr_list().check_expr();
    if (args.size() != 3) {
      return false;
    }
    pass.run(args.get(0), vctx);
    String iterVar = args.get(1).getText();
    if (iterVar.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      Schema elemType = ConstraintResolver.resolveCollectionElementType(args.get(0), vctx);
      pass.run(args.get(2), vctx.withBinding(iterVar, elemType));
    }
    return true;
  }

  // ---------------------------------------------------------------------
  // Comparison validation
  // ---------------------------------------------------------------------

  /**
   * Walk comparison expressions and, when both sides are simple column refs,
   * check (a) the type is ordered (rejects BOOLEAN/ENUM/STRUCT/ARRAY/MAP/
   * UNION/VARIANT for {@code <}/{@code <=}/{@code >}/{@code >=}) and (b) the
   * two sides are CEL-comparable (no string vs bytes, no timestamp vs
   * duration). Function-result and arithmetic-result types aren't tracked in
   * v1 — those comparisons defer to runtime.
   */
  static void validateComparisons(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Check_expr_compareContext) {
      LogicalTypesParser.Check_expr_compareContext cmp =
          (LogicalTypesParser.Check_expr_compareContext) node;
      java.util.List<LogicalTypesParser.Check_expr_likeContext> sides = cmp.check_expr_like();
      if (sides.size() == 2) {
        validateCompare(cmp, sides.get(0), sides.get(1), vctx);
      }
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateComparisons)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateComparisons(node.getChild(i), vctx);
    }
  }

  private static void validateCompare(
      LogicalTypesParser.Check_expr_compareContext cmp,
      LogicalTypesParser.Check_expr_likeContext lhs,
      LogicalTypesParser.Check_expr_likeContext rhs,
      ConstraintValidationContext vctx) {
    String op = findComparisonOp(cmp);
    boolean isEquality = "=".equals(op) || "<>".equals(op) || "!=".equals(op);
    // SQL `x = NULL` / `x <> NULL` is always UNKNOWN (and CHECK passes); CEL
    // `x == null` is a real boolean comparison that fails for non-null rows.
    // Translating `=`/`<>` against NULL would silently flip the constraint's
    // semantics, so reject at parse time and direct the user to IS NULL.
    if (ConstraintResolver.isLiteralNullLikeChild(lhs)
        || ConstraintResolver.isLiteralNullLikeChild(rhs)) {
      throw locatedError(cmp,
          "Comparison " + op + " against NULL is always UNKNOWN in SQL "
              + "but would emit CEL that fails for non-null rows; "
              + "use IS NULL or IS NOT NULL instead.");
    }
    Schema lhsType = ConstraintResolver.tryResolveSimpleColumnRefType(lhs, vctx);
    Schema rhsType = ConstraintResolver.tryResolveSimpleColumnRefType(rhs, vctx);
    // Cross-category check for EQUALITY operators only. CEL spec: `==`/`!=`
    // accept any two types and return false for type mismatch — strict
    // cel-java accepts `int == string`. SQL semantics treats this as a
    // user mistake (always-false), so reject explicitly. Ordering
    // operators (`<`/`<=`/`>`/`>=`) DON'T need this check — strict
    // cel-java rejects `int < string` (no overload).
    if (isEquality && lhsType != null && rhsType != null
        && !ConstraintResolver.areComparable(lhsType.getType(), rhsType.getType())) {
      throw locatedError(cmp,
          "Cannot compare " + lhsType.getType() + " with " + rhsType.getType()
              + " using " + op + " — CEL evaluates equality across types as "
              + "always-false. Use explicit CAST to align types, or IS NULL "
              + "if testing for null.");
    }
    // Orderable-type check for ordering operators. nessie cel-java has
    // `_<_(bool, bool)` (CEL spec: false < true), so strict does NOT
    // reject `boolCol < boolCol` even though SQL doesn't order BOOLEAN.
    // Same for ENUM/STRUCT/ARRAY/MAP/UNION/VARIANT — CEL `==` accepts
    // them but ordering operators are SQL-meaningless.
    if (!isEquality) {
      if (lhsType != null && !ConstraintResolver.isOrderable(lhsType.getType())) {
        throw locatedError(cmp,
            "Cannot use relational operator " + op + " on " + lhsType.getType()
                + " — only numeric, string, bytes, and date/time types are "
                + "ordered. Use = / <> for equality.");
      }
      if (rhsType != null && !ConstraintResolver.isOrderable(rhsType.getType())) {
        throw locatedError(cmp,
            "Cannot use relational operator " + op + " on " + rhsType.getType()
                + " — only numeric, string, bytes, and date/time types are "
                + "ordered. Use = / <> for equality.");
      }
    }
  }

  private static String findComparisonOp(
      LogicalTypesParser.Check_expr_compareContext cmp) {
    for (int i = 0; i < cmp.getChildCount(); i++) {
      ParseTree child = cmp.getChild(i);
      if (child instanceof TerminalNode) {
        return child.getText();
      }
    }
    return "=";
  }

  // ---------------------------------------------------------------------
  // Arithmetic-operand intent validation
  // ---------------------------------------------------------------------

  /**
   * SQL {@code +}/{@code -}/{@code *}/{@code /}/{@code %} are numeric only,
   * but the translator emits them as CEL {@code +}/{@code -}/etc. as-is —
   * and CEL {@code _+_(string, string)} is valid (string concat), so
   * {@code 'a' + 'b'} would silently pass strict cel-java type-check
   * despite the user almost certainly having meant {@code ||}. Enforce
   * SQL intent here: arithmetic operands must be numeric.
   *
   * <p>Mixed-numeric (int + double) and mixed-category (string + int)
   * cases are caught by the strict cel-java post-emit checker — this
   * pass complements strict by catching the same-non-numeric-type case
   * (string + string, bytes + bytes, bool + bool) that strict accepts as
   * a valid CEL overload but is wrong for SQL.
   */
  static void validateArithmeticOperands(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Check_expr_addContext) {
      LogicalTypesParser.Check_expr_addContext add =
          (LogicalTypesParser.Check_expr_addContext) node;
      List<LogicalTypesParser.Check_expr_mulContext> muls = add.check_expr_mul();
      if (muls.size() > 1) {
        for (LogicalTypesParser.Check_expr_mulContext mul : muls) {
          checkArithmeticOperandIsNumeric(mul, vctx, "+/-");
        }
      }
    }
    if (node instanceof LogicalTypesParser.Check_expr_mulContext) {
      LogicalTypesParser.Check_expr_mulContext mul =
          (LogicalTypesParser.Check_expr_mulContext) node;
      List<LogicalTypesParser.Check_expr_unary_signContext> signs =
          mul.check_expr_unary_sign();
      if (signs.size() > 1) {
        for (LogicalTypesParser.Check_expr_unary_signContext sign : signs) {
          checkArithmeticUnarySignIsNumeric(sign, vctx);
        }
      }
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateArithmeticOperands)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateArithmeticOperands(node.getChild(i), vctx);
    }
  }

  private static void checkArithmeticOperandIsNumeric(
      LogicalTypesParser.Check_expr_mulContext mul,
      ConstraintValidationContext vctx, String opLabel) {
    Schema t = ConstraintResolver.tryResolveMulType(mul, vctx);
    if (t == null) {
      return;
    }
    String cat = ConstraintResolver.categoryOf(t.getType());
    // "datetime" is allowed for +/- to support timestamp arithmetic
    // (timestamp ± interval, timestamp − timestamp); the strict cel-java
    // checker validates the specific temporal overload (rejecting e.g.
    // timestamp + timestamp). Interval operands resolve to null and are skipped.
    if (!"int".equals(cat) && !"double".equals(cat) && !"decimal".equals(cat)
        && !"datetime".equals(cat)) {
      throw locatedError(mul,
          "Operator " + opLabel + " operand must be numeric (int/double/"
              + "decimal) or a timestamp (± interval); got " + t.getType()
              + ". For string/bytes concatenation use ||.");
    }
  }

  private static void checkArithmeticUnarySignIsNumeric(
      LogicalTypesParser.Check_expr_unary_signContext sign,
      ConstraintValidationContext vctx) {
    Schema t = ConstraintResolver.tryResolveUnarySignType(sign, vctx);
    if (t == null) {
      return;
    }
    String cat = ConstraintResolver.categoryOf(t.getType());
    if (!"int".equals(cat) && !"double".equals(cat) && !"decimal".equals(cat)) {
      throw locatedError(sign,
          "Operator * / % operand must be numeric (int/double/decimal); "
              + "got " + t.getType() + ".");
    }
  }

  // ---------------------------------------------------------------------
  // Concat-operand intent validation
  // ---------------------------------------------------------------------

  /**
   * SQL {@code ||} is string/bytes concatenation only — but the translator
   * emits it as CEL {@code +}, which works for numerics too. Without this
   * pass, {@code qty || 1} (where qty is INT) would silently emit a valid
   * but semantically wrong {@code this.qty + 1} addition. Strict cel-java
   * checks the emitted CEL types but can't see the user's intent — it
   * accepts {@code int + int} regardless of whether the user wrote
   * {@code +} or {@code ||}. So this pass enforces the SQL-level rule
   * (operands of {@code ||} must be string/bytes).
   *
   * <p>NULL operand and operand-type-mismatch detection are now handled by
   * the strict cel-java checker post-emit; this pass focuses solely on
   * the intent check.
   */
  static void validateConcatOperands(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Check_expr_concatContext) {
      LogicalTypesParser.Check_expr_concatContext concat =
          (LogicalTypesParser.Check_expr_concatContext) node;
      List<LogicalTypesParser.Check_expr_addContext> adds = concat.check_expr_add();
      if (adds.size() > 1) {
        for (LogicalTypesParser.Check_expr_addContext add : adds) {
          checkConcatOperandIsStringLike(add, vctx);
        }
      }
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateConcatOperands)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateConcatOperands(node.getChild(i), vctx);
    }
  }

  private static void checkConcatOperandIsStringLike(
      LogicalTypesParser.Check_expr_addContext add, ConstraintValidationContext vctx) {
    // Only inspect the leaf type when there's no nested arithmetic — `||`
    // applied to a compound numeric expression like `(x + 1)` is rare and
    // we let it through (strict-check will catch the resulting emit if
    // the types don't unify).
    if (add.check_expr_mul().size() > 1) {
      return;
    }
    Schema type = ConstraintResolver.tryResolveMulType(add.check_expr_mul(0), vctx);
    if (type == null) {
      return;
    }
    if (!ConstraintResolver.isStringOrBytes(type.getType())) {
      throw locatedError(add,
          "Operator || is string/bytes concatenation only — got "
              + type.getType() + ". Use + for numeric addition.");
    }
  }

  // ---------------------------------------------------------------------
  // CASE branch validation
  // ---------------------------------------------------------------------

  /**
   * Walk every {@code case_expr}, resolve the result type of each WHEN's THEN
   * branch and the ELSE expression, and reject when any pair of resolved
   * types crosses categories (e.g.
   * {@code CASE WHEN c THEN 1 ELSE 'x' END}). The translator emits a CEL
   * ternary which fails to compile when branches have incompatible types;
   * catching it here gives a parse-time message instead of a CEL evaluation
   * error far from the source.
   *
   * <p>Best-effort, mirroring the other type-aware passes: branches whose
   * type can't be resolved (function results, complex expressions) are
   * skipped — only known-mismatched pairs are flagged.
   */
  static void validateCaseBranches(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Case_exprContext) {
      checkCaseBranchTypes((LogicalTypesParser.Case_exprContext) node, vctx);
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateCaseBranches)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateCaseBranches(node.getChild(i), vctx);
    }
  }

  private static void checkCaseBranchTypes(
      LogicalTypesParser.Case_exprContext ctx, ConstraintValidationContext vctx) {
    // CASE without ELSE emits `... ? then : null` — CEL ternary requires both
    // branches to share a type, and `<resultType>` vs `null` doesn't unify.
    // SQL semantics return NULL when no branch matches, but CEL has no way to
    // express that without losing the result type. Direct the user to add an
    // explicit ELSE.
    if (findElseBranch(ctx) == null) {
      throw locatedError(ctx,
          "CASE expression is missing an ELSE branch — CEL ternaries require "
              + "both branches to share a type. Add an explicit "
              + "`ELSE <default>` (or use COALESCE for nullable defaults).");
    }
    // Two checks per case_expr:
    //  1. All WHEN/THEN result expressions plus any ELSE share a category
    //     (so the emitted CEL ternary type-checks).
    //  2. For SIMPLE CASE (CASE x WHEN v1 THEN ... WHEN v2 THEN ... END), the
    //     case-arg `x` and every WHEN's value `vN` share a category — the
    //     translator emits `x == vN` per branch, which CEL refuses to compile
    //     when the sides cross categories.
    LogicalTypesParser.Check_exprContext caseArg = findCaseArg(ctx);
    if (caseArg != null) {
      // Reject `WHEN NULL` for simple CASE: SQL `subject = NULL` is always
      // UNKNOWN (so the branch never matches), but the translator emits
      // `subject == NULL` per WHEN — a real CEL null-equality test that
      // actually fires for null subjects. Same class of semantic flip as
      // bare `x = NULL` (rejected by validateCompare).
      for (LogicalTypesParser.When_clauseContext when : ctx.when_clause()) {
        if (ConstraintResolver.isLiteralNull(when.check_expr(0))) {
          throw locatedError(when,
              "Simple CASE WHEN value cannot be NULL — SQL `subject = NULL` "
                  + "is always UNKNOWN but the emit produces `subject == null`, "
                  + "which silently flips the branch's semantics. "
                  + "Use the searched form: `CASE WHEN subject IS NULL THEN ...`.");
        }
      }
      Schema caseArgType = ConstraintResolver.tryResolveCheckExprType(caseArg, vctx);
      if (caseArgType != null && ConstraintResolver.categoryOf(caseArgType.getType()) != null) {
        for (LogicalTypesParser.When_clauseContext when : ctx.when_clause()) {
          Schema whenValType = ConstraintResolver.tryResolveCheckExprType(when.check_expr(0), vctx);
          if (whenValType == null) {
            continue;
          }
          if (!ConstraintResolver.areComparable(caseArgType.getType(), whenValType.getType())) {
            throw locatedError(ctx,
                "Simple CASE: subject type " + caseArgType.getType()
                    + " is incompatible with WHEN value type "
                    + whenValType.getType()
                    + ". The translator emits `subject == value` per WHEN, "
                    + "which CEL rejects across categories.");
          }
        }
      }
    }

    // Reject literal NULL as a branch result with a clearer message than
    // strict-check produces (the strict checker reports a ternary-branch
    // type-mismatch but doesn't suggest COALESCE / explicit default).
    List<LogicalTypesParser.Check_exprContext> branches = new ArrayList<>();
    for (LogicalTypesParser.When_clauseContext when : ctx.when_clause()) {
      branches.add(when.check_expr(1));
    }
    LogicalTypesParser.Check_exprContext elseExpr = findElseBranch(ctx);
    if (elseExpr != null) {
      branches.add(elseExpr);
    }
    for (LogicalTypesParser.Check_exprContext branch : branches) {
      if (ConstraintResolver.isLiteralNull(branch)) {
        throw locatedError(branch,
            "CASE branch result cannot be a literal NULL — CEL ternary "
                + "branches must share a type, and bare `null` doesn't "
                + "unify with the other branch type. Use COALESCE or an "
                + "explicit typed default.");
      }
    }
    // Cross-category branches (e.g. WHEN ... THEN 1 ELSE 'a') are caught
    // by strict cel-java post-emit type-check — the emitted ternary fails
    // to unify branch types.
  }

  /**
   * Extract the optional case-arg from a {@code case_expr} — the
   * {@code check_expr} that appears between {@code CASE} and the first
   * {@code WHEN}. Returns null for searched-CASE form
   * ({@code CASE WHEN c THEN ...}).
   */
  private static LogicalTypesParser.Check_exprContext findCaseArg(
      LogicalTypesParser.Case_exprContext ctx) {
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof LogicalTypesParser.When_clauseContext) {
        return null;
      }
      if (child instanceof LogicalTypesParser.Check_exprContext) {
        return (LogicalTypesParser.Check_exprContext) child;
      }
    }
    return null;
  }

  /**
   * Extract the {@code check_expr} that follows the {@code ELSE} keyword in a
   * case_expr, or null if no ELSE clause is present. Mirrors the parsing
   * logic in {@code visitCase} since the grammar exposes both the optional
   * case-arg and the optional ELSE as untagged {@code check_expr} children.
   */
  private static LogicalTypesParser.Check_exprContext findElseBranch(
      LogicalTypesParser.Case_exprContext ctx) {
    boolean afterElse = false;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode
          && "ELSE".equalsIgnoreCase(child.getText())) {
        afterElse = true;
      } else if (afterElse
          && child instanceof LogicalTypesParser.Check_exprContext) {
        return (LogicalTypesParser.Check_exprContext) child;
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------
  // BETWEEN validation
  // ---------------------------------------------------------------------

  /**
   * Walk every {@code check_expr_between} and reject when the subject and
   * either bound resolve to incompatible categories — e.g.
   * {@code intCol BETWEEN 'a' AND 'b'}. The two-binary-side comparison
   * validator only fires on {@code check_expr_compare}; BETWEEN is a 3-ary
   * relation that lives outside that path, so we mirror the same coarse
   * type-resolution here.
   */
  static void validateBetweenOperands(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Check_expr_betweenContext) {
      LogicalTypesParser.Check_expr_betweenContext between =
          (LogicalTypesParser.Check_expr_betweenContext) node;
      if (between.BETWEEN() != null) {
        checkBetweenOperandTypes(between, vctx);
      }
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateBetweenOperands)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateBetweenOperands(node.getChild(i), vctx);
    }
  }

  private static void checkBetweenOperandTypes(
      LogicalTypesParser.Check_expr_betweenContext ctx,
      ConstraintValidationContext vctx) {
    List<LogicalTypesParser.Check_expr_inContext> ins = ctx.check_expr_in();
    // Bare NULL bounds emit `(null) <= x && x <= (null)` which CEL has no
    // overload for — runtime "no matching overload" error. Reject early.
    rejectNullBound(ins.get(1), "lower");
    rejectNullBound(ins.get(2), "upper");
    // Null-returning function check applies to ALL three positions —
    // subject, lower bound, upper bound. The emit produces
    // `lo <= subject && subject <= hi`, so a null in any position hits
    // a comparison with no null overload at runtime.
    rejectNullReturningFuncBound(ins.get(0), "subject");
    rejectNullReturningFuncBound(ins.get(1), "lower bound");
    rejectNullReturningFuncBound(ins.get(2), "upper bound");
    // IS NULL / IS NOT NULL bounds, and cross-category subject vs bound,
    // are now caught by the strict cel-java post-emit checker:
    //   - `(x IS NULL) <= y` emits `bool <= int` (no overload)
    //   - `'a' <= int_subject` has no `_<=_(string,int)` overload
    // No hand-coded checks needed.
  }

  /**
   * Reject a BETWEEN operand that's a function call which can return null
   * (currently just NULLIF). Strict cel-java accepts the emit because the
   * function's return type is the non-null type, but at runtime a null
   * result hits a comparison op with no null overload and the constraint
   * crashes. Applies to subject and both bounds: the emit shape
   * {@code lo <= subj && subj <= hi} fails for a null in any position.
   */
  private static void rejectNullReturningFuncBound(
      LogicalTypesParser.Check_expr_inContext operand, String role) {
    String funcName = ConstraintResolver.tryExtractLeafFuncNameIn(operand);
    if (funcName != null
        && ConstraintResolver.NULL_RETURNING_FUNCS.contains(funcName)) {
      throw locatedError(operand,
          "BETWEEN " + role + " cannot be " + funcName
              + "(...) — the function can return null, and CEL has no "
              + "comparison overload taking null. Use COALESCE to provide "
              + "an explicit fallback value.");
    }
  }

  private static void rejectNullBound(
      LogicalTypesParser.Check_expr_inContext bound, String role) {
    if (ConstraintResolver.isBoundLiteralNull(bound)) {
      throw locatedError(bound,
          "BETWEEN " + role + " bound cannot be NULL — CEL has no `<=` "
              + "overload taking null. Use IS NULL or COALESCE to handle "
              + "missing bounds explicitly.");
    }
  }

  // ---------------------------------------------------------------------
  // IN list/target validation
  // ---------------------------------------------------------------------

  /**
   * For every {@code x IN (a, b, c)} parenthesized list, require all elements
   * to share a comparable type category. CEL list literals are strongly typed
   * (no implicit dyn() promotion), so {@code [1, 'two', true]} would fail
   * runtime type-checking; reject at parse time instead. Elements whose type
   * we can't resolve (function results, complex sub-expressions) defer to
   * runtime — same conservative policy as the rest of the validator.
   */
  static void validateInList(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Check_expr_inContext) {
      LogicalTypesParser.Check_expr_inContext in =
          (LogicalTypesParser.Check_expr_inContext) node;
      if (in.IN() != null
          && in.in_target() instanceof LogicalTypesParser.InTargetParenListContext) {
        validateInParenList(
            (LogicalTypesParser.InTargetParenListContext) in.in_target(), vctx);
      }
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateInList)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateInList(node.getChild(i), vctx);
    }
  }

  /**
   * Validate IN (paren-list) elements:
   * <ul>
   *   <li>Reject literal NULL elements — SQL-semantic intent ambiguity that
   *       strict can't see (CEL dyn-promotes the list).</li>
   *   <li>Reject element-type heterogeneity — strict accepts {@code list<dyn>}
   *       silently.</li>
   * </ul>
   *
   * <p>Other checks moved to strict-check: target-is-collection, LHS-vs-elem
   * comparability, and one-element-collection (`x IN (col)` where col is a
   * collection) are all caught by strict's {@code _in_} overload mismatch.
   */
  private static void validateInParenList(
      LogicalTypesParser.InTargetParenListContext list,
      ConstraintValidationContext vctx) {
    List<LogicalTypesParser.Check_exprContext> items =
        list.check_expr_list().check_expr();
    for (LogicalTypesParser.Check_exprContext item : items) {
      if (ConstraintResolver.isLiteralNull(item)) {
        throw locatedError(item,
            "IN list cannot contain NULL — use `x IS NULL OR x IN (...)` "
                + "to test for null membership separately.");
      }
    }
    String firstCategory = null;
    Schema.Type firstType = null;
    for (LogicalTypesParser.Check_exprContext item : items) {
      Schema t = ConstraintResolver.tryResolveCheckExprType(item, vctx);
      if (t == null) {
        continue;
      }
      String cat = ConstraintResolver.categoryOf(t.getType());
      if (cat == null) {
        continue;
      }
      if (firstCategory == null) {
        firstCategory = cat;
        firstType = t.getType();
        continue;
      }
      if (!firstCategory.equals(cat)) {
        throw locatedError(item,
            "IN list element type " + t.getType() + " is not comparable "
                + "with the earlier element type " + firstType
                + " — heterogeneous list literals would emit a `list<dyn>` "
                + "that silently accepts mismatched membership tests. Make "
                + "all elements share a category (numeric, string, bytes, "
                + "or date/time).");
      }
    }
  }

  // ---------------------------------------------------------------------
  // Function-arg validation
  // ---------------------------------------------------------------------

  /**
   * For functions whose CEL emit unifies argument types — GREATEST, LEAST,
   * COALESCE, NULLIF — require all args to share a comparable type category
   * (matching the rest of the validator's policy). Also reject literal NULL
   * for GREATEST/LEAST/NULLIF: each emits CEL operators that fail on null
   * operands. COALESCE explicitly handles nulls so a literal NULL arg is
   * allowed there (the emit's null-check guards the value branch).
   *
   * <p>Args whose type can't be resolved (function results, complex
   * sub-expressions) are skipped — the validator stays best-effort.
   */
  static void validateFunctionArgs(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Func_applicationContext) {
      LogicalTypesParser.Func_applicationContext fctx =
          (LogicalTypesParser.Func_applicationContext) node;
      String name = fctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
      List<LogicalTypesParser.Check_exprContext> args =
          fctx.check_expr_list() != null
              ? fctx.check_expr_list().check_expr()
              : java.util.Collections.emptyList();
      // Argument-type validation (string-funcs receivers, CONTAINS/etc.
      // homogeneous strings, GREATEST/LEAST/COALESCE/NULLIF category
      // unification) is now handled by strict cel-java post-emit. The
      // only remaining check here is literal-NULL rejection in
      // GREATEST/LEAST/NULLIF/COALESCE — strict accepts `null` as a CEL
      // type, but our SQL-semantic policy is to reject it explicitly.
      if (NULL_REJECTING_FUNCS.contains(name) && !args.isEmpty()) {
        rejectLiteralNullArgs(name, args);
      }
      // Column-level skip-on-null contract: COALESCE(<constrained-col>,
      // fallback) is dead because `this` is guaranteed non-null at
      // evaluation time, so the fallback args never run. Fires only when
      // the first arg is a bare reference to the constrained column;
      // macro iter-var bindings inside macro bodies (which shadow the
      // column name) and nested-field args are allowed.
      if ("COALESCE".equals(name) && !args.isEmpty()) {
        LogicalTypesParser.ColumnrefContext col =
            ConstraintResolver.tryExtractLeafColumnRef(args.get(0));
        if (col != null && col.indirection() == null
            && vctx.isConstrainedColumn(colidName(col.colid()))) {
          throw locatedError(args.get(0),
              "COALESCE() at column level with the bare column as first arg "
                  + "is dead code — the runtime skips evaluation when the "
                  + "column value is null/missing, so `this` is guaranteed "
                  + "non-null and the fallback args never run. Drop the "
                  + "COALESCE wrapping, or apply COALESCE to a nullable "
                  + "nested field or move the test to a table-level CHECK.");
        }
      }
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateFunctionArgs)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateFunctionArgs(node.getChild(i), vctx);
    }
  }

  private static final java.util.Set<String> NULL_REJECTING_FUNCS =
      java.util.Collections.unmodifiableSet(new java.util.HashSet<>(
          java.util.Arrays.asList(
              "GREATEST", "LEAST", "COALESCE", "NULLIF")));

  /**
   * Reject literal NULL args in GREATEST/LEAST/NULLIF/COALESCE. Strict
   * cel-java accepts {@code null} as a typed value, but our SQL semantic
   * is that a literal NULL in these contexts is dead code (NULLIF: never
   * matches; COALESCE: never the first non-null; GREATEST/LEAST: emits
   * CEL operators with no null overload). Direct the user to IS NULL or
   * to drop the NULL argument.
   */
  private static void rejectLiteralNullArgs(
      String name, List<LogicalTypesParser.Check_exprContext> args) {
    for (LogicalTypesParser.Check_exprContext arg : args) {
      if (ConstraintResolver.isLiteralNull(arg)) {
        throw locatedError(arg,
            name + "() cannot take a literal NULL — the emit applies CEL "
                + "operators that have no null overload (or, for COALESCE, "
                + "produces a ternary whose branches don't type-unify). "
                + "Drop the NULL argument or use IS NULL to test for null.");
      }
    }
  }

  // ---------------------------------------------------------------------
  // IS NULL operand validation (dead-code detection)
  // ---------------------------------------------------------------------

  /**
   * Walk every {@code IS NULL} / {@code IS NOT NULL} test and reject when
   * the operand can never be null. Three shapes are detected:
   * <ul>
   *   <li>Function call to a never-null-returning function (e.g.
   *       {@code LENGTH(name) IS NULL}) — the emit produces a well-typed
   *       CEL expression ({@code dyn(size(this.name)) == null}) that
   *       strict cel-java accepts, but the comparison is unconditionally
   *       false at runtime.</li>
   *   <li>Bare column reference at column-level placement — the runtime
   *       guarantees {@code this} is non-null when the CHECK fires (the
   *       skip-on-null contract: column-level CHECKs are not invoked for
   *       missing/unset/null values). So {@code <col> IS NULL} at column
   *       level can never fire, and {@code <col> IS NOT NULL} is always
   *       true. Use NOT NULL at the schema level for nullability, or
   *       move the test to a table-level CHECK where {@code has(this.x)}
   *       supports format-aware presence checks.</li>
   *   <li>Column reference whose entire indirection chain is declared
   *       NOT NULL (e.g. {@code zip IS NULL} where the {@code zip}
   *       column is NOT NULL, or {@code addr.zip IS NULL} where both
   *       {@code addr} and {@code zip} are NOT NULL) — same shape of
   *       silently-dead constraint.</li>
   * </ul>
   *
   * <p>Best-effort, mirroring the rest of the validator: only flagged
   * when the operand resolves cleanly via the dedicated extraction
   * helpers in {@link ConstraintResolver}; compound expressions or any
   * nullable / unresolvable step in a chain defers to runtime.
   */
  static void validateIsNullOperand(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Check_expr_isnullContext) {
      LogicalTypesParser.Check_expr_isnullContext is =
          (LogicalTypesParser.Check_expr_isnullContext) node;
      if (is.IS() != null) {
        String funcName = ConstraintResolver.tryExtractLeafFuncNameCompare(
            is.check_expr_compare());
        if (funcName != null
            && ConstraintResolver.NEVER_NULL_FUNCS.contains(funcName)) {
          throw locatedError(is,
              "IS NULL on " + funcName + "(...) is dead code — the function "
                  + "always returns a non-null value, so the comparison is "
                  + "unconditionally false. Drop the IS NULL test, or apply "
                  + "IS NULL to a column reference instead.");
        }
        LogicalTypesParser.ColumnrefContext col =
            ConstraintResolver.tryExtractLeafColumnRefCompare(is.check_expr_compare());
        if (col != null) {
          // Column-level + bare reference to the constrained column →
          // skip-on-null contract: `this` is guaranteed non-null when the
          // CHECK fires, so the IS NULL test is dead regardless of declared
          // nullability. Macro iter-var bindings (which shadow the column
          // name) are NOT subject to this rule — only the constrained
          // column gets the skip-on-null guarantee.
          if (col.indirection() == null
              && vctx.isConstrainedColumn(colidName(col.colid()))) {
            throw locatedError(is,
                "IS NULL on a column-level constraint never fires — the "
                    + "runtime skips evaluation when the column value is "
                    + "null/missing, so `this` is guaranteed non-null at "
                    + "evaluation time. Use NOT NULL at the schema level "
                    + "for nullability declarations, or move the test to a "
                    + "table-level CHECK where format-aware presence checks "
                    + "(has(this.x)) are available.");
          }
          if (ConstraintResolver.isWhollyNonNullableColumnRef(col, vctx)) {
            throw locatedError(is,
                "IS NULL on '" + col.getText() + "' is dead code — every "
                    + "step in the column reference is declared NOT NULL, so "
                    + "the value can never be null. Drop the IS NULL test, "
                    + "or apply it to a nullable column.");
          }
        }
      }
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateIsNullOperand)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateIsNullOperand(node.getChild(i), vctx);
    }
  }

  // ---------------------------------------------------------------------
  // Macro-body validation
  // ---------------------------------------------------------------------

  /**
   * Walk every macro call ({@code EVERY/ANY/ONE}) and reject when its
   * predicate body (arg 2) isn't boolean-shaped. The macro itself returns
   * boolean regardless of body shape, so {@link #validateBooleanRoot} can't
   * see the issue from the top level — we have to descend into each macro.
   *
   * <p>Body validation runs in a context that includes the iteration
   * variable, mirroring the binding-aware passes.
   */
  static void validateMacroBodies(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Func_applicationContext) {
      LogicalTypesParser.Func_applicationContext fctx =
          (LogicalTypesParser.Func_applicationContext) node;
      String name = fctx.identifier().getText().toUpperCase(java.util.Locale.ROOT);
      boolean isMacro = "EVERY".equals(name) || "ANY".equals(name) || "ONE".equals(name);
      if (isMacro && fctx.check_expr_list() != null) {
        List<LogicalTypesParser.Check_exprContext> args =
            fctx.check_expr_list().check_expr();
        if (args.size() == 3) {
          String iterVar = args.get(1).getText();
          ConstraintValidationContext bodyCtx = vctx;
          if (iterVar.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            Schema elemType = ConstraintResolver.resolveCollectionElementType(args.get(0), vctx);
            bodyCtx = vctx.withBinding(iterVar, elemType);
          }
          if (!isBooleanShaped(args.get(2), bodyCtx)) {
            throw locatedError(args.get(2),
                name + "() predicate body must yield a boolean, got: '"
                    + args.get(2).getText() + "'. Use a comparison, "
                    + "logical operator, IS NULL, IN, BETWEEN, LIKE, or a "
                    + "boolean-returning function (CONTAINS, STARTS_WITH, "
                    + "IS_EMAIL, etc.).");
          }
          // Continue recursing into the body to catch nested macros.
          validateMacroBodies(args.get(2), bodyCtx);
          // Also recurse into arg 0 (collection) under the outer context.
          validateMacroBodies(args.get(0), vctx);
          return;
        }
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateMacroBodies(node.getChild(i), vctx);
    }
  }

  // ---------------------------------------------------------------------
  // ---------------------------------------------------------------------
  // Numeric-literal range validation
  // ---------------------------------------------------------------------

  /**
   * Walk every numeric literal and reject overflow before emit.
   * <ul>
   *   <li>Float: {@code 1e500} parses as {@code +Infinity} in CEL — silently
   *       turns {@code x > 1e500} into an always-false constraint. Reject
   *       at parse time.</li>
   *   <li>Int: values outside {@code Long} range would require special CEL
   *       handling (BigInt isn't a built-in). Reject so the user sees a
   *       parse error rather than a runtime CEL coercion failure.</li>
   * </ul>
   */
  static void validateLiterals(org.antlr.v4.runtime.tree.ParseTree node) {
    if (node instanceof LogicalTypesParser.LiteralContext) {
      LogicalTypesParser.LiteralContext lit = (LogicalTypesParser.LiteralContext) node;
      if (lit.doubleLiteral() != null) {
        // Approximate-numeric (exponent) literal — emits as a CEL double, so an
        // out-of-range magnitude would overflow to ±Infinity (silently
        // always-false). Reject it.
        String text = lit.doubleLiteral().getText();
        try {
          double v = Double.parseDouble(text);
          if (Double.isInfinite(v) || Double.isNaN(v)) {
            throw locatedError(lit,
                "Float literal " + text + " is out of range (overflows to "
                    + (Double.isNaN(v) ? "NaN" : "Infinity") + " in CEL); "
                    + "constraint would be silently always-false.");
          }
        } catch (NumberFormatException e) {
          throw locatedError(lit, "Malformed float literal: " + text);
        }
      } else if (lit.decimalLiteral() != null) {
        // Exact-numeric literal — carried as an arbitrary-precision BigDecimal at
        // runtime, so any magnitude is fine; only check well-formedness.
        String text = lit.decimalLiteral().getText();
        try {
          Double.parseDouble(text);
        } catch (NumberFormatException e) {
          throw locatedError(lit, "Malformed decimal literal: " + text);
        }
      } else if (lit.intLiteral() != null) {
        String text = lit.intLiteral().getText();
        try {
          Long.parseLong(text);
        } catch (NumberFormatException e) {
          throw locatedError(lit,
              "Integer literal " + text + " is out of range for CEL int "
                  + "(64-bit signed). Maximum: 9223372036854775807.");
        }
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateLiterals(node.getChild(i));
    }
  }

  // ---------------------------------------------------------------------
  // LIKE pattern validation
  // ---------------------------------------------------------------------

  /**
   * Walk every {@code LIKE} clause and reject SQL-spec ESCAPE-clause
   * violations:
   * <ul>
   *   <li>Malformed ESCAPE clause (length != 1) — must be flagged as such
   *       before the trailing-escape check runs (otherwise we'd silently
   *       fall back to the default {@code \} and complain about a
   *       "trailing backslash" when the real bug is a multi-char ESCAPE).</li>
   *   <li>Trailing escape character with no following char (SQL spec:
   *       "Invalid escape sequence"). The default escape char is
   *       {@code \}, so even {@code 'foo\'} without an explicit ESCAPE
   *       clause is malformed.</li>
   * </ul>
   *
   * <p>Receiver-type checking ({@code intCol LIKE 'foo%'}) is the strict
   * cel-java post-emit checker's job — {@code (int).matches(string)} has
   * no CEL overload, so the strict checker rejects it.
   */
  static void validateLikePatterns(
      org.antlr.v4.runtime.tree.ParseTree node, ConstraintValidationContext vctx) {
    if (node instanceof LogicalTypesParser.Check_expr_likeContext) {
      LogicalTypesParser.Check_expr_likeContext like =
          (LogicalTypesParser.Check_expr_likeContext) node;
      if (like.LIKE() != null && like.stringLiteral() != null) {
        validateLikePattern(like);
      }
    }
    if (recurseIntoMacro(node, vctx,
        ConstraintValidator::validateLikePatterns)) {
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      validateLikePatterns(node.getChild(i), vctx);
    }
  }

  private static void validateLikePattern(
      LogicalTypesParser.Check_expr_likeContext like) {
    String pattern = ConstraintPatterns.extractSqlStringContent(
        like.stringLiteral().getText());
    char escapeChar = '\\';
    if (like.escape_clause() != null) {
      String escRaw = ConstraintPatterns.extractSqlStringContent(
          like.escape_clause().stringLiteral().getText());
      if (escRaw.length() != 1) {
        throw locatedError(like.escape_clause(),
            "ESCAPE clause must specify exactly one character, got: '"
                + escRaw + "'");
      }
      escapeChar = escRaw.charAt(0);
    }
    for (int i = 0; i < pattern.length(); i++) {
      if (pattern.charAt(i) == escapeChar) {
        if (i + 1 >= pattern.length()) {
          throw locatedError(like,
              "LIKE pattern ends with the escape character ('"
                  + escapeChar + "') with no following character — "
                  + "this is an invalid escape sequence per the SQL spec.");
        }
        i++;  // skip the escaped char
      }
    }
  }
}
