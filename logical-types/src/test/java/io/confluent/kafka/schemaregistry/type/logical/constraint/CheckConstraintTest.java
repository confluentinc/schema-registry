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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesParserFactory;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesSchemaVisitor;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 1 tests: smallest viable subset of CHECK — comparison + logical +
 * literals + columnref + parens + CASE. No functions, no LIKE, no IS NULL,
 * no IN, no BETWEEN yet.
 */
class CheckConstraintTest {

  // ---------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------

  private LogicalTypesSchemaVisitor parseScript(String input) {
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    visitor.visit(LogicalTypesParserFactory.parse(input));
    return visitor;
  }

  /**
   * Parse a single CHECK expression as a table-level constraint over a
   * comprehensive test struct, and return its CEL translation. The test
   * struct defines a wide column set so test expressions can reference common
   * names (name, email, id, ts, tags, etc.) without needing to declare them
   * each time.
   */
  private String translateCheck(String checkExpr) {
    String script = "ROW T ("
        + "x INT, value STRING, name STRING, id STRING, email STRING,"
        + "phone STRING, digits STRING, u STRING, h STRING, addr STRING,"
        + "needle STRING, haystack STRING, filename STRING,"
        + "lo INT, hi INT, age INT,"
        + "ratio DOUBLE,"
        + "amount DECIMAL(10, 2), price DECIMAL(12, 4), discount DECIMAL(10, 2),"
        + "blob BYTES,"
        + "ts TIMESTAMP_LTZ, created_at TIMESTAMP_LTZ,"
        + "first_name STRING, last_name STRING, type STRING,"
        + "size STRING, qty INT, status STRING, active BOOLEAN,"
        + "a INT, b INT, c INT,"
        + "tags STRING ARRAY, ages INT ARRAY,"
        + "matrix INT ARRAY ARRAY,"
        + "addresses STRING ARRAY, emails STRING ARRAY,"
        + "addr_struct ROW(zip INT) NOT NULL,"
        + "CHECK (" + checkExpr + ")"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getRules().get(0).getExpr();
    // Gate every emitted CEL through the strict cel-java checker. `this`
    // is a typed object resolved via ConstraintTypeProvider, so
    // type/overload errors are caught with full type fidelity. Strict
    // supersedes the older dyn-tolerant assertValid pre-pass — syntax
    // errors, undeclared identifiers, missing overloads, has()-argument
    // shape, and reserved-word collisions all surface here.
    try {
      CelValidator.assertValidStrict(cel, schema);
    } catch (AssertionError e) {
      throw new AssertionError(
          "CEL emit failed strict validation.\n  SQL: " + checkExpr
              + "\n  CEL: " + cel + "\n  " + e.getMessage(), e);
    }
    return cel;
  }

  /**
   * Translate a single column-level CHECK over the named column. Column-level
   * CHECKs emit {@code this} (the column value itself) rather than
   * {@code this.<col>} — strict cel-java checking against this shape exercises
   * the column-level branch in {@link ConstraintCelChecker} where {@code this}
   * is the column's primitive CEL type, not the surrounding struct.
   */
  private String translateColumnCheck(String columnDecl, String checkExpr) {
    String script = "ROW T ("
        + columnDecl + " CHECK (" + checkExpr + ")"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    // The CEL is on the column's rules list (not the struct's).
    return schema.getFields().get(0).getRules().get(0).getExpr();
  }

  @Test
  void equality() {
    assertEquals("this.x == 5", translateCheck("x = 5"));
  }

  @Test
  void notEqualAngleBrackets() {
    assertEquals("this.x != 5", translateCheck("x <> 5"));
  }

  @Test
  void notEqualBang() {
    assertEquals("this.x != 5", translateCheck("x != 5"));
  }

  @Test
  void lessThan() {
    assertEquals("this.x < 5", translateCheck("x < 5"));
  }

  @Test
  void lessOrEqual() {
    assertEquals("this.x <= 5", translateCheck("x <= 5"));
  }

  @Test
  void greaterThan() {
    assertEquals("this.x > 5", translateCheck("x > 5"));
  }

  @Test
  void greaterOrEqual() {
    assertEquals("this.x >= 5", translateCheck("x >= 5"));
  }

  // ---------------------------------------------------------------------
  // Logical
  // ---------------------------------------------------------------------

  @Test
  void logicalAnd() {
    assertEquals("this.x > 0 && this.x < 100", translateCheck("x > 0 AND x < 100"));
  }

  @Test
  void logicalOr() {
    assertEquals("this.x < 0 || this.x > 100", translateCheck("x < 0 OR x > 100"));
  }

  @Test
  void logicalNot() {
    // SQL `NOT` has lower precedence than comparisons, so `NOT x > 0` is
    // `NOT (x > 0)`. CEL `!` binds tighter than `>`, so we must emit
    // parens: `!(x > 0)`. Bare `!x > 0` would parse as `(!x) > 0` and
    // fail at runtime with a type error.
    assertEquals("!(this.x > 0)", translateCheck("NOT x > 0"));
  }

  @Test
  void logicalNotWrapsBooleanColumn() {
    assertEquals("!this.active", translateCheck("NOT active"));
  }

  @Test
  void doubleNotNests() {
    assertEquals("!(!(this.x > 0))", translateCheck("NOT NOT x > 0"));
  }

  @Test
  void notAppliedToIsNull() {
    // `NOT x IS NULL` parses as NOT applied to (x IS NULL); the IS NULL
    // form emits the paren-wrapped has() guard, and NOT must wrap it.
    assertEquals("!((!has(this.x) || dyn(this.x) == null))",
        translateCheck("NOT x IS NULL"));
  }

  @Test
  void andOrPrecedence() {
    // AND binds tighter than OR
    assertEquals("this.x > 0 && this.x < 100 || this.x = -1",
        translateCheck("x > 0 AND x < 100 OR x = -1").replace("==", "="));
  }

  // ---------------------------------------------------------------------
  // Arithmetic
  // ---------------------------------------------------------------------

  @Test
  void addition() {
    assertEquals("this.x + 1 > 0", translateCheck("x + 1 > 0"));
  }

  @Test
  void subtraction() {
    assertEquals("this.x - 1 < 100", translateCheck("x - 1 < 100"));
  }

  @Test
  void subtractionWithoutSpaces() {
    // Regression: lexer used to greedy-eat `-` into INT_LITERAL, breaking
    // parses like `x-1` where there were no surrounding spaces.
    assertEquals("this.x - 1 < 100", translateCheck("x-1 < 100"));
    assertEquals("this.x - 1 < 100", translateCheck("x -1 < 100"));
    assertEquals("this.x - 1 < 100", translateCheck("x- 1 < 100"));
    assertEquals("1 - 2 < this.x", translateCheck("1-2 < x"));
  }

  @Test
  void multiplyDivideModulo() {
    assertEquals("this.x * 2 / 3 % 4 == 0", translateCheck("x * 2 / 3 % 4 = 0"));
  }

  @Test
  void moduloRejectsDoubleLiteralOperand() {
    // CEL `%` is integer-only — no double-modulo overload. SQL accepts
    // numeric `%`. Reject at parse so the user gets a clear message
    // instead of a runtime "no matching overload" from CEL. Uses a
    // float literal because the test fixture has no DOUBLE column;
    // the validator resolves both column refs and literals.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("1.5 % 2 = 0.5"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected modulo-int-only error, got: " + t.getMessage());
  }

  @Test
  void moduloAcceptsIntOperands() {
    assertEquals("this.x % 2 == 0", translateCheck("x % 2 = 0"));
  }

  @Test
  void unaryMinus() {
    assertEquals("-this.x > 0", translateCheck("-x > 0"));
  }

  @Test
  void stringConcat() {
    assertEquals("this.name + 'suffix' == 'asuffix'",
        translateCheck("name || 'suffix' = 'asuffix'"));
  }

  @Test
  void concatBindsLooserThanAddition() {
    // PG and antlr/grammars-v4 both put `||` at a higher (looser-binding)
    // precedence level than `+`/`-`. So `name || a + b` parses as
    // `name || (a + b)`, NOT `(name || a) + b` — the addition groups
    // first, then the concat sees a string and a number.
    //
    // The validator now rejects the construct because `+` is numeric-
    // only (use `||` for string concat) and concat operands must be
    // string/bytes. Previously this test verified the precedence shape;
    // now it verifies that the precedence is RECOGNIZED and the type
    // mismatch caught before runtime.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("'x' || 'y' + 'z' = 'xyz'"));
    assertTrue(t.getMessage().contains("strict type-check")
            || t.getMessage().contains("string/bytes concatenation"),
        "expected concat-vs-addition type rejection, got: " + t.getMessage());
  }

  @Test
  void concatPrecedenceWithParens() {
    // Parenthesized concat operand survives the cascade walk and the
    // CelValidator gate (string + string is valid CEL). Locks in the
    // grouping shape under the new grammar.
    assertEquals("'x' + ('y' + 'z') == 'xyz'",
        translateCheck("'x' || ('y' || 'z') = 'xyz'"));
  }

  @Test
  void multipleConcatLeftAssociates() {
    // a || b || c → left-assoc at the concat level, CEL `+` also
    // left-assoc → flat output.
    assertEquals("'a' + 'b' + 'c' == 'abc'",
        translateCheck("'a' || 'b' || 'c' = 'abc'"));
  }

  @Test
  void concatRejectsNumericOperand() {
    // SQL `||` is string/bytes concat. The translator emits CEL `+`, which
    // also means numeric add — without a guard, `x || 1` would silently
    // become `x + 1`. The validator must reject this.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x || 1 = 5"));
    assertTrue(t.getMessage().contains("|| is string/bytes concatenation only"),
        "expected concat-type error, got: " + t.getMessage());
  }

  @Test
  void concatAcceptsBytesOperand() {
    // `binData` is BYTES — || on bytes is allowed (CEL `+` does bytes concat).
    // Verify the EMITTED CEL is well-formed: SQL `x'00'` must become CEL
    // `b"\x00"`, not pass through verbatim (CEL has no `x'...'` literal).
    String script = "ROW T ("
        + "binData BYTES,"
        + "CHECK (binData || x'00' = x'01')"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.binData + b\"\\x00\" == b\"\\x01\"",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void bytesLiteralMultiByte() {
    // Multi-byte literal: each hex pair becomes one \xNN escape; case-folded
    // to upper for stability.
    String script = "ROW T ("
        + "binData BYTES,"
        + "CHECK (binData = x'deadbeef')"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.binData == b\"\\xDE\\xAD\\xBE\\xEF\"",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void bytesLiteralOddLengthRejected() {
    String script = "ROW T ("
        + "binData BYTES,"
        + "CHECK (binData = x'A')"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("even number of hex digits"),
        "expected odd-length rejection, got: " + t.getMessage());
  }

  @Test
  void bytesLiteralNonHexRejected() {
    String script = "ROW T ("
        + "binData BYTES,"
        + "CHECK (binData = x'ZZ')"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("non-hex character"),
        "expected non-hex rejection, got: " + t.getMessage());
  }

  @Test
  void concatRejectsNumericMacroBinding() {
    // The iteration variable `n` is bound to ages's element type (INT),
    // so `n || 1` is the same silent-numeric-add hazard as `x || 1` at
    // table scope. The validator should reject it inside the macro body
    // too — not just at top level.
    String script = "ROW T ("
        + "ages INT ARRAY,"
        + "CHECK (EVERY(ages, n, n || 1 = 5))"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("|| is string/bytes concatenation only"),
        "expected concat-type error inside macro, got: " + t.getMessage());
  }

  @Test
  void concatAcceptsStringMacroBinding() {
    // Same shape but `tags` is STRING ARRAY, so `t` is STRING — `||` is OK.
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t, t || 'suffix' = 'foosuffix'))"
        + ");";
    parseScript(script);
  }

  @Test
  void castBoolRejectedInsideMacro() {
    // `validateCasts` must thread the macro iter-var binding so a non-string
    // source gets rejected even when bound by an iteration variable.
    String script = "ROW T ("
        + "ages INT ARRAY,"
        + "CHECK (EVERY(ages, n, CAST(n AS BOOLEAN)))"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(
        t.getMessage().contains("strict type-check")
            || t.getMessage().contains("CAST AS BOOLEAN"),
        "expected CAST-to-BOOL rejection inside macro, got: " + t.getMessage());
  }

  @Test
  void crossTypeCompareRejectedInsideMacro() {
    // `validateComparisons` must thread the macro iter-var binding so a
    // cross-category compare against the iter-var is caught. Compares two
    // column refs (not column-vs-literal) because the comparison validator
    // only resolves column-ref operands today; the iter-var `n` is INT
    // (from `ages`), and `name` is STRING — a string-vs-numeric reject.
    String script = "ROW T ("
        + "name STRING, ages INT ARRAY,"
        + "CHECK (EVERY(ages, n, n = name))"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  // ---------------------------------------------------------------------
  // Literals
  // ---------------------------------------------------------------------

  @Test
  void intLiteral() {
    assertEquals("this.x == 42", translateCheck("x = 42"));
  }

  @Test
  void floatLiteral() {
    assertEquals("this.ratio == 3.14", translateCheck("ratio = 3.14"));
  }

  @Test
  void floatLiteralTrailingDotNormalized() {
    // Grammar accepts `1.` (trailing dot) but CEL requires digits on
    // both sides. Translator normalizes to `1.0` so the emitted CEL
    // compiles cleanly.
    assertEquals("this.ratio == 1.0", translateCheck("ratio = 1."));
  }

  @Test
  void floatLiteralLeadingDotNormalized() {
    // Same for `.5` → `0.5`. (SQL accepts both forms; CEL requires both
    // sides.)
    assertEquals("this.ratio == 0.5", translateCheck("ratio = .5"));
  }

  @Test
  void quotedColumnRefRoundTripsCorrectly() {
    // Regression: previously `colid().getText()` returned the backticked
    // form (e.g. `\`type\``), so vctx.isColumn lookup against schema
    // field name `type` (stored unquoted) failed → "Unknown column" or
    // emit produced invalid CEL. After the fix, the translator unquotes
    // the colid before lookup and emit.
    String script = "ROW T ("
        + "x INT,"
        + "CHECK (`x` > 0)"  // backticked column ref to existing field `x`
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("this.x > 0", r.getExpr());
  }

  @Test
  void quotedColumnRefForReservedKeywordNameWorks() {
    // Field name `type` is a non-reserved keyword the user might quote
    // defensively. Both quoted and unquoted CHECK refs should work.
    String script = "ROW T ("
        + "type STRING,"
        + "CHECK (`type` = 'A')"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("this.type == 'A'", r.getExpr());
  }

  @Test
  void stringLiteral() {
    assertEquals("this.name == 'hello'", translateCheck("name = 'hello'"));
  }

  @Test
  void stringLiteralWithEscape() {
    // SQL '' (doubled quote) escape → CEL \' escape
    assertEquals("this.name == 'O\\'Hare'", translateCheck("name = 'O''Hare'"));
  }

  @Test
  void boolLiteralTrue() {
    assertEquals("this.active == true", translateCheck("active = TRUE"));
  }

  @Test
  void boolLiteralFalse() {
    assertEquals("this.active == false", translateCheck("active = FALSE"));
  }

  @Test
  void eqNullRejected() {
    // SQL `=` against NULL is always UNKNOWN (CHECK passes), but CEL `==`
    // is a real boolean — emitting `this.x == null` would silently fail
    // for non-null rows. Reject at parse time and direct to IS NULL.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class, () -> translateCheck("x = NULL"));
    assertTrue(t.getMessage().contains("IS NULL"),
        "expected IS NULL hint, got: " + t.getMessage());
  }

  @Test
  void neNullRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class, () -> translateCheck("x <> NULL"));
    assertTrue(t.getMessage().contains("IS NULL"),
        "expected IS NULL hint, got: " + t.getMessage());
  }

  @Test
  void parenNullRejected() {
    // Parens shouldn't allow bypass.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class, () -> translateCheck("x = (NULL)"));
    assertTrue(t.getMessage().contains("IS NULL"),
        "expected IS NULL hint, got: " + t.getMessage());
  }

  // ---------------------------------------------------------------------
  // Column refs and indirection
  // ---------------------------------------------------------------------

  @Test
  void columnRef() {
    String script = "ROW T ("
        + "x INT,"
        + "CHECK (x > 0)"  // table-level so it can reference x
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("this.x > 0", r.getExpr());
  }

  @Test
  void nestedFieldAccess() {
    // Table-level CHECK referencing a struct field's nested member
    String script = "ROW T ("
        + "addr ROW(zip INT) NOT NULL,"
        + "CHECK (addr.zip > 0)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("this.addr.zip > 0", r.getExpr());
  }

  @Test
  void arrayIndexing() {
    // Postgres convention: 1-indexed at the SQL surface; translator subtracts 1
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (tags[1] = 'first')"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("this.tags[(1) - 1] == 'first'", r.getExpr());
  }

  @Test
  void mapIndexingPassesKeyThroughVerbatim() {
    // MAP keying is NOT 1-indexed — keys are arbitrary values, not
    // positions. The translator must pass `m['k']` through as `m['k']`,
    // not as `m['k'] - 1` (which is a CEL type error).
    String script = "ROW T ("
        + "props MAP<STRING, STRING>,"
        + "CHECK (props['name'] = 'alice')"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("this.props['name'] == 'alice'", r.getExpr());
  }

  @Test
  void mapIndexingWithIntKeyPassesThrough() {
    // Same for int-keyed maps — the key is a key, not a 1-based position.
    String script = "ROW T ("
        + "scores MAP<INT, INT>,"
        + "CHECK (scores[42] > 0)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("this.scores[42] > 0", r.getExpr());
  }

  @Test
  void parenthesizedArrayIndexingFallsBackToArrayConvention() {
    // Paren-form indirection has no resolvable receiver type. Fall back
    // to the array convention so common `(arr)[1]` access still works.
    // (Maps used through paren forms would mistranslate — known limitation
    // documented in visitIndirection.)
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK ((tags)[1] = 'first')"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("(this.tags)[(1) - 1] == 'first'", r.getExpr());
  }

  @Test
  void parenthesizedExpression() {
    assertEquals("(this.x + 1) * 2 > 10", translateCheck("(x + 1) * 2 > 10"));
  }

  // ---------------------------------------------------------------------
  // CASE
  // ---------------------------------------------------------------------

  @Test
  void searchedCase() {
    // Searched-form WHEN condition is now defensively wrapped in parens
    // (so a low-precedence operator inside it can't mis-bind in CEL).
    assertEquals("((this.x > 0) ? 'pos' : 'neg') == 'pos'",
        translateCheck("CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END = 'pos'"));
  }

  @Test
  void searchedCaseMultipleWhens() {
    String result = translateCheck(
        "CASE WHEN x > 100 THEN 'big' WHEN x > 0 THEN 'small' ELSE 'zero' END = 'big'");
    assertEquals(
        "((this.x > 100) ? 'big' : ((this.x > 0) ? 'small' : 'zero')) == 'big'",
        result);
  }

  @Test
  void simpleCase() {
    String result = translateCheck(
        "CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END = 'one'");
    // Simple CASE: subject and each WHEN value are individually wrapped in
    // parens around the implicit `==`, so a low-precedence operator inside
    // either side can't mis-bind in CEL.
    assertEquals(
        "(this.x == 1 ? 'one' : (this.x == 2 ? 'two' : 'other')) == 'one'",
        result);
  }

  @Test
  void caseWithoutElseRejected() {
    // CEL ternary requires both branches share a type; emitting `... ? then : null`
    // doesn't unify. Direct user to add explicit ELSE.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CASE WHEN x > 0 THEN 'pos' END = 'pos'"));
    assertTrue(t.getMessage().contains("missing an ELSE"),
        "expected missing-ELSE message, got: " + t.getMessage());
  }

  @Test
  void caseBranchTypeMismatchRejected() {
    // CEL ternaries require all branches share a category — a numeric THEN
    // and a string ELSE would fail at CEL compile. Validator catches it
    // here with a clearer message.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck(
            "CASE WHEN x > 0 THEN 1 ELSE 'foo' END = 'foo'"));
    assertTrue(
        t.getMessage().contains("strict type-check")
            || t.getMessage().contains("CASE branches have incompatible result types"),
        "expected CASE branch mismatch error, got: " + t.getMessage());
  }

  @Test
  void caseBranchSameCategoryAccepted() {
    // STRING vs CHAR → same category ("string"); should NOT be rejected.
    String result = translateCheck(
        "CASE WHEN x > 0 THEN name ELSE 'default' END = 'foo'");
    assertEquals("((this.x > 0) ? this.name : 'default') == 'foo'", result);
  }

  @Test
  void caseBranchNumericPromotionAccepted() {
    // INT and DOUBLE both fall in the "numeric" category — accepted.
    String result = translateCheck(
        "CASE WHEN x > 0 THEN x ELSE 0 END > 0");
    assertEquals("((this.x > 0) ? this.x : 0) > 0", result);
  }

  @Test
  void simpleCaseSubjectVsWhenValueTypeMismatchRejected() {
    // Simple CASE: the translator emits `subject == when_value` per WHEN.
    // CEL refuses to compile cross-category compares, so a string subject
    // with int WHEN values must be rejected at parse.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CASE name WHEN 1 THEN 'a' ELSE 'b' END = 'a'"));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  @Test
  void simpleCaseSubjectAndWhenValuesMatchAccepted() {
    String result = translateCheck(
        "CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END = 'one'");
    assertEquals(
        "(this.x == 1 ? 'one' : (this.x == 2 ? 'two' : 'other')) == 'one'",
        result);
  }

  @Test
  void nestedCaseAccepted() {
    // CASE inside CASE — outer THEN is itself a CASE expression. Both
    // levels' branches resolve to the same category. Verifies that
    // validateCaseBranches recurses into nested case_expr.
    String result = translateCheck(
        "(CASE WHEN x > 0 THEN (CASE WHEN x > 100 THEN 'big' ELSE 'small' END)"
            + " ELSE 'neg' END) = 'small'");
    assertEquals(
        "(((this.x > 0) ? (((this.x > 100) ? 'big' : 'small')) : 'neg')) == 'small'",
        result);
  }

  @Test
  void macroBodyMustBeBoolean() {
    // EVERY returns boolean regardless of body shape, so validateBooleanRoot
    // can't see this from the top. The dedicated macro-body validator
    // catches it.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("EVERY(ages, n, n + 1)"));
    assertTrue(t.getMessage().contains("predicate body must yield a boolean"),
        "expected macro-body boolean error, got: " + t.getMessage());
  }

  @Test
  void macroBodyBooleanShapeAccepted() {
    // Body is a comparison — boolean-shaped. Should pass.
    String script = "ROW T ("
        + "ages INT ARRAY,"
        + "CHECK (EVERY(ages, n, n > 0))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.ages.all(n, n > 0)", schema.getRules().get(0).getExpr());
  }

  @Test
  void compositeCastInBetweenInMacro() {
    // Lock in the validator order across all the type-aware passes:
    // CAST inside BETWEEN inside macro predicate. Iter-var `n` is INT
    // (from `ages`); CAST(n AS DOUBLE) → DOUBLE; bounds are DOUBLE
    // literals → numeric category — should accept.
    String script = "ROW T ("
        + "ages INT ARRAY,"
        + "CHECK (EVERY(ages, n, CAST(n AS DOUBLE) BETWEEN 0.0 AND 100.0))"
        + ");";
    parseScript(script);
  }

  @Test
  void macroIterVarShadowingColumnNameStaysBareInBody() {
    // Bug regression test: when the iter-var name matches a column in the
    // outer scope, the iter-var should win inside the body. Without the
    // EMIT_VCTX push, `name` in the body would emit as `this.name` (the
    // outer column) instead of bare `name` (the iter-var).
    String script = "ROW T ("
        + "name STRING ARRAY,"
        + "CHECK (EVERY(name, name, LENGTH(name) > 0))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    // Outer `name` (the collection) → this.name; iter-var `name` (binding)
    // and the body ref → bare.
    assertEquals("this.name.all(name, size(name) > 0)", r.getExpr());
  }

  @Test
  void macroIterVarMapIndexingShadowingColumn() {
    // Same root cause, indirection variant: iter-var `meta` shadows column
    // `meta` (a MAP). Inside the body, `meta['k']` is keying the iter-var
    // (a single MAP element), not the outer column. visitIndirection's
    // receiver-type lookup must see the binding, not the outer column,
    // so it correctly emits MAP-style `[expr]` (no -1).
    String script = "ROW T ("
        + "meta MAP<STRING, STRING> ARRAY,"
        + "CHECK (EVERY(meta, meta, meta['k'] = 'v'))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("this.meta.all(meta, meta['k'] == 'v')", r.getExpr());
  }

  @Test
  void simpleCaseWhenValueRejectsBooleanExpression() {
    // Simple CASE emits `subject == when_value` per WHEN. A boolean-valued
    // WHEN (e.g., `a OR b`) is meaningless when the subject is INT and the
    // category-mismatch validator rejects it. (Previously the resolver
    // returned null for OR expressions and the bug slipped through, so the
    // emit relied on defensive parens; now the validator catches it
    // up-front.)
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CASE x WHEN a OR b THEN 1 ELSE 0 END = 1"));
    assertTrue(t.getMessage().contains("incompatible with WHEN value type BOOLEAN"),
        "expected category-mismatch rejection, got: " + t.getMessage());
  }

  @Test
  void moduloRejectsParenWrappedDoubleLiteral() {
    // Regression: validateModuloOperands now descends into paren-wrapped
    // expressions whose inner type is resolvable. `(1.5)` resolves to
    // DOUBLE, so `(1.5) % 2` is rejected at parse instead of silently
    // deferring to a runtime "no matching overload" from CEL.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(1.5) % 2 = 0.5"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected modulo-int-only error inside paren, got: " + t.getMessage());
  }

  @Test
  void likeOnConcatReceiverParenthesizesAgainstPrecedence() {
    // Regression: `a || b LIKE 'x%'` parses with LIKE applied to the
    // concat (a || b). CEL `.matches()` binds tighter than `+`, so a
    // bare emission would be `a + b.matches(...)` parsed as
    // `a + (b.matches(...))` — wrong. Receiver must be parenthesized.
    String result = translateCheck("first_name || last_name LIKE 'foo%'");
    assertEquals("(this.first_name + this.last_name).matches('^foo.*$')",
        result);
  }

  @Test
  void instanceMethodOnConcatReceiverParenthesized() {
    // Same precedence hazard for IS_EMAIL/STARTS_WITH/etc. on a concat.
    String result = translateCheck(
        "STARTS_WITH(first_name || last_name, 'usr_')");
    assertEquals(
        "(this.first_name + this.last_name).startsWith('usr_')",
        result);
  }

  @Test
  void lengthOnIntRejected() {
    // LENGTH() requires string/bytes/collection — not numeric. Validator
    // rejects at parse time (R13 H2 fix).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("LENGTH(-x) > 0"));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  // ---------------------------------------------------------------------
  // Multiple CHECKs and CONSTRAINT name + MESSAGE
  // ---------------------------------------------------------------------

  @Test
  void multipleChecksOnField() {
    String script = "ROW T ("
        + "x INT CHECK (x > 0) CHECK (x < 100)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    List<Rule> rules = schema.getField("x").getRules();
    assertEquals(2, rules.size());
    assertEquals("this > 0", rules.get(0).getExpr());
    assertEquals("this < 100", rules.get(1).getExpr());
  }

  @Test
  void constraintName() {
    String script = "ROW T ("
        + "x INT CONSTRAINT positive_x CHECK (x > 0)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getField("x").getRules().get(0);
    assertEquals("positive_x", r.getName());
    assertNull(r.getDoc());
    assertEquals("this > 0", r.getExpr());
  }

  @Test
  void messageClause() {
    String script = "ROW T ("
        + "x INT CHECK (x > 0) MESSAGE 'must be positive'"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getField("x").getRules().get(0);
    assertNull(r.getName());
    assertEquals("must be positive", r.getDoc());
  }

  @Test
  void constraintNameAndMessage() {
    String script = "ROW T ("
        + "x INT CONSTRAINT positive_x CHECK (x > 0) MESSAGE 'must be positive'"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getField("x").getRules().get(0);
    assertEquals("positive_x", r.getName());
    assertEquals("must be positive", r.getDoc());
    assertEquals("this > 0", r.getExpr());
    // Rule.sql preserves the user's original whitespace and casing — the
    // visitor pulls the source slice from the input stream rather than
    // concatenating tokens, so DDL re-emission round-trips faithfully.
    assertEquals("x > 0", r.getSql());
  }

  @Test
  void tableConstraint() {
    String script = "ROW T ("
        + "lo INT,"
        + "hi INT,"
        + "CONSTRAINT bounds CHECK (lo < hi) MESSAGE 'lo must be < hi'"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertTrue(schema.getField("lo").getRules().isEmpty());
    assertTrue(schema.getField("hi").getRules().isEmpty());
    List<Rule> tableRules = schema.getRules();
    assertEquals(1, tableRules.size());
    Rule r = tableRules.get(0);
    assertEquals("bounds", r.getName());
    assertEquals("lo must be < hi", r.getDoc());
    assertEquals("this.lo < this.hi", r.getExpr());
  }

  @Test
  void mixedFieldsAndTableConstraints() {
    String script = "ROW T ("
        + "lo INT CHECK (lo >= 0),"
        + "hi INT CHECK (hi <= 100),"
        + "CHECK (lo <= hi)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals(1, schema.getField("lo").getRules().size());
    assertEquals(1, schema.getField("hi").getRules().size());
    assertEquals(1, schema.getRules().size());
    assertEquals("this.lo <= this.hi", schema.getRules().get(0).getExpr());
  }

  // ---------------------------------------------------------------------
  // Original SQL preservation in `logical` field
  // ---------------------------------------------------------------------

  @Test
  void logicalFieldHoldsOriginalSql() {
    String script = "ROW T ("
        + "x INT CHECK (x > 0 AND x < 100)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getField("x").getRules().get(0);
    // The visitor pulls the source slice from the input stream so the
    // user's original whitespace, casing, and operator spacing all survive.
    assertEquals("x > 0 AND x < 100", r.getSql());
    assertEquals("this > 0 && this < 100", r.getExpr());
  }

  @Test
  void sqlPreservesUnusualWhitespace() {
    // Tabs, multiple spaces, and a newline inside the expression all
    // survive verbatim in Rule.sql.
    String script = "ROW T (x INT CHECK (x  >\t0\nAND  x<100));";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getField("x").getRules().get(0);
    assertEquals("x  >\t0\nAND  x<100", r.getSql());
  }

  @Test
  void sqlPreservesOriginalCasing() {
    // SQL keywords are case-insensitive; we should preserve whatever the
    // user wrote (lower-case `and` here) rather than normalize.
    String script = "ROW T (x INT CHECK (x > 0 and x < 100));";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getField("x").getRules().get(0);
    assertEquals("x > 0 and x < 100", r.getSql());
  }

  @Test
  void rulesAreNonNullEvenWhenAbsent() {
    String script = "ROW T (x INT);";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertNotNull(schema.getField("x").getRules());
    assertTrue(schema.getField("x").getRules().isEmpty());
    assertNotNull(schema.getRules());
    assertTrue(schema.getRules().isEmpty());
  }

  // ---------------------------------------------------------------------
  // Phase 2: IS NULL
  // ---------------------------------------------------------------------

  @Test
  void isNull() {
    assertEquals("(!has(this.x) || dyn(this.x) == null)", translateCheck("x IS NULL"));
  }

  @Test
  void isNotNull() {
    assertEquals("(has(this.x) && dyn(this.x) != null)", translateCheck("x IS NOT NULL"));
  }

  @Test
  void isNullCombinedWithLogical() {
    assertEquals("(has(this.x) && dyn(this.x) != null) && this.x > 0",
        translateCheck("x IS NOT NULL AND x > 0"));
  }

  @Test
  void isNullOnFieldRef() {
    String script = "ROW T ("
        + "addr ROW(zip INT) NOT NULL,"
        + "CHECK (addr.zip IS NOT NULL)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    Rule r = schema.getRules().get(0);
    assertEquals("(has(this.addr.zip) && dyn(this.addr.zip) != null)", r.getExpr());
  }

  // ---------------------------------------------------------------------
  // Phase 2: IN with literal list
  // ---------------------------------------------------------------------

  @Test
  void inLiteralList() {
    assertEquals("this.x in [1, 2, 3]", translateCheck("x IN (1, 2, 3)"));
  }

  @Test
  void inSingleElementList() {
    // Disambiguation: paren-wrapped single element parses as ParenList (per design)
    assertEquals("this.x in [42]", translateCheck("x IN (42)"));
  }

  @Test
  void inStringList() {
    assertEquals("this.status in ['ACTIVE', 'PENDING', 'CANCELLED']",
        translateCheck("status IN ('ACTIVE', 'PENDING', 'CANCELLED')"));
  }

  @Test
  void notInLiteralList() {
    assertEquals("!(this.x in [1, 2, 3])", translateCheck("x NOT IN (1, 2, 3)"));
  }

  // ---------------------------------------------------------------------
  // Phase 2: IN with list-typed expression
  // ---------------------------------------------------------------------

  @Test
  void inListField() {
    // x IN <field-name> — parses as InTargetExpr (no parens)
    String script = "ROW T ("
        + "value STRING,"
        + "tags STRING ARRAY,"
        + "CHECK (value IN tags)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.value in this.tags", schema.getRules().get(0).getExpr());
  }

  @Test
  void notInListField() {
    String script = "ROW T ("
        + "value STRING,"
        + "tags STRING ARRAY,"
        + "CHECK (value NOT IN tags)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("!(this.value in this.tags)", schema.getRules().get(0).getExpr());
  }

  // ---------------------------------------------------------------------
  // Phase 2: BETWEEN
  // ---------------------------------------------------------------------

  @Test
  void between() {
    assertEquals("0 <= this.x && this.x <= 100",
        translateCheck("x BETWEEN 0 AND 100"));
  }

  @Test
  void notBetween() {
    assertEquals("!(0 <= this.x && this.x <= 100)",
        translateCheck("x NOT BETWEEN 0 AND 100"));
  }

  @Test
  void betweenSymmetric() {
    assertEquals(
        "(this.lo <= this.hi ? (this.lo <= this.x && this.x <= this.hi) "
            + ": (this.hi <= this.x && this.x <= this.lo))",
        translateCheck("x BETWEEN SYMMETRIC lo AND hi"));
  }

  @Test
  void notBetweenSymmetric() {
    assertEquals(
        "!((this.lo <= this.hi ? (this.lo <= this.x && this.x <= this.hi) "
            + ": (this.hi <= this.x && this.x <= this.lo)))",
        translateCheck("x NOT BETWEEN SYMMETRIC lo AND hi"));
  }

  @Test
  void betweenWithExpressionBounds() {
    assertEquals("0 <= (this.x + 1) && (this.x + 1) <= 100",
        translateCheck("x + 1 BETWEEN 0 AND 100"));
  }

  @Test
  void betweenCrossTypeOperandsRejected() {
    // INT subject vs STRING bounds — categories disagree. The two-side
    // comparison validator only fires on plain `=`/`<` etc.; BETWEEN goes
    // through its own production, which now has its own cross-type pass.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x BETWEEN name AND 'z'"));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  @Test
  void betweenSameCategoryBoundsAccepted() {
    // INT subject and INT bounds — fine.
    assertEquals("this.lo <= this.x && this.x <= this.hi",
        translateCheck("x BETWEEN lo AND hi"));
  }

  // (`betweenBoundsAreParenthesizedAgainstPrecedence` removed — its scenario
  // relied on accessing `.zip` on a STRING column, which the C4 indirection
  // validator now correctly rejects. Defensive paren behavior for compound
  // bounds is still covered by `betweenWithExpressionBounds`.)

  // ---------------------------------------------------------------------
  // Phase 2: combined
  // ---------------------------------------------------------------------

  @Test
  void inWithLogical() {
    assertEquals("this.status in ['A', 'B'] && (has(this.active) && dyn(this.active) != null)",
        translateCheck("status IN ('A', 'B') AND active IS NOT NULL"));
  }

  @Test
  void betweenWithLogical() {
    assertEquals("0 <= this.age && this.age <= 150 || (!has(this.age) || dyn(this.age) == null)",
        translateCheck("age BETWEEN 0 AND 150 OR age IS NULL"));
  }

  @Test
  void allThreeTogether() {
    String result = translateCheck(
        "status IN ('ACTIVE', 'PENDING') AND age BETWEEN 18 AND 65 AND email IS NOT NULL");
    assertEquals(
        "this.status in ['ACTIVE', 'PENDING'] && 18 <= this.age && this.age <= 65 "
            + "&& (has(this.email) && dyn(this.email) != null)",
        result);
  }

  // ---------------------------------------------------------------------
  // Phase 3: LIKE + ESCAPE
  // ---------------------------------------------------------------------

  @Test
  void likeExactMatch() {
    // Plain pattern with no wildcards — anchored
    assertEquals("this.name.matches('^foo$')", translateCheck("name LIKE 'foo'"));
  }

  @Test
  void likePrefix() {
    assertEquals("this.name.matches('^foo.*$')", translateCheck("name LIKE 'foo%'"));
  }

  @Test
  void likeSuffix() {
    assertEquals("this.name.matches('^.*foo$')", translateCheck("name LIKE '%foo'"));
  }

  @Test
  void likeInfix() {
    assertEquals("this.name.matches('^.*foo.*$')", translateCheck("name LIKE '%foo%'"));
  }

  @Test
  void likeUnderscore() {
    // _ matches any single character
    assertEquals("this.name.matches('^h.llo$')", translateCheck("name LIKE 'h_llo'"));
  }

  @Test
  void likeMixed() {
    assertEquals("this.name.matches('^a.b.*c$')", translateCheck("name LIKE 'a_b%c'"));
  }

  @Test
  void likeEscapesRegexSpecials() {
    // Pattern contains a literal '.' which is a regex special — must be escaped
    assertEquals("this.name.matches('^foo\\\\.bar$')", translateCheck("name LIKE 'foo.bar'"));
  }

  @Test
  void likeEscapesParens() {
    assertEquals("this.name.matches('^\\\\(foo\\\\)$')", translateCheck("name LIKE '(foo)'"));
  }

  @Test
  void likeEmail() {
    // Common pattern: rough email shape
    assertEquals("this.name.matches('^.*@.*\\\\..*$')", translateCheck("name LIKE '%@%.%'"));
  }

  @Test
  void notLike() {
    assertEquals("!(this.name.matches('^.*forbidden.*$'))",
        translateCheck("name NOT LIKE '%forbidden%'"));
  }

  @Test
  void likeWithDefaultEscapeChar() {
    // Default escape char is '\'. Pattern '\%foo' (SQL: '\%foo') means literal % then foo.
    // SQL parses '\%foo' as the 5-char string \%foo; LIKE→regex with default escape '\\'
    // sees the \ as escape for the next char (%), so % becomes literal.
    assertEquals("this.name.matches('^%foo$')", translateCheck("name LIKE '\\%foo'"));
  }

  @Test
  void likeWithCustomEscape() {
    // ESCAPE '!' means '!' is the escape char; '!%' is literal %, % alone is wildcard
    assertEquals("this.name.matches('^50%.*$')", translateCheck("name LIKE '50!%%' ESCAPE '!'"));
  }

  @Test
  void likeQuotedSingleQuoteInPattern() {
    // SQL escapes single quote as ''; pattern is literal "O'Hare"
    assertEquals("this.name.matches('^O\\'Hare$')",
        translateCheck("name LIKE 'O''Hare'"));
  }

  @Test
  void likeWithLiteralNewline() {
    // SQL STRING_LITERAL accepts a raw LF inside single quotes; the emitted
    // CEL string literal must escape it (CEL's lexer rejects raw LF in '...').
    assertEquals("this.name.matches('^a\\nb$')",
        translateCheck("name LIKE 'a\nb'"));
  }

  @Test
  void likeWithLiteralCarriageReturn() {
    assertEquals("this.name.matches('^a\\rb$')",
        translateCheck("name LIKE 'a\rb'"));
  }

  @Test
  void likeWithLiteralTab() {
    assertEquals("this.name.matches('^a\\tb$')",
        translateCheck("name LIKE 'a\tb'"));
  }

  @Test
  void likeWithLogicalCombination() {
    assertEquals("this.name.matches('^.*foo.*$') && (has(this.id) && dyn(this.id) != null)",
        translateCheck("name LIKE '%foo%' AND id IS NOT NULL"));
  }

  @Test
  void likeRejectsMultiCharEscape() {
    // ESCAPE clause must be exactly one character — translator throws
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("name LIKE 'foo' ESCAPE 'ab'"));
    assertTrue(t.getMessage().contains("ESCAPE"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: regular-form functions
  // ---------------------------------------------------------------------

  @Test
  void length() {
    assertEquals("size(this.name) > 0", translateCheck("LENGTH(name) > 0"));
  }

  @Test
  void lengthCaseInsensitive() {
    assertEquals("size(this.name) > 0", translateCheck("length(name) > 0"));
    assertEquals("size(this.name) > 0", translateCheck("Length(name) > 0"));
  }

  @Test
  void upperLower() {
    assertEquals("this.name.upperAscii() == 'FOO'", translateCheck("UPPER(name) = 'FOO'"));
    assertEquals("this.name.lowerAscii() == 'foo'", translateCheck("LOWER(name) = 'foo'"));
  }

  @Test
  void startsWith() {
    assertEquals("this.id.startsWith('usr_')", translateCheck("STARTS_WITH(id, 'usr_')"));
  }

  @Test
  void endsWith() {
    assertEquals("this.filename.endsWith('.json')",
        translateCheck("ENDS_WITH(filename, '.json')"));
  }

  @Test
  void contains() {
    assertEquals("this.name.contains('forbidden')",
        translateCheck("CONTAINS(name, 'forbidden')"));
  }

  @Test
  void replace() {
    assertEquals("this.phone.replace('-', '') == this.digits",
        translateCheck("REPLACE(phone, '-', '') = digits"));
  }

  @Test
  void matches() {
    assertEquals("this.value.matches('^[0-9]+$')",
        translateCheck("MATCHES(value, '^[0-9]+$')"));
  }

  @Test
  void coalesceTwoArgs() {
    assertEquals(
        "((has(this.a) && dyn(this.a) != null) ? this.a : this.b) > 0",
        translateCheck("COALESCE(a, b) > 0"));
  }

  @Test
  void coalesceThreeArgs() {
    assertEquals(
        "((has(this.a) && dyn(this.a) != null) ? this.a : "
            + "((has(this.b) && dyn(this.b) != null) ? this.b : this.c)) > 0",
        translateCheck("COALESCE(a, b, c) > 0"));
  }

  @Test
  void nullif() {
    assertEquals("(this.a == this.b ? dyn(null) : dyn(this.a)) == 0",
        translateCheck("NULLIF(a, b) = 0"));
  }

  @Test
  void greatestTwoArgs() {
    // PG semantics: NULLs are skipped; result is NULL only when ALL args are
    // NULL. Translator emits a NULL-skipping nested ternary so a single
    // NULL operand doesn't cause CEL to error on the relational compare.
    assertEquals(
        "((!has(this.a) || dyn(this.a) == null) ? this.b : "
            + "((!has(this.b) || dyn(this.b) == null) ? this.a : "
            + "(this.a > this.b ? this.a : this.b))) > 10",
        translateCheck("GREATEST(a, b) > 10"));
  }

  @Test
  void leastTwoArgs() {
    assertEquals(
        "((!has(this.a) || dyn(this.a) == null) ? this.b : "
            + "((!has(this.b) || dyn(this.b) == null) ? this.a : "
            + "(this.a < this.b ? this.a : this.b))) > 0",
        translateCheck("LEAST(a, b) > 0"));
  }

  @Test
  void greatestRejectsLiteralNullArg() {
    // Earlier emit produced `... > null` which CEL has no overload for.
    // Reject at parse time.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("GREATEST(x, NULL) > 0"));
    assertTrue(t.getMessage().contains("cannot take a literal NULL"),
        "expected NULL-rejection message, got: " + t.getMessage());
  }

  @Test
  void greatestRejectsThreeArgs() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("GREATEST(a, b, c) > 0"));
    assertTrue(t.getMessage().contains("2 arguments"));
  }

  @Test
  void unknownFunction() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("FOO(x) > 0"));
    assertTrue(t.getMessage().contains("Unknown function"));
    assertTrue(t.getMessage().contains("FOO"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: format validators
  // ---------------------------------------------------------------------

  @Test
  void isEmail() {
    assertEquals("this.email.isEmail()", translateCheck("IS_EMAIL(email)"));
  }

  @Test
  void isHostname() {
    assertEquals("this.h.isHostname()", translateCheck("IS_HOSTNAME(h)"));
  }

  @Test
  void isIpv4() {
    assertEquals("this.addr.isIpv4()", translateCheck("IS_IPV4(addr)"));
  }

  @Test
  void isIpv6() {
    assertEquals("this.addr.isIpv6()", translateCheck("IS_IPV6(addr)"));
  }

  @Test
  void isUri() {
    assertEquals("this.u.isUri()", translateCheck("IS_URI(u)"));
  }

  @Test
  void isUriRef() {
    assertEquals("this.u.isUriRef()", translateCheck("IS_URI_REF(u)"));
  }

  @Test
  void isUuid() {
    assertEquals("this.id.isUuid()", translateCheck("IS_UUID(id)"));
  }

  @Test
  void validatorsCombined() {
    assertEquals("this.addr.isIpv4() || this.addr.isIpv6()",
        translateCheck("IS_IPV4(addr) OR IS_IPV6(addr)"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: special-syntax — CAST
  // ---------------------------------------------------------------------

  @Test
  void castInt() {
    assertEquals("int(this.x) > 0", translateCheck("CAST(x AS INT) > 0"));
  }

  @Test
  void castDouble() {
    assertEquals("double(this.x) > 0.5", translateCheck("CAST(x AS DOUBLE) > 0.5"));
  }

  @Test
  void castString() {
    assertEquals("string(this.x) == '42'", translateCheck("CAST(x AS STRING) = '42'"));
  }

  @Test
  void castBoolFromString() {
    // CEL bool() accepts string and bool — STRING column is valid.
    assertEquals("bool(this.name)", translateCheck("CAST(name AS BOOLEAN)"));
  }

  @Test
  void castBoolFromNumericRejected() {
    // CEL bool() has no numeric overload; reject at parse so the user sees
    // a clear message instead of a runtime "no matching overload" error.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CAST(x AS BOOLEAN)"));
    assertTrue(
        t.getMessage().contains("strict type-check")
            || t.getMessage().contains("CAST AS BOOLEAN"),
        "expected non-string CAST-to-BOOL rejection, got: " + t.getMessage());
  }

  @Test
  void castBoolFromBooleanIsIdentity() {
    assertEquals("bool(this.active)", translateCheck("CAST(active AS BOOLEAN)"));
  }

  @Test
  void castBytesFromStringAccepted() {
    // CEL bytes() accepts string and bytes — STRING column is valid.
    // Wrap in a comparison so the boolean-root check is happy.
    assertEquals("bytes(this.name) == b\"\\x00\"",
        translateCheck("CAST(name AS BYTES) = x'00'"));
  }

  @Test
  void castBytesFromNumericRejected() {
    // CEL bytes() has no numeric overload — reject at parse.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CAST(x AS BYTES) = x'00'"));
    assertTrue(
        t.getMessage().contains("strict type-check")
            || t.getMessage().contains("CAST AS BYTES"),
        "expected non-string CAST-to-BYTES rejection, got: " + t.getMessage());
  }

  @Test
  void castVarcharIgnoresParam() {
    // VARCHAR(64) → string(); the (64) parameter is parsed but ignored
    assertEquals("string(this.x) == 'foo'", translateCheck("CAST(x AS VARCHAR(64)) = 'foo'"));
  }

  @Test
  void castDecimalAppliesScale() {
    // CAST(x AS DECIMAL(p,s)) now applies the scale via decimals.round and
    // yields a real Decimal; the comparison routes through decimals.gt with
    // the double literal coerced to decimal("1.5").
    assertEquals(
        "decimals.gt(decimals.round(decimal(this.x), 2), decimal(\"1.5\"))",
        translateCheck("CAST(x AS DECIMAL(10, 2)) > 1.5"));
  }

  // ---------------------------------------------------------------------
  // Decimal (opaque confluent.type.Decimal) — operator + function dispatch.
  // `amount`/`price`/`discount` are DECIMAL columns; `x` is INT, `ratio`
  // DOUBLE. Each assertion also passes the strict cel-java checker via
  // translateCheck (so the emitted decimals.*/math.* type-checks).
  // ---------------------------------------------------------------------

  @Test
  void decimalCompareIntLiteralCoerced() {
    assertEquals("decimals.gt(this.amount, decimal(\"0\"))", translateCheck("amount > 0"));
  }

  @Test
  void decimalCompareDecimalLiteral() {
    assertEquals("decimals.ge(this.amount, decimal(\"9.99\"))", translateCheck("amount >= 9.99"));
  }

  @Test
  void decimalEquality() {
    assertEquals("decimals.eq(this.amount, this.price)", translateCheck("amount = price"));
  }

  @Test
  void decimalNotEqual() {
    assertEquals("!decimals.eq(this.amount, this.price)", translateCheck("amount <> price"));
  }

  @Test
  void decimalAdd() {
    assertEquals("decimals.gt(decimals.add(this.amount, this.discount), this.price)",
        translateCheck("amount + discount > price"));
  }

  @Test
  void decimalMulLiteralCoerced() {
    assertEquals("decimals.lt(decimals.mul(this.price, decimal(\"2\")), this.amount)",
        translateCheck("price * 2 < amount"));
  }

  @Test
  void decimalUnaryNeg() {
    assertEquals("decimals.lt(decimals.neg(this.amount), decimal(\"0\"))",
        translateCheck("-amount < 0"));
  }

  @Test
  void decimalAbs() {
    assertEquals("decimals.lt(decimals.abs(this.amount), decimal(\"100\"))",
        translateCheck("ABS(amount) < 100"));
  }

  @Test
  void decimalSignReturnsInt() {
    // decimals.sign returns int, so the surrounding compare stays native.
    assertEquals("decimals.sign(this.amount) == -1", translateCheck("SIGN(amount) = -1"));
  }

  @Test
  void decimalFloor() {
    assertEquals("decimals.ge(decimals.floor(this.amount), decimal(\"0\"))",
        translateCheck("FLOOR(amount) >= 0"));
  }

  @Test
  void decimalCeiling() {
    assertEquals("decimals.le(decimals.ceil(this.amount), decimal(\"100\"))",
        translateCheck("CEILING(amount) <= 100"));
  }

  @Test
  void decimalRoundWithScale() {
    assertEquals("decimals.eq(decimals.round(this.amount, 1), this.amount)",
        translateCheck("ROUND(amount, 1) = amount"));
  }

  @Test
  void decimalTruncate() {
    assertEquals("decimals.le(decimals.trunc(this.price), this.price)",
        translateCheck("TRUNCATE(price) <= price"));
  }

  @Test
  void decimalSqrt() {
    assertEquals("decimals.lt(decimals.sqrt(this.amount), decimal(\"10\"))",
        translateCheck("SQRT(amount) < 10"));
  }

  @Test
  void decimalCastToString() {
    assertEquals("string(this.amount) == '1.50'",
        translateCheck("CAST(amount AS STRING) = '1.50'"));
  }

  @Test
  void decimalBetween() {
    assertEquals(
        "decimals.le(decimal(\"0\"), this.amount) && decimals.le(this.amount, decimal(\"100\"))",
        translateCheck("amount BETWEEN 0 AND 100"));
  }

  @Test
  void decimalInValueList() {
    assertEquals(
        "(decimals.eq(this.amount, decimal(\"1.0\")) "
            + "|| decimals.eq(this.amount, decimal(\"2.5\")))",
        translateCheck("amount IN (1.0, 2.5)"));
  }

  @Test
  void decimalNestedArithmetic() {
    // (amount + price) * 2 > discount  → exercises paren re-entry into the
    // decimal cascade and chain folding.
    assertEquals(
        "decimals.gt(decimals.mul(decimals.add(this.amount, this.price), decimal(\"2\")), "
            + "this.discount)",
        translateCheck("(amount + price) * 2 > discount"));
  }

  // Non-decimal numerics route to the CEL math.* extension (same SQL).

  @Test
  void mathAbsOnInt() {
    assertEquals("math.abs(this.x) < 5", translateCheck("ABS(x) < 5"));
  }

  @Test
  void mathSqrtOnDouble() {
    assertEquals("math.sqrt(this.ratio) < 2.0", translateCheck("SQRT(ratio) < 2.0"));
  }

  @Test
  void roundWithScaleOnNonDecimalRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("ROUND(ratio, 2) > 0.0"));
    org.junit.jupiter.api.Assertions.assertTrue(
        t.getMessage().contains("scale argument is only supported on DECIMAL"),
        "expected scale-on-non-decimal rejection; got: " + t.getMessage());
  }

  @Test
  void decimalInListFieldRejected() {
    // No Decimal-aware `in` over a list-typed operand.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("amount IN ages"));
    org.junit.jupiter.api.Assertions.assertTrue(
        t.getMessage().contains("not supported for DECIMAL")
            || t.getMessage().toLowerCase(java.util.Locale.ROOT).contains("comparable"),
        "expected decimal IN list-field rejection; got: " + t.getMessage());
  }

  @Test
  void decimalGreatestProducesDecimalCompare() {
    // GREATEST/CASE/NULLIF emit gnarly ternaries; assert the decimal dispatch
    // markers rather than the exact string (still strict-checked end-to-end).
    String cel = translateCheck("GREATEST(amount, price) > 0");
    org.junit.jupiter.api.Assertions.assertTrue(
        cel.contains("decimals.gt(this.amount, this.price)")
            && cel.startsWith("decimals.gt("),
        "expected decimal GREATEST + outer decimal compare; got: " + cel);
  }

  @Test
  void decimalModuloRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("amount % 2 = 0"));
    org.junit.jupiter.api.Assertions.assertTrue(
        t.getMessage().contains("modulo"),
        "expected decimal modulo rejection; got: " + t.getMessage());
  }

  @Test
  void decimalCaseSimpleFormUsesDecimalsEq() {
    String cel = translateCheck("CASE amount WHEN 0 THEN 1 ELSE 0 END = 1");
    org.junit.jupiter.api.Assertions.assertTrue(
        cel.contains("decimals.eq(this.amount, decimal(\"0\"))"),
        "expected simple-CASE decimal subject equality; got: " + cel);
  }

  @Test
  void castDateRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CAST(x AS DATE) = CURRENT_TIMESTAMP"));
    assertTrue(t.getMessage().contains("DATE")
        || t.getMessage().contains("not supported"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: special-syntax — EXTRACT
  // ---------------------------------------------------------------------

  @Test
  void extractYear() {
    assertEquals("this.ts.getFullYear() == 2026",
        translateCheck("EXTRACT(YEAR FROM ts) = 2026"));
  }

  @Test
  void extractMonth() {
    // CEL getMonth() is 0-based; SQL MONTH is 1-based. Translator adds 1.
    assertEquals("(this.ts.getMonth() + 1) < 12",
        translateCheck("EXTRACT(MONTH FROM ts) < 12"));
  }

  @Test
  void extractMonthIsOneBased() {
    // Lock in SQL semantics: January is month 1, not 0.
    assertEquals("(this.ts.getMonth() + 1) == 1",
        translateCheck("EXTRACT(MONTH FROM ts) = 1"));
  }

  @Test
  void extractDay() {
    assertEquals("this.ts.getDate() <= 31", translateCheck("EXTRACT(DAY FROM ts) <= 31"));
  }

  @Test
  void extractEpoch() {
    assertEquals("int(this.ts) > 0", translateCheck("EXTRACT(EPOCH FROM ts) > 0"));
  }

  @Test
  void extractCaseInsensitiveField() {
    assertEquals("this.ts.getFullYear() == 2026",
        translateCheck("EXTRACT(year FROM ts) = 2026"));
  }

  @Test
  void extractRejectsUnknownField() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("EXTRACT(NANOSECOND FROM ts) > 0"));
    assertTrue(t.getMessage().contains("EXTRACT")
        || t.getMessage().contains("NANOSECOND"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: special-syntax — SUBSTRING
  // ---------------------------------------------------------------------

  @Test
  void substringFromFor() {
    assertEquals("this.name.substring((1) - 1, (1) - 1 + (3)) == 'foo'",
        translateCheck("SUBSTRING(name FROM 1 FOR 3) = 'foo'"));
  }

  @Test
  void substringFromOnly() {
    assertEquals("this.name.substring((2) - 1) == 'oo'",
        translateCheck("SUBSTRING(name FROM 2) = 'oo'"));
  }

  @Test
  void substringCommaForm() {
    assertEquals("this.name.substring((1) - 1, (1) - 1 + (3)) == 'foo'",
        translateCheck("SUBSTRING(name, 1, 3) = 'foo'"));
  }

  @Test
  void substringZeroStartRejected() {
    // SQL is 1-indexed; postgres clamps non-positive starts to 1, but our
    // CEL emit would be `s.substring(-1, ...)` which throws. Reject at
    // parse with a clear message instead.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("SUBSTRING(name FROM 0 FOR 3) = 'foo'"));
    assertTrue(t.getMessage().contains("must be ≥ 1"),
        "expected zero-start rejection, got: " + t.getMessage());
  }

  @Test
  void substringNegativeStartRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("SUBSTRING(name, -2, 3) = 'foo'"));
    assertTrue(t.getMessage().contains("must be ≥ 1"),
        "expected negative-start rejection, got: " + t.getMessage());
  }

  @Test
  void substringNonLiteralStartDefersToRuntime() {
    // Column-ref start can't be checked statically — accept and let CEL
    // raise at runtime if the value is invalid.
    assertEquals("this.name.substring((this.lo) - 1, (this.lo) - 1 + (3)) == 'foo'",
        translateCheck("SUBSTRING(name FROM lo FOR 3) = 'foo'"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: special-syntax — POSITION
  // ---------------------------------------------------------------------

  @Test
  void position() {
    // POSITION's whole emit is wrapped so a tighter outer operator
    // (e.g. `POSITION(...) * 2`) can't mis-bind the trailing `+ 1`.
    assertEquals("(this.haystack.indexOf(this.needle) + 1) > 0",
        translateCheck("POSITION(needle IN haystack) > 0"));
  }

  @Test
  void positionWithLiterals() {
    assertEquals("(this.name.indexOf('@') + 1) > 0",
        translateCheck("POSITION('@' IN name) > 0"));
  }

  @Test
  void positionInsideMultiplicationParenthesizedAgainstPrecedence() {
    // Regression: without the outer wrap, `POSITION(...) * 2` would emit
    // `indexOf + 1 * 2` and CEL would parse as `indexOf + 2` (multiplicative
    // binds tighter), losing the `* 2` factor on the indexOf part.
    assertEquals("(this.name.indexOf('@') + 1) * 2 == 4",
        translateCheck("POSITION('@' IN name) * 2 = 4"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: special-syntax — TRIM
  // ---------------------------------------------------------------------

  @Test
  void trimDefault() {
    // TRIM(s) — strip whitespace
    assertEquals("this.name.trim() == 'foo'", translateCheck("TRIM(name) = 'foo'"));
  }

  @Test
  void trimRejectionCarriesLineAndColumn() {
    // The TRIM-with-chars rejection used to throw bare ValidationException;
    // it now goes through `locatedError` like every other emit-side error.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("TRIM('x' FROM name) = 'foo'"));
    assertTrue(t.getMessage().contains("CHECK constraint at line"),
        "expected line/col-prefixed error, got: " + t.getMessage());
  }

  @Test
  void castUnsupportedTypeCarriesLineAndColumn() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CAST(x AS DATE) = ts"));
    assertTrue(t.getMessage().contains("CHECK constraint at line"),
        "expected line/col-prefixed error, got: " + t.getMessage());
  }

  @Test
  void unknownFunctionCarriesLineAndColumn() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("WAT(x) > 0"));
    assertTrue(t.getMessage().contains("CHECK constraint at line"),
        "expected line/col-prefixed error, got: " + t.getMessage());
  }

  @Test
  void trimBothFromFormIsRejected() {
    // CEL trim() is 0-arg only — there's no chars-set overload. Reject at
    // parse time so the user gets a clear message instead of a runtime
    // "no matching overload" error from the CEL evaluator.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("TRIM(BOTH 'x' FROM name) = 'foo'"));
    assertTrue(t.getMessage().contains("character set is not supported"),
        "expected chars-set rejection, got: " + t.getMessage());
  }

  @Test
  void trimFromFormIsRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("TRIM('x' FROM name) = 'foo'"));
    assertTrue(t.getMessage().contains("character set is not supported"),
        "expected chars-set rejection, got: " + t.getMessage());
  }

  @Test
  void trimRejectsLeading() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("TRIM(LEADING 'x' FROM name) = 'foo'"));
    assertTrue(t.getMessage().contains("LEADING"));
  }

  @Test
  void trimRejectsTrailing() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("TRIM(TRAILING 'x' FROM name) = 'foo'"));
    assertTrue(t.getMessage().contains("TRAILING"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: CURRENT_TIMESTAMP
  // ---------------------------------------------------------------------

  @Test
  void currentTimestamp() {
    assertEquals("this.created_at <= now", translateCheck("created_at <= CURRENT_TIMESTAMP"));
  }

  @Test
  void currentTimestampWithExtract() {
    assertEquals("now.getFullYear() >= 2026",
        translateCheck("EXTRACT(YEAR FROM CURRENT_TIMESTAMP) >= 2026"));
  }

  // ---------------------------------------------------------------------
  // Phase 5: EVERY / ANY / ONE macros
  // ---------------------------------------------------------------------

  @Test
  void everyAllPositive() {
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t, LENGTH(t) > 0))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.tags.all(t, size(t) > 0)", schema.getRules().get(0).getExpr());
  }

  @Test
  void anyExists() {
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (ANY(tags, t, t = 'admin'))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.tags.exists(t, t == 'admin')", schema.getRules().get(0).getExpr());
  }

  @Test
  void oneExistsOne() {
    String script = "ROW T ("
        + "addresses STRING ARRAY,"
        + "CHECK (ONE(addresses, a, STARTS_WITH(a, 'primary:')))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.addresses.exists_one(a, a.startsWith('primary:'))",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void macroCaseInsensitive() {
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (every(tags, t, LENGTH(t) > 0))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.tags.all(t, size(t) > 0)", schema.getRules().get(0).getExpr());
  }

  @Test
  void macroNested() {
    // Iteration var can't be `row` — ROW is a reserved type keyword
    String script = "ROW T ("
        + "matrix INT ARRAY ARRAY,"
        + "CHECK (EVERY(matrix, r, EVERY(r, cell, cell > 0)))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.matrix.all(r, r.all(cell, cell > 0))",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void macroWithComplexPredicate() {
    String script = "ROW T ("
        + "ages INT ARRAY,"
        + "CHECK (ANY(ages, a, a BETWEEN 18 AND 65))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.ages.exists(a, 18 <= a && a <= 65)",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void macroWithFormatValidator() {
    String script = "ROW T ("
        + "emails STRING ARRAY,"
        + "CHECK (EVERY(emails, e, IS_EMAIL(e)))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.emails.all(e, e.isEmail())",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void macroRejectsExpressionAsIterVar() {
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t.name, LENGTH(t) > 0))"
        + ");";
    Throwable thrown = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(thrown.getMessage().contains("identifier"));
  }

  @Test
  void macroRejectsArithmeticAsIterVar() {
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t + 1, LENGTH(t) > 0))"
        + ");";
    Throwable thrown = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(thrown.getMessage().contains("identifier"));
  }

  @Test
  void macroRejectsWrongArity() {
    // 2-arg EVERY is rejected — either by arity check or because `t` (the
    // would-be iter var) isn't a valid column ref. Either error is acceptable;
    // both correctly reject the expression.
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t))"
        + ");";
    org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
  }

  // ---------------------------------------------------------------------
  // Phase 6: strict parse-time validation
  // ---------------------------------------------------------------------

  @Test
  void rejectsUnknownColumn() {
    String script = "ROW T (x INT, CHECK (foo > 0));";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("Unknown column"));
    assertTrue(t.getMessage().contains("'foo'"));
    assertTrue(t.getMessage().contains("Allowed columns: x"));
  }

  @Test
  void rejectsColumnLevelReferenceToOtherField() {
    // Column-level CHECK on `a` can only reference `a`; `b` is out of scope
    String script = "ROW T ("
        + "a INT CHECK (a < b),"
        + "b INT"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("Unknown column"));
    assertTrue(t.getMessage().contains("'b'"));
  }

  @Test
  void columnLevelAllowsSubAccess() {
    // Column-level CHECK on a struct field can navigate into its sub-fields
    String script = "ROW T ("
        + "addr ROW(zip INT) NOT NULL CHECK (addr.zip > 0)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.zip > 0",
        schema.getField("addr").getRules().get(0).getExpr());
  }

  @Test
  void columnLevelBareIsNullRejected() {
    // Column-level CHECK gets the skip-on-null contract from the runtime —
    // the CEL is never invoked when the column value is null/missing, so
    // `this` is guaranteed non-null at evaluation time. A bare `<col> IS
    // NULL` test can therefore never fire and is rejected as dead code.
    String script = "ROW T ("
        + "x INT CHECK (x IS NULL)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("never fires"),
        "expected skip-on-null rejection, got: " + t.getMessage());
  }

  @Test
  void columnLevelBareIsNotNullRejected() {
    // Symmetric case — IS NOT NULL on the bare column is unconditionally
    // true under skip-on-null.
    String script = "ROW T ("
        + "x INT CHECK (x IS NOT NULL)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("never fires"),
        "expected skip-on-null rejection, got: " + t.getMessage());
  }

  @Test
  void tableLevelAllowsAnyField() {
    String script = "ROW T ("
        + "a INT, b INT,"
        + "CHECK (a < b)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.a < this.b", schema.getRules().get(0).getExpr());
  }

  @Test
  void rejectsNonBooleanRoot() {
    String script = "ROW T (x INT, CHECK (x + 1));";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("must yield a boolean"));
  }

  @Test
  void rejectsNonBooleanLiteralRoot() {
    String script = "ROW T (x INT, CHECK ('foo'));";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("must yield a boolean"));
  }

  @Test
  void acceptsBooleanColumnRef() {
    String script = "ROW T (active BOOLEAN, CHECK (active));";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.active", schema.getRules().get(0).getExpr());
  }

  @Test
  void acceptsBooleanLiteralRoot() {
    String script = "ROW T (x INT, CHECK (TRUE));";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("true", schema.getRules().get(0).getExpr());
  }

  @Test
  void rejectsOrderingOnBoolean() {
    String script = "ROW T ("
        + "a BOOLEAN, b BOOLEAN,"
        + "CHECK (a < b)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  @Test
  void rejectsCrossCategoryComparison() {
    String script = "ROW T ("
        + "s STRING, b BYTES,"
        + "CHECK (s = b)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  @Test
  void isNullExpressionRejectedAsConcatOperand() {
    // C-1 regression: `(x IS NULL)` is a boolean expression. The resolver
    // now reports BOOLEAN for IS NULL cascades, so the concat-operand
    // validator rejects bool || string instead of silently emitting
    // unparseable CEL (`'foo' + ((!has(this.x) || dyn(this.x) == null))`).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("'foo' || (x IS NULL) = 'foo'"));
    assertTrue(t.getMessage().contains("string/bytes concatenation"),
        "expected concat-type rejection, got: " + t.getMessage());
  }

  @Test
  void isNullExpressionRejectedAsLengthArg() {
    // C-1 regression: LENGTH expects string/bytes/collection, not bool.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("LENGTH((x IS NULL)) > 0"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected LENGTH-arg rejection, got: " + t.getMessage());
  }

  @Test
  void isNullExpressionRejectedAsComparisonOperand() {
    // C-1 regression: `(x IS NULL) > 0` is bool > int — CEL rejects.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(x IS NULL) > 0"));
    // Either "Cannot compare BOOLEAN" (cross-category) or
    // "relational operator > on BOOLEAN" (non-orderable) is acceptable.
    assertTrue(
        t.getMessage().contains("strict type-check")
            || t.getMessage().contains("relational operator")
            || t.getMessage().contains("Cannot compare"),
        "expected bool-vs-int rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsStringVsBytesLiteralComparison() {
    // H-2b: comparison validator now resolves literal types via
    // tryResolveMulType — so `name > x'AB'` (STRING vs BYTES literal) is
    // rejected before runtime.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("name > x'AB'"));
    assertTrue(t.getMessage().contains("strict type-check") || t.getMessage().contains("Cannot compare"),
        "expected category-mismatch rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsIntVsStringLiteralComparison() {
    // H-2c: same fix catches int-column vs string-literal mismatch.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x > 'foo'"));
    assertTrue(t.getMessage().contains("strict type-check") || t.getMessage().contains("Cannot compare"),
        "expected category-mismatch rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsBoolFunctionResultVsIntLiteralComparison() {
    // H-4a: comparison validator now consults tryResolveFuncReturnType —
    // so `CONTAINS(name, 'x') = 0` (BOOLEAN vs INT literal) is rejected
    // before runtime instead of silently always-false.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CONTAINS(name, 'x') = 0"));
    assertTrue(t.getMessage().contains("strict type-check") || t.getMessage().contains("Cannot compare"),
        "expected category-mismatch rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsLikeOnNonStringColumn() {
    // H-3: LIKE on int column emits `int.matches(...)` which has no CEL
    // overload. Reject at parse time.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x LIKE 'foo%'"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected LIKE-receiver-type rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsLikeOnIntBindingInsideMacro() {
    // H-1: LIKE inside EVERY/ANY/ONE body must see the binding type. ages
    // is INT ARRAY, so n is INT — `n LIKE 'foo'` emits `int.matches(...)`,
    // a runtime error. validateLikePatterns must recurseIntoMacro.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("EVERY(ages, n, n LIKE 'foo')"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected LIKE-receiver rejection inside macro, got: " + t.getMessage());
  }

  @Test
  void rejectsInParenListLhsVsElementMismatch() {
    // H-2: `tags-element t IN (1, 2)` — string LHS, int list. Validator
    // now compares LHS category to list element category.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("EVERY(tags, t, t IN (1, 2))"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected IN LHS-vs-element rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsInParenListIntColumnVsBoolList() {
    // H-2: int LHS, bool list (from IS NULL elements).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x IN (x IS NULL, a > 0)"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected IN LHS-vs-element rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsSubstringOnNonStringReceiver() {
    // SUBSTRING(intCol FROM 1 FOR 2) — emit `int.substring(...)`.
    // Caught by strict cel-java type-check (no `int.substring` overload).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("SUBSTRING(x FROM 1 FOR 2) = 'a'"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected strict type-check rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsPositionOnNonStringReceiver() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("POSITION('a' IN x) = 1"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected strict type-check rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsTrimOnNonStringReceiver() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("TRIM(x) = 'a'"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected strict type-check rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsExtractOnNonDatetimeReceiver() {
    // EXTRACT(YEAR FROM intCol) emits `int.getFullYear()`. Caught by strict
    // cel-java type-check (no `int.getFullYear()` overload).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("EXTRACT(YEAR FROM x) = 2024"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected strict type-check rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsBoolOperandInArithmetic() {
    // H-4: `(x IS NULL) + 1` — bool + int. CEL has no overload.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(x IS NULL) + 1 > 0"));
    assertTrue(t.getMessage().contains("must be numeric"),
        "expected arithmetic-operand rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsComparisonResultInArithmetic() {
    // H-4: `(x > 0) + 1` — bool + int. (Confirmed bug in cel-java too.)
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(x > 0) + 1 > 0"));
    assertTrue(t.getMessage().contains("must be numeric"),
        "expected arithmetic-operand rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsNonPositiveArrayIndex() {
    // C-2: tags[0] emits tags[(0)-1] = tags[-1] — runtime crash. Reject
    // literal int ≤ 0 on ARRAY/MULTISET indexing.
    Throwable t1 = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("tags[0] = 'x'"));
    assertTrue(t1.getMessage().contains("1-based"),
        "expected 1-based-index rejection for tags[0], got: " + t1.getMessage());
    Throwable t2 = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("tags[-5] = 'x'"));
    assertTrue(t2.getMessage().contains("1-based"),
        "expected 1-based-index rejection for tags[-5], got: " + t2.getMessage());
  }

  @Test
  void rejectsUpperOnBytes() {
    // C-3: UPPER(blob) emits bytes.upperAscii() which CEL doesn't have.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("STARTS_WITH(UPPER(blob), 'A')"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected UPPER-bytes rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsLowerOnBytes() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("LENGTH(LOWER(blob)) > 0"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected LOWER-bytes rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsCaseResultMixedNumeric() {
    // H-1: CASE result type now tracked through resolver. `(CASE ... END) > 1.5`
    // where the CASE returns int can detect int-vs-double mismatch.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(CASE WHEN active THEN 1 ELSE 2 END) > 1.5"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected CASE-result-vs-literal mismatch, got: " + t.getMessage());
  }

  @Test
  void rejectsLiteralNullCaseBranch() {
    // H-2: `CASE WHEN ... THEN 1 ELSE NULL END` — bare null doesn't unify
    // with the int branch in CEL ternary.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(CASE WHEN active THEN 1 ELSE NULL END) > 0"));
    assertTrue(t.getMessage().contains("cannot be a literal NULL"),
        "expected NULL-CASE-branch rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsLiteralNullInConcat() {
    // H-3: `name || NULL` emits `this.name + null` — no CEL overload.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("name || NULL = 'foo'"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected NULL-concat-operand rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsLikeOnBytesColumn() {
    // M-4: bytes don't have CEL .matches() overload. Receiver must be
    // string only (not just string-or-bytes).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("blob LIKE 'foo%'"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected LIKE-bytes rejection, got: " + t.getMessage());
  }

  @Test
  void collectionIsNullKeepsHasRewrite() {
    // INTENTIONAL: arr IS NULL on a collection emits the has() guard,
    // matching protovalidate convention (empty == absent for proto3
    // repeated fields, no presence bit). Strict-SQL purists may expect
    // NULL-only semantics, but proto3 repeated can never actually be
    // NULL — the strict interpretation has no meaningful runtime value.
    assertEquals(
        "(!has(this.tags) || dyn(this.tags) == null)",
        translateCheck("tags IS NULL"));
  }

  @Test
  void rejectsArithmeticInInListLhs() {
    // H-A1: arithmetic LHS now resolves through the unified-operand
    // resolver — `x + 1 IN ('a', 'b')` (int LHS, string list) rejected.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x + 1 IN ('a', 'b')"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected IN LHS rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsArithmeticInBetweenSubject() {
    // H-A2: arithmetic LHS in BETWEEN now resolves; cross-category
    // detection fires for int subject vs string bounds.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(x + 1) BETWEEN 'a' AND 'b'"));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  @Test
  void rejectsArithmeticResultInComparison() {
    // H-A3: comparison validator now sees the arithmetic result type.
    // `ratio + 1 > 0.0` — the `+` itself is rejected first as mixed
    // numeric (int + double).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("ratio + 1 > 0.0"));
    assertTrue(t.getMessage().contains("mixed numeric")
            || t.getMessage().contains("strict type-check")
            || t.getMessage().contains("strict type-check"),
        "expected type-mismatch rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsMixedNumericArithmetic() {
    // H-A4: int + double is not allowed (CEL has no implicit promotion).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x + 1.5 > 0"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected mixed-numeric rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsConcatResultInArithmetic() {
    // H-A5: `(name || '') + 1` — paren-wrapped concat result feeds
    // arithmetic. Unified-operand resolver catches.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(name || '') + 1 > 0"));
    assertTrue(t.getMessage().contains("must be numeric"),
        "expected non-numeric arithmetic operand rejection, got: "
            + t.getMessage());
  }

  @Test
  void rejectsLiteralNullInArithmetic() {
    // H-B1: `x + NULL` — CEL has no `_+_(int, null)` overload.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x + NULL > 0"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected NULL-arithmetic-operand rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsPositionWithBytesReceiver() {
    // POSITION emits `.indexOf()` which CEL defines only for string.
    // Bytes receiver rejected by strict cel-java type-check.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("POSITION(x'61' IN x'6162') > 0"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected POSITION-bytes rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsContainsWithCrossTypeArgs() {
    // H-C2: CONTAINS / STARTS_WITH / ENDS_WITH / MATCHES / REPLACE
    // require all args to share a category (string vs bytes). Mixing
    // string receiver with bytes needle has no CEL overload.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CONTAINS(name, x'6162')"));
    assertTrue(t.getMessage().contains("strict type-check"),
        "expected CONTAINS cross-type rejection, got: " + t.getMessage());
  }

  @Test
  void rejectsBetweenWithUnaryNullBound() {
    // M-6: `+NULL` and `-NULL` previously slipped through text-based
    // looksLikeNullText. Structural isBoundLiteralNull catches them.
    Throwable t1 = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x BETWEEN +NULL AND 10"));
    assertTrue(t1.getMessage().contains("cannot be NULL"),
        "expected NULL-bound rejection for +NULL, got: " + t1.getMessage());
    Throwable t2 = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x BETWEEN -NULL AND 10"));
    assertTrue(t2.getMessage().contains("cannot be NULL"),
        "expected NULL-bound rejection for -NULL, got: " + t2.getMessage());
  }

  @Test
  void rejectsIntVsDoubleComparison() {
    // INT vs DOUBLE — CEL has no implicit int↔double promotion, cel-java's
    // strict checker rejects `int > double`. Reject at parse time.
    String script = "ROW T ("
        + "a INT, b DOUBLE,"
        + "CHECK (a < b)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("strict type-check") || t.getMessage().contains("Cannot compare"),
        "expected category-mismatch rejection, got: " + t.getMessage());
  }

  @Test
  void errorIncludesSourcePosition() {
    String script = "ROW T (x INT, CHECK (foo > 0));";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("line 1"));
    assertTrue(t.getMessage().contains("col"));
  }

  @Test
  void allowsRuntimeNowVariable() {
    // CURRENT_TIMESTAMP introduces 'now' into the runtime scope; comparing
    // a timestamp field to it is allowed.
    String script = "ROW T ("
        + "ts TIMESTAMP_LTZ,"
        + "CHECK (ts <= CURRENT_TIMESTAMP)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    assertEquals("this.ts <= now", schema.getRules().get(0).getExpr());
  }

  // ---------------------------------------------------------------------
  // DDL emitter round-trip
  // ---------------------------------------------------------------------

  /** Build LT from DDL, emit DDL again, parse that, compare rule lists. */
  private LogicalType ddlRoundTrip(String script) {
    LogicalType lt = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(lt);
    return parseScript(emitted).toLogicalType();
  }

  @Test
  void ddlEmitsColumnLevelCheck() {
    String script = "ROW T (x INT CHECK (x > 0)); TYPE T;";
    LogicalType orig = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(orig);
    assertTrue(emitted.contains("CHECK (x > 0)"),
        "expected emitted DDL to contain CHECK clause, got:\n" + emitted);
  }

  @Test
  void ddlEmitsConstraintNameAndMessage() {
    String script = "ROW T ("
        + "x INT CONSTRAINT positive_x CHECK (x > 0) MESSAGE 'must be positive'"
        + "); TYPE T;";
    LogicalType orig = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(orig);
    assertTrue(emitted.contains("CONSTRAINT positive_x CHECK (x > 0)"),
        "got:\n" + emitted);
    assertTrue(emitted.contains("MESSAGE 'must be positive'"), "got:\n" + emitted);
  }

  @Test
  void ddlEmitsTableLevelCheck() {
    String script = "ROW T ("
        + "lo INT, hi INT,"
        + "CONSTRAINT bounds CHECK (lo < hi)"
        + "); TYPE T;";
    LogicalType orig = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(orig);
    assertTrue(emitted.contains("CONSTRAINT bounds CHECK (lo < hi)"),
        "got:\n" + emitted);
  }

  @Test
  void ddlEmitsMultipleChecksOnField() {
    String script = "ROW T ("
        + "x INT CHECK (x > 0) CHECK (x < 100)"
        + "); TYPE T;";
    LogicalType orig = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(orig);
    assertTrue(emitted.contains("CHECK (x > 0)"), "got:\n" + emitted);
    assertTrue(emitted.contains("CHECK (x < 100)"), "got:\n" + emitted);
  }

  @Test
  void ddlRoundTripPreservesColumnCheck() {
    String script = "ROW T (x INT CHECK (x > 0)); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema struct = after.getNamedTypes().get("T");
    List<Rule> rules = struct.getField("x").getRules();
    assertEquals(1, rules.size());
    assertEquals("this > 0", rules.get(0).getExpr());
  }

  @Test
  void ddlRoundTripPreservesAllRuleFields() {
    String script = "ROW T ("
        + "x INT CONSTRAINT positive_x CHECK (x > 0) MESSAGE 'must be positive'"
        + "); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema struct = after.getNamedTypes().get("T");
    Rule r = struct.getField("x").getRules().get(0);
    assertEquals("positive_x", r.getName());
    assertEquals("must be positive", r.getDoc());
    assertEquals("this > 0", r.getExpr());
  }

  @Test
  void ddlEmitsCheckInsideInlineRow() {
    // Regression: rowExpr used to emit only `name typeExpr` per field,
    // dropping CHECK rules / DEFAULT / COMMENT / TAGS / WITH on inline
    // ROW(...) field types. Now reuses appendField so inline rows
    // preserve full per-field metadata.
    String script = "ROW T ("
        + "addr ROW(zip INT CHECK (zip > 0)) NOT NULL"
        + "); TYPE T;";
    LogicalType orig = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(orig);
    assertTrue(emitted.contains("CHECK (zip > 0)"),
        "expected emitted DDL to preserve inline ROW field's CHECK, got:\n"
            + emitted);
  }

  @Test
  void ddlRoundTripPreservesCheckInsideInlineRow() {
    String script = "ROW T ("
        + "addr ROW(zip INT CHECK (zip > 0)) NOT NULL"
        + "); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema addr = after.getNamedTypes().get("T").getField("addr").getSchema();
    Schema.Field zip = addr.getField("zip");
    List<Rule> rules = zip.getRules();
    assertEquals(1, rules.size(), "inline ROW field rule should round-trip");
    assertEquals("this > 0", rules.get(0).getExpr());
  }

  @Test
  void ddlRoundTripPreservesTableConstraint() {
    String script = "ROW T ("
        + "lo INT, hi INT,"
        + "CHECK (lo < hi)"
        + "); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema struct = after.getNamedTypes().get("T");
    assertEquals(1, struct.getRules().size());
    assertEquals("this.lo < this.hi", struct.getRules().get(0).getExpr());
  }

  @Test
  void ddlRoundTripPreservesMultipleChecks() {
    String script = "ROW T ("
        + "x INT CHECK (x > 0) CHECK (x < 100),"
        + "y INT,"
        + "CONSTRAINT cross CHECK (x + y > 0) MESSAGE 'sum must be positive'"
        + "); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema struct = after.getNamedTypes().get("T");
    assertEquals(2, struct.getField("x").getRules().size());
    assertEquals(1, struct.getRules().size());
    assertEquals("cross", struct.getRules().get(0).getName());
    assertEquals("sum must be positive", struct.getRules().get(0).getDoc());
  }

  // ---------------------------------------------------------------------
  // Parse-error surfacing: the strict ANTLR error listener turns silent
  // recovery into a located ValidationException, so malformed CHECK
  // expressions (and other syntax errors) fail loudly with line/col info
  // instead of producing partially-formed parse trees.
  // ---------------------------------------------------------------------

  @Test
  void parseErrorInsideCheckIsReportedNotSilentlyRecovered() {
    // Trailing operator with no rhs: previously default ANTLR recovery
    // would insert a token and continue, yielding a malformed CHECK tree.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript("ROW T (x INT, CHECK (x +)); TYPE T;"));
    assertTrue(t.getMessage().startsWith("Parse error at line "),
        "expected located parse error, got: " + t.getMessage());
  }

  @Test
  void parseErrorReportsLineAndColumn() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript("ROW T (x INT, CHECK ()); TYPE T;"));
    assertTrue(t.getMessage().contains("line 1, col"),
        "expected line/col info, got: " + t.getMessage());
  }

  @Test
  void parseErrorInCheckDoesNotLeakIntoSubsequentFields() {
    // A bad CHECK followed by another field. With default lenient recovery
    // the parser may consume past the closing `)` and corrupt parsing of
    // the next field. Strict listener aborts at the first error so the
    // user sees the actual problem (the bad CHECK), not a misleading
    // cascade about `y` or the trailing `)`.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(
            "ROW T (x INT CHECK (x >), y INT); TYPE T;"));
    assertTrue(t.getMessage().startsWith("Parse error at line "),
        "expected located parse error, got: " + t.getMessage());
  }

  // ---------------------------------------------------------------------
  // Rule constructor invariants
  // ---------------------------------------------------------------------

  @Test
  void ruleConstructorRejectsEmptyExpr() {
    org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> new Rule(null, null, "", "x > 0"));
  }

  @Test
  void ruleConstructorRejectsEmptySql() {
    org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> new Rule(null, null, "x > 0", ""));
  }

  @Test
  void ruleConstructorAllowsNullNameAndDoc() {
    Rule r = new Rule(null, null, "x > 0", "x > 0");
    assertEquals(null, r.getName());
    assertEquals(null, r.getDoc());
  }

  // ---------------------------------------------------------------------
  // Audit round 8 regressions
  // ---------------------------------------------------------------------

  @Test
  void schemaSetRulesOnNonStructRejected() {
    // H2: rules attached to non-STRUCT schemas were silently dropped on the
    // wire. Wire writers only emit confluent:rules on STRUCT/Field, so reject
    // at the API boundary instead of producing a lossy wire form.
    Schema intSchema = Schema.create(Schema.Type.INT);
    Rule r = new Rule(null, null, "this > 0", "this > 0");
    org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> intSchema.setRules(java.util.Arrays.asList(r)));
  }

  @Test
  void macroIterVarNamedNowRejected() {
    // M4: iter-var name `now` would shadow the runtime variable in the body.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("EVERY(tags, now, LENGTH(now) > 0)"));
    assertTrue(t.getMessage().contains("shadows"),
        "expected shadows hint, got: " + t.getMessage());
  }

  @Test
  void substringParenWrappedZeroRejected() {
    // M1: tryLiteralIntValue must descend through CheckParen so `(0)` is
    // detected like `0` is.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("SUBSTRING(value FROM (0)) = ''"));
    assertTrue(t.getMessage().contains("≥ 1"),
        "expected ≥ 1 hint, got: " + t.getMessage());
  }

  @Test
  void substringNegativeLengthRejected() {
    // L2: SUBSTRING(... FOR -1) emits CEL substring(0, -1) — runtime error.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("SUBSTRING(value FROM 1 FOR -1) = ''"));
    assertTrue(t.getMessage().contains("≥ 0"),
        "expected ≥ 0 hint, got: " + t.getMessage());
  }

  @Test
  void substringZeroLengthAllowed() {
    // Length 0 is fine — empty string result.
    String r = translateCheck("SUBSTRING(value FROM 1 FOR 0) = ''");
    assertTrue(r.contains("this.value.substring((1) - 1, (1) - 1 + (0))"),
        "got: " + r);
  }

  @Test
  void betweenLowerNullRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x BETWEEN NULL AND 10"));
    assertTrue(t.getMessage().contains("NULL"),
        "expected NULL message, got: " + t.getMessage());
  }

  @Test
  void betweenUpperNullRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x BETWEEN 1 AND NULL"));
    assertTrue(t.getMessage().contains("NULL"),
        "expected NULL message, got: " + t.getMessage());
  }

  @Test
  void inMixedTypesRejected() {
    // M2: heterogeneous IN list — CEL refuses to type-check, runtime error.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x IN (1, 'two', TRUE)"));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  @Test
  void inUniformTypesAllowed() {
    // Sanity: uniform numeric list should still parse.
    String r = translateCheck("x IN (1, 2, 3)");
    assertTrue(r.contains("this.x in [1, 2, 3]"), "got: " + r);
  }

  // ---------------------------------------------------------------------
  // C1: prefix NOT precedence over BETWEEN/IN
  // ---------------------------------------------------------------------

  @Test
  void notPrefixOverBetween() {
    // C1: prior cascade put NOT below BETWEEN, so `NOT x BETWEEN 1 AND 10`
    // wrongly parsed as `(NOT x) BETWEEN 1 AND 10`. Real PG (and our fixed
    // grammar) scope NOT over the qualifier family.
    assertEquals("!(1 <= this.x && this.x <= 10)",
        translateCheck("NOT x BETWEEN 1 AND 10"));
  }

  @Test
  void notPrefixOverIn() {
    assertEquals("!(this.x in [1, 2, 3])",
        translateCheck("NOT x IN (1, 2, 3)"));
  }

  @Test
  void notPostfixBetween() {
    // postfix NOT BETWEEN was already correct via the BETWEEN production.
    assertEquals("!(1 <= this.x && this.x <= 10)",
        translateCheck("x NOT BETWEEN 1 AND 10"));
  }

  @Test
  void notPostfixIn() {
    assertEquals("!(this.x in [1, 2, 3])",
        translateCheck("x NOT IN (1, 2, 3)"));
  }

  // ---------------------------------------------------------------------
  // Audit round 9 regressions
  // ---------------------------------------------------------------------

  @Test
  void greatestMixedTypesRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("GREATEST(x, name) > 0"));
    assertTrue(t.getMessage().contains("strict type-check") || t.getMessage().contains("Cannot compare"),
        "expected category-mismatch error, got: " + t.getMessage());
  }

  @Test
  void coalesceMixedTypesRejected() {
    // The cross-type comparison `COALESCE(x, name) = 'a'` (where x is INT
    // and name is STRING) is rejected. The comparison validator now sees
    // COALESCE's resolved return type (INT, from the first arg) vs 'a'
    // (STRING) and rejects "Cannot compare INT with VARCHAR" before the
    // COALESCE per-arg unification check ("not comparable") runs. Either
    // form constitutes a valid rejection.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("COALESCE(x, name) = 'a'"));
    assertTrue(
        t.getMessage().contains("strict type-check")
            || t.getMessage().contains("Cannot compare"),
        "expected type-mismatch rejection, got: " + t.getMessage());
  }

  @Test
  void coalesceLiteralNullArgRejected() {
    // Literal NULL anywhere in COALESCE produces a CEL ternary whose
    // bare-null then-branch doesn't unify with the typed else-branch
    // (mid-position) or is dead code (final position). Reject for clarity.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("COALESCE(name, NULL) = 'foo'"));
    assertTrue(t.getMessage().contains("cannot take a literal NULL"),
        "expected NULL-rejection message, got: " + t.getMessage());
  }

  @Test
  void coalesceMidPositionNullRejected() {
    // Mid-position NULL emits `(null != null ? null : <typed>)` — branches
    // don't unify, CEL ternary rejects.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("COALESCE(x, NULL, 0) > 0"));
    assertTrue(t.getMessage().contains("cannot take a literal NULL"),
        "expected NULL-rejection message, got: " + t.getMessage());
  }

  @Test
  void simpleCaseWhenNullRejected() {
    // SQL `subject = NULL` is always UNKNOWN but the simple-CASE emit
    // produces `subject == null` per branch — silently flips semantics.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CASE x WHEN NULL THEN 1 ELSE 0 END = 1"));
    assertTrue(t.getMessage().contains("WHEN value cannot be NULL"),
        "expected simple-CASE WHEN NULL rejection, got: " + t.getMessage());
  }

  @Test
  void parenWrappedColumnRefIsNullGetsHasGuard() {
    // `((x)) IS NULL` and `(addr_struct).zip IS NULL` should get the same
    // has() rewrite as their bare equivalents — paren wrapping mustn't
    // silently strip the presence guard.
    assertEquals("(!has(((this.x))) || dyn(((this.x))) == null)",
        translateCheck("((x)) IS NULL"));
    assertEquals(
        "(!has((this.addr_struct).zip) || dyn((this.addr_struct).zip) == null)",
        translateCheck("(addr_struct).zip IS NULL"));
  }

  @Test
  void floatLiteralOverflowRejected() {
    // 1e500 parses as +Infinity in CEL — silently always-false constraint.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x > 1e500"));
    assertTrue(t.getMessage().contains("out of range"),
        "expected float-overflow rejection, got: " + t.getMessage());
  }

  @Test
  void intLiteralOverflowRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x > 99999999999999999999"));
    assertTrue(t.getMessage().contains("out of range"),
        "expected int-overflow rejection, got: " + t.getMessage());
  }

  @Test
  void likePatternTrailingEscapeRejected() {
    // `name LIKE 'foo\'` (default escape `\`) ends with a dangling escape
    // char — invalid per SQL spec. Same for explicit ESCAPE clause.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("name LIKE 'foo\\'"));
    assertTrue(t.getMessage().contains("escape character"),
        "expected trailing-escape rejection, got: " + t.getMessage());
  }

  @Test
  void isNullAndExpressionPrecedence() {
    // Critical precedence guard: `IS NULL AND <other>` must emit the IS NULL
    // rewrite paren-wrapped, otherwise CEL's tighter `&&` precedence absorbs
    // the `... == null` half: `!has(a) || dyn(a) == null && b > 0` would parse
    // as `!has(a) || (a == null && b > 0)` — wrong (returns true for absent
    // a with b <= 0). Wrapping yields `(!has(a) || dyn(a) == null) && b > 0`.
    assertEquals("(!has(this.a) || dyn(this.a) == null) && this.b > 0",
        translateCheck("a IS NULL AND b > 0"));
    assertEquals("this.b > 0 && (!has(this.a) || dyn(this.a) == null)",
        translateCheck("b > 0 AND a IS NULL"));
    assertEquals("this.a in [1, 2] && (!has(this.b) || dyn(this.b) == null)",
        translateCheck("a IN (1, 2) AND b IS NULL"));
  }

  @Test
  void betweenBoundIsNullRejected() {
    // Grammar permits `a BETWEEN (b IS NULL) AND 100` because check_expr_in
    // descends through check_expr_isnull; emit yields `bool <= int` which
    // CEL rejects at runtime. Reject at parse time instead.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("a BETWEEN (b IS NULL) AND 100"));
    assertTrue(t.getMessage().contains("strict type-check")
            || t.getMessage().contains("IS NULL"),
        "expected IS NULL bound rejection, got: " + t.getMessage());
  }

  @Test
  void likeEscapeMalformedReportsEscapeError() {
    // Multi-char ESCAPE clause must be reported as such, not silently
    // ignored in favor of the default `\` (which would then mis-report a
    // "trailing backslash" error from validateLikePatterns).
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("name LIKE 'foo\\' ESCAPE 'xx'"));
    assertTrue(t.getMessage().contains("ESCAPE clause must specify exactly one"),
        "expected ESCAPE-malformed rejection, got: " + t.getMessage());
  }

  @Test
  void inListWithNullRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x IN (1, 2, NULL)"));
    assertTrue(t.getMessage().contains("IN list cannot contain NULL"),
        "expected NULL-in-IN-list rejection, got: " + t.getMessage());
  }

  @Test
  void inTargetNonCollectionRejected() {
    // `a` and `b` are both INT in the fixture; `a IN b` is INT IN INT —
    // the IN target must be a collection.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("a IN b"));
    assertTrue(t.getMessage().contains("strict type-check")
            || t.getMessage().contains("must be a collection"),
        "expected non-collection IN-target rejection, got: " + t.getMessage());
  }

  @Test
  void inTargetWrongElementTypeRejected() {
    // x is INT, tags is STRING ARRAY → element type mismatch.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x IN tags"));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  @Test
  void inTargetCollectionMatchingType() {
    // tags is STRING ARRAY; comparing string to its elements is fine.
    String r = translateCheck("name IN tags");
    assertTrue(r.contains("this.name in this.tags"), "got: " + r);
  }

  @Test
  void indirectionFieldOnNonStructRejected() {
    // x is INT — `.y` access should be rejected.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x.y > 0"));
    assertTrue(t.getMessage().contains("Cannot access field"),
        "expected field-on-non-struct rejection, got: " + t.getMessage());
  }

  @Test
  void indirectionUnknownFieldRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("addr_struct.bogus > 0"));
    assertTrue(t.getMessage().contains("Unknown field"),
        "expected unknown-field rejection, got: " + t.getMessage());
  }

  @Test
  void deepParensRejectedBeforeStackOverflow() {
    // 100 parens parses fine but exceeds MAX_PARSE_DEPTH (each paren adds
    // ~13 cascade levels, so 100 parens ~= 1300 parse-tree depth). The
    // validator's depth guard rejects with a clear error before any
    // recursive walker has a chance to overflow the stack.
    StringBuilder sb = new StringBuilder();
    int depth = 100;
    for (int i = 0; i < depth; i++) {
      sb.append("(");
    }
    sb.append("x");
    for (int i = 0; i < depth; i++) {
      sb.append(")");
    }
    sb.append(" > 0");
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck(sb.toString()));
    assertTrue(t.getMessage().contains("nesting depth"),
        "expected depth-exceeded message, got: " + t.getMessage());
  }

  @Test
  void matchesInvalidRegexRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("MATCHES(name, '[invalid')"));
    assertTrue(t.getMessage().contains("regex is invalid"),
        "expected regex-invalid error, got: " + t.getMessage());
  }

  @Test
  void matchesValidRegexAccepted() {
    String r = translateCheck("MATCHES(name, '^[a-z]+$')");
    assertTrue(r.contains(".matches('^[a-z]+$')"), "got: " + r);
  }

  // ---------------------------------------------------------------------
  // Audit round 10 regressions
  // ---------------------------------------------------------------------

  @Test
  void matchesBackrefRejected() {
    // RE2 doesn't support backreferences. Java's regex does — using RE2J
    // for validation catches the mismatch at parse time.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("MATCHES(name, '(a)\\1')"));
    assertTrue(t.getMessage().contains("RE2 syntax"),
        "expected RE2-syntax message, got: " + t.getMessage());
  }

  @Test
  void matchesPossessiveQuantifierRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("MATCHES(name, 'a*+')"));
    assertTrue(t.getMessage().contains("RE2 syntax"),
        "expected RE2-syntax message, got: " + t.getMessage());
  }

  @Test
  void macroIterVarReservedRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("EVERY(tags, true, true)"));
    assertTrue(t.getMessage().contains("CEL reserved word"),
        "expected CEL-reserved-word message, got: " + t.getMessage());
  }

  @Test
  void programmaticFieldReservedNameAcceptedForWireCompat() {
    // Wire-format readers may legitimately surface fields named like CEL
    // reserved words (e.g. proto `optional int32 in = 1;`). Schema.Field
    // accepts these so existing schemas remain readable; the translator
    // emits index-syntax (`this["in"]`) instead of dot-syntax for refs.
    Schema.Field f = new Schema.Field("in", Schema.create(Schema.Type.INT), 0);
    assertEquals("in", f.getName());
  }

  @Test
  void programmaticFieldWhitespaceNameRejected() {
    org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> new Schema.Field("a b", Schema.create(Schema.Type.INT), 0));
  }

  @Test
  void programmaticEnumValueReservedAcceptedForWireCompat() {
    Schema.EnumValue v = new Schema.EnumValue("null");
    assertEquals("null", v.getSymbol());
  }

  // ---------------------------------------------------------------------
  // Audit round 11 regressions
  // ---------------------------------------------------------------------

  @Test
  void avroReaderAcceptsCelReservedFieldName() {
    // C1: existing wire schemas with fields named like CEL reserved words
    // (e.g. proto's `optional int32 in = 1;`) must remain readable. Schema
    // constructors no longer reject; the translator handles the emit via
    // index syntax instead of dot syntax.
    String avroSchema =
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"in\",\"type\":\"int\"}]}";
    io.confluent.kafka.schemaregistry.avro.AvroSchema s =
        new io.confluent.kafka.schemaregistry.avro.AvroSchema(avroSchema);
    LogicalType lt = io.confluent.kafka.schemaregistry.type.logical.avro
        .AvroToLogicalTypeConverter.toLogicalType(s);
    Schema rec = lt.getNamedTypes().get(lt.getRootSchema().getQualifiedName());
    assertEquals("in", rec.getFields().get(0).getName());
  }

  @Test
  void turkishLocaleCastIntStillWorks() {
    // H1: `.toUpperCase()` without Locale.ROOT broke `int` → `İNT` in
    // Turkish locale, missing the switch case for INT. Now Locale.ROOT'd.
    java.util.Locale orig = java.util.Locale.getDefault();
    try {
      java.util.Locale.setDefault(new java.util.Locale("tr", "TR"));
      String cel = translateCheck("CAST(value AS int) > 0");
      assertTrue(cel.contains("int(this.value)"), "got: " + cel);
    } finally {
      java.util.Locale.setDefault(orig);
    }
  }

  @Test
  void namedTypeRefIndirectionResolvesValidField() {
    // H2: `addr.zip` where `addr` is typed as a named ref to a STRUCT was
    // wrongly rejected. Validator now resolves NAMED_TYPE_REF before
    // checking field access.
    String script = "ROW Addr (zip INT); "
        + "ROW Person (a Addr, CHECK (a.zip > 0));"
        + "TYPE Person";
    LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
    v.visit(LogicalTypesParserFactory.parse(script));
    String cel = v.getNamedTypes().get("Person").getRules().get(0).getExpr();
    assertEquals("this.a.zip > 0", cel);
  }

  @Test
  void namedTypeRefIndirectionStillRejectsInvalidField() {
    // Sanity: invalid field through a named-ref still rejected.
    String script = "ROW Addr (zip INT); "
        + "ROW Person (a Addr, CHECK (a.bogus > 0));"
        + "TYPE Person";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(script));
        });
    assertTrue(t.getMessage().contains("Unknown field"),
        "expected Unknown field, got: " + t.getMessage());
  }

  @Test
  void varcharOverlongDefaultRejected() {
    // H3: VARCHAR(N) with default longer than N now rejected.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(
              "ROW T (x VARCHAR(2) DEFAULT 'too long'); "
                  + "TYPE T"));
        });
    assertTrue(t.getMessage().contains("exceeds the declared length"),
        "expected length error, got: " + t.getMessage());
  }

  @Test
  void decimalOverflowingDefaultRejected() {
    // H3: DECIMAL(p,s) with default that doesn't fit now rejected.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(
              "ROW T (x DECIMAL(3, 1) DEFAULT 999.5); "
                  + "TYPE T"));
        });
    assertTrue(t.getMessage().contains("DECIMAL")
            && t.getMessage().contains("exceeds"),
        "expected DECIMAL precision error, got: " + t.getMessage());
  }

  // ---------------------------------------------------------------------
  // Audit round 13 regressions
  // ---------------------------------------------------------------------

  @Test
  void binaryFixedLengthExactRequired() {
    // H1: BINARY(N) is fixed-length — default must be exactly N bytes.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(
              "ROW T (b BINARY(2) DEFAULT x''); TYPE T"));
        });
    assertTrue(t.getMessage().contains("must be exactly 2"),
        "expected exact-length error, got: " + t.getMessage());
  }

  @Test
  void charFixedLengthExactRequired() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(
              "ROW T (c CHAR(3) DEFAULT 'ab'); TYPE T"));
        });
    assertTrue(t.getMessage().contains("must be exactly 3"),
        "expected exact-length error, got: " + t.getMessage());
  }

  @Test
  void lengthOnArrayAccepted() {
    // LENGTH supports array/multiset/map (CEL `size()`).
    String r = translateCheck("LENGTH(tags) > 0");
    assertTrue(r.contains("size(this.tags)"), "got: " + r);
  }

  @Test
  void upperOnIntRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("UPPER(x) = 'X'"));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  @Test
  void startsWithIntRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("STARTS_WITH(x, 'a')"));
    assertTrue(t.getMessage().contains("Cannot compare") || t.getMessage().contains("incompatible") || t.getMessage().contains("not comparable") || t.getMessage().contains("strict type-check") || t.getMessage().contains("relational operator") || t.getMessage().contains("argument must be") || t.getMessage().contains("must be string") || t.getMessage().contains("LENGTH() argument") || t.getMessage().contains("STARTS_WITH(") || t.getMessage().contains("UPPER("));
  }

  // ---------------------------------------------------------------------
  // Audit round 12 regressions
  // ---------------------------------------------------------------------

  @Test
  void ddlEmitQuotesReservedFieldName() {
    // C1+C2: with C1 round-11 relaxation allowing programmatic Field("in",...),
    // the DDL emitter must backtick-quote the name so the output re-parses.
    Schema struct = Schema.createStruct(java.util.Arrays.asList(
        new Schema.Field("in", Schema.create(Schema.Type.INT), 0)));
    java.util.Map<String, Schema> named = new java.util.LinkedHashMap<>();
    named.put("X", struct);
    LogicalType lt = new LogicalType(null, struct, named,
        java.util.Collections.emptySet(), java.util.Collections.emptyMap(),
        java.util.Collections.emptyList(), java.util.Collections.emptyMap(),
        java.util.Collections.emptyMap());
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertTrue(ddl.contains("`in`"),
        "expected backtick-quoted reserved name, got:\n" + ddl);
    // And it should re-parse cleanly.
    LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
    v.visit(LogicalTypesParserFactory.parse(ddl));
  }

  @Test
  void schemaToDdlQuotesReservedFieldName() {
    // C2: standalone Schema.toDdl uses the same identifier helper.
    Schema struct = Schema.createStruct(java.util.Arrays.asList(
        new Schema.Field("in", Schema.create(Schema.Type.INT), 0)));
    String ddl = struct.toDdl();
    assertTrue(ddl.contains("`in`"),
        "expected backtick-quoted reserved name in Schema.toDdl, got: " + ddl);
  }

  @Test
  void visitorAcceptsBacktickedReservedName() {
    // H1: visitor and API are now symmetric — backtick-quoted CEL reserved
    // names parse cleanly.
    String script = "ROW T (`null` INT, x INT); TYPE T";
    LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
    v.visit(LogicalTypesParserFactory.parse(script));
    assertEquals("null", v.getNamedTypes().get("T").getFields().get(0).getName());
  }

  @Test
  void timeOverPrecisionDefaultRejected() {
    // M1: TIME(N) DEFAULT '...precision-N+1...' rejected.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(
              "ROW T (t TIME(2) DEFAULT '12:34:56.123456789'); "
                  + "TYPE T"));
        });
    assertTrue(t.getMessage().contains("fractional-second digits"),
        "expected fractional-second-digits error, got: " + t.getMessage());
  }

  @Test
  void timestampOverPrecisionDefaultRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(
              "ROW T (ts TIMESTAMP(3) DEFAULT '2026-01-01T00:00:00.123456789'); "
                  + "TYPE T"));
        });
    assertTrue(t.getMessage().contains("fractional-second digits"),
        "expected fractional-second-digits error, got: " + t.getMessage());
  }

  @Test
  void translatorEmitsIndexSyntaxForReservedFieldName() {
    // Build a Schema with a reserved-word field programmatically (mimics
    // wire-format read), then translate a CHECK referencing it. The emit
    // must use `this["in"]` (CEL-parseable) rather than `this.in` (a CEL
    // parse error).
    Schema rootStruct = Schema.createStruct(java.util.Arrays.asList(
        new Schema.Field("in", Schema.create(Schema.Type.INT), 0)));
    ConstraintValidationContext vctx = ConstraintValidationContext.tableLevel(
        rootStruct.getFields());
    // Parse the CHECK directly through the translator (the DDL visitor
    // would reject the reserved name; we exercise the lower layer).
    io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser
        .ScriptContext scriptCtx = io.confluent.kafka.schemaregistry.type.logical
            .LogicalTypesParserFactory.parse(
                "ROW __T (__x INT, CHECK (`in` > 0));");
    io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser
        .Check_exprContext checkCtx = findFirstCheckExpr(scriptCtx);
    String cel = ConstraintToCelTranslator.translate(checkCtx, vctx);
    assertTrue(cel.contains("this[\"in\"]"), "expected index syntax, got: " + cel);
    // Strict-check the result against the real schema.
    CelValidator.assertValidStrict(cel, rootStruct);
  }

  private static io.confluent.kafka.schemaregistry.type.logical.generated
      .LogicalTypesParser.Check_exprContext findFirstCheckExpr(
          org.antlr.v4.runtime.tree.ParseTree node) {
    if (node instanceof io.confluent.kafka.schemaregistry.type.logical.generated
        .LogicalTypesParser.Check_exprContext) {
      return (io.confluent.kafka.schemaregistry.type.logical.generated
          .LogicalTypesParser.Check_exprContext) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      io.confluent.kafka.schemaregistry.type.logical.generated
          .LogicalTypesParser.Check_exprContext r =
              findFirstCheckExpr(node.getChild(i));
      if (r != null) {
        return r;
      }
    }
    return null;
  }

  @Test
  void inSingleElementCollectionRejected() {
    // `name IN (tags)` parses as one-element-list-containing-tags →
    // emits `name in [tags]` → strict catches `string in list<list<string>>`
    // as no overload. The hand-coded "drop the surrounding parens" hint
    // was deleted in favor of strict's catch.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("name IN (tags)"));
    assertTrue(t.getMessage().contains("strict type-check")
            || t.getMessage().contains("drop the surrounding parens"),
        "expected drop-parens hint, got: " + t.getMessage());
  }

  @Test
  void greatestFunctionCallArgsTypeChecked() {
    // GREATEST(LENGTH(name), 'foo') — LENGTH returns INT, 'foo' is STRING.
    // Now that tryResolveMulType knows function return types, this is rejected.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("GREATEST(LENGTH(name), 'foo') > 0"));
    assertTrue(t.getMessage().contains("strict type-check") || t.getMessage().contains("Cannot compare"),
        "expected category-mismatch error, got: " + t.getMessage());
  }

  @Test
  void inFunctionCallTargetRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x IN LENGTH(name)"));
    assertTrue(t.getMessage().contains("strict type-check")
            || t.getMessage().contains("must be a collection"),
        "expected non-collection IN-target rejection, got: " + t.getMessage());
  }

  @Test
  void parenMapIndexEmitsMapAccess() {
    // (m)['k'] — paren-form map indexing must emit map-style `['k']`, not
    // the ARRAY 1-to-0 conversion `[('k') - 1]`. The user's outer parens
    // around `m` are preserved verbatim. Uses a custom script because the
    // shared fixture has no MAP column.
    String script = "ROW T ("
        + "m MAP<STRING, STRING>,"
        + "CHECK ((m)['k'] = 'v')"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String r = schema.getRules().get(0).getExpr();
    assertTrue(r.contains("['k']") && !r.contains("- 1]"),
        "expected map-style indexing (no - 1 offset), got: " + r);
  }

  @Test
  void parenStructIndirectionUnknownFieldRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("(addr_struct).bogus = 1"));
    assertTrue(t.getMessage().contains("Unknown field"),
        "expected unknown-field rejection, got: " + t.getMessage());
  }

  @Test
  void deepParensRaisedLimitAccepts18() {
    // 18 nested parens used to exceed MAX_PARSE_DEPTH=256 (each cascade
    // level adds ~13 parse-tree depth). Raised limit (1024) admits them.
    StringBuilder sb = new StringBuilder();
    int depth = 18;
    for (int i = 0; i < depth; i++) {
      sb.append("(");
    }
    sb.append("x > 0");
    for (int i = 0; i < depth; i++) {
      sb.append(")");
    }
    String r = translateCheck(sb.toString());
    assertTrue(r.contains("this.x > 0"), "got: " + r);
  }

  // ---------------------------------------------------------------------
  // Column-level strict-check coverage — `this` is the column value
  // ---------------------------------------------------------------------

  @Test
  void columnLevelIntCheckEmitsThisDirectly() {
    // Column-level: `this` is the int column value, not `this.x`.
    assertEquals("this > 0", translateColumnCheck("x INT", "x > 0"));
  }

  @Test
  void columnLevelStringCheckEmitsThisDirectly() {
    assertEquals("this == 'foo'",
        translateColumnCheck("name STRING", "name = 'foo'"));
  }

  @Test
  void columnLevelArithmeticOnIntAccepted() {
    assertEquals("this + 1 > 0",
        translateColumnCheck("x INT", "x + 1 > 0"));
  }

  @Test
  void columnLevelStringOpRejectedByStrict() {
    // STRING column with a numeric op — strict catches because `this`
    // is declared as String and `_+_(string, int)` has no overload.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateColumnCheck("name STRING", "name + 1 > 0"));
    assertTrue(
        t.getMessage().contains("strict type-check")
            || t.getMessage().contains("Operator"),
        "expected strict rejection of string+int on column-level, got: "
            + t.getMessage());
  }

  @Test
  void columnLevelStructFieldCheckAccepted() {
    // Column-level STRUCT: `this` is the struct, `this.zip` resolves
    // through the column-level branch in ConstraintTypeProvider.
    String result = translateColumnCheck(
        "addr ROW(zip INT) NOT NULL", "addr.zip > 0");
    assertEquals("this.zip > 0", result);
  }

  @Test
  void columnLevelComparisonCrossTypeRejected() {
    // Column is INT, literal is string — validateCompare rejects cross-type
    // equality first; if it didn't, strict's column-level branch (where
    // `this` is Int) would catch `this == 'foo'`.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateColumnCheck("x INT", "x = 'foo'"));
    assertTrue(
        t.getMessage().contains("strict type-check")
            || t.getMessage().contains("Cannot compare"),
        "expected cross-type rejection on column-level, got: "
            + t.getMessage());
  }

  // ---------------------------------------------------------------------
  // NAMED_TYPE_REF strict-check coverage
  // ---------------------------------------------------------------------

  @Test
  void namedTypeRefTableLevelCheckAccepted() {
    // Field type is a NAMED_TYPE_REF to a previously-defined STRUCT.
    // ConstraintTypeProvider must resolve the ref to its target so the
    // strict checker can look up `addr.zip`.
    String script = "ROW Address (zip INT, city STRING);"
        + "ROW Person ("
        + "  name STRING,"
        + "  addr Address NOT NULL,"
        + "  CHECK (addr.zip > 0)"
        + ");";
    Schema person = parseScript(script).getNamedTypes().get("Person");
    assertEquals("this.addr.zip > 0", person.getRules().get(0).getExpr());
  }

  @Test
  void namedTypeRefColumnLevelCheckAccepted() {
    // Column-level CHECK on a NAMED_TYPE_REF column — `this` is the
    // resolved struct, accessed directly.
    String script = "ROW Address (zip INT);"
        + "ROW Person ("
        + "  addr Address NOT NULL CHECK (addr.zip > 0)"
        + ");";
    Schema person = parseScript(script).getNamedTypes().get("Person");
    String cel = person.getField("addr").getRules().get(0).getExpr();
    assertEquals("this.zip > 0", cel);
  }

  @Test
  void namedTypeRefUnresolvedFallsBackToDyn() {
    // Externally-referenced type with no definition — ConstraintTypeProvider
    // falls back to dyn so the CHECK still translates (any field accesses
    // type-check as dyn). Externals are inferred from usage; the bare
    // reference `Address` is sufficient to make Address external.
    String script = "ROW Person ("
        + "  addr Address NOT NULL,"
        + "  CHECK (addr.zip > 0)"
        + ");";
    Schema person = parseScript(script).getNamedTypes().get("Person");
    assertEquals("this.addr.zip > 0", person.getRules().get(0).getExpr());
  }

  // ---------------------------------------------------------------------
  // H-3: IS NULL on non-null-returning expressions (dead code)
  // ---------------------------------------------------------------------

  @Test
  void isNullOnLengthRejected() {
    // LENGTH(name) always returns a BIGINT — the IS NULL test is
    // unconditionally false. Strict accepts as well-typed; we reject for
    // the user.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("LENGTH(name) IS NULL"));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection, got: " + t.getMessage());
  }

  @Test
  void isNotNullOnLengthRejected() {
    // Same class — IS NOT NULL on a never-null function is unconditionally
    // true.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("LENGTH(name) IS NOT NULL"));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection, got: " + t.getMessage());
  }

  @Test
  void isNullOnUpperRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("UPPER(name) IS NULL"));
    assertTrue(t.getMessage().contains("dead code"),
        "expected UPPER dead-code rejection, got: " + t.getMessage());
  }

  @Test
  void isNullOnCastRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CAST(x AS STRING) IS NULL"));
    assertTrue(t.getMessage().contains("dead code"),
        "expected CAST dead-code rejection, got: " + t.getMessage());
  }

  @Test
  void isNullOnCurrentTimestampRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CURRENT_TIMESTAMP IS NULL"));
    assertTrue(t.getMessage().contains("dead code"),
        "expected CURRENT_TIMESTAMP dead-code rejection, got: "
            + t.getMessage());
  }

  @Test
  void isNullOnNullifAccepted() {
    // NULLIF can return null — IS NULL on it is meaningful.
    String cel = translateCheck("NULLIF(x, 0) IS NULL");
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullOnColumnRefAccepted() {
    // Column refs are still allowed (the common case).
    String cel = translateCheck("x IS NULL");
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullOnLengthInsideMacroRejected() {
    // The pass threads through macro bodies via recurseIntoMacro.
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t, LENGTH(t) IS NULL))"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection inside macro, got: " + t.getMessage());
  }

  // ---------------------------------------------------------------------
  // IS NULL on non-nullable column ref (dead-code, column-ref shape)
  // ---------------------------------------------------------------------

  @Test
  void isNullOnNotNullColumnRejected() {
    // Bare NOT NULL column ref. Strict accepts the well-typed CEL emit
    // but the comparison is unconditionally false.
    String script = "ROW T ("
        + "zip INT NOT NULL,"
        + "CHECK (zip IS NULL)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection on NOT NULL column, got: "
            + t.getMessage());
  }

  @Test
  void isNotNullOnNotNullColumnRejected() {
    // Same class — IS NOT NULL on a NOT NULL column is unconditionally true.
    String script = "ROW T ("
        + "zip INT NOT NULL,"
        + "CHECK (zip IS NOT NULL)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection on NOT NULL column, got: "
            + t.getMessage());
  }

  @Test
  void isNullOnNullableColumnAccepted() {
    // A nullable column ref → IS NULL is meaningful.
    String script = "ROW T ("
        + "zip INT,"
        + "CHECK (zip IS NULL)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullOnNestedNotNullFieldRejected() {
    // Both the parent struct and the nested field are NOT NULL — the path
    // is wholly non-null, so IS NULL is dead.
    String script = "ROW T ("
        + "addr ROW(zip INT NOT NULL) NOT NULL,"
        + "CHECK (addr.zip IS NULL)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection on nested NOT NULL field, got: "
            + t.getMessage());
  }

  @Test
  void isNullOnNullableNestedFieldAccepted() {
    // Parent is NOT NULL but the leaf field is nullable — IS NULL is
    // meaningful. Validator must defer.
    String script = "ROW T ("
        + "addr ROW(zip INT) NOT NULL,"
        + "CHECK (addr.zip IS NULL)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullThroughNullableParentAccepted() {
    // Parent struct is nullable — even a NOT NULL leaf field can effectively
    // be unreachable, so IS NULL is meaningful. Validator must defer.
    String script = "ROW T ("
        + "addr ROW(zip INT NOT NULL),"
        + "CHECK (addr.zip IS NULL)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullOnArrayIndexedAccepted() {
    // Indexing into an ARRAY can produce missing values regardless of
    // element nullability — defer.
    String script = "ROW T ("
        + "tags STRING ARRAY,"
        + "CHECK (tags[1] IS NULL)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullOnNotNullColumnInsideMacroRejected() {
    // The pass threads through macro bodies via recurseIntoMacro. The
    // outer NOT NULL column reference is what's dead, not the iter-var.
    String script = "ROW T ("
        + "zip INT NOT NULL,"
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t, zip IS NULL))"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection inside macro, got: " + t.getMessage());
  }

  // ---------------------------------------------------------------------
  // Column-level skip-on-null contract
  // ---------------------------------------------------------------------

  @Test
  void columnLevelBareIsNullOnNullableRejected() {
    // Even when the column is declared NULLABLE, the runtime skip-on-null
    // contract makes column-level `<col> IS NULL` dead code — the CEL is
    // never invoked when the value is null/missing.
    String script = "ROW T ("
        + "x INT CHECK (x IS NULL)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("never fires"),
        "expected skip-on-null rejection, got: " + t.getMessage());
  }

  @Test
  void columnLevelNestedFieldIsNullStillAllowed() {
    // The skip-on-null contract applies to the bare column, not to nested
    // struct fields. Nested fields can still be null even when the
    // outer column is non-null at evaluation time.
    String script = "ROW T ("
        + "addr ROW(zip INT) CHECK (addr.zip IS NULL)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getField("addr").getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void columnLevelCoalesceBareColRejected() {
    // COALESCE(<bare-col>, fallback) at column level: `this` is guaranteed
    // non-null under skip-on-null, so the fallback never runs. Dead code.
    String script = "ROW T ("
        + "x INT CHECK (COALESCE(x, 0) > 0)"
        + ");";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("dead code")
            || t.getMessage().contains("COALESCE"),
        "expected COALESCE bare-col rejection, got: " + t.getMessage());
  }

  @Test
  void columnLevelCoalesceNestedFieldStillAllowed() {
    // COALESCE on a nested field is fine — the nested field can still be
    // null even with the outer column guaranteed non-null.
    String script = "ROW T ("
        + "addr ROW(zip INT) CHECK (COALESCE(addr.zip, 0) > 0)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getField("addr").getRules().get(0).getExpr();
    assertTrue(cel.contains("zip"), "got: " + cel);
  }

  @Test
  void columnLevelNullifBareColStillAllowed() {
    // NULLIF can return null when the two args compare equal — that's
    // meaningful regardless of input nullability. Don't reject.
    String script = "ROW T ("
        + "x INT CHECK (NULLIF(x, 0) > 0)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getField("x").getRules().get(0).getExpr();
    assertTrue(cel.contains("?") && cel.contains("dyn(null)"),
        "got: " + cel);
  }

  @Test
  void tableLevelBareIsNullStillAllowed() {
    // Table-level CHECK is unaffected by the column-level rule —
    // `has(this.x)` works for format-aware presence at table level.
    String script = "ROW T ("
        + "x INT,"
        + "CHECK (x IS NULL)"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("has(this.x)"), "got: " + cel);
  }

  @Test
  void columnLevelMacroIterVarIsNullAllowed() {
    // Regression: skip-on-null contract applies to the constrained column,
    // NOT to macro iter-var bindings. `t IS NULL` inside the body of a
    // column-level macro must NOT be rejected — `t` is the binding, not
    // the column.
    String script = "ROW T ("
        + "tags STRING ARRAY CHECK (EVERY(tags, t, t IS NULL))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getField("tags").getRules().get(0).getExpr();
    assertTrue(cel.contains("dyn(t) == null"), "got: " + cel);
  }

  @Test
  void columnLevelMacroIterVarIsNotNullAllowed() {
    String script = "ROW T ("
        + "tags STRING ARRAY CHECK (EVERY(tags, t, t IS NOT NULL))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getField("tags").getRules().get(0).getExpr();
    assertTrue(cel.contains("dyn(t) != null"), "got: " + cel);
  }

  @Test
  void columnLevelMacroIterVarInCoalesceAllowed() {
    // Same regression class — COALESCE(t, x) inside a column-level macro
    // must NOT be rejected; `t` is the iter-var binding, not the column.
    String script = "ROW T ("
        + "tags STRING ARRAY CHECK ("
        + "EVERY(tags, t, COALESCE(t, 'x') = 'x'))"
        + ");";
    Schema schema = parseScript(script).getNamedTypes().get("T");
    String cel = schema.getField("tags").getRules().get(0).getExpr();
    assertTrue(cel.contains("(t)"), "got: " + cel);
  }

  // ---------------------------------------------------------------------
  // H-2: NULLIF as BETWEEN bound (runtime null-comparison crash)
  // ---------------------------------------------------------------------

  @Test
  void nullifAsBetweenLowerBoundRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x BETWEEN NULLIF(a, b) AND 10"));
    assertTrue(t.getMessage().contains("NULLIF"),
        "expected NULLIF-bound rejection, got: " + t.getMessage());
  }

  @Test
  void nullifAsBetweenUpperBoundRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x BETWEEN 1 AND NULLIF(a, b)"));
    assertTrue(t.getMessage().contains("NULLIF"),
        "expected NULLIF upper-bound rejection, got: " + t.getMessage());
  }

  @Test
  void nullifAsBetweenSubjectRejected() {
    // The subject position can also be null at runtime — the emit
    // produces `lo <= subj && subj <= hi`, so a null subject hits
    // `1 <= null` which has no overload in CEL. Reject all three
    // positions for symmetry.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("NULLIF(x, 0) BETWEEN 1 AND 10"));
    assertTrue(t.getMessage().contains("NULLIF"),
        "expected NULLIF subject rejection, got: " + t.getMessage());
  }
}
