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

import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.extensions.CelExtensions;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.CelDecimal;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesParserFactory;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesSchemaVisitor;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
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

  // The null test of translateCheck's nullable x.
  private static final String X_NULL = "(!has(this.x) || dyn(this.x) == null)";

  /**
   * Parse a single CHECK expression as a table-level constraint over a
   * comprehensive test struct, and return its CEL translation. The test
   * struct defines a wide column set so test expressions can reference common
   * names (name, email, id, ts, tags, etc.) without needing to declare them
   * each time.
   */
  private String translateCheck(String checkExpr) {
    return translateCheckOf(NULLABLE_COLUMNS, checkExpr);
  }

  // translateCheck's columns NOT NULL, for how an operator translates: a CHECK over nullable
  // ones also passes on a NULL.
  private String translateCheckNotNull(String checkExpr) {
    return translateCheckOf(NOT_NULL_COLUMNS, checkExpr);
  }

  private static final String NULLABLE_COLUMNS = ""
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
      + "dprices DECIMAL(10, 2) ARRAY, dprices_ms DECIMAL(10, 2) MULTISET,"
      + "matrix INT ARRAY ARRAY,"
      + "addresses STRING ARRAY, emails STRING ARRAY,"
      + "addr_struct STRUCT(zip INT) NOT NULL,"
      + "data VARIANT,";

  private static final String NOT_NULL_COLUMNS = ""
      + "x INT NOT NULL, value STRING NOT NULL, name STRING NOT NULL, id STRING NOT NULL,"
      + "email STRING NOT NULL, phone STRING NOT NULL, digits STRING NOT NULL,"
      + "u STRING NOT NULL, h STRING NOT NULL, addr STRING NOT NULL,"
      + "needle STRING NOT NULL, haystack STRING NOT NULL, filename STRING NOT NULL,"
      + "lo INT NOT NULL, hi INT NOT NULL, age INT NOT NULL,"
      + "ratio DOUBLE NOT NULL,"
      + "amount DECIMAL(10, 2) NOT NULL, price DECIMAL(12, 4) NOT NULL,"
      + "discount DECIMAL(10, 2) NOT NULL,"
      + "blob BYTES NOT NULL,"
      + "ts TIMESTAMP_LTZ NOT NULL, created_at TIMESTAMP_LTZ NOT NULL,"
      + "first_name STRING NOT NULL, last_name STRING NOT NULL, type STRING NOT NULL,"
      + "size STRING NOT NULL, qty INT NOT NULL, status STRING NOT NULL,"
      + "active BOOLEAN NOT NULL,"
      + "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL,"
      + "tags STRING NOT NULL ARRAY NOT NULL, ages INT NOT NULL ARRAY NOT NULL,"
      + "dprices DECIMAL(10, 2) NOT NULL ARRAY NOT NULL,"
      + "dprices_ms DECIMAL(10, 2) NOT NULL MULTISET NOT NULL,"
      + "matrix INT NOT NULL ARRAY NOT NULL ARRAY NOT NULL,"
      + "addresses STRING NOT NULL ARRAY NOT NULL, emails STRING NOT NULL ARRAY NOT NULL,"
      + "addr_struct STRUCT(zip INT NOT NULL) NOT NULL,"
      + "data VARIANT NOT NULL,";

  private String translateCheckOf(String columns, String checkExpr) {
    String script = "STRUCT T (" + columns + "CHECK (" + checkExpr + "));";
    Schema schema = parseScript(script).getRootSchema();
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
    String script = "STRUCT T ("
        + columnDecl + " CHECK (" + checkExpr + ")"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    // The CEL is on the column's rules list (not the struct's).
    return schema.getFields().get(0).getRules().get(0).getExpr();
  }

  @Test
  void equality() {
    assertEquals("this.x == 5", translateCheckNotNull("x = 5"));
  }

  @Test
  void notEqualAngleBrackets() {
    assertEquals("this.x != 5", translateCheckNotNull("x <> 5"));
  }

  @Test
  void notEqualBang() {
    assertEquals("this.x != 5", translateCheckNotNull("x != 5"));
  }

  @Test
  void lessThan() {
    assertEquals("this.x < 5", translateCheckNotNull("x < 5"));
  }

  @Test
  void lessOrEqual() {
    assertEquals("this.x <= 5", translateCheckNotNull("x <= 5"));
  }

  @Test
  void greaterThan() {
    assertEquals("this.x > 5", translateCheckNotNull("x > 5"));
  }

  @Test
  void greaterOrEqual() {
    assertEquals("this.x >= 5", translateCheckNotNull("x >= 5"));
  }

  // ---------------------------------------------------------------------
  // Logical
  // ---------------------------------------------------------------------

  @Test
  void logicalAnd() {
    assertEquals("this.x > 0 && this.x < 100", translateCheckNotNull("x > 0 AND x < 100"));
  }

  @Test
  void logicalOr() {
    assertEquals("this.x < 0 || this.x > 100", translateCheckNotNull("x < 0 OR x > 100"));
  }

  @Test
  void logicalNot() {
    // SQL `NOT` has lower precedence than comparisons, so `NOT x > 0` is
    // `NOT (x > 0)`. CEL `!` binds tighter than `>`, so we must emit
    // parens: `!(x > 0)`. Bare `!x > 0` would parse as `(!x) > 0` and
    // fail at runtime with a type error.
    assertEquals("!(this.x > 0)", translateCheckNotNull("NOT x > 0"));
  }

  @Test
  void logicalNotWrapsBooleanColumn() {
    assertEquals("!this.active", translateCheckNotNull("NOT active"));
  }

  @Test
  void doubleNotNests() {
    assertEquals("this.x > 0", translateCheckNotNull("NOT NOT x > 0"));
  }

  @Test
  void notAppliedToIsNull() {
    // `NOT x IS NULL` parses as NOT applied to (x IS NULL); the IS NULL
    // form emits the paren-wrapped has() guard, and NOT must wrap it.
    assertEquals("has(this.x) && dyn(this.x) != null",
        translateCheck("NOT x IS NULL"));
  }

  @Test
  void andOrPrecedence() {
    // AND binds tighter than OR
    assertEquals("(this.x > 0 && this.x < 100) || this.x = -1",
        translateCheckNotNull("x > 0 AND x < 100 OR x = -1").replace("==", "="));
  }

  // ---------------------------------------------------------------------
  // Arithmetic
  // ---------------------------------------------------------------------

  @Test
  void addition() {
    assertEquals("this.x + 1 > 0", translateCheckNotNull("x + 1 > 0"));
  }

  @Test
  void subtraction() {
    assertEquals("this.x - 1 < 100", translateCheckNotNull("x - 1 < 100"));
  }

  @Test
  void subtractionWithoutSpaces() {
    // Regression: lexer used to greedy-eat `-` into INT_LITERAL, breaking
    // parses like `x-1` where there were no surrounding spaces.
    assertEquals("this.x - 1 < 100", translateCheckNotNull("x-1 < 100"));
    assertEquals("this.x - 1 < 100", translateCheckNotNull("x -1 < 100"));
    assertEquals("this.x - 1 < 100", translateCheckNotNull("x- 1 < 100"));
    // Worked out as a constant.
    assertEquals("-1 < this.x", translateCheckNotNull("1-2 < x"));
  }

  @Test
  void multiplyDivideModulo() {
    assertEquals("this.x * 2 / 3 % 4 == 0", translateCheckNotNull("x * 2 / 3 % 4 = 0"));
  }

  @Test
  void moduloOnDecimalEmitsDecimalsMod() {
    // `1.5` is a DECIMAL literal (no exponent), so `1.5 % 2` coerces to the
    // common type DECIMAL and modulo emits decimals.mod.
    assertEquals(
        "decimals.eq(decimals.mod(decimal(\"1.5\"), decimal(\"2\")), decimal(\"0.5\"))",
        translateCheck("1.5 % 2 = 0.5"));
  }

  @Test
  void moduloOnDecimalColumnEmitsDecimalsMod() {
    // amount is a DECIMAL column; `amount % 100 = 0` → decimals.mod.
    assertEquals(
        "decimals.eq(decimals.mod(this.amount, decimal(\"100\")), decimal(\"0\"))",
        translateCheckNotNull("amount % 100 = 0"));
  }

  @Test
  void moduloRejectsDoubleOperand() {
    // No double-modulo operator exists; `ratio % 2` coerces to DOUBLE and is
    // rejected at parse time with a clear message.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("ratio % 2 = 0"));
    assertTrue(t.getMessage().contains("modulo") && t.getMessage().contains("DOUBLE"),
        "expected DOUBLE-modulo rejection, got: " + t.getMessage());
  }

  @Test
  void moduloAcceptsIntOperands() {
    assertEquals("this.x % 2 == 0", translateCheckNotNull("x % 2 = 0"));
  }

  @Test
  void unaryMinus() {
    assertEquals("-this.x > 0", translateCheckNotNull("-x > 0"));
  }

  @Test
  void stringConcat() {
    assertEquals("this.name + 'suffix' == 'asuffix'",
        translateCheckNotNull("name || 'suffix' = 'asuffix'"));
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
    String script = "STRUCT T ("
        + "binData BYTES,"
        + "CHECK (binData || x'00' = x'01')"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.binData) || dyn(this.binData) == null || this.binData + b\"\\x00\" =="
            + " b\"\\x01\"",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void bytesLiteralMultiByte() {
    // Multi-byte literal: each hex pair becomes one \xNN escape; case-folded
    // to upper for stability.
    String script = "STRUCT T ("
        + "binData BYTES,"
        + "CHECK (binData = x'deadbeef')"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.binData) || dyn(this.binData) == null || this.binData =="
            + " b\"\\xDE\\xAD\\xBE\\xEF\"",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void bytesLiteralOddLengthRejected() {
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t, t || 'suffix' = 'foosuffix'))"
        + ");";
    parseScript(script);
  }

  @Test
  void castBoolRejectedInsideMacro() {
    // `validateCasts` must thread the macro iter-var binding so a non-string
    // source gets rejected even when bound by an iteration variable.
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    assertEquals("this.x == 42", translateCheckNotNull("x = 42"));
  }

  @Test
  void decimalPointLiteral() {
    assertEquals("this.ratio == 3.14", translateCheckNotNull("ratio = 3.14"));
  }

  @Test
  void decimalPointLiteralTrailingDotNormalized() {
    // Grammar accepts `1.` (trailing dot) but CEL requires digits on
    // both sides. Translator normalizes to `1.0` so the emitted CEL
    // compiles cleanly.
    assertEquals("this.ratio == 1.0", translateCheckNotNull("ratio = 1."));
  }

  @Test
  void decimalPointLiteralLeadingDotNormalized() {
    // Same for `.5` → `0.5`. (SQL accepts both forms; CEL requires both
    // sides.)
    assertEquals("this.ratio == 0.5", translateCheckNotNull("ratio = .5"));
  }

  @Test
  void quotedColumnRefRoundTripsCorrectly() {
    // Regression: previously `colid().getText()` returned the backticked
    // form (e.g. `\`type\``), so vctx.isColumn lookup against schema
    // field name `type` (stored unquoted) failed → "Unknown column" or
    // emit produced invalid CEL. After the fix, the translator unquotes
    // the colid before lookup and emit.
    String script = "STRUCT T ("
        + "x INT,"
        + "CHECK (`x` > 0)"  // backticked column ref to existing field `x`
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("!has(this.x) || dyn(this.x) == null || this.x > 0", r.getExpr());
  }

  @Test
  void quotedColumnRefForReservedKeywordNameWorks() {
    // Field name `type` is a non-reserved keyword the user might quote
    // defensively. Both quoted and unquoted CHECK refs should work.
    String script = "STRUCT T ("
        + "type STRING,"
        + "CHECK (`type` = 'A')"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("!has(this.type) || dyn(this.type) == null || this.type == 'A'",
        r.getExpr());
  }

  @Test
  void stringLiteral() {
    assertEquals("this.name == 'hello'", translateCheckNotNull("name = 'hello'"));
  }

  @Test
  void stringLiteralWithEscape() {
    // SQL '' (doubled quote) escape → CEL \' escape
    assertEquals("this.name == 'O\\'Hare'", translateCheckNotNull("name = 'O''Hare'"));
  }

  @Test
  void boolLiteralTrue() {
    assertEquals("this.active", translateCheckNotNull("active = TRUE"));
  }

  @Test
  void boolLiteralFalse() {
    assertEquals("!this.active", translateCheckNotNull("active = FALSE"));
  }

  @Test
  void aSignedBoolLiteralIsStillRefused() {
    // Signs that cancel don't make a BOOLEAN literal unsigned.
    for (String sql : List.of("active = -(-TRUE)", "-(-TRUE) = active", "active <> -(-FALSE)")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckNotNull(sql), sql);
      assertTrue(e.getMessage().contains("strict type-check"), e.getMessage());
    }
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
    String script = "STRUCT T ("
        + "x INT,"
        + "CHECK (x > 0)"  // table-level so it can reference x
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("!has(this.x) || dyn(this.x) == null || this.x > 0", r.getExpr());
  }

  @Test
  void nestedFieldAccess() {
    // Table-level CHECK referencing a struct field's nested member
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT) NOT NULL,"
        + "CHECK (addr.zip > 0)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("!has(this.addr.zip) || dyn(this.addr.zip) == null || this.addr.zip > 0",
        r.getExpr());
  }

  @Test
  void arrayIndexing() {
    // Postgres convention: 1-indexed at the SQL surface; translator subtracts 1
    String script = "STRUCT T ("
        + "tags STRING ARRAY,"
        + "CHECK (tags[1] = 'first')"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("!has(this.tags) || dyn(this.tags) == null || size(this.tags) < 1 ||"
            + " dyn(this.tags[0]) == null || this.tags[0] == 'first'",
        r.getExpr());
  }

  @Test
  void mapIndexingPassesKeyThroughVerbatim() {
    // MAP keying is NOT 1-indexed — keys are arbitrary values, not
    // positions. The translator must pass `m['k']` through as `m['k']`,
    // not as `m['k'] - 1` (which is a CEL type error).
    String script = "STRUCT T ("
        + "props MAP<STRING, STRING>,"
        + "CHECK (props['name'] = 'alice')"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("!has(this.props) || dyn(this.props) == null || !('name' in this.props) ||"
            + " dyn(this.props['name']) == null || this.props['name'] == 'alice'",
        r.getExpr());
  }

  @Test
  void mapIndexingWithIntKeyIsRefused() {
    // An int-keyed map is held as entries, which a subscript would index as a list.
    ValidationException e = assertThrows(ValidationException.class,
        () -> parseScript("STRUCT T (scores MAP<INT, INT>, CHECK (scores[42] > 0));"));
    assertTrue(e.getMessage().contains("INT keys is not supported"), e.getMessage());
  }

  @Test
  void parenthesizedArrayIndexingFallsBackToArrayConvention() {
    // Paren-form indirection has no resolvable receiver type. Fall back
    // to the array convention so common `(arr)[1]` access still works.
    // (Maps used through paren forms would mistranslate — known limitation
    // documented in visitIndirection.)
    String script = "STRUCT T ("
        + "tags STRING ARRAY,"
        + "CHECK ((tags)[1] = 'first')"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("!has(this.tags) || dyn(this.tags) == null || size(dyn(this.tags)) < 1 ||"
            + " dyn(this.tags[0]) == null || this.tags[0] == 'first'",
        r.getExpr());
  }

  @Test
  void parenthesizedExpression() {
    assertEquals("(this.x + 1) * 2 > 10", translateCheckNotNull("(x + 1) * 2 > 10"));
  }

  // ---------------------------------------------------------------------
  // CASE
  // ---------------------------------------------------------------------

  @Test
  void searchedCase() {
    // Searched-form WHEN condition is now defensively wrapped in parens
    // (so a low-precedence operator inside it can't mis-bind in CEL).
    // A condition that may be NULL is false then, as SQL takes a branch only when TRUE.
    assertEquals("has(this.x) && dyn(this.x) != null && this.x > 0",
        translateCheck("CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END = 'pos'"));
  }

  @Test
  void searchedCaseMultipleWhens() {
    String result = translateCheck(
        "CASE WHEN x > 100 THEN 'big' WHEN x > 0 THEN 'small' ELSE 'zero' END = 'big'");
    // The second WHEN is still evaluated for what it raises.
    assertEquals("has(this.x) && dyn(this.x) != null && this.x > 100 ? true : (has(this.x) &&"
            + " dyn(this.x) != null && this.x > 0 ? false : false)", result);
  }

  @Test
  void simpleCase() {
    String result = translateCheck(
        "CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END = 'one'");
    // Simple CASE: subject and each WHEN value are individually wrapped in
    // parens around the implicit `==`, so a low-precedence operator inside
    // either side can't mis-bind in CEL. A NULL subject matches no WHEN value.
    assertEquals("has(this.x) && dyn(this.x) != null && this.x == 1", result);
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
    assertEquals("has(this.x) && dyn(this.x) != null && this.x > 0 ? !has(this.name) ||"
            + " dyn(this.name) == null || this.name == 'foo' : false", result);
  }

  @Test
  void caseBranchNumericPromotionAccepted() {
    // INT and DOUBLE both fall in the "numeric" category — accepted.
    String result = translateCheck(
        "CASE WHEN x > 0 THEN x ELSE 0 END > 0");
    assertEquals("has(this.x) && dyn(this.x) != null && this.x > 0", result);
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
    assertEquals("has(this.x) && dyn(this.x) != null && this.x == 1", result);
  }

  @Test
  void nestedCaseAccepted() {
    // CASE inside CASE — outer THEN is itself a CASE expression. Both
    // levels' branches resolve to the same category. Verifies that
    // validateCaseBranches recurses into nested case_expr.
    String result = translateCheck(
        "(CASE WHEN x > 0 THEN (CASE WHEN x > 100 THEN 'big' ELSE 'small' END)"
            + " ELSE 'neg' END) = 'small'");
    assertEquals("has(this.x) && dyn(this.x) != null && this.x > 0 ? !(this.x > 100) : false",
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
    String script = "STRUCT T ("
        + "ages INT ARRAY,"
        + "CHECK (EVERY(ages, n, n > 0))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.ages) || dyn(this.ages) == null || this.ages.all(n, dyn(n) == null ||"
            + " n > 0)", schema.getRules().get(0).getExpr());
  }

  @Test
  void compositeCastInBetweenInMacro() {
    // Lock in the validator order across all the type-aware passes:
    // CAST inside BETWEEN inside macro predicate. Iter-var `n` is INT
    // (from `ages`); CAST(n AS DOUBLE) → DOUBLE; bounds are DOUBLE
    // literals → numeric category — should accept.
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
        + "name STRING ARRAY,"
        + "CHECK (EVERY(name, name, LENGTH(name) > 0))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    // Outer `name` (the collection) → this.name; iter-var `name` (binding)
    // and the body ref → bare.
    assertEquals("!has(this.name) || dyn(this.name) == null || this.name.all(name, dyn(name) =="
            + " null || size(name) > 0)", r.getExpr());
  }

  @Test
  void macroIterVarMapIndexingShadowingColumn() {
    // Same root cause, indirection variant: iter-var `meta` shadows column
    // `meta` (a MAP). Inside the body, `meta['k']` is keying the iter-var
    // (a single MAP element), not the outer column. visitIndirection's
    // receiver-type lookup must see the binding, not the outer column,
    // so it correctly emits MAP-style `[expr]` (no -1).
    String script = "STRUCT T ("
        + "meta MAP<STRING, STRING> ARRAY,"
        + "CHECK (EVERY(meta, meta, meta['k'] = 'v'))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("!has(this.meta) || dyn(this.meta) == null || this.meta.all(meta, dyn(meta) =="
            + " null || !('k' in meta) || dyn(meta['k']) == null || meta['k'] == 'v')",
        r.getExpr());
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
  void moduloOnParenWrappedDecimalEmitsDecimalsMod() {
    // `(1.5)` is a paren-wrapped DECIMAL literal, so `(1.5) % 2` coerces to
    // DECIMAL and modulo emits decimals.mod (the parens collapse in the value).
    assertEquals(
        "decimals.eq(decimals.mod(decimal(\"1.5\"), decimal(\"2\")), decimal(\"0.5\"))",
        translateCheck("(1.5) % 2 = 0.5"));
  }

  @Test
  void likeOnConcatReceiverParenthesizesAgainstPrecedence() {
    // Regression: `a || b LIKE 'x%'` parses with LIKE applied to the
    // concat (a || b). CEL `.matches()` binds tighter than `+`, so a
    // bare emission would be `a + b.matches(...)` parsed as
    // `a + (b.matches(...))` — wrong. Receiver must be parenthesized.
    String result = translateCheckNotNull("first_name || last_name LIKE 'foo%'");
    assertEquals("(this.first_name + this.last_name).matches('^foo.*$')",
        result);
  }

  @Test
  void instanceMethodOnConcatReceiverParenthesized() {
    // Same precedence hazard for IS_EMAIL/STARTS_WITH/etc. on a concat.
    String result = translateCheckNotNull(
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
    String script = "STRUCT T ("
        + "x INT CHECK (x > 0) CHECK (x < 100)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    List<Rule> rules = schema.getField("x").getRules();
    assertEquals(2, rules.size());
    assertEquals("this > 0", rules.get(0).getExpr());
    assertEquals("this < 100", rules.get(1).getExpr());
  }

  @Test
  void constraintName() {
    String script = "STRUCT T ("
        + "x INT CONSTRAINT positive_x CHECK (x > 0)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getField("x").getRules().get(0);
    assertEquals("positive_x", r.getName());
    assertNull(r.getDoc());
    assertEquals("this > 0", r.getExpr());
  }

  @Test
  void messageClause() {
    String script = "STRUCT T ("
        + "x INT CHECK (x > 0) MESSAGE 'must be positive'"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getField("x").getRules().get(0);
    assertNull(r.getName());
    assertEquals("must be positive", r.getDoc());
  }

  @Test
  void constraintNameAndMessage() {
    String script = "STRUCT T ("
        + "x INT CONSTRAINT positive_x CHECK (x > 0) MESSAGE 'must be positive'"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
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
    String script = "STRUCT T ("
        + "lo INT,"
        + "hi INT,"
        + "CONSTRAINT bounds CHECK (lo < hi) MESSAGE 'lo must be < hi'"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertTrue(schema.getField("lo").getRules().isEmpty());
    assertTrue(schema.getField("hi").getRules().isEmpty());
    List<Rule> tableRules = schema.getRules();
    assertEquals(1, tableRules.size());
    Rule r = tableRules.get(0);
    assertEquals("bounds", r.getName());
    assertEquals("lo must be < hi", r.getDoc());
    assertEquals("!has(this.lo) || dyn(this.lo) == null || !has(this.hi) || dyn(this.hi) == null"
            + " || this.lo < this.hi", r.getExpr());
  }

  @Test
  void mixedFieldsAndTableConstraints() {
    String script = "STRUCT T ("
        + "lo INT CHECK (lo >= 0),"
        + "hi INT CHECK (hi <= 100),"
        + "CHECK (lo <= hi)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals(1, schema.getField("lo").getRules().size());
    assertEquals(1, schema.getField("hi").getRules().size());
    assertEquals(1, schema.getRules().size());
    assertEquals("!has(this.lo) || dyn(this.lo) == null || !has(this.hi) || dyn(this.hi) == null"
            + " || this.lo <= this.hi", schema.getRules().get(0).getExpr());
  }

  // ---------------------------------------------------------------------
  // Original SQL preservation in `logical` field
  // ---------------------------------------------------------------------

  @Test
  void logicalFieldHoldsOriginalSql() {
    String script = "STRUCT T ("
        + "x INT CHECK (x > 0 AND x < 100)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
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
    String script = "STRUCT T (x INT CHECK (x  >\t0\nAND  x<100));";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getField("x").getRules().get(0);
    assertEquals("x  >\t0\nAND  x<100", r.getSql());
  }

  @Test
  void sqlPreservesOriginalCasing() {
    // SQL keywords are case-insensitive; we should preserve whatever the
    // user wrote (lower-case `and` here) rather than normalize.
    String script = "STRUCT T (x INT CHECK (x > 0 and x < 100));";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getField("x").getRules().get(0);
    assertEquals("x > 0 and x < 100", r.getSql());
  }

  @Test
  void rulesAreNonNullEvenWhenAbsent() {
    String script = "STRUCT T (x INT);";
    Schema schema = parseScript(script).getRootSchema();
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
    assertEquals("!has(this.x) || dyn(this.x) == null", translateCheck("x IS NULL"));
  }

  @Test
  void isNotNull() {
    assertEquals("has(this.x) && dyn(this.x) != null", translateCheck("x IS NOT NULL"));
  }

  @Test
  void isNullCombinedWithLogical() {
    assertEquals("has(this.x) && dyn(this.x) != null && this.x > 0",
        translateCheck("x IS NOT NULL AND x > 0"));
  }

  @Test
  void isNullOnFieldRef() {
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT) NOT NULL,"
        + "CHECK (addr.zip IS NOT NULL)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    Rule r = schema.getRules().get(0);
    assertEquals("has(this.addr.zip) && dyn(this.addr.zip) != null", r.getExpr());
  }

  // ---------------------------------------------------------------------
  // Phase 2: IN with literal list
  // ---------------------------------------------------------------------

  @Test
  void inLiteralList() {
    assertEquals("this.x in [1, 2, 3]", translateCheckNotNull("x IN (1, 2, 3)"));
  }

  @Test
  void inSingleElementList() {
    // Disambiguation: paren-wrapped single element parses as ParenList (per design)
    assertEquals("this.x in [42]", translateCheckNotNull("x IN (42)"));
  }

  @Test
  void inStringList() {
    assertEquals("this.status in ['ACTIVE', 'PENDING', 'CANCELLED']",
        translateCheckNotNull("status IN ('ACTIVE', 'PENDING', 'CANCELLED')"));
  }

  @Test
  void notInLiteralList() {
    assertEquals("!(this.x in [1, 2, 3])", translateCheckNotNull("x NOT IN (1, 2, 3)"));
  }

  // ---------------------------------------------------------------------
  // Phase 2: IN with list-typed expression
  // ---------------------------------------------------------------------

  @Test
  void inListField() {
    // x IN <field-name> — parses as InTargetExpr (no parens)
    String script = "STRUCT T ("
        + "value STRING,"
        + "tags STRING NOT NULL ARRAY,"
        + "CHECK (value IN tags)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.tags) || dyn(this.tags) == null || ((!has(this.value) ||"
            + " dyn(this.value) == null) && size(this.tags) > 0) || (size(this.tags) == 0 ? false"
            + " : this.value in this.tags)",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void notInListField() {
    String script = "STRUCT T ("
        + "value STRING,"
        + "tags STRING NOT NULL ARRAY,"
        + "CHECK (value NOT IN tags)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.tags) || dyn(this.tags) == null || ((!has(this.value) ||"
            + " dyn(this.value) == null) && size(this.tags) > 0) || !(size(this.tags) == 0 ?"
            + " false : this.value in this.tags)",
        schema.getRules().get(0).getExpr());
  }

  // ---------------------------------------------------------------------
  // Phase 2: BETWEEN
  // ---------------------------------------------------------------------

  @Test
  void between() {
    assertEquals("0 <= this.x && this.x <= 100",
        translateCheckNotNull("x BETWEEN 0 AND 100"));
  }

  @Test
  void notBetween() {
    assertEquals("!(0 <= this.x && this.x <= 100)",
        translateCheckNotNull("x NOT BETWEEN 0 AND 100"));
  }

  @Test
  void betweenSymmetric() {
    assertEquals(
        "this.lo <= this.hi ? this.lo <= this.x && this.x <= this.hi : this.hi <= this.x &&"
            + " this.x <= this.lo",
        translateCheckNotNull("x BETWEEN SYMMETRIC lo AND hi"));
  }

  @Test
  void notBetweenSymmetric() {
    assertEquals(
        "!(this.lo <= this.hi ? this.lo <= this.x && this.x <= this.hi : this.hi <= this.x &&"
            + " this.x <= this.lo)",
        translateCheckNotNull("x NOT BETWEEN SYMMETRIC lo AND hi"));
  }

  @Test
  void betweenWithExpressionBounds() {
    assertEquals("0 <= this.x + 1 && this.x + 1 <= 100",
        translateCheckNotNull("x + 1 BETWEEN 0 AND 100"));
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
        translateCheckNotNull("x BETWEEN lo AND hi"));
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
    assertEquals("(!has(this.status) || dyn(this.status) == null || this.status in ['A', 'B']) &&"
            + " has(this.active) && dyn(this.active) != null",
        translateCheck("status IN ('A', 'B') AND active IS NOT NULL"));
  }

  @Test
  void betweenWithLogical() {
    assertEquals("!has(this.age) || dyn(this.age) == null || (0 <= this.age && this.age <= 150)",
        translateCheck("age BETWEEN 0 AND 150 OR age IS NULL"));
  }

  @Test
  void allThreeTogether() {
    String result = translateCheck(
        "status IN ('ACTIVE', 'PENDING') AND age BETWEEN 18 AND 65 AND email IS NOT NULL");
    assertEquals(
        "(!has(this.status) || dyn(this.status) == null || this.status in ['ACTIVE', 'PENDING'])"
            + " && (!has(this.age) || dyn(this.age) == null || (18 <= this.age && this.age <="
            + " 65)) && has(this.email) && dyn(this.email) != null",
        result);
  }

  // ---------------------------------------------------------------------
  // Phase 3: LIKE + ESCAPE
  // ---------------------------------------------------------------------

  @Test
  void likeExactMatch() {
    // Plain pattern with no wildcards — anchored
    assertEquals("this.name.matches('^foo$')", translateCheckNotNull("name LIKE 'foo'"));
  }

  @Test
  void likePrefix() {
    assertEquals("this.name.matches('^foo.*$')", translateCheckNotNull("name LIKE 'foo%'"));
  }

  @Test
  void likeSuffix() {
    assertEquals("this.name.matches('^.*foo$')", translateCheckNotNull("name LIKE '%foo'"));
  }

  @Test
  void likeInfix() {
    assertEquals("this.name.matches('^.*foo.*$')", translateCheckNotNull("name LIKE '%foo%'"));
  }

  @Test
  void likeUnderscore() {
    // _ matches any single character
    assertEquals("this.name.matches('^h.llo$')", translateCheckNotNull("name LIKE 'h_llo'"));
  }

  @Test
  void likeMixed() {
    assertEquals("this.name.matches('^a.b.*c$')", translateCheckNotNull("name LIKE 'a_b%c'"));
  }

  @Test
  void likeEscapesRegexSpecials() {
    // Pattern contains a literal '.' which is a regex special — must be escaped
    assertEquals("this.name.matches('^foo\\\\.bar$')",
        translateCheckNotNull("name LIKE 'foo.bar'"));
  }

  @Test
  void likeEscapesParens() {
    assertEquals("this.name.matches('^\\\\(foo\\\\)$')",
        translateCheckNotNull("name LIKE '(foo)'"));
  }

  @Test
  void likeEmail() {
    // Common pattern: rough email shape
    assertEquals("this.name.matches('^.*@.*\\\\..*$')", translateCheckNotNull("name LIKE '%@%.%'"));
  }

  @Test
  void notLike() {
    assertEquals("!this.name.matches('^.*forbidden.*$')",
        translateCheckNotNull("name NOT LIKE '%forbidden%'"));
  }

  @Test
  void likeWithDefaultEscapeChar() {
    // Default escape char is '\'. Pattern '\%foo' (SQL: '\%foo') means literal % then foo.
    // SQL parses '\%foo' as the 5-char string \%foo; LIKE→regex with default escape '\\'
    // sees the \ as escape for the next char (%), so % becomes literal.
    assertEquals("this.name.matches('^%foo$')", translateCheckNotNull("name LIKE '\\%foo'"));
  }

  @Test
  void likeWithCustomEscape() {
    // ESCAPE '!' means '!' is the escape char; '!%' is literal %, % alone is wildcard
    assertEquals("this.name.matches('^50%.*$')",
        translateCheckNotNull("name LIKE '50!%%' ESCAPE '!'"));
  }

  @Test
  void likeQuotedSingleQuoteInPattern() {
    // SQL escapes single quote as ''; pattern is literal "O'Hare"
    assertEquals("this.name.matches('^O\\'Hare$')",
        translateCheckNotNull("name LIKE 'O''Hare'"));
  }

  @Test
  void likeWithLiteralNewline() {
    // SQL STRING_LITERAL accepts a raw LF inside single quotes; the emitted
    // CEL string literal must escape it (CEL's lexer rejects raw LF in '...').
    assertEquals("this.name.matches('^a\\nb$')",
        translateCheckNotNull("name LIKE 'a\nb'"));
  }

  @Test
  void likeWithLiteralCarriageReturn() {
    assertEquals("this.name.matches('^a\\rb$')",
        translateCheckNotNull("name LIKE 'a\rb'"));
  }

  @Test
  void likeWithLiteralTab() {
    assertEquals("this.name.matches('^a\\tb$')",
        translateCheckNotNull("name LIKE 'a\tb'"));
  }

  @Test
  void likeWithLogicalCombination() {
    assertEquals("(!has(this.name) || dyn(this.name) == null || this.name.matches('^.*foo.*$'))"
            + " && has(this.id) && dyn(this.id) != null",
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
    assertEquals("size(this.name) > 0", translateCheckNotNull("LENGTH(name) > 0"));
  }

  @Test
  void lengthCaseInsensitive() {
    assertEquals("size(this.name) > 0", translateCheckNotNull("length(name) > 0"));
    assertEquals("size(this.name) > 0", translateCheckNotNull("Length(name) > 0"));
  }

  @Test
  void upperLower() {
    assertEquals("this.name.upperAscii() == 'FOO'", translateCheckNotNull("UPPER(name) = 'FOO'"));
    assertEquals("this.name.lowerAscii() == 'foo'", translateCheckNotNull("LOWER(name) = 'foo'"));
  }

  @Test
  void startsWith() {
    assertEquals("this.id.startsWith('usr_')", translateCheckNotNull("STARTS_WITH(id, 'usr_')"));
  }

  @Test
  void endsWith() {
    assertEquals("this.filename.endsWith('.json')",
        translateCheckNotNull("ENDS_WITH(filename, '.json')"));
  }

  @Test
  void contains() {
    assertEquals("this.name.contains('forbidden')",
        translateCheckNotNull("CONTAINS(name, 'forbidden')"));
  }

  @Test
  void replace() {
    assertEquals("this.phone.replace('-', '') == this.digits",
        translateCheckNotNull("REPLACE(phone, '-', '') = digits"));
  }

  @Test
  void matches() {
    assertEquals("this.value.matches('^[0-9]+$')",
        translateCheckNotNull("MATCHES(value, '^[0-9]+$')"));
  }

  @Test
  void coalesceTwoArgs() {
    // A last argument that may be NULL gives NULL rather than being read: JSON
    // leaves a NULL field out.
    assertEquals(
        "((!has(this.a) || dyn(this.a) == null) && (!has(this.b) || dyn(this.b) == null)) ||"
            + " (has(this.a) && dyn(this.a) != null ? this.a : (!has(this.b) || dyn(this.b) =="
            + " null ? dyn(null) : this.b)) > 0",
        translateCheck("COALESCE(a, b) > 0"));
  }

  @Test
  void coalesceThreeArgs() {
    assertEquals(
        "((!has(this.a) || dyn(this.a) == null) && (!has(this.b) || dyn(this.b) == null) &&"
            + " (!has(this.c) || dyn(this.c) == null)) || (has(this.a) && dyn(this.a) != null ?"
            + " this.a : (has(this.b) && dyn(this.b) != null ? this.b : (!has(this.c) ||"
            + " dyn(this.c) == null ? dyn(null) : this.c))) > 0",
        translateCheck("COALESCE(a, b, c) > 0"));
  }

  @Test
  void aNestedCheckIsEmittedOnceAtEachLevel() {
    // A NULL test or a common type emits a subexpression again, which multiplied the work at
    // each level of nesting; one over CEL's limit is refused before it grows further.
    String columns = "t BOOLEAN NOT NULL, d DECIMAL(10, 2), i INT, x DOUBLE, "
        + "an INT ARRAY NOT NULL, nt BOOLEAN";
    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      // Alternating AND and OR over a NULL emit each operand once.
      translateCheckOver(columns, nest("nt", "((%s) AND nt) OR nt"));
      translateCheckOver(columns, nest("d", "CASE WHEN t THEN %s ELSE d END") + " = d");
      translateCheckOver(columns, nest("i", "CASE WHEN t THEN %s ELSE x END") + " = x");
      translateCheckOver(columns, nest("d", "COALESCE(%s, d)") + " = d");
      translateCheckOver(columns, nest("d", "COALESCE(d, %s)") + " = d");
      translateCheckOver(columns, nest("i", "GREATEST(%s, x)") + " = x");
      // The IS NULL lint's NULL analysis emits too.
      translateCheckOver(columns, "ABS(" + nest("d", "CASE WHEN t THEN %s ELSE d END")
          + ") IS NULL");
      // So do the NULL tests of AND and OR, a CASE, and a quantifier's body.
      translateCheckOver(columns, nest("t", "((%s) AND t) IS NULL"));
      translateCheckOver(columns, nest("i", "CASE WHEN %s > 0 THEN 1 ELSE 2 END") + " = 1");
      // And a quantifier's NULL test, of each one compared in another's body.
      String quantifiers = nest("t", "EVERY(an, e, %s = TRUE)", 30);
      translateCheckOver(columns, "CASE WHEN " + quantifiers + " THEN 1 ELSE 2 END = 1");
      translateCheckOver(columns, "(" + quantifiers + " OR i > 0) IS NOT NULL");
      translateCheckOver(columns, "CASE WHEN " + nest("t", "ANY(an, e, %s IN (TRUE))", 30)
          + " THEN 1 ELSE 2 END = 1");

      for (String tooLarge : List.of(nest("d", "NULLIF(%s, d)") + " = d",
          nest("d", "NULLIF(d, %s)") + " = d", nest("t", "((%s) AND i > 0) IS NULL"),
          nest("i", "CASE WHEN EVERY(an, e, (%s) = e) THEN i ELSE 2 END", 20) + " = 1",
          nest("i", "CASE WHEN %s + an[1] > 0 THEN i ELSE 2 END") + " = 1",
          nest("d", "COALESCE(CASE WHEN t THEN %s ELSE i END, x)") + " = x",
          nest("1 / i > 0", "((%s) AND nt) OR i > 0"))) {
        ValidationException e = assertThrows(ValidationException.class,
            () -> translateCheckOver(columns, tooLarge));
        assertTrue(e.getMessage().contains("CHECK constraint is too large"), e.getMessage());
      }
    });
  }

  // leaf nested 30 deep in a call or CASE, %s marking where.
  private static String nest(String leaf, String wrap) {
    return nest(leaf, wrap, 30);
  }

  private static String nest(String leaf, String wrap, int depth) {
    String nested = leaf;
    for (int i = 0; i < depth; i++) {
      nested = String.format(wrap, nested);
    }
    return nested;
  }

  @Test
  void aCheckTooLargeForCelSaysSo() {
    // A nested GREATEST of strings repeats its arguments at each level: nine deep fits, ten do
    // not.
    String columns = "s0 STRING, s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, "
        + "s6 STRING, s7 STRING, s8 STRING, s9 STRING, s10 STRING";
    String nested = "s10";
    for (int i = 0; i < 10; i++) {
      if (i == 9) {
        translateCheckOver(columns, nested + " > 'a'");
      }
      nested = "GREATEST(s" + i + ", " + nested + ")";
    }
    String ten = nested;
    ValidationException e = assertThrows(ValidationException.class,
        () -> translateCheckOver(columns, ten + " > 'a'"));
    assertTrue(e.getMessage().contains("CHECK constraint is too large"), e.getMessage());
  }

  @Test
  void aNestedLastCoalesceIsWrittenOnce() {
    // Its NULL is already null, so it is not tested again, which would double it at each level;
    // read as a CASE's value, it's emitted as the flat one is.
    assertEquals(translateCheck("CASE WHEN active THEN COALESCE(a, b, c) ELSE 0 END > 0"),
        translateCheck("CASE WHEN active THEN COALESCE(a, COALESCE(b, c)) ELSE 0 END > 0"));
  }

  @Test
  void nullif() {
    assertEquals("!has(this.a) || dyn(this.a) == null || (has(this.b) && dyn(this.b) != null"
            + " && this.a == this.b) || this.a == 0",
        translateCheck("NULLIF(a, b) = 0"));
  }

  @Test
  void greatestTwoArgs() {
    // NULL when either argument is, as in Flink.
    assertEquals(
        "!has(this.a) || dyn(this.a) == null || !has(this.b) || dyn(this.b) == null ||"
            + " math.greatest(this.a, this.b) > 10",
        translateCheck("GREATEST(a, b) > 10"));
  }

  @Test
  void leastTwoArgs() {
    assertEquals(
        "!has(this.a) || dyn(this.a) == null || !has(this.b) || dyn(this.b) == null ||"
            + " math.least(this.a, this.b) > 0",
        translateCheck("LEAST(a, b) > 0"));
  }

  @Test
  void greatestDoubleCoercesToMathExt() {
    // ratio (double) + x (int) → common DOUBLE; the int operand is cast and the
    // inner selection uses math.greatest on two doubles.
    String cel = translateCheck("GREATEST(ratio, x) > 0.0");
    assertTrue(cel.contains("math.greatest(this.ratio, double(this.x))"),
        "expected double-coerced math.greatest; got: " + cel);
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
    assertEquals("this.email.isEmail()", translateCheckNotNull("IS_EMAIL(email)"));
  }

  @Test
  void isHostname() {
    assertEquals("this.h.isHostname()", translateCheckNotNull("IS_HOSTNAME(h)"));
  }

  @Test
  void isIpv4() {
    assertEquals("this.addr.isIpv4()", translateCheckNotNull("IS_IPV4(addr)"));
  }

  @Test
  void isIpv6() {
    assertEquals("this.addr.isIpv6()", translateCheckNotNull("IS_IPV6(addr)"));
  }

  @Test
  void isUri() {
    assertEquals("this.u.isUri()", translateCheckNotNull("IS_URI(u)"));
  }

  @Test
  void isUriRef() {
    assertEquals("this.u.isUriRef()", translateCheckNotNull("IS_URI_REF(u)"));
  }

  @Test
  void isUuid() {
    assertEquals("this.id.isUuid()", translateCheckNotNull("IS_UUID(id)"));
  }

  @Test
  void validatorsCombined() {
    assertEquals("this.addr.isIpv4() || this.addr.isIpv6()",
        translateCheckNotNull("IS_IPV4(addr) OR IS_IPV6(addr)"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: special-syntax — CAST
  // ---------------------------------------------------------------------

  @Test
  void castInt() {
    assertEquals("int(this.x) > 0", translateCheckNotNull("CAST(x AS INT) > 0"));
  }

  @Test
  void castDouble() {
    assertEquals("double(this.x) > 0.5", translateCheckNotNull("CAST(x AS DOUBLE) > 0.5"));
  }

  @Test
  void castString() {
    assertEquals("string(this.x) == '42'", translateCheckNotNull("CAST(x AS STRING) = '42'"));
  }

  @Test
  void castBoolFromString() {
    // CEL bool() accepts string and bool — STRING column is valid.
    assertEquals("bool(this.name)", translateCheckNotNull("CAST(name AS BOOLEAN)"));
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
    assertEquals("bool(this.active)", translateCheckNotNull("CAST(active AS BOOLEAN)"));
  }

  @Test
  void castBytesFromStringAccepted() {
    // CEL bytes() accepts string and bytes — STRING column is valid.
    // Wrap in a comparison so the boolean-root check is happy.
    assertEquals("bytes(this.name) == b\"\\x00\"",
        translateCheckNotNull("CAST(name AS BYTES) = x'00'"));
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
    assertEquals("string(this.x) == 'foo'",
        translateCheckNotNull("CAST(x AS VARCHAR(64)) = 'foo'"));
  }

  @Test
  void castDecimalAppliesScale() {
    // CAST(x AS DECIMAL(p,s)) now applies the scale via decimals.round and
    // yields a real Decimal; the comparison routes through decimals.gt with
    // the double literal coerced to decimal("1.5").
    assertEquals(
        "decimals.gt(decimals.round(decimal(this.x), 2), decimal(\"1.5\"))",
        translateCheckNotNull("CAST(x AS DECIMAL(10, 2)) > 1.5"));
  }

  // ---------------------------------------------------------------------
  // Decimal (opaque confluent.type.Decimal) — operator + function dispatch.
  // `amount`/`price`/`discount` are DECIMAL columns; `x` is INT, `ratio`
  // DOUBLE. Each assertion also passes the strict cel-java checker via
  // translateCheck (so the emitted decimals.*/math.* type-checks).
  // ---------------------------------------------------------------------

  @Test
  void decimalCompareIntLiteralCoerced() {
    assertEquals("decimals.gt(this.amount, decimal(\"0\"))", translateCheckNotNull("amount > 0"));
  }

  @Test
  void decimalCompareDecimalLiteral() {
    assertEquals("decimals.ge(this.amount, decimal(\"9.99\"))",
        translateCheckNotNull("amount >= 9.99"));
  }

  @Test
  void decimalEquality() {
    assertEquals("decimals.eq(this.amount, this.price)", translateCheckNotNull("amount = price"));
  }

  @Test
  void decimalNotEqual() {
    assertEquals("!decimals.eq(this.amount, this.price)", translateCheckNotNull("amount <> price"));
  }

  @Test
  void decimalAdd() {
    assertEquals("decimals.gt(decimals.add(this.amount, this.discount), this.price)",
        translateCheckNotNull("amount + discount > price"));
  }

  @Test
  void decimalMulLiteralCoerced() {
    assertEquals("decimals.lt(decimals.mul(this.price, decimal(\"2\")), this.amount)",
        translateCheckNotNull("price * 2 < amount"));
  }

  @Test
  void decimalUnaryNeg() {
    assertEquals("decimals.lt(decimals.neg(this.amount), decimal(\"0\"))",
        translateCheckNotNull("-amount < 0"));
  }

  @Test
  void decimalAbs() {
    assertEquals("decimals.lt(decimals.abs(this.amount), decimal(\"100\"))",
        translateCheckNotNull("ABS(amount) < 100"));
  }

  @Test
  void decimalSignReturnsInt() {
    // decimals.sign returns int, so the surrounding compare stays native.
    assertEquals("decimals.sign(this.amount) == -1", translateCheckNotNull("SIGN(amount) = -1"));
  }

  @Test
  void decimalFloor() {
    assertEquals("decimals.ge(decimals.floor(this.amount), decimal(\"0\"))",
        translateCheckNotNull("FLOOR(amount) >= 0"));
  }

  @Test
  void decimalCeiling() {
    assertEquals("decimals.le(decimals.ceil(this.amount), decimal(\"100\"))",
        translateCheckNotNull("CEILING(amount) <= 100"));
  }

  @Test
  void decimalRoundWithScale() {
    assertEquals("decimals.eq(decimals.round(this.amount, 1), this.amount)",
        translateCheckNotNull("ROUND(amount, 1) = amount"));
  }

  @Test
  void decimalTruncate() {
    assertEquals("decimals.le(decimals.trunc(this.price), this.price)",
        translateCheckNotNull("TRUNCATE(price) <= price"));
  }

  @Test
  void decimalSqrt() {
    assertEquals("decimals.lt(decimals.sqrt(this.amount), decimal(\"10\"))",
        translateCheckNotNull("SQRT(amount) < 10"));
  }

  @Test
  void decimalCastToString() {
    assertEquals("string(this.amount) == '1.50'",
        translateCheckNotNull("CAST(amount AS STRING) = '1.50'"));
  }

  @Test
  void decimalCastToDouble() {
    // CAST(decimalExpr AS DOUBLE) → double(<decimal value>) (the (Decimal) ->
    // double overload), symmetric with the STRING cast.
    assertEquals("double(this.amount) > 1.5",
        translateCheckNotNull("CAST(amount AS DOUBLE) > 1.5"));
  }

  @Test
  void decimalBetween() {
    assertEquals(
        "decimals.le(decimal(\"0\"), this.amount) && decimals.le(this.amount, decimal(\"100\"))",
        translateCheckNotNull("amount BETWEEN 0 AND 100"));
  }

  @Test
  void decimalInValueList() {
    assertEquals(
        "decimals.eq(this.amount, decimal(\"1.0\")) || decimals.eq(this.amount, decimal(\"2.5\"))",
        translateCheckNotNull("amount IN (1.0, 2.5)"));
  }

  @Test
  void decimalNestedArithmetic() {
    // (amount + price) * 2 > discount  → exercises paren re-entry into the
    // decimal cascade and chain folding.
    assertEquals(
        "decimals.gt(decimals.mul(decimals.add(this.amount, this.price), decimal(\"2\")), "
            + "this.discount)",
        translateCheckNotNull("(amount + price) * 2 > discount"));
  }

  // Non-decimal numerics route to the CEL math.* extension (same SQL).

  @Test
  void mathAbsOnInt() {
    assertEquals("math.abs(this.x) < 5", translateCheckNotNull("ABS(x) < 5"));
  }

  @Test
  void mathSqrtOnDouble() {
    assertEquals("math.sqrt(this.ratio) < 2.0", translateCheckNotNull("SQRT(ratio) < 2.0"));
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
    String cel = translateCheckNotNull("GREATEST(amount, price) > 0");
    org.junit.jupiter.api.Assertions.assertTrue(
        cel.contains("decimals.greatest(this.amount, this.price)")
            && cel.startsWith("decimals.gt("),
        "expected decimal GREATEST + outer decimal compare; got: " + cel);
  }

  @Test
  void decimalModuloEmitsDecimalsMod() {
    // amount is DECIMAL; `amount % 2` → decimals.mod (remainder, sign of dividend).
    assertEquals(
        "decimals.eq(decimals.mod(this.amount, decimal(\"2\")), decimal(\"0\"))",
        translateCheckNotNull("amount % 2 = 0"));
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
    // The receiver is normalized via timestamp(...) (ts is TIMESTAMP_LTZ).
    assertEquals("timestamp(this.ts).getFullYear() == 2026",
        translateCheckNotNull("EXTRACT(YEAR FROM ts) = 2026"));
  }

  @Test
  void extractMonth() {
    // CEL getMonth() is 0-based; SQL MONTH is 1-based. Translator adds 1.
    assertEquals("timestamp(this.ts).getMonth() + 1 < 12",
        translateCheckNotNull("EXTRACT(MONTH FROM ts) < 12"));
  }

  @Test
  void extractMonthIsOneBased() {
    // Lock in SQL semantics: January is month 1, not 0.
    assertEquals("timestamp(this.ts).getMonth() + 1 == 1",
        translateCheckNotNull("EXTRACT(MONTH FROM ts) = 1"));
  }

  @Test
  void extractDay() {
    assertEquals("timestamp(this.ts).getDate() <= 31",
        translateCheckNotNull("EXTRACT(DAY FROM ts) <= 31"));
  }

  @Test
  void extractEpoch() {
    assertEquals("int(timestamp(this.ts)) > 0",
        translateCheckNotNull("EXTRACT(EPOCH FROM ts) > 0"));
  }

  @Test
  void extractCaseInsensitiveField() {
    assertEquals("timestamp(this.ts).getFullYear() == 2026",
        translateCheckNotNull("EXTRACT(year FROM ts) = 2026"));
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
    assertEquals("this.name.substring(0, 3) == 'foo'",
        translateCheckNotNull("SUBSTRING(name FROM 1 FOR 3) = 'foo'"));
  }

  @Test
  void substringFromOnly() {
    assertEquals("this.name.substring(1) == 'oo'",
        translateCheckNotNull("SUBSTRING(name FROM 2) = 'oo'"));
  }

  @Test
  void substringCommaForm() {
    assertEquals("this.name.substring(0, 3) == 'foo'",
        translateCheckNotNull("SUBSTRING(name, 1, 3) = 'foo'"));
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
    assertEquals("this.name.substring(this.lo - 1, this.lo - 1 + 3) == 'foo'",
        translateCheckNotNull("SUBSTRING(name FROM lo FOR 3) = 'foo'"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: special-syntax — POSITION
  // ---------------------------------------------------------------------

  @Test
  void position() {
    // POSITION's whole emit is wrapped so a tighter outer operator
    // (e.g. `POSITION(...) * 2`) can't mis-bind the trailing `+ 1`.
    assertEquals("this.haystack.indexOf(this.needle) + 1 > 0",
        translateCheckNotNull("POSITION(needle IN haystack) > 0"));
  }

  @Test
  void positionWithLiterals() {
    assertEquals("this.name.indexOf('@') + 1 > 0",
        translateCheckNotNull("POSITION('@' IN name) > 0"));
  }

  @Test
  void positionInsideMultiplicationParenthesizedAgainstPrecedence() {
    // Regression: without the outer wrap, `POSITION(...) * 2` would emit
    // `indexOf + 1 * 2` and CEL would parse as `indexOf + 2` (multiplicative
    // binds tighter), losing the `* 2` factor on the indexOf part.
    assertEquals("(this.name.indexOf('@') + 1) * 2 == 4",
        translateCheckNotNull("POSITION('@' IN name) * 2 = 4"));
  }

  @Test
  void positionParsesAnywhereAsBareCheckExpr() {
    // Parsed on its own (as KTable does), a check_expr needle let ANTLR take
    // `'@' IN name` as the needle and then fail at the closing parenthesis.
    for (String sql : List.of("2 = POSITION('@' IN name)",
        "qty BETWEEN 1 AND POSITION('@' IN name)", "POSITION('@' IN name) = 2")) {
      for (PredictionMode mode : List.of(PredictionMode.SLL, PredictionMode.LL)) {
        LogicalTypesParser parser = LogicalTypesParserFactory.newParser(sql);
        parser.getInterpreter().setPredictionMode(mode);
        parser.check_expr();
        assertEquals(Token.EOF, parser.getCurrentToken().getType(), sql + " in " + mode);
      }
    }
  }

  @Test
  void positionNeedleWithAnOperatorNeedsParentheses() {
    org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheckNotNull("POSITION('a' || 'b' IN name) > 0"));
    assertEquals("this.name.indexOf('a' + 'b') + 1 > 0",
        translateCheckNotNull("POSITION(('a' || 'b') IN name) > 0"));
  }

  @Test
  void positionFrom() {
    assertEquals("!has(this.name) || dyn(this.name) == null || !has(this.qty) || dyn(this.qty) =="
            + " null || (this.qty > size(this.name) ? 0 : this.name.indexOf('@', this.qty <= 1 ? 0"
            + " : this.qty - 1) + 1) > 0",
        translateCheck("POSITION('@' IN name FROM qty) > 0"));
  }

  @Test
  void positionFromOfANullIsNull() throws Exception {
    // An empty needle or a start past the end gives its answer without the other operands, so
    // a NULL one is tested first: read as a CASE's value, it's NULL, and the CHECK passes.
    String columns = "s STRING, ns STRING, ni INT";
    Map<String, Object> present = new HashMap<>();
    present.put("s", "abc");
    present.put("ns", null);
    present.put("ni", null);
    for (Map<String, Object> self : List.of(present, Map.<String, Object>of("s", "abc"))) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("CASE WHEN s = 'abc' THEN POSITION('' IN ns FROM 1) ELSE 0 END = 7",
          "CASE WHEN s = 'abc' THEN POSITION('' IN s FROM ni) ELSE 0 END = 7",
          "CASE WHEN s = 'abc' THEN POSITION(ns IN s FROM 9) ELSE 0 END = 7",
          "POSITION('' IN s FROM 1) = 1")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(
            CelUtils.ScriptType.JSON, cel, null, CelUtils.toVarDecls(CelUtils.toDeclTypes(vars)))
            .eval(vars), sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void positionFromMatchesFlink() throws Exception {
    // Each answer is Flink's SqlFunctionUtils.position for the same arguments.
    for (String sql : List.of("POSITION('b' IN 'abcb' FROM 3) = 4",
        "POSITION('b' IN 'abcb' FROM 0) = 2", "POSITION('b' IN 'abcb' FROM -5) = 2",
        "POSITION('b' IN 'abcb' FROM 4) = 4", "POSITION('b' IN 'abcb' FROM 5) = 0",
        "POSITION('b' IN 'abcb' FROM 9) = 0", "POSITION('' IN 'abc' FROM 9) = 1",
        "POSITION('b' IN '' FROM 1) = 0", "POSITION('b' IN 'aébéb' FROM 4) = 5",
        "POSITION('b' IN 'abcb' FROM -9223372036854775807 - 1) = 2")) {
      String cel = translateCheck(sql);
      assertEquals(true, evalConstant(cel), sql + " -> " + cel);
    }
  }

  private static Object evalConstant(String cel) throws Exception {
    CelCompiler compiler = CelCompilerFactory.standardCelCompilerBuilder()
        .addLibraries(CelExtensions.strings()).build();
    CelRuntime runtime = CelRuntimeFactory.standardCelRuntimeBuilder()
        .addLibraries(CelExtensions.strings()).build();
    return runtime.createProgram(compiler.compile(cel).getAst()).eval();
  }

  @Test
  void dashDashStartsALineComment() {
    assertEquals("this.x > 1", translateCheckNotNull("x > 1 -- not 1 - -1\n"));
    assertEquals("this.x - -1 > 0", translateCheckNotNull("x - -1 > 0"));
  }

  // ---------------------------------------------------------------------
  // Phase 4: special-syntax — TRIM
  // ---------------------------------------------------------------------

  @Test
  void trimDefault() {
    // TRIM(s) — strip whitespace
    assertEquals("this.name.trim() == 'foo'", translateCheckNotNull("TRIM(name) = 'foo'"));
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
    // created_at (TIMESTAMP_LTZ) is normalized; CURRENT_TIMESTAMP (now) is
    // already a CEL timestamp and stays unwrapped.
    assertEquals("timestamp(this.created_at) <= now",
        translateCheckNotNull("created_at <= CURRENT_TIMESTAMP"));
  }

  @Test
  void currentTimestampWithExtract() {
    // EXTRACT receiver is CURRENT_TIMESTAMP (now) — left unwrapped.
    assertEquals("now.getFullYear() >= 2026",
        translateCheck("EXTRACT(YEAR FROM CURRENT_TIMESTAMP) >= 2026"));
  }

  @Test
  void timestampColumnVsColumn() {
    // Both operands are TIMESTAMP_LTZ → both normalized; operator stays native.
    assertEquals("timestamp(this.ts) < timestamp(this.created_at)",
        translateCheckNotNull("ts < created_at"));
  }

  @Test
  void timestampBetween() {
    assertEquals(
        "timestamp(this.created_at) <= timestamp(this.ts) "
            + "&& timestamp(this.ts) <= now",
        translateCheckNotNull("ts BETWEEN created_at AND CURRENT_TIMESTAMP"));
  }

  @Test
  void extractHour() {
    assertEquals("timestamp(this.ts).getHours() >= 0",
        translateCheckNotNull("EXTRACT(HOUR FROM ts) >= 0"));
  }

  // ---------------------------------------------------------------------
  // Temporal literals (TIMESTAMP '…') + INTERVAL arithmetic. `ts`/`created_at`
  // are TIMESTAMP_LTZ columns. All assertions also pass the strict cel-java
  // checker via translateCheck.
  // ---------------------------------------------------------------------

  @Test
  void timestampLiteralComparison() {
    assertEquals(
        "timestamp(this.ts) >= timestamp(\"2020-01-01T00:00:00Z\")",
        translateCheckNotNull("ts >= TIMESTAMP '2020-01-01 00:00:00'"));
  }

  @Test
  void timestampLiteralWithOffsetPassesThrough() {
    assertEquals(
        "timestamp(this.ts) < timestamp(\"2020-01-01T00:00:00+02:00\")",
        translateCheckNotNull("ts < TIMESTAMP '2020-01-01T00:00:00+02:00'"));
  }

  @Test
  void intervalRelativeWindow() {
    // The dominant idiom: "within the last 7 days".
    assertEquals(
        "timestamp(this.created_at) > now - duration(\"604800s\")",
        translateCheckNotNull("created_at > CURRENT_TIMESTAMP - INTERVAL '7' DAY"));
  }

  @Test
  void intervalPlusTimestampCommutative() {
    assertEquals(
        "duration(\"86400s\") + timestamp(this.created_at) > timestamp(this.ts)",
        translateCheckNotNull("INTERVAL '1' DAY + created_at > ts"));
  }

  @Test
  void intervalOnTimestampColumn() {
    // Full arithmetic: interval applied to a column (not just CURRENT_TIMESTAMP).
    assertEquals(
        "timestamp(this.created_at) - duration(\"86400s\") < timestamp(this.ts)",
        translateCheckNotNull("created_at - INTERVAL '1' DAY < ts"));
  }

  @Test
  void timestampMinusTimestampYieldsDuration() {
    // T - T → duration; compared against an INTERVAL (elapsed-time check).
    assertEquals(
        "timestamp(this.ts) - timestamp(this.created_at) > duration(\"3600s\")",
        translateCheckNotNull("ts - created_at > INTERVAL '1' HOUR"));
  }

  @Test
  void intervalUnitsConvertToSeconds() {
    assertEquals("timestamp(this.ts) > now - duration(\"90s\")",
        translateCheckNotNull("ts > CURRENT_TIMESTAMP - INTERVAL '90' SECOND"));
    assertEquals("timestamp(this.ts) > now - duration(\"120s\")",
        translateCheckNotNull("ts > CURRENT_TIMESTAMP - INTERVAL '2' MINUTE"));
  }

  @Test
  void intervalYearRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("created_at > CURRENT_TIMESTAMP - INTERVAL '1' YEAR"));
    org.junit.jupiter.api.Assertions.assertTrue(
        t.getMessage().contains("INTERVAL YEAR is not supported"),
        "expected YEAR rejection; got: " + t.getMessage());
  }

  @Test
  void intervalBadUnitRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("ts > CURRENT_TIMESTAMP - INTERVAL '1' FORTNIGHT"));
    org.junit.jupiter.api.Assertions.assertTrue(
        t.getMessage().contains("Unsupported INTERVAL unit"),
        "expected bad-unit rejection; got: " + t.getMessage());
  }

  @Test
  void intervalNonIntegerRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("ts > CURRENT_TIMESTAMP - INTERVAL '1.5' DAY"));
    org.junit.jupiter.api.Assertions.assertTrue(
        t.getMessage().contains("must be an integer"),
        "expected non-integer rejection; got: " + t.getMessage());
  }

  @Test
  void timestampLiteralMalformedRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("ts >= TIMESTAMP 'not-a-date'"));
    org.junit.jupiter.api.Assertions.assertTrue(
        t.getMessage().contains("Invalid TIMESTAMP literal"),
        "expected malformed-timestamp rejection; got: " + t.getMessage());
  }

  @Test
  void timestampPlusTimestampRejectedByChecker() {
    // T + T has no CEL overload — caught at strict-check (registration) time.
    org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("ts + created_at <= CURRENT_TIMESTAMP"));
  }

  @Test
  void timestampLiteralBetween() {
    assertEquals(
        "timestamp(\"2020-01-01T00:00:00Z\") <= timestamp(this.ts) "
            + "&& timestamp(this.ts) <= now",
        translateCheckNotNull("ts BETWEEN TIMESTAMP '2020-01-01 00:00:00' AND CURRENT_TIMESTAMP"));
  }

  // Phase B: temporal operands in IN / CASE / COALESCE / NULLIF / GREATEST / LEAST.

  @Test
  void timestampInValueList() {
    assertEquals(
        "timestamp(this.ts) in [timestamp(\"2020-01-01T00:00:00Z\"), "
            + "timestamp(\"2021-01-01T00:00:00Z\")]",
        translateCheckNotNull("ts IN (TIMESTAMP '2020-01-01 00:00:00', "
            + "TIMESTAMP '2021-01-01 00:00:00')"));
  }

  @Test
  void timestampGreatest() {
    String cel = translateCheck("GREATEST(ts, created_at) <= CURRENT_TIMESTAMP");
    org.junit.jupiter.api.Assertions.assertTrue(
        cel.contains("timestamp(this.ts) > timestamp(this.created_at)")
            && cel.contains("<= now"),
        "expected timestamp GREATEST; got: " + cel);
  }

  @Test
  void timestampCoalesce() {
    String cel = translateCheck("COALESCE(ts, created_at) <= CURRENT_TIMESTAMP");
    org.junit.jupiter.api.Assertions.assertTrue(
        cel.contains("timestamp(this.ts)")
            && cel.contains("timestamp(this.created_at)")
            && cel.contains("<= now"),
        "expected timestamp COALESCE; got: " + cel);
  }

  @Test
  void timestampCaseSubject() {
    String cel = translateCheck(
        "CASE ts WHEN TIMESTAMP '2020-01-01 00:00:00' THEN 1 ELSE 0 END = 1");
    org.junit.jupiter.api.Assertions.assertTrue(
        cel.contains("timestamp(this.ts) == timestamp(\"2020-01-01T00:00:00Z\")"),
        "expected timestamp simple-CASE; got: " + cel);
  }

  // ---------------------------------------------------------------------
  // Phase 5: EVERY / ANY / ONE macros
  // ---------------------------------------------------------------------

  @Test
  void everyAllPositive() {
    String script = "STRUCT T ("
        + "tags STRING ARRAY,"
        + "CHECK (EVERY(tags, t, LENGTH(t) > 0))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.tags) || dyn(this.tags) == null || this.tags.all(t, dyn(t) == null ||"
            + " size(t) > 0)", schema.getRules().get(0).getExpr());
  }

  @Test
  void anyExists() {
    String script = "STRUCT T ("
        + "tags STRING ARRAY,"
        + "CHECK (ANY(tags, t, t = 'admin'))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.tags) || dyn(this.tags) == null || this.tags.exists(t, dyn(t) == null"
            + " || t == 'admin')", schema.getRules().get(0).getExpr());
  }

  @Test
  void oneExistsOne() {
    String script = "STRUCT T ("
        + "addresses STRING NOT NULL ARRAY NOT NULL,"
        + "CHECK (ONE(addresses, a, STARTS_WITH(a, 'primary:')))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("this.addresses.exists_one(a, a.startsWith('primary:'))",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void macroCaseInsensitive() {
    String script = "STRUCT T ("
        + "tags STRING ARRAY,"
        + "CHECK (every(tags, t, LENGTH(t) > 0))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.tags) || dyn(this.tags) == null || this.tags.all(t, dyn(t) == null ||"
            + " size(t) > 0)", schema.getRules().get(0).getExpr());
  }

  @Test
  void macroNested() {
    // Iteration var can't be `row` — STRUCT is a reserved type keyword
    String script = "STRUCT T ("
        + "matrix INT ARRAY ARRAY,"
        + "CHECK (EVERY(matrix, r, EVERY(r, cell, cell > 0)))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.matrix) || dyn(this.matrix) == null || this.matrix.all(r, dyn(r) =="
            + " null || r.all(cell, dyn(cell) == null || cell > 0))",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void macroWithComplexPredicate() {
    String script = "STRUCT T ("
        + "ages INT ARRAY,"
        + "CHECK (ANY(ages, a, a BETWEEN 18 AND 65))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.ages) || dyn(this.ages) == null || this.ages.exists(a, dyn(a) == null"
            + " || (18 <= a && a <= 65))",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void macroWithFormatValidator() {
    String script = "STRUCT T ("
        + "emails STRING ARRAY,"
        + "CHECK (EVERY(emails, e, IS_EMAIL(e)))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.emails) || dyn(this.emails) == null || this.emails.all(e, dyn(e) =="
            + " null || e.isEmail())",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void aMapQuantifierBindsEachKey() throws Exception {
    // CEL's all/exists/exists_one over a map bind its keys; a value is reached by m[k].
    String columns = "m MAP<STRING, INT NOT NULL> NOT NULL, s STRING";
    assertEquals("this.m.all(k, k != 'zz')",
        translateCheckOver(columns, "EVERY(m, k, k <> 'zz')"));
    // A string key is never NULL, so testing it is dead code, as for a NOT NULL column.
    for (String sql : List.of("EVERY(m, k, k IS NULL)", "ANY(m, k, k IS NOT NULL)")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, sql), sql);
      assertTrue(e.getMessage().contains("dead code"), e.getMessage());
    }
    ValidationException multiset = assertThrows(ValidationException.class,
        () -> translateCheckOver("ms MULTISET<STRING> NOT NULL", "EVERY(ms, e, e IS NULL)"));
    assertTrue(multiset.getMessage().contains("dead code"), multiset.getMessage());
    Map<String, Object> self = new HashMap<>();
    self.put("m", Map.of("a", 1, "b", 2));
    self.put("s", "b");
    Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
    for (String sql : List.of("EVERY(m, k, k <> 'zz')", "ANY(m, k, k = s)",
        "ONE(m, k, k = 'a')", "EVERY(m, k, m[k] > 0)", "ONE(m, k, LENGTH(k) = 1 AND m[k] = 2)")) {
      String cel = translateCheckOver(columns, sql);
      assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
          CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars), sql + " -> " + cel);
    }
    // A key the quantifier binds is in the map it ranges over, so isn't tested for; one bound
    // over another map is.
    assertEquals("this.m.all(k, this.m[k] > 0)",
        translateCheckOver(columns, "EVERY(m, k, m[k] > 0)"));
    assertEquals("this.n.all(k, !(k in this.m) || this.m[k] > 0)",
        translateCheckOver(columns + ", n MAP<STRING, INT NOT NULL> NOT NULL",
            "EVERY(n, k, m[k] > 0)"));
    // Nor is one bound over a map its own outer namesake names.
    String nested = translateCheckOver("mm MAP<STRING, MAP<STRING, INT NOT NULL> NOT NULL> NOT NULL",
        "EVERY(mm, k, EVERY(mm[k], k, mm[k][k] > 0))");
    assertEquals("this.mm.all(k, this.mm[k].all(k, !(k in this.mm) || !(k in dyn(this.mm[k]))"
        + " || this.mm[k][k] > 0))", nested);
    assertEquals(true,
        evalOver(nested, Map.of("mm", Map.of("a", Map.of("b", 1), "b", Map.of("x", 2)))), nested);
    // A key compared as its value is refused.
    assertThrows(ValidationException.class,
        () -> translateCheckOver(columns, "EVERY(m, k, k > 0)"));
  }

  @Test
  void macroRejectsExpressionAsIterVar() {
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T (x INT, CHECK (foo > 0));";
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT) NOT NULL CHECK (addr.zip > 0)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.zip) || dyn(this.zip) == null || this.zip > 0",
        schema.getField("addr").getRules().get(0).getExpr());
  }

  @Test
  void columnLevelBareIsNullRejected() {
    // Column-level CHECK gets the skip-on-null contract from the runtime —
    // the CEL is never invoked when the column value is null/missing, so
    // `this` is guaranteed non-null at evaluation time. A bare `<col> IS
    // NULL` test can therefore never fire and is rejected as dead code.
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
        + "a INT, b INT,"
        + "CHECK (a < b)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.a) || dyn(this.a) == null || !has(this.b) || dyn(this.b) == null ||"
            + " this.a < this.b", schema.getRules().get(0).getExpr());
  }

  @Test
  void rejectsNonBooleanRoot() {
    String script = "STRUCT T (x INT, CHECK (x + 1));";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("must yield a boolean"));
  }

  @Test
  void rejectsNonBooleanLiteralRoot() {
    String script = "STRUCT T (x INT, CHECK ('foo'));";
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript(script));
    assertTrue(t.getMessage().contains("must yield a boolean"));
  }

  @Test
  void acceptsBooleanColumnRef() {
    String script = "STRUCT T (active BOOLEAN, CHECK (active));";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.active) || dyn(this.active) == null || this.active",
        schema.getRules().get(0).getExpr());
  }

  @Test
  void acceptsBooleanLiteralRoot() {
    String script = "STRUCT T (x INT, CHECK (TRUE));";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("true", schema.getRules().get(0).getExpr());
  }

  @Test
  void rejectsOrderingOnBoolean() {
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
  void caseResultMixedNumericCoerced() {
    // CASE returns int (1/2); compared to 1.5 (decimal) → common DECIMAL,
    // so the int CASE result is wrapped with decimal() (no longer rejected).
    String cel = translateCheck("(CASE WHEN active THEN 1 ELSE 2 END) > 1.5");
    assertTrue(cel.startsWith("decimals.gt(") && cel.contains("decimal(\"1.5\")"),
        "expected decimal-coerced CASE comparison, got: " + cel);
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
        "!has(this.tags) || dyn(this.tags) == null",
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
  void doubleArithmeticResultCoerced() {
    // ratio (double) + 1 (int) → DOUBLE; compared to 0.0 (decimal literal)
    // → common DOUBLE. The int operand emits as `1.0`.
    assertEquals("this.ratio + 1.0 > 0.0", translateCheckNotNull("ratio + 1 > 0.0"));
  }

  @Test
  void mixedIntDecimalArithmeticCoerced() {
    // x (int) + 1.5 (decimal) → common DECIMAL; compared to 0 (int) →
    // DECIMAL. Both int operands are wrapped with decimal().
    assertEquals(
        "decimals.gt(decimals.add(decimal(this.x), decimal(\"1.5\")), decimal(\"0\"))",
        translateCheckNotNull("x + 1.5 > 0"));
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
  void intVsDoubleComparisonCoerced() {
    // int vs double now coerces to the common type (double) — the int side
    // is cast with double() (Flink-faithful; no longer rejected).
    assertEquals("double(this.x) < this.ratio", translateCheckNotNull("x < ratio"));
  }

  // ---- numeric coercion: common-type comparison ----

  @Test
  void doubleVsIntComparisonCoerced() {
    // DOUBLE vs INT → common DOUBLE; the int literal emits as 5.0.
    assertEquals("this.ratio == 5.0", translateCheckNotNull("ratio = 5"));
  }

  @Test
  void intVsDecimalLiteralComparisonIsDecimal() {
    // INT vs DECIMAL literal (no exponent) → common DECIMAL.
    assertEquals("decimals.gt(decimal(this.qty), decimal(\"1.5\"))",
        translateCheckNotNull("qty > 1.5"));
  }

  @Test
  void intVsDecimalLiteralEqualityIsDecimal() {
    // 12.34 is a DECIMAL literal → INT coerces up to DECIMAL.
    assertEquals("decimals.eq(decimal(this.x), decimal(\"12.34\"))",
        translateCheckNotNull("x = 12.34"));
  }

  @Test
  void decimalColVsDecimalLiteralIsDecimal() {
    assertEquals("decimals.eq(this.amount, decimal(\"12.34\"))",
        translateCheckNotNull("amount = 12.34"));
  }

  @Test
  void decimalColVsDoubleColIsDouble() {
    // DECIMAL vs DOUBLE → common DOUBLE (Flink-faithful) via the
    // double(Decimal) overload — not an exact-decimal compare.
    assertEquals("double(this.amount) == this.ratio",
        translateCheckNotNull("amount = ratio"));
  }

  @Test
  void decimalColVsExponentLiteralIsDouble() {
    // An exponent literal (1.5e2) is approximate → DOUBLE; DECIMAL coerces up.
    assertEquals("double(this.amount) == 1.5e2",
        translateCheckNotNull("amount = 1.5e2"));
  }

  @Test
  void intArithmeticWithExponentLiteralIsDouble() {
    // x (int) + 1.5e0 (double) → DOUBLE; > 10 (int) → DOUBLE.
    assertEquals("double(this.x) + 1.5e0 > 10.0",
        translateCheckNotNull("x + 1.5e0 > 10"));
  }

  @Test
  void nullTestsCoverOnlyTheNullableSteps() {
    // A NOT NULL step is present whenever its parent is, and Protobuf's has() is false for a
    // zero, so only the deepest nullable step of a path is tested.
    String columns = "a INT NOT NULL, n INT, r STRUCT(x INT NOT NULL, y INT)";
    assertEquals("this.a == 0", translateCheckOver(columns, "COALESCE(a, 7) = 0"));
    assertEquals("has(this.n) && dyn(this.n) != null && this.n == 0",
        translateCheckOver(columns, "COALESCE(n, 7) = 0"));
    assertEquals("has(this.r) && dyn(this.r) != null && this.r.x == 0",
        translateCheckOver(columns, "COALESCE(r.x, 7) = 0"));
    assertEquals("!has(this.n) || dyn(this.n) == null || math.greatest(this.a, this.n) == 0",
        translateCheckOver(columns, "GREATEST(a, n) = 0"));
    assertEquals("!has(this.r) || dyn(this.r) == null",
        translateCheckOver(columns, "r.x IS NULL"));
    assertEquals("has(this.r) && dyn(this.r) != null && has(this.r.y) && dyn(this.r.y) != null", translateCheckOver(columns, "r.y IS NOT NULL"));
    // A parenthesized selection, and a macro variable's fields, the same way.
    assertEquals("has(this.r) && dyn(this.r) != null && this.r.x == 0",
        translateCheckOver(columns, "COALESCE((r).x, 7) = 0"));
    String arrays = "items STRUCT(x INT NOT NULL) NOT NULL ARRAY NOT NULL, "
        + "maybe STRUCT(x INT NOT NULL) ARRAY NOT NULL";
    assertEquals("this.items.all(e, e.x == 0)",
        translateCheckOver(arrays, "EVERY(items, e, COALESCE(e.x, 7) = 0)"));
    assertEquals("this.maybe.all(e, dyn(e) != null && e.x == 0)",
        translateCheckOver(arrays, "EVERY(maybe, e, COALESCE(e.x, 7) = 0)"));
  }

  @Test
  void aNullStructDecidesANullTestOfItsFields() throws Exception {
    // Avro and JSON give a null struct as null, which no field can be selected from, so each
    // nullable step is tested, outermost first.
    String columns = "r STRUCT(f0 INT, q STRUCT(g INT))";
    assertEquals("!has(this.r) || dyn(this.r) == null || !has(this.r.f0) || dyn(this.r.f0) == null", translateCheckOver(columns, "r.f0 IS NULL"));
    Map<String, Object> self = new HashMap<>();
    self.put("r", null);
    Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
    for (String sql : List.of("r.f0 IS NULL", "NOT (r.q.g IS NOT NULL)", "COALESCE(r.f0, 7) = 7",
        "GREATEST(r.q.g, 1) IS NULL", "CASE WHEN r.f0 IS NULL THEN 1 ELSE 2 END = 1")) {
      String cel = translateCheckOver(columns, sql);
      assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
          CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars), sql + " -> " + cel);
    }
  }

  @Test
  void greatestOfANullWithACastIsNull() throws Exception {
    // With an argument NULL the result is NULL, not a cast of NULL, so an outer GREATEST or
    // LEAST is NULL too and COALESCE skips it.
    String columns = "a INT, x DOUBLE, d DECIMAL(10, 2), t TIMESTAMP_LTZ(3), u TIMESTAMP_LTZ(3)";
    Map<String, Object> self = new HashMap<>();
    self.put("a", 5);
    self.put("x", null);
    self.put("d", null);
    self.put("t", null);
    self.put("u", null);
    Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
    for (String sql : List.of("GREATEST(a, GREATEST(x, d)) IS NULL",
        "LEAST(a, LEAST(d, x)) IS NULL", "COALESCE(GREATEST(x, a), 7) = 7",
        "COALESCE(LEAST(d, x), 7) = 7",
        "COALESCE(GREATEST(x, d), 7) = 7", "COALESCE(LEAST(t, u), TIMESTAMP '2020-01-01 00:00:00') "
            + "= TIMESTAMP '2020-01-01 00:00:00'")) {
      String cel = translateCheckOver(columns, sql);
      assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
          CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars), sql + " -> " + cel);
    }
  }

  @Test
  void greatestOfANestedOrAbsentNullIsNull() throws Exception {
    // A NULL argument of a narrower type is tested before it is widened and, as JSON leaves a
    // NULL field out, before it is read, so a nested GREATEST or LEAST is NULL too.
    String columns = "a INT, b BIGINT, x DOUBLE, d DECIMAL(10, 2), r STRUCT(x INT)";
    Map<String, Object> present = new HashMap<>();
    present.put("a", null);
    present.put("b", null);
    present.put("x", 0.5);
    present.put("d", null);
    present.put("r", null);
    for (Map<String, Object> self : List.of(present, Map.<String, Object>of("x", 0.5))) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("GREATEST(x, GREATEST(a, b)) IS NULL",
          "LEAST(x, LEAST(b, a)) IS NULL", "GREATEST(GREATEST(x, a), x) IS NULL",
          "COALESCE(LEAST(d, LEAST(a, b)), 7) = 7", "COALESCE(GREATEST(a, d), 7) = 7",
          "COALESCE(GREATEST(r.x, d), 7) = 7", "COALESCE(LEAST(b, d), 7) = 7")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self + " -> " + cel);
      }
    }
  }

  @Test
  void aNullGreatestArgumentIsNull() throws Exception {
    // GREATEST and LEAST are NULL when any argument is, as in Flink, not the other argument.
    String columns = "i INT NOT NULL, ni INT, nb BIGINT, nd DECIMAL(10, 2), b BIGINT";
    Map<String, Object> absent = Map.of("i", 1, "b", 40000L);
    Map<String, Object> present = new HashMap<>(absent);
    for (String col : List.of("ni", "nb", "nd")) {
      present.put(col, null);
    }
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("LEAST(2.5, ni) IS NULL", "GREATEST(nd, i) IS NULL",
          "NOT (COALESCE(GREATEST(nb, i), 0) > 0)",
          "CASE WHEN GREATEST(ni, b) > 0 THEN 1 ELSE 2 END = 2",
          "GREATEST(GREATEST(i, nb), i) IS NULL")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aNullCaseResultIsNull() throws Exception {
    // A NULL branch is tested before it is widened and, as JSON leaves a NULL field out, before
    // it is read, so IS NULL and COALESCE see the CASE's NULL.
    String columns = "t BOOLEAN NOT NULL, nd DECIMAL(10, 2), i INT, ni INT, x DOUBLE";
    Map<String, Object> present = new HashMap<>();
    present.put("t", false);
    present.put("nd", null);
    present.put("i", 0);
    present.put("ni", null);
    present.put("x", null);
    for (Map<String, Object> self : List.of(present, Map.<String, Object>of("t", false, "i", 0))) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("CASE WHEN t THEN nd ELSE NULLIF(i, 0) END IS NULL",
          "CASE WHEN t THEN nd ELSE ni END IS NULL",
          "COALESCE(CASE WHEN t THEN nd ELSE ni END, -1) = -1",
          "CASE WHEN t THEN x ELSE ni END IS NULL", "CASE WHEN t THEN ni ELSE ni END IS NULL")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self + " -> " + cel);
      }
    }
  }

  @Test
  void aNullNullifArgumentIsTestedFirst() throws Exception {
    // A NULL first argument gives NULL and a NULL second gives the first, before either is
    // compared, widened or, as JSON leaves a NULL field out, read.
    String columns = "d DECIMAL(10, 2) NOT NULL, nd DECIMAL(10, 2), i INT NOT NULL, ni INT, "
        + "s STRING NOT NULL, ns STRING, nx DOUBLE, nb BIGINT";
    Map<String, Object> absent = new HashMap<>();
    absent.put("d", CelDecimal.of(new BigDecimal("1.50")));
    absent.put("i", 1);
    absent.put("s", "a");
    Map<String, Object> present = new HashMap<>(absent);
    for (String c : List.of("nd", "ni", "ns", "nx", "nb")) {
      present.put(c, null);
    }
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("NULLIF(d, nd) = d", "NULLIF(i, ni) = i", "NULLIF(s, ns) = s",
          "NULLIF(nd, d) IS NULL", "NULLIF(ni, nd) IS NULL",
          "COALESCE(NULLIF(nx, nb), -1) <> 2.5")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void anExpressionOfANullIsNull() throws Exception {
    // An operator or function of a NULL is NULL, so it is tested by its inputs before it's
    // evaluated, which fails on a NULL or, as JSON leaves a NULL field out, a missing key.
    String columns = "ni INT, ns STRING, nd DECIMAL(10, 2), arr INT ARRAY NOT NULL";
    assertEquals("!has(this.ni) || dyn(this.ni) == null || dyn(this.ni + 1) == null",
        translateCheckOver(columns, "ni + 1 IS NULL"));
    Map<String, Object> absent = new HashMap<>();
    absent.put("arr", List.of(1));
    Map<String, Object> present = new HashMap<>(absent);
    for (String col : List.of("ni", "ns", "nd")) {
      present.put(col, null);
    }
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("ni + 1 IS NULL", "NOT (ni + 1 IS NOT NULL)",
          "COALESCE(ni + 1, 0) = 0", "COALESCE(UPPER(ns), 'x') = 'x'",
          "LEAST(LENGTH(ns), 2.5) IS NULL", "(ni = 1) IS NULL", "-ni IS NULL",
          "(ns || 'x') IS NULL", "(ns LIKE 'a%') IS NULL", "nd * 2 IS NULL",
          "EVERY(arr, e, (e > nd) IS NULL)")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aNullCaseConditionFallsThrough() throws Exception {
    // A condition, or a simple CASE's match, that is NULL takes no branch, as SQL takes one only
    // when TRUE; NOT, AND and OR keep SQL's three values.
    String columns = "ni INT, nt BOOLEAN, nd DECIMAL(10, 2), ns STRING, t BOOLEAN NOT NULL";
    Map<String, Object> absent = Map.of("t", true);
    Map<String, Object> present = new HashMap<>(absent);
    for (String col : List.of("ni", "nt", "nd", "ns")) {
      present.put(col, null);
    }
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("CASE WHEN ni > 0 THEN 1 ELSE 2 END = 2",
          "CASE WHEN nt THEN 1 ELSE 2 END = 2", "CASE nd WHEN 1.5 THEN 1 ELSE 2 END = 2",
          "CASE ns WHEN 'a' THEN 1 ELSE 2 END = 2",
          "NOT (CASE ns WHEN ns THEN 1 WHEN 'a' THEN 2 ELSE 3 END <= 2.5)",
          "CASE WHEN NOT (ni > 0) THEN 1 ELSE 2 END = 2",
          "CASE WHEN ni > 0 OR t THEN 1 ELSE 2 END = 1",
          "CASE WHEN ni > 0 AND t THEN 1 ELSE 2 END = 2",
          "CASE WHEN (ni > 0 OR NOT t) THEN 1 ELSE 2 END = 2")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void anInOverANullIsThreeValued() throws Exception {
    // An IN is TRUE on a match, else NULL when its subject or an element is NULL; each element is
    // tested before it is compared, widened or, as JSON leaves a NULL field out, read.
    String columns = "x DOUBLE NOT NULL, nd DECIMAL(10, 2), i INT NOT NULL, ni INT, nx DOUBLE, "
        + "nb BIGINT, r STRUCT(a BIGINT), s STRING NOT NULL, ns STRING";
    assertEquals("this.i == 1 || (has(this.ni) && dyn(this.ni) != null && this.i == this.ni)"
        + " ? true : !has(this.ni) || dyn(this.ni) == null",
        translateCheckOver(columns, "i IN (1, ni)"));
    Map<String, Object> absent = new HashMap<>();
    absent.put("x", 0.5);
    absent.put("i", 1);
    absent.put("s", "a");
    Map<String, Object> present = new HashMap<>(absent);
    for (String col : List.of("nd", "ni", "nx", "nb", "r", "ns")) {
      present.put(col, null);
    }
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("x IN (x, nd)", "i IN (1, ni)", "1 IN (1, r.a)",
          "(1 NOT IN (nb, r.a)) IS NULL", "(i IN (2, ni)) IS NULL", "(nx IN (0, 0)) IS NULL",
          "(ns NOT IN (ns, s)) IS NULL", "COALESCE(i IN (2, ni), TRUE) = TRUE",
          "CASE WHEN i IN (2, ni) THEN 1 ELSE 2 END = 2",
          "CASE WHEN i NOT IN (2, ni) THEN 1 ELSE 2 END = 2",
          "CASE WHEN nx IN (0, 1) THEN 1 ELSE 2 END = 2")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aNullCaseOrNullifInsideAnExpressionIsNull() throws Exception {
    // CASE, NULLIF and an IN with an element that may be NULL give NULL as null, so one inside
    // an expression, an IN or a comparison is tested by its value, not evaluated through it.
    String columns = "i INT NOT NULL, d DECIMAL(10, 2) NOT NULL, x DOUBLE NOT NULL, "
        + "s STRING NOT NULL, t BOOLEAN NOT NULL, nx DOUBLE, ns STRING, b BIGINT, "
        + "r STRUCT(a INT)";
    Map<String, Object> absent = new HashMap<>();
    absent.put("i", 0);
    absent.put("d", CelDecimal.of(new BigDecimal("1.50")));
    absent.put("x", 2.0);
    absent.put("s", "a");
    absent.put("t", true);
    absent.put("b", 1L);
    absent.put("r", Map.of("a", 1));
    Map<String, Object> present = new HashMap<>(absent);
    present.put("nx", null);
    present.put("ns", null);
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("(NULLIF(r.a, 1) IN (0, -1)) IS NULL",
          "((CASE WHEN r.a NOT IN (i, d) THEN ns ELSE s END) IN ('a', 'Ab')) IS NULL",
          "CASE (nx NOT IN (-1, b)) <> (-1 IN (x, d)) WHEN TRUE THEN 1 ELSE 2 END = 2",
          "ABS(NULLIF(r.a, ABS(b))) IS NULL", "LENGTH(CASE WHEN t THEN ns ELSE s END) IS NULL",
          "COALESCE(UPPER(CASE WHEN t THEN ns ELSE s END), 'x') = 'x'")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aBetweenOrLogicOverANullIsThreeValued() throws Exception {
    // BETWEEN is FALSE when either comparison is and AND, OR and NOT follow Kleene's logic, so a
    // NULL makes one NULL only when the rest doesn't decide it.
    String columns = "i INT NOT NULL, x DOUBLE NOT NULL, s STRING NOT NULL, t BOOLEAN NOT NULL, "
        + "ni INT, nb BIGINT, nd DECIMAL(10, 2), nx DOUBLE, ns STRING";
    assertEquals("!has(this.ni) || dyn(this.ni) == null || (1 <= this.ni && this.ni <= 2 ? false"
            + " : false)",
        translateCheckOver(columns, "(ni BETWEEN 1 AND 2) IS NULL"));
    Map<String, Object> absent = Map.of("i", 0, "x", 0.5, "s", "a", "t", true);
    Map<String, Object> present = new HashMap<>(absent);
    for (String col : List.of("ni", "nb", "nd", "nx", "ns")) {
      present.put(col, null);
    }
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("(ni BETWEEN i AND ni) IS NULL",
          "(nd BETWEEN SYMMETRIC x AND nb) IS NULL", "(nd BETWEEN 1 AND 2) IS NULL",
          "((x BETWEEN nx AND 1) OR ('a' <> s)) IS NULL",
          "((ns LIKE 'a%') AND (s LIKE 'a%')) IS NULL", "(x BETWEEN nx AND 0) IS NOT NULL",
          "(x NOT BETWEEN 1 AND nx) IS NOT NULL", "(i BETWEEN nd AND -1) IS NOT NULL",
          "((ns LIKE 'a%') AND (s LIKE 'b%')) IS NOT NULL", "((ns LIKE 'a%') OR t) IS NOT NULL",
          "CASE WHEN x BETWEEN nx AND 1 THEN 1 ELSE 2 END = 2",
          "CASE WHEN NOT (x BETWEEN nx AND 0) THEN 1 ELSE 2 END = 1",
          "COALESCE((ns LIKE 'a%') AND t, FALSE) = FALSE")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void anInOverAnArrayWithANullIsThreeValued() throws Exception {
    // An IN over an array is TRUE on a match, else NULL when its subject, the array or an
    // element is NULL, as an IN over a list is; with elements never NULL, NULL when the array
    // is, or its subject is and the array isn't empty.
    String columns = "i INT NOT NULL, ni INT, arr INT NOT NULL ARRAY NOT NULL, "
        + "an INT ARRAY NOT NULL, na INT NOT NULL ARRAY";
    assertEquals("this.i in this.an ? true : this.an.exists(v, dyn(v) == null)",
        translateCheckOver(columns, "i IN an"));
    // One whose elements are never NULL is two-valued.
    assertEquals("((!has(this.ni) || dyn(this.ni) == null) && size(this.arr) > 0) ||"
            + " !(size(this.arr) == 0 ? false : this.ni in this.arr)",
        translateCheckOver(columns, "ni NOT IN arr"));
    Map<String, Object> absent = new HashMap<>();
    absent.put("i", 2);
    absent.put("arr", List.of(1, -2));
    absent.put("an", Arrays.asList(1, null));
    Map<String, Object> present = new HashMap<>(absent);
    present.put("ni", null);
    present.put("na", null);
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("(i IN an) IS NULL", "(ni NOT IN arr) IS NULL",
          "CASE WHEN i NOT IN an THEN 1 ELSE 2 END = 2", "(3 IN na) IS NULL", "1 IN an",
          "COALESCE(i IN an, TRUE) = TRUE", "(i IN arr) IS NOT NULL")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aBooleanCoalesceOrNullifMayBeTheWholeCheck() throws Exception {
    // A COALESCE or NULLIF of BOOLEANs is a BOOLEAN, as a CASE of them is.
    String columns = "i INT NOT NULL, ni INT, nt BOOLEAN, nx DOUBLE, "
        + "arr INT NOT NULL ARRAY NOT NULL";
    for (Object[] c : new Object[][] {{"COALESCE(i IN arr, TRUE)", 1, null, true},
        {"COALESCE(i IN arr, TRUE)", 5, null, false}, {"COALESCE(nt, TRUE)", 1, null, true},
        {"NULLIF(i IN arr, TRUE)", 5, null, false},
        {"COALESCE(nx BETWEEN SYMMETRIC (LENGTH('a')) AND nx, FALSE)", 1, null, false},
        {"COALESCE(nx BETWEEN SYMMETRIC (LENGTH('a')) AND nx, FALSE)", 1, 1.0, true}}) {
      Map<String, Object> present = new HashMap<>();
      present.put("i", c[1]);
      present.put("arr", List.of(1, -2));
      present.put("ni", null);
      present.put("nt", null);
      present.put("nx", c[2]);
      Map<String, Object> absent = new HashMap<>(present);
      absent.values().removeIf(Objects::isNull);
      String cel = translateCheckOver(columns, (String) c[0]);
      for (Map<String, Object> self : List.of(present, absent)) {
        Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
        assertEquals(c[3], CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            c[0] + " on " + self + " -> " + cel);
      }
    }
    assertThrows(ValidationException.class, () -> translateCheckOver(columns, "COALESCE(ni, 0)"));
  }

  @Test
  void aNumericCaseIsNotACheck() {
    // A CASE, or a COALESCE or NULLIF of them, is a CHECK only when its branches are BOOLEANs.
    String columns = "t BOOLEAN NOT NULL, nt BOOLEAN";
    for (String sql : List.of("CASE WHEN t THEN 1 ELSE 2 END",
        "COALESCE(CASE WHEN t THEN 1 ELSE 2 END, CASE WHEN t THEN 3 ELSE 4 END)",
        "NULLIF(CASE WHEN t THEN 1 ELSE 2 END, 1)")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, sql), sql);
      assertTrue(e.getMessage().contains("must yield a boolean"), e.getMessage());
    }
    translateCheckOver(columns, "CASE WHEN t THEN t ELSE NOT t END");
    translateCheckOver(columns, "COALESCE(CASE WHEN t THEN nt ELSE t END, FALSE)");
  }

  @Test
  void aBooleanArgumentIsGroupedInItsComparison() throws Exception {
    // CEL binds == tighter than && and as tight as `in`, so NULLIF's and GREATEST's comparison
    // groups an argument that is itself a predicate.
    String columns = "t BOOLEAN NOT NULL, t2 BOOLEAN NOT NULL, nt BOOLEAN, i INT NOT NULL";
    for (Object[] c : new Object[][] {{"NOT (NULLIF(t, t2 AND t) IS NOT NULL)", false, false},
        {"NOT (NULLIF(t, t IN (TRUE, FALSE)) IS NULL)", false, false},
        {"NULLIF(t2 AND t, t) IS NULL", false, false},
        {"NOT (NULLIF(t OR t2, FALSE) IS NULL)", true, false},
        {"NULLIF(t, t2 AND nt) IS NULL", false, false}, {"NULLIF(t, i = 0) IS NULL", true, false},
        {"GREATEST(t, t AND t2) = TRUE", true, false}, {"NOT LEAST(t2 AND t, t)", true, false}}) {
      Map<String, Object> present = new HashMap<>();
      present.put("t", c[1]);
      present.put("t2", c[2]);
      present.put("i", 0);
      present.put("nt", null);
      Map<String, Object> absent = new HashMap<>(present);
      absent.remove("nt");
      String cel = translateCheckOver(columns, (String) c[0]);
      for (Map<String, Object> self : List.of(present, absent)) {
        Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            c[0] + " on " + self + " -> " + cel);
      }
    }
  }

  @Test
  void anInOverAMultisetOfNonStringsIsRefused() throws Exception {
    // A MULTISET is held as entries of a value and its count, which no value matches, except
    // one of strings, which Avro and JSON hold as a map of them.
    String columns = "i INT NOT NULL, s STRING NOT NULL, ms INT MULTISET NOT NULL, "
        + "nms DECIMAL(10, 2) NOT NULL MULTISET, sms STRING NOT NULL MULTISET NOT NULL";
    for (String sql : List.of("i IN ms", "1.5 NOT IN nms", "(i IN ms) IS NULL")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, sql), sql);
      assertTrue(e.getMessage().contains("IN over a MULTISET of"), e.getMessage());
    }
    String cel = translateCheckOver(columns, "s IN sms");
    Map<String, Object> vars = Map.of("this",
        CelUtils.toCelValue(Map.of("i", 0, "s", "a", "sms", Map.of("a", 2))));
    assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
        CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars), cel);
  }

  @Test
  void aCoalesceWithAnArgumentNeverNullIsNeverNull() throws Exception {
    // COALESCE with an argument never NULL never is, whatever the others are.
    String columns = "s STRING NOT NULL, ni INT";
    String vg = "VARIANT_GET(PARSE_JSON(s), '$.a' RETURNING INT)";
    Map<String, Object> present = new HashMap<>();
    present.put("s", "{\"a\": 1}");
    present.put("ni", null);
    for (Map<String, Object> self : List.of(present, Map.<String, Object>of("s", "{\"a\": 1}"))) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("(COALESCE(" + vg + ", 0) > 0 AND ni > 0) IS NULL",
          "(COALESCE(" + vg + ", 0) + ni) IS NULL")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aCastOfAParenthesizedFieldIsNullWhenTheFieldIs() throws Exception {
    // A parenthesized field resolves to its type as an unparenthesized one does, rather than
    // being taken for a VARIANT, whose CAST can be NULL however its inputs are.
    String columns = "ns STRING, r STRUCT(a INT)";
    Map<String, Object> present = new HashMap<>();
    present.put("ns", null);
    present.put("r", null);
    for (Map<String, Object> self : List.of(present, Map.<String, Object>of())) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("(CAST((r).a AS STRING) = 'q' OR ns = 'q') IS NULL",
          "COALESCE(CAST((r).a AS STRING), 'z') = 'z'", "CAST((r).a AS STRING) IS NULL")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aQuantifierOverANullIsThreeValued() throws Exception {
    // EVERY is TRUE when its body is for every element and FALSE when it is FALSE for any, ANY
    // the other way round; over a NULL collection both are NULL, over an empty one never.
    String columns = "ni INT, arr INT NOT NULL ARRAY NOT NULL, an INT ARRAY NOT NULL, "
        + "na INT NOT NULL ARRAY, ea INT NOT NULL ARRAY NOT NULL";
    Map<String, Object> absent = new HashMap<>();
    absent.put("arr", List.of(1, -2));
    absent.put("an", Arrays.asList(1, null));
    absent.put("ea", List.of());
    Map<String, Object> present = new HashMap<>(absent);
    present.put("ni", null);
    present.put("na", null);
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("EVERY(arr, e, e > ni) IS NULL",
          "NOT (EVERY(arr, e, e > ni) IS NOT NULL)", "ANY(an, e, e + 1 > 0) IS NOT NULL",
          "ANY(an, e, e > 5) IS NULL", "EVERY(an, e, e > 5) IS NOT NULL",
          "EVERY(na, e, e > 0) IS NULL", "EVERY(ea, e, e > ni) IS NOT NULL",
          "EVERY(arr, e, e IN (1, ni)) IS NULL",
          "CASE WHEN EVERY(arr, e, e > ni) THEN 1 ELSE 2 END = 2",
          "CASE WHEN NOT EVERY(arr, e, e > ni) THEN 1 ELSE 2 END = 2",
          "CASE EVERY(arr, e, GREATEST(e, ni) > 0) WHEN FALSE THEN 1 ELSE 2 END = 2",
          "COALESCE(ANY(an, e, e > 5), TRUE) = TRUE")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aSubscriptOfANullOrMissingIsNull() throws Exception {
    // A subscript is NULL when its collection, index or element is, or its key or index is
    // missing, as SQL gives it.
    String columns = "mv MAP<STRING, INT> NOT NULL, nm MAP<STRING, INT>, na INT NOT NULL ARRAY, "
        + "an INT ARRAY NOT NULL, i INT NOT NULL, ni INT";
    assertEquals("size(this.an) < 5 || dyn(this.an[4]) == null",
        translateCheckOver(columns, "an[5] IS NULL"));
    Map<String, Object> mv = new HashMap<>();
    mv.put("k", null);
    mv.put("j", 2);
    Map<String, Object> absent = new HashMap<>();
    absent.put("mv", mv);
    absent.put("an", Arrays.asList(1, null));
    absent.put("i", 1);
    Map<String, Object> present = new HashMap<>(absent);
    present.put("nm", null);
    present.put("na", null);
    present.put("ni", null);
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("mv['k'] + 1 IS NULL", "mv['zz'] IS NULL",
          "mv['j'] IS NOT NULL", "COALESCE(mv['zz'], -1) = -1",
          "CASE WHEN mv['k'] > 0 THEN 1 ELSE 2 END = 2", "COALESCE(na[1], -1) = -1",
          "an[2] IS NULL", "an[5] IS NULL", "an[1] IS NOT NULL", "an[ni] IS NULL",
          "nm['k'] IS NULL", "(an[5] > 0 OR i > 0) IS NOT NULL")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aVariantFunctionOfANullIsNull() throws Exception {
    // VARIANT_GET and TRY_VARIANT_GET are NULL when an input is or the path is missing, which
    // their CEL gives as null; TO_JSON and TYPE_OF_VARIANT when their input is.
    String columns = "s STRING NOT NULL, nv VARIANT, ni INT";
    String vg = "VARIANT_GET(PARSE_JSON(s), '$.a' RETURNING INT)";
    Map<String, Object> present = new HashMap<>();
    present.put("s", "{}");
    present.put("nv", null);
    present.put("ni", null);
    for (Map<String, Object> self : List.of(present, Map.<String, Object>of("s", "{}"))) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of(vg + " IS NULL", "(" + vg + " > 0 OR ni > 0) IS NULL",
          vg + " + 1 IS NULL", "CASE WHEN " + vg + " > 0 THEN 1 ELSE 2 END = 2",
          "VARIANT_GET(nv, '$.a' RETURNING INT) IS NULL", "COALESCE(TO_JSON(nv), 'z') = 'z'",
          "TYPE_OF_VARIANT(nv) IS NULL",
          "TRY_VARIANT_GET(PARSE_JSON(s), '$.a' RETURNING INT) IS NULL")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void anExpressionWhoseNullCantBeToldIsRefusedWhereTested() {
    // Evaluated instead, VARIANT_IS_NULL could answer as if its NULL were a value, so a NULL
    // test of one over a nullable input is refused; ONE's is told, and one over none is dead.
    String columns = "ni INT, i INT NOT NULL, arr INT NOT NULL ARRAY NOT NULL, nv VARIANT";
    for (String sql : List.of("VARIANT_IS_NULL(nv) IS NULL",
        "COALESCE(VARIANT_IS_NULL(nv), TRUE) = TRUE")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, sql), sql);
      assertTrue(e.getMessage().contains("can't tell when"), e.getMessage());
    }
    ValidationException dead = assertThrows(ValidationException.class,
        () -> translateCheckOver(columns, "ONE(arr, e, e > i) IS NULL"));
    assertTrue(dead.getMessage().contains("dead code"), dead.getMessage());
    translateCheckOver(columns, "ONE(arr, e, e > ni)");
  }

  @Test
  void aOneIsThreeValued() throws Exception {
    // ONE is TRUE when exactly one body is TRUE and every other FALSE, FALSE when two or more are
    // TRUE or all are FALSE, and NULL otherwise, as EVERY and ANY fold theirs.
    String columns = "ni INT, nt BOOLEAN, arr INT NOT NULL ARRAY NOT NULL, "
        + "an INT ARRAY NOT NULL, ae INT NOT NULL ARRAY NOT NULL";
    assertEquals("this.arr.exists_one(e, e > 0)",
        translateCheckOver(columns, "ONE(arr, e, e > 0)"));
    Map<String, Object> absent = new HashMap<>();
    absent.put("arr", List.of(1, -2));
    absent.put("an", Arrays.asList(0, null, 2));
    absent.put("ae", List.of());
    absent.put("nt", false);
    Map<String, Object> present = new HashMap<>(absent);
    present.put("ni", null);
    Map<String, Object> one = new HashMap<>(absent);
    one.put("ni", 1);
    Map<List<Map<String, Object>>, List<String>> rows = Map.of(List.of(present, absent),
        List.of("CASE WHEN ONE(an, e, e = ni OR nt) THEN 1 ELSE 2 END = 2",
            "CASE WHEN ONE(arr, e, e > ni) THEN 1 ELSE 2 END = 2", "ONE(arr, e, e > ni) IS NULL",
            "CASE ONE(arr, e, e > ni) WHEN TRUE THEN 1 ELSE 2 END = 2",
            "COALESCE(ONE(arr, e, e > ni), TRUE)", "COALESCE(ONE(an, e, e = 0), FALSE) = FALSE",
            "NOT ONE(ae, e, e > ni)", "ONE(an, e, e > 1) IS NULL"),
        List.of(one), List.of("NOT ONE(an, e, e + ni > 0)", "ONE(arr, e, e >= ni)",
            "NOT ONE(arr, e, e > ni)", "ONE(an, e, e > ni) IS NULL"));
    for (Map.Entry<List<Map<String, Object>>, List<String>> row : rows.entrySet()) {
      for (Map<String, Object> self : row.getKey()) {
        for (String sql : row.getValue()) {
          String cel = translateCheckOver(columns, sql);
          assertEquals(true, evalOver(cel, self), sql + " on " + self + " -> " + cel);
        }
      }
    }
  }

  @Test
  void aCheckFailsOnlyWhenFalse() throws Exception {
    // As in SQL, a CHECK fails only when it's FALSE: a NULL passes, on a null or a left-out
    // field alike, while an error such as a division by zero or bad JSON still fails.
    String columns = "ni INT, nb BIGINT, ns STRING, z INT NOT NULL, t BOOLEAN NOT NULL, "
        + "arr INT NOT NULL ARRAY NOT NULL, an INT ARRAY NOT NULL";
    assertEquals("!has(this.ni) || dyn(this.ni) == null || this.ni > 0",
        translateCheckOver(columns, "ni > 0"));
    Map<String, Object> absent = Map.of("z", 0, "t", false, "arr", List.of(1),
        "an", Arrays.asList(2, null));
    Map<String, Object> present = new HashMap<>(absent);
    present.put("ni", null);
    present.put("nb", null);
    present.put("ns", null);
    for (String sql : List.of("ni > 0", "ni <> 5", "ni NOT IN (1, 2)", "NOT (ni = 1)",
        "(nb / 0.00) IN (-1, 2.5)", "(nb / 0.00) BETWEEN 1 AND 2", "ni IN (1, nb)",
        "(ni / z) IN (1, nb)", "ni IN an", "ONE(arr, e, e > ni) <> FALSE",
        "NULLIF(ni, 1) <> 5", "COALESCE(ni, ni) <> 5", "POSITION('' IN ns FROM 1) <> 1",
        "(ni / z) > 0 OR t", "CAST(ns AS INT) > 0", "PARSE_JSON(ns) IS NOT NULL OR ns IS NULL")) {
      String cel = translateCheckOver(columns, sql);
      for (Map<String, Object> self : List.of(present, absent)) {
        assertEquals(true, evalOver(cel, self), sql + " on " + self.keySet() + " -> " + cel);
      }
    }
    Map<String, Object> bad = Map.of("ni", 5, "nb", 40000L, "ns", "not-json", "z", 0,
        "t", false, "arr", List.of(1), "an", Arrays.asList(2, null));
    for (String sql : List.of("(nb / 0.00) IN (-1, 2.5)", "(nb / 0.00) BETWEEN 1 AND 2",
        "(ni / z) IN (1, nb)", "(ni / z) IN an", "(ni / z) > 0 OR t", "CAST(ns AS INT) > 0",
        "PARSE_JSON(ns) IS NOT NULL OR ns IS NULL")) {
      String cel = translateCheckOver(columns, sql);
      assertThrows(CelEvaluationException.class, () -> evalOver(cel, bad), sql + " -> " + cel);
    }
    Map<String, Object> negative = new HashMap<>(bad);
    negative.put("ni", -1);
    assertEquals(false, evalOver(translateCheckOver(columns, "ni > 0"), negative));
  }

  @Test
  void aLiteralWithItsOwnSignKeepsIt() throws Exception {
    // The grammar lets a literal carry a sign after the unary one: - -5 is 5.
    String columns = "x INT NOT NULL, a INT";
    assertEquals("this.x == -1", translateCheckOver(columns, "x = + -1"));
    assertEquals("!has(this.a) || dyn(this.a) == null || this.a > -(-5)",
        translateCheckOver(columns, "a > - -5"));
    Object[][] rows = {
        {"x = + -1", row("x", -1), true},
        {"x = + -1", row("x", 1), false},
        {"a > - -5", row("a", 6), true},
        {"a > - -5", row("a", 4), false},
        {"CAST(a AS DOUBLE) > - -5", row("a", 6), true},
        {"CAST(a AS DOUBLE) > - -5", row("a", 4), false},
        {"a > - - 5E0", row("a", 6), true},
        {"a > - - 5E0", row("a", 4), false},
    };
    for (Object[] r : rows) {
      @SuppressWarnings("unchecked")
      Map<String, Object> self = new HashMap<>((Map<String, Object>) r[1]);
      self.putIfAbsent("x", 0);
      String cel = translateCheckOver(columns, (String) r[0]);
      assertEquals(r[2], evalOver(cel, self), r[0] + " on " + self + " -> " + cel);
    }
  }

  @Test
  void aLongChainOrLiteralIsRefusedOrTranslatedWithoutOverflowingTheStack() {
    // A flat SQL chain nests in CEL, so one too deep is refused before its tree is walked.
    String columns = "i INT NOT NULL, s STRING NOT NULL";
    for (String op : List.of(" + ", " * ", " || ")) {
      String chain = String.join(op, Collections.nCopies(20_000, op.equals(" || ") ? "s" : "i"));
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, chain + (op.equals(" || ") ? " <> ''" : " > 0")));
      assertTrue(e.getMessage().contains("nests deeper than"), e.getMessage());
    }
    // A long string literal is checked without a regex.
    String text = "x".repeat(50_000);
    assertEquals("this.s == '" + text + "'", translateCheckOver(columns, "s = '" + text + "'"));
  }

  @Test
  void aCheckIsShortenedByTheRulesOfLogic() throws Exception {
    // A NULL test a sibling or a CASE condition has already made is left out, and adjacent
    // operands with the same one share it.
    String columns = "i INT NOT NULL, z INT NOT NULL, a INT, b INT, d DECIMAL(10, 2), t BOOLEAN";
    String aNull = "!has(this.a) || dyn(this.a) == null";
    String bNull = "!has(this.b) || dyn(this.b) == null";
    String aSet = "has(this.a) && dyn(this.a) != null";
    assertEquals(aNull + " || (this.a > 0 && this.a < 10)",
        translateCheckOver(columns, "a > 0 AND a < 10"));
    assertEquals("!has(this.d) || dyn(this.d) == null || (decimals.ge(this.d, decimal(\"0\"))"
            + " && decimals.le(this.d, decimal(\"100\")))",
        translateCheckOver(columns, "d >= 0 AND d <= 100"));
    assertEquals(aNull + " || (this.a > 0 && (" + bNull + " || this.a + this.b > 0))",
        translateCheckOver(columns, "a > 0 AND a + b > 0"));
    assertEquals(aNull + " || this.a > 0 || this.a < -10",
        translateCheckOver(columns, "a > 0 OR a < -10"));
    assertEquals(aNull + " || this.a > 0", translateCheckOver(columns, "a IS NULL OR a > 0"));
    // An AND whose raising first operand would raise first left to right isn't absorbed.
    assertEquals("this.a > 0 || (this.i / this.z > 0 && this.a > 0)", translateCheckOver(
        "a INT NOT NULL, i INT NOT NULL, z INT NOT NULL", "a > 0 OR (i / z > 0 AND a > 0)"));
    assertEquals(aSet + " && this.a > 0", translateCheckOver(columns, "a IS NOT NULL AND a > 0"));
    assertEquals(aNull + " || (this.a > 0 && (" + bNull + " || this.b > 0))",
        translateCheckOver(columns, "a IS NULL OR (a > 0 AND b > 0)"));
    assertEquals(aNull + " || " + bNull + " || math.greatest(this.a, this.b) > 0",
        translateCheckOver(columns, "GREATEST(a, b) > 0"));
    assertEquals(aSet + " && this.a > 0",
        translateCheckOver(columns, "CASE WHEN a > 0 THEN 1 ELSE 2 END = 1"));
    String caseValue = translateCheckOver(columns, "CASE WHEN a > 0 THEN a ELSE 0 END >= 0");
    assertEquals("(" + aSet + " && this.a > 0 ? this.a : 0) >= 0", caseValue);
    Object[][] rows = {
        {"a > 0 AND a < 10", row("a", null), true},
        {"a > 0 AND a < 10", row(), true},
        {"a > 0 AND a < 10", row("a", 5), true},
        {"a > 0 AND a < 10", row("a", 20), false},
        {"a IS NOT NULL AND a > 0", row("a", null), false},
        {"a IS NOT NULL AND a > 0", row(), false},
        {"a IS NOT NULL AND a > 0", row("a", 1), true},
        {"GREATEST(a, b) > 0", row("a", null, "b", -1), true},
        {"GREATEST(a, b) > 0", row("a", 1, "b", -1), true},
        {"GREATEST(a, b) > 0", row("a", -1, "b", -2), false},
        {"CASE WHEN a > 0 THEN a ELSE 0 END >= 0", row("a", null), true},
        {"CASE WHEN a > 0 THEN a ELSE 0 END >= 0", row("a", -3), true},
    };
    for (Object[] r : rows) {
      @SuppressWarnings("unchecked")
      Map<String, Object> self = new HashMap<>((Map<String, Object>) r[1]);
      self.putIfAbsent("i", 1);
      self.putIfAbsent("z", 1);
      String cel = translateCheckOver(columns, (String) r[0]);
      assertEquals(r[2], evalOver(cel, self), r[0] + " on " + self + " -> " + cel);
    }
    // Compared, a CASE's ternary stays one, so its branch's error still raises.
    String compared = translateCheckOver(columns,
        "(CASE WHEN a > 0 THEN i / z > 0 ELSE FALSE END) <> t");
    assertEquals("!has(this.t) || dyn(this.t) == null || (" + aSet
        + " && this.a > 0 ? this.i / this.z > 0 : false) != this.t", compared);
    for (boolean t : new boolean[] {true, false}) {
      assertThrows(Exception.class,
          () -> evalOver(compared, Map.of("i", 1, "z", 0, "a", 5, "t", t)), compared);
    }
  }

  @Test
  void aCheckIsSplitOnACaseCondition() throws Exception {
    // A CHECK that always evaluates a CASE's condition is split on it, each side knowing it.
    String columns = "a INT, b INT, t BOOLEAN NOT NULL, i INT NOT NULL, z INT NOT NULL";
    String aNull = "!has(this.a) || dyn(this.a) == null";
    String bNull = "!has(this.b) || dyn(this.b) == null";
    String aSet = "has(this.a) && dyn(this.a) != null";
    assertEquals(aSet + " && this.a > 0 ? " + bNull + " || this.b > 1 : " + aNull
        + " || this.a > 1", translateCheckOver(columns, "CASE WHEN a > 0 THEN b ELSE a END > 1"));
    assertEquals(aSet + " && this.a == 1", translateCheckOver(columns,
        "CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END = 'one'"));
    assertEquals("this.t ? " + aNull + " : " + bNull,
        translateCheckOver(columns, "CASE WHEN t THEN a ELSE b END IS NULL"));
    // An equality can't raise, so its value is a fact.
    assertEquals(aNull + " || this.a == 5", translateCheckOver(columns, "NULLIF(a, 5) IS NULL"));
    assertEquals(aNull, translateCheckOver(columns, "(a IN (0, 1)) IS NULL"));
    // What the condition raises, the CHECK still does.
    String raising = translateCheckOver(columns,
        "(CASE WHEN i / z > 0 THEN a ELSE b END > 0) = t");
    assertEquals("this.i / this.z > 0 ? " + aNull + " || (this.a > 0) == this.t : " + bNull
        + " || (this.b > 0) == this.t", raising);
    assertThrows(Exception.class,
        () -> evalOver(raising, Map.of("i", 1, "z", 0, "a", 5, "t", true)), raising);
    // With the condition kept as R OR TRUE, the inner CASE still raises R's error.
    String kept = assertTimeoutPreemptively(Duration.ofSeconds(30), () -> translateCheckOver(
        columns, "CASE WHEN i / z > 0 OR TRUE THEN (CASE WHEN i / z > 0 THEN 1 ELSE 2 END)"
            + " ELSE 3 END = 1"));
    assertEquals(true, evalOver(kept, Map.of("i", 1, "z", 1)), kept);
    assertEquals(false, evalOver(kept, Map.of("i", -1, "z", 1)), kept);
    assertThrows(Exception.class, () -> evalOver(kept, Map.of("i", 1, "z", 0)), kept);
    Object[][] rows = {
        {"CASE WHEN a > 0 THEN b ELSE a END > 1", row(), true},
        {"CASE WHEN a > 0 THEN b ELSE a END > 1", row("a", 5), true},
        {"CASE WHEN a > 0 THEN b ELSE a END > 1", row("a", 5, "b", 0), false},
        {"CASE WHEN a > 0 THEN b ELSE a END > 1", row("a", 5, "b", 3), true},
        {"CASE WHEN a > 0 THEN b ELSE a END > 1", row("a", -1, "b", 3), false},
        {"CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END = 'one'", row(), false},
        {"CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END = 'one'", row("a", 1), true},
        {"CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END = 'one'", row("a", 2),
            false},
        {"NULLIF(a, 5) IS NULL", row("a", 5), true},
        {"NULLIF(a, 5) IS NULL", row("a", 4), false},
        {"COALESCE(a, b) > 0", row(), true},
        {"COALESCE(a, b) > 0", row("b", -1), false},
        {"COALESCE(a, b) > 0", row("a", 1, "b", -1), true},
        {"(CASE WHEN i / z > 0 THEN a ELSE b END > 0) = t", row("a", 5, "t", true), true},
        {"(CASE WHEN i / z > 0 THEN a ELSE b END > 0) = t", row("a", 5, "t", false), false},
        {"(CASE WHEN i / z > 0 THEN a ELSE b END > 0) = t", row("t", false), true},
    };
    for (Object[] r : rows) {
      @SuppressWarnings("unchecked")
      Map<String, Object> self = new HashMap<>((Map<String, Object>) r[1]);
      self.putIfAbsent("i", 1);
      self.putIfAbsent("z", 1);
      self.putIfAbsent("t", true);
      String cel = translateCheckOver(columns, (String) r[0]);
      assertEquals(r[2], evalOver(cel, self), r[0] + " on " + self + " -> " + cel);
    }
  }

  @Test
  void aLongCheckIsShortenedInTime() {
    // Splits simplify a bounded amount again, and a macro's body is given each known fact once,
    // so a long CHECK isn't simplified for minutes.
    String columns = "l INT NOT NULL ARRAY NOT NULL, x INT, y INT";
    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      translateCheckOver(columns, "EVERY(l, e, " + clauses(1200) + ")");
      // A wide OR known in each of many macros' bodies, and a long chain of facts.
      StringBuilder macros = new StringBuilder("EVERY(l, e, (" + ors(2000) + ")");
      for (int j = 0; j < 400; j++) {
        macros.append(" AND EVERY(l, f, f > ").append(j).append(")");
      }
      translateCheckOver(columns, macros + ")");
      StringBuilder decided = new StringBuilder("EVERY(l, e, (" + ors(3500) + ")");
      for (int j = 0; j < 3499; j++) {
        decided.append(" AND e <> ").append(j);
      }
      translateCheckOver(columns, decided + ")");
      String sum = "(CASE WHEN " + clauses(200) + " THEN 1 ELSE 0 END)";
      for (int j = 0; j < 20; j++) {
        sum = "(CASE WHEN e > " + j + " THEN 1 ELSE 2 END) + (" + sum + ")";
      }
      translateCheckOver(columns, "EVERY(l, e, " + sum + " > 0)");
      sum = "(CASE WHEN " + clauses(1200) + " THEN x ELSE y END)";
      for (int j = 0; j < 60; j++) {
        sum = "(CASE WHEN e > " + j + " THEN x ELSE y END) + (" + sum + ")";
      }
      translateCheckOver(columns, "EVERY(l, e, " + sum + " > 0)");
    });
  }

  // e = 0 OR e = 1 OR ..., n of them.
  private static String ors(int n) {
    StringBuilder or = new StringBuilder();
    for (int k = 0; k < n; k++) {
      or.append(k == 0 ? "" : " OR ").append("e = ").append(k);
    }
    return or.toString();
  }

  // (e = 0 OR e = 1) AND (e = 2 OR e = 3) AND ..., n of them.
  private static String clauses(int n) {
    StringBuilder and = new StringBuilder();
    for (int k = 0; k < n; k++) {
      and.append(k == 0 ? "" : " AND ").append("(e = ").append(2 * k).append(" OR e = ")
          .append(2 * k + 1).append(")");
    }
    return and.toString();
  }

  @Test
  void aCheckReadsAsItsNullTestOrItsPredicate() throws Exception {
    // A CHECK that may be NULL passes unless it's FALSE, so it's its NULL test or its predicate;
    // one with a raising operand beside a NULL keeps the form that tests a deciding one first.
    String columns = "i INT NOT NULL, a INT, b INT, amount DECIMAL(10, 2), t BOOLEAN, "
        + "arr INT NOT NULL ARRAY, an INT ARRAY NOT NULL, z INT NOT NULL, s STRING";
    String aNull = "!has(this.a) || dyn(this.a) == null";
    String bNull = "!has(this.b) || dyn(this.b) == null";
    assertEquals("!has(this.amount) || dyn(this.amount) == null || decimals.ge(this.amount,"
            + " decimal(\"0\"))",
        translateCheckOver(columns, "amount >= 0"));
    assertEquals(aNull + " || (1 <= this.a && this.a <= 10)",
        translateCheckOver(columns, "a BETWEEN 1 AND 10"));
    // || binds loosest, so only an && joining it brackets it.
    assertEquals(aNull + " || this.a > 0 || " + bNull + " || this.b > 0",
        translateCheckOver(columns, "a > 0 OR b > 0"));
    assertEquals("(" + aNull + " || this.a > 0) && (" + bNull + " || this.b > 0)",
        translateCheckOver(columns, "a > 0 AND b > 0"));
    // NOT of a column's NULL test is the opposite test.
    assertEquals("has(this.a) && dyn(this.a) != null && (" + bNull + " || this.b > 0)",
        translateCheckOver(columns, "NOT (a IS NULL) AND b > 0"));
    assertEquals(aNull + " || " + bNull + " || this.b > 0",
        translateCheckOver(columns, "NOT (a IS NOT NULL) OR b > 0"));
    assertEquals("this.an.all(e, dyn(e) == null || e > 0)",
        translateCheckOver(columns, "EVERY(an, e, e > 0)"));
    assertEquals("!has(this.t) || dyn(this.t) == null || this.t",
        translateCheckOver(columns, "t = TRUE"));
    assertEquals("!has(this.s) || dyn(this.s) == null || this.s.startsWith('a')",
        translateCheckOver(columns, "STARTS_WITH(s, 'a')"));
    assertEquals("(has(this.a) && dyn(this.a) != null && this.a > 0) || 1 / this.z > 0 ? true : "
        + aNull + " || this.a > 0", translateCheckOver(columns, "a > 0 OR 1 / z > 0"));
    assertEquals(aNull + " || (1 <= this.a && this.a <= 10 / this.z)",
        translateCheckOver(columns, "a BETWEEN 1 AND 10 / z"));
    Object[][] rows = {
        {"amount >= 0", row("amount", null), true},
        {"amount >= 0", row("amount", CelDecimal.of(new BigDecimal("5.00"))), true},
        {"amount >= 0", row("amount", CelDecimal.of(new BigDecimal("-5.00"))), false},
        {"a > 0 AND b > 0", row("a", null, "b", 1), true},
        {"a > 0 AND b > 0", row("a", null, "b", -1), false},
        {"a IS NULL OR a > 0", row("a", null), true},
        {"a IS NULL OR a > 0", row("a", -1), false},
        {"ANY(arr, e, e > a)", row("arr", List.of(), "a", null), false},
        {"ANY(arr, e, e > a)", row("arr", List.of(1), "a", null), true},
        {"a BETWEEN 1 AND 10", row("a", null), true},
        {"a BETWEEN 1 AND 10", row("a", 11), false},
        {"a NOT BETWEEN 1 AND 10", row("a", 5), false},
        {"EVERY(an, e, e > 0)", row("an", Arrays.asList(1, null)), true},
        {"EVERY(an, e, e > 0)", row("an", Arrays.asList(0, null)), false},
        {"t = TRUE", row("t", null), true},
        {"t = TRUE", row("t", false), false},
        {"(a > 0 OR b > 0) AND b < 0", row("a", 1, "b", 1), false},
        {"(a > 0 OR b > 0) AND b < 0", row("a", null, "b", 1), false},
        {"(a > 0 OR b > 0) AND b < 0", row("a", null, "b", -1), true},
        {"NOT (a IS NULL) AND b > 0", row("a", null, "b", 1), false},
        {"NOT (a IS NULL) AND b > 0", row("a", 1, "b", null), true},
        {"NOT (a IS NOT NULL) OR b > 0", row("a", null, "b", -1), true},
        {"NOT (a IS NOT NULL) OR b > 0", row("a", 1, "b", -1), false},
        {"COALESCE(t, t)", row("t", null), true},
        {"COALESCE(t, t)", row("t", false), false}};
    for (Object[] r : rows) {
      String cel = translateCheckOver(columns, (String) r[0]);
      @SuppressWarnings("unchecked")
      Map<String, Object> present = (Map<String, Object>) r[1];
      Map<String, Object> absent = new HashMap<>(present);
      absent.values().removeIf(Objects::isNull);
      for (Map<String, Object> self : List.of(present, absent)) {
        Map<String, Object> full = new HashMap<>(self);
        full.putIfAbsent("i", 1);
        full.putIfAbsent("z", 1);
        if (!present.containsKey("an")) {
          full.put("an", List.of());
        }
        assertEquals(r[2], evalOver(cel, full), r[0] + " on " + self + " -> " + cel);
      }
    }
  }

  // A row of alternating names and values, which may be null.
  private static Map<String, Object> row(Object... kv) {
    Map<String, Object> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  @Test
  void aNullDoesNotHideAnError() throws Exception {
    // A NULL operand decides neither AND nor OR, nor a quantifier or BETWEEN, so another's error
    // still fails the CHECK, as SQL's does; a TRUE or FALSE one still decides it.
    String columns = "ni INT, z INT NOT NULL, i INT NOT NULL, an INT ARRAY NOT NULL";
    Map<String, Object> absent = Map.of("z", 0, "i", 5, "an", Arrays.asList(null, 0));
    Map<String, Object> present = new HashMap<>(absent);
    present.put("ni", null);
    for (String sql : List.of("ni > 0 OR 1 / z > 0", "NOT (ni > 0 AND 1 / z > 0)",
        "ANY(an, e, 1 / e > 0)", "NOT EVERY(an, e, 1 / e > 0)",
        "NOT ((1 / z) BETWEEN ni AND 5)", "CASE WHEN ni > 0 AND 1 / z > 0 THEN 1 ELSE 2 END = 2",
        "NOT ONE(an, e, 1 / e > 0)")) {
      String cel = translateCheckOver(columns, sql);
      for (Map<String, Object> self : List.of(present, absent)) {
        assertThrows(CelEvaluationException.class, () -> evalOver(cel, self),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
    for (String sql : List.of("i > 0 OR ni > 0 OR 1 / z > 0",
        "NOT (i < 0 AND ni > 0 AND 1 / z > 0)")) {
      String cel = translateCheckOver(columns, sql);
      for (Map<String, Object> self : List.of(present, absent)) {
        assertEquals(true, evalOver(cel, self), sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void eachRaisingOperandKeepsItsErrorBesideANull() throws Exception {
    // An operand that can raise an error, one of each kind, still fails the CHECK beside a NULL.
    String columns = "ni INT, i INT NOT NULL, z INT NOT NULL, s STRING NOT NULL, "
        + "p STRING NOT NULL, d DECIMAL(10, 2) NOT NULL, sc INT NOT NULL";
    Map<String, Object> self = Map.of("i", 5, "z", 0, "s", "abc", "p", "(",
        "d", CelDecimal.of(new BigDecimal("1.00")), "sc", -700_000_000);
    for (String raising : List.of("i / z > 0", "i % z > 0", "CAST(s AS INT) > 0",
        "PARSE_JSON(s) IS NOT NULL", "VARIANT_GET(TRY_PARSE_JSON('{}'), p) IS NOT NULL",
        "TRY_VARIANT_GET(TRY_PARSE_JSON('{}'), p RETURNING INT) > 0", "ROUND(d, sc) > 0",
        "TRUNCATE(d, sc) > 0", "ROUND(d, -700000000) > 0")) {
      String cel = translateCheckOver(columns, "ni > 0 OR " + raising);
      assertThrows(CelEvaluationException.class, () -> evalOver(cel, self), raising + " -> " + cel);
    }
    // Flink gives FALSE for a bad regex, so a NULL may drop CEL's error, and the operands are
    // joined as is.
    for (String celOnly : List.of("MATCHES(s, p)")) {
      String cel = translateCheckOver(columns, "ni > 0 OR " + celOnly);
      assertTrue(cel.startsWith("!has(this.ni) || dyn(this.ni) == null || this.ni > 0 || "),
          cel);
      assertEquals(true, evalOver(cel, self), celOnly + " -> " + cel);
    }
  }

  @Test
  void aNullTestThatRaisesKeepsItsError() throws Exception {
    // A NULL test can raise where its predicate doesn't, as CEL's null == null is true, so it's
    // the condition of a ternary there, not an operand of ||.
    String columns = "ni INT, i INT NOT NULL, z INT NOT NULL, b BOOLEAN NOT NULL, "
        + "arr INT NOT NULL ARRAY NOT NULL";
    Map<String, Object> present = row("ni", null, "i", 1, "z", 0, "b", true, "arr", List.of(1));
    Map<String, Object> absent = new HashMap<>(present);
    absent.remove("ni");
    for (String sql : List.of("(ni = ni OR i / z > 0) = TRUE", "(ni = ni OR i / z > 0) <> FALSE",
        "NOT ((ni = ni OR i / z > 0) = FALSE)", "(ni = ni OR i / z > 0) = b",
        "EVERY(arr, e, (ni = ni OR e / z > 0) = TRUE)",
        "(ni = ni OR i / z > 0) = TRUE AND i > 0")) {
      String cel = translateCheckOver(columns, sql);
      for (Map<String, Object> self : List.of(present, absent)) {
        assertThrows(CelEvaluationException.class, () -> evalOver(cel, self), sql + " -> " + cel);
      }
    }
  }

  @Test
  void anIsNullOfInputsNeverOrAlwaysNullIsNoTernary() throws Exception {
    // Over inputs never NULL, the operand is only evaluated, so an error still fails; over a
    // literal NULL, the result is a constant.
    String columns = "s STRING NOT NULL, i INT NOT NULL, z INT NOT NULL";
    assertEquals("dyn(variants.parseJson(this.s)) != null",
        translateCheckOver(columns, "PARSE_JSON(s) IS NOT NULL"));
    assertEquals("dyn(this.i / this.z) != null", translateCheckOver(columns, "i / z IS NOT NULL"));
    assertEquals("this.i / this.z > 0 ? true : true",
        translateCheckOver(columns, "(i / z > 0) IS NOT NULL"));
    assertEquals("true", translateCheckOver(columns, "CAST(NULL AS INT) IS NULL"));
    assertEquals("false", translateCheckOver(columns, "CAST(NULL AS INT) IS NOT NULL"));
    Map<String, Object> good = row("s", "5", "i", 1, "z", 1);
    Map<String, Object> bad = row("s", "{x", "i", 1, "z", 0);
    assertEquals(true, evalOver(translateCheckOver(columns, "PARSE_JSON(s) IS NOT NULL"), good));
    assertEquals(true, evalOver(translateCheckOver(columns, "i / z IS NOT NULL"), good));
    assertEquals(true, evalOver(translateCheckOver(columns, "(i / z > 0) IS NOT NULL"), good));
    for (String sql : List.of("PARSE_JSON(s) IS NOT NULL", "i / z IS NOT NULL",
        "(i / z > 0) IS NOT NULL")) {
      String cel = translateCheckOver(columns, sql);
      assertThrows(CelEvaluationException.class, () -> evalOver(cel, bad), sql + " -> " + cel);
    }
  }

  @Test
  void aMalformedLiteralVariantPathIsRefused() {
    // A literal path is parsed as the variants functions parse it, so it fails here, not on
    // every record.
    String columns = "v VARIANT NOT NULL";
    for (String sql : List.of("VARIANT_GET(v, '$[') IS NULL", "VARIANT_GET(v, 'a') IS NULL",
        "TRY_VARIANT_GET(v, '$[-1]' RETURNING INT) > 0",
        "TRY_VARIANT_GET(v, ('$[') RETURNING INT) > 0")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, sql), sql);
      assertTrue(e.getMessage().contains("is malformed"), e.getMessage());
    }
    translateCheckOver(columns, "VARIANT_GET(v, '$.a[0]') IS NULL");
    translateCheckOver(columns, "TRY_VARIANT_GET(v, '$[\"b\"]' RETURNING INT) > 0");
  }

  @Test
  void aRoundScaleIsAtMost38() throws Exception {
    // A scale over DECIMAL's 38 gives the value back, as in Flink, rather than padding it, which
    // took time growing with the scale; the declared scale doesn't bound it, as a JSON or
    // Protobuf value carries its own.
    String columns = "d DECIMAL(10, 2) NOT NULL, i INT NOT NULL, ti TINYINT NOT NULL, "
        + "bi BIGINT NOT NULL, s STRING NOT NULL, abi BIGINT NOT NULL ARRAY NOT NULL, "
        + "mb MAP<STRING, BIGINT NOT NULL> NOT NULL, ai INT NOT NULL ARRAY NOT NULL";
    assertTrue(translateCheckOver(columns, "ROUND(d, 1) > 0")
        .contains("decimals.round(this.d, 1)"));
    assertTrue(translateCheckOver(columns, "ROUND(d, 50) > 0")
        .contains("decimals.round(this.d, 38)"));
    String cel = translateCheckOver(columns, "ROUND(d, i) = d");
    assertTrue(cel.contains("decimals.round(this.d, this.i < 38 ? this.i : 38)"), cel);
    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> assertEquals(true,
        evalOver(cel, Map.of("d", CelDecimal.of(new BigDecimal("1.00")), "i", 100_000_000)),
        cel));
    String three = translateCheckOver(columns, "ROUND(d, 3) = d");
    assertEquals(true, evalOver(three, Map.of("d", CelDecimal.of(new BigDecimal("1.234")))),
        three);
    // Flink takes the scale as an INT: a column's or variable's BIGINT is refused, while an
    // expression's integer, which resolves as CEL's int64, isn't.
    for (String sql : List.of("ROUND(d, 3000000000) > 0", "TRUNCATE(d, -2147483649) > 0",
        "ROUND(d, bi) > 0", "TRUNCATE(d, bi) > 0", "EVERY(abi, e, ROUND(d, e) > 0)",
        "ROUND(d, abi[1]) > 0", "TRUNCATE(d, (abi)[1]) > 0", "ROUND(d, mb['k']) > 0")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, sql), sql);
      assertTrue(e.getMessage().contains("takes an INT scale"), e.getMessage());
    }
    for (String sql : List.of("ROUND(d, ti) > 0 AND TRUNCATE(d, -2) > 0",
        "ROUND(d, CAST(2 AS INT)) > 0", "TRUNCATE(d, CAST(2 AS SMALLINT)) > 0",
        "ROUND(d, CAST(bi AS INT)) > 0", "ROUND(d, LENGTH(s)) > 0",
        "ROUND(d, CASE WHEN i > 0 THEN 1 ELSE 2 END) > 0", "ROUND(d, ai[1]) > 0")) {
      translateCheckOver(columns, sql);
    }
  }

  @Test
  void aReservedNameOfANullStructIsNull() throws Exception {
    // has() can't take r["in"], so the path is tested step by step: a NULL r gives NULL.
    String columns = "r STRUCT(`in` INT NOT NULL)";
    Map<String, Object> present = new HashMap<>();
    present.put("r", null);
    String cel = translateCheckOver(columns, "COALESCE(r.`in`, 7) = 7");
    for (Map<String, Object> self : List.of(present, Map.<String, Object>of())) {
      assertEquals(true, evalOver(cel, self), self + " -> " + cel);
    }
    assertEquals(false, evalOver(cel, Map.of("r", Map.of("in", 5))), cel);
  }

  @Test
  void aCastOfANullIsNull() throws Exception {
    // A literal NULL has no type, so isn't taken for a VARIANT: its CAST is NULL.
    String columns = "i INT NOT NULL, t BOOLEAN NOT NULL";
    String inCase = translateCheckOver(columns,
        "CASE WHEN t THEN CAST(NULL AS INT) ELSE i END < 1");
    assertEquals(true, evalOver(inCase, Map.of("t", false, "i", 0)), inCase);
    assertEquals(true, evalOver(inCase, Map.of("t", true, "i", 5)), inCase);
    assertEquals(false, evalOver(inCase, Map.of("t", false, "i", 5)), inCase);
    assertEquals("(this.t ? true : dyn(this.i) == null) ? true : this.i < 1", inCase);
    for (String sql : List.of("COALESCE(CAST(NULL AS INT), i) = i",
        "(CAST(NULL AS INT)) IS NULL", "CAST(NULL AS BOOLEAN) OR t",
        "COALESCE(CAST(NULL AS BOOLEAN), t) = t", "CAST(NULL AS BYTES) IS NULL")) {
      String cel = translateCheckOver(columns, sql);
      assertEquals(true, evalOver(cel, Map.of("t", false, "i", 5)), sql + " -> " + cel);
    }
  }

  @Test
  void aSimpleCaseOverATimestampBetweenComparesItWhole() throws Exception {
    // The subject is grouped, as a BETWEEN's && would bind to its last comparison.
    String cel = translateCheckOver("lt TIMESTAMP_LTZ(3) NOT NULL",
        "(CASE lt BETWEEN TIMESTAMP '2010-01-01 00:00:00' AND lt WHEN FALSE THEN 1 ELSE 2 END)"
            + " = 1");
    assertTrue(cel.contains("(timestamp(\"2010-01-01T00:00:00Z\") <= timestamp(this.lt)"
        + " && timestamp(this.lt) <= timestamp(this.lt)) == false"), cel);
    assertEquals(true, evalOver(cel, Map.of("lt", Instant.parse("1999-01-01T00:00:00Z"))), cel);
    assertEquals(false, evalOver(cel, Map.of("lt", Instant.parse("2020-01-01T00:00:00Z"))), cel);
  }

  @Test
  void anIsNullOfAPredicateKeepsItsOperandsErrors() throws Exception {
    // A predicate's CEL may be || or &&, which hide an error once compared, so IS NULL tests its
    // value as a ternary's condition, and a division by zero or bad JSON fails as SQL's does.
    String columns = "nb BIGINT, ni INT, ns STRING, an INT ARRAY NOT NULL, z INT NOT NULL";
    Map<String, Object> bad = Map.of("nb", 40000L, "ni", 5, "ns", "not-json",
        "an", Arrays.asList(1, null), "z", 0);
    for (String sql : List.of("((nb / 0.00) IN (-1, 2.5)) IS NOT NULL",
        "((nb / 0.00) BETWEEN 1 AND 2) IS NOT NULL", "((ni / z) BETWEEN 1 AND 2) IS NULL",
        "NOT ((VARIANT_GET(PARSE_JSON(ns), '$.a') IS NULL) = TRUE)",
        "NOT ((an[1 / z] IS NULL) = TRUE)")) {
      String cel = translateCheckOver(columns, sql);
      assertThrows(CelEvaluationException.class, () -> evalOver(cel, bad), sql + " -> " + cel);
    }
    Map<String, Object> good = Map.of("nb", 5L, "ni", 4, "ns", "{}",
        "an", Arrays.asList(1, null), "z", 2);
    for (String sql : List.of("((nb / 2.5) IN (-1, 2.0)) IS NOT NULL",
        "((nb / 2.5) BETWEEN 1 AND 2) IS NOT NULL", "((ni / z) BETWEEN 1 AND 2) IS NOT NULL",
        "NOT ((VARIANT_GET(PARSE_JSON(ns), '$.a') IS NULL) = FALSE)",
        "(an[4 / z] IS NULL) = TRUE")) {
      String cel = translateCheckOver(columns, sql);
      assertEquals(true, evalOver(cel, good), sql + " -> " + cel);
    }
  }

  @Test
  void anInOrLengthOfASubscriptIsTypedAsDyn() throws Exception {
    // cel-java types in and size() of a subscript by their first overload, a list's or a
    // string's, so a map's or multiset's reached by one is given as dyn, but not to the strict
    // check, which still refuses an ill-typed one.
    String columns = "s STRING NOT NULL, ns STRING, i INT NOT NULL, "
        + "mm MAP<STRING, MAP<STRING, INT> NOT NULL> NOT NULL, "
        + "mms MAP<STRING, STRING NOT NULL MULTISET NOT NULL> NOT NULL, "
        + "ma MAP<STRING, ARRAY<INT NOT NULL> NOT NULL> NOT NULL, "
        + "mi MAP<STRING, INT> NOT NULL, ai INT NOT NULL ARRAY NOT NULL";
    for (String sql : List.of("i NOT IN mm['k']", "s NOT IN ma['k']", "LENGTH(mi['k']) = 1",
        "i IN ai[1]", "LENGTH((mi['k'])) = 1")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, sql), sql);
      assertTrue(e.getMessage().contains("strict type-check"), e.getMessage());
    }
    Map<String, Object> self = Map.of("s", "a", "ns", "a", "mm", Map.of("k", Map.of("a", 1)),
        "mms", Map.of("k", Map.of("a", 1)), "ma", Map.of("k", List.of(5)));
    for (String sql : List.of("s IN mm['k']", "(s IN mm['k']) IS NOT NULL",
        "COALESCE(s IN mm['k'], s = 'b')", "COALESCE(ns IN mms['k'], FALSE)",
        "LENGTH(ma['k']) = 1", "LENGTH((ma['k'])) = 1")) {
      String cel = translateCheckOver(columns, sql);
      assertEquals(true, evalOver(cel, self), sql + " -> " + cel);
    }
  }

  @Test
  void aNullInAnEmptyCollectionIsFalse() throws Exception {
    // SQL's = ANY over no elements is FALSE whatever its subject, as ANY() is, so a NULL IN an
    // empty array, MULTISET or MAP is FALSE, and NULL only in a non-empty one.
    String columns = "ni INT, ns STRING, arr INT NOT NULL ARRAY NOT NULL, an INT ARRAY NOT NULL, "
        + "ms STRING NOT NULL MULTISET NOT NULL, nms STRING NOT NULL MULTISET, "
        + "mv MAP<STRING, INT> NOT NULL";
    assertEquals("(!has(this.ni) || dyn(this.ni) == null) && size(this.arr) > 0 ? true :"
            + " ((size(this.arr) == 0 ? false : this.ni in this.arr) ? false : false)",
        translateCheckOver(columns, "(ni IN arr) IS NULL"));
    List<String> empty = List.of("ni NOT IN arr", "ni NOT IN an", "NOT (ni IN an)",
        "COALESCE(ni NOT IN arr, FALSE)", "(ni NOT IN arr) IS NOT NULL",
        "COALESCE(ni IN an, TRUE) = FALSE", "(ni IN an) IS NOT NULL",
        "CASE WHEN ni IN arr THEN 1 ELSE 2 END = 2", "ns NOT IN ms", "(ns IN ms) IS NOT NULL",
        "COALESCE(ns IN mv, TRUE) = FALSE", "ANY(arr, x, x = ni) IS NOT NULL",
        "(ns IN nms) IS NULL");
    List<String> nonEmpty = List.of("(ni IN arr) IS NULL", "(ni NOT IN an) IS NULL",
        "COALESCE(ni IN arr, TRUE)", "(ns IN ms) IS NULL", "(ns NOT IN mv) IS NULL",
        "COALESCE(ns IN mv, TRUE)", "ANY(arr, x, x = ni) IS NULL", "(ns IN nms) IS NULL");
    for (List<String> sqls : List.of(empty, nonEmpty)) {
      boolean none = sqls == empty;
      Map<String, Object> absent = new HashMap<>();
      absent.put("arr", none ? List.of() : List.of(1));
      absent.put("an", none ? List.of() : Arrays.asList(1, null));
      absent.put("ms", none ? Map.of() : Map.of("a", 1));
      absent.put("mv", none ? Map.of() : Map.of("a", 1));
      Map<String, Object> present = new HashMap<>(absent);
      present.put("ni", null);
      present.put("ns", null);
      present.put("nms", null);
      for (Map<String, Object> self : List.of(present, absent)) {
        for (String sql : sqls) {
          String cel = translateCheckOver(columns, sql);
          assertEquals(true, evalOver(cel, self), sql + " on " + self + " -> " + cel);
        }
      }
    }
  }

  @Test
  void anIsNullOperandIsStillEvaluated() throws Exception {
    // IS NULL of an expression tells its NULL from its inputs but still evaluates it, so a value
    // SQL fails on, such as bad JSON or a division by zero, fails here too.
    String columns = "v STRING, ni INT, b INT NOT NULL";
    assertEquals("has(this.v) && dyn(this.v) != null && dyn(variants.parseJson(this.v)) != null",
        translateCheckOver(columns, "PARSE_JSON(v) IS NOT NULL"));
    Map<String, Object> bad = Map.of("v", "not-json", "ni", 5, "b", 0);
    Map<String, Object> good = Map.of("v", "{}", "ni", 4, "b", 2);
    Map<String, Object> present = new HashMap<>();
    present.put("v", null);
    present.put("ni", null);
    present.put("b", 0);
    // Each CHECK's value on a good row and on a NULL one.
    Map<String, List<Boolean>> rows = Map.of("PARSE_JSON(v) IS NOT NULL", List.of(true, false),
        "CAST(v AS INT) IS NULL", List.of(false, true), "ni / b IS NULL", List.of(false, true),
        "NOT ((ni / b) IS NOT NULL)", List.of(false, true),
        "(PARSE_JSON(v) IS NOT NULL) = TRUE", List.of(true, false));
    for (Map.Entry<String, List<Boolean>> row : rows.entrySet()) {
      String sql = row.getKey();
      String cel = translateCheckOver(columns, sql);
      assertThrows(CelEvaluationException.class, () -> evalOver(cel, bad), sql + " -> " + cel);
      if (!sql.startsWith("CAST")) {
        assertEquals(row.getValue().get(0), evalOver(cel, good), sql + " -> " + cel);
      }
      for (Map<String, Object> self : List.of(present, Map.<String, Object>of("b", 0))) {
        assertEquals(row.getValue().get(1), evalOver(cel, self),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
  }

  @Test
  void aMapWithoutStringKeysIsNotReadAsAMap() {
    // Avro and JSON hold a MAP without string keys as entries of a key and its value, which a
    // subscript, quantifier, IN or comparison would read as a list, as a quantifier or
    // comparison would a MULTISET of non-strings.
    String columns = "m MAP<INT, STRING> NOT NULL, nm MAP<INT, STRING>, "
        + "sm MAP<STRING, INT> NOT NULL, r STRUCT(m MAP<BIGINT, INT>) NOT NULL, "
        + "m2 MAP<INT, STRING> NOT NULL, msi INT NOT NULL MULTISET NOT NULL, "
        + "msi2 INT NOT NULL MULTISET NOT NULL";
    for (String sql : List.of("m[1] = 'a'", "EVERY(m, k, k > 0)", "ANY(nm, k, k > 0)",
        "(m)[1] IS NULL", "r.m[1] > 0", "EVERY(sm, k, ONE(m, j, j > 0))", "1 IN m",
        "ANY(msi, x, x = 5)", "EVERY(msi, x, x > 0)", "m = m2", "msi <> msi2")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheckOver(columns, sql), sql);
      assertTrue(e.getMessage().contains("is not supported: it is held as entries"),
          e.getMessage());
    }
    for (String sql : List.of("nm IS NULL", "sm['a'] > 0",
        "EVERY(sm, k, sm[k] > 0)", "'a' IN sm")) {
      translateCheckOver(columns, sql);
    }
  }

  @Test
  void aFieldOfAParenthesizedReceiverIsNullStepByStep() throws Exception {
    // (e).x and (arr[1]).x are NULL when their receiver or the field is, as e.x and arr[1].x are.
    String columns = "items STRUCT(x INT) ARRAY NOT NULL, "
        + "arr STRUCT(x INT) NOT NULL ARRAY NOT NULL";
    Map<String, Object> present = new HashMap<>();
    present.put("x", null);
    Map<String, Object> seven = Map.of("x", 7);
    for (Map<String, Object> self : List.of(
        Map.<String, Object>of("items", Arrays.asList(null, seven), "arr", List.of(present)),
        Map.<String, Object>of("items", Arrays.asList(Map.of(), seven), "arr",
            List.of(Map.of())))) {
      for (String sql : List.of("EVERY(items, e, COALESCE((e).x, 7) = 7)",
          "EVERY(items, e, (e).x IS NULL OR (e).x = 7)", "(arr[1]).x IS NULL",
          "COALESCE((arr[1]).x, 3) = 3")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, evalOver(cel, self), sql + " on " + self + " -> " + cel);
      }
    }
  }

  @Test
  void aReservedNameInASubscriptPathIsTestedForNull() throws Exception {
    // has() can't take r["in"], so a reserved name's step is tested by its value.
    Schema ints = Schema.createArray(Schema.create(Schema.Type.INT).setNullable(false));
    Schema inner = Schema.createStruct(Arrays.asList(new Schema.Field("in", ints, 0)))
        .setNullable(false);
    Schema root = Schema.createStruct(Arrays.asList(new Schema.Field("r", inner, 0)));
    ConstraintValidationContext vctx = ConstraintValidationContext.tableLevel(root.getFields());
    String cel = ConstraintToCelTranslator.translate(findFirstCheckExpr(LogicalTypesParserFactory
        .parse("STRUCT __T (__x INT, CHECK (COALESCE(r.`in`[1], 0) = 0));")), vctx);
    CelValidator.assertValidStrict(cel, root);
    Map<String, Object> nullIn = new HashMap<>();
    nullIn.put("in", null);
    assertEquals(true, evalOver(cel, Map.of("r", nullIn)), cel);
    assertEquals(true, evalOver(cel, Map.of("r", Map.of("in", List.of(0)))), cel);
  }

  @Test
  void aSubscriptBehindAMapKeyIsTestedForNull() throws Exception {
    // cel-java types size() and in of a subscript of dyn by their first overload, a string's
    // or a list's, so those collections are given as dyn.
    String columns = "mm MAP<STRING, MAP<STRING, INT> NOT NULL> NOT NULL, "
        + "ma MAP<STRING, ARRAY<INT NOT NULL> NOT NULL> NOT NULL";
    Map<String, Object> self = Map.of("mm", Map.of("a", Map.of("b", 1)),
        "ma", Map.of("k", List.of(1)));
    for (String sql : List.of("mm['a']['zz'] IS NULL", "mm['a']['b'] IS NOT NULL",
        "ma['k'][5] IS NULL", "ma['k'][1] IS NOT NULL", "COALESCE(mm['a']['zz'], 0) = 0")) {
      String cel = translateCheckOver(columns, sql);
      assertEquals(true, evalOver(cel, self), sql + " -> " + cel);
    }
  }

  private static Object evalOver(String cel, Map<String, Object> self) throws Exception {
    Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
    return CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
        CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars);
  }

  @Test
  void aNullGuardDoesNotHideATypeError() {
    // The dyn branch a NULL GREATEST, LEAST, COALESCE or NULLIF gives would pass any type; the
    // strict check sees each call's own result type instead.
    for (String[] c : new String[][] {
        {"a INT, b INT", "UPPER(COALESCE(a, b)) = 'X'"},
        {"a INT, b INT", "UPPER(GREATEST(a, b)) = 'X'"},
        {"a INT, b INT", "UPPER(NULLIF(a, b)) = 'X'"},
        {"a INT, b INT", "NULLIF('A', UPPER(COALESCE(a, b))) IS NULL"},
        {"a INT, b INT", "(UPPER(COALESCE(a, b)) = 'X') IS NULL"},
        {"a INT, x DOUBLE", "UPPER(LEAST(x, a)) = 'X'"},
        {"r STRUCT(x INT), s STRING", "COALESCE((r).x, s) = 'a'"},
        {"a INT ARRAY, b STRING ARRAY", "EVERY(COALESCE(a, b), e, e > 0)"},
        {"xs INT ARRAY", "EVERY(xs, e, UPPER(COALESCE(e, e)) = 'X')"}}) {
      assertThrows(ValidationException.class, () -> translateCheckOver(c[0], c[1]), c[1]);
    }
  }

  @Test
  void coalesceSkipsANullNestedCall() throws Exception {
    // A NULL argument that is not a plain column is tested before it is widened, so COALESCE
    // reaches its fallback.
    String columns = "a INT, b BIGINT, x DOUBLE, d DECIMAL(10, 2)";
    Map<String, Object> present = new HashMap<>();
    present.put("a", null);
    present.put("b", null);
    present.put("x", 0.5);
    present.put("d", null);
    // JSON leaves a NULL field out, so a NULL last argument is read guarded too.
    for (Map<String, Object> self : List.of(present, Map.<String, Object>of("x", 0.5))) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("COALESCE(GREATEST(a, b), x) = x",
          "COALESCE(COALESCE(a, b), x) = x", "COALESCE(LEAST(a, b), d, 7) = 7",
          "COALESCE(COALESCE(d, a), x) = x")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self + " -> " + cel);
      }
    }
  }

  @Test
  void anEnumComparesAsItsSymbol() throws Exception {
    // An ENUM is its symbol's text, as IN, CASE and COALESCE already took it, so it compares,
    // orders and concatenates as a string.
    String columns = "e ENUM('A', 'B', 'C') NOT NULL, s STRING";
    assertEquals("this.e == 'A'", translateCheckOver(columns, "e = 'A'"));
    Map<String, Object> self = new HashMap<>();
    self.put("e", "A");
    self.put("s", "B");
    Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
    for (String sql : List.of("e = 'A'", "'A' = e", "e <> s", "e < 'B'", "s >= e",
        "e || 'x' = 'Ax'", "CASE e WHEN 'A' THEN 1 ELSE 2 END = 1", "e IN ('A', 'C')")) {
      String cel = translateCheckOver(columns, sql);
      assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
          CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars), sql + " -> " + cel);
    }
  }

  private String translateCheckOver(String columns, String checkExpr) {
    Schema schema = parseScript("STRUCT T (" + columns + ", CHECK (" + checkExpr + "));")
        .getRootSchema();
    String cel = schema.getRules().get(0).getExpr();
    CelValidator.assertValidStrict(cel, schema);
    return cel;
  }

  @Test
  void integerOperandsAreCombinedBeforeWidening() {
    // SQL types each operator by its own operands: the INTs are divided, then widened.
    assertEquals("decimals.eq(decimal(this.x / 2), this.amount)",
        translateCheckNotNull("x / 2 = amount"));
    assertEquals("double(this.x / 2) == this.ratio", translateCheckNotNull("x / 2 = ratio"));
    assertEquals("double(this.x % 2) == this.ratio", translateCheckNotNull("x % 2 = ratio"));
    assertEquals("decimals.gt(decimals.add(this.amount, decimal(this.x / 2)), decimal(\"0\"))",
        translateCheckNotNull("amount + x / 2 > 0"));
    assertEquals("double(decimals.add(this.amount, decimal(\"1\"))) == this.ratio",
        translateCheckNotNull("amount + 1 = ratio"));
  }

  @Test
  void narrowerOperandsKeepTheirOwnArithmetic() throws Exception {
    // Each answer is Flink's: INT / INT truncates, BIGINT and DECIMAL sums are exact.
    for (String sql : List.of("7 / 2 = 3.0", "7 / 2 = 3.0E0", "-7 / 2 = -3.0", "7 % 4 = 3.0E0",
        "7 / 2 + 0.5 = 3.5", "0.5 + 7 / 2 = 3.5", "-(7 / 2) = -3.0",
        "CASE WHEN TRUE THEN 7 / 2 ELSE 1.5 END = 3.0", "7 / 2 BETWEEN 3.0 AND 3.0",
        "7 / 2 IN (3.5, 3.0)", "GREATEST(7 / 2, 1.5) = 3.0",
        "9007199254740993 + 1 = 9007199254740994E0", "0.10 + 0.20 = 0.3E0")) {
      String cel = translateCheck(sql);
      assertEquals(true, evalWithDecimals(cel), sql + " -> " + cel);
    }
  }

  @Test
  void narrowerOperandsStillOverflowAndDivideByZero() {
    for (String sql : List.of("9223372036854775807 + 1 > 0.0",
        "9223372036854775807 + 1 > 0.0E0", "-(-9223372036854775807 - 1) > 0.0",
        "1 / 0 <> 0.0E0", "1 % 0 <> 0.0E0")) {
      String cel = translateCheck(sql);
      assertThrows(CelEvaluationException.class, () -> evalWithDecimals(cel), sql + " -> " + cel);
    }
  }

  private static Object evalWithDecimals(String cel) throws Exception {
    return CelUtils.buildProgram(CelUtils.ScriptType.AVRO, cel, null, List.of()).eval(Map.of());
  }

  // ---- numeric coercion: BETWEEN / IN ----

  @Test
  void betweenIntDoubleCoercesToDouble() {
    // x (int) BETWEEN ratio (double) AND 5 (int) → common DOUBLE.
    assertEquals(
        "this.ratio <= double(this.x) && double(this.x) <= 5.0",
        translateCheckNotNull("x BETWEEN ratio AND 5"));
  }

  @Test
  void betweenDecimalDoubleCoercesToDouble() {
    // DECIMAL subject with a DOUBLE bound → common DOUBLE (not decimal).
    assertEquals(
        "this.ratio <= double(this.amount) && double(this.amount) <= 5.0",
        translateCheckNotNull("amount BETWEEN ratio AND 5"));
  }

  @Test
  void inListDoubleCoercion() {
    // ratio (double) IN (1, 2) → common DOUBLE; int elements emit as doubles.
    assertEquals("this.ratio in [1.0, 2.0]", translateCheckNotNull("ratio IN (1, 2)"));
  }

  @Test
  void inListIntDecimalCoercion() {
    // qty (int) IN (1.5, 2.5) → common DECIMAL → OR of decimals.eq.
    assertEquals(
        "decimals.eq(decimal(this.qty), decimal(\"1.5\")) || decimals.eq(decimal(this.qty),"
            + " decimal(\"2.5\"))",
        translateCheckNotNull("qty IN (1.5, 2.5)"));
  }

  // ---- Calcite/Flink operator precedence ----

  @Test
  void concatStillEmitsPlus() {
    // `||` (now at the multiplicative level) still maps to CEL `+`.
    assertEquals("this.name + this.last_name == 'foo'",
        translateCheckNotNull("name || last_name = 'foo'"));
  }

  @Test
  void betweenBindsTighterThanComparison() {
    // BETWEEN binds tighter than `=`, so `x BETWEEN 0 AND 10 = active` groups
    // as `(x BETWEEN 0 AND 10) = active` (bool = bool). Under the old
    // Postgres precedence this parsed the upper bound as `10 = active` and was
    // rejected.
    assertEquals("(0 <= this.x && this.x <= 10) == this.active",
        translateCheckNotNull("x BETWEEN 0 AND 10 = active"));
  }

  @Test
  void inBindsTighterThanComparison() {
    // IN binds tighter than `=`, so `x IN (1, 2) = active` groups as
    // `(x IN (1, 2)) = active`. Under the old precedence this was a parse error.
    assertEquals("(this.x in [1, 2]) == this.active",
        translateCheckNotNull("x IN (1, 2) = active"));
  }

  // ---- Variant CHECK functions (Tier 1/2 + TYPE_OF_VARIANT/VARIANT_IS_NULL) ----

  @Test
  void variantParseJson() {
    // NULL exactly when its input is, and parsed otherwise, so bad JSON fails.
    assertEquals("has(this.value) && dyn(this.value) != null &&"
            + " dyn(variants.parseJson(this.value)) != null",
        translateCheck("PARSE_JSON(value) IS NOT NULL"));
  }

  @Test
  void variantTryParseJson() {
    // NULL when its input is, or for invalid JSON, which its CEL gives as null.
    assertEquals("has(this.value) && dyn(this.value) != null &&"
            + " dyn(variants.tryParseJson(this.value)) != null",
        translateCheck("TRY_PARSE_JSON(value) IS NOT NULL"));
  }

  @Test
  void variantGetUntyped() {
    // 2-arg VARIANT_GET returns a sub-variant; IS NOT NULL is the existence test.
    assertEquals("has(this.data) && dyn(this.data) != null && dyn(variants.path(this.data,"
            + " '$.addr')) != null",
        translateCheck("VARIANT_GET(data, '$.addr') IS NOT NULL"));
  }

  @Test
  void variantGetReturning() {
    assertEquals(
        "!has(this.data) || dyn(this.data) == null || dyn(variants.as(variants.path(this.data,"
            + " '$.age'), \"int\")) == null ? true : variants.as(variants.path(this.data,"
            + " '$.age'), \"int\") > 18",
        translateCheck("VARIANT_GET(data, '$.age' RETURNING INT) > 18"));
  }

  @Test
  void tryVariantGetReturning() {
    assertEquals(
        "!has(this.data) || dyn(this.data) == null || dyn(variants.tryAs(variants.path(this.data,"
            + " '$.age'), \"int\")) == null || variants.tryAs(variants.path(this.data, '$.age'),"
            + " \"int\") > 18",
        translateCheck("TRY_VARIANT_GET(data, '$.age' RETURNING INT) > 18"));
  }

  @Test
  void variantGetReturningDecimal() {
    assertEquals(
        "!has(this.data) || dyn(this.data) == null || dyn(variants.as(variants.path(this.data,"
            + " '$.amt'), \"decimal\")) == null ? true : decimals.gt(variants.as(variants.path(thi"
            + "s.data, '$.amt'), \"decimal\"), decimal(\"0\"))",
        translateCheck("VARIANT_GET(data, '$.amt' RETURNING DECIMAL(10,2)) > 0"));
  }

  @Test
  void variantGetReturningTimestamp() {
    // RETURNING TIMESTAMP resolves to a timestamp, so it composes with a
    // timestamp column (which normalizes via timestamp).
    assertEquals(
        "!has(this.data) || dyn(this.data) == null || dyn(variants.as(variants.path(this.data,"
            + " '$.ts'), \"timestamp\")) == null || !has(this.created_at) || dyn(this.created_at)"
            + " == null ? true : variants.as(variants.path(this.data, '$.ts'), \"timestamp\") >"
            + " timestamp(this.created_at)",
        translateCheck("VARIANT_GET(data, '$.ts' RETURNING TIMESTAMP) > created_at"));
  }

  @Test
  void variantGetOverParseJson() {
    assertEquals(
        "!has(this.value) || dyn(this.value) == null ||"
            + " dyn(variants.as(variants.path(variants.parseJson(this.value), '$.x'),"
            + " \"string\")) == null ? true : variants.as(variants.path(variants.parseJson(this.va"
            + "lue), '$.x'), \"string\") == 'ok'",
        translateCheck("VARIANT_GET(PARSE_JSON(value), '$.x' RETURNING STRING) = 'ok'"));
  }

  @Test
  void variantToJson() {
    assertEquals("variants.toJson(this.data) == '{}'",
        translateCheckNotNull("TO_JSON(data) = '{}'"));
  }

  @Test
  void typeOfVariant() {
    assertEquals("variants.type(this.data) == 'OBJECT'",
        translateCheckNotNull("TYPE_OF_VARIANT(data) = 'OBJECT'"));
  }

  @Test
  void variantIsNull() {
    assertEquals("variants.isNull(this.data)",
        translateCheck("VARIANT_IS_NULL(data)"));
  }

  @Test
  void variantGetRejectsUnsupportedReturnType() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("VARIANT_GET(data, '$.d' RETURNING DATE) > 0"));
    assertTrue(t.getMessage().contains("not supported"),
        "expected unsupported-return-type rejection, got: " + t.getMessage());
  }

  @Test
  void errorIncludesSourcePosition() {
    String script = "STRUCT T (x INT, CHECK (foo > 0));";
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
    String script = "STRUCT T ("
        + "ts TIMESTAMP_LTZ,"
        + "CHECK (ts <= CURRENT_TIMESTAMP)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    assertEquals("!has(this.ts) || dyn(this.ts) == null || timestamp(this.ts) <= now",
        schema.getRules().get(0).getExpr());
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
    String script = "STRUCT T (x INT CHECK (x > 0)); TYPE T;";
    LogicalType orig = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(orig);
    assertTrue(emitted.contains("CHECK (x > 0)"),
        "expected emitted DDL to contain CHECK clause, got:\n" + emitted);
  }

  @Test
  void ddlEmitsConstraintNameAndMessage() {
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
        + "x INT CHECK (x > 0) CHECK (x < 100)"
        + "); TYPE T;";
    LogicalType orig = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(orig);
    assertTrue(emitted.contains("CHECK (x > 0)"), "got:\n" + emitted);
    assertTrue(emitted.contains("CHECK (x < 100)"), "got:\n" + emitted);
  }

  @Test
  void ddlRoundTripPreservesColumnCheck() {
    String script = "STRUCT T (x INT CHECK (x > 0)); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema struct = after.getRootSchema();
    List<Rule> rules = struct.getField("x").getRules();
    assertEquals(1, rules.size());
    assertEquals("this > 0", rules.get(0).getExpr());
  }

  @Test
  void ddlRoundTripPreservesAllRuleFields() {
    String script = "STRUCT T ("
        + "x INT CONSTRAINT positive_x CHECK (x > 0) MESSAGE 'must be positive'"
        + "); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema struct = after.getRootSchema();
    Rule r = struct.getField("x").getRules().get(0);
    assertEquals("positive_x", r.getName());
    assertEquals("must be positive", r.getDoc());
    assertEquals("this > 0", r.getExpr());
  }

  @Test
  void ddlEmitsCheckInsideInlineRow() {
    // Regression: rowExpr used to emit only `name typeExpr` per field,
    // dropping CHECK rules / DEFAULT / COMMENT / TAGS / WITH on inline
    // STRUCT(...) field types. Now reuses appendField so inline rows
    // preserve full per-field metadata.
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT CHECK (zip > 0)) NOT NULL"
        + "); TYPE T;";
    LogicalType orig = parseScript(script).toLogicalType();
    String emitted = LogicalTypeToDdlConverter.toDdl(orig);
    assertTrue(emitted.contains("CHECK (zip > 0)"),
        "expected emitted DDL to preserve inline STRUCT field's CHECK, got:\n"
            + emitted);
  }

  @Test
  void ddlRoundTripPreservesCheckInsideInlineRow() {
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT CHECK (zip > 0)) NOT NULL"
        + "); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema addr = after.getRootSchema().getField("addr").getSchema();
    Schema.Field zip = addr.getField("zip");
    List<Rule> rules = zip.getRules();
    assertEquals(1, rules.size(), "inline STRUCT field rule should round-trip");
    assertEquals("this > 0", rules.get(0).getExpr());
  }

  @Test
  void ddlRoundTripPreservesTableConstraint() {
    String script = "STRUCT T ("
        + "lo INT, hi INT,"
        + "CHECK (lo < hi)"
        + "); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema struct = after.getRootSchema();
    assertEquals(1, struct.getRules().size());
    assertEquals("!has(this.lo) || dyn(this.lo) == null || !has(this.hi) || dyn(this.hi) == null"
            + " || this.lo < this.hi", struct.getRules().get(0).getExpr());
  }

  @Test
  void ddlRoundTripPreservesMultipleChecks() {
    String script = "STRUCT T ("
        + "x INT CHECK (x > 0) CHECK (x < 100),"
        + "y INT,"
        + "CONSTRAINT cross CHECK (x + y > 0) MESSAGE 'sum must be positive'"
        + "); TYPE T;";
    LogicalType after = ddlRoundTrip(script);
    Schema struct = after.getRootSchema();
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
        () -> parseScript("STRUCT T (x INT, CHECK (x +)); TYPE T;"));
    assertTrue(t.getMessage().startsWith("Parse error at line "),
        "expected located parse error, got: " + t.getMessage());
  }

  @Test
  void parseErrorReportsLineAndColumn() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> parseScript("STRUCT T (x INT, CHECK ()); TYPE T;"));
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
            "STRUCT T (x INT CHECK (x >), y INT); TYPE T;"));
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
    assertTrue(r.contains("this.value.substring(0, 0)"),
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
        translateCheckNotNull("NOT x BETWEEN 1 AND 10"));
  }

  @Test
  void notPrefixOverIn() {
    assertEquals("!(this.x in [1, 2, 3])",
        translateCheckNotNull("NOT x IN (1, 2, 3)"));
  }

  @Test
  void notPostfixBetween() {
    // postfix NOT BETWEEN was already correct via the BETWEEN production.
    assertEquals("!(1 <= this.x && this.x <= 10)",
        translateCheckNotNull("x NOT BETWEEN 1 AND 10"));
  }

  @Test
  void notPostfixIn() {
    assertEquals("!(this.x in [1, 2, 3])",
        translateCheckNotNull("x NOT IN (1, 2, 3)"));
  }

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
        t.getMessage().contains("not comparable")
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
    assertEquals("!has(this.x) || dyn(this.x) == null",
        translateCheck("((x)) IS NULL"));
    assertEquals(
        "!has(this.addr_struct.zip) || dyn(this.addr_struct.zip) == null",
        translateCheck("(addr_struct).zip IS NULL"));
  }

  @Test
  void doubleLiteralOverflowRejected() {
    // 1e500 is an exponent (double) literal that parses as +Infinity — a
    // silently always-false constraint.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x > 1e500"));
    assertTrue(t.getMessage().contains("out of range"),
        "expected double-overflow rejection, got: " + t.getMessage());
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
    assertEquals("(!has(this.a) || dyn(this.a) == null) && (!has(this.b) || dyn(this.b) == null"
            + " || this.b > 0)",
        translateCheck("a IS NULL AND b > 0"));
    assertEquals("(!has(this.b) || dyn(this.b) == null || this.b > 0) && (!has(this.a) ||"
            + " dyn(this.a) == null)",
        translateCheck("b > 0 AND a IS NULL"));
    assertEquals("(!has(this.a) || dyn(this.a) == null || this.a in [1, 2]) && (!has(this.b) ||"
            + " dyn(this.b) == null)",
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
    assertTrue(t.getMessage().contains("reserved word"),
        "expected reserved-word message, got: " + t.getMessage());
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
    Schema rec = lt.getRootSchema();
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
    String script = "STRUCT Addr (zip INT); "
        + "STRUCT Person (a Addr, CHECK (a.zip > 0));"
        + "TYPE Person";
    LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
    v.visit(LogicalTypesParserFactory.parse(script));
    String cel = v.getRootSchema().getRules().get(0).getExpr();
    assertEquals("!has(this.a) || dyn(this.a) == null || !has(this.a.zip) || dyn(this.a.zip) =="
            + " null || this.a.zip > 0", cel);
  }

  @Test
  void namedTypeRefIndirectionStillRejectsInvalidField() {
    // Sanity: invalid field through a named-ref still rejected.
    String script = "STRUCT Addr (zip INT); "
        + "STRUCT Person (a Addr, CHECK (a.bogus > 0));"
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
              "STRUCT T (x VARCHAR(2) DEFAULT 'too long'); "
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
              "STRUCT T (x DECIMAL(3, 1) DEFAULT 999.5); "
                  + "TYPE T"));
        });
    assertTrue(t.getMessage().contains("DECIMAL")
            && t.getMessage().contains("exceeds"),
        "expected DECIMAL precision error, got: " + t.getMessage());
  }

  @Test
  void binaryFixedLengthExactRequired() {
    // H1: BINARY(N) is fixed-length — default must be exactly N bytes.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(
              "STRUCT T (b BINARY(2) DEFAULT x''); TYPE T"));
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
              "STRUCT T (c CHAR(3) DEFAULT 'ab'); TYPE T"));
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
    String script = "STRUCT T (`null` INT, x INT); TYPE T";
    LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
    v.visit(LogicalTypesParserFactory.parse(script));
    assertEquals("null", v.getRootSchema().getFields().get(0).getName());
  }

  @Test
  void timeOverPrecisionDefaultRejected() {
    // M1: TIME(N) DEFAULT '...precision-N+1...' rejected.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> {
          LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
          v.visit(LogicalTypesParserFactory.parse(
              "STRUCT T (t TIME(2) DEFAULT '12:34:56.123456789'); "
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
              "STRUCT T (ts TIMESTAMP(3) DEFAULT '2026-01-01T00:00:00.123456789'); "
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
                "STRUCT __T (__x INT, CHECK (`in` > 0));");
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
    String script = "STRUCT T ("
        + "m MAP<STRING, STRING>,"
        + "CHECK ((m)['k'] = 'v')"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
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
        "addr STRUCT(zip INT) NOT NULL", "addr.zip > 0");
    assertEquals("!has(this.zip) || dyn(this.zip) == null || this.zip > 0", result);
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
    String script = "STRUCT Address (zip INT, city STRING);"
        + "STRUCT Person ("
        + "  name STRING,"
        + "  addr Address NOT NULL,"
        + "  CHECK (addr.zip > 0)"
        + ");";
    Schema person = parseScript(script).getRootSchema();
    assertEquals("!has(this.addr.zip) || dyn(this.addr.zip) == null || this.addr.zip > 0",
        person.getRules().get(0).getExpr());
  }

  @Test
  void namedTypeRefColumnLevelCheckAccepted() {
    // Column-level CHECK on a NAMED_TYPE_REF column — `this` is the
    // resolved struct, accessed directly.
    String script = "STRUCT Address (zip INT);"
        + "STRUCT Person ("
        + "  addr Address NOT NULL CHECK (addr.zip > 0)"
        + ");";
    Schema person = parseScript(script).getRootSchema();
    String cel = person.getField("addr").getRules().get(0).getExpr();
    assertEquals("!has(this.zip) || dyn(this.zip) == null || this.zip > 0", cel);
  }

  @Test
  void namedTypeRefUnresolvedFallsBackToDyn() {
    // Externally-referenced type with no definition — ConstraintTypeProvider
    // falls back to dyn so the CHECK still translates (any field accesses
    // type-check as dyn). Externals are inferred from usage; the bare
    // reference `Address` is sufficient to make Address external.
    String script = "STRUCT Person ("
        + "  addr Address NOT NULL,"
        + "  CHECK (addr.zip > 0)"
        + ");";
    Schema person = parseScript(script).getRootSchema();
    assertEquals("this.addr.zip > 0", person.getRules().get(0).getExpr());
  }

  // ---------------------------------------------------------------------
  // H-3: IS NULL on non-null-returning expressions (dead code)
  // ---------------------------------------------------------------------

  @Test
  void isNullOnLengthRejected() {
    // LENGTH of a NOT NULL name is never NULL — the IS NULL test is
    // unconditionally false. Strict accepts as well-typed; we reject for
    // the user.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheckOver("name STRING NOT NULL", "LENGTH(name) IS NULL"));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection, got: " + t.getMessage());
  }

  @Test
  void isNotNullOnLengthRejected() {
    // Same class — IS NOT NULL on a never-null function is unconditionally
    // true.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheckOver("name STRING NOT NULL", "LENGTH(name) IS NOT NULL"));
    assertTrue(t.getMessage().contains("dead code"),
        "expected dead-code rejection, got: " + t.getMessage());
  }

  @Test
  void isNullOnUpperRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheckOver("name STRING NOT NULL", "UPPER(name) IS NULL"));
    assertTrue(t.getMessage().contains("dead code"),
        "expected UPPER dead-code rejection, got: " + t.getMessage());
  }

  @Test
  void isNullOnCastRejected() {
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheckOver("x INT NOT NULL", "CAST(x AS STRING) IS NULL"));
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
  void isNullOnAFunctionOfANullableArgumentAccepted() throws Exception {
    // A function NULL only for a NULL argument can be NULL when an argument can.
    String columns = "ns STRING, ni INT, nd DECIMAL(10, 2), s STRING NOT NULL";
    Map<String, Object> absent = Map.of("s", "a");
    Map<String, Object> present = new HashMap<>(absent);
    for (String col : List.of("ns", "ni", "nd")) {
      present.put(col, null);
    }
    for (Map<String, Object> self : List.of(present, absent)) {
      Map<String, Object> vars = Map.of("this", CelUtils.toCelValue(self));
      for (String sql : List.of("UPPER(ns) IS NULL", "NOT (LENGTH(ns) IS NOT NULL)",
          "SUBSTRING(ns FROM 1) IS NULL", "TRIM(ns) IS NULL", "REPLACE(s, ns, 'x') IS NULL",
          "POSITION(ns IN s) IS NULL", "ABS(ni) IS NULL", "CAST(ni AS BIGINT) IS NULL",
          "GREATEST(nd, ni) IS NULL", "GREATEST(nd, 1) IS NULL")) {
        String cel = translateCheckOver(columns, sql);
        assertEquals(true, CelUtils.buildProgram(CelUtils.ScriptType.JSON, cel, null,
            CelUtils.toVarDecls(CelUtils.toDeclTypes(vars))).eval(vars),
            sql + " on " + self.keySet() + " -> " + cel);
      }
    }
    // GREATEST of arguments never NULL never is, and a column-level CHECK never sees its column
    // NULL.
    assertThrows(ValidationException.class,
        () -> translateCheckOver(columns, "GREATEST(LENGTH(s), 1) IS NULL"));
    assertThrows(ValidationException.class,
        () -> parseScript("STRUCT T (ns STRING CHECK (UPPER(ns) IS NULL));"));
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
    String script = "STRUCT T ("
        + "tags STRING NOT NULL ARRAY,"
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
        + "zip INT,"
        + "CHECK (zip IS NULL)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullOnNestedNotNullFieldRejected() {
    // Both the parent struct and the nested field are NOT NULL — the path
    // is wholly non-null, so IS NULL is dead.
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT NOT NULL) NOT NULL,"
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
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT) NOT NULL,"
        + "CHECK (addr.zip IS NULL)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullThroughNullableParentAccepted() {
    // Parent struct is nullable — even a NOT NULL leaf field can effectively
    // be unreachable, so IS NULL is meaningful. Validator must defer.
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT NOT NULL),"
        + "CHECK (addr.zip IS NULL)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullOnArrayIndexedAccepted() {
    // Indexing into an ARRAY can produce missing values regardless of
    // element nullability — defer.
    String script = "STRUCT T ("
        + "tags STRING ARRAY,"
        + "CHECK (tags[1] IS NULL)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void isNullOnNotNullColumnInsideMacroRejected() {
    // The pass threads through macro bodies via recurseIntoMacro. The
    // outer NOT NULL column reference is what's dead, not the iter-var.
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT) CHECK (addr.zip IS NULL)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getField("addr").getRules().get(0).getExpr();
    assertTrue(cel.contains("== null"), "got: " + cel);
  }

  @Test
  void columnLevelCoalesceBareColRejected() {
    // COALESCE(<bare-col>, fallback) at column level: `this` is guaranteed
    // non-null under skip-on-null, so the fallback never runs. Dead code.
    String script = "STRUCT T ("
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
    String script = "STRUCT T ("
        + "addr STRUCT(zip INT) CHECK (COALESCE(addr.zip, 0) > 0)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getField("addr").getRules().get(0).getExpr();
    assertTrue(cel.contains("zip"), "got: " + cel);
  }

  @Test
  void columnLevelNullifBareColStillAllowed() {
    // NULLIF can return null when the two args compare equal — that's
    // meaningful regardless of input nullability. Don't reject.
    String script = "STRUCT T ("
        + "x INT CHECK (NULLIF(x, 0) > 0)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getField("x").getRules().get(0).getExpr();
    // NULL when x is 0, as it is when x is.
    assertEquals("this == 0 || dyn(this) == null || this > 0", cel);
  }

  @Test
  void tableLevelBareIsNullStillAllowed() {
    // Table-level CHECK is unaffected by the column-level rule —
    // `has(this.x)` works for format-aware presence at table level.
    String script = "STRUCT T ("
        + "x INT,"
        + "CHECK (x IS NULL)"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getRules().get(0).getExpr();
    assertTrue(cel.contains("has(this.x)"), "got: " + cel);
  }

  @Test
  void columnLevelMacroIterVarIsNullAllowed() {
    // Regression: skip-on-null contract applies to the constrained column,
    // NOT to macro iter-var bindings. `t IS NULL` inside the body of a
    // column-level macro must NOT be rejected — `t` is the binding, not
    // the column.
    String script = "STRUCT T ("
        + "tags STRING ARRAY CHECK (EVERY(tags, t, t IS NULL))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getField("tags").getRules().get(0).getExpr();
    assertTrue(cel.contains("dyn(t) == null"), "got: " + cel);
  }

  @Test
  void columnLevelMacroIterVarIsNotNullAllowed() {
    String script = "STRUCT T ("
        + "tags STRING ARRAY CHECK (EVERY(tags, t, t IS NOT NULL))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
    String cel = schema.getField("tags").getRules().get(0).getExpr();
    assertTrue(cel.contains("dyn(t) != null"), "got: " + cel);
  }

  @Test
  void columnLevelMacroIterVarInCoalesceAllowed() {
    // Same regression class — COALESCE(t, x) inside a column-level macro
    // must NOT be rejected; `t` is the iter-var binding, not the column.
    String script = "STRUCT T ("
        + "tags STRING ARRAY CHECK ("
        + "EVERY(tags, t, COALESCE(t, 'x') = 'x'))"
        + ");";
    Schema schema = parseScript(script).getRootSchema();
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

  // ---------------------------------------------------------------------
  // Deep-review regressions (audit round 12)
  // ---------------------------------------------------------------------

  @Test
  void numericInPredicateComparedToInPredicateKeepsParens() {
    // Bug #1: a numeric-but-boolean-shaped operand (`age IN (1,2)`) was
    // classified as numeric (all leaves numeric), taking the coercion path
    // which assumes a bare value and dropped the IN list / mis-grouped the
    // operators. Such operands must fall through to the wrapped native path.
    // INT common type previously emitted unparenthesized
    // `age in [...] == qty in [...]` (mis-binds under CEL, caught only by
    // strict-check); now each side is parenthesized.
    assertEquals(
        "(this.age in [1, 2]) == (this.qty in [3, 4])",
        translateCheckNotNull("age IN (1, 2) = qty IN (3, 4)"));
  }

  @Test
  void numericBetweenPredicateComparedKeepsBounds() {
    // Bug #1 (double/decimal variant was the silent one): the bounds of a
    // numeric BETWEEN operand were dropped entirely (emitNumericBetweenAs
    // read only the BETWEEN subject), yielding well-typed but wrong CEL.
    // Both bounds must survive.
    assertEquals(
        "(1.0 <= this.ratio && this.ratio <= 10.0) "
            + "== (2.0 <= this.ratio && this.ratio <= 5.0)",
        translateCheckNotNull("(ratio BETWEEN 1.0 AND 10.0) = (ratio BETWEEN 2.0 AND 5.0)"));
  }

  @Test
  void variantGetReturningBooleanAcceptedAtBooleanRoot() {
    // Bug #2: isBooleanFunc only treated CAST(x AS BOOLEAN) as a boolean
    // special-form; VARIANT_GET/TRY_VARIANT_GET ... RETURNING BOOLEAN are
    // boolean too, so a top-level CHECK over one was wrongly rejected.
    String cel = translateCheck("VARIANT_GET(data, '$.active' RETURNING BOOLEAN)");
    assertTrue(cel.contains("variants.as") && cel.contains("\"boolean\""),
        "expected variants.as(..., \"boolean\"), got: " + cel);
    String tryCel =
        translateCheck("TRY_VARIANT_GET(data, '$.active' RETURNING BOOLEAN)");
    assertTrue(tryCel.contains("\"boolean\""),
        "expected boolean extraction, got: " + tryCel);
  }

  @Test
  void aMultisetHasNoSubscript() {
    // A MULTISET has no order, and Avro and JSON hold one as a map of each value to its count,
    // so a subscript would read the wrong value or fail.
    for (String sql : List.of("dprices_ms[1] > 5.0", "(dprices_ms)[1] > 5.0")) {
      ValidationException e = assertThrows(ValidationException.class,
          () -> translateCheck(sql), sql);
      assertTrue(e.getMessage().contains("A subscript of a MULTISET is not supported"),
          e.getMessage());
    }
    assertTrue(translateCheck("dprices[1] > 5.0")
        .contains("decimals.gt(this.dprices[0], decimal(\"5.0\"))"));
  }


  @Test
  void castAsRealResolvesToDoubleAndCoercesOperand() {
    // Bug #5: celTypeFromCastType matched FLOAT/DOUBLE but not REAL, so
    // CAST(x AS REAL) resolved to null — the other operand was not coerced
    // to double (double vs int → strict fail). REAL must resolve to double.
    String cel = translateCheck("CAST(amount AS REAL) > age");
    assertTrue(cel.contains("double(this.age)"),
        "int operand must be coerced to double, got: " + cel);
  }

  @Test
  void nestedStructReservedFieldNameStrictChecks() {
    // Bug #4: the bracket-access `_[_]` overload was declared only for the
    // root struct type, so a CEL-reserved field name on a *nested* struct
    // (`parent.in`, emitted as `this.parent["in"]`) failed strict-check.
    // The overload is now declared for every synthetic struct type.
    Schema inner = Schema.createStruct(java.util.Arrays.asList(
        new Schema.Field("in", Schema.create(Schema.Type.INT), 0)));
    Schema rootStruct = Schema.createStruct(java.util.Arrays.asList(
        new Schema.Field("parent", inner, 0)));
    ConstraintValidationContext vctx = ConstraintValidationContext.tableLevel(
        rootStruct.getFields());
    io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser
        .ScriptContext scriptCtx = io.confluent.kafka.schemaregistry.type.logical
            .LogicalTypesParserFactory.parse(
                "STRUCT __T (__x INT, CHECK (parent.`in` > 0));");
    io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser
        .Check_exprContext checkCtx = findFirstCheckExpr(scriptCtx);
    String cel = ConstraintToCelTranslator.translate(checkCtx, vctx);
    assertTrue(cel.contains("this.parent[\"in\"]"),
        "expected nested index syntax, got: " + cel);
    // Before the fix this threw (no _[_] overload for the nested struct type).
    CelValidator.assertValidStrict(cel, rootStruct);
  }

  // ---------------------------------------------------------------------
  // Deep-review regressions (audit round 13)
  // ---------------------------------------------------------------------

  @Test
  void namedRefStructMapIndexEmitsMapKeyNotArrayIndex() {
    // HIGH: the emitter's indirection walk didn't resolve NAMED_TYPE_REF, so a
    // MAP field reached through a named-type struct was mistaken for an ARRAY
    // and emitted `[(key) - 1]` (1-based→0-based) instead of `[key]`. For a
    // string-keyed map that failed strict-check (`string - int`); for an
    // int-keyed map it was a silent off-by-one. The visitor strict-checks
    // during visit(), so before the fix this script threw.
    String script = "STRUCT Addr (tags MAP<STRING, STRING>); "
        + "STRUCT Person (addr Addr, CHECK (addr.tags['home'] = 'x'));"
        + "TYPE Person";
    LogicalTypesSchemaVisitor v = new LogicalTypesSchemaVisitor();
    v.visit(LogicalTypesParserFactory.parse(script));
    String cel = v.getRootSchema().getRules().get(0).getExpr();
    assertEquals("!has(this.addr) || dyn(this.addr) == null || !has(this.addr.tags) ||"
            + " dyn(this.addr.tags) == null || !('home' in this.addr.tags) ||"
            + " dyn(this.addr.tags['home']) == null || this.addr.tags['home'] == 'x'",
        cel);
  }

  @Test
  void tryVariantGetAsBetweenBoundRejected() {
    // MEDIUM: TRY_VARIANT_GET(... RETURNING T) returns null on a type/path
    // mismatch (emits variants.tryAs(...)) — the same null-bound crash hazard
    // NULLIF is rejected for. Unlike its VARIANT-result siblings, RETURNING a
    // comparable scalar means strict-check does not catch it.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck(
            "x BETWEEN TRY_VARIANT_GET(data, '$.lo' RETURNING INT) AND 10"));
    assertTrue(t.getMessage().contains("TRY_VARIANT_GET"),
        "expected TRY_VARIANT_GET-bound rejection, got: " + t.getMessage());
  }

  @Test
  void isNullOnNumericAndVariantNeverNullFuncsRejected() {
    // LOW parity: numeric scalar funcs and the never-null variant funcs joined
    // NEVER_NULL_FUNCS, so `f(x) IS NULL` is rejected as dead code, consistent
    // with LENGTH/UPPER/etc.
    for (String expr : new String[] {"ABS(x) IS NULL", "TYPE_OF_VARIANT(data) IS NULL"}) {
      Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
          ValidationException.class,
          () -> translateCheckOver("x INT NOT NULL, data VARIANT NOT NULL", expr),
          "expected dead-code rejection for: " + expr);
      assertTrue(t.getMessage().contains("dead code"),
          "expected dead-code rejection, got: " + t.getMessage());
    }
  }

  @Test
  void variantGetReturningTimestampComparisonTypeChecks() {
    // Confirms the deferred LOW item is a non-bug: subtreeHasTemporal not
    // flagging VARIANT_GET RETURNING TIMESTAMP / CURRENT_TIMESTAMP is harmless,
    // because both already emit CEL timestamps — the native comparison path
    // produces correct, well-typed CEL without the timestamp() wrap (which
    // is only needed for timestamp COLUMNS and TIMESTAMP/INTERVAL literals).
    String cel = translateCheck(
        "VARIANT_GET(data, '$.a' RETURNING TIMESTAMP) > CURRENT_TIMESTAMP");
    assertTrue(cel.contains("variants.as") && cel.contains("\"timestamp\"")
            && cel.contains("now"),
        "got: " + cel);
  }

  // ---------------------------------------------------------------------
  // Deep-review regressions (audit round 14)
  // ---------------------------------------------------------------------

  @Test
  void comparisonOfBooleanPredicatesNotCoercedAsNumeric() {
    // Completes the round-12 fix: coercedNumericRaw classified BETWEEN/IN/LIKE
    // as boolean but missed the comparison and logical operators (>, =, AND,
    // OR, NOT, IS NULL). So `(a > 0.0) = (b > 0.0)` — a legitimate "same sign"
    // constraint — was folded to a numeric value and emitted `double((..))`,
    // a strict-check false-rejection for double/decimal leaves. Now each side
    // falls to the native path.
    assertEquals(
        "(this.ratio > 0.5) == (this.ratio < 0.3)",
        translateCheckNotNull("(ratio > 0.5) = (ratio < 0.3)"));
  }

  @Test
  void castDoublePrecisionAlias() {
    // celTypeFromCastType accepted DOUBLE PRECISION (startsWith DOUBLE) but the
    // emit mappers exact-matched the head and threw — a false rejection of the
    // ANSI spelling. Now consistent.
    assertEquals("double(this.x) > 0.5",
        translateCheckNotNull("CAST(x AS DOUBLE PRECISION) > 0.5"));
  }

  @Test
  void castCharacterVaryingAlias() {
    assertEquals("string(this.value) == 'a'",
        translateCheckNotNull("CAST(value AS CHARACTER VARYING) = 'a'"));
  }

  @Test
  void castBinaryVaryingAlias() {
    assertEquals("bytes(this.name) == b\"\\x00\"",
        translateCheckNotNull("CAST(name AS BINARY VARYING) = x'00'"));
  }

  // ---------------------------------------------------------------------
  // Deep-review regressions (audit round 15)
  // ---------------------------------------------------------------------

  @Test
  void caseResultBranchesCoerceToCommonNumericLikeCoalesce() {
    // CASE result branches were emitted without coercion, so mixed numeric
    // branches (THEN amount DECIMAL, ELSE x INT) emitted `c ? Decimal : int`
    // and failed strict-check — while the sibling COALESCE coerces. Now CASE
    // coerces its branches AND resolves to the common numeric type, so the
    // surrounding comparison coerces too.
    assertEquals(
        "has(this.x) && dyn(this.x) != null && this.x > 0 ? !has(this.amount) ||"
            + " dyn(this.amount) == null || decimals.eq(this.amount, decimal(\"1\")) : !has(this.x)"
            + " || dyn(this.x) == null || dyn(decimal(this.x)) == null || decimals.eq(decimal(this.x),"
            + " decimal(\"1\"))",
        translateCheck("CASE WHEN x > 0 THEN amount ELSE x END = 1"));
  }

  @Test
  void caseResultBranchesCoerceToDouble() {
    // THEN ratio (DOUBLE), ELSE x (INT) → x coerced to double in both branches.
    String cel = translateCheck("CASE WHEN x > 0 THEN ratio ELSE x END > 0.0");
    assertTrue(cel.contains("double(this.x)"),
        "int branch should coerce to double, got: " + cel);
  }

  @Test
  void caseWithStringAndNumericBranchesStillRejected() {
    // Genuinely incompatible branches (numeric vs string) remain rejected —
    // the common-type fold only unifies numeric categories.
    org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("CASE WHEN x > 0 THEN 1 ELSE name END = 1"));
  }

  // ---------------------------------------------------------------------
  // Deep-review regressions (audit round 16)
  // ---------------------------------------------------------------------

  @Test
  void coalesceReturnTypeIsCommonNotFirstArg() {
    // COALESCE/GREATEST/LEAST resolved their return type to the FIRST arg's
    // type, but the emit coerces all branches to the common type — so a wider
    // arg in a non-first position made the surrounding context coerce against
    // the wrong (narrow) type and fail strict-check. The result type is now
    // the common type, regardless of arg order.
    String a = translateCheckNotNull("COALESCE(amount, x) = 1");
    String b = translateCheckNotNull("COALESCE(x, amount) = 1");
    assertTrue(a.startsWith("decimals.eq(") && b.startsWith("decimals.eq("),
        "both arg orders should route through decimals.eq, got:\n  " + a + "\n  " + b);
  }

  @Test
  void greatestReturnTypeIsCommonNotFirstArg() {
    // Same order-independence for GREATEST (and LEAST).
    String a = translateCheckNotNull("GREATEST(amount, x) > 1");
    String b = translateCheckNotNull("GREATEST(x, amount) > 1");
    assertTrue(a.startsWith("decimals.gt(") && b.startsWith("decimals.gt("),
        "both arg orders should route through decimals.gt, got:\n  " + a + "\n  " + b);
  }

  @Test
  void coalesceDoubleCommonRegardlessOfOrder() {
    // int-first with a double later arg resolves to double (was int → wrong).
    assertEquals(
        "((!has(this.x) || dyn(this.x) == null) && (!has(this.ratio) || dyn(this.ratio) == null))"
            + " || (has(this.x) && dyn(this.x) != null ? double(this.x) : (!has(this.ratio) ||"
            + " dyn(this.ratio) == null ? dyn(null) : this.ratio)) > 1.0",
        translateCheck("COALESCE(x, ratio) > 1.0"));
  }

  @Test
  void mixedNumericArithmeticResolvesToCommonType() {
    // unifyOperandTypes returned null for mixed numeric arithmetic, so
    // `x + amount` (INT + DECIMAL) had an unknown type. A parenthesized
    // occurrence then emitted `decimal((int + Decimal))` — a native int+Decimal
    // add with no CEL overload. It now resolves to DECIMAL and re-enters the
    // decimal cascade.
    assertEquals(
        "decimals.gt(decimals.add(decimal(this.x), this.amount), decimal(\"0\"))",
        translateCheckNotNull("(x + amount) > 0"));
    // (Bare, non-parenthesized form already worked — kept as a guard.)
    assertEquals(
        "decimals.gt(decimals.add(decimal(this.x), this.amount), decimal(\"0\"))",
        translateCheckNotNull("x + amount > 0"));
  }

  @Test
  void mixedNumericArithmeticInsideCaseResolves() {
    // The unknown arithmetic type also poisoned CASE/COALESCE/GREATEST return
    // types (the arithmetic branch was skipped in the common-type fold), so the
    // outer comparison stayed native and hit a Decimal-vs-int mismatch. With the
    // arithmetic now resolving to DECIMAL, the CASE resolves to DECIMAL and the
    // whole construct routes through decimals.*.
    String cel = translateCheckNotNull("CASE WHEN x > 0 THEN x + amount ELSE x END = 1");
    assertTrue(cel.startsWith("decimals.eq("),
        "expected decimals.eq, got: " + cel);
  }

  @Test
  void mixedNumericArithmeticInsideCoalesceAndGreatestResolves() {
    // COALESCE/GREATEST/LEAST null checks used the native emit of each branch;
    // for a mixed-numeric arithmetic branch that native emit is an
    // un-coercible int+Decimal add. Such a branch is now tested by its inputs,
    // and plain column branches by has(). Both arg orders work.
    String c = translateCheckNotNull("COALESCE(x + amount, x) = 1");
    assertTrue(c.startsWith("decimals.eq(") && c.contains("decimals.add(decimal(this.x), "
        + "this.amount)") && !c.contains("this.x + this.amount"), "got: " + c);
    String g = translateCheckNotNull("GREATEST(x + amount, x) > 1");
    assertTrue(g.startsWith("decimals.gt(") && g.contains("decimals.add(decimal(this.x), "
        + "this.amount)") && !g.contains("this.x + this.amount"), "got: " + g);
  }

  // ---------------------------------------------------------------------
  // Deep-review regressions (audit round 17)
  // ---------------------------------------------------------------------

  @Test
  void inListAllowsMixedNumericElementCategories() {
    // validateInParenList required all elements to share an exact category, so
    // a mixed-numeric list like (1, 2.5) was wrongly rejected — even though the
    // emit coerces subject and elements to their common numeric type. INT
    // subject with a DECIMAL element now coerces to decimal.
    assertEquals(
        "!has(this.x) || dyn(this.x) == null || decimals.eq(decimal(this.x), decimal(\"1\")) ||"
            + " decimals.eq(decimal(this.x), decimal(\"2.5\"))",
        translateCheck("x IN (1, 2.5)"));
    // INT subject, DECIMAL + DOUBLE elements → common double, each element widened only after
    // its NULL test.
    assertEquals("!has(this.x) || dyn(this.x) == null || ((has(this.amount)"
        + " && dyn(this.amount) != null && double(this.x) == double(this.amount))"
        + " || (has(this.ratio) && dyn(this.ratio) != null && double(this.x) == this.ratio)"
        + " ? true : !has(this.amount) || dyn(this.amount)"
        + " == null || !has(this.ratio) || dyn(this.ratio) == null)",
        translateCheck("x IN (amount, ratio)"));
  }

  @Test
  void inListStillRejectsNumericVsStringElements() {
    // Genuinely incompatible element categories remain rejected.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheck("x IN (1, name)"));
    assertTrue(t.getMessage().contains("not comparable"),
        "expected incompatible-element rejection, got: " + t.getMessage());
  }

  // ---------------------------------------------------------------------
  // Deep-review regressions (audit round 18)
  // ---------------------------------------------------------------------

  @Test
  void macroIterationVariableNamedThisRejected() {
    // The macro iteration variable is bound as a CEL macro var; naming it
    // `this` shadows the implicit row/root reference, so the body can no
    // longer reach the row's columns (e.g. EVERY(tags, this, this = name)
    // emitted `this.name` against the loop element and failed strict-check).
    // It's now rejected up front with a clear message, like `now` and CEL
    // reserved words already were.
    Throwable t = org.junit.jupiter.api.Assertions.assertThrows(
        ValidationException.class,
        () -> translateCheckNotNull("EVERY(tags, this, this = 'x')"));
    assertTrue(t.getMessage().contains("this")
            && t.getMessage().toLowerCase(java.util.Locale.ROOT).contains("shadow"),
        "expected 'this' shadow rejection, got: " + t.getMessage());
    // A normal iteration variable name still works.
    assertEquals("this.tags.all(t, t == 'x')",
        translateCheckNotNull("EVERY(tags, t, t = 'x')"));
  }
}
