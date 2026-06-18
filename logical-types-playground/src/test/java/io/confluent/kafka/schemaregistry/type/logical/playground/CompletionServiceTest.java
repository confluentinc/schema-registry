/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.type.logical.playground;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.type.logical.playground.dto.CompleteResponse;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for Tier 1 CHECK-expression completion. The detector should
 * route a caret inside {@code CHECK (...)} parens to the CHECK-mode candidate
 * set (operator keywords + function whitelist + columns) and otherwise leave
 * the existing DDL-keyword path untouched.
 */
class CompletionServiceTest {

  private final CompletionService svc = new CompletionService();

  /** Place the caret at the position marked by `|` in {@code source}. */
  private CompleteResponse completeAt(String source) {
    int caret = source.indexOf('|');
    if (caret < 0) {
      throw new IllegalArgumentException("source must contain a `|` caret marker");
    }
    String stripped = source.substring(0, caret) + source.substring(caret + 1);
    return svc.complete(stripped, caret);
  }

  private Set<String> labels(CompleteResponse r) {
    return r.items.stream().map(i -> i.label).collect(Collectors.toSet());
  }

  // ---------------------------------------------------------------------
  // CHECK-mode detection
  // ---------------------------------------------------------------------

  @Test
  void tableLevelCheckReturnsAllStructFields() {
    // Caret inside a table-level CHECK at expression-start position
    // (right after `(`). Tier 2 narrows the keyword set to expression-
    // openers — AND/OR/IS only appear AFTER a value, so they shouldn't
    // be here. Columns + functions + expression-start keywords should.
    String src = "STRUCT T (a INT, b STRING, lo INT, hi INT, CHECK (|));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("a"), "missing column 'a': " + got);
    assertTrue(got.contains("b"), "missing column 'b': " + got);
    assertTrue(got.contains("lo"), "missing column 'lo': " + got);
    assertTrue(got.contains("hi"), "missing column 'hi': " + got);
    assertTrue(got.contains("LENGTH"), "missing function LENGTH");
    assertTrue(got.contains("CASE"), "missing keyword CASE (expression-start)");
    assertTrue(got.contains("IS_EMAIL"), "missing validator IS_EMAIL");
    assertTrue(got.contains("NOT"), "missing keyword NOT (expression-start)");
    assertFalse(got.contains("AND"),
        "AND should be hidden at expression-start (Tier 2): " + got);
    assertFalse(got.contains("IS"),
        "IS should be hidden at expression-start (Tier 2): " + got);
  }

  @Test
  void columnLevelCheckScopesToAttachedFieldOnly() {
    // Column-level CHECK on `x INT CHECK (|)`. Only `x` is in scope; other
    // fields of the surrounding struct are NOT.
    String src = "STRUCT T (a INT, x INT CHECK (|), b STRING);";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("x"), "missing attached field 'x': " + got);
    assertFalse(got.contains("a"), "should not include sibling 'a': " + got);
    assertFalse(got.contains("b"), "should not include sibling 'b': " + got);
  }

  @Test
  void columnLevelCheckWithConstraintNameStillScopesToField() {
    String src = "STRUCT T (a INT, x INT CONSTRAINT positive_x CHECK (|));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("x"), "missing 'x': " + got);
    assertFalse(got.contains("a"), "should not include 'a': " + got);
  }

  @Test
  void prefixFiltersFunctionsAndColumns() {
    // Caret right after `IS_E` — expect IS_EMAIL filtered in, AND filtered out.
    String src = "STRUCT T (email STRING, CHECK (IS_E|));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("IS_EMAIL"), "missing IS_EMAIL: " + got);
    assertFalse(got.contains("AND"), "AND should be filtered by prefix: " + got);
  }

  @Test
  void outsideCheckClauseReturnsDdlKeywords() {
    // Caret at a DDL position — old keyword path still runs.
    String src = "STRUCT T (x |);";
    Set<String> got = labels(completeAt(src));
    // After a fieldName the existing service suggests TYPE_POSITION
    // primitives; INT/STRING are concrete examples.
    assertTrue(got.contains("INT") || got.contains("STRING"),
        "expected primitive type at type-position: " + got);
    // CHECK-mode functions should NOT appear here.
    assertFalse(got.contains("LENGTH"),
        "LENGTH should not appear outside CHECK: " + got);
    assertFalse(got.contains("IS_EMAIL"),
        "IS_EMAIL should not appear outside CHECK: " + got);
  }

  @Test
  void nestedParensDoNotEscapeCheckScope() {
    // Caret is inside a paren NESTED within the CHECK — still CHECK mode.
    String src = "STRUCT T (a INT, CHECK ((a + |)));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("a"), "missing 'a' in nested-paren CHECK: " + got);
    assertTrue(got.contains("LENGTH"), "missing LENGTH in nested-paren CHECK");
  }

  @Test
  void closedCheckDoesNotLeakIntoNextField() {
    // Caret AFTER a closed CHECK clause — should be back in DDL mode, not CHECK.
    String src = "STRUCT T (a INT CHECK (a > 0), |);";
    Set<String> got = labels(completeAt(src));
    assertFalse(got.contains("LENGTH"),
        "LENGTH should not leak past closed CHECK: " + got);
  }

  // ---------------------------------------------------------------------
  // Tier 2: position-aware narrowing
  // ---------------------------------------------------------------------

  @Test
  void afterColumnSuggestsOperators() {
    // Caret right after a column name → expect AND/OR/IS/IN/BETWEEN/NOT/LIKE,
    // NOT columns or functions (those would chain weirdly: `x LENGTH(...)`
    // is invalid).
    String src = "STRUCT T (x INT, CHECK (x |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("IS"), "missing IS after column: " + got);
    assertTrue(got.contains("AND"), "missing AND after column: " + got);
    assertTrue(got.contains("BETWEEN"), "missing BETWEEN after column: " + got);
    assertTrue(got.contains("LIKE"), "missing LIKE after column: " + got);
    assertFalse(got.contains("LENGTH"),
        "function should not appear after column: " + got);
    assertFalse(got.contains("x"),
        "column should not appear after another column: " + got);
  }

  @Test
  void afterIsSuggestsNotAndNull() {
    String src = "STRUCT T (x INT, CHECK (x IS |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("NULL"), "missing NULL after IS: " + got);
    assertTrue(got.contains("NOT"), "missing NOT after IS: " + got);
    assertFalse(got.contains("LENGTH"),
        "function should not appear after IS: " + got);
    assertFalse(got.contains("AND"),
        "AND should not appear after IS: " + got);
  }

  @Test
  void afterIsNotSuggestsOnlyNull() {
    String src = "STRUCT T (x INT, CHECK (x IS NOT |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("NULL"), "missing NULL after IS NOT: " + got);
    assertFalse(got.contains("LENGTH"));
    assertFalse(got.contains("BETWEEN"));
  }

  @Test
  void afterExtractParenSuggestsFieldNames() {
    String src = "STRUCT T (ts TIMESTAMP, CHECK (EXTRACT(|));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("YEAR"));
    assertTrue(got.contains("MONTH"));
    assertTrue(got.contains("EPOCH"));
    assertFalse(got.contains("LENGTH"),
        "functions should not appear at EXTRACT field position");
  }

  @Test
  void afterExtractFieldSuggestsFrom() {
    String src = "STRUCT T (ts TIMESTAMP, CHECK (EXTRACT(YEAR |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("FROM"), "missing FROM after EXTRACT field");
    assertFalse(got.contains("LENGTH"));
    assertFalse(got.contains("AND"));
  }

  @Test
  void afterCastAsSuggestsTypes() {
    String src = "STRUCT T (x INT, CHECK (CAST(x AS |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("INT"), "missing INT after CAST AS");
    assertTrue(got.contains("STRING"), "missing STRING after CAST AS");
    assertTrue(got.contains("BOOLEAN"));
    assertFalse(got.contains("LENGTH"));
    assertFalse(got.contains("CASE"));
  }

  @Test
  void afterBetweenSuggestsSymmetricAndExpressionStart() {
    String src = "STRUCT T (x INT, CHECK (x BETWEEN |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("SYMMETRIC"));
    assertTrue(got.contains("CURRENT_TIMESTAMP"));
    assertTrue(got.contains("x"), "column should be available as a bound");
    assertTrue(got.contains("LENGTH"), "function should be available as a bound");
  }

  @Test
  void afterNotPostValueSuggestsPostfixPredicates() {
    // `x NOT |` → user is heading toward NOT IN / NOT BETWEEN / NOT LIKE.
    String src = "STRUCT T (x INT, CHECK (x NOT |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("IN"));
    assertTrue(got.contains("BETWEEN"));
    assertTrue(got.contains("LIKE"));
    assertFalse(got.contains("LENGTH"),
        "function should not appear at NOT-postfix position");
  }

  @Test
  void afterAndSuggestsExpressionStart() {
    // `x > 0 AND |` → fresh sub-expression. Should offer columns + functions
    // + NOT/CASE etc., NOT operators (which would be a syntax error).
    String src = "STRUCT T (x INT, CHECK (x > 0 AND |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("x"));
    assertTrue(got.contains("LENGTH"));
    assertTrue(got.contains("CASE"));
    assertFalse(got.contains("AND"),
        "AND should not chain — expression-start, not after-value: " + got);
    assertFalse(got.contains("IS"));
  }

  @Test
  void afterCaseWhenValueSuggestsThen() {
    String src = "STRUCT T (x INT, CHECK (CASE WHEN x > 0 |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("THEN"), "missing THEN after WHEN expr");
    assertFalse(got.contains("ELSE"),
        "ELSE shouldn't appear before THEN: " + got);
  }

  @Test
  void afterCaseThenValueSuggestsWhenElseEnd() {
    String src = "STRUCT T (x INT, CHECK (CASE WHEN x > 0 THEN 1 |));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("WHEN"));
    assertTrue(got.contains("ELSE"));
    assertTrue(got.contains("END"));
  }

  @Test
  void macroBodyIncludesIterVarAsColumn() {
    // Inside `EVERY(tags, t, |)` the iter-var `t` should appear in the
    // column list alongside the schema columns.
    String src = "STRUCT T (tags STRING ARRAY, "
        + "CHECK (EVERY(tags, t, |)));";
    Set<String> got = labels(completeAt(src));
    assertTrue(got.contains("t"), "missing iter-var 't' inside macro body");
    assertTrue(got.contains("tags"), "schema column should still be in scope");
    assertTrue(got.contains("LENGTH"), "functions still offered in macro body");
  }
}
