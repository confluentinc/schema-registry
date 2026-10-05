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

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.cel.runtime.CelRuntime;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.RegexEngine;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.ScriptType;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.CelDecimal;
import java.math.BigDecimal;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Numeric equality for CEL Decimals, everywhere CEL asks — not just at a top-level {@code ==}.
 *
 * <p>{@code in} and container / nested equality never consult the {@code equals} function
 * binding; they go through {@code List.contains} and the recursive {@code objectEquals} walk,
 * which reach the value's own {@code equals}. So under a bare-{@link BigDecimal} runtime value
 * {@code decimal("2.0") == decimal("2.00")} could be patched true while
 * {@code decimal("2.0") in [decimal("2.00")]} stayed false. {@link CelDecimal} fixes all of them
 * at once.
 */
public class CelDecimalEqualityTest {

  /** Evaluate a variable-free boolean expression. */
  private static boolean evalBool(String expr) throws Exception {
    CelRuntime.Program program = CelUtils.buildProgram(
        ScriptType.JSON, expr, null, Collections.emptyList(), RegexEngine.DEFAULT);
    return (Boolean) program.eval();
  }

  private static void assertEval(boolean expected, String expr) throws Exception {
    assertEquals(expected, evalBool(expr), expr);
  }

  // ---- the baseline: a top-level == is numeric and scale-insensitive ----

  @Test
  void topLevelEquality() throws Exception {
    assertEval(true, "decimal(\"2.0\") == decimal(\"2.00\")");
    assertEval(true, "decimal(\"2.0\") == decimal(\"2.0\")");
    assertEval(false, "decimal(\"2.0\") == decimal(\"2.1\")");
    // Zero is the value hashCode has to special-case.
    assertEval(true, "decimal(\"0\") == decimal(\"0.000\")");
    assertEval(true, "decimal(\"-2.0\") == decimal(\"-2.000\")");
  }

  @Test
  void topLevelInequalityNegates() throws Exception {
    assertEval(false, "decimal(\"2.0\") != decimal(\"2.00\")");
    assertEval(false, "decimal(\"2.0\") != decimal(\"2.0\")");
    assertEval(true, "decimal(\"2.0\") != decimal(\"2.1\")");
  }

  @Test
  void equalityAgreesWithDecimalsEq() throws Exception {
    for (String[] pair : new String[][] {
        {"2.0", "2.00"}, {"2.0", "2.0"}, {"2.0", "2.1"},
        {"0", "0.000"}, {"-2.0", "-2.000"}, {"1e2", "100"}}) {
      String a = "decimal(\"" + pair[0] + "\")";
      String b = "decimal(\"" + pair[1] + "\")";
      assertEquals(evalBool("decimals.eq(" + a + ", " + b + ")"), evalBool(a + " == " + b),
          "== must agree with decimals.eq for " + pair[0] + " vs " + pair[1]);
    }
  }

  // ---- THE PAYOFF: `in` (was FALSE for differing scales) ----

  @Test
  void inList_sameValueDifferentScale_isTrue() throws Exception {
    assertEval(true, "decimal(\"2.0\") in [decimal(\"2.00\")]");
  }

  @Test
  void inList_sameValueSameScale_isTrue() throws Exception {
    assertEval(true, "decimal(\"2.0\") in [decimal(\"2.0\")]");
  }

  @Test
  void inList_differentValue_isFalse() throws Exception {
    assertEval(false, "decimal(\"2.0\") in [decimal(\"2.1\")]");
  }

  @Test
  void inList_amongSeveralElements() throws Exception {
    assertEval(true,
        "decimal(\"2.0\") in [decimal(\"1.5\"), decimal(\"2.000\"), decimal(\"9\")]");
    assertEval(false,
        "decimal(\"2.0\") in [decimal(\"1.5\"), decimal(\"2.001\"), decimal(\"9\")]");
    assertEval(false, "decimal(\"2.0\") in []");
  }

  // ---- THE PAYOFF: container equality ----

  @Test
  void listEquality() throws Exception {
    assertEval(true, "[decimal(\"2.0\")] == [decimal(\"2.00\")]");
    assertEval(false, "[decimal(\"2.0\")] == [decimal(\"2.1\")]");
    assertEval(false, "[decimal(\"2.0\")] == [decimal(\"2.0\"), decimal(\"3.0\")]");
    assertEval(true, "[decimal(\"1.0\"), decimal(\"2.0\")] "
        + "== [decimal(\"1.000\"), decimal(\"2.00\")]");
    assertEval(false, "[decimal(\"2.0\")] != [decimal(\"2.00\")]");
  }

  @Test
  void mapEquality() throws Exception {
    assertEval(true, "{\"k\": decimal(\"2.0\")} == {\"k\": decimal(\"2.00\")}");
    assertEval(false, "{\"k\": decimal(\"2.0\")} == {\"k\": decimal(\"2.1\")}");
    assertEval(false, "{\"k\": decimal(\"2.0\")} == {\"j\": decimal(\"2.00\")}");
    assertEval(false, "{\"k\": decimal(\"2.0\")} != {\"k\": decimal(\"2.00\")}");
  }

  // ---- THE PAYOFF: nesting — the recursive objectEquals walk has to reach all the way down ----

  @Test
  void nestedListEquality() throws Exception {
    assertEval(true, "[[decimal(\"2.0\")]] == [[decimal(\"2.00\")]]");
    assertEval(false, "[[decimal(\"2.0\")]] == [[decimal(\"2.1\")]]");
  }

  @Test
  void nestedMapInListEquality() throws Exception {
    assertEval(true, "[{\"k\": decimal(\"2.0\")}] == [{\"k\": decimal(\"2.00\")}]");
    assertEval(false, "[{\"k\": decimal(\"2.0\")}] == [{\"k\": decimal(\"2.1\")}]");
    assertEval(true,
        "{\"a\": {\"b\": [decimal(\"2.0\")]}} == {\"a\": {\"b\": [decimal(\"2.00\")]}}");
    assertEval(false,
        "{\"a\": {\"b\": [decimal(\"2.0\")]}} == {\"a\": {\"b\": [decimal(\"2.1\")]}}");
  }

  @Test
  void containerInList() throws Exception {
    assertEval(true, "[decimal(\"2.0\")] in [[decimal(\"2.00\")]]");
    assertEval(false, "[decimal(\"2.0\")] in [[decimal(\"2.1\")]]");
    assertEval(true, "{\"k\": decimal(\"2.0\")} in [{\"k\": decimal(\"2.00\")}]");
    assertEval(false, "{\"k\": decimal(\"2.0\")} in [{\"k\": decimal(\"2.1\")}]");
  }

  // ---- derived values compare the same way as constructed ones ----

  @Test
  void derivedDecimalsCompareNumerically() throws Exception {
    // Every decimals.* result must be a CelDecimal too, or these fail to dispatch.
    assertEval(true, "decimals.add(decimal(\"1.5\"), decimal(\"0.50\")) == decimal(\"2\")");
    assertEval(true, "decimals.round(decimal(\"2.004\"), 2) == decimal(\"2\")");
    assertEval(true, "decimals.neg(decimal(\"2.0\")) == decimal(\"-2.00\")");
    assertEval(true, "decimals.sqrt(decimal(\"144\")) == decimal(\"12.0\")");
    assertEval(true, "decimals.trunc(decimal(\"2.99\"), 1) == decimal(\"2.9\")");
    assertEval(true, "decimals.greatest(decimal(\"2.0\"), decimal(\"1\")) == decimal(\"2\")");
    assertEval(true, "decimals.add(decimal(\"1\"), decimal(\"1\")) in [decimal(\"2.000\")]");
    // And the scalar-returning consumers still unwrap.
    assertEval(true, "decimals.sign(decimal(\"-2.0\")) == -1");
    assertEval(true, "string(decimal(\"2.50\")) == \"2.50\"");
    assertEval(true, "double(decimal(\"2.50\")) == 2.5");
    // decimal(decimal(x)) — the dyn re-entry arm of DecimalUtils.toBigDecimal.
    assertEval(true, "decimal(decimal(\"2.0\")) == decimal(\"2.00\")");
  }

  // ---- the wrapper's own contract ----

  @Test
  void wrapperEqualsAndHashCodeAreConsistent() {
    CelDecimal a = CelDecimal.of(new BigDecimal("2.0"));
    CelDecimal b = CelDecimal.of(new BigDecimal("2.00"));
    CelDecimal c = CelDecimal.of(new BigDecimal("2.1"));
    assertEquals(a, b, "2.0 and 2.00 must be equal");
    assertEquals(a.hashCode(), b.hashCode(), "equal values must hash alike");
    assertEquals(false, a.equals(c));
    assertEquals(0, a.compareTo(b));
    assertEquals(-1, Integer.signum(a.compareTo(c)));
    // Zero across scales — the hashCode special case.
    assertEquals(CelDecimal.of(new BigDecimal("0")), CelDecimal.of(new BigDecimal("0.000")));
    assertEquals(CelDecimal.of(new BigDecimal("0")).hashCode(),
        CelDecimal.of(new BigDecimal("0.000")).hashCode());
    assertEquals(CelDecimal.of(new BigDecimal("-0.00")).hashCode(),
        CelDecimal.of(new BigDecimal("0")).hashCode());
    // Not a Number — load-bearing, see CelDecimal's javadoc.
    assertEquals(false, Number.class.isAssignableFrom(CelDecimal.class),
        "CelDecimal must not be a Number");
    assertEquals(new BigDecimal("2.0"), a.value());
    assertEquals("2.0", a.toString());
    assertEquals(false, a.equals(new BigDecimal("2.0")),
        "a raw BigDecimal is not a CelDecimal");
  }
}
