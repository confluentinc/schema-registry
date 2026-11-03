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

import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.FALSE;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.NULL;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.TRUE;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.and;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.binary;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.call;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.dyn;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.ident;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.isNull;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.literal;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.method;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.not;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.or;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.select;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.ternary;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelSimplifier.simplify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CelSimplifierTest {

  private static final CelNode A = field("a");
  private static final CelNode B = field("b");
  private static final CelNode A_NULL = nullTest(A);
  private static final CelNode B_NULL = nullTest(B);
  private static final CelNode A_POSITIVE = binary(A, ">", literal("0"));
  private static final CelNode B_POSITIVE = binary(B, ">", literal("0"));
  // 1 / this.z > 0, which raises when z is 0.
  private static final CelNode RAISES =
      binary(binary(literal("1"), "/", field("z")), ">", literal("0"));

  private static CelNode field(String name) {
    return select(ident("this"), name);
  }

  private static CelNode nullTest(CelNode field) {
    return or(not(call("has", field)), isNull(field));
  }

  private static String simplified(CelNode node) {
    return simplify(node).toString();
  }

  @Test
  void aRepeatedOperandAndAConstantAreFolded() {
    assertEquals("!has(this.a) || dyn(this.a) == null || this.a > 0",
        simplified(or(A_NULL, A_NULL, A_POSITIVE)));
    assertEquals("this.a > 0", simplified(or(FALSE, A_POSITIVE)));
    assertEquals("true", simplified(or(A_NULL, TRUE, RAISES)));
    // Taken left to right, what raises before a deciding constant still does.
    assertEquals("1 / this.z > 0 || true", simplified(or(RAISES, TRUE, A_POSITIVE)));
    assertEquals("this.a > 0", simplified(ternary(TRUE, A_POSITIVE, RAISES)));
    assertEquals("this.a > 0", simplified(not(not(A_POSITIVE))));
  }

  @Test
  void aSafeOperandIsAFactForThoseAfterIt() {
    assertEquals("has(this.a) && dyn(this.a) != null && this.a > 0",
        simplified(and(CelSimplifier.negated(A_NULL, false), or(A_NULL, A_POSITIVE))));
    // GREATEST's value is tested again inside its comparison.
    CelNode greatest = call("math.greatest", A, B);
    CelNode value = ternary(or(A_NULL, B_NULL), dyn(NULL), greatest);
    assertEquals("!has(this.a) || dyn(this.a) == null || !has(this.b) || dyn(this.b) == null"
            + " || math.greatest(this.a, this.b) > 0",
        simplified(or(A_NULL, B_NULL, binary(value, ">", literal("0")))));
  }

  @Test
  void anOperandThatMayRaiseIsNoFact() {
    CelNode check = or(RAISES, and(not(RAISES), A_POSITIVE));
    assertEquals(check.toString(), simplified(check));
    // A nested field's has() raises when its parent is NULL.
    CelNode x = select(field("r"), "x");
    CelNode nested = and(call("has", x), or(not(call("has", x)), A_POSITIVE));
    assertEquals(nested.toString(), simplified(nested));
  }

  @Test
  void aConditionIsAFactInItsBranches() {
    assertEquals("this.a > 0 ? this.b : 1",
        simplified(ternary(A_POSITIVE, ternary(A_POSITIVE, B, literal("2")), literal("1"))));
  }

  @Test
  void aMacroVariableHidesAnOuterOne() {
    CelNode e = ident("e");
    CelNode body = or(isNull(e), binary(e, ">", literal("0")));
    CelNode check = and(CelNode.isNotNull(e), method(field("l"), "all", e, body));
    assertEquals("dyn(e) != null && this.l.all(e, dyn(e) == null || e > 0)",
        simplified(check));
  }

  @Test
  void adjacentOperandsWithTheSameNullTestShareIt() {
    assertEquals("!has(this.a) || dyn(this.a) == null || (this.a > 0 && this.a < 10)",
        simplified(and(or(A_NULL, A_POSITIVE), or(A_NULL, binary(A, "<", literal("10"))))));
    // Not past an operand without it, which it would then decide.
    CelNode apart = and(or(A_NULL, A_POSITIVE), B_POSITIVE, or(A_NULL, A_POSITIVE));
    assertEquals("(!has(this.a) || dyn(this.a) == null || this.a > 0) && this.b > 0",
        simplified(apart));
  }

  @Test
  void aTernaryOnASafeConditionIsAJunctionWhereNotCompared() {
    CelNode caseCondition = ternary(A_NULL, FALSE, A_POSITIVE);
    assertEquals("has(this.a) && dyn(this.a) != null && this.a > 0",
        simplified(caseCondition));
    assertEquals("!has(this.a) || dyn(this.a) == null || 1 / this.z > 0",
        simplified(ternary(A_NULL, TRUE, RAISES)));
    // Compared, an error out of && is unequal where the ternary's raises.
    CelNode compared = binary(ternary(caseCondition, RAISES, FALSE), "!=", field("t"));
    assertEquals("(has(this.a) && dyn(this.a) != null && this.a > 0 ? 1 / this.z > 0 : false)"
        + " != this.t", simplified(compared));
    // A condition that may raise keeps its ternary.
    CelNode raising = ternary(RAISES, FALSE, A_POSITIVE);
    assertEquals(raising.toString(), simplified(raising));
    CelNode sameBranches = ternary(RAISES, FALSE, FALSE);
    assertEquals(sameBranches.toString(), simplified(sameBranches));
  }

  @Test
  void aJunctionComparedIsntLeftASingleOperandThatMayRaise() {
    CelNode compared = binary(dyn(or(A_NULL, RAISES)), "==", NULL);
    CelNode check = or(A_NULL, compared);
    assertEquals(check.toString(), simplified(check));
  }
}
