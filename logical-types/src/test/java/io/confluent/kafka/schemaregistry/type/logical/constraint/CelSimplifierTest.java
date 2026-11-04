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
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.index;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.integer;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.isNull;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.list;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.literal;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.method;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.not;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.or;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.select;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.ternary;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelSimplifier.simplify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class CelSimplifierTest {

  private static final CelNode A = field("a");
  private static final CelNode B = field("b");
  private static final CelNode A_NULL = nullTest(A);
  private static final CelNode A_SET = CelSimplifier.negated(nullTest(A), false);
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

  @Test
  void aConstantIsWorkedOut() {
    CelNode first = binary(literal("1"), "-", literal("1"));
    assertEquals("this.l[0] > 0", simplified(binary(index(field("l"), first), ">", literal("0"))));
    assertEquals("has(this.l[0].x)", simplified(call("has", select(index(field("l"), first), "x"))));
    assertEquals("-1.0 < this.x", simplified(binary(call("double", integer(-1)), "<", field("x"))));
    // Doubles by value; an int and a double, and an overflow, are left to CEL.
    assertEquals("true", simplified(binary(literal("1.0"), "==", literal("1e0"))));
    CelNode mixed = binary(literal("1"), "==", literal("1.0"));
    assertEquals(mixed.toString(), simplified(mixed));
    CelNode overflow = binary(literal("9223372036854775807"), "+", literal("1"));
    assertEquals(overflow.toString(), simplified(overflow));
    assertEquals("false", simplified(binary(dyn(literal("'a'")), "==", NULL)));
  }

  @Test
  void anOperandImpliedByAnotherIsAbsorbed() {
    assertEquals("this.a > 0", simplified(or(A_POSITIVE, and(A_POSITIVE, B_POSITIVE))));
    // Not by one after it, nor by one before it equal to an operand but its first: taken left to
    // right, what it raises would raise first.
    CelNode later = or(and(A_POSITIVE, RAISES), A_POSITIVE);
    assertEquals(later.toString(), simplified(later));
    CelNode second = or(A_POSITIVE, and(RAISES, A_POSITIVE));
    assertEquals(second.toString(), simplified(second));
    // A first operand of the outer kind goes by its own operands, in an || or an &&.
    assertEquals("this.a > 0 || this.b > 0",
        simplified(or(A_POSITIVE, B_POSITIVE, and(or(A_POSITIVE, B_POSITIVE), RAISES))));
    assertEquals("this.a > 0 && this.b > 0",
        simplified(and(A_POSITIVE, B_POSITIVE, or(and(A_POSITIVE, B_POSITIVE), RAISES))));
    // c ? false : false only raises, as an operand raising with c next does.
    assertEquals("1 / this.z > 0", simplified(or(ternary(RAISES, FALSE, FALSE), RAISES)));
    CelNode alone = or(ternary(RAISES, FALSE, FALSE), A_POSITIVE);
    assertEquals(alone.toString(), simplified(alone));
    CelNode apart = or(ternary(RAISES, FALSE, FALSE), A_POSITIVE, RAISES);
    assertEquals(apart.toString(), simplified(apart));
  }

  @Test
  void adjacentOperandsOfAnOrWithTheSameGuardShareIt() {
    assertEquals("has(this.a) && dyn(this.a) != null && (this.a > 0 || this.a < -10)",
        simplified(or(and(A_SET, A_POSITIVE), and(A_SET, binary(A, "<", integer(-10))))));
  }

  @Test
  void aNodeIsSplitOnAConditionItAlwaysEvaluates() {
    CelNode value = ternary(A_POSITIVE, literal("1"), literal("2"));
    assertEquals("this.a > 0", simplified(binary(value, "==", literal("1"))));
    CelNode nullable = ternary(A_POSITIVE, dyn(NULL), B);
    assertEquals("this.a > 0 ? true : dyn(this.b) == null || this.b > 1",
        simplified(or(isNull(nullable), binary(nullable, ">", literal("1")))));
    // Compared, a junction's error is unequal where the condition's raises, so only its
    // operands are split.
    CelNode raising = ternary(RAISES, dyn(NULL), B);
    CelNode compared = binary(or(isNull(raising), binary(raising, ">", literal("1"))), "==",
        field("t"));
    assertEquals("((1 / this.z > 0 ? true : dyn(this.b) == null) || (1 / this.z > 0 ? dyn(null)"
        + " : this.b) > 1) == this.t", simplified(compared));
  }

  @Test
  void anEqualityOfWhatCanBeReadIsAFact() {
    CelNode one = binary(A, "==", literal("1"));
    assertEquals("!has(this.a) || dyn(this.a) == null || this.a == 1 || this.b > 0",
        simplified(or(A_NULL, one, ternary(one, FALSE, B_POSITIVE))));
    CelNode in = binary(A, "in", list(List.of(literal("1"), literal("2"))));
    assertEquals("!has(this.a) || dyn(this.a) == null",
        simplified(or(A_NULL, ternary(in, FALSE, FALSE))));
  }

  @Test
  void nestedTernariesWithASharedBranchAreOne() {
    assertEquals("!has(this.a) || dyn(this.a) == null || this.a > 0 ? this.b : 1",
        simplified(ternary(A_NULL, B, ternary(A_POSITIVE, B, literal("1")))));
    // Merged, the condition is one already known.
    CelNode one = binary(A, "==", literal("1"));
    CelNode check = or(and(A_SET, binary(A, "!=", literal("1"))),
        ternary(A_NULL, B_POSITIVE, ternary(one, B_POSITIVE, binary(B, ">", literal("1")))));
    assertEquals("(has(this.a) && dyn(this.a) != null && this.a != 1) || this.b > 0",
        simplified(check));
    CelNode merged = or(and(A_SET, one),
        ternary(A_SET, ternary(one, binary(B, ">", literal("1")), B_POSITIVE), B_POSITIVE));
    assertEquals("(has(this.a) && dyn(this.a) != null && this.a == 1) || this.b > 0",
        simplified(merged));
    // Known from a ternary's condition, one that may raise.
    CelNode raising = ternary(or(A_NULL, RAISES),
        ternary(A_NULL, B_POSITIVE, ternary(RAISES, B_POSITIVE, binary(B, ">", literal("1")))),
        FALSE);
    assertEquals("!has(this.a) || dyn(this.a) == null || 1 / this.z > 0 ? this.b > 0 : false",
        simplified(raising));
  }
}
