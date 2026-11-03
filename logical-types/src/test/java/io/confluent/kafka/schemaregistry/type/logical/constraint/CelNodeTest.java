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

import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.and;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.binary;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.call;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.ident;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.index;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.literal;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.method;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.negate;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.not;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.or;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.select;
import static io.confluent.kafka.schemaregistry.type.logical.constraint.CelNode.ternary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.cel.common.CelValidationResult;
import dev.cel.common.ast.CelExpr;
import dev.cel.parser.CelParser;
import dev.cel.parser.CelParserFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class CelNodeTest {

  private static final CelParser PARSER = CelParserFactory.standardCelParserBuilder().build();

  private static final CelNode A = ident("a");
  private static final CelNode B = ident("b");
  private static final CelNode C = ident("c");

  @Test
  void anOperandIsBracketedOnlyWherePrecedenceNeedsIt() {
    assertEquals("a + b * c", binary(A, "+", binary(B, "*", C)).toString());
    assertEquals("(a + b) * c", binary(binary(A, "+", B), "*", C).toString());
    assertEquals("a - b - c", binary(binary(A, "-", B), "-", C).toString());
    assertEquals("a - (b - c)", binary(A, "-", binary(B, "-", C)).toString());
    assertEquals("a + b == c", binary(binary(A, "+", B), "==", C).toString());
    assertEquals("a.f(b + c)[a - b].g", select(index(method(A, "f", binary(B, "+", C)),
        binary(A, "-", B)), "g").toString());
    assertEquals("(a + b).size()", method(binary(A, "+", B), "size").toString());
    assertEquals("(-a).abs()", method(negate(A), "abs").toString());
    assertEquals("-a.abs()", negate(method(A, "abs")).toString());
    // CEL's parser takes a '-' before a number as its sign.
    assertEquals("-1", negate(literal("1")).toString());
    assertEquals("-(1.f)", negate(select(literal("1"), "f")).toString());
    assertEquals("!(a == b)", not(binary(A, "==", B)).toString());
    assertEquals("!f(a)", not(call("f", A)).toString());
    assertEquals("a || b ? a : b", ternary(or(A, B), A, B).toString());
    assertEquals("f(a ? b : c, [a, b])", call("f", ternary(A, B, C),
        CelNode.list(Arrays.asList(A, B))).toString());
  }

  @Test
  void aTernaryInAnOperatorAndAnAndUnderAnOrAreBracketedForReading() {
    assertEquals("a || (b && c)", or(A, and(B, C)).toString());
    assertEquals("(a || b) && c", and(or(A, B), C).toString());
    assertEquals("(a ? b : c) == a", binary(ternary(A, B, C), "==", A).toString());
    assertEquals("a ? b : (c ? a : b)", ternary(A, B, ternary(C, A, B)).toString());
    assertEquals("(a ? b : c) ? a : b", ternary(ternary(A, B, C), A, B).toString());
    assertEquals("a && (b ? c : a)", and(A, ternary(B, C, A)).toString());
    // A comparison of comparisons, which CEL groups to the left.
    assertEquals("(a == b) == c", binary(binary(A, "==", B), "==", C).toString());
    assertEquals("a == (b == c)", binary(A, "==", binary(B, "==", C)).toString());
  }

  @Test
  void aNegationOfANegationIsBracketed() {
    // CEL's parser drops !! and --.
    assertEquals("!(!a)", not(not(A)).toString());
    assertEquals("-(-a)", negate(negate(A)).toString());
  }

  @Test
  void aJunctionIsFlattenedAndComparedByStructure() {
    CelNode nested = or(or(A, B), C);
    assertEquals("a || b || c", nested.toString());
    assertEquals(or(A, B, C), nested);
    assertEquals(or(A, B, C).hashCode(), nested.hashCode());
    assertNotEquals(or(A, B, C), and(A, B, C));
    assertNotEquals(literal("a"), A);
    assertTrue(nested.contains(B));
    assertFalse(nested.contains(or(A, B)));
    assertEquals(B, CelNode.isNullOf(CelNode.isNull(B)));
    assertNull(CelNode.isNullOf(binary(B, "==", CelNode.NULL)));
  }

  @Test
  void aNodesSizeIsItsTextsInCodePoints() {
    CelNode node = and(or(A, CelNode.string("😀")), ternary(A, B, C));
    String text = node.toString();
    assertEquals(text.codePointCount(0, text.length()), node.size());
  }

  @Test
  void eachPrintedTreeParsesBackToItself() throws Exception {
    // Random trees, each printed and parsed back with cel-java, whose tree must be the same.
    Random random = new Random(42);
    for (int i = 0; i < 3000; i++) {
      CelNode node = randomNode(random, 5);
      String text = node.toString();
      CelValidationResult parsed = PARSER.parse(text);
      assertFalse(parsed.hasError(), text);
      assertEquals(shape(node), shape(parsed.getAst().getExpr()), text);
      assertEquals(text.codePointCount(0, text.length()), node.size(), text);
    }
  }

  private static final String[] BINARY_OPS =
      {"==", "!=", "<", "<=", ">", ">=", "in", "+", "-", "*", "/", "%"};

  private static CelNode randomNode(Random random, int depth) {
    if (depth == 0 || random.nextInt(5) == 0) {
      CelNode[] leaves = {A, B, C, literal("1"), CelNode.string("s\"q"), CelNode.TRUE,
          CelNode.NULL, select(ident("this"), "x")};
      return leaves[random.nextInt(leaves.length)];
    }
    switch (random.nextInt(10)) {
      case 0:
        return not(randomNode(random, depth - 1));
      case 1:
        return negate(randomNode(random, depth - 1));
      case 2:
        return binary(randomNode(random, depth - 1),
            BINARY_OPS[random.nextInt(BINARY_OPS.length)], randomNode(random, depth - 1));
      case 3:
        return and(randomNode(random, depth - 1), randomNode(random, depth - 1));
      case 4:
        return or(randomNode(random, depth - 1), randomNode(random, depth - 1));
      case 5:
        return ternary(randomNode(random, depth - 1), randomNode(random, depth - 1),
            randomNode(random, depth - 1));
      case 6:
        return index(randomNode(random, depth - 1), randomNode(random, depth - 1));
      case 7:
        return select(randomNode(random, depth - 1), "f");
      case 8:
        return method(randomNode(random, depth - 1), "g", randomNode(random, depth - 1));
      default:
        return call("h", randomNode(random, depth - 1),
            CelNode.list(Arrays.asList(randomNode(random, depth - 1))));
    }
  }

  // A tree's shape, ||/&& chains flattened, as cel-java parses it.
  private static String shape(CelExpr e) {
    switch (e.getKind()) {
      case CALL:
        List<String> args = new ArrayList<>();
        String fn = e.call().function();
        if (fn.equals("_||_") || fn.equals("_&&_")) {
          flatten(e, fn, args);
        } else {
          for (CelExpr arg : e.call().args()) {
            args.add(shape(arg));
          }
        }
        return (e.call().target().isPresent() ? shape(e.call().target().get()) + "." : "")
            + fn + args;
      case SELECT:
        return shape(e.select().operand()) + "." + e.select().field();
      case LIST:
        List<String> elements = new ArrayList<>();
        for (CelExpr element : e.list().elements()) {
          elements.add(shape(element));
        }
        return "list" + elements;
      case IDENT:
        return e.ident().name();
      case CONSTANT:
        return e.constant().toString();
      default:
        throw new AssertionError("Unexpected " + e);
    }
  }

  private static void flatten(CelExpr e, String fn, List<String> out) {
    if (e.getKind() == CelExpr.ExprKind.Kind.CALL && e.call().function().equals(fn)) {
      for (CelExpr arg : e.call().args()) {
        flatten(arg, fn, out);
      }
    } else {
      out.add(shape(e));
    }
  }

  // A node's shape, as shape(CelExpr) gives cel-java's.
  private static String shape(CelNode node) throws Exception {
    // CEL's parser takes a negated number as a negative literal.
    if (node instanceof CelNode.Literal || node instanceof CelNode.Unary
        && ((CelNode.Unary) node).op.equals("-")
        && ((CelNode.Unary) node).operand instanceof CelNode.Literal) {
      return shape(PARSER.parse(node.toString()).getAst().getExpr());
    }
    if (node instanceof CelNode.Ident) {
      return node.toString();
    }
    if (node instanceof CelNode.Select) {
      return shape(((CelNode.Select) node).operand) + "." + ((CelNode.Select) node).field;
    }
    if (node instanceof CelNode.Index) {
      return "_[_]" + shapes(Arrays.asList(((CelNode.Index) node).operand,
          ((CelNode.Index) node).index));
    }
    if (node instanceof CelNode.Call) {
      CelNode.Call call = (CelNode.Call) node;
      return (call.target != null ? shape(call.target) + "." : "") + call.function
          + shapes(call.args);
    }
    if (node instanceof CelNode.Unary) {
      return (((CelNode.Unary) node).op + "_") + shapes(node.children());
    }
    if (node instanceof CelNode.Binary) {
      String op = ((CelNode.Binary) node).op;
      return (op.equals("in") ? "@in" : "_" + op + "_") + shapes(node.children());
    }
    if (node instanceof CelNode.Junction) {
      return (((CelNode.Junction) node).and ? "_&&_" : "_||_") + shapes(node.children());
    }
    if (node instanceof CelNode.Ternary) {
      return "_?_:_" + shapes(node.children());
    }
    return "list" + shapes(node.children());
  }

  private static List<String> shapes(List<CelNode> nodes) throws Exception {
    List<String> out = new ArrayList<>();
    for (CelNode node : nodes) {
      out.add(shape(node));
    }
    return out;
  }
}
