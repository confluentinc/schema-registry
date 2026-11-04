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

import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A CEL expression the CHECK translator emits: a tree, compared by structure and printed once.
 * Its text brackets an operand only where CEL's precedence needs it, and also a ternary inside
 * an operator and an {@code &&} under {@code ||}, for reading. A negation of a negation is
 * bracketed too, as CEL's parser drops {@code !!} and {@code --}, and so is a member of a number
 * after {@code -}, which it takes as the number's sign. Its size, saturating at
 * {@link Integer#MAX_VALUE}, is computed without printing it, so one over CEL's limit is refused
 * before it is printed.
 */
abstract class CelNode {

  // CEL's precedence levels, loosest first.
  static final int TERNARY = 0;
  static final int OR = 1;
  static final int AND = 2;
  static final int RELATION = 3;
  static final int ADD = 4;
  static final int MUL = 5;
  static final int UNARY = 6;
  static final int MEMBER = 7;

  static final CelNode TRUE = new Literal("true");
  static final CelNode FALSE = new Literal("false");
  static final CelNode NULL = new Literal("null");

  // CEL's number tokens. A string or bytes token is checked by quoted(), as a regex would
  // overflow the stack on a long one.
  private static final Pattern NUMBER = Pattern.compile("(?:\\d+|0[xX][0-9a-fA-F]+)[uU]?"
      + "|(?:\\d+\\.\\d+|\\.\\d+)(?:[eE][+-]?\\d+)?|\\d+[eE][+-]?\\d+");

  /**
   * The deepest tree built: the tree's walks recurse, and cel-java refuses CEL far shallower.
   */
  static final int MAX_DEPTH = 1024;

  private final int depth;
  private int hash;
  private int size = -1;

  // Refused deeper than MAX_DEPTH, before anything walks it.
  CelNode(List<CelNode> children) {
    int deepest = 0;
    for (CelNode child : children) {
      deepest = Math.max(deepest, child.depth);
    }
    depth = deepest + 1;
    if (depth > MAX_DEPTH) {
      throw new ValidationException("CHECK expression's CEL nests deeper than " + MAX_DEPTH
          + " — simplify or break the constraint into smaller pieces.");
    }
  }

  abstract int precedence();

  abstract void print(StringBuilder sb);

  abstract boolean sameAs(CelNode other);

  abstract int computeHash();

  // The operands, in order.
  abstract List<CelNode> children();

  /**
   * The length of this node's text, in code points, computed once without printing it.
   */
  final long size() {
    if (size < 0) {
      size = (int) Math.min(measure(), Integer.MAX_VALUE);
    }
    return size;
  }

  // The size of this node's text from its children's.
  abstract long measure();

  /**
   * True if {@code node} is this or one of its subexpressions.
   */
  final boolean contains(CelNode node) {
    return contains(node, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  // Each shared subexpression is searched once.
  private boolean contains(CelNode node, Set<CelNode> seen) {
    if (!seen.add(this)) {
      return false;
    }
    if (equals(node)) {
      return true;
    }
    for (CelNode child : children()) {
      if (child.contains(node, seen)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof CelNode && o.getClass() == getClass() && hashCode() == o.hashCode()
        && sameAs((CelNode) o);
  }

  @Override
  public final int hashCode() {
    if (hash == 0) {
      int h = computeHash();
      hash = h == 0 ? 1 : h;
    }
    return hash;
  }

  @Override
  public final String toString() {
    StringBuilder sb = new StringBuilder((int) Math.min(size(), 1 << 16));
    print(sb);
    return sb.toString();
  }

  // ---------------------------------------------------------------------------------------------
  // Factories
  // ---------------------------------------------------------------------------------------------

  static CelNode bool(boolean value) {
    return value ? TRUE : FALSE;
  }

  /**
   * A literal, as CEL's token for it: a number, a quoted string or bytes.
   */
  static CelNode literal(String token) {
    if (!NUMBER.matcher(token).matches() && !quoted(token)) {
      throw new IllegalArgumentException("Not a CEL literal: " + token);
    }
    return new Literal(token);
  }

  // True if token is a quoted string, or b and a quoted one: an escape takes the next character,
  // which can't be the closing quote.
  private static boolean quoted(String token) {
    int open = token.startsWith("b") || token.startsWith("B") ? 1 : 0;
    int close = token.length() - 1;
    if (close <= open || token.charAt(open) != '"' && token.charAt(open) != '\''
        || token.charAt(close) != token.charAt(open)) {
      return false;
    }
    for (int i = open + 1; i < close; i++) {
      if (token.charAt(i) == '\\') {
        if (++i == close) {
          return false;
        }
      } else if (token.charAt(i) == token.charAt(open)) {
        return false;
      }
    }
    return true;
  }

  /**
   * A number literal from its text, a negative one as - before its magnitude.
   */
  static CelNode number(String text) {
    return text.startsWith("-") ? negate(literal(text.substring(1))) : literal(text);
  }

  /**
   * An int literal of {@code value}, a negative one as - before its magnitude.
   */
  static CelNode integer(long value) {
    return number(Long.toString(value));
  }

  /**
   * A double-quoted string literal of {@code value}.
   */
  static CelNode string(String value) {
    return new Literal("\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"");
  }

  static CelNode ident(String name) {
    return new Ident(name);
  }

  static CelNode select(CelNode operand, String field) {
    return new Select(operand, field);
  }

  static CelNode index(CelNode operand, CelNode index) {
    return new Index(operand, index);
  }

  static CelNode call(String function, CelNode... args) {
    return new Call(null, function, Arrays.asList(args));
  }

  static CelNode call(String function, List<CelNode> args) {
    return new Call(null, function, args);
  }

  static CelNode method(CelNode target, String function, CelNode... args) {
    return new Call(target, function, Arrays.asList(args));
  }

  static CelNode method(CelNode target, String function, List<CelNode> args) {
    return new Call(target, function, args);
  }

  static CelNode dyn(CelNode operand) {
    return call("dyn", operand);
  }

  static CelNode not(CelNode operand) {
    return new Unary("!", operand);
  }

  static CelNode negate(CelNode operand) {
    return new Unary("-", operand);
  }

  static CelNode binary(CelNode left, String op, CelNode right) {
    return new Binary(left, op, right);
  }

  /**
   * {@code dyn(operand) == null}.
   */
  static CelNode isNull(CelNode operand) {
    return binary(dyn(operand), "==", NULL);
  }

  /**
   * {@code dyn(operand) != null}.
   */
  static CelNode isNotNull(CelNode operand) {
    return binary(dyn(operand), "!=", NULL);
  }

  /**
   * The operand {@code test} tests, if it is {@code dyn(operand) == null}, else null.
   */
  static CelNode isNullOf(CelNode test) {
    if (!(test instanceof Binary) || !((Binary) test).op.equals("==")
        || !((Binary) test).right.equals(NULL) || !(((Binary) test).left instanceof Call)) {
      return null;
    }
    Call dyn = (Call) ((Binary) test).left;
    return dyn.target == null && dyn.function.equals("dyn") && dyn.args.size() == 1
        ? dyn.args.get(0) : null;
  }

  static CelNode and(CelNode... operands) {
    return Junction.of(true, Arrays.asList(operands));
  }

  static CelNode and(List<CelNode> operands) {
    return Junction.of(true, operands);
  }

  static CelNode or(CelNode... operands) {
    return Junction.of(false, Arrays.asList(operands));
  }

  static CelNode or(List<CelNode> operands) {
    return Junction.of(false, operands);
  }

  static CelNode ternary(CelNode condition, CelNode whenTrue, CelNode whenFalse) {
    return new Ternary(condition, whenTrue, whenFalse);
  }

  static CelNode list(List<CelNode> elements) {
    return new ListLit(elements);
  }

  // ---------------------------------------------------------------------------------------------
  // Printing
  // ---------------------------------------------------------------------------------------------

  // True if child is bracketed where an operand of at least the given precedence goes.
  static boolean bracketed(CelNode child, int min) {
    return child.precedence() < min;
  }

  static void printOperand(StringBuilder sb, CelNode child, int min) {
    if (bracketed(child, min)) {
      sb.append('(');
      child.print(sb);
      sb.append(')');
    } else {
      child.print(sb);
    }
  }

  static long measureOperand(CelNode child, int min) {
    return child.size() + (bracketed(child, min) ? 2 : 0);
  }

  static long codePoints(String text) {
    return text.codePointCount(0, text.length());
  }

  static void printList(StringBuilder sb, List<CelNode> nodes) {
    for (int i = 0; i < nodes.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      nodes.get(i).print(sb);
    }
  }

  static long measureList(List<CelNode> nodes) {
    long n = Math.max(0, 2 * (nodes.size() - 1));
    for (CelNode node : nodes) {
      n += node.size();
    }
    return n;
  }

  // True if node's text starts with a number.
  static boolean startsWithNumber(CelNode node) {
    while (true) {
      if (node instanceof Literal) {
        char first = ((Literal) node).text.charAt(0);
        return first >= '0' && first <= '9' || first == '.';
      }
      if (node instanceof Select && !bracketed(((Select) node).operand, MEMBER)) {
        node = ((Select) node).operand;
      } else if (node instanceof Index && !bracketed(((Index) node).operand, MEMBER)) {
        node = ((Index) node).operand;
      } else if (node instanceof Call && ((Call) node).target != null
          && !bracketed(((Call) node).target, MEMBER)) {
        node = ((Call) node).target;
      } else {
        return false;
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Nodes
  // ---------------------------------------------------------------------------------------------

  // A literal or identifier, by its token.
  private abstract static class Token extends CelNode {
    final String text;

    Token(String text) {
      super(Collections.emptyList());
      this.text = text;
    }

    @Override
    int precedence() {
      return MEMBER;
    }

    @Override
    void print(StringBuilder sb) {
      sb.append(text);
    }

    @Override
    long measure() {
      return codePoints(text);
    }

    @Override
    boolean sameAs(CelNode other) {
      return text.equals(((Token) other).text);
    }

    @Override
    int computeHash() {
      return text.hashCode();
    }

    @Override
    List<CelNode> children() {
      return Collections.emptyList();
    }
  }

  static final class Literal extends Token {
    Literal(String token) {
      super(token);
    }

    /**
     * True if this is a non-negative int literal.
     */
    boolean isNatural() {
      return !text.isEmpty() && text.chars().allMatch(c -> c >= '0' && c <= '9');
    }
  }

  static final class Ident extends Token {
    Ident(String name) {
      super(name);
    }
  }

  static final class Select extends CelNode {
    final CelNode operand;
    final String field;

    Select(CelNode operand, String field) {
      super(Collections.singletonList(operand));
      this.operand = operand;
      this.field = field;
    }

    @Override
    int precedence() {
      return MEMBER;
    }

    @Override
    void print(StringBuilder sb) {
      printOperand(sb, operand, MEMBER);
      sb.append('.').append(field);
    }

    @Override
    long measure() {
      return measureOperand(operand, MEMBER) + 1 + codePoints(field);
    }

    @Override
    boolean sameAs(CelNode other) {
      Select o = (Select) other;
      return field.equals(o.field) && operand.equals(o.operand);
    }

    @Override
    int computeHash() {
      return Objects.hash(operand, field);
    }

    @Override
    List<CelNode> children() {
      return Collections.singletonList(operand);
    }
  }

  static final class Index extends CelNode {
    final CelNode operand;
    final CelNode index;

    Index(CelNode operand, CelNode index) {
      super(Arrays.asList(operand, index));
      this.operand = operand;
      this.index = index;
    }

    @Override
    int precedence() {
      return MEMBER;
    }

    @Override
    void print(StringBuilder sb) {
      printOperand(sb, operand, MEMBER);
      sb.append('[');
      index.print(sb);
      sb.append(']');
    }

    @Override
    long measure() {
      return measureOperand(operand, MEMBER) + index.size() + 2;
    }

    @Override
    boolean sameAs(CelNode other) {
      Index o = (Index) other;
      return operand.equals(o.operand) && index.equals(o.index);
    }

    @Override
    int computeHash() {
      return Objects.hash(operand, index, 1);
    }

    @Override
    List<CelNode> children() {
      return Arrays.asList(operand, index);
    }
  }

  // A function call, or with a target a method call.
  static final class Call extends CelNode {
    final CelNode target;
    final String function;
    final List<CelNode> args;

    Call(CelNode target, String function, List<CelNode> args) {
      super(target == null ? args : withTarget(target, args));
      this.target = target;
      this.function = function;
      this.args = Collections.unmodifiableList(new ArrayList<>(args));
    }

    @Override
    int precedence() {
      return MEMBER;
    }

    @Override
    void print(StringBuilder sb) {
      if (target != null) {
        printOperand(sb, target, MEMBER);
        sb.append('.');
      }
      sb.append(function).append('(');
      printList(sb, args);
      sb.append(')');
    }

    @Override
    long measure() {
      return (target == null ? 0 : measureOperand(target, MEMBER) + 1) + codePoints(function)
          + 2 + measureList(args);
    }

    @Override
    boolean sameAs(CelNode other) {
      Call o = (Call) other;
      return function.equals(o.function) && Objects.equals(target, o.target)
          && args.equals(o.args);
    }

    @Override
    int computeHash() {
      return Objects.hash(target, function, args);
    }

    @Override
    List<CelNode> children() {
      return target == null ? args : withTarget(target, args);
    }

    private static List<CelNode> withTarget(CelNode target, List<CelNode> args) {
      List<CelNode> all = new ArrayList<>(args.size() + 1);
      all.add(target);
      all.addAll(args);
      return all;
    }
  }

  static final class Unary extends CelNode {
    final String op;
    final CelNode operand;

    Unary(String op, CelNode operand) {
      super(Collections.singletonList(operand));
      this.op = op;
      this.operand = operand;
    }

    @Override
    int precedence() {
      return UNARY;
    }

    // CEL's parser takes a '-' before a number as its sign, even one a member is selected from,
    // so that one is bracketed: -(1.f), not (-1).f.
    private int operandMin() {
      return op.equals("-") && !(operand instanceof Literal) && startsWithNumber(operand)
          ? MEMBER + 1 : MEMBER;
    }

    @Override
    void print(StringBuilder sb) {
      sb.append(op);
      printOperand(sb, operand, operandMin());
    }

    @Override
    long measure() {
      return 1 + measureOperand(operand, operandMin());
    }

    @Override
    boolean sameAs(CelNode other) {
      Unary o = (Unary) other;
      return op.equals(o.op) && operand.equals(o.operand);
    }

    @Override
    int computeHash() {
      return Objects.hash(op, operand);
    }

    @Override
    List<CelNode> children() {
      return Collections.singletonList(operand);
    }
  }

  static final class Binary extends CelNode {
    final CelNode left;
    final String op;
    final CelNode right;
    private final int precedence;

    Binary(CelNode left, String op, CelNode right) {
      super(Arrays.asList(left, right));
      this.left = left;
      this.op = op;
      this.right = right;
      switch (op) {
        case "==":
        case "!=":
        case "<":
        case "<=":
        case ">":
        case ">=":
        case "in":
          precedence = RELATION;
          break;
        case "+":
        case "-":
          precedence = ADD;
          break;
        case "*":
        case "/":
        case "%":
          precedence = MUL;
          break;
        default:
          throw new IllegalArgumentException("Not a CEL binary operator: " + op);
      }
    }

    @Override
    int precedence() {
      return precedence;
    }

    // A comparison's operands are bracketed when comparisons themselves; arithmetic is
    // left-associative.
    private int leftMin() {
      return precedence == RELATION ? ADD : precedence;
    }

    private int rightMin() {
      return precedence + 1;
    }

    @Override
    void print(StringBuilder sb) {
      printOperand(sb, left, leftMin());
      sb.append(' ').append(op).append(' ');
      printOperand(sb, right, rightMin());
    }

    @Override
    long measure() {
      return measureOperand(left, leftMin()) + op.length() + 2
          + measureOperand(right, rightMin());
    }

    @Override
    boolean sameAs(CelNode other) {
      Binary o = (Binary) other;
      return op.equals(o.op) && left.equals(o.left) && right.equals(o.right);
    }

    @Override
    int computeHash() {
      return Objects.hash(left, op, right);
    }

    @Override
    List<CelNode> children() {
      return Arrays.asList(left, right);
    }
  }

  // && or ||, over two or more operands, a nested one of the same kind flattened into it.
  static final class Junction extends CelNode {
    final boolean and;
    final List<CelNode> operands;

    private Junction(boolean and, List<CelNode> operands) {
      super(operands);
      this.and = and;
      this.operands = Collections.unmodifiableList(operands);
    }

    static CelNode of(boolean and, List<CelNode> operands) {
      List<CelNode> flat = new ArrayList<>();
      for (CelNode operand : operands) {
        if (operand instanceof Junction && ((Junction) operand).and == and) {
          flat.addAll(((Junction) operand).operands);
        } else {
          flat.add(operand);
        }
      }
      if (flat.isEmpty()) {
        throw new IllegalArgumentException("No operands");
      }
      return flat.size() == 1 ? flat.get(0) : new Junction(and, flat);
    }

    @Override
    int precedence() {
      return and ? AND : OR;
    }

    @Override
    void print(StringBuilder sb) {
      for (int i = 0; i < operands.size(); i++) {
        if (i > 0) {
          sb.append(and ? " && " : " || ");
        }
        printOperand(sb, operands.get(i), RELATION);
      }
    }

    @Override
    long measure() {
      long n = 4 * (operands.size() - 1);
      for (CelNode operand : operands) {
        n += measureOperand(operand, RELATION);
      }
      return n;
    }

    @Override
    boolean sameAs(CelNode other) {
      Junction o = (Junction) other;
      return and == o.and && operands.equals(o.operands);
    }

    @Override
    int computeHash() {
      return Objects.hash(and, operands);
    }

    @Override
    List<CelNode> children() {
      return operands;
    }
  }

  static final class Ternary extends CelNode {
    final CelNode condition;
    final CelNode whenTrue;
    final CelNode whenFalse;

    Ternary(CelNode condition, CelNode whenTrue, CelNode whenFalse) {
      super(Arrays.asList(condition, whenTrue, whenFalse));
      this.condition = condition;
      this.whenTrue = whenTrue;
      this.whenFalse = whenFalse;
    }

    @Override
    int precedence() {
      return TERNARY;
    }

    @Override
    void print(StringBuilder sb) {
      printOperand(sb, condition, OR);
      sb.append(" ? ");
      printOperand(sb, whenTrue, OR);
      sb.append(" : ");
      printOperand(sb, whenFalse, OR);
    }

    @Override
    long measure() {
      return measureOperand(condition, OR) + 3 + measureOperand(whenTrue, OR) + 3
          + measureOperand(whenFalse, OR);
    }

    @Override
    boolean sameAs(CelNode other) {
      Ternary o = (Ternary) other;
      return condition.equals(o.condition) && whenTrue.equals(o.whenTrue)
          && whenFalse.equals(o.whenFalse);
    }

    @Override
    int computeHash() {
      return Objects.hash(condition, whenTrue, whenFalse, 2);
    }

    @Override
    List<CelNode> children() {
      return Arrays.asList(condition, whenTrue, whenFalse);
    }
  }

  static final class ListLit extends CelNode {
    final List<CelNode> elements;

    ListLit(List<CelNode> elements) {
      super(elements);
      this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    @Override
    int precedence() {
      return MEMBER;
    }

    @Override
    void print(StringBuilder sb) {
      sb.append('[');
      printList(sb, elements);
      sb.append(']');
    }

    @Override
    long measure() {
      return 2 + measureList(elements);
    }

    @Override
    boolean sameAs(CelNode other) {
      return elements.equals(((ListLit) other).elements);
    }

    @Override
    int computeHash() {
      return Objects.hash(elements, 3);
    }

    @Override
    List<CelNode> children() {
      return elements;
    }
  }
}
