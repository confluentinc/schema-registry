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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Shortens a CHECK's CEL by the rules of logic, keeping every verdict, errors included.
 *
 * <p>CEL's {@code &&} and {@code ||} let a deciding operand win over another's error, so with an
 * error read as unknown they follow three-valued logic. A node is safe when it can't raise:
 * {@code has()} of a field, a NULL test of a variable or of a field known set, and {@code !} or a
 * junction of those. Walking a junction left to right, each safe operand passed is a fact for
 * those after it, and a ternary's condition is one in its branches; a node equal to a fact is
 * that constant. Operands are never reordered, so a NULL test still comes before what it guards.
 * An equality of what can be read is safe too, as CEL's {@code ==} holds between any two values.
 *
 * <p>A node that always evaluates a ternary's condition is split on it, {@code c ? node : node}
 * with {@code c} a fact in each, when that is shorter: what the condition raises, the node raises
 * too. Constants are worked out, and operands sharing a prefix factored.
 *
 * <p>Compared, as an operand of anything but a truth, cel-java gives an error out of {@code &&}
 * or {@code ||} as unequal instead of raising it. There a ternary isn't made a junction, nor a
 * junction a single operand or split, unless what would raise can't.
 *
 * <p>Every rule also keeps the verdict (true, false or raising, though not always the same
 * error) of a runtime that takes {@code &&} and {@code ||} left to right and raises the first
 * error: what raised before a deciding operand still raises, and an operand goes only for what
 * comes before it, unless it can't raise, or, but for raising, it is the identity and raises only
 * where the next one does.
 */
final class CelSimplifier {

  // Macros whose first argument is a variable bound in the others.
  private static final Set<String> MACROS =
      Set.of("all", "exists", "exists_one", "map", "filter");
  // Those whose body is a truth.
  private static final Set<String> TRUTH_MACROS = Set.of("all", "exists", "exists_one");
  private static final CelNode THIS = CelNode.ident("this");
  private static final Set<String> ARITHMETIC = Set.of("+", "-", "*");
  // Splits simplify their node again, each charged its code points times one more than the depth
  // of the facts, as looking a fact up walks their levels: in all, up to this many times the
  // CHECK's code points, and at most MAX_SPLIT_WORK.
  private static final int SPLIT_WORK = 64;
  private static final long MAX_SPLIT_WORK = 250_000;
  private static final Pattern DOUBLE =
      Pattern.compile("(?:\\d+\\.\\d+|\\.\\d+)(?:[eE][+-]?\\d+)?|\\d+[eE][+-]?\\d+");

  private CelSimplifier() {
  }

  /**
   * {@code check} simplified, or as it is if that wouldn't be shorter.
   */
  static CelNode simplify(CelNode check) {
    Facts root = Facts.root(Math.min(SPLIT_WORK * check.size(), MAX_SPLIT_WORK));
    CelNode simplified = simplify(check, root, false);
    return simplified.size() <= check.size() ? simplified : check;
  }

  private static CelNode simplify(CelNode node, Facts facts, boolean compared) {
    CelNode known = facts.get(node);
    if (known != null) {
      return known;
    }
    CelNode out;
    if (node instanceof CelNode.Junction) {
      out = junction((CelNode.Junction) node, facts, compared);
    } else if (node instanceof CelNode.Ternary) {
      out = ternary((CelNode.Ternary) node, facts, compared);
    } else if (node instanceof CelNode.Unary) {
      CelNode.Unary unary = (CelNode.Unary) node;
      if (unary.op.equals("!")) {
        CelNode operand = simplify(unary.operand, facts, false);
        out = negated(operand, compared && !safe(operand, facts));
      } else {
        out = CelNode.negate(simplify(unary.operand, facts, true));
      }
    } else if (node instanceof CelNode.Binary) {
      CelNode.Binary binary = (CelNode.Binary) node;
      CelNode left = simplify(binary.left, facts, true);
      CelNode right = simplify(binary.right, facts, true);
      CelNode folded = folded(left, binary.op, right);
      out = folded != null ? folded : CelNode.binary(left, binary.op, right);
    } else if (node instanceof CelNode.Call) {
      out = call((CelNode.Call) node, facts);
    } else if (node instanceof CelNode.Select) {
      CelNode.Select select = (CelNode.Select) node;
      out = CelNode.select(simplify(select.operand, facts, true), select.field);
    } else if (node instanceof CelNode.Index) {
      CelNode.Index index = (CelNode.Index) node;
      out = CelNode.index(simplify(index.operand, facts, true),
          simplify(index.index, facts, true));
    } else if (node instanceof CelNode.ListLit) {
      out = CelNode.list(simplifyAll(((CelNode.ListLit) node).elements, facts, true));
    } else {
      return node;
    }
    if (!(out instanceof CelNode.Junction) && !out.children().isEmpty()) {
      CelNode split = split(out, facts, compared);
      if (split != null) {
        out = split;
      }
    }
    known = facts.get(out);
    return known != null ? known : out;
  }

  private static List<CelNode> simplifyAll(List<CelNode> nodes, Facts facts, boolean compared) {
    List<CelNode> out = new ArrayList<>(nodes.size());
    for (CelNode node : nodes) {
      out.add(simplify(node, facts, compared));
    }
    return out;
  }

  private static CelNode call(CelNode.Call call, Facts facts) {
    if (call.target == null && call.function.equals("has")) {
      // Its argument names a field, which isn't read; a subscript's constant is still worked out.
      return call.args.size() == 1 ? CelNode.call("has", subscriptsFolded(call.args.get(0))) : call;
    }
    if (call.target == null) {
      List<CelNode> args = simplifyAll(call.args, facts, true);
      Long value = call.function.equals("double") && args.size() == 1 ? longValue(args.get(0))
          : null;
      return value != null ? CelNode.number(value + ".0") : CelNode.call(call.function, args);
    }
    CelNode target = simplify(call.target, facts, true);
    if (!MACROS.contains(call.function) || call.args.size() < 2
        || !(call.args.get(0) instanceof CelNode.Ident)) {
      return CelNode.method(target, call.function, simplifyAll(call.args, facts, true));
    }
    // Facts about the variable are an outer one's, not the one bound here.
    CelNode variable = call.args.get(0);
    List<CelNode> args = new ArrayList<>();
    args.add(variable);
    args.addAll(simplifyAll(call.args.subList(1, call.args.size()), facts.without(variable),
        !TRUTH_MACROS.contains(call.function)));
    return CelNode.method(target, call.function, args);
  }

  private static CelNode junction(CelNode.Junction junction, Facts facts, boolean compared) {
    boolean and = junction.and;
    CelNode identity = CelNode.bool(and);
    CelNode decider = CelNode.bool(!and);
    List<CelNode> out = new ArrayList<>();
    boolean allSafe = true;
    Facts known = facts;
    operands:
    for (CelNode operand : junction.operands) {
      CelNode simplified = simplify(operand, known, false);
      List<CelNode> parts = simplified instanceof CelNode.Junction
          && ((CelNode.Junction) simplified).and == and
          ? ((CelNode.Junction) simplified).operands : Collections.singletonList(simplified);
      for (CelNode part : parts) {
        CelNode value = known.get(part);
        if (value != null) {
          part = value;
        }
        if (part.equals(identity) || out.contains(part)) {
          continue;
        }
        if (part.equals(decider)) {
          // Whatever raises before it still does where operands are taken left to right.
          if (allSafe) {
            return decider;
          }
          out.add(part);
          break operands;
        }
        out.add(part);
        if (safe(part, known)) {
          known = known.with(part, and);
        } else {
          allSafe = false;
        }
      }
    }
    if (out.isEmpty()) {
      return identity;
    }
    out = absorbed(out, and);
    if (out.size() > 1) {
      out = splitRuns(out, and, facts, compared);
    }
    if (out.size() == 1 && compared && !(out.get(0) instanceof CelNode.Junction)
        && !safe(out.get(0), facts)) {
      return junction;
    }
    return factor(out, and);
  }

  // a || (a && b) is a, as is a && (a || b): a junction of the other kind goes when its first
  // operand comes before it, or each operand of that first one if it's of this kind; that
  // decides the junction before the rest of it is taken. So does c ? false : false in an ||, or
  // c ? true : true in an &&, which only raises, where an operand before it or the next one
  // raises when c does: left to right, what raised still does, if not always first.
  private static List<CelNode> absorbed(List<CelNode> operands, boolean and) {
    List<CelNode> out = new ArrayList<>(operands);
    for (int i = out.size() - 1; i >= 0; i--) {
      if (absorbed(out, i, and)) {
        out.remove(i);
      }
    }
    return out;
  }

  // True if operands' i-th is absorbed by those before it, or a raise-only one also by the next.
  private static boolean absorbed(List<CelNode> operands, int i, boolean and) {
    CelNode operand = operands.get(i);
    if (operand instanceof CelNode.Ternary) {
      CelNode.Ternary ternary = (CelNode.Ternary) operand;
      CelNode identity = CelNode.bool(and);
      if (!ternary.whenTrue.equals(identity) || !ternary.whenFalse.equals(identity)) {
        return false;
      }
      for (int j = 0; j <= i + 1 && j < operands.size(); j++) {
        CelNode other = operands.get(j);
        if (j != i && (other.equals(ternary.condition)
            || strictlyEvaluates(other, ternary.condition))) {
          return true;
        }
      }
      return false;
    }
    if (!(operand instanceof CelNode.Junction) || ((CelNode.Junction) operand).and == and) {
      return false;
    }
    CelNode first = ((CelNode.Junction) operand).operands.get(0);
    return operands.subList(0, i).containsAll(parts(first, and));
  }

  // Adjacent operands of an && that start with the same operands S of an || are
  // S || (rest && rest ...), and the other way round for an ||.
  private static CelNode factor(List<CelNode> operands, boolean and) {
    List<CelNode> out = new ArrayList<>();
    int i = 0;
    while (i < operands.size()) {
      List<CelNode> first = parts(operands.get(i), !and);
      int j = i + 1;
      int k = j < operands.size() ? commonPrefix(first, parts(operands.get(j), !and)) : 0;
      if (k == 0) {
        out.add(operands.get(i++));
        continue;
      }
      List<CelNode> prefix = first.subList(0, k);
      List<CelNode> rests = new ArrayList<>();
      rests.add(CelNode.Junction.of(!and, first.subList(k, first.size())));
      for (; j < operands.size(); j++) {
        List<CelNode> next = parts(operands.get(j), !and);
        if (next.size() <= k || !next.subList(0, k).equals(prefix)) {
          break;
        }
        rests.add(CelNode.Junction.of(!and, next.subList(k, next.size())));
      }
      List<CelNode> joined = new ArrayList<>(prefix);
      joined.add(CelNode.Junction.of(and, rests));
      out.add(CelNode.Junction.of(!and, joined));
      i = j;
    }
    return CelNode.Junction.of(and, out);
  }

  // The operands of node if it's an && (or an ||), else node.
  private static List<CelNode> parts(CelNode node, boolean and) {
    return node instanceof CelNode.Junction && ((CelNode.Junction) node).and == and
        ? ((CelNode.Junction) node).operands : Collections.singletonList(node);
  }

  // The operands a and b start with, leaving each at least one.
  private static int commonPrefix(List<CelNode> a, List<CelNode> b) {
    int n = 0;
    while (n < a.size() - 1 && n < b.size() - 1 && a.get(n).equals(b.get(n))) {
      n++;
    }
    return n;
  }

  private static CelNode ternary(CelNode.Ternary ternary, Facts facts, boolean compared) {
    CelNode condition = simplify(ternary.condition, facts, false);
    if (condition.equals(CelNode.TRUE)) {
      return simplify(ternary.whenTrue, facts, compared);
    }
    if (condition.equals(CelNode.FALSE)) {
      return simplify(ternary.whenFalse, facts, compared);
    }
    // The condition raises before either branch is taken, so its value is a fact in each.
    CelNode whenTrue = simplify(ternary.whenTrue, facts.with(condition, true), compared);
    CelNode whenFalse = simplify(ternary.whenFalse, facts.with(condition, false), compared);
    return joined(condition, whenTrue, whenFalse, facts, compared);
  }

  // condition ? whenTrue : whenFalse, its branches simplified.
  private static CelNode joined(CelNode condition, CelNode whenTrue, CelNode whenFalse,
      Facts facts, boolean compared) {
    // A condition shared() merged may already be known.
    CelNode known = facts.get(condition);
    if (known != null) {
      return known.equals(CelNode.TRUE) ? whenTrue : whenFalse;
    }
    boolean safe = safe(condition, facts);
    if (safe && whenTrue.equals(whenFalse)) {
      return whenTrue;
    }
    if (whenTrue.equals(CelNode.FALSE) && whenFalse.equals(CelNode.TRUE)) {
      // ! raises the condition's error as the ternary does.
      return negated(condition, compared && !safe);
    }
    if (whenTrue.equals(CelNode.TRUE) && whenFalse.equals(CelNode.FALSE) && (!compared || safe)) {
      return condition;
    }
    if (safe && !compared) {
      if (whenTrue.equals(CelNode.TRUE)) {
        return CelNode.or(condition, whenFalse);
      }
      if (whenTrue.equals(CelNode.FALSE)) {
        return CelNode.and(negated(condition, false), whenFalse);
      }
      if (whenFalse.equals(CelNode.FALSE)) {
        return CelNode.and(condition, whenTrue);
      }
      if (whenFalse.equals(CelNode.TRUE)) {
        return CelNode.or(negated(condition, false), whenTrue);
      }
    }
    if (safe) {
      CelNode shared = shared(condition, whenTrue, whenFalse, facts, compared);
      if (shared != null) {
        return shared;
      }
    }
    return CelNode.ternary(condition, whenTrue, whenFalse);
  }

  // c ? x : (d ? x : y) as c || d ? x : y and so on, where c can't raise: each branch was
  // simplified where it's taken.
  private static CelNode shared(CelNode condition, CelNode whenTrue, CelNode whenFalse,
      Facts facts, boolean compared) {
    if (whenFalse instanceof CelNode.Ternary) {
      CelNode.Ternary inner = (CelNode.Ternary) whenFalse;
      if (inner.whenTrue.equals(whenTrue)) {
        return joined(CelNode.or(condition, inner.condition), whenTrue, inner.whenFalse, facts,
            compared);
      }
      if (inner.whenFalse.equals(whenTrue)) {
        return joined(CelNode.or(condition, negated(inner.condition, false)), whenTrue,
            inner.whenTrue, facts, compared);
      }
    }
    if (whenTrue instanceof CelNode.Ternary) {
      CelNode.Ternary inner = (CelNode.Ternary) whenTrue;
      if (inner.whenFalse.equals(whenFalse)) {
        return joined(CelNode.and(condition, inner.condition), inner.whenTrue, whenFalse, facts,
            compared);
      }
      if (inner.whenTrue.equals(whenFalse)) {
        return joined(CelNode.and(condition, negated(inner.condition, false)), inner.whenFalse,
            whenFalse, facts, compared);
      }
    }
    return null;
  }

  // The conditions of the ternaries node always evaluates, each raising what it raises: a
  // junction and a ternary's branches aren't always evaluated, nor a macro's body; has() is
  // left out too.
  private static void strictConditions(CelNode node, List<CelNode> out) {
    if (node instanceof CelNode.Ternary) {
      out.add(((CelNode.Ternary) node).condition);
      strictConditions(((CelNode.Ternary) node).condition, out);
      return;
    }
    if (node instanceof CelNode.Junction) {
      return;
    }
    if (node instanceof CelNode.Call) {
      CelNode.Call call = (CelNode.Call) node;
      if (call.target == null && call.function.equals("has")) {
        return;
      }
      if (call.target != null) {
        strictConditions(call.target, out);
        if (MACROS.contains(call.function) && call.args.size() >= 2
            && call.args.get(0) instanceof CelNode.Ident) {
          return;
        }
      }
      for (CelNode arg : call.args) {
        strictConditions(arg, out);
      }
      return;
    }
    for (CelNode child : node.children()) {
      strictConditions(child, out);
    }
  }

  private static CelNode split(CelNode node, Facts facts, boolean compared) {
    List<CelNode> conditions = new ArrayList<>();
    strictConditions(node instanceof CelNode.Ternary ? ((CelNode.Ternary) node).condition : node,
        conditions);
    for (CelNode condition : conditions) {
      if (facts.get(condition) == null) {
        return splitOn(node, condition, facts, compared);
      }
    }
    return null;
  }

  // condition ? node : node, each simplified with condition a fact, if shorter.
  private static CelNode splitOn(CelNode node, CelNode condition, Facts facts,
      boolean compared) {
    long cost = node.size() * (facts.depth + 1);
    if (!facts.spend(cost)) {
      return null;
    }
    CelNode whenTrue = simplify(node, facts.with(condition, true), compared);
    // Not shorter however its other side comes out, else its other side is charged too.
    if (whenTrue.size() >= node.size() || !facts.spend(cost)) {
      return null;
    }
    CelNode whenFalse = simplify(node, facts.with(condition, false), compared);
    CelNode out = joined(condition, whenTrue, whenFalse, facts, compared);
    return out.size() < node.size() ? out : null;
  }

  // Adjacent operands that always evaluate the same condition, split on it. A junction's error
  // is told apart from the one the condition raises only where it's compared.
  private static List<CelNode> splitRuns(List<CelNode> operands, boolean and, Facts facts,
      boolean compared) {
    List<List<CelNode>> strict = new ArrayList<>(operands.size());
    for (CelNode operand : operands) {
      List<CelNode> conditions = new ArrayList<>();
      strictConditions(operand, conditions);
      strict.add(conditions);
    }
    List<CelNode> out = new ArrayList<>();
    Facts known = facts;
    int i = 0;
    while (i < operands.size()) {
      CelNode split = null;
      int end = i + 1;
      for (CelNode condition : strict.get(i)) {
        if (known.get(condition) != null) {
          continue;
        }
        int j = i + 1;
        while (j < operands.size() && strict.get(j).contains(condition)) {
          j++;
        }
        boolean whole = i == 0 && j == operands.size();
        if (j - i < 2 || whole && compared && !safe(condition, known)) {
          continue;
        }
        split = splitOn(CelNode.Junction.of(and, operands.subList(i, j)), condition, known,
            whole && compared);
        if (split != null) {
          end = j;
          break;
        }
      }
      for (int k = i; k < end; k++) {
        if (safe(operands.get(k), known)) {
          known = known.with(operands.get(k), and);
        }
      }
      out.add(split != null ? split : operands.get(i));
      i = end;
    }
    return out;
  }

  private static boolean strictlyEvaluates(CelNode node, CelNode condition) {
    List<CelNode> conditions = new ArrayList<>();
    strictConditions(node, conditions);
    return conditions.contains(condition);
  }

  // left op right worked out where both are constants, else null; arithmetic that would
  // overflow is left to raise.
  private static CelNode folded(CelNode left, String op, CelNode right) {
    Long a = longValue(left);
    Long b = longValue(right);
    if (a != null && b != null && ARITHMETIC.contains(op)) {
      try {
        long value = op.equals("+") ? Math.addExact(a, b)
            : op.equals("-") ? Math.subtractExact(a, b) : Math.multiplyExact(a, b);
        return value == Long.MIN_VALUE ? null : CelNode.integer(value);
      } catch (ArithmeticException e) {
        return null;
      }
    }
    CelNode l = undyn(left);
    CelNode r = undyn(right);
    a = longValue(l);
    b = longValue(r);
    if (a != null && b != null) {
      return compared(Long.compare(a, b), op);
    }
    Double x = doubleValue(l);
    Double y = doubleValue(r);
    if (x != null && y != null) {
      // By value, as 1.0 and 1e0 are one double; no literal is NaN.
      return compared(x < y ? -1 : x > y ? 1 : 0, op);
    }
    return equalConstants(l, op, r);
  }

  // A comparison whose operands compared as order.
  private static CelNode compared(int order, String op) {
    switch (op) {
      case "==": return CelNode.bool(order == 0);
      case "!=": return CelNode.bool(order != 0);
      case "<": return CelNode.bool(order < 0);
      case "<=": return CelNode.bool(order <= 0);
      case ">": return CelNode.bool(order > 0);
      case ">=": return CelNode.bool(order >= 0);
      default: return null;
    }
  }

  // l == r or l != r of null, a bool or a plain string, where they're one kind or one is null;
  // an int and a double are left to CEL's numeric equality.
  private static CelNode equalConstants(CelNode l, String op, CelNode r) {
    if (!op.equals("==") && !op.equals("!=")) {
      return null;
    }
    int kind = kind(l);
    int other = kind(r);
    boolean comparable = kind == other ? kind != 3 : kind == 0 || other == 0;
    if (kind < 0 || other < 0 || !comparable) {
      return null;
    }
    boolean equal = kind == 2 ? text(l).equals(text(r)) : l.equals(r);
    return CelNode.bool(equal == op.equals("=="));
  }

  // 0 for null, 1 a bool, 2 a plain string, 3 an int or a double, else -1.
  private static int kind(CelNode node) {
    if (node.equals(CelNode.NULL)) {
      return 0;
    }
    if (node.equals(CelNode.TRUE) || node.equals(CelNode.FALSE)) {
      return 1;
    }
    if (text(node) != null) {
      return 2;
    }
    return longValue(node) != null || doubleValue(node) != null ? 3 : -1;
  }

  // The text of a quoted string literal without escapes, else null.
  private static String text(CelNode node) {
    if (!(node instanceof CelNode.Literal)) {
      return null;
    }
    String token = node.toString();
    char quote = token.charAt(0);
    boolean quoted = quote == '\'' || quote == '"';
    boolean plain = quoted && token.length() >= 2
        && token.indexOf(quote, 1) == token.length() - 1 && token.indexOf('\\') < 0;
    return plain ? token.substring(1, token.length() - 1) : null;
  }

  // An int literal's value, its sign included, else null.
  private static Long longValue(CelNode node) {
    boolean negative = node instanceof CelNode.Unary && ((CelNode.Unary) node).op.equals("-");
    CelNode operand = negative ? ((CelNode.Unary) node).operand : node;
    if (!(operand instanceof CelNode.Literal) || !((CelNode.Literal) operand).isNatural()) {
      return null;
    }
    try {
      return Long.parseLong((negative ? "-" : "") + operand);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  // A double literal's value, its sign included, else null.
  private static Double doubleValue(CelNode node) {
    boolean negative = node instanceof CelNode.Unary && ((CelNode.Unary) node).op.equals("-");
    CelNode operand = negative ? ((CelNode.Unary) node).operand : node;
    if (!(operand instanceof CelNode.Literal) || !DOUBLE.matcher(operand.toString()).matches()) {
      return null;
    }
    double value = Double.parseDouble(operand.toString());
    return negative ? -value : value;
  }

  private static CelNode undyn(CelNode node) {
    while (node instanceof CelNode.Call && ((CelNode.Call) node).function.equals("dyn")
        && ((CelNode.Call) node).target == null && ((CelNode.Call) node).args.size() == 1) {
      node = ((CelNode.Call) node).args.get(0);
    }
    return node;
  }

  // A field selection with each subscript's constant worked out, a map value's dyn() included.
  private static CelNode subscriptsFolded(CelNode node) {
    if (node instanceof CelNode.Select) {
      CelNode.Select select = (CelNode.Select) node;
      return CelNode.select(subscriptsFolded(select.operand), select.field);
    }
    if (node instanceof CelNode.Index) {
      CelNode.Index index = (CelNode.Index) node;
      return CelNode.index(subscriptsFolded(index.operand), constantFolded(index.index));
    }
    if (CelNode.isDyn(node)) {
      return CelNode.dyn(subscriptsFolded(((CelNode.Call) node).args.get(0)));
    }
    return node;
  }

  private static CelNode constantFolded(CelNode node) {
    if (!(node instanceof CelNode.Binary)) {
      return node;
    }
    CelNode.Binary binary = (CelNode.Binary) node;
    CelNode left = constantFolded(binary.left);
    CelNode right = constantFolded(binary.right);
    CelNode folded = folded(left, binary.op, right);
    return folded != null ? folded : CelNode.binary(left, binary.op, right);
  }

  /**
   * {@code !node}: by De Morgan's laws where that's no longer, unless {@code plain}, as where a
   * junction's error would then be told apart from the one ! raises.
   */
  static CelNode negated(CelNode node, boolean plain) {
    if (node.equals(CelNode.TRUE) || node.equals(CelNode.FALSE)) {
      return CelNode.bool(node.equals(CelNode.FALSE));
    }
    CelNode flipped = flipped(node);
    if (flipped != null) {
      return flipped;
    }
    if (plain) {
      return CelNode.not(node);
    }
    if (node instanceof CelNode.Unary && ((CelNode.Unary) node).op.equals("!")) {
      return ((CelNode.Unary) node).operand;
    }
    if (node instanceof CelNode.Junction) {
      CelNode pushed = pushed((CelNode.Junction) node);
      CelNode kept = CelNode.not(node);
      return pushed.size() <= kept.size() ? pushed : kept;
    }
    return CelNode.not(node);
  }

  private static CelNode pushed(CelNode.Junction junction) {
    List<CelNode> operands = new ArrayList<>(junction.operands.size());
    for (CelNode operand : junction.operands) {
      operands.add(negated(operand, false));
    }
    return factor(operands, !junction.and);
  }

  // a != b for a == b and the other way round, else null.
  private static CelNode flipped(CelNode node) {
    if (!(node instanceof CelNode.Binary)) {
      return null;
    }
    CelNode.Binary binary = (CelNode.Binary) node;
    return binary.op.equals("==") ? CelNode.binary(binary.left, "!=", binary.right)
        : binary.op.equals("!=") ? CelNode.binary(binary.left, "==", binary.right) : null;
  }

  /**
   * True if {@code node} can't raise, given {@code facts}.
   */
  static boolean safe(CelNode node, Facts facts) {
    if (node.equals(CelNode.TRUE) || node.equals(CelNode.FALSE)) {
      return true;
    }
    if (node instanceof CelNode.Unary) {
      return ((CelNode.Unary) node).op.equals("!") && safe(((CelNode.Unary) node).operand, facts);
    }
    if (node instanceof CelNode.Junction) {
      CelNode.Junction junction = (CelNode.Junction) node;
      Facts known = facts;
      for (CelNode operand : junction.operands) {
        if (!safe(operand, known)) {
          return false;
        }
        known = known.with(operand, junction.and);
      }
      return true;
    }
    if (node instanceof CelNode.Binary && equality((CelNode.Binary) node)) {
      // CEL's == holds between any two values.
      return value(((CelNode.Binary) node).left, facts)
          && value(((CelNode.Binary) node).right, facts);
    }
    CelNode field = hasField(node);
    return field != null && readable(((CelNode.Select) field).operand, facts);
  }

  // ==, != or in a list.
  private static boolean equality(CelNode.Binary binary) {
    return binary.op.equals("==") || binary.op.equals("!=")
        || binary.op.equals("in") && binary.right instanceof CelNode.ListLit;
  }

  // True if node can be read without raising: a literal, a negative number, a variable, a field
  // known set of what can be read, or dyn() or a list of those.
  private static boolean value(CelNode node, Facts facts) {
    if (node instanceof CelNode.Literal || node instanceof CelNode.Ident) {
      return true;
    }
    if (node instanceof CelNode.Unary) {
      return longValue(node) != null || doubleValue(node) != null;
    }
    if (node instanceof CelNode.Select) {
      return readable(((CelNode.Select) node).operand, facts)
          && CelNode.TRUE.equals(facts.get(CelNode.call("has", node)));
    }
    if (node instanceof CelNode.Call) {
      return undyn(node) != node && value(undyn(node), facts);
    }
    if (node instanceof CelNode.ListLit) {
      for (CelNode element : ((CelNode.ListLit) node).elements) {
        if (!value(element, facts)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  // f in has(f) of a field, else null.
  private static CelNode hasField(CelNode node) {
    if (!(node instanceof CelNode.Call)) {
      return null;
    }
    CelNode.Call call = (CelNode.Call) node;
    boolean has = call.target == null && call.function.equals("has") && call.args.size() == 1;
    return has && call.args.get(0) instanceof CelNode.Select ? call.args.get(0) : null;
  }

  // True if a field of node can be read without raising: this, or a variable or field known not
  // NULL (and a field known set).
  private static boolean readable(CelNode node, Facts facts) {
    if (node instanceof CelNode.Ident) {
      return node.equals(THIS) || CelNode.TRUE.equals(facts.get(CelNode.isNotNull(node)));
    }
    return node instanceof CelNode.Select
        && readable(((CelNode.Select) node).operand, facts)
        && CelNode.TRUE.equals(facts.get(CelNode.call("has", node)))
        && CelNode.TRUE.equals(facts.get(CelNode.isNotNull(node)));
  }

  /**
   * What is known true or false, each fact with what it decides.
   */
  static final class Facts {
    private final Facts parent;
    private final Map<CelNode, Boolean> known = new HashMap<>();
    private final int depth;
    // The work left for splits, shared by one simplification.
    private final long[] work;

    private Facts(Facts parent, long[] work) {
      this.parent = parent;
      this.work = work;
      this.depth = parent == null ? 0 : parent.depth + 1;
    }

    static Facts root(long work) {
      return new Facts(null, new long[] {work});
    }

    // True, using it, if cost is left.
    boolean spend(long cost) {
      if (work[0] < cost) {
        return false;
      }
      work[0] -= cost;
      return true;
    }

    // The constant node is known to be, or null if it isn't known.
    CelNode get(CelNode node) {
      if (node.equals(CelNode.TRUE) || node.equals(CelNode.FALSE)) {
        return node;
      }
      for (Facts facts = this; facts != null; facts = facts.parent) {
        Boolean value = facts.known.get(node);
        if (value != null) {
          return CelNode.bool(value);
        }
      }
      return null;
    }

    Facts with(CelNode node, boolean value) {
      Facts out = new Facts(this, work);
      out.add(node, value);
      return out;
    }

    // These facts but those that mention variable.
    Facts without(CelNode variable) {
      Facts out = new Facts(null, work);
      for (Facts facts = this; facts != null; facts = facts.parent) {
        for (Map.Entry<CelNode, Boolean> fact : facts.known.entrySet()) {
          if (!fact.getKey().contains(variable)) {
            out.known.putIfAbsent(fact.getKey(), fact.getValue());
          }
        }
      }
      return out;
    }

    private void add(CelNode node, boolean value) {
      if (node.equals(CelNode.TRUE) || node.equals(CelNode.FALSE)
          || known.putIfAbsent(node, value) != null) {
        return;
      }
      if (node instanceof CelNode.Unary && ((CelNode.Unary) node).op.equals("!")) {
        add(((CelNode.Unary) node).operand, !value);
      } else {
        known.putIfAbsent(CelNode.not(node), !value);
      }
      CelNode flipped = flipped(node);
      if (flipped != null) {
        known.putIfAbsent(flipped, !value);
      }
      if (node instanceof CelNode.Junction) {
        CelNode.Junction junction = (CelNode.Junction) node;
        // A true && has each operand true, a false || each false.
        if (junction.and == value) {
          for (CelNode operand : junction.operands) {
            add(operand, value);
          }
        }
        known.putIfAbsent(pushed(junction), !value);
      }
      if (node instanceof CelNode.Ternary) {
        // c ? x : y is decided by c when a branch is the other constant.
        CelNode.Ternary ternary = (CelNode.Ternary) node;
        if (ternary.whenTrue.equals(CelNode.bool(!value))) {
          add(ternary.condition, false);
          add(ternary.whenFalse, value);
        } else if (ternary.whenFalse.equals(CelNode.bool(!value))) {
          add(ternary.condition, true);
          add(ternary.whenTrue, value);
        }
      }
    }
  }
}
