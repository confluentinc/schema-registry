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

/**
 * Shortens a CHECK's CEL by the rules of logic, keeping every verdict, errors included.
 *
 * <p>CEL's {@code &&} and {@code ||} let a deciding operand win over another's error, so with an
 * error read as unknown they follow three-valued logic. A node is safe when it can't raise:
 * {@code has()} of a field, a NULL test of a variable or of a field known set, and {@code !} or a
 * junction of those. Walking a junction left to right, each safe operand passed is a fact for
 * those after it, and a ternary's condition is one in its branches; a node equal to a fact is
 * that constant. Operands are never reordered, so a NULL test still comes before what it guards.
 *
 * <p>Compared, as an operand of anything but a truth, cel-java gives an error out of {@code &&}
 * or {@code ||} as unequal instead of raising it. There a ternary isn't made a junction, nor a
 * junction a single operand, unless what would raise can't.
 */
final class CelSimplifier {

  // Macros whose first argument is a variable bound in the others.
  private static final Set<String> MACROS =
      Set.of("all", "exists", "exists_one", "map", "filter");
  // Those whose body is a truth.
  private static final Set<String> TRUTH_MACROS = Set.of("all", "exists", "exists_one");
  private static final CelNode THIS = CelNode.ident("this");

  private CelSimplifier() {
  }

  /**
   * {@code check} simplified, or as it is if that wouldn't be shorter.
   */
  static CelNode simplify(CelNode check) {
    CelNode simplified = simplify(check, Facts.NONE, false);
    return simplified.size() <= check.size() ? simplified : check;
  }

  private static CelNode simplify(CelNode node, Facts facts, boolean compared) {
    Boolean known = facts.get(node);
    if (known != null) {
      return CelNode.bool(known);
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
      out = CelNode.binary(simplify(binary.left, facts, true), binary.op,
          simplify(binary.right, facts, true));
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
    known = facts.get(out);
    return known != null ? CelNode.bool(known) : out;
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
      // Its argument names a field; it isn't read.
      return call;
    }
    if (call.target == null) {
      return CelNode.call(call.function, simplifyAll(call.args, facts, true));
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
        Boolean value = known.get(part);
        if (value != null) {
          part = CelNode.bool(value);
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
    if (out.size() == 1 && compared && !(out.get(0) instanceof CelNode.Junction)
        && !safe(out.get(0), facts)) {
      return junction;
    }
    return and ? factor(out) : CelNode.or(out);
  }

  // Adjacent || operands that start with the same operands S are S || (rest && rest ...).
  private static CelNode factor(List<CelNode> operands) {
    List<CelNode> out = new ArrayList<>();
    int i = 0;
    while (i < operands.size()) {
      List<CelNode> first = disjuncts(operands.get(i));
      int j = i + 1;
      int k = j < operands.size() ? commonPrefix(first, disjuncts(operands.get(j))) : 0;
      if (k == 0) {
        out.add(operands.get(i++));
        continue;
      }
      List<CelNode> prefix = first.subList(0, k);
      List<CelNode> rests = new ArrayList<>();
      rests.add(CelNode.or(first.subList(k, first.size())));
      for (; j < operands.size(); j++) {
        List<CelNode> next = disjuncts(operands.get(j));
        if (next.size() <= k || !next.subList(0, k).equals(prefix)) {
          break;
        }
        rests.add(CelNode.or(next.subList(k, next.size())));
      }
      List<CelNode> joined = new ArrayList<>(prefix);
      joined.add(CelNode.and(rests));
      out.add(CelNode.or(joined));
      i = j;
    }
    return CelNode.and(out);
  }

  private static List<CelNode> disjuncts(CelNode node) {
    return node instanceof CelNode.Junction && !((CelNode.Junction) node).and
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
    return CelNode.ternary(condition, whenTrue, whenFalse);
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
    return junction.and ? CelNode.or(operands) : factor(operands);
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
    CelNode field = hasField(node);
    if (field != null) {
      return readable(((CelNode.Select) field).operand, facts);
    }
    CelNode tested = nullTested(node);
    if (tested instanceof CelNode.Ident) {
      return true;
    }
    return tested instanceof CelNode.Select
        && readable(((CelNode.Select) tested).operand, facts)
        && Boolean.TRUE.equals(facts.get(CelNode.call("has", tested)));
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

  // x in dyn(x) == null or dyn(x) != null, else null.
  private static CelNode nullTested(CelNode node) {
    CelNode tested = CelNode.isNullOf(node);
    if (tested == null) {
      CelNode flipped = flipped(node);
      tested = flipped == null ? null : CelNode.isNullOf(flipped);
    }
    return tested;
  }

  // True if a field of node can be read without raising: this, or a variable or field known not
  // NULL (and a field known set).
  private static boolean readable(CelNode node, Facts facts) {
    if (node instanceof CelNode.Ident) {
      return node.equals(THIS) || Boolean.TRUE.equals(facts.get(CelNode.isNotNull(node)));
    }
    return node instanceof CelNode.Select
        && readable(((CelNode.Select) node).operand, facts)
        && Boolean.TRUE.equals(facts.get(CelNode.call("has", node)))
        && Boolean.TRUE.equals(facts.get(CelNode.isNotNull(node)));
  }

  /**
   * What is known true or false, each fact with what it decides.
   */
  static final class Facts {
    static final Facts NONE = new Facts(null, Collections.emptyMap());

    private final Facts parent;
    private final Map<CelNode, Boolean> known;

    private Facts(Facts parent, Map<CelNode, Boolean> known) {
      this.parent = parent;
      this.known = known;
    }

    Boolean get(CelNode node) {
      for (Facts facts = this; facts != null; facts = facts.parent) {
        Boolean value = facts.known.get(node);
        if (value != null) {
          return value;
        }
      }
      return null;
    }

    Facts with(CelNode node, boolean value) {
      Map<CelNode, Boolean> added = new HashMap<>();
      add(added, node, value);
      return new Facts(this, added);
    }

    // These facts but those that mention variable.
    Facts without(CelNode variable) {
      Map<CelNode, Boolean> kept = new HashMap<>();
      for (Facts facts = this; facts != null; facts = facts.parent) {
        for (Map.Entry<CelNode, Boolean> fact : facts.known.entrySet()) {
          if (!fact.getKey().contains(variable)) {
            kept.putIfAbsent(fact.getKey(), fact.getValue());
          }
        }
      }
      return new Facts(null, kept);
    }

    private static void add(Map<CelNode, Boolean> facts, CelNode node, boolean value) {
      if (node.equals(CelNode.TRUE) || node.equals(CelNode.FALSE)
          || facts.putIfAbsent(node, value) != null) {
        return;
      }
      if (node instanceof CelNode.Unary && ((CelNode.Unary) node).op.equals("!")) {
        add(facts, ((CelNode.Unary) node).operand, !value);
      } else {
        facts.putIfAbsent(CelNode.not(node), !value);
      }
      CelNode flipped = flipped(node);
      if (flipped != null) {
        facts.putIfAbsent(flipped, !value);
      }
      if (node instanceof CelNode.Junction) {
        CelNode.Junction junction = (CelNode.Junction) node;
        // A true && has each operand true, a false || each false.
        if (junction.and == value) {
          for (CelNode operand : junction.operands) {
            add(facts, operand, value);
          }
        }
        facts.putIfAbsent(pushed(junction), !value);
      }
      if (node instanceof CelNode.Ternary) {
        // c ? x : y is decided by c when a branch is the other constant.
        CelNode.Ternary ternary = (CelNode.Ternary) node;
        if (ternary.whenTrue.equals(CelNode.bool(!value))) {
          add(facts, ternary.condition, false);
          add(facts, ternary.whenFalse, value);
        } else if (ternary.whenFalse.equals(CelNode.bool(!value))) {
          add(facts, ternary.condition, true);
          add(facts, ternary.whenTrue, value);
        }
      }
    }
  }
}
