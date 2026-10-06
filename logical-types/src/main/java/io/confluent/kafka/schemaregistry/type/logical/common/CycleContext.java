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

package io.confluent.kafka.schemaregistry.type.logical.common;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;

/** A context for keeping track of the current path context in a schema conversion process. */
public class CycleContext<T> {

  private final Set<T> seenSchemas = new HashSet<>();
  private final Deque<String> fieldsPath = new ArrayDeque<>();
  // The defaults and named-type uses recorded where conversion is: the root, or a named type's
  // body while it converts, relative to it from defaultsBase.
  private Frame frame = new Frame();
  private List<Integer> defaultsBase = List.of();
  // Each converted named type's own defaults and uses, relative to it.
  private final Map<String, Frame> typeFrames = new HashMap<>();

  public boolean addSeenSchema(T schema) {
    return seenSchemas.add(schema);
  }

  public void removeSeenSchema(T schema) {
    seenSchemas.remove(schema);
  }

  public void pushFieldPath(String fieldName) {
    fieldsPath.push(fieldName);
  }

  public void popFieldPath() {
    fieldsPath.pop();
  }

  /** Records a default value at the given field-index path. */
  public void putDefaultValue(List<Integer> path, Object value) {
    frame.defaults.put(relative(path), value);
  }

  /**
   * Converts a named type's body, first used at {@code path}, keeping its defaults relative to it:
   * the body is shared by reference, so each use places them with {@link #putTypeDefaults}.
   */
  public <R> R convertNamedType(String name, List<Integer> path, Supplier<R> conversion) {
    final Frame outer = frame;
    final List<Integer> outerBase = defaultsBase;
    final Frame own = new Frame();
    frame = own;
    defaultsBase = path;
    try {
      final R body = conversion.get();
      typeFrames.putIfAbsent(name, own);
      return body;
    } finally {
      frame = outer;
      defaultsBase = outerBase;
    }
  }

  /**
   * Records a use of a named type at {@code path}: its defaults land there, as inlining it would.
   * They are placed when the defaults are read, so its body need not be converted yet.
   */
  public void putTypeDefaults(String name, List<Integer> path) {
    frame.uses.add(new SimpleImmutableEntry<>(name, relative(path)));
  }

  /**
   * Path-keyed map of field-default values collected during conversion, each named type's placed
   * at its uses as inlining it would. Within a recursive cycle, each of its types is placed once
   * each time the cycle is entered, at its first use in field order, so never below itself.
   */
  public Map<List<Integer>, Object> getDefaultValues() {
    final Map<String, Integer> components = components();
    final Set<Integer> recursive = recursiveComponents(components);
    final Set<String> withDefaults = typesWithDefaults();
    final Map<List<Integer>, Object> out = new HashMap<>();
    // A work stack, not recursion: a chain of references can be longer than the call stack is
    // deep. Uses are pushed last first, so the first field's are placed first.
    final Deque<Placement> work = new ArrayDeque<>();
    work.push(new Placement(null, frame, null, Collections.emptyList(), null));
    while (!work.isEmpty()) {
      final Placement at = work.pop();
      if (at.entered != null && !at.entered.add(at.name)) {
        continue;
      }
      for (Map.Entry<List<Integer>, Object> entry : at.frame.defaults.entrySet()) {
        out.put(at.pathTo(entry.getKey()), entry.getValue());
      }
      final List<Map.Entry<String, List<Integer>>> uses = at.frame.uses;
      for (int i = uses.size() - 1; i >= 0; i--) {
        final String used = uses.get(i).getKey();
        if (!withDefaults.contains(used)) {
          continue;
        }
        // The types placed since entering this cycle; a use from outside it enters it anew.
        final Set<String> entered = sameCycle(components, at.name, used) ? at.entered
            : recursive.contains(components.get(used)) ? new HashSet<>() : null;
        work.push(new Placement(used, typeFrames.get(used), at, uses.get(i).getValue(), entered));
      }
    }
    return out;
  }

  // The components holding a cycle: some use stays inside them, a type's use of itself included.
  private Set<Integer> recursiveComponents(Map<String, Integer> components) {
    final Set<Integer> recursive = new HashSet<>();
    for (Map.Entry<String, Frame> type : typeFrames.entrySet()) {
      for (Map.Entry<String, List<Integer>> use : type.getValue().uses) {
        if (typeFrames.containsKey(use.getKey())
            && sameCycle(components, type.getKey(), use.getKey())) {
          recursive.add(components.get(type.getKey()));
        }
      }
    }
    return recursive;
  }

  // Whether a use stays inside a recursive type's cycle (a self-use included).
  private static boolean sameCycle(Map<String, Integer> components, String from, String to) {
    return from != null && components.get(from).equals(components.get(to));
  }

  /**
   * Each named type's strongly connected component of the use graph, by Tarjan's algorithm with
   * a work stack: types in one component reach each other, so they share a cycle.
   */
  private Map<String, Integer> components() {
    final Map<String, Integer> index = new HashMap<>();
    final Map<String, Integer> low = new HashMap<>();
    final Map<String, Integer> component = new HashMap<>();
    final Deque<String> stack = new ArrayDeque<>();
    final Set<String> onStack = new HashSet<>();
    for (String start : typeFrames.keySet()) {
      if (index.containsKey(start)) {
        continue;
      }
      final Deque<Visit> visits = new ArrayDeque<>();
      visits.push(enter(start, index, low, stack, onStack));
      while (!visits.isEmpty()) {
        final Visit visit = visits.peek();
        final List<Map.Entry<String, List<Integer>>> uses = typeFrames.get(visit.name).uses;
        if (visit.next < uses.size()) {
          final String used = uses.get(visit.next++).getKey();
          if (!typeFrames.containsKey(used)) {
            continue;
          }
          if (!index.containsKey(used)) {
            visits.push(enter(used, index, low, stack, onStack));
          } else if (onStack.contains(used)) {
            low.put(visit.name, Math.min(low.get(visit.name), index.get(used)));
          }
          continue;
        }
        visits.pop();
        if (!visits.isEmpty()) {
          final String parent = visits.peek().name;
          low.put(parent, Math.min(low.get(parent), low.get(visit.name)));
        }
        if (low.get(visit.name).equals(index.get(visit.name))) {
          final int id = component.size();
          String member;
          do {
            member = stack.pop();
            onStack.remove(member);
            component.put(member, id);
          } while (!member.equals(visit.name));
        }
      }
    }
    return component;
  }

  private static Visit enter(String name, Map<String, Integer> index, Map<String, Integer> low,
      Deque<String> stack, Set<String> onStack) {
    index.put(name, index.size());
    low.put(name, index.get(name));
    stack.push(name);
    onStack.add(name);
    return new Visit(name);
  }

  // The named types with a default at or below them; the others are not expanded at their uses.
  private Set<String> typesWithDefaults() {
    final Map<String, List<String>> usedBy = new HashMap<>();
    final Deque<String> pending = new ArrayDeque<>();
    for (Map.Entry<String, Frame> type : typeFrames.entrySet()) {
      for (Map.Entry<String, List<Integer>> use : type.getValue().uses) {
        if (typeFrames.containsKey(use.getKey())) {
          usedBy.computeIfAbsent(use.getKey(), k -> new ArrayList<>()).add(type.getKey());
        }
      }
      if (!type.getValue().defaults.isEmpty()) {
        pending.push(type.getKey());
      }
    }
    final Set<String> found = new HashSet<>();
    while (!pending.isEmpty()) {
      final String name = pending.pop();
      if (found.add(name)) {
        usedBy.getOrDefault(name, Collections.emptyList()).forEach(pending::push);
      }
    }
    return found;
  }

  private List<Integer> relative(List<Integer> path) {
    return defaultsBase.isEmpty()
        ? path : new ArrayList<>(path.subList(defaultsBase.size(), path.size()));
  }

  /**
   * Captures what a conversion attempt can change here; running the result undoes the attempt.
   */
  public Runnable checkpoint() {
    final Set<T> seen = new HashSet<>(seenSchemas);
    final Deque<String> path = new ArrayDeque<>(fieldsPath);
    final Frame target = frame;
    final Map<List<Integer>, Object> defaults = new HashMap<>(frame.defaults);
    final List<Map.Entry<String, List<Integer>>> uses = new ArrayList<>(frame.uses);
    final Map<String, Frame> types = new HashMap<>(typeFrames);
    return () -> {
      seenSchemas.clear();
      seenSchemas.addAll(seen);
      fieldsPath.clear();
      fieldsPath.addAll(path);
      target.defaults.clear();
      target.defaults.putAll(defaults);
      target.uses.clear();
      target.uses.addAll(uses);
      typeFrames.clear();
      typeFrames.putAll(types);
    };
  }

  public String getCyclicSchemaErrorMessage() {
    StringJoiner joiner = new StringJoiner(".");
    Iterator<String> it = fieldsPath.descendingIterator();
    while (it.hasNext()) {
      joiner.add(it.next());
    }
    return "Cyclic schemas are not supported.\nFound a cycle in the field: " + joiner;
  }

  // What conversion recorded in one place: defaults, and uses of named types, by path.
  private static final class Frame {
    final Map<List<Integer>, Object> defaults = new HashMap<>();
    final List<Map.Entry<String, List<Integer>>> uses = new ArrayList<>();
  }

  // A frame placed below its parent placement, at its use's path from there; within a recursive
  // cycle, with the types placed since the cycle was entered.
  private static final class Placement {
    final String name;
    final Frame frame;
    final Placement parent;
    final List<Integer> step;
    final Set<String> entered;

    Placement(String name, Frame frame, Placement parent, List<Integer> step,
        Set<String> entered) {
      this.name = name;
      this.frame = frame;
      this.parent = parent;
      this.step = step;
      this.entered = entered;
    }

    // The full path of a default recorded here: built only when one is, not at every use.
    List<Integer> pathTo(List<Integer> suffix) {
      int size = suffix.size();
      for (Placement p = this; p != null; p = p.parent) {
        size += p.step.size();
      }
      // Filled from the end: the suffix, then each placement's step up to the root.
      final Integer[] path = new Integer[size];
      int at = size;
      for (int i = suffix.size() - 1; i >= 0; i--) {
        path[--at] = suffix.get(i);
      }
      for (Placement p = this; p != null; p = p.parent) {
        for (int i = p.step.size() - 1; i >= 0; i--) {
          path[--at] = p.step.get(i);
        }
      }
      return new ArrayList<>(Arrays.asList(path));
    }
  }

  // A type being visited by components(), and the next of its uses to follow.
  private static final class Visit {
    final String name;
    int next;

    Visit(String name) {
      this.name = name;
    }
  }
}
