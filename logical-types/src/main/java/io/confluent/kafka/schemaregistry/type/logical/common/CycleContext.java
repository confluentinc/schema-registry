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

import java.util.AbstractMap;
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
   * Converts a named type's body at {@code path} (Avro at its first use, the other formats at the
   * empty path), keeping its defaults relative to it: the body is shared by reference, so each use
   * places them with {@link #putTypeDefaults}.
   */
  public <R> R convertNamedType(String name, List<Integer> path, Supplier<R> conversion) {
    final Frame outer = frame;
    final List<Integer> outerBase = defaultsBase;
    final Frame own = new Frame();
    frame = own;
    defaultsBase = path;
    try {
      final R body = conversion.get();
      // The latest complete conversion's, as the body kept is: one re-entered while its body
      // converted (a JSON definition through another not yet known) can miss what that held.
      typeFrames.put(name, own);
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
   * at its uses as inlining it would, up to where a type recurs on the path below itself. Read
   * once conversion is done: the map places them on first access.
   */
  public Map<List<Integer>, Object> getDefaultValues() {
    // Placed on first read: callers converting only to compare or check types (provenance, the
    // LOGICAL policy, DDL) never read defaults, and placing them can be exponential in the schema.
    return new Placed(this::placeDefaults);
  }

  private Map<List<Integer>, Object> placeDefaults() {
    final Map<String, Integer> components = components();
    final Set<Integer> recursive = recursiveComponents(components);
    final Set<String> withDefaults = typesWithDefaults();
    final Map<List<Integer>, Object> out = new HashMap<>();
    // A work stack, not recursion: a chain of references can be longer than the call stack is
    // deep. A recursive type is open from its placement to its exit marker, below its uses.
    final Openings open = new Openings();
    final Deque<Placement> work = new ArrayDeque<>();
    work.push(new Placement(null, frame, null, Collections.emptyList(), null));
    while (!work.isEmpty()) {
      final Placement at = work.pop();
      if (at.frame == null) {
        open.exit();
        continue;
      }
      Step witness = null;
      if (at.name != null && recursive.contains(components.get(at.name))) {
        // Skipped where it recurs, or where every default below it is past a recurrence: so
        // each placement made leads to a default, and the walk is bounded by what it emits.
        if (open.contains(at.name)) {
          continue;
        }
        witness = at.witness != null ? at.witness : reaches(at.name, open, components,
            withDefaults);
        if (witness == null) {
          continue;
        }
        open.enter(at.name);
        work.push(new Placement(at.name, null, null, null, null));
      }
      for (Map.Entry<List<Integer>, Object> entry : at.frame.defaults.entrySet()) {
        out.put(at.pathTo(entry.getKey()), entry.getValue());
      }
      final List<Map.Entry<String, List<Integer>>> uses = at.frame.uses;
      for (int i = uses.size() - 1; i >= 0; i--) {
        final String used = uses.get(i).getKey();
        if (withDefaults.contains(used)) {
          // The witness's next step needs no search: none of its rest was open, nor is now.
          final Step rest = witness != null && witness.next != null
              && witness.next.name.equals(used) ? witness.next : null;
          work.push(new Placement(used, typeFrames.get(used), at, uses.get(i).getValue(), rest));
        }
      }
    }
    return out;
  }

  /**
   * A path from {@code start}, a recursive type, to a default reachable without passing a type
   * open on the path: a default of its cycle, or a use leaving the cycle toward one; null when
   * none is. Outside the cycle no open type is reachable, so a type with defaults there has them.
   */
  private Step reaches(String start, Openings open, Map<String, Integer> components,
      Set<String> withDefaults) {
    if (open.isDead(start)) {
      return null;
    }
    final Integer cycle = components.get(start);
    final Map<String, String> reachedFrom = new HashMap<>();
    final Deque<String> pending = new ArrayDeque<>();
    reachedFrom.put(start, null);
    pending.push(start);
    while (!pending.isEmpty()) {
      final String name = pending.pop();
      final Frame type = typeFrames.get(name);
      if (!type.defaults.isEmpty()) {
        return pathFrom(name, reachedFrom);
      }
      for (Map.Entry<String, List<Integer>> use : type.uses) {
        final String used = use.getKey();
        if (!withDefaults.contains(used)) {
          continue;
        }
        if (!components.get(used).equals(cycle)) {
          return pathFrom(name, reachedFrom);
        }
        if (!open.contains(used) && !open.isDead(used) && !reachedFrom.containsKey(used)) {
          reachedFrom.put(used, name);
          pending.push(used);
        }
      }
    }
    // Nothing reachable from any of them while the types open now stay open.
    open.markDead(reachedFrom.keySet());
    return null;
  }

  // The search's path from its start to {@code end}, start first.
  private static Step pathFrom(String end, Map<String, String> reachedFrom) {
    Step path = null;
    for (String name = end; name != null; name = reachedFrom.get(name)) {
      path = new Step(name, path);
    }
    return path;
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

  // Whether a use stays inside its type's strongly connected component (a self-use included).
  private static boolean sameCycle(Map<String, Integer> components, String from, String to) {
    return components.get(from).equals(components.get(to));
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

  // A frame placed below its parent placement, at its use's path from there; with no frame, the
  // exit of a recursive type's placement. A witness, when known, is a path from it to a default.
  private static final class Placement {
    final String name;
    final Frame frame;
    final Placement parent;
    final List<Integer> step;
    final Step witness;

    Placement(String name, Frame frame, Placement parent, List<Integer> step, Step witness) {
      this.name = name;
      this.frame = frame;
      this.parent = parent;
      this.step = step;
      this.witness = witness;
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

  /**
   * The recursive types open on the current path, opened and closed as a stack, and the types
   * known to reach no default while some of them stay open. A type found so under the innermost
   * open type stays so while that one is open: any opened since only blocks more.
   */
  private static final class Openings {
    private final Set<String> open = new HashSet<>();
    private final Deque<String> order = new ArrayDeque<>();
    private final Deque<Integer> serials = new ArrayDeque<>();
    private final Set<Integer> live = new HashSet<>();
    private final Map<String, Integer> deadWhile = new HashMap<>();
    private int next = 1;

    boolean contains(String name) {
      return open.contains(name);
    }

    void enter(String name) {
      open.add(name);
      serials.push(next);
      live.add(next++);
      order.push(name);
    }

    void exit() {
      live.remove(serials.pop());
      open.remove(order.pop());
    }

    // 0 while nothing is open: what holds then holds throughout.
    private int innermost() {
      return serials.isEmpty() ? 0 : serials.peek();
    }

    boolean isDead(String name) {
      final Integer since = deadWhile.get(name);
      return since != null && (since == 0 || live.contains(since));
    }

    void markDead(Set<String> types) {
      final int since = innermost();
      for (String type : types) {
        deadWhile.put(type, since);
      }
    }
  }

  // The defaults, placed once on first read; the conversion's state is released then.
  private static final class Placed extends AbstractMap<List<Integer>, Object> {
    private Supplier<Map<List<Integer>, Object>> placement;
    private volatile Map<List<Integer>, Object> placed;

    Placed(Supplier<Map<List<Integer>, Object>> placement) {
      this.placement = placement;
    }

    private Map<List<Integer>, Object> placed() {
      Map<List<Integer>, Object> result = placed;
      if (result == null) {
        synchronized (this) {
          result = placed;
          if (result == null) {
            result = placement.get();
            placed = result;
            placement = null;
          }
        }
      }
      return result;
    }

    @Override
    public Set<Entry<List<Integer>, Object>> entrySet() {
      return placed().entrySet();
    }

    @Override
    public Object get(Object key) {
      return placed().get(key);
    }

    @Override
    public boolean containsKey(Object key) {
      return placed().containsKey(key);
    }

    @Override
    public int size() {
      return placed().size();
    }
  }

  // A path of named types, as a list sharing its tails.
  private static final class Step {
    final String name;
    final Step next;

    Step(String name, Step next) {
      this.name = name;
      this.next = next;
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
