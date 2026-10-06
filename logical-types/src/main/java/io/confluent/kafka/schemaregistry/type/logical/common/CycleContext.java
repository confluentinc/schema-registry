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
   * at its uses as inlining it would. A type's defaults stop where it recurs below itself.
   */
  public Map<List<Integer>, Object> getDefaultValues() {
    final Set<String> withDefaults = typesWithDefaults();
    final Map<List<Integer>, Object> out = new HashMap<>();
    // A work stack, not recursion: a chain of references can be longer than the call stack is
    // deep. A type is open between its placement and its exit, so a recurrence is on its path.
    final Set<String> open = new HashSet<>();
    final Deque<Placement> work = new ArrayDeque<>();
    work.push(new Placement(null, frame, Collections.emptyList(), false));
    while (!work.isEmpty()) {
      final Placement at = work.pop();
      if (at.exit) {
        open.remove(at.name);
        continue;
      }
      if (at.name != null) {
        if (!open.add(at.name)) {
          continue;
        }
        work.push(new Placement(at.name, null, null, true));
      }
      for (Map.Entry<List<Integer>, Object> entry : at.frame.defaults.entrySet()) {
        out.put(concat(at.path, entry.getKey()), entry.getValue());
      }
      for (Map.Entry<String, List<Integer>> use : at.frame.uses) {
        if (withDefaults.contains(use.getKey())) {
          work.push(new Placement(use.getKey(), typeFrames.get(use.getKey()),
              concat(at.path, use.getValue()), false));
        }
      }
    }
    return out;
  }

  // The named types with a default at or below them; the others are not expanded at their uses.
  private Set<String> typesWithDefaults() {
    final Map<String, List<String>> usedBy = new HashMap<>();
    final Deque<String> pending = new ArrayDeque<>();
    for (Map.Entry<String, Frame> type : typeFrames.entrySet()) {
      for (Map.Entry<String, List<Integer>> use : type.getValue().uses) {
        usedBy.computeIfAbsent(use.getKey(), k -> new ArrayList<>()).add(type.getKey());
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

  private static List<Integer> concat(List<Integer> prefix, List<Integer> suffix) {
    final List<Integer> path = new ArrayList<>(prefix.size() + suffix.size());
    path.addAll(prefix);
    path.addAll(suffix);
    return path;
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

  // A frame to place at a path, as the named type it belongs to; or that type's exit.
  private static final class Placement {
    final String name;
    final Frame frame;
    final List<Integer> path;
    final boolean exit;

    Placement(String name, Frame frame, List<Integer> path, boolean exit) {
      this.name = name;
      this.frame = frame;
      this.path = path;
      this.exit = exit;
    }
  }
}
