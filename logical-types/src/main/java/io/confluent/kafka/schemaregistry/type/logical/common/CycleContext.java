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
   * at its uses. Defaults stop at a type's first recurrence below itself.
   */
  public Map<List<Integer>, Object> getDefaultValues() {
    final Map<String, Map<List<Integer>, Object>> placed = new HashMap<>();
    return place(frame, new HashSet<>(), placed);
  }

  private Map<List<Integer>, Object> place(Frame from, Set<String> open,
      Map<String, Map<List<Integer>, Object>> placed) {
    final Map<List<Integer>, Object> out = new HashMap<>(from.defaults);
    for (Map.Entry<String, List<Integer>> use : from.uses) {
      for (Map.Entry<List<Integer>, Object> entry
          : typeDefaults(use.getKey(), open, placed).entrySet()) {
        final List<Integer> at = new ArrayList<>(use.getValue());
        at.addAll(entry.getKey());
        out.put(at, entry.getValue());
      }
    }
    return out;
  }

  // A named type's defaults, its uses placed; none for a type below itself or never converted.
  private Map<List<Integer>, Object> typeDefaults(String name, Set<String> open,
      Map<String, Map<List<Integer>, Object>> placed) {
    final Map<List<Integer>, Object> done = placed.get(name);
    if (done != null) {
      return done;
    }
    final Frame own = typeFrames.get(name);
    if (own == null || !open.add(name)) {
      return Collections.emptyMap();
    }
    final Map<List<Integer>, Object> result = place(own, open, placed);
    open.remove(name);
    placed.put(name, result);
    return result;
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
}
