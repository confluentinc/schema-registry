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

import java.util.ArrayDeque;
import java.util.ArrayList;
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
  private Map<List<Integer>, Object> defaultValues = new HashMap<>();
  // While a named type's body converts, defaults are recorded relative to it, from this path.
  private List<Integer> defaultsBase = List.of();
  // Each converted named type's defaults, relative to it; each use places them under its path.
  private final Map<String, Map<List<Integer>, Object>> typeDefaults = new HashMap<>();
  private final Set<String> typesConverting = new HashSet<>();

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
    defaultValues.put(defaultsBase.isEmpty()
        ? path : new ArrayList<>(path.subList(defaultsBase.size(), path.size())), value);
  }

  /**
   * Converts a named type's body, first used at {@code path}, keeping its defaults relative to it:
   * the body is shared by reference, so each use places them with {@link #putTypeDefaults}.
   */
  public <R> R convertNamedType(String name, List<Integer> path, Supplier<R> conversion) {
    final Map<List<Integer>, Object> outer = defaultValues;
    final List<Integer> outerBase = defaultsBase;
    final Map<List<Integer>, Object> own = new HashMap<>();
    defaultValues = own;
    defaultsBase = path;
    typesConverting.add(name);
    try {
      final R body = conversion.get();
      typeDefaults.putIfAbsent(name, own);
      return body;
    } finally {
      defaultValues = outer;
      defaultsBase = outerBase;
      typesConverting.remove(name);
    }
  }

  /** Whether a named type's body has been converted, so its defaults are known. */
  public boolean isNamedTypeConverted(String name) {
    return typeDefaults.containsKey(name);
  }

  /** Whether a named type's body is being converted: a use of it now is recursive. */
  public boolean isNamedTypeConverting(String name) {
    return typesConverting.contains(name);
  }

  /**
   * Records a named type's defaults under the path of a use, as inlining it there would. A use
   * within its own body adds none: defaults stop at the first recurrence.
   */
  public void putTypeDefaults(String name, List<Integer> path) {
    final Map<List<Integer>, Object> own = typeDefaults.get(name);
    if (own == null) {
      return;
    }
    for (Map.Entry<List<Integer>, Object> entry : own.entrySet()) {
      final List<Integer> at = new ArrayList<>(path);
      at.addAll(entry.getKey());
      putDefaultValue(at, entry.getValue());
    }
  }

  /** Path-keyed map of field-default values collected during conversion. */
  public Map<List<Integer>, Object> getDefaultValues() {
    return defaultValues;
  }

  /**
   * Captures what a conversion attempt can change here; running the result undoes the attempt.
   */
  public Runnable checkpoint() {
    final Set<T> seen = new HashSet<>(seenSchemas);
    final Deque<String> path = new ArrayDeque<>(fieldsPath);
    final Map<List<Integer>, Object> target = defaultValues;
    final Map<List<Integer>, Object> defaults = new HashMap<>(defaultValues);
    final Map<String, Map<List<Integer>, Object>> types = new HashMap<>(typeDefaults);
    return () -> {
      seenSchemas.clear();
      seenSchemas.addAll(seen);
      fieldsPath.clear();
      fieldsPath.addAll(path);
      target.clear();
      target.putAll(defaults);
      typeDefaults.clear();
      typeDefaults.putAll(types);
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
}
