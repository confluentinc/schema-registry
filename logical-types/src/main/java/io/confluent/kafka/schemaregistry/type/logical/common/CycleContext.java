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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/** A context for keeping track of the current path context in a schema conversion process. */
public class CycleContext<T> {

  private final Set<T> seenSchemas = new HashSet<>();
  private final Deque<String> fieldsPath = new ArrayDeque<>();
  private final Map<List<Integer>, Object> defaultValues = new HashMap<>();

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
    defaultValues.put(path, value);
  }

  /** Path-keyed map of field-default values collected during conversion. */
  public Map<List<Integer>, Object> getDefaultValues() {
    return defaultValues;
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
