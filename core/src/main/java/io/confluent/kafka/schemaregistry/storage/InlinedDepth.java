/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * How deep a logical type nests once its named types are inlined, a cycle cut where it closes.
 * Iterative over the named types, each body measured once, so a deep or dense graph costs its size.
 * Every level counts, a leaf's included, so against the shared limit it rejects a level or two
 * sooner than a count of a location's path.
 */
final class InlinedDepth {

  private InlinedDepth() {
  }

  static int of(LogicalType logicalType) {
    Map<String, Schema> named = logicalType.getNamedTypes();
    Map<String, Integer> depths = new HashMap<>();
    Set<String> open = new HashSet<>();
    // Post-order over the reference graph: a body is measured once every type it uses is.
    Set<String> roots = new LinkedHashSet<>();
    LogicalType.collectNamedRefs(logicalType.getRootSchema(), roots);
    Deque<String> stack = new ArrayDeque<>(roots);
    while (!stack.isEmpty()) {
      String name = stack.peek();
      if (depths.containsKey(name) || !named.containsKey(name)) {
        stack.pop();
        continue;
      }
      if (open.add(name)) {
        Set<String> refs = new LinkedHashSet<>();
        LogicalType.collectNamedRefs(named.get(name), refs);
        for (String ref : refs) {
          if (!depths.containsKey(ref) && !open.contains(ref)) {
            stack.push(ref);
          }
        }
        continue;
      }
      stack.pop();
      depths.put(name, depth(named.get(name), depths));
    }
    return depth(logicalType.getRootSchema(), depths);
  }

  // A body's depth, its references counted as already measured (a cycle's back edge as 0).
  private static int depth(Schema schema, Map<String, Integer> depths) {
    if (schema == null) {
      return 0;
    }
    List<Schema> children = new ArrayList<>();
    switch (schema.getType()) {
      case NAMED_TYPE_REF:
        return depths.getOrDefault(schema.getQualifiedName(), 0);
      case STRUCT:
        schema.getFields().forEach(f -> children.add(f.getSchema()));
        break;
      case UNION:
        schema.getBranches().forEach(b -> children.add(b.getSchema()));
        break;
      case ARRAY:
      case MULTISET:
        children.add(schema.getElementType());
        break;
      case MAP:
        children.add(schema.getKeyType());
        children.add(schema.getValueType());
        break;
      default:
        return 1;
    }
    int max = 0;
    for (Schema child : children) {
      max = Math.max(max, depth(child, depths));
    }
    return max + 1;
  }
}
