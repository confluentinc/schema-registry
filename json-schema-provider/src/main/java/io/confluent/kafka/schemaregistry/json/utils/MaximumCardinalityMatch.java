/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Uses the Hopcroft–Karp algorithm for maximum cardinality matching.
 */
public class MaximumCardinalityMatch<V, T> {

  private final Set<Edge<V, T>> edges;
  private final Set<V> partition1;
  private final Set<V> partition2;

  private Map<V, Set<Edge<V, T>>> adjacencyList;
  private List<V> vertices;
  private Map<V, Integer> vertexIndexMap;

  private int matchedVertices;

  private final int nil = 0;
  private final int infinity = Integer.MAX_VALUE;

  private int[] matching;
  private int[] dist;

  private Queue<Integer> queue;

  public MaximumCardinalityMatch(
      Set<Edge<V, T>> edges, Set<V> partition1, Set<V> partition2) {
    this.edges = edges;

    if (partition1.size() <= partition2.size()) {
      this.partition1 = partition1;
      this.partition2 = partition2;
    } else {
      this.partition1 = partition2;
      this.partition2 = partition1;
    }
  }

  private Edge<V, T> edge(V source, V target) {
    return adjacencyList.getOrDefault(source, Collections.emptySet()).stream()
        .filter(e -> e.source() == target || e.target() == target)
        .findAny()
        .orElse(null);
  }

  private List<V> neighborsOf(V source) {
    return adjacencyList.getOrDefault(source, Collections.emptySet()).stream()
        .map(e -> e.source() == source ? e.target() : e.source())
        .collect(Collectors.toList());
  }

  private void init() {
    // Use IdentityHashMap to account for vertices in both partitions which appear equal
    adjacencyList = new IdentityHashMap<>();
    for (Edge<V, T> edge : edges) {
      Set<Edge<V, T>> adj = adjacencyList.computeIfAbsent(edge.source(), k -> new HashSet<>());
      adj.add(edge);
      adj = adjacencyList.computeIfAbsent(edge.target(), k -> new HashSet<>());
      adj.add(edge);
    }

    vertices = new ArrayList<>();
    vertices.add(null);
    vertices.addAll(partition1);
    vertices.addAll(partition2);
    // Use IdentityHashMap to account for vertices in both partitions which appear equal
    vertexIndexMap = new IdentityHashMap<>();
    for (int i = 0; i < vertices.size(); i++) {
      vertexIndexMap.put(vertices.get(i), i);
    }

    matching = new int[vertices.size() + 1];
    dist = new int[partition1.size() + 1];
    queue = new ArrayDeque<>(vertices.size());
  }

  private void computeInitialMatching() {
    // compute initial matching
    for (V u0 : partition1) {
      int u = vertexIndexMap.get(u0);

      for (V v0 : neighborsOf(u0)) {
        int v = vertexIndexMap.get(v0);
        if (matching[v] == nil) {
          matching[v] = u;
          matching[u] = v;
          matchedVertices++;
          break;
        }
      }
    }
  }

  private boolean bfs() {
    queue.clear();

    for (int u = 1; u <= partition1.size(); u++) {
      if (matching[u] == nil) {
        dist[u] = 0;
        queue.add(u);
      } else {
        dist[u] = infinity;
      }
    }
    dist[nil] = infinity;

    while (!queue.isEmpty()) {
      int u = queue.poll();
      if (dist[u] < dist[nil]) {
        for (V v0 : neighborsOf(vertices.get(u))) {
          int v = vertexIndexMap.get(v0);
          if (dist[matching[v]] == infinity) {
            dist[matching[v]] = dist[u] + 1;
            queue.add(matching[v]);
          }
        }
      }
    }
    return dist[nil] != infinity;
  }

  private boolean dfs(int u) {
    if (u != nil) {
      for (V v0 : neighborsOf(vertices.get(u))) {
        int v = vertexIndexMap.get(v0);
        if (dist[matching[v]] == dist[u] + 1) {
          if (dfs(matching[v])) {
            matching[v] = u;
            matching[u] = v;
            return true;
          }
        }
      }
      dist[u] = infinity;
      return false;
    }
    return true;
  }

  public Set<Edge<V, T>> getMatching() {
    init();
    computeInitialMatching();

    while (matchedVertices < partition1.size() && bfs()) {
      for (int v = 1; v <= partition1.size() && matchedVertices < partition1.size(); v++) {
        if (matching[v] == nil) {
          if (dfs(v)) {
            matchedVertices++;
          }
        }
      }
    }
    assert matchedVertices <= partition1.size();

    Set<Edge<V, T>> edges = new HashSet<>();
    for (int i = 0; i < vertices.size(); i++) {
      if (matching[i] != nil) {
        edges.add(edge(vertices.get(i), vertices.get(matching[i])));
      }
    }
    return edges;
  }
}

