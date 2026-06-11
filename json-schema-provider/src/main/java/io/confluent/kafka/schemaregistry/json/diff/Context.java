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

package io.confluent.kafka.schemaregistry.json.diff;

import org.everit.json.schema.Schema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Context {
  // Safety backstop (DGS-24489): a compatibility check can never run unbounded and starve the
  // registry. A deterministic call budget bounds total work (and, since per-call cost is bounded
  // by schema size, wall time too). Overridable via system property; the default is generous so
  // only pathological schemas trip it.
  static final String MAX_CALLS_PROP = "schema.registry.json.diff.max.calls";
  static final long DEFAULT_MAX_CALLS = 50_000_000L;

  private final Set<Difference.Type> compatibleChanges;
  private final Set<Schema> schemas;
  private final Deque<String> jsonPath;
  private final List<Difference> diffs;
  private final long maxCalls;
  // Single shared counter across all subcontexts of one top-level comparison.
  private final long[] callCount;
  // Memoization of compare(original, update) results, keyed by object identity, shared across
  // all subcontexts of a single top-level comparison. Collapses the exponential re-computation
  // that recursive schemas with large oneOf/anyOf unions otherwise trigger (DGS-24489).
  private final Map<Schema, Map<Schema, List<Difference>>> resultCache;
  // Pairs currently being computed on the call stack — re-entry means a reference cycle, handled
  // identically to the path-based schema guard below (contributes no differences).
  private final Map<Schema, Set<Schema>> inProgress;

  public Context(Set<Difference.Type> compatibleChanges) {
    this.compatibleChanges = compatibleChanges;
    this.schemas = Collections.newSetFromMap(new IdentityHashMap<>());
    this.jsonPath = new ArrayDeque<>();
    this.diffs = new ArrayList<>();
    this.resultCache = new IdentityHashMap<>();
    this.inProgress = new IdentityHashMap<>();
    this.maxCalls = Long.getLong(MAX_CALLS_PROP, DEFAULT_MAX_CALLS);
    this.callCount = new long[] {0L};
  }

  private Context(Context parent) {
    this.compatibleChanges = parent.compatibleChanges;
    this.schemas = Collections.newSetFromMap(new IdentityHashMap<>());
    this.jsonPath = new ArrayDeque<>();
    this.diffs = new ArrayList<>();
    // Cache, in-progress set, and the call budget are shared by reference across the comparison.
    this.resultCache = parent.resultCache;
    this.inProgress = parent.inProgress;
    this.maxCalls = parent.maxCalls;
    this.callCount = parent.callCount;
  }

  /**
   * Records one comparison call and enforces the call budget. Throws {@link IllegalStateException}
   * (the same hard-stop signal used for unresolved {@code $ref}s) if the comparison has made too
   * many calls, aborting a pathological check rather than letting it starve the registry.
   */
  public void checkBudget() {
    if (++callCount[0] > maxCalls) {
      throw new IllegalStateException(
          "Compatibility check exceeded the limit of " + maxCalls + " schema comparisons");
    }
  }

  public Context getSubcontext() {
    Context ctx = new Context(this);
    ctx.schemas.addAll(this.schemas);
    ctx.jsonPath.addAll(this.jsonPath);
    return ctx;
  }

  public List<Difference> getCachedResult(final Schema original, final Schema update) {
    Map<Schema, List<Difference>> byUpdate = resultCache.get(original);
    return byUpdate == null ? null : byUpdate.get(update);
  }

  public void cacheResult(
      final Schema original, final Schema update, final List<Difference> result) {
    resultCache.computeIfAbsent(original, k -> new IdentityHashMap<>()).put(update, result);
  }

  public boolean isInProgress(final Schema original, final Schema update) {
    Set<Schema> updates = inProgress.get(original);
    return updates != null && updates.contains(update);
  }

  public void enterPair(final Schema original, final Schema update) {
    inProgress.computeIfAbsent(original, k -> Collections.newSetFromMap(new IdentityHashMap<>()))
        .add(update);
  }

  public void exitPair(final Schema original, final Schema update) {
    Set<Schema> updates = inProgress.get(original);
    if (updates != null) {
      updates.remove(update);
    }
  }

  public int differencesSize() {
    return diffs.size();
  }

  public List<Difference> differencesSince(final int mark) {
    return new ArrayList<>(diffs.subList(mark, diffs.size()));
  }

  public SchemaScope enterSchema(final Schema schema) {
    return !schemas.contains(schema) ? new SchemaScope(schema) : null;
  }

  public class SchemaScope implements AutoCloseable {
    private final Schema schema;

    public SchemaScope(final Schema schema) {
      this.schema = schema;
      schemas.add(schema);
    }

    @Override
    public void close() {
      schemas.remove(schema);
    }
  }

  public PathScope enterPath(final String path) {
    return new PathScope(path);
  }

  public class PathScope implements AutoCloseable {
    public PathScope(final String path) {
      jsonPath.addLast(path);
    }

    @Override
    public void close() {
      jsonPath.removeLast();
    }
  }

  public boolean isCompatible() {
    boolean notCompatible = getDifferences().stream()
        .map(Difference::getType)
        .anyMatch(t -> !compatibleChanges.contains(t));
    return !notCompatible;
  }

  public List<Difference> getDifferences() {
    return diffs;
  }

  public void addDifference(final Difference.Type type) {
    diffs.add(new Difference(type, jsonPathString(jsonPath)));
  }

  public void addDifference(final String attribute, final Difference.Type type) {
    jsonPath.addLast(attribute);
    addDifference(type);
    jsonPath.removeLast();
  }

  public void addDifferences(final List<Difference> differences) {
    diffs.addAll(differences);
  }

  private static String jsonPathString(final Deque<String> jsonPath) {
    return "#/" + String.join("/", jsonPath);
  }
}
