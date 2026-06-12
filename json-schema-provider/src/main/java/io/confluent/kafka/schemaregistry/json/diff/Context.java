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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Context {
  private final Set<Difference.Type> compatibleChanges;
  // Memoizes the result of comparing a given (original, update) pair so that the cross-product
  // comparisons performed for combined schemas (oneOf/anyOf/allOf) are not recomputed for every
  // path that reaches the same pair. Without this, recursive schemas where many definitions refer
  // to one another produce a combinatorial explosion. Differences are stored relative to the pair
  // being compared (i.e. with an empty path prefix) and rebased onto the current path on reuse.
  private final Map<SchemaPair, List<Difference>> memo;
  // Pairs currently being compared on the call stack. A pair that is re-encountered while still in
  // progress is a recursive cycle; it is treated as compatible (no differences) and is not cached,
  // since its truncated result is only valid within the enclosing comparison.
  private final Set<SchemaPair> inProgress;
  private final Deque<String> jsonPath;
  private final List<Difference> diffs;

  public Context(Set<Difference.Type> compatibleChanges) {
    this(compatibleChanges, new HashMap<>(), new HashSet<>());
  }

  private Context(
      Set<Difference.Type> compatibleChanges,
      Map<SchemaPair, List<Difference>> memo,
      Set<SchemaPair> inProgress) {
    this.compatibleChanges = compatibleChanges;
    this.memo = memo;
    this.inProgress = inProgress;
    this.jsonPath = new ArrayDeque<>();
    this.diffs = new ArrayList<>();
  }

  public Context getSubcontext() {
    // The memo and in-progress set are shared by reference so that caching and cycle detection
    // span the whole comparison, not just a single branch.
    Context ctx = new Context(this.compatibleChanges, this.memo, this.inProgress);
    ctx.jsonPath.addAll(this.jsonPath);
    return ctx;
  }

  /**
   * Compares a pair of schemas with memoization, returning the differences with paths relative to
   * the pair (i.e. as if the pair were the root). If the pair is already cached the cached result
   * is returned; if it is currently being compared higher in the stack it is treated as a
   * compatible cycle and an empty list is returned without caching. Otherwise {@code comparator}
   * is invoked on a fresh subcontext rooted at the pair, and its differences are cached.
   */
  public List<Difference> compareMemoized(
      final Schema original, final Schema update, final PairComparator comparator) {
    SchemaPair key = new SchemaPair(original, update);
    List<Difference> cached = memo.get(key);
    if (cached != null) {
      return cached;
    }
    if (!inProgress.add(key)) {
      // Recursive cycle: assume compatible and do not cache the truncated result.
      return Collections.emptyList();
    }
    try {
      Context subctx = new Context(this.compatibleChanges, this.memo, this.inProgress);
      comparator.compare(subctx);
      List<Difference> result = subctx.getDifferences();
      memo.put(key, result);
      return result;
    } finally {
      inProgress.remove(key);
    }
  }

  /**
   * Adds differences that were computed relative to a nested pair, rebasing each one onto the
   * current path.
   */
  public void addDifferencesRebased(final List<Difference> relativeDiffs) {
    if (jsonPath.isEmpty()) {
      diffs.addAll(relativeDiffs);
      return;
    }
    String prefix = jsonPathString(jsonPath);
    for (Difference diff : relativeDiffs) {
      diffs.add(new Difference(diff.getType(), rebase(prefix, diff.getJsonPath())));
    }
  }

  @FunctionalInterface
  public interface PairComparator {
    void compare(Context subctx);
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

  // Combines a path prefix (e.g. "#/a/b") with a path relative to it (e.g. "#/x/y" -> "#/a/b/x/y").
  // The relative path "#/" denotes the root of the pair and resolves to the prefix itself.
  private static String rebase(final String prefix, final String relativePath) {
    if (relativePath.length() <= 2) {
      return prefix;
    }
    return prefix + "/" + relativePath.substring(2);
  }

  // Identity-based key over a pair of schemas. Two distinct Java objects are never equal even if
  // structurally identical, which is what allows a genuine change deep in a recursive structure to
  // be detected rather than masked by the cache.
  private static final class SchemaPair {
    private final Schema original;
    private final Schema update;

    SchemaPair(final Schema original, final Schema update) {
      this.original = original;
      this.update = update;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SchemaPair)) {
        return false;
      }
      SchemaPair that = (SchemaPair) o;
      return this.original == that.original && this.update == that.update;
    }

    @Override
    public int hashCode() {
      return 31 * System.identityHashCode(original) + System.identityHashCode(update);
    }
  }
}
