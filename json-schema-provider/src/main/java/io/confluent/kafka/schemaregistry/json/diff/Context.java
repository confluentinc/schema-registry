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
  // Pairs known (from a previous fixpoint pass) to be incompatible, mapped to their pair-relative
  // differences. When a recursive cycle re-enters such a pair, its real differences are propagated
  // instead of optimistically assuming the cycle compatible. Iterating this seed to a fixpoint is
  // what makes cross-branch memoization sound. See SchemaDiff#compare.
  private final Map<SchemaPair, List<Difference>> incompatibleSeed;
  // Set to true (in the shared holder) whenever a recursive cycle is optimistically assumed
  // compatible. If a pass never does this, its result does not depend on any assumption and the
  // fixpoint iteration can stop immediately.
  private final boolean[] optimismUsed;
  private final Deque<String> jsonPath;
  private final List<Difference> diffs;

  public Context(Set<Difference.Type> compatibleChanges) {
    this(compatibleChanges, Collections.emptyMap());
  }

  public Context(Set<Difference.Type> compatibleChanges,
      Map<SchemaPair, List<Difference>> incompatibleSeed) {
    this(compatibleChanges, new HashMap<>(), new HashSet<>(), incompatibleSeed, new boolean[1]);
  }

  private Context(
      Set<Difference.Type> compatibleChanges,
      Map<SchemaPair, List<Difference>> memo,
      Set<SchemaPair> inProgress,
      Map<SchemaPair, List<Difference>> incompatibleSeed,
      boolean[] optimismUsed) {
    this.compatibleChanges = compatibleChanges;
    this.memo = memo;
    this.inProgress = inProgress;
    this.incompatibleSeed = incompatibleSeed;
    this.optimismUsed = optimismUsed;
    this.jsonPath = new ArrayDeque<>();
    this.diffs = new ArrayList<>();
  }

  public Context getSubcontext() {
    // The memo and in-progress set are shared by reference so that caching and cycle detection
    // span the whole comparison, not just a single branch.
    Context ctx = new Context(this.compatibleChanges, this.memo, this.inProgress,
        this.incompatibleSeed, this.optimismUsed);
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
      // Recursive cycle. If this pair is already known to be incompatible (from a prior fixpoint
      // pass), propagate its differences rather than optimistically assuming compatibility.
      List<Difference> seeded = incompatibleSeed.get(key);
      if (seeded != null) {
        return seeded;
      }
      // Otherwise assume compatible (greatest fixed point) and do not cache the truncated result.
      optimismUsed[0] = true;
      return Collections.emptyList();
    }
    try {
      Context subctx = new Context(this.compatibleChanges, this.memo, this.inProgress,
          this.incompatibleSeed, this.optimismUsed);
      comparator.compare(subctx);
      // Cache an immutable snapshot so the memoized value cannot be corrupted by a caller (or a
      // future refactor) mutating the list returned from getDifferences().
      List<Difference> result =
          Collections.unmodifiableList(new ArrayList<>(subctx.getDifferences()));
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

  /**
   * Returns the memoized pairs whose differences make them incompatible, mapped to those
   * (pair-relative) differences. Used by {@link SchemaDiff#compare} to grow the incompatible seed
   * between fixpoint passes.
   */
  public Map<SchemaPair, List<Difference>> collectIncompatiblePairs() {
    Map<SchemaPair, List<Difference>> incompatible = new HashMap<>();
    for (Map.Entry<SchemaPair, List<Difference>> entry : memo.entrySet()) {
      boolean entryIncompatible = entry.getValue().stream()
          .anyMatch(d -> !compatibleChanges.contains(d.getType()));
      if (entryIncompatible) {
        incompatible.put(entry.getKey(), entry.getValue());
      }
    }
    return incompatible;
  }

  /** Whether this comparison optimistically assumed any recursive cycle compatible. */
  public boolean usedOptimisticTruncation() {
    return optimismUsed[0];
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
  static final class SchemaPair {
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
