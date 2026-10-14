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

package io.confluent.kafka.schemaregistry.type.logical.policy;

import io.confluent.kafka.schemaregistry.type.logical.policy.CompatibilityChecker.FieldView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * How a comparison pairs the writer's struct members with the reader's: the members compared with
 * each other, and so which are added and which deleted.
 *
 * <p>{@link #byName()} is the pairing the checks have always used. {@link #byIds} pairs by the ids
 * of the members' locations — a provenance report's check ids, say — keyed by index path: each
 * member's index in its struct or union, a collection's element {@code 0}, a map's key {@code 0}
 * and value {@code 1}, named types inlined. Under it a rename is a continuing member, and two
 * members of one name but different ids are a deletion and an addition.
 *
 * <p>By name, a named type met twice as the same pair is compared once, its findings at the first
 * path. By ids each use is compared on its own, since each has its own ids and two uses may pair
 * differently; the ids bound the walk, as every member it reaches must have one.
 *
 * <p>Map keys are not paired: Iceberg's key rule is an equality, by name and position.
 *
 * <p>Internal to Schema Registry: not a supported API, and it may change in any release.
 */
public final class FieldPairing {

  private static final FieldPairing BY_NAME = new FieldPairing(null, null);

  private final Map<List<Integer>, Integer> originalIds;
  private final Map<List<Integer>, Integer> updateIds;

  private FieldPairing(Map<List<Integer>, Integer> originalIds,
      Map<List<Integer>, Integer> updateIds) {
    this.originalIds = originalIds;
    this.updateIds = updateIds;
  }

  /**
   * Pairs members of one name, comparing a named type met twice as the same pair once.
   */
  public static FieldPairing byName() {
    return BY_NAME;
  }

  /**
   * Pairs members whose locations have one id.
   *
   * @param originalIds the writer's schema's ids, by index path
   * @param updateIds the reader's schema's ids, by index path
   */
  public static FieldPairing byIds(Map<List<Integer>, Integer> originalIds,
      Map<List<Integer>, Integer> updateIds) {
    return new FieldPairing(Objects.requireNonNull(originalIds, "originalIds"),
        Objects.requireNonNull(updateIds, "updateIds"));
  }

  /** Whether a named type met twice as the same pair is compared once. */
  boolean comparesSharedTypesOnce() {
    return originalIds == null;
  }

  /**
   * Each reader member's partner among the writer's, by its index, or {@code -1} for none.
   *
   * @throws IllegalStateException by ids, for a member whose location has no id, or two members
   *     of one struct with one id; by name, for two writer members of one name
   */
  int[] partners(IndexPath originalPath, List<FieldView> originalFields, IndexPath updatePath,
      List<FieldView> updateFields) {
    int[] partners = new int[updateFields.size()];
    if (originalIds == null) {
      // As the checks always have: a duplicate writer name throws, as their toMap did, its
      // message shortened.
      Map<String, Integer> byName = new HashMap<>();
      for (int i = 0; i < originalFields.size(); i++) {
        if (byName.put(originalFields.get(i).name, i) != null) {
          throw new IllegalStateException("Duplicate key " + originalFields.get(i).name);
        }
      }
      for (int i = 0; i < partners.length; i++) {
        partners[i] = byName.getOrDefault(updateFields.get(i).name, -1);
      }
      return partners;
    }
    Map<Integer, Integer> byId = new HashMap<>();
    for (int i = 0; i < originalFields.size(); i++) {
      if (byId.put(idAt(originalIds, originalPath.child(i)), i) != null) {
        throw new IllegalStateException("Two members with one id under " + originalPath);
      }
    }
    for (int i = 0; i < partners.length; i++) {
      partners[i] = byId.getOrDefault(idAt(updateIds, updatePath.child(i)), -1);
    }
    return partners;
  }

  private static int idAt(Map<List<Integer>, Integer> ids, IndexPath path) {
    Integer id = ids.get(path.toList());
    if (id == null) {
      throw new IllegalStateException("No id for the member at " + path);
    }
    return id;
  }

  /**
   * A location's index path, built a step at a time as a comparison walks down.
   */
  static final class IndexPath {

    static final IndexPath ROOT = new IndexPath(null, -1);

    private final IndexPath parent;
    private final int index;

    private IndexPath(IndexPath parent, int index) {
      this.parent = parent;
      this.index = index;
    }

    IndexPath child(int index) {
      return new IndexPath(this, index);
    }

    List<Integer> toList() {
      List<Integer> steps = new ArrayList<>();
      for (IndexPath at = this; at.parent != null; at = at.parent) {
        steps.add(at.index);
      }
      Collections.reverse(steps);
      return steps;
    }

    @Override
    public String toString() {
      return toList().toString();
    }
  }
}
