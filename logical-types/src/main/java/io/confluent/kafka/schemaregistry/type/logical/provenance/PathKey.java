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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Where an entity sits in one version's {@link LogicalType}: an index path from the root schema,
 * following every reference, or the use of a named type at one such path.
 *
 * <p>The index path follows the same convention as {@link LogicalType#getDefaultValues()} — a
 * struct field or union branch appends its index within its container, an array or multiset element
 * appends {@code 0}, and a map appends {@code 0} for its key and {@code 1} for its value. Indexes
 * are list positions within {@code getFields()} / {@code getBranches()}, which is what the format
 * readers use to build their own default-value paths; they are not
 * {@code Schema.Field#getPosition()}, which a {@code oneof} makes unreliable.
 *
 * <p>Entities are keyed where they are used, so a member of a named type has the index path of
 * its use. The use itself is an entity too, keyed apart by {@link #getTypeName()}: {@code null}
 * for a member, otherwise the qualified name of the type used there.
 *
 * <p>This is a location, not an identity. It says where an entity was found in one version and is
 * only stable while nothing before it moves; {@link Provenance} is what survives across versions.
 */
public final class PathKey {

  private static final PathKey ROOT = new PathKey(null, Collections.emptyList());

  private final String typeName;
  private final List<Integer> indexPath;
  private final int hash;

  private PathKey(String typeName, List<Integer> indexPath) {
    this.typeName = typeName;
    this.indexPath = indexPath;
    this.hash = Objects.hash(typeName, indexPath);
  }

  /**
   * The root of the {@link LogicalType}'s root schema.
   */
  public static PathKey ofRoot() {
    return ROOT;
  }

  /**
   * The root of the named type {@code qualifiedName}, which is also that named type's own key.
   */
  public static PathKey ofNamedType(String qualifiedName) {
    return new PathKey(Objects.requireNonNull(qualifiedName, "qualifiedName"),
        Collections.emptyList());
  }

  /**
   * The use of the named type {@code qualifiedName} at {@code at} — an entity of its own, keyed
   * apart from the member holding it.
   */
  public static PathKey ofTypeUse(String qualifiedName, PathKey at) {
    return new PathKey(Objects.requireNonNull(qualifiedName, "qualifiedName"), at.indexPath);
  }

  /** This path extended by one step. */
  public PathKey child(int index) {
    List<Integer> extended = new ArrayList<>(indexPath.size() + 1);
    extended.addAll(indexPath);
    extended.add(index);
    return new PathKey(typeName, Collections.unmodifiableList(extended));
  }

  /**
   * The qualified name of the named type this path is rooted at, or {@code null} when it is rooted
   * at the root schema.
   */
  public String getTypeName() {
    return typeName;
  }

  /** The index path from that root, empty for the root itself. */
  public List<Integer> getIndexPath() {
    return indexPath;
  }

  /** True when this path names a root itself — the root schema, or a named type's definition. */
  public boolean isRoot() {
    return indexPath.isEmpty();
  }

  /**
   * This entity's index within its immediate container — the last step of the path.
   *
   * @throws IllegalStateException if this path is a root and has no container
   */
  public int position() {
    if (indexPath.isEmpty()) {
      throw new IllegalStateException("A root path has no position within a container: " + this);
    }
    return indexPath.get(indexPath.size() - 1);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PathKey)) {
      return false;
    }
    PathKey that = (PathKey) o;
    return Objects.equals(typeName, that.typeName) && indexPath.equals(that.indexPath);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(typeName != null ? typeName : "$root");
    for (Integer step : indexPath) {
      sb.append('/').append(step);
    }
    return sb.toString();
  }
}
