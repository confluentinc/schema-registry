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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One member at one place in an inlined schema: the chain of provenances from the root down to it.
 *
 * <p>{@link Provenance} identifies a logical entity, and a named type shared by two fields has
 * exactly one — but inlining puts its members at two places, and a consumer whose model has no
 * shared types (an Iceberg schema, a Flink {@code RowType}) needs to tell those places apart. The
 * chain does that: {@code home.city} is {@code [home, city]} and {@code work.city} is
 * {@code [work, city]}, differing in their first element.
 *
 * <p>The chain is the right key precisely because it is not a path. A field that moves from
 * position 1 to position 2 keeps its provenance, so the chain is unchanged and the location is
 * recognised as the same one across versions — which is what lets a consumer allocate a stable
 * identifier per location, or match two versions' members without reasoning about prefixes.
 *
 * <p>Collections contribute nothing: an array element or a map value is a step, not an entity, so
 * a member beneath one chains directly to the field that holds the collection.
 */
public final class LocatedProvenance {

  private final List<Provenance> chain;
  private final int hash;

  LocatedProvenance(List<Provenance> chain) {
    this.chain = chain;
    this.hash = chain.hashCode();
  }

  /**
   * The provenances from the outermost enclosing member down to this one, this one last. Never
   * empty.
   */
  public List<Provenance> getChain() {
    return chain;
  }

  /**
   * The member's own provenance — the last of the chain. Two locations of one shared named type's
   * member have equal entities and unequal chains.
   */
  public Provenance getEntity() {
    return chain.get(chain.size() - 1);
  }

  /**
   * How deep this location sits, counting members only. Collection steps do not contribute.
   */
  public int depth() {
    return chain.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocatedProvenance)) {
      return false;
    }
    LocatedProvenance that = (LocatedProvenance) o;
    return hash == that.hash && chain.equals(that.chain);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Provenance provenance : chain) {
      if (sb.length() > 0) {
        sb.append(" > ");
      }
      sb.append(provenance);
    }
    return sb.toString();
  }

  static LocatedProvenance of(List<Provenance> ancestors, Provenance member) {
    List<Provenance> extended = new ArrayList<>(ancestors.size() + 1);
    extended.addAll(ancestors);
    extended.add(member);
    return new LocatedProvenance(Collections.unmodifiableList(extended));
  }
}
