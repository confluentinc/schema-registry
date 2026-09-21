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

import java.util.Objects;

/**
 * A {@link Scope} reached by descending through unnamed type nodes — a collection's element, or a
 * map's key or value — from a base scope. See {@link Scope} for why a {@code LogicalType} needs
 * this and an Avro or Protobuf entity tree does not.
 */
public final class StepScope implements Scope {

  private final Scope base;
  private final String steps;
  private final int hash;

  private StepScope(Scope base, String steps) {
    this.base = base;
    this.steps = steps;
    this.hash = Objects.hash(base, steps);
  }

  /**
   * {@code base} taken one structural step deeper. Steps accumulate against the nearest
   * non-stepped base, so the representation stays canonical.
   */
  public static Scope step(Scope base, String step) {
    Objects.requireNonNull(base, "base");
    if (base instanceof StepScope) {
      StepScope stepped = (StepScope) base;
      return new StepScope(stepped.base, stepped.steps + step);
    }
    return new StepScope(base, step);
  }

  /**
   * The scope these steps descend from: a {@link RootScope} or an {@link Identity}.
   */
  public Scope getBase() {
    return base;
  }

  /**
   * The accumulated structural steps, such as {@code []} or <code>{value}</code>.
   */
  public String getSteps() {
    return steps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StepScope)) {
      return false;
    }
    StepScope that = (StepScope) o;
    return hash == that.hash && steps.equals(that.steps) && base.equals(that.base);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return base + steps;
  }
}
