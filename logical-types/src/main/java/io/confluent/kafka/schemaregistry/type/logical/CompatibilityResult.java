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

package io.confluent.kafka.schemaregistry.type.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The outcome of a {@link CompatibilityChecker#compare} call.
 *
 * <p>All violations are collected rather than failing on the first one, so a caller can present the
 * complete set of problems with a schema change in one pass.
 */
public class CompatibilityResult {

  private static final CompatibilityResult COMPATIBLE =
      new CompatibilityResult(Collections.emptyList());

  private final List<Incompatibility> incompatibilities;

  private CompatibilityResult(List<Incompatibility> incompatibilities) {
    this.incompatibilities = Collections.unmodifiableList(incompatibilities);
  }

  /** Returns a result with no violations. */
  public static CompatibilityResult compatible() {
    return COMPATIBLE;
  }

  /**
   * Returns a result carrying the given violations, or {@link #compatible()} if there are none.
   */
  public static CompatibilityResult of(List<Incompatibility> incompatibilities) {
    if (incompatibilities == null || incompatibilities.isEmpty()) {
      return COMPATIBLE;
    }
    return new CompatibilityResult(new ArrayList<>(incompatibilities));
  }

  public boolean isCompatible() {
    return incompatibilities.isEmpty();
  }

  /** The violations found, in the order they were discovered. Empty when compatible. */
  public List<Incompatibility> getIncompatibilities() {
    return incompatibilities;
  }

  /**
   * Renders every violation as a single newline-separated string, suitable for an error message.
   * Returns the empty string when compatible.
   */
  public String describe() {
    return incompatibilities.stream()
        .map(Incompatibility::toString)
        .collect(Collectors.joining("\n"));
  }

  @Override
  public String toString() {
    return isCompatible()
        ? "CompatibilityResult{compatible}"
        : "CompatibilityResult{" + incompatibilities.size() + " incompatibilities: "
            + describe() + "}";
  }
}
