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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The outcome of a {@link LogicalTypeChecker#validate} call.
 *
 * <p>All violations are collected rather than failing on the first one, so a caller can present the
 * complete set of problems with a schema in one pass. Mirrors {@link CompatibilityResult}.
 */
public class ValidityResult {

  private static final ValidityResult VALID = new ValidityResult(Collections.emptyList());

  private final List<Invalidity> invalidities;

  private ValidityResult(List<Invalidity> invalidities) {
    this.invalidities = Collections.unmodifiableList(invalidities);
  }

  /** Returns a result with no violations. */
  public static ValidityResult valid() {
    return VALID;
  }

  /**
   * Returns a result carrying the given violations, or {@link #valid()} if there are none.
   */
  public static ValidityResult of(List<Invalidity> invalidities) {
    if (invalidities == null || invalidities.isEmpty()) {
      return VALID;
    }
    return new ValidityResult(new ArrayList<>(invalidities));
  }

  public boolean isValid() {
    return invalidities.isEmpty();
  }

  /** The violations found, in the order they were discovered. Empty when valid. */
  public List<Invalidity> getInvalidities() {
    return invalidities;
  }

  /**
   * Renders every violation as a single newline-separated string, suitable for an error message.
   * Returns the empty string when valid.
   */
  public String describe() {
    return invalidities.stream()
        .map(Invalidity::toString)
        .collect(Collectors.joining("\n"));
  }

  @Override
  public String toString() {
    return isValid()
        ? "ValidityResult{valid}"
        : "ValidityResult{" + invalidities.size() + " invalidities: " + describe() + "}";
  }
}
