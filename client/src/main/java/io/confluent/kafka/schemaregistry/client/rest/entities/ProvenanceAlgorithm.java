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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import java.util.Locale;

/**
 * A released version of the provenance algorithm. A fix ships as a new version, and every earlier
 * one keeps answering exactly as it did, so a caller can move to it deliberately and back again.
 * Each is effective from when it was released: under {@link #DYNAMIC_NAME}, a version registered
 * since is matched to its predecessor by it, and one registered before by the one before it.
 *
 * <p>A version's registration time is persisted from the release that stores createTs on; a new
 * algorithm must take effect only in a later release, or a soft delete written by an older
 * registry could leave nodes disagreeing on when a version was registered.
 */
public enum ProvenanceAlgorithm {

  // The first: effective for every version, whenever registered.
  V1("v1", 0L);

  /**
   * The version a request that names none is answered with. It never changes: a newer version is
   * only ever asked for by name, so no upgrade moves a caller's pairings.
   */
  public static final ProvenanceAlgorithm DEFAULT = V1;

  /**
   * The name asking for each version to be matched to its predecessor by the version effective
   * when it was registered, so a newer algorithm never changes the ids an older one assigned.
   */
  public static final String DYNAMIC_NAME = "dynamic";

  private final String name;
  // Epoch millis from which the version is effective.
  private final long effectiveTimestamp;

  ProvenanceAlgorithm(String name, long effectiveTimestamp) {
    this.name = name;
    this.effectiveTimestamp = effectiveTimestamp;
  }

  public String getName() {
    return name;
  }

  public long getEffectiveTimestamp() {
    return effectiveTimestamp;
  }

  /**
   * The newest version effective at {@code timestamp}, in epoch millis; an unknown one counts as
   * the epoch, which {@link #V1} is effective from.
   */
  public static ProvenanceAlgorithm effectiveAt(Long timestamp) {
    long at = timestamp != null ? timestamp : 0L;
    ProvenanceAlgorithm newest = V1;
    for (ProvenanceAlgorithm algorithm : values()) {
      if (algorithm.effectiveTimestamp <= at
          && algorithm.effectiveTimestamp >= newest.effectiveTimestamp) {
        newest = algorithm;
      }
    }
    return newest;
  }

  /**
   * Whether {@code name}, in any case, is {@link #DYNAMIC_NAME}.
   */
  public static boolean isDynamic(String name) {
    return DYNAMIC_NAME.equalsIgnoreCase(name);
  }

  /**
   * The version called {@code name}, in any case, or {@link #DEFAULT} when none is named.
   *
   * @throws IllegalArgumentException if no version has that name
   */
  public static ProvenanceAlgorithm of(String name) {
    if (name == null || name.isEmpty()) {
      return DEFAULT;
    }
    for (ProvenanceAlgorithm algorithm : values()) {
      if (algorithm.name.equals(name.toLowerCase(Locale.ROOT))) {
        return algorithm;
      }
    }
    throw new IllegalArgumentException("Unknown provenance algorithm '" + name + "'");
  }
}
