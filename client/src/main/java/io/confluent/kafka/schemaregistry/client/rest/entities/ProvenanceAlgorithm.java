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
 */
public enum ProvenanceAlgorithm {

  // The first.
  V1("v1");

  /**
   * The version a schema version not stamped with one is matched by. A request that names none is
   * answered by each version's own, which never changes once registered, so no upgrade moves a
   * caller's pairings; until versions are stamped, that is this one throughout.
   */
  public static final ProvenanceAlgorithm DEFAULT = V1;

  private final String name;

  ProvenanceAlgorithm(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
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
