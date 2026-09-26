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

  V1("v1");

  /**
   * The version a request that names none, or names {@link #LATEST_NAME}, is answered with.
   */
  public static final ProvenanceAlgorithm LATEST = V1;

  /**
   * The name asking for {@link #LATEST}, whichever version that is where the question is answered.
   */
  public static final String LATEST_NAME = "latest";

  private final String name;

  ProvenanceAlgorithm(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  /**
   * The version called {@code name}, in any case, or {@link #LATEST} when none is named or it is
   * {@link #LATEST_NAME}.
   *
   * @throws IllegalArgumentException if no version has that name
   */
  public static ProvenanceAlgorithm of(String name) {
    if (name == null || name.isEmpty() || LATEST_NAME.equalsIgnoreCase(name)) {
      return LATEST;
    }
    for (ProvenanceAlgorithm algorithm : values()) {
      if (algorithm.name.equals(name.toLowerCase(Locale.ROOT))) {
        return algorithm;
      }
    }
    throw new IllegalArgumentException("Unknown provenance algorithm '" + name + "'");
  }
}
