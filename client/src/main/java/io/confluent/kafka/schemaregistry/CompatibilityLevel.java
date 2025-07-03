/*
 * Copyright 2014-2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry;

public enum CompatibilityLevel {
  NONE,
  BACKWARD,
  BACKWARD_TRANSITIVE,
  FORWARD,
  FORWARD_TRANSITIVE,
  FULL,
  FULL_TRANSITIVE;

  public final String name;

  private CompatibilityLevel() {
    this.name = name();
  }

  public static CompatibilityLevel forName(String name) {
    if (name == null) {
      return null;
    }

    name = name.toUpperCase();
    if (NONE.name.equals(name)) {
      return NONE;
    } else if (BACKWARD.name.equals(name)) {
      return BACKWARD;
    } else if (FORWARD.name.equals(name)) {
      return FORWARD;
    } else if (FULL.name.equals(name)) {
      return FULL;
    } else if (BACKWARD_TRANSITIVE.name.equals(name)) {
      return BACKWARD_TRANSITIVE;
    } else if (FORWARD_TRANSITIVE.name.equals(name)) {
      return FORWARD_TRANSITIVE;
    } else if (FULL_TRANSITIVE.name.equals(name)) {
      return FULL_TRANSITIVE;
    } else {
      return null;
    }
  }
}
