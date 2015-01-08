/**
 * Copyright 2014 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.avro;

import io.confluent.rest.RestConfigException;

public enum AvroCompatibilityLevel {
  NONE("none", AvroCompatibilityChecker.NO_OP_CHECKER),
  BACKWARD("backward", AvroCompatibilityChecker.BACKWARD_CHECKER),
  FORWARD("forward", AvroCompatibilityChecker.FORWARD_CHECKER),
  FULL("full", AvroCompatibilityChecker.FULL_CHECKER);

  public final String name;
  public final AvroCompatibilityChecker compatibilityChecker;

  private AvroCompatibilityLevel(String name, AvroCompatibilityChecker compatibilityChecker) {
    this.name = name;
    this.compatibilityChecker = compatibilityChecker;
  }

  public static AvroCompatibilityLevel forName(String name) throws RestConfigException {
    if (name == null) {
      throw new RestConfigException("Null compatibility level is invalid");
    }
    if (NONE.name.equals(name)) {
      return NONE;
    } else if (BACKWARD.name.equals(name)) {
      return BACKWARD;
    } else if (FORWARD.name.equals(name)) {
      return FORWARD;
    } else if (FULL.name.equals(name)) {
      return FULL;
    } else {
      throw new RestConfigException("Unknown avro compatibility level: " + name);
    }
  }
}
