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
 * A {@link FormatIdentity} following a name that carries no alias mechanism — a JSON Schema
 * property, or a Protobuf message, which has no native rename continuity.
 */
public final class StringIdentity implements FormatIdentity {

  private final String value;

  StringIdentity(String value) {
    this.value = Objects.requireNonNull(value, "value");
  }

  /** The name. */
  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof StringIdentity && ((StringIdentity) o).value.equals(value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public String toString() {
    return value;
  }
}
