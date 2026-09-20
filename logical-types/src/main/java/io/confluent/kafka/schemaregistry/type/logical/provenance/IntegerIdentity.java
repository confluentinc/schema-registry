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

import io.confluent.kafka.schemaregistry.type.logical.Schema;

import java.util.Objects;

/**
 * A {@link FormatIdentity} following a Protobuf field number, recorded as
 * {@link Schema#PROTOBUF_FIELD_NUMBER} or derived positionally under
 * {@link IdentityPolicy#PROTOBUF}.
 */
public final class IntegerIdentity implements FormatIdentity {

  private final int value;

  IntegerIdentity(int value) {
    this.value = value;
  }

  /** The field number. */
  public int getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof IntegerIdentity && ((IntegerIdentity) o).value == value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return "#" + value;
  }
}
