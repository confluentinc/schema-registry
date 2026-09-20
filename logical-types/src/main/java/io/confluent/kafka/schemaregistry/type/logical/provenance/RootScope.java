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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;

/**
 * The top-level {@link Scope}: the qualified names of {@link LogicalType#getNamedTypes()}, and the
 * members of an anonymous root schema.
 *
 * <p>Both live here, and they cannot collide because a name is resolved per
 * {@link EntityKind} — a named type is a {@link EntityKind#NAMED_TYPE} and a root member is a
 * {@link EntityKind#FIELD} or {@link EntityKind#BRANCH}.
 */
public final class RootScope implements Scope {

  /** The single root scope instance. */
  public static final RootScope INSTANCE = new RootScope();

  private RootScope() {
  }

  @Override
  public String toString() {
    return "$root";
  }
}
