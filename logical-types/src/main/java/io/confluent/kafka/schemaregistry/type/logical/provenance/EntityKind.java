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

/**
 * The kind of schema entity a {@link Provenance} value describes.
 *
 * <p>Part of both {@link Identity} and the name resolution index's key, so a named type and a
 * member of the same name in the same scope stay separate entities and neither can prune or
 * inherit the other's name mapping.
 *
 * <p>{@link #NAMED_TYPE} is the record of the Avro, Protobuf and JSON Schema models, widened
 * because a {@code LogicalType} named type may also be an enum. {@link #BRANCH} has no counterpart
 * in those models: it is the member of a {@code UNION}, which a {@code LogicalType} can express and
 * they cannot. It is kept distinct from {@link #FIELD} deliberately — retyping a {@code STRUCT}
 * into a {@code UNION} is too drastic a change for its members to stay in correspondence.
 *
 * <p>Only {@link #FIELD} and {@link #BRANCH} take part in the intersection used for data mapping.
 * A named type's provenance establishes its members' scope and tracks the container's own lifetime,
 * but a record and a field are different kinds of thing and intersecting them would be meaningless.
 */
public enum EntityKind {

  /**
   * An entry of {@code LogicalType.getNamedTypes()} — a record, message, enum or {@code $def}.
   */
  NAMED_TYPE,

  /**
   * A {@code STRUCT} field.
   */
  FIELD,

  /**
   * A {@code UNION} branch — a Protobuf {@code oneof} member or an Avro union member.
   */
  BRANCH;

  /** True for the kinds that carry data and therefore participate in projection. */
  public boolean isMember() {
    return this != NAMED_TYPE;
  }
}
