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
 * The hierarchical context a name or number is resolved in, so that {@code User.name} and
 * {@code Order.name} cannot collide.
 *
 * <p>There are three kinds. {@link RootScope} is the top level, holding the named type definitions
 * and the members of an anonymous root schema. An {@link Identity} is itself a scope — the members
 * of an entity's type are scoped by that entity's resolved identity, which is what lets a renamed
 * record keep its children: the record's identity survives the rename through its aliases, so its
 * members' scope is unchanged. {@link StepScope} covers the gap between the two.
 *
 * <p><b>Why {@link StepScope} exists.</b> An Avro, Protobuf or JSON Schema entity tree alternates
 * strictly between records and members, so a member's scope is always its parent's identity. A
 * {@code LogicalType} interposes unnamed type nodes — an array element, a map key, a map value —
 * between an entity and the members underneath it. Without a step, the key and value of a
 * {@code MAP<STRUCT, STRUCT>} would put their fields in one scope and collide. The steps also stay
 * descriptive rather than collapsing to index positions ({@code []} and <code>{key}</code> are both
 * index {@code 0}) so that retyping a field from {@code ARRAY<STRUCT>} to {@code MAP<K, STRUCT>}
 * moves the inner members to a new scope and correctly starts a new presence interval.
 */
public interface Scope {
}
