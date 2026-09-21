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
 * The format-specific half of an {@link Identity} — what distinguishes one entity from its peers
 * within a {@link Scope}.
 *
 * <p>Three shapes, each with different continuity behaviour:
 *
 * <ul>
 *   <li>{@link IntegerIdentity} — a Protobuf field number. Globally stable: renaming the field
 *       changes nothing, and reusing the number after a gap resolves to the same identity on a new
 *       presence interval.</li>
 *   <li>{@link StringIdentity} — a name that is stable on its own terms, with no alias mechanism
 *       behind it: a JSON Schema property, or a Protobuf message. Reusing the name after a gap
 *       likewise resolves to the same identity on a new interval, and a rename is simply a drop
 *       plus an add.</li>
 *   <li>{@link MintedIdentity} — a name resolved through an alias index, for Avro. Because an alias
 *       can reconnect to a historical identity, and because a name released by its holder must not
 *       be implicitly inherited, the minting version is folded into the value to keep independently
 *       minted identities apart.</li>
 * </ul>
 *
 * <p>Only {@link MintedIdentity} participates in the name resolution index; the other two are
 * computed directly from the entity and need no history to resolve.
 */
public interface FormatIdentity {
}
