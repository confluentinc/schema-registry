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
import io.confluent.kafka.schemaregistry.type.logical.Schema;

/**
 * Which signal establishes an entity's logical identity, standing in for the source format.
 *
 * <p>The identity rules are format-specific, but a {@link LogicalType} carries no format
 * discriminator. {@link #AUTO} recovers what it can from the data — a member carrying a
 * {@link Schema#PROTOBUF_FIELD_NUMBER} follows that number, anything else follows its name and
 * {@link Schema#AVRO_ALIASES} — and the three named policies let a caller who knows the source
 * state it instead.
 */
public enum IdentityPolicy {

  /**
   * Use the recorded Protobuf field number when a member has one, otherwise the member's name and
   * aliases. Safe for a sequence of unknown or mixed provenance, and the default.
   *
   * <p><b>Known hole.</b> The Protobuf reader records field numbers all-or-nothing per message and
   * records none when the message's numbering is trivially sequential (see
   * {@code Schema.Field#getFieldNumber()}). A rename inside such a message therefore reaches LT
   * with no number on either version, and {@code AUTO} reads it as a drop plus an add. Use
   * {@link #PROTOBUF} when the sequence is known to be Protobuf-derived.
   */
  AUTO,

  /**
   * Avro rules: every entity is identified by name, with aliases establishing continuity across a
   * rename, and recorded field numbers ignored. Identities are {@link MintedIdentity}, so a name
   * released by its holder and later reused mints a distinct identity.
   */
  AVRO,

  /**
   * Protobuf rules: a member follows its field number, and a named type follows its name, which
   * Protobuf gives no way to alias — renaming a message breaks identity continuity natively.
   *
   * <p>Numbers are derived for a struct that records none. The derivation mirrors the reader's
   * all-or-nothing rule exactly: when no member of a struct records a number, the numbering was the
   * sequence the writer reproduces positionally, so the regular fields take 1..n in declaration
   * order and the {@code oneof} branches continue the sequence. A {@code oneof} container field
   * itself never has a number and falls back to its name, which is right — a oneof's identity is
   * its name.
   *
   * <p>Only sound for a Protobuf-derived sequence. Applied to an Avro one it would make identity
   * positional, and a legal field reorder would put two unrelated fields in correspondence.
   */
  PROTOBUF,

  /**
   * JSON Schema rules: every entity is identified by its property name, with no alias mechanism, so
   * a rename is a drop plus an add. Identities are {@link StringIdentity} and therefore stable:
   * reusing a name after a gap resolves to the same identity, on a new presence interval.
   */
  JSON
}
