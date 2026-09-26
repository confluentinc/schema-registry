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
 * Which rules match a location to the previous version's, standing in for the source format.
 *
 * <p>The rules are format-specific, but a {@link LogicalType} carries no format discriminator, so
 * the caller states the format of each version.
 */
public enum IdentityPolicy {

  /**
   * Avro rules: a location matches by name, with aliases naming what the previous version called
   * it, and recorded field numbers ignored.
   */
  AVRO,

  /**
   * Protobuf rules: a member follows its field number, and a named type follows its name, which
   * Protobuf gives no way to alias — renaming a message breaks continuity natively.
   *
   * <p>Numbers are derived for a struct that records none. The derivation mirrors the reader's
   * all-or-nothing rule exactly: when no member of a struct records a number, the numbering was the
   * sequence the writer reproduces positionally, so the regular fields take 1..n in declaration
   * order and the {@code oneof} branches continue the sequence. A {@code oneof} container field
   * itself never has a number, and follows its members' numbers, so renaming it changes nothing.
   *
   * <p>Only sound for a Protobuf-derived sequence. Applied to an Avro one it would make identity
   * positional, and a legal field reorder would put two unrelated fields in correspondence.
   */
  PROTOBUF,

  /**
   * JSON Schema rules: a property follows its name, with no alias mechanism, so a rename is a drop
   * plus an add; a union branch follows its hint, else its content.
   */
  JSON;

  /**
   * The policy for a registry schema type — {@code AVRO}, {@code PROTOBUF} or {@code JSON}, as
   * {@code ParsedSchema.schemaType()} reports it.
   *
   * <p>Unknown types throw rather than defaulting, because silently guessing is exactly what this
   * avoids.
   */
  public static IdentityPolicy forSchemaType(String schemaType) {
    if (schemaType == null) {
      throw new IllegalArgumentException("No schema type given");
    }
    switch (schemaType) {
      case "AVRO":
        return AVRO;
      case "PROTOBUF":
        return PROTOBUF;
      case "JSON":
        return JSON;
      default:
        throw new IllegalArgumentException("Unsupported schema type: " + schemaType);
    }
  }
}
