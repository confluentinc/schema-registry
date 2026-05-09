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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

/**
 * Canonical CEL type-name labels for the extended types (Decimal, Timestamp, Variant).
 * Each label is a stable identifier used as the {@code Decls.newObjectType(...)} string
 * across all clients — the strings are part of the cross-language spec, the backing Java
 * classes are an implementation detail of this client.
 */
public final class CelTypes {

  /**
   * CEL type label for {@code confluent.type.Decimal}. Same string in every client.
   */
  public static final String DECIMAL = "confluent.type.Decimal";
  public static final String TIMESTAMP = "google.protobuf.Timestamp";
  public static final String VARIANT = "confluent.type.Variant";

  private CelTypes() {
  }
}
