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

import dev.cel.common.types.OpaqueType;

/**
 * Canonical CEL type-name labels for the extended types (Decimal, Timestamp, Variant)
 * plus their Java cel-java {@link OpaqueType} decls. The labels are the cross-language
 * spec — every client must use the same strings; the backing Java class is an
 * implementation detail of this client. Timestamp uses CEL's built-in
 * {@code SimpleType.TIMESTAMP} so there is no OpaqueType for it.
 */
public final class CelTypeLabels {

  /**
   * CEL type label for {@code confluent.type.Decimal}.
   */
  public static final String DECIMAL_NAME = "confluent.type.Decimal";

  /**
   * CEL type label for {@code google.protobuf.Timestamp} (= CEL built-in timestamp).
   */
  public static final String TIMESTAMP_NAME = "google.protobuf.Timestamp";

  /**
   * CEL type label for {@code confluent.type.Variant}.
   */
  public static final String VARIANT_NAME = "confluent.type.Variant";

  /**
   * Java cel-java type declaration for {@link #DECIMAL_NAME}.
   */
  public static final OpaqueType DECIMAL = OpaqueType.create(DECIMAL_NAME);

  /**
   * Java cel-java type declaration for {@link #VARIANT_NAME}.
   */
  public static final OpaqueType VARIANT = OpaqueType.create(VARIANT_NAME);

  private CelTypeLabels() {
  }
}
