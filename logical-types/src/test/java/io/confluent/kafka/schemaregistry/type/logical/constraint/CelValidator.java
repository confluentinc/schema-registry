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

package io.confluent.kafka.schemaregistry.type.logical.constraint;

import io.confluent.kafka.schemaregistry.type.logical.Schema;

/**
 * Test-side strict cel-java type-check helpers. Thin wrappers around the
 * production {@link ConstraintCelChecker} that translate
 * {@link io.confluent.kafka.schemaregistry.type.logical.ValidationException}
 * into {@link AssertionError} for JUnit ergonomics.
 *
 * <p>Strict mode declares {@code this} with the schema's actual field
 * types (not {@code dyn}), so type/overload mismatches that the older
 * dyn-tolerant {@code assertValid} layer masked are caught here. Strict
 * has fully replaced the dyn-tolerant validator — keep this file lean.
 */
public final class CelValidator {

  private CelValidator() {
    // static utility
  }

  /**
   * Strict cel-java type-check against a real schema. Use for table-level
   * CHECK validation where {@code this} is the surrounding struct.
   */
  public static void assertValidStrict(String celExpr, Schema rootStruct) {
    if (rootStruct.getType() != Schema.Type.STRUCT) {
      throw new IllegalArgumentException(
          "Strict check requires a STRUCT root, got " + rootStruct.getType());
    }
    ConstraintValidationContext vctx =
        ConstraintValidationContext.tableLevel(rootStruct.getFields());
    try {
      ConstraintCelChecker.strictCheck(celExpr, vctx, null);
    } catch (io.confluent.kafka.schemaregistry.type.logical.ValidationException e) {
      throw new AssertionError(e.getMessage(), e);
    }
  }

  /**
   * Strict variant for column-level CHECK placement: {@code this} is the
   * field's value (or struct, for nested STRUCT columns), not the
   * surrounding row.
   */
  public static void assertValidStrictColumnLevel(
      String celExpr, String fieldName, Schema fieldSchema) {
    ConstraintValidationContext vctx =
        ConstraintValidationContext.columnLevel(fieldName, fieldSchema);
    try {
      ConstraintCelChecker.strictCheck(celExpr, vctx, null);
    } catch (io.confluent.kafka.schemaregistry.type.logical.ValidationException e) {
      throw new AssertionError(e.getMessage(), e);
    }
  }
}
