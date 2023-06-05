/*
 * Copyright 2018 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CompatibilityChecker {

  // Check if the new schema can be used to read data produced by the previous schema
  private static final SchemaValidator BACKWARD_VALIDATOR =
      new SchemaValidatorBuilder().canReadStrategy()
      .validateLatest();
  public static final CompatibilityChecker BACKWARD_CHECKER = new CompatibilityChecker(
      BACKWARD_VALIDATOR);

  // Check if data produced by the new schema can be read by the previous schema
  private static final SchemaValidator FORWARD_VALIDATOR =
      new SchemaValidatorBuilder().canBeReadStrategy()
      .validateLatest();
  public static final CompatibilityChecker FORWARD_CHECKER = new CompatibilityChecker(
      FORWARD_VALIDATOR);

  // Check if the new schema is both forward and backward compatible with the previous schema
  private static final SchemaValidator FULL_VALIDATOR =
      new SchemaValidatorBuilder().mutualReadStrategy()
      .validateLatest();
  public static final CompatibilityChecker FULL_CHECKER = new CompatibilityChecker(FULL_VALIDATOR);

  // Check if the new schema can be used to read data produced by all earlier schemas
  private static final SchemaValidator BACKWARD_TRANSITIVE_VALIDATOR =
      new SchemaValidatorBuilder().canReadStrategy()
      .validateAll();
  public static final CompatibilityChecker BACKWARD_TRANSITIVE_CHECKER = new CompatibilityChecker(
      BACKWARD_TRANSITIVE_VALIDATOR);

  // Check if data produced by the new schema can be read by all earlier schemas
  private static final SchemaValidator FORWARD_TRANSITIVE_VALIDATOR =
      new SchemaValidatorBuilder().canBeReadStrategy()
      .validateAll();
  public static final CompatibilityChecker FORWARD_TRANSITIVE_CHECKER = new CompatibilityChecker(
      FORWARD_TRANSITIVE_VALIDATOR);

  // Check if the new schema is both forward and backward compatible with all earlier schemas
  private static final SchemaValidator FULL_TRANSITIVE_VALIDATOR =
      new SchemaValidatorBuilder().mutualReadStrategy()
      .validateAll();
  public static final CompatibilityChecker FULL_TRANSITIVE_CHECKER = new CompatibilityChecker(
      FULL_TRANSITIVE_VALIDATOR);

  private static final SchemaValidator NO_OP_VALIDATOR =
      (schema, schemas) -> Collections.emptyList();

  public static final CompatibilityChecker NO_OP_CHECKER =
      new CompatibilityChecker(NO_OP_VALIDATOR);

  private final SchemaValidator validator;

  private CompatibilityChecker(SchemaValidator validator) {
    this.validator = validator;
  }

  // visible for testing
  public List<String> isCompatible(
      ParsedSchema newSchema, List<? extends ParsedSchema> previousSchemas
  ) {
    return isCompatibleWithHolders(newSchema, previousSchemas.stream()
        .map(SimpleParsedSchemaHolder::new)
        .collect(Collectors.toCollection(ArrayList::new)));
  }

  public List<String> isCompatibleWithHolders(
      ParsedSchema newSchema, List<ParsedSchemaHolder> previousSchemas
  ) {
    List<ParsedSchemaHolder> previousSchemasCopy = new ArrayList<>(previousSchemas);
    // Validator checks in list order, but checks should occur in reverse chronological order
    Collections.reverse(previousSchemasCopy);
    return validator.validate(newSchema, previousSchemasCopy);
  }

  public static CompatibilityChecker checker(CompatibilityLevel level) {
    switch (level) {
      case NONE:
        return CompatibilityChecker.NO_OP_CHECKER;
      case BACKWARD:
        return CompatibilityChecker.BACKWARD_CHECKER;
      case BACKWARD_TRANSITIVE:
        return CompatibilityChecker.BACKWARD_TRANSITIVE_CHECKER;
      case FORWARD:
        return CompatibilityChecker.FORWARD_CHECKER;
      case FORWARD_TRANSITIVE:
        return CompatibilityChecker.FORWARD_TRANSITIVE_CHECKER;
      case FULL:
        return CompatibilityChecker.FULL_CHECKER;
      case FULL_TRANSITIVE:
        return CompatibilityChecker.FULL_TRANSITIVE_CHECKER;
      default:
        throw new IllegalArgumentException("Invalid level " + level);
    }
  }
}
