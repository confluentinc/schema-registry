/*
 * Copyright 2014-2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated  Use {@link io.confluent.kafka.schemaregistry.CompatibilityChecker} instead
 */
@Deprecated
public class AvroCompatibilityChecker {

  private static final Logger log = LoggerFactory.getLogger(AvroCompatibilityChecker.class);

  // Check if the new schema can be used to read data produced by the previous schema
  private static final SchemaValidator BACKWARD_VALIDATOR =
      new SchemaValidatorBuilder().canReadStrategy().validateLatest();
  public static final AvroCompatibilityChecker BACKWARD_CHECKER
      = new AvroCompatibilityChecker(BACKWARD_VALIDATOR);

  // Check if data produced by the new schema can be read by the previous schema
  private static final SchemaValidator FORWARD_VALIDATOR =
      new SchemaValidatorBuilder().canBeReadStrategy().validateLatest();
  public static final AvroCompatibilityChecker FORWARD_CHECKER
      = new AvroCompatibilityChecker(FORWARD_VALIDATOR);

  // Check if the new schema is both forward and backward compatible with the previous schema
  private static final SchemaValidator FULL_VALIDATOR =
      new SchemaValidatorBuilder().mutualReadStrategy().validateLatest();
  public static final AvroCompatibilityChecker FULL_CHECKER
      = new AvroCompatibilityChecker(FULL_VALIDATOR);

  // Check if the new schema can be used to read data produced by all earlier schemas
  private static final SchemaValidator BACKWARD_TRANSITIVE_VALIDATOR =
      new SchemaValidatorBuilder().canReadStrategy().validateAll();
  public static final AvroCompatibilityChecker BACKWARD_TRANSITIVE_CHECKER
      = new AvroCompatibilityChecker(BACKWARD_TRANSITIVE_VALIDATOR);

  // Check if data produced by the new schema can be read by all earlier schemas
  private static final SchemaValidator FORWARD_TRANSITIVE_VALIDATOR =
      new SchemaValidatorBuilder().canBeReadStrategy().validateAll();
  public static final AvroCompatibilityChecker FORWARD_TRANSITIVE_CHECKER
      = new AvroCompatibilityChecker(FORWARD_TRANSITIVE_VALIDATOR);

  // Check if the new schema is both forward and backward compatible with all earlier schemas
  private static final SchemaValidator FULL_TRANSITIVE_VALIDATOR =
      new SchemaValidatorBuilder().mutualReadStrategy().validateAll();
  public static final AvroCompatibilityChecker FULL_TRANSITIVE_CHECKER
      = new AvroCompatibilityChecker(FULL_TRANSITIVE_VALIDATOR);

  private static final SchemaValidator NO_OP_VALIDATOR = new SchemaValidator() {
    @Override
    public void validate(Schema schema, Iterable<Schema> schemas) throws SchemaValidationException {
      // do nothing
    }
  };
  public static final AvroCompatibilityChecker NO_OP_CHECKER = new AvroCompatibilityChecker(
      NO_OP_VALIDATOR);

  private final SchemaValidator validator;

  private AvroCompatibilityChecker(SchemaValidator validator) {
    this.validator = validator;
  }

  /**
   * Check the compatibility between the new schema and the latest schema
   */
  public boolean isCompatible(Schema newSchema, Schema latestSchema) {
    return isCompatible(newSchema, Collections.singletonList(latestSchema));
  }

  /**
   * Check the compatibility between the new schema and the specified schemas
   *
   * @param previousSchemas Full schema history in chronological order
   */
  public boolean isCompatible(Schema newSchema, List<Schema> previousSchemas) {
    List<Schema> previousSchemasCopy = new ArrayList<>(previousSchemas);
    try {
      // Validator checks in list order, but checks should occur in reverse chronological order
      Collections.reverse(previousSchemasCopy);
      validator.validate(newSchema, previousSchemasCopy);
    } catch (SchemaValidationException e) {
      return false;
    } catch (Exception e) {
      log.error("Unexpected exception during compatibility check", e);
      return false;
    }

    return true;
  }
}
