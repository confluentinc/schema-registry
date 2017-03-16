/*
 * Copyright 2015 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.exceptions;

import io.confluent.rest.exceptions.RestConstraintViolationException;

/**
 * Indicates an invalid schema that does not conform to the expected format of the schema
 */
public class RestInvalidSchemaException extends RestConstraintViolationException {

  public static final int ERROR_CODE = Errors.INVALID_SCHEMA_ERROR_CODE;

  public RestInvalidSchemaException() {
    this("Invalid compatibility level. Valid values are none, backward, forward and full");
  }

  public RestInvalidSchemaException(String message) {
    super(message, ERROR_CODE);
  }
}
