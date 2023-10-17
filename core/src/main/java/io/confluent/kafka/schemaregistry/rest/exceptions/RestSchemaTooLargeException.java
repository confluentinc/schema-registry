/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest.exceptions;

import io.confluent.rest.exceptions.RestConstraintViolationException;

public class RestSchemaTooLargeException extends RestConstraintViolationException {

  private static final int ERROR_CODE = Errors.SCHEMA_TOO_LARGE_ERROR_CODE;

  public RestSchemaTooLargeException(String message) {
    super(message, ERROR_CODE);
  }
}
