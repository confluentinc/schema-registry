/*
 * Copyright 2020 Confluent Inc.
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

/**
 * Indicates a reference exists to the given schema, so it cannot be deleted.
 */
public class RestReferenceExistsException extends RestConstraintViolationException {

  public static final int ERROR_CODE = Errors.REFERENCE_EXISTS_ERROR_CODE;
  public static final String REFERENCE_EXISTS_MESSAGE_FORMAT = "One or more references exist "
      + "to the schema %s.";

  public RestReferenceExistsException(String schemaKey) {
    super(String.format(REFERENCE_EXISTS_MESSAGE_FORMAT, schemaKey), ERROR_CODE);
  }
}
