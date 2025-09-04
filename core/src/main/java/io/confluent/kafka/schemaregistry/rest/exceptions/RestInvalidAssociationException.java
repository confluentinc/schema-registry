/*
 * Copyright 2021 Confluent Inc.
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
 * Indicates an invalid association property value
 */
public class RestInvalidAssociationException extends RestConstraintViolationException {

  public static final int ERROR_CODE = Errors.INVALID_ASSOCIATION_ERROR_CODE;
  public static final String INVALID_ASSOCIATION_MESSAGE_FORMAT =
      "The association specified an invalid value for property: '%s', reason: %s";

  public RestInvalidAssociationException(String propertyName, String reason) {
    super(String.format(INVALID_ASSOCIATION_MESSAGE_FORMAT, propertyName, reason), ERROR_CODE);
  }

}
