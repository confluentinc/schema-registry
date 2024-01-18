/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dekregistry.web.rest.exceptions;

import io.confluent.rest.exceptions.RestConstraintViolationException;

public class RestInvalidKeyException extends RestConstraintViolationException {

  public static final int ERROR_CODE = DekRegistryErrors.INVALID_KEY_ERROR_CODE;
  public static final String INVALID_OR_MISSING_KEY_MESSAGE_FORMAT =
      "Invalid or missing value for field '%s' of key";

  public RestInvalidKeyException(String field) {
    super(String.format(INVALID_OR_MISSING_KEY_MESSAGE_FORMAT, field), ERROR_CODE);
  }
}
