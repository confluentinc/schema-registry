/*
 * Copyright 2014-2019 Confluent Inc.
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
 * Indicates that the version is not a valid version id. Allowed values are between [1,
 * 2^31-1] and the string "latest"
 */
public class RestInvalidVersionException extends RestConstraintViolationException {

  public static final int ERROR_CODE = Errors.INVALID_VERSION_ERROR_CODE;
  public static final String INVALID_VERSION_MESSAGE_FORMAT = "The specified version '%s' "
          + "is not a valid version id. "
          + "Allowed values are between [1, 2^31-1] and the string \"latest\"";

  public RestInvalidVersionException(String version) {
    super(String.format(INVALID_VERSION_MESSAGE_FORMAT, version), ERROR_CODE);
  }

}
