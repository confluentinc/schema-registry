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
 * Indicates an invalid subject name
 */
public class RestInvalidSubjectException extends RestConstraintViolationException {

  public static final int ERROR_CODE = Errors.INVALID_SUBJECT_ERROR_CODE;
  public static final String INVALID_SUBJECT_MESSAGE_FORMAT = "The specified subject '%s' "
      + "is not a valid.";

  public RestInvalidSubjectException(String subject) {
    super(String.format(INVALID_SUBJECT_MESSAGE_FORMAT, subject), ERROR_CODE);
  }

}
