/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.eventfeed.web.rest.exceptions;

import io.confluent.rest.exceptions.RestConstraintViolationException;

/**
 * Indicates an invalid CloudEvent that does not conform to the expected format
 */
public class RestInvalidCloudEventException extends RestConstraintViolationException {

  public static final int ERROR_CODE = Errors.INVALID_CLOUD_EVENT_ERROR_CODE;
  public static final String INVALID_CLOUD_EVENT_MESSAGE_FORMAT =
          "The cloud event specified an invalid value for property: '%s', detail: %s";

  public RestInvalidCloudEventException(String message) {
    super(message, ERROR_CODE);
  }

  public RestInvalidCloudEventException(String property, String detail) {
    this(String.format(INVALID_CLOUD_EVENT_MESSAGE_FORMAT, property, detail));
  }
}
