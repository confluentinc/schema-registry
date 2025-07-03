/*
 * Copyright 2014-2020 Confluent Inc.
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

import javax.ws.rs.core.Response;

import io.confluent.rest.exceptions.RestException;

/**
 * An exception thrown when the registered schema is not compatible with the latest schema according
 * to the compatibility level.
 */
public class RestIncompatibleSchemaException extends RestException {

  public static final int DEFAULT_ERROR_CODE =
      Response.Status.CONFLICT.getStatusCode();

  public RestIncompatibleSchemaException(String message, int errorCode) {
    this(message, errorCode, null);
  }

  public RestIncompatibleSchemaException(String message, int errorCode, Throwable cause) {
    super(message, Response.Status.CONFLICT.getStatusCode(), errorCode, cause);
  }
}
