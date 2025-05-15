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

import io.confluent.rest.exceptions.RestException;
import javax.ws.rs.core.Response;

public class RestConflictException extends RestException {

  public static final int DEFAULT_ERROR_CODE =
      Response.Status.CONFLICT.getStatusCode();

  public RestConflictException(String message, int errorCode) {
    this(message, errorCode, null);
  }

  public RestConflictException(String message, int errorCode, Throwable cause) {
    super(message, DEFAULT_ERROR_CODE, errorCode, cause);
  }
}
