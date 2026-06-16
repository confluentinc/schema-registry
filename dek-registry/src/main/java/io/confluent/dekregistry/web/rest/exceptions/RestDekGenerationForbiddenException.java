/*
 * Copyright 2025 Confluent Inc.
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
import jakarta.ws.rs.core.Response;

public class RestDekGenerationForbiddenException extends RestException {

  public static final int DEFAULT_ERROR_CODE =
      Response.Status.FORBIDDEN.getStatusCode();

  public RestDekGenerationForbiddenException(String message) {
    this(message, DekRegistryErrors.DEK_GENERATION_FORBIDDEN_ERROR_CODE, null);
  }

  public RestDekGenerationForbiddenException(String message, int errorCode, Throwable cause) {
    super(message, DEFAULT_ERROR_CODE, errorCode, cause);
  }
}
