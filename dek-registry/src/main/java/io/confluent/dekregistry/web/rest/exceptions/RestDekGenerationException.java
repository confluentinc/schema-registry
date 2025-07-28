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

import io.confluent.rest.exceptions.RestServerErrorException;

public class RestDekGenerationException extends RestServerErrorException {

  public static final int ERROR_CODE = DekRegistryErrors.DEK_GENERATION_ERROR_CODE;

  public RestDekGenerationException(String message) {
    super(message, ERROR_CODE);
  }
}
