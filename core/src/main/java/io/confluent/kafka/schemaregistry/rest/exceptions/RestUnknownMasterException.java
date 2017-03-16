/*
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest.exceptions;

/**
 * Indicates that the node that is asked to serve the request is not the current master and
 * is not aware of the master node to forward the request to
 */

import io.confluent.rest.exceptions.RestServerErrorException;

public class RestUnknownMasterException extends RestServerErrorException {

  private static final int ERROR_CODE = Errors.UNKNOWN_MASTER_ERROR_CODE;

  public RestUnknownMasterException(String message) {
    super(message, ERROR_CODE);
  }

  public RestUnknownMasterException(String message, Throwable cause) {
    super(message, ERROR_CODE, cause);
  }
}
