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

/**
 * Indicates that the node that is asked to serve the request is not the current leader and
 * is not aware of the leader node to forward the request to
 */

import io.confluent.rest.exceptions.RestServerErrorException;

public class RestUnknownLeaderException extends RestServerErrorException {

  private static final int ERROR_CODE = Errors.UNKNOWN_LEADER_ERROR_CODE;

  public RestUnknownLeaderException(String message) {
    super(message, ERROR_CODE);
  }

  public RestUnknownLeaderException(String message, Throwable cause) {
    super(message, ERROR_CODE, cause);
  }
}
