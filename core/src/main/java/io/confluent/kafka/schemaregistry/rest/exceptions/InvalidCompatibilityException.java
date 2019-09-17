/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.rest.exceptions.RestException;

public class InvalidCompatibilityException extends RestException {

  private static final int UNPROCESSABLE_ENTITY_STATUS_CODE = 422;

  public InvalidCompatibilityException() {
    super("Invalid compatibility level. Valid values are none, backward, forward, full, "
                    + "backward_transitive, forward_transitive, and full_transitive",
          UNPROCESSABLE_ENTITY_STATUS_CODE,
          UNPROCESSABLE_ENTITY_STATUS_CODE);
  }

}
