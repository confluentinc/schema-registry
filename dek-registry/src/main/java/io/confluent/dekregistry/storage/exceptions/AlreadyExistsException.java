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

package io.confluent.dekregistry.storage.exceptions;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;

public class AlreadyExistsException extends SchemaRegistryException {

  public AlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public AlreadyExistsException(String message) {
    super(message);
  }

  public AlreadyExistsException(Throwable cause) {
    super(cause);
  }

  public AlreadyExistsException() {
    super();
  }
}
