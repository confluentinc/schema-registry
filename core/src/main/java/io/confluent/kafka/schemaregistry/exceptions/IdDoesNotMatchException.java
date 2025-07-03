/*
 * Copyright 2014-2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.exceptions;

/**
 * Indicates a schema already exists with a different ID.
 */
public class IdDoesNotMatchException extends SchemaRegistryException {

  public IdDoesNotMatchException(int registeredId, int inputId) {
    super("Schema already registered with id " + registeredId + " instead of input id " + inputId);
  }

  public IdDoesNotMatchException(String message, Throwable cause) {
    super(message, cause);
  }

  public IdDoesNotMatchException(String message) {
    super(message);
  }

  public IdDoesNotMatchException(Throwable cause) {
    super(cause);
  }

  public IdDoesNotMatchException() {
    super();
  }
}
