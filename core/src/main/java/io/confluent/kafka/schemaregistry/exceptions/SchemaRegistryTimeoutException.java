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
 * Indicates that some schema registry operation timed out.
 */
public class SchemaRegistryTimeoutException extends SchemaRegistryException {

  public SchemaRegistryTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaRegistryTimeoutException(String message) {
    super(message);
  }

  public SchemaRegistryTimeoutException(Throwable cause) {
    super(cause);
  }

  public SchemaRegistryTimeoutException() {
    super();
  }
}
