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
 * Indicates an error while initializing schema registry
 */
public class SchemaRegistryInitializationException extends SchemaRegistryException {

  public SchemaRegistryInitializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaRegistryInitializationException(String message) {
    super(message);
  }

  public SchemaRegistryInitializationException(Throwable cause) {
    super(cause);
  }

  public SchemaRegistryInitializationException() {
    super();
  }
}
