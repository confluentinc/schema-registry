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
 * Indicates that the version is not a valid version id. Allowed values are between [1,
 * 2^31-1] and the string "latest"
 */
public class InvalidVersionException extends SchemaRegistryException {

  public InvalidVersionException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidVersionException(String message) {
    super(message);
  }

  public InvalidVersionException(Throwable cause) {
    super(cause);
  }

  public InvalidVersionException() {
    super();
  }
}
