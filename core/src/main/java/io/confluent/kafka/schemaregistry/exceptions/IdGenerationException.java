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

public class IdGenerationException extends SchemaRegistryException {

  public IdGenerationException(String message, Throwable cause) {
    super(message, cause);
  }

  public IdGenerationException(String message) {
    super(message);
  }

  public IdGenerationException(Throwable cause) {
    super(cause);
  }

  public IdGenerationException() {
    super();
  }
}
