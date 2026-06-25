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

public class DekGenerationException extends SchemaRegistryException {

  // True if the failure was an authentication/authorization error from the KMS (a 401/403), which
  // is a caller/configuration problem and should be surfaced as a 4xx rather than a 5xx.
  private final boolean accessDenied;

  public DekGenerationException(String message, Throwable cause, boolean accessDenied) {
    super(message, cause);
    this.accessDenied = accessDenied;
  }

  public DekGenerationException(String message, Throwable cause) {
    this(message, cause, false);
  }

  public DekGenerationException(String message) {
    super(message);
    this.accessDenied = false;
  }

  public DekGenerationException(Throwable cause) {
    super(cause);
    this.accessDenied = false;
  }

  public DekGenerationException() {
    super();
    this.accessDenied = false;
  }

  public boolean isAccessDenied() {
    return accessDenied;
  }
}
