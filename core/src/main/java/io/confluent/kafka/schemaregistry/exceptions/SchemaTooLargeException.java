/*
 * Copyright 2022 Confluent Inc.
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

public class SchemaTooLargeException extends SchemaRegistryException {

  public SchemaTooLargeException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaTooLargeException(String message) {
    super(message);
  }

  public SchemaTooLargeException(Throwable cause) {
    super(cause);
  }

  public SchemaTooLargeException() {
    super();
  }
}
