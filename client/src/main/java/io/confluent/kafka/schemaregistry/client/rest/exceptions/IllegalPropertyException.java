/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.rest.exceptions;

public class IllegalPropertyException extends RuntimeException {

  private final String propertyName;
  private final String detail;

  public IllegalPropertyException(final String propertyName, final String detail) {
    super("property: " + propertyName + "; detail: " + detail);
    this.propertyName = propertyName;
    this.detail = detail;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public String getDetail() {
    return detail;
  }
}