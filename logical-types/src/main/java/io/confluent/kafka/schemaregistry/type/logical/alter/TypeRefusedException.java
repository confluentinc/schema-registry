/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.type.logical.alter;

/**
 * A statement about a type that this package refuses, with the reason as its message. It is
 * not a {@code ValidationException}, which the logical types parser and converters throw, so
 * that code here can tell its own refusals from theirs.
 */
public class TypeRefusedException extends RuntimeException {

  public TypeRefusedException(String message) {
    super(message);
  }

  public TypeRefusedException(String message, Throwable cause) {
    super(message, cause);
  }
}
