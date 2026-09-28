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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

/**
 * A history whose names and aliases do not determine one identity per location — two fields
 * aliasing one old name, a type aliasing two old types, an alias that is its own name. A property
 * of the registered schemas, so a caller cannot have provenance for it and should stop asking.
 */
public class AmbiguousProvenanceException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public AmbiguousProvenanceException(String message) {
    super(message);
  }
}
