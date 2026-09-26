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
 * A schema that refers to itself, directly or through other named types, and so has no finite
 * inlining. Every consumer of inlined paths — an Iceberg schema, a Flink row type — needs one, so
 * such a schema has no provenance report at all.
 */
public class RecursiveTypeException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public RecursiveTypeException(String qualifiedName) {
    super("Cannot inline a recursive named type: " + qualifiedName);
  }
}
