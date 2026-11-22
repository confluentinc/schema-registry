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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

/**
 * What this package asks of a schema registry, so that a client of one and a registry's own code
 * can both answer. A subject is named as the registry names it, with its context if it has one.
 * A lookup that fails throws an unchecked exception of the implementation's.
 */
public interface TypeLookup {

  /** The latest live version of a subject, or null if it has none. */
  SchemaMetadata latest(String subject);

  /** A version of a subject, soft-deleted or not, or null if there is none. */
  SchemaMetadata version(String subject, int version);

  /** A stored version's schema, parsed in its subject's context with its references. */
  ParsedSchema parse(SchemaMetadata version);
}
