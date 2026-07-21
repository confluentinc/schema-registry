/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;

/**
 * An extension of {@link ParsedSchemaHolder} that also provides access to the underlying
 * {@link SchemaValue}. This interface allows code that needs the schema value (such as
 * {@link AbstractSchemaRegistry#maybePopulateFromPrevious}) to work with any implementation
 * that can provide schema values, not just {@link LazyParsedSchemaHolder}.
 */
public interface SchemaValueHolder extends ParsedSchemaHolder {

  /**
   * Returns the schema value.
   *
   * @return the schema value
   * @throws SchemaRegistryException if the schema value cannot be retrieved
   */
  SchemaValue schemaValue() throws SchemaRegistryException;
}
