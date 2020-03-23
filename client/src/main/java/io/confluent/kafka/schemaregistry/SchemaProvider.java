/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry;

import org.apache.kafka.common.Configurable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

/**
 * A factory for parsed schemas.
 *
 * <p>This factory produces instances of {@link io.confluent.kafka.schemaregistry.ParsedSchema}.
 * To have schema registry use a specific factory, it's fully qualified class name needs to be
 * specified using the configuration property <code>schema.providers</code>.
 */
public interface SchemaProvider extends Configurable {

  String SCHEMA_VERSION_FETCHER_CONFIG = "schemaVersionFetcher";

  default void configure(Map<String, ?> configs) {
  }

  /**
   * Returns the schema type.
   *
   * @return the schema type
   */
  String schemaType();

  /**
   * Parses a string representing a schema.
   *
   * @param schemaString the schema
   * @param references a list of schema references
   * @return an optional parsed schema
   */
  Optional<ParsedSchema> parseSchema(String schemaString, List<SchemaReference> references);
}
