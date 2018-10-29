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

import java.util.Map;

public interface SchemaProvider extends Configurable {

  default void configure(Map<String, ?> var1) {
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
   * @return a parsed schema, or null if the schema could not be parsed
   */
  ParsedSchema parseSchema(String schemaString);
}
