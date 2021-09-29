/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.id;

import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;

/**
 * Internal interface used for generating id while registering schema.
 */
public interface IdGenerator {

  /**
   * Provides the id for the schema.
   *
   * @param schema the schema being registered; never {@code null}
   * @return the identifier for the schema; never {@code null}
   * @throws IdGenerationException exception when id can't be generated
   */
  int id(SchemaValue schema) throws IdGenerationException;

  /**
   * Configure the underlying generator.
   *
   * @param config Schema Registry Configs
   */
  void configure(SchemaRegistryConfig config);

  /**
   * Initialize the underlying generator.
   *
   * @throws IdGenerationException exception when IdGenerator can't be initialized
   */
  void init() throws IdGenerationException;

  /**
   * Returns the current max id from the generator
   *
   * @param schema current schema in the request to Schema Registry
   * @return the max id; never {@code null}
   */
  int getMaxId(SchemaValue schema);

  /**
   * Callback method that is invoked when a schema is registered.
   * IdGenerator implementation can optionally use this to update the next possible id.
   *
   * @param schemaKey the registered SchemaKey; never {@code null}
   * @param schemaValue the registered SchemaValue; never {@code null}
   */
  void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue);

}
