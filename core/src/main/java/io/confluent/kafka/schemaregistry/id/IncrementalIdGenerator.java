/*
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

package io.confluent.kafka.schemaregistry.id;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;

public class IncrementalIdGenerator implements IdGenerator {

  private int maxIdInKafkaStore = -1;

  @Override
  public int id(Schema schema) throws IdGenerationException {
    int nextId = Math.max(
        KafkaSchemaRegistry.MIN_VERSION,
        maxIdInKafkaStore + 1
    );
    return nextId;
  }

  @Override
  public void configure(SchemaRegistryConfig config) {

  }

  @Override
  public void init() throws IdGenerationException {

  }

  @Override
  public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
    if (maxIdInKafkaStore < schemaValue.getId()) {
      maxIdInKafkaStore = schemaValue.getId();
    }
  }
}
