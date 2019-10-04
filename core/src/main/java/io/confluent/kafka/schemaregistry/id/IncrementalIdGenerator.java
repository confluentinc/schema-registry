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

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.masterelector.kafka.KafkaGroupMasterElector;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncrementalIdGenerator implements IdGenerator {

  Logger log = LoggerFactory.getLogger(KafkaGroupMasterElector.class);

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
  public int maxId(Integer currentId) {
    if (currentId > maxIdInKafkaStore) {
      log.debug("Requested ID is greater than max ID");
    }
    return maxIdInKafkaStore;
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
