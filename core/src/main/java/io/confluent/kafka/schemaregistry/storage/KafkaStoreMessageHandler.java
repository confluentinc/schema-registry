/*
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.rest.entities.Config;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.resources.SchemaMd5AndSubject;

public class KafkaStoreMessageHandler
    implements StoreUpdateHandler<SchemaRegistryKey, SchemaRegistryValue> {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreMessageHandler.class);
  private final KafkaSchemaRegistry schemaRegistry;

  public KafkaStoreMessageHandler(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  /**
   * Invoked on every new schema written to the Kafka store
   *
   * @param key    Key associated with the schema.
   * @param schema Schema written to the Kafka store
   */
  @Override
  public void handleUpdate(SchemaRegistryKey key, SchemaRegistryValue schema) {
    // apply config updates
    if (key.getKeyType() == SchemaRegistryKeyType.CONFIG) {
      Config config = (Config) schema;
      if (config.getCompatibilityLevel() != null) {
        ConfigKey configKey = (ConfigKey) key;
        if (configKey.getSubject() != null) {
          log.info("Compatibility level for subject " + configKey.getSubject() + " updated to "
                   + config.getCompatibilityLevel().name);
        } else {
          log.info("Compatibility level updated to " + config.getCompatibilityLevel().name);
        }

      }
    } else if (key.getKeyType() == SchemaRegistryKeyType.SCHEMA) {
      Schema schemaObj = (Schema) schema;
      SchemaKey schemaKey = (SchemaKey) key;
      schemaRegistry.guidToSchemaKey.put(schemaObj.getId(), schemaKey);

      MD5 md5 = MD5.ofString(schemaObj.getSchema());
      SchemaMd5AndSubject schemaMd5AndSubject = new SchemaMd5AndSubject(schemaKey.getSubject(), md5);
      schemaRegistry.schemaHashToGuid.put(schemaMd5AndSubject, schemaObj.getId());
    }
  }
}
