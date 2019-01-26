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

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;

public class KafkaStoreMessageHandler
    implements StoreUpdateHandler<SchemaRegistryKey, SchemaRegistryValue> {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreMessageHandler.class);
  private final KafkaSchemaRegistry schemaRegistry;
  private final Store store;

  public KafkaStoreMessageHandler(KafkaSchemaRegistry schemaRegistry,
                                  Store store) {
    this.schemaRegistry = schemaRegistry;
    this.store = store;
  }

  /**
   * Invoked before every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   */
  public boolean validateUpdate(SchemaRegistryKey key, SchemaRegistryValue value) {
    if (key.getKeyType() == SchemaRegistryKeyType.SCHEMA) {
      SchemaValue schemaObj = (SchemaValue) value;
      if (schemaObj != null) {
        SchemaKey oldKey = schemaRegistry.guidToSchemaKey.get(schemaObj.getId());
        if (oldKey != null) {
          SchemaValue oldSchema;
          try {
            oldSchema = (SchemaValue) store.get(oldKey);
          } catch (StoreException e) {
            log.error("Error while retrieving schema", e);
            return false;
          }
          if (oldSchema != null && !oldSchema.equals(schemaObj.getSchema())) {
            log.error("Found a schema with duplicate ID {}", schemaObj.getId());
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Invoked on every new schema written to the Kafka store
   *
   * @param key   Key associated with the schema.
   * @param value Value written to the Kafka store
   */
  @Override
  public void handleUpdate(SchemaRegistryKey key, SchemaRegistryValue value) {
    if (key.getKeyType() == SchemaRegistryKeyType.SCHEMA) {
      handleSchemaUpdate((SchemaKey) key,
                         (SchemaValue) value);
    } else if (key.getKeyType() == SchemaRegistryKeyType.DELETE_SUBJECT) {
      handleDeleteSubject((DeleteSubjectValue) value);
    }
  }

  private void handleDeleteSubject(DeleteSubjectValue deleteSubjectValue) {
    //mark all versions as deleted in the local store
    String subject = deleteSubjectValue.getSubject();
    Integer deleteTillVersion = deleteSubjectValue.getVersion();
    for (int version = 1; version <= deleteTillVersion; version++) {
      try {

        SchemaKey schemaKey = new SchemaKey(subject, version);
        SchemaValue schemaValue = (SchemaValue) this.store.get(schemaKey);
        if (schemaValue != null) {
          schemaValue.setDeleted(true);
          this.store.put(schemaKey, schemaValue);
        }
      } catch (StoreException e) {
        log.error("Failed to delete subject in the local store");
      }
    }
  }

  private void handleSchemaUpdate(SchemaKey schemaKey, SchemaValue schemaObj) {
    if (schemaObj != null) {
      schemaRegistry.guidToSchemaKey.put(schemaObj.getId(), schemaKey);

      // Update the maximum id seen so far
      if (schemaRegistry.getMaxIdInKafkaStore() < schemaObj.getId()) {
        schemaRegistry.setMaxIdInKafkaStore(schemaObj.getId());
      }

      MD5 md5 = MD5.ofString(schemaObj.getSchema());
      SchemaIdAndSubjects schemaIdAndSubjects = schemaRegistry.schemaHashToGuid.get(md5);
      if (schemaIdAndSubjects == null) {
        schemaIdAndSubjects = new SchemaIdAndSubjects(schemaObj.getId());
      }
      schemaIdAndSubjects.addSubjectAndVersion(schemaKey.getSubject(), schemaKey.getVersion());
      schemaRegistry.schemaHashToGuid.put(md5, schemaIdAndSubjects);
    }
  }
}
