/*
 * Copyright 2020 Confluent Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeStoreUpdateHandler
    implements StoreUpdateHandler<SchemaRegistryKey, SchemaRegistryValue> {

  private static final Logger log = LoggerFactory.getLogger(CompositeStoreUpdateHandler.class);

  private final StoreUpdateHandler<SchemaRegistryKey, SchemaRegistryValue>[] handlers;

  public CompositeStoreUpdateHandler(
      StoreUpdateHandler<SchemaRegistryKey, SchemaRegistryValue> ...handlers) {
    this.handlers = handlers;
  }

  /**
   * Invoked before every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   */
  public boolean validateUpdate(SchemaRegistryKey key, SchemaRegistryValue value, long timestamp) {
    for (StoreUpdateHandler<SchemaRegistryKey, SchemaRegistryValue> handler : handlers) {
      boolean valid = handler.validateUpdate(key, value, timestamp);
      if (!valid) {
        return false;
      }
    }
    return true;
  }

  /**
   * Invoked on every new schema written to the Kafka store
   *
   * @param key   Key associated with the schema.
   * @param value Value written to the Kafka lookupCache
   */
  @Override
  public void handleUpdate(SchemaRegistryKey key,
                           SchemaRegistryValue value,
                           SchemaRegistryValue oldValue,
                           long timestamp) {
    for (StoreUpdateHandler<SchemaRegistryKey, SchemaRegistryValue> handler : handlers) {
      handler.handleUpdate(key, value, oldValue, timestamp);
    }
  }
}
