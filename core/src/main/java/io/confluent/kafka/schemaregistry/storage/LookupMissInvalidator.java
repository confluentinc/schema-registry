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

import org.apache.kafka.common.TopicPartition;

/**
 * Drops the remembered lookup misses of the tenant of each record read from the schemas topic.
 * It must run after every other handler, so that a lookup that sees the new count also sees
 * the record applied; the tenant is the one the handlers run under, null meaning every tenant.
 */
public class LookupMissInvalidator implements SchemaUpdateHandler {

  private final SchemaRegistry schemaRegistry;
  private final LookupMissCache lookupMissCache;

  public LookupMissInvalidator(SchemaRegistry schemaRegistry, LookupMissCache lookupMissCache) {
    this.schemaRegistry = schemaRegistry;
    this.lookupMissCache = lookupMissCache;
  }

  @Override
  public void handleUpdate(SchemaRegistryKey key,
                           SchemaRegistryValue value,
                           SchemaRegistryValue oldValue,
                           TopicPartition tp,
                           long offset,
                           long timestamp) {
    if (key.getKeyType() != SchemaRegistryKeyType.NOOP) {
      lookupMissCache.writeApplied(schemaRegistry.tenant());
    }
  }
}
