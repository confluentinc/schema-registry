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

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;

/**
 * Wraps the update handlers to drop the remembered lookup misses of the tenant of each record
 * read from the schemas topic, after the handlers have applied it, even if one of them throws.
 * The tenant is the one this is called under; null means every tenant.
 */
public class LookupMissInvalidator implements SchemaUpdateHandler {

  private final SchemaUpdateHandler delegate;
  private final LookupMissCache lookupMissCache;
  private final Supplier<String> tenant;

  public LookupMissInvalidator(SchemaUpdateHandler delegate, LookupMissCache lookupMissCache,
                               Supplier<String> tenant) {
    this.delegate = delegate;
    this.lookupMissCache = lookupMissCache;
    this.tenant = tenant;
  }

  @Override
  public void configure(Map<String, ?> map) {
    delegate.configure(map);
  }

  @Override
  public void init(Map<TopicPartition, Long> checkpoints) throws StoreInitializationException {
    delegate.init(checkpoints);
  }

  @Override
  public void cacheInitialized(Map<TopicPartition, Long> checkpoints) {
    delegate.cacheInitialized(checkpoints);
  }

  @Override
  public void startBatch(int count) {
    delegate.startBatch(count);
  }

  @Override
  public ValidationStatus validateUpdate(SchemaRegistryKey key, SchemaRegistryValue value,
                                         TopicPartition tp, long offset, long timestamp) {
    return delegate.validateUpdate(key, value, tp, offset, timestamp);
  }

  @Override
  public void handleUpdate(SchemaRegistryKey key,
                           SchemaRegistryValue value,
                           SchemaRegistryValue oldValue,
                           TopicPartition tp,
                           long offset,
                           long timestamp) {
    // Read first, so that a handler changing the thread's tenant cannot redirect the bump
    String recordTenant = tenant.get();
    try {
      delegate.handleUpdate(key, value, oldValue, tp, offset, timestamp);
    } finally {
      lookupMissCache.writeApplied(recordTenant);
    }
  }

  @Override
  public Map<TopicPartition, Long> checkpoint(int count) {
    return delegate.checkpoint(count);
  }

  @Override
  public void endBatch(int count) {
    delegate.endBatch(count);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
