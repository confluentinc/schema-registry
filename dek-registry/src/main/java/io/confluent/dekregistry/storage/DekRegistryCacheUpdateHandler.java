/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dekregistry.storage;

import io.confluent.dekregistry.metrics.MetricsManager;
import io.kcache.CacheUpdateHandler;
import java.io.IOException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DekRegistryCacheUpdateHandler
    implements CacheUpdateHandler<EncryptionKeyId, EncryptionKey> {

  private static final Logger log = LoggerFactory.getLogger(DekRegistryCacheUpdateHandler.class);

  private MetricsManager metricsManager;

  public DekRegistryCacheUpdateHandler(MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
  }

  @Override
  public ValidationStatus validateUpdate(
      EncryptionKeyId key,
      EncryptionKey value,
      TopicPartition tp,
      long offset,
      long timestamp) {
    if (value != null) {
      // Store the offset and timestamp in the cached value
      value.setOffset(offset);
      value.setTimestamp(timestamp);
    }

    return ValidationStatus.SUCCESS;
  }

  /**
   * Invoked on every new schema written to the Kafka store
   *
   * @param key   Key associated with the schema.
   * @param value Value written to the Kafka lookupCache
   */
  @Override
  public void handleUpdate(
      EncryptionKeyId key,
      EncryptionKey value,
      EncryptionKey oldValue,
      TopicPartition tp,
      long offset,
      long timestamp) {
    String tenant = key.getTenant();
    if (value == null) {
      if (oldValue != null) {
        metricsManager.decrementKeyCount(tenant, key.getType());
      }
    } else if (oldValue == null) {
      metricsManager.incrementKeyCount(tenant, key.getType());
    }
  }

  @Override
  public void close() throws IOException {
  }
}
