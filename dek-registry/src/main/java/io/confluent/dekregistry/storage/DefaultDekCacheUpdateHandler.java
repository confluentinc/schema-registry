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

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDekCacheUpdateHandler implements DekCacheUpdateHandler {

  private static final Logger log = LoggerFactory.getLogger(DefaultDekCacheUpdateHandler.class);

  private final DekRegistry dekRegistry;

  public DefaultDekCacheUpdateHandler(DekRegistry dekRegistry) {
    this.dekRegistry = dekRegistry;
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
        // Delete KEK/DEK case
        dekRegistry.getMetricsManager().decrementKeyCount(tenant, key.getType());
        if (oldValue instanceof KeyEncryptionKey) {
          KeyEncryptionKey oldKek = (KeyEncryptionKey) oldValue;
          if (oldKek.isShared()) {
            dekRegistry.getSharedKeys().remove(oldKek.getKmsKeyId(), (KeyEncryptionKeyId) key);
            dekRegistry.getMetricsManager().decrementSharedKeyCount(tenant);
          }
        }
      }
    } else if (oldValue == null) {
      // Add KEK/DEK case
      dekRegistry.getMetricsManager().incrementKeyCount(tenant, key.getType());
      if (value instanceof KeyEncryptionKey) {
        KeyEncryptionKey kek = (KeyEncryptionKey) value;
        if (kek.isShared()) {
          dekRegistry.getSharedKeys().put(kek.getKmsKeyId(), (KeyEncryptionKeyId) key);
          dekRegistry.getMetricsManager().incrementSharedKeyCount(tenant);
        }
      }
    } else {
      // Update KEK case
      if (value instanceof KeyEncryptionKey && oldValue instanceof KeyEncryptionKey) {
        KeyEncryptionKey kek = (KeyEncryptionKey) value;
        KeyEncryptionKey oldKek = (KeyEncryptionKey) oldValue;
        if (!oldKek.isShared() && kek.isShared()) {
          // Not Shared -> Shared
          dekRegistry.getSharedKeys().put(kek.getKmsKeyId(), (KeyEncryptionKeyId) key);
          dekRegistry.getMetricsManager().incrementSharedKeyCount(tenant);
        } else if (oldKek.isShared() && !kek.isShared()) {
          // Shared -> Not Shared
          dekRegistry.getSharedKeys().remove(oldKek.getKmsKeyId(), (KeyEncryptionKeyId) key);
          dekRegistry.getMetricsManager().decrementSharedKeyCount(tenant);
        }
      }
    }
  }

  @Override
  public void close() {
  }
}
