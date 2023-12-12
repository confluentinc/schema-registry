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

import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.KMS_TYPE_SUFFIX;

import io.confluent.kafka.schemaregistry.encryption.tink.KmsClients;
import java.util.Objects;
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
        dekRegistry.getMetricsManager().decrementKeyCount(tenant, key.getType());
      }
    } else if (oldValue == null) {
      dekRegistry.getMetricsManager().incrementKeyCount(tenant, key.getType());
    }
    if (value instanceof KeyEncryptionKey && oldValue instanceof KeyEncryptionKey) {
      KeyEncryptionKey kek = (KeyEncryptionKey) value;
      KeyEncryptionKey oldKek = (KeyEncryptionKey) oldValue;
      if (!Objects.equals(kek.getKmsProps(), oldKek.getKmsProps())) {
        String kekUrl = kek.getKmsType() + KMS_TYPE_SUFFIX + kek.getKmsKeyId();
        KmsClients.remove(kekUrl);
      }
    }
  }

  @Override
  public void close() {
  }
}
