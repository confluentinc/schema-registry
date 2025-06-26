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

package io.confluent.dpregistry.storage;

import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDataProductCacheUpdateHandler implements DataProductCacheUpdateHandler {

  private static final Logger log = LoggerFactory.getLogger(
      DefaultDataProductCacheUpdateHandler.class);

  private final DataProductRegistry dataProductRegistry;

  public DefaultDataProductCacheUpdateHandler(DataProductRegistry dataProductRegistry) {
    this.dataProductRegistry = dataProductRegistry;
  }

  public KafkaSchemaRegistry getSchemaRegistry() {
    return dataProductRegistry.getSchemaRegistry();
  }

  @Override
  public ValidationStatus validateUpdate(
      DataProductKey key,
      DataProductValue value,
      TopicPartition tp,
      long offset,
      long timestamp) {
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
      DataProductKey key,
      DataProductValue value,
      DataProductValue oldValue,
      TopicPartition tp,
      long offset,
      long timestamp) {
    // TODO RAY metrics
    /*
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
     */
    String name = key.getName();
    Integer version = key.getVersion();
    SchemaValue keySchema = null;
    SchemaValue oldKeySchema = null;
    SchemaValue valueSchema = null;
    SchemaValue oldValueSchema = null;
    if (value != null && value.getSchemas() != null) {
      DataProductSchemas schemas = value.getSchemas();
      keySchema = schemas.getKey();
      valueSchema = schemas.getValue();
    }
    if (oldValue != null && oldValue.getSchemas() != null) {
      DataProductSchemas schemas = oldValue.getSchemas();
      oldKeySchema = schemas.getKey();
      oldValueSchema = schemas.getValue();
    }
    // For now we use subject names
    handleSchemaUpdate(name + "-key", version, keySchema, oldKeySchema);
    handleSchemaUpdate(name + "-value", version, valueSchema, oldValueSchema);
  }

  private void handleSchemaUpdate(
      String subject,
      Integer version,
      SchemaValue schemaValue,
      SchemaValue oldSchemaValue) {
    try {
      getSchemaRegistry().getKafkaStore().put(
          new SchemaKey(subject, version),
          schemaValue
      );
    } catch (StoreException e) {
      throw new RuntimeException(e);
    }
    // TODO RAY metrics
    //final MetricsContainer metricsContainer = schemaRegistry.getMetricsContainer();
    /*
    SchemaKey schemaKey = new SchemaKey(subject, version);
    if (schemaValue != null) {
      // Update the maximum id seen so far
      getSchemaRegistry().getIdentityGenerator().schemaRegistered(schemaKey, schemaValue);

      if (schemaValue.isDeleted()) {
        getSchemaRegistry().getLookupCache().schemaDeleted(schemaKey, schemaValue, oldSchemaValue);
      } else {
        getSchemaRegistry().getLookupCache().schemaRegistered(
            schemaKey, schemaValue, oldSchemaValue);
      }
    } else if (oldSchemaValue != null) {
      getSchemaRegistry().getLookupCache().schemaTombstoned(schemaKey, oldSchemaValue);
    }
     */
  }

  @Override
  public void close() {
  }
}
