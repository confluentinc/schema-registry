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

package io.confluent.kafka.schemaregistry.storage;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.id.IdGenerator;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.metrics.SchemaRegistryMetric;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStoreMessageHandler implements SchemaUpdateHandler {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreMessageHandler.class);
  private final KafkaSchemaRegistry schemaRegistry;
  private final LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache;
  private IdGenerator idGenerator;

  public KafkaStoreMessageHandler(KafkaSchemaRegistry schemaRegistry,
                                  LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache,
                                  IdGenerator idGenerator) {
    this.schemaRegistry = schemaRegistry;
    this.lookupCache = lookupCache;
    this.idGenerator = idGenerator;
  }

  /**
   * Invoked before every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   */
  @Override
  public ValidationStatus validateUpdate(SchemaRegistryKey key, SchemaRegistryValue value,
                                         TopicPartition tp, long offset, long timestamp) {
    if (key.getKeyType() == SchemaRegistryKeyType.SCHEMA) {
      SchemaValue schemaObj = (SchemaValue) value;
      if (schemaObj != null) {
        normalize(schemaObj);
        try {
          SchemaKey oldKey = lookupCache.schemaKeyById(schemaObj.getId());
          if (oldKey != null) {
            SchemaValue oldSchema;
            oldSchema = (SchemaValue) lookupCache.get(oldKey);
            if (oldSchema != null && !oldSchema.getSchema().equals(schemaObj.getSchema())) {
              log.error("Found a schema with duplicate ID {}.  This schema will not be "
                      + "registered since a schema already exists with this ID.",
                  schemaObj.getId());
              return schemaRegistry.isLeader()
                  ? ValidationStatus.ROLLBACK_FAILURE : ValidationStatus.IGNORE_FAILURE;
            }
          }
        } catch (StoreException e) {
          log.error("Error while retrieving schema", e);
          return schemaRegistry.isLeader()
              ? ValidationStatus.ROLLBACK_FAILURE : ValidationStatus.IGNORE_FAILURE;
        }
      }
    }
    return ValidationStatus.SUCCESS;
  }

  @VisibleForTesting
  protected static void normalize(SchemaValue schemaValue) {
    if (ProtobufSchema.TYPE.equals(schemaValue.getSchemaType())) {
      // Normalize the schema if it is Protobuf (due to changes in Protobuf canonicalization)
      String normalized = new ProtobufSchema(schemaValue.getSchema()).canonicalString();
      schemaValue.setSchema(normalized);
    }
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
                           TopicPartition tp,
                           long offset,
                           long timestamp) {
    if (key.getKeyType() == SchemaRegistryKeyType.SCHEMA) {
      handleSchemaUpdate((SchemaKey) key,
          (SchemaValue) value,
          (SchemaValue) oldValue);
    } else if (value == null) {
      // ignore non-schema tombstone
    } else if (key.getKeyType() == SchemaRegistryKeyType.DELETE_SUBJECT) {
      handleDeleteSubject((DeleteSubjectValue) value);
    } else if (key.getKeyType() == SchemaRegistryKeyType.CLEAR_SUBJECT) {
      handleClearSubject((ClearSubjectValue) value);
    }
  }

  private void handleDeleteSubject(DeleteSubjectValue deleteSubjectValue) {
    //mark all versions as deleted in the local lookupCache
    String subject = deleteSubjectValue.getSubject();
    Integer deleteTillVersion = deleteSubjectValue.getVersion();
    for (int version = 1; version <= deleteTillVersion; version++) {
      try {

        SchemaKey schemaKey = new SchemaKey(subject, version);
        SchemaValue schemaValue = (SchemaValue) this.lookupCache.get(schemaKey);
        if (schemaValue != null) {
          schemaValue.setDeleted(true);
          SchemaValue oldSchemaValue = (SchemaValue) lookupCache.put(schemaKey, schemaValue);
          lookupCache.schemaDeleted(schemaKey, oldSchemaValue);
        }
      } catch (StoreException e) {
        log.error("Failed to delete subject {} in the local cache", subject, e);
      }
    }
  }

  private void handleClearSubject(ClearSubjectValue clearSubjectValue) {
    String subject = clearSubjectValue.getSubject();
    try {
      lookupCache.clearSubjects(subject);
    } catch (StoreException e) {
      log.error("Failed to clear subject {} in the local cache", subject, e);
    }
  }

  private void handleSchemaUpdate(SchemaKey schemaKey,
                                  SchemaValue schemaValue,
                                  SchemaValue oldSchemaValue) {
    final MetricsContainer metricsContainer = schemaRegistry.getMetricsContainer();
    if (schemaValue != null) {
      // Update the maximum id seen so far
      idGenerator.schemaRegistered(schemaKey, schemaValue);

      if (schemaValue.isDeleted()) {
        lookupCache.schemaDeleted(schemaKey, schemaValue);
        updateMetrics(metricsContainer.getSchemasDeleted(),
                      metricsContainer.getSchemasDeleted(getSchemaType(schemaValue)));
      } else {
        lookupCache.schemaRegistered(schemaKey, schemaValue);
        updateMetrics(metricsContainer.getSchemasCreated(),
                      metricsContainer.getSchemasCreated(getSchemaType(schemaValue)));
      }
    } else {
      lookupCache.schemaTombstoned(schemaKey, oldSchemaValue);
    }
  }

  private static String getSchemaType(SchemaValue schemaValue) {
    return schemaValue.getSchemaType() == null ? AvroSchema.TYPE : schemaValue.getSchemaType();
  }

  private static void updateMetrics(SchemaRegistryMetric total, SchemaRegistryMetric perType) {
    total.record();
    if (perType != null) {
      perType.record();
    }
  }
}
