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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.id.IdGenerator;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.metrics.SchemaRegistryMetric;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

public class KafkaStoreMessageHandler implements SchemaUpdateHandler {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreMessageHandler.class);
  private final KafkaSchemaRegistry schemaRegistry;
  private final LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache;
  private IdGenerator idGenerator;
  private File backupFile;
  private List<String> backupSubjectsBlacklist;

  public KafkaStoreMessageHandler(KafkaSchemaRegistry schemaRegistry,
                                  LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache,
                                  IdGenerator idGenerator,
                                  String backupsDir,
                                  String backupFilePrefix) {
    this.schemaRegistry = schemaRegistry;
    this.lookupCache = lookupCache;
    this.idGenerator = idGenerator;
    this.backupFile = new File(backupsDir + "/" + backupFilePrefix + ".txt");
    if (schemaRegistry.config().getBoolean(SchemaRegistryConfig.WRITE_BACKUPS)) {
      try {
        File directory = new File(backupsDir);
        if (!directory.exists()) {
          if (!directory.mkdir()) {
            log.error("failed to create backup directory: " + directory.getAbsolutePath());
            schemaRegistry.getMetricsContainer().getBackupDirectoryCreationFailure().increment();
          } else {
            log.info("created new directory: " + directory.getAbsolutePath());
          }
        }
        if (!backupFile.exists()) {
          if (!backupFile.createNewFile()) {
            log.error("failed to create backup file: " + backupFile.getAbsolutePath());
            schemaRegistry.getMetricsContainer().getBackupFileCreationFailure().increment();
          } else {
            log.info("created new file: " + backupFile.getAbsolutePath());
          }
        }
        backupSubjectsBlacklist = Arrays.asList(schemaRegistry.config()
                .getString(SchemaRegistryConfig.BACKUPS_SUBJECT_BLACKLIST)
                .split(" "));
      } catch (IOException e) {
        log.error("io exception when trying to create backup file: "
                + backupFile.getAbsolutePath());
        schemaRegistry.getMetricsContainer().getBackupFileWriteFailure().increment();
      }
    }
  }

  /**
   * Invoked before every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   */
  public boolean validateUpdate(SchemaRegistryKey key, SchemaRegistryValue value,
                                TopicPartition tp, long offset, long timestamp) {
    if (key.getKeyType() == SchemaRegistryKeyType.SCHEMA) {
      SchemaValue schemaObj = (SchemaValue) value;
      if (schemaObj != null) {
        SchemaKey oldKey = lookupCache.schemaKeyById(schemaObj.getId());
        if (oldKey != null) {
          SchemaValue oldSchema;
          try {
            oldSchema = (SchemaValue) lookupCache.get(oldKey);
          } catch (StoreException e) {
            log.error("Error while retrieving schema", e);
            return false;
          }
          if (oldSchema != null && !oldSchema.getSchema().equals(schemaObj.getSchema())) {
            log.error("Found a schema with duplicate ID {}.  This schema will not be "
                    + "registered since a schema already exists with this ID.",
                schemaObj.getId());
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
   * @param value Value written to the Kafka lookupCache
   */
  @Override
  public void handleUpdate(SchemaRegistryKey key,
                           SchemaRegistryValue value,
                           SchemaRegistryValue oldValue,
                           TopicPartition tp,
                           long offset,
                           long timestamp) {
    if (key.getKeyType() != SchemaRegistryKeyType.SCHEMA && value == null) {
      // ignore non-schema tombstone
      return;
    }

    if (key.getKeyType() == SchemaRegistryKeyType.SCHEMA) {
      handleSchemaUpdate((SchemaKey) key,
          (SchemaValue) value,
          (SchemaValue) oldValue);
    } else if (key.getKeyType() == SchemaRegistryKeyType.DELETE_SUBJECT) {
      handleDeleteSubject((DeleteSubjectValue) value);
    } else if (key.getKeyType() == SchemaRegistryKeyType.CLEAR_SUBJECT) {
      handleClearSubject((ClearSubjectValue) value);
    }
    if (schemaRegistry.config().getBoolean(SchemaRegistryConfig.WRITE_BACKUPS)) {
      recordBackup(key, value, tp, timestamp);
    }
  }

  private void recordBackup(SchemaRegistryKey key,
                            SchemaRegistryValue value,
                            TopicPartition tp,
                            long timestamp) {
    boolean ignoreSubject = false;
    if (key instanceof SubjectKey) {
      String subject = ((SubjectKey) key).getSubject();
      if (subject != null) {
        int index = subject.indexOf("_");
        if (index != -1) {
          String tenantRemovedSubject = subject.substring(index + 1);
          ignoreSubject = backupSubjectsBlacklist.contains(tenantRemovedSubject);
        }
      }
    }
    if (key.getKeyType() != SchemaRegistryKeyType.NOOP && !ignoreSubject) {
      try {
        FileOutputStream fileStream = new FileOutputStream(backupFile, true);
        OutputStreamWriter fr = new OutputStreamWriter(fileStream, "UTF-8");
        String output = key.keyType.toString()
                        + "\t"
                        + formatKey(key)
                        + "\t"
                        + formatMessage(value)
                        + "\t"
                        + tp.toString()
                        + "\t"
                        + timestamp
                        + "\n";
        fr.write(output);
        fr.close();
      } catch (IOException e) {
        log.error("failed to write backup file for schema registry key: " + key);
      }
    }
  }

  private String formatKey(SchemaRegistryKey key) {
    ObjectMapper obj = new ObjectMapper();
    try {
      String jsonStr = obj.writeValueAsString(key);
      return jsonStr;
    } catch (JsonProcessingException e) {
      log.error("failed to process key");
    }
    return "";
  }

  private String formatMessage(SchemaRegistryValue value) {
    ObjectMapper obj = new ObjectMapper();
    try {
      String jsonStr = obj.writeValueAsString(value);
      return jsonStr;
    } catch (JsonProcessingException e) {
      log.error("failed to process message");
    }
    return "";
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
          lookupCache.put(schemaKey, schemaValue);
          lookupCache.schemaDeleted(schemaKey, schemaValue);
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
      if (schemaValue.isDeleted()) {
        lookupCache.schemaDeleted(schemaKey, schemaValue);
        updateMetrics(metricsContainer.getSchemasDeleted(),
                      metricsContainer.getSchemasDeleted(getSchemaType(schemaValue)));
      } else {
        // Update the maximum id seen so far
        idGenerator.schemaRegistered(schemaKey, schemaValue);
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
    total.increment();
    if (perType != null) {
      perType.increment();
    }
  }
}
