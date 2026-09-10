/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.schema.backup.core;

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared helper for backup metadata wrapping in SR converters.
 */
public class BackupConverterHelper {

  private static final Logger log = LoggerFactory.getLogger(BackupConverterHelper.class);

  private final BackupSchemaFetcher schemaFetcher;
  private final BackupReferenceResolver referenceResolver;
  private final Map<Schema, Schema> wrapperSchemaCache;

  public BackupConverterHelper(
      SchemaRegistryClient schemaRegistry,
      Map<Schema, Schema> wrapperSchemaCache) {
    this.schemaFetcher = new BackupSchemaFetcher(schemaRegistry);
    this.referenceResolver = new BackupReferenceResolver(schemaRegistry);
    this.wrapperSchemaCache = wrapperSchemaCache;
  }

  public BackupReferenceResolver getReferenceResolver() {
    return referenceResolver;
  }

  public static boolean isBackupEnabled(Map<String, ?> configs) {
    Object val = configs.get(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG);
    return "true".equalsIgnoreCase(val != null ? val.toString() : null);
  }

  public SchemaAndValue wrapWithBackupMetadata(
      SchemaAndValue original, String topic,
      ParsedSchemaAndValue.SchemaInfo schemaInfo,
      String schemaType, boolean isKey,
      BackupReferenceResolver.ParsedSchemaFactory schemaFactory,
      SubjectNameComputer subjectComputer)
      throws IOException, RestClientException {

    Integer schemaId = schemaInfo.id();
    String schemaGuid = schemaInfo.guid() != null
        ? schemaInfo.guid().toString() : null;

    BackupSchemaFetcher.BackupSchemaInfo info;
    if (schemaId != null) {
      info = schemaFetcher.fetchSchemaInfo(schemaId);
    } else if (schemaGuid != null) {
      info = schemaFetcher.fetchSchemaInfoByGuid(schemaGuid);
    } else {
      throw new IllegalArgumentException(
          "Schema info has neither id nor guid for topic=" + topic);
    }
    String rawSchema = info.getRawSchema();

    Map<String, String> resolved = new LinkedHashMap<>();
    if (!info.getDirectReferences().isEmpty()) {
      for (Map.Entry<String, BackupSchemaFetcher.RefTreeEntry> e
          : info.getReferenceTree().entrySet()) {
        resolved.put(e.getKey(), e.getValue().getSchema());
      }
    }

    ParsedSchema parsed = schemaFactory.create(
        rawSchema, info.getDirectReferences(), resolved);
    String subject = subjectComputer.computeSubjectName(topic, isKey, parsed);
    Integer schemaVersion = info.getVersionForSubject(subject);

    Schema wrapperSchema;
    if (original.schema() == null) {
      wrapperSchema = BackupWrapper.buildSchema(null);
    } else {
      wrapperSchema = wrapperSchemaCache.computeIfAbsent(
          original.schema(), ds -> BackupWrapper.buildSchema(ds));
    }

    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        schemaId, schemaVersion, schemaType, subject,
        rawSchema, info.getReferenceTreeJson(), info.getDirectRefsJson(),
        schemaGuid);
    Struct wrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.value(), fields);

    log.debug("Wrapped backup metadata: topic={}, isKey={}, schemaId={}, guid={}, hasRefs={}",
        topic, isKey, schemaId, schemaGuid,
        info.getReferenceTreeJson() != null);
    return new SchemaAndValue(wrapperSchema, wrapper);
  }

  @FunctionalInterface
  public interface SubjectNameComputer {
    String computeSubjectName(String topic, boolean isKey, ParsedSchema schema);
  }
}
