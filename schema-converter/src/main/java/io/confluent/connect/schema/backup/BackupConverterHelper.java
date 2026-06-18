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

package io.confluent.connect.schema.backup;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared helper for backup metadata wrapping in SR converters.
 * Extracts the common algorithm from AvroConverter, ProtobufConverter,
 * and JsonSchemaConverter to eliminate duplication.
 *
 * <p>Each converter creates one instance in {@code configure()} and
 * delegates wrapping to {@link #wrapWithBackupMetadata}.
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

  /**
   * Returns the reference resolver for use in restoreFromWrapper().
   */
  public BackupReferenceResolver getReferenceResolver() {
    return referenceResolver;
  }

  /**
   * Checks if backup envelope mode is enabled in converter configs.
   */
  public static boolean isBackupEnabled(Map<String, ?> configs) {
    Object val = configs.get(BackupWrapper.SCHEMA_BACKUP_ENABLED_CONFIG);
    return "true".equalsIgnoreCase(val != null ? val.toString() : null);
  }

  /**
   * Wraps a Connect SchemaAndValue in a BackupWrapper struct with
   * schema metadata from Schema Registry.
   *
   * @param original the deserialized Connect data
   * @param topic the Kafka topic name
   * @param schemaId the SR global schema ID (from wire bytes)
   * @param schemaType the schema type constant (e.g. SCHEMA_TYPE_AVRO)
   * @param isKey whether this is a key converter
   * @param schemaFactory creates a ParsedSchema from raw text + refs
   * @param subjectComputer computes the SR subject name
   * @return wrapped SchemaAndValue with backup metadata
   */
  public SchemaAndValue wrapWithBackupMetadata(
      SchemaAndValue original, String topic, int schemaId,
      String schemaType, boolean isKey,
      BackupReferenceResolver.ParsedSchemaFactory schemaFactory,
      SubjectNameComputer subjectComputer)
      throws IOException, RestClientException {

    BackupSchemaFetcher.BackupSchemaInfo info =
        schemaFetcher.fetchSchemaInfo(schemaId);
    String rawSchema = info.getRawSchema();

    Map<String, String> resolved = new HashMap<>();
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

    Struct wrapper = BackupWrapper.buildWrapper(
        wrapperSchema, original.value(),
        schemaId, schemaVersion, schemaType, subject, rawSchema,
        info.getReferenceTreeJson(),
        info.getDirectRefsJson());

    log.debug("Wrapped backup metadata: topic={}, isKey={}, schemaId={}, hasRefs={}",
        topic, isKey, schemaId, info.getReferenceTreeJson() != null);
    return new SchemaAndValue(wrapperSchema, wrapper);
  }

  /**
   * Computes the SR subject name for a given topic and schema.
   * Each converter's inner Serializer implements this.
   */
  @FunctionalInterface
  public interface SubjectNameComputer {
    String computeSubjectName(String topic, boolean isKey, ParsedSchema schema);
  }
}
