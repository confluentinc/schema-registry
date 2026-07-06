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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves and registers schema references in the target Schema Registry during restore.
 */
public class BackupReferenceResolver {

  private static final Logger log = LoggerFactory.getLogger(BackupReferenceResolver.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int MAX_DEPTH = SchemaBackupConfig.MAX_REFERENCE_DEPTH;

  private final SchemaRegistryClient schemaRegistry;
  private final Map<String, Integer> registrationCache = new ConcurrentHashMap<>();

  public BackupReferenceResolver(SchemaRegistryClient schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  public static class ResolutionResult {
    private final List<SchemaReference> targetRefs;
    private final Map<String, String> resolvedSchemas;

    private ResolutionResult(List<SchemaReference> targetRefs,
        Map<String, String> resolvedSchemas) {
      this.targetRefs = targetRefs;
      this.resolvedSchemas = resolvedSchemas;
    }

    static ResolutionResult of(List<SchemaReference> targetRefs,
        Map<String, String> resolvedSchemas) {
      return new ResolutionResult(targetRefs, resolvedSchemas);
    }

    public List<SchemaReference> getTargetRefs() {
      return targetRefs;
    }

    public Map<String, String> getResolvedSchemas() {
      return resolvedSchemas;
    }

    public boolean hasReferences() {
      return targetRefs != null && !targetRefs.isEmpty();
    }

    private static final ResolutionResult EMPTY =
        new ResolutionResult(Collections.emptyList(), Collections.emptyMap());

    public static ResolutionResult empty() {
      return EMPTY;
    }
  }

  public ResolutionResult resolveFromWrapper(
      Schema wrapperSchema, Struct wrapper, ParsedSchemaFactory factory) {
    String treeJson = wrapperSchema.field(BackupWrapper.FIELD_REFERENCE_TREE) != null
        ? wrapper.getString(BackupWrapper.FIELD_REFERENCE_TREE) : null;
    String directRefsJson = wrapperSchema.field(BackupWrapper.FIELD_DIRECT_REFS) != null
        ? wrapper.getString(BackupWrapper.FIELD_DIRECT_REFS) : null;

    Map<String, BackupSchemaFetcher.RefTreeEntry> refTree =
        parseReferenceTree(treeJson);
    List<SchemaReference> directRefs =
        parseDirectRefs(directRefsJson);

    if (refTree.isEmpty() || directRefs.isEmpty()) {
      return ResolutionResult.empty();
    }

    List<SchemaReference> targetRefs = new ArrayList<>();
    // LinkedHashMap: schema parsers need transitive deps before their dependents
    Map<String, String> resolvedSchemas = new LinkedHashMap<>();
    registerRefsRecursive(
        directRefs, refTree, factory, targetRefs, resolvedSchemas);
    return ResolutionResult.of(targetRefs, resolvedSchemas);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, BackupSchemaFetcher.RefTreeEntry> parseReferenceTree(String json) {
    if (json == null || json.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      Map<String, Map<String, Object>> raw = JSON.readValue(json,
          new TypeReference<Map<String, Map<String, Object>>>() {});
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new LinkedHashMap<>();
      for (Map.Entry<String, Map<String, Object>> e : raw.entrySet()) {
        tree.put(e.getKey(), parseRefTreeEntry(e.getValue()));
      }
      return tree;
    } catch (IOException e) {
      throw new DataException(
          "Cannot parse reference tree JSON for restore. "
          + "Backup metadata may be corrupt.", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static BackupSchemaFetcher.RefTreeEntry parseRefTreeEntry(Map<String, Object> v) {
    String subject = (String) v.get(SchemaBackupConfig.REF_FIELD_SUBJECT);
    int version = v.get(SchemaBackupConfig.REF_FIELD_VERSION) instanceof Number
        ? ((Number) v.get(SchemaBackupConfig.REF_FIELD_VERSION)).intValue() : 0;
    String schema = (String) v.get(SchemaBackupConfig.REF_FIELD_SCHEMA);
    List<SchemaReference> refs = Collections.emptyList();
    Object refsObj = v.get(SchemaBackupConfig.REF_FIELD_REFERENCES);
    if (refsObj instanceof List) {
      refs = new ArrayList<>();
      for (Object item : (List<?>) refsObj) {
        if (item instanceof Map) {
          Map<String, Object> m = (Map<String, Object>) item;
          refs.add(new SchemaReference(
              (String) m.get(SchemaBackupConfig.REF_FIELD_NAME),
              (String) m.get(SchemaBackupConfig.REF_FIELD_SUBJECT),
              m.get(SchemaBackupConfig.REF_FIELD_VERSION) instanceof Number
                  ? ((Number) m.get(SchemaBackupConfig.REF_FIELD_VERSION)).intValue() : 0));
        }
      }
    }
    int globalId = v.get(SchemaBackupConfig.REF_FIELD_GLOBAL_ID) instanceof Number
        ? ((Number) v.get(SchemaBackupConfig.REF_FIELD_GLOBAL_ID)).intValue() : 0;
    String schemaType = v.get(SchemaBackupConfig.REF_FIELD_SCHEMA_TYPE) instanceof String
        ? (String) v.get(SchemaBackupConfig.REF_FIELD_SCHEMA_TYPE) : null;
    return new BackupSchemaFetcher.RefTreeEntry(
        subject, version, schema, refs, globalId, schemaType);
  }

  public static List<SchemaReference> parseDirectRefs(String json) {
    if (json == null || json.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      return JSON.readValue(json, new TypeReference<List<SchemaReference>>() {});
    } catch (IOException e) {
      throw new DataException(
          "Cannot parse direct references JSON for restore. "
          + "Backup metadata may be corrupt.", e);
    }
  }

  public void registerRefsRecursive(
      List<SchemaReference> refs,
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree,
      ParsedSchemaFactory factory,
      List<SchemaReference> targetRefsOut,
      Map<String, String> resolvedSchemasOut) {
    registerRefsRecursive(
        refs, tree, factory, targetRefsOut, resolvedSchemasOut, 0);
  }

  private void registerRefsRecursive(
      List<SchemaReference> refs,
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree,
      ParsedSchemaFactory factory,
      List<SchemaReference> targetRefsOut,
      Map<String, String> resolvedSchemasOut,
      int depth) {
    if (refs == null) {
      return;
    }
    if (depth >= MAX_DEPTH) {
      throw new DataException(
          "Reference registration depth limit (" + MAX_DEPTH
          + ") exceeded. Possible circular reference chain.");
    }
    for (SchemaReference ref : refs) {
      registerSingleRef(ref, tree, factory, targetRefsOut, resolvedSchemasOut, depth);
    }
  }

  private void registerSingleRef(
      SchemaReference ref,
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree,
      ParsedSchemaFactory factory,
      List<SchemaReference> targetRefsOut,
      Map<String, String> resolvedSchemasOut,
      int depth) {
    String cacheKey = ref.getSubject() + ":" + ref.getVersion();
    Integer targetVersion = registrationCache.get(cacheKey);
    if (targetVersion != null) {
      targetRefsOut.add(new SchemaReference(
          ref.getName(), ref.getSubject(), targetVersion));
      BackupSchemaFetcher.RefTreeEntry entry = tree.get(ref.getName());
      if (entry != null) {
        addTransitiveSchemas(ref.getName(), entry, tree, resolvedSchemasOut);
      }
      return;
    }

    BackupSchemaFetcher.RefTreeEntry entry = tree.get(ref.getName());
    if (entry == null) {
      throw new DataException(
          "Cannot guarantee pristine restore: reference '"
          + ref.getName()
          + "' (subject=" + ref.getSubject()
          + ") not found in reference tree. "
          + "Backup may be incomplete.");
    }

    List<SchemaReference> childTargetRefs = new ArrayList<>();
    Map<String, String> childResolvedSchemas = new LinkedHashMap<>();
    if (!entry.getReferences().isEmpty()) {
      registerRefsRecursive(
          entry.getReferences(), tree, factory,
          childTargetRefs, childResolvedSchemas, depth + 1);
    }

    resolvedSchemasOut.putAll(childResolvedSchemas);

    try {
      ParsedSchema refSchema = factory.create(
          entry.getSchema(), childTargetRefs, childResolvedSchemas);
      schemaRegistry.register(ref.getSubject(), refSchema);
      targetVersion = schemaRegistry.getVersion(
          ref.getSubject(), refSchema);
      registrationCache.put(cacheKey, targetVersion);
      log.debug("Registered reference: subject={}, "
          + "srcVersion={}, targetVersion={}",
          ref.getSubject(), ref.getVersion(), targetVersion);
    } catch (IOException e) {
      throw new RetriableException(
          "Network error registering reference '" + ref.getSubject() + "'", e);
    } catch (RestClientException e) {
      throw new DataException(
          "Failed to register reference '" + ref.getSubject() + "'", e);
    }
    targetRefsOut.add(new SchemaReference(
        ref.getName(), ref.getSubject(), targetVersion));
    resolvedSchemasOut.put(ref.getName(), entry.getSchema());
  }

  private void addTransitiveSchemas(
      String refName,
      BackupSchemaFetcher.RefTreeEntry entry,
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree,
      Map<String, String> resolvedSchemasOut) {
    addTransitiveSchemas(refName, entry, tree, resolvedSchemasOut, 0);
  }

  private void addTransitiveSchemas(
      String refName,
      BackupSchemaFetcher.RefTreeEntry entry,
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree,
      Map<String, String> resolvedSchemasOut,
      int depth) {
    if (depth >= MAX_DEPTH) {
      throw new DataException(
          "Reference tree depth limit (" + MAX_DEPTH
          + ") exceeded while collecting transitive schemas. "
          + "Possible circular reference chain.");
    }
    if (entry.getReferences() != null) {
      for (SchemaReference childRef : entry.getReferences()) {
        BackupSchemaFetcher.RefTreeEntry childEntry = tree.get(childRef.getName());
        if (childEntry != null && !resolvedSchemasOut.containsKey(childRef.getName())) {
          addTransitiveSchemas(
              childRef.getName(), childEntry, tree, resolvedSchemasOut, depth + 1);
        }
      }
    }
    resolvedSchemasOut.put(refName, entry.getSchema());
  }

  @FunctionalInterface
  public interface ParsedSchemaFactory {
    ParsedSchema create(String rawSchema, List<SchemaReference> refs,
        Map<String, String> resolvedSchemas);
  }
}
