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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves and registers schema references during backup restore.
 *
 * <p>Handles depth-first registration of transitive reference chains
 * (e.g., User→Address→Country). Each reference is registered in the target
 * Schema Registry before its parent, ensuring the parent can reference it
 * by the correct target version.
 */
public class BackupReferenceResolver {

  private static final Logger log = LoggerFactory.getLogger(BackupReferenceResolver.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int MAX_DEPTH = 50;

  private final SchemaRegistryClient schemaRegistry;
  private final Map<String, Integer> registrationCache = new ConcurrentHashMap<>();

  /**
   * Creates a resolver that registers references in the given Schema Registry.
   */
  public BackupReferenceResolver(SchemaRegistryClient schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  /**
   * Result of reference resolution: the target references (direct only)
   * and all resolved schema texts (direct + transitive).
   *
   * <p>{@code targetRefs} contains only DIRECT references with versions
   * remapped to the target Schema Registry — used for schema registration.
   *
   * <p>{@code resolvedSchemas} contains ALL schema texts (direct and
   * transitive) — used by schema parsers to resolve nested type references.
   */
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

  /**
   * Resolves schema references from a BackupWrapper struct.
   *
   * <p>Extracts referenceTree and directRefs from the wrapper, registers all
   * referenced schemas depth-first in the target Schema Registry, and returns
   * the resolution result for constructing the main schema.
   *
   * <p>Returns {@link ResolutionResult#empty()} if the schema has no SR references
   * (i.e., only inline types).
   *
   * @param wrapperSchema the wrapper's Connect Schema
   * @param wrapper       the wrapper Struct containing reference metadata
   * @param factory       schema-type-specific factory for creating ParsedSchema instances
   * @return resolution result with target refs and resolved schema texts
   */
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
    // LinkedHashMap preserves insertion order (depth-first = leaves first)
    // which is required by schema parsers that iterate resolvedSchemas.values()
    // and need transitive dependencies parsed before their dependents
    Map<String, String> resolvedSchemas = new java.util.LinkedHashMap<>();
    registerRefsRecursive(
        directRefs, refTree, factory, targetRefs, resolvedSchemas);
    return ResolutionResult.of(targetRefs, resolvedSchemas);
  }

  /**
   * Parses a reference tree JSON string into a map of reference name to tree entry.
   *
   * @param json the reference tree JSON, or null/empty for no references
   * @return parsed reference tree, empty map if input is null/empty
   * @throws DataException if the JSON is malformed
   */
  @SuppressWarnings("unchecked")
  public static Map<String, BackupSchemaFetcher.RefTreeEntry> parseReferenceTree(String json) {
    if (json == null || json.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      Map<String, Map<String, Object>> raw = JSON.readValue(json,
          new TypeReference<Map<String, Map<String, Object>>>() {});
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
      for (Map.Entry<String, Map<String, Object>> e : raw.entrySet()) {
        Map<String, Object> v = e.getValue();
        String subject = (String) v.get("subject");
        int version = v.get("version") instanceof Number
            ? ((Number) v.get("version")).intValue() : 0;
        String schema = (String) v.get("schema");
        List<SchemaReference> refs = Collections.emptyList();
        Object refsObj = v.get("references");
        if (refsObj instanceof List) {
          refs = new ArrayList<>();
          for (Object item : (List<?>) refsObj) {
            if (item instanceof Map) {
              Map<String, Object> m = (Map<String, Object>) item;
              refs.add(new SchemaReference(
                  (String) m.get("name"),
                  (String) m.get("subject"),
                  m.get("version") instanceof Number
                      ? ((Number) m.get("version")).intValue() : 0));
            }
          }
        }
        int globalId = v.get("globalId") instanceof Number
            ? ((Number) v.get("globalId")).intValue() : 0;
        String schemaType = v.get("schemaType") instanceof String
            ? (String) v.get("schemaType") : null;
        tree.put(e.getKey(), new BackupSchemaFetcher.RefTreeEntry(
            subject, version, schema, refs, globalId, schemaType));
      }
      return tree;
    } catch (IOException e) {
      throw new org.apache.kafka.connect.errors.DataException(
          "Cannot parse reference tree JSON for restore. "
          + "Backup metadata may be corrupt.", e);
    }
  }

  /**
   * Parses a direct references JSON array into a list of SchemaReference.
   *
   * @param json the direct references JSON array, or null/empty for no references
   * @return parsed references, empty list if input is null/empty
   * @throws DataException if the JSON is malformed
   */
  public static List<SchemaReference> parseDirectRefs(String json) {
    if (json == null || json.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      return JSON.readValue(json, new TypeReference<List<SchemaReference>>() {});
    } catch (IOException e) {
      throw new org.apache.kafka.connect.errors.DataException(
          "Cannot parse direct references JSON for restore. "
          + "Backup metadata may be corrupt.", e);
    }
  }

  /**
   * Registers schema references depth-first in the target Schema Registry.
   * Populates {@code targetRefsOut} with direct references (version-remapped)
   * and {@code resolvedSchemasOut} with all schema texts (direct + transitive).
   */
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
      throw new org.apache.kafka.connect.errors.DataException(
          "Reference registration depth limit (" + MAX_DEPTH
          + ") exceeded. Possible circular reference chain.");
    }
    for (SchemaReference ref : refs) {
      String cacheKey = ref.getSubject() + ":" + ref.getVersion();
      Integer targetVersion = registrationCache.get(cacheKey);
      if (targetVersion != null) {
        targetRefsOut.add(new SchemaReference(
            ref.getName(), ref.getSubject(), targetVersion));
        BackupSchemaFetcher.RefTreeEntry entry = tree.get(ref.getName());
        if (entry != null) {
          addTransitiveSchemas(ref.getName(), entry, tree, resolvedSchemasOut);
        }
        continue;
      }

      BackupSchemaFetcher.RefTreeEntry entry = tree.get(ref.getName());
      if (entry == null) {
        throw new org.apache.kafka.connect.errors.DataException(
            "Cannot guarantee pristine restore: reference '"
            + ref.getName()
            + "' (subject=" + ref.getSubject()
            + ") not found in reference tree. "
            + "Backup may be incomplete.");
      }

      List<SchemaReference> childTargetRefs = new ArrayList<>();
      Map<String, String> childResolvedSchemas = new java.util.LinkedHashMap<>();
      if (!entry.getReferences().isEmpty()) {
        registerRefsRecursive(
            entry.getReferences(), tree, factory,
            childTargetRefs, childResolvedSchemas, depth + 1);
      }

      // Propagate transitive schema texts to the output so parent schemas
      // can resolve nested type references (e.g., Address→Country)
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
      } catch (IOException | RestClientException e) {
        throw new org.apache.kafka.connect.errors.DataException(
            "Cannot guarantee pristine restore: failed to register"
            + " reference schema '" + ref.getSubject()
            + "'. Schema references must be resolvable for "
            + "correct backup/restore fidelity.", e);
      }
      targetRefsOut.add(new SchemaReference(
          ref.getName(), ref.getSubject(), targetVersion));
      resolvedSchemasOut.put(ref.getName(), entry.getSchema());
    }
  }

  private void addTransitiveSchemas(
      String refName,
      BackupSchemaFetcher.RefTreeEntry entry,
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree,
      Map<String, String> resolvedSchemasOut) {
    if (entry.getReferences() != null) {
      for (SchemaReference childRef : entry.getReferences()) {
        BackupSchemaFetcher.RefTreeEntry childEntry = tree.get(childRef.getName());
        if (childEntry != null && !resolvedSchemasOut.containsKey(childRef.getName())) {
          addTransitiveSchemas(childRef.getName(), childEntry, tree, resolvedSchemasOut);
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
