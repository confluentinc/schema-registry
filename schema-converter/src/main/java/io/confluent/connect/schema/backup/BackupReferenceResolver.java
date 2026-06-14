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
 */
public class BackupReferenceResolver {

  private static final Logger log = LoggerFactory.getLogger(BackupReferenceResolver.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int MAX_DEPTH = 50;

  private final SchemaRegistryClient schemaRegistry;
  private final Map<String, Integer> registrationCache = new ConcurrentHashMap<>();

  public BackupReferenceResolver(SchemaRegistryClient schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

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

  public void registerRefsRecursive(
      List<SchemaReference> refs,
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree,
      ParsedSchemaFactory factory,
      List<SchemaReference> remappedOut,
      Map<String, String> resolvedOut) {
    registerRefsRecursive(
        refs, tree, factory, remappedOut, resolvedOut, 0);
  }

  private void registerRefsRecursive(
      List<SchemaReference> refs,
      Map<String, BackupSchemaFetcher.RefTreeEntry> tree,
      ParsedSchemaFactory factory,
      List<SchemaReference> remappedOut,
      Map<String, String> resolvedOut,
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
        remappedOut.add(new SchemaReference(
            ref.getName(), ref.getSubject(), targetVersion));
        BackupSchemaFetcher.RefTreeEntry entry = tree.get(ref.getName());
        if (entry != null) {
          resolvedOut.put(ref.getName(), entry.getSchema());
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

      List<SchemaReference> childRemapped = new ArrayList<>();
      Map<String, String> childResolved = new HashMap<>();
      if (!entry.getReferences().isEmpty()) {
        registerRefsRecursive(
            entry.getReferences(), tree, factory,
            childRemapped, childResolved, depth + 1);
      }

      try {
        ParsedSchema refSchema = factory.create(
            entry.getSchema(), childRemapped, childResolved);
        schemaRegistry.register(ref.getSubject(), refSchema);
        targetVersion = schemaRegistry.getVersion(
            ref.getSubject(), refSchema);
        registrationCache.put(cacheKey, targetVersion);
        log.debug("Pre-registered reference: subject={}, "
            + "srcVersion={}, targetVersion={}",
            ref.getSubject(), ref.getVersion(), targetVersion);
      } catch (IOException | RestClientException e) {
        throw new org.apache.kafka.connect.errors.DataException(
            "Cannot guarantee pristine restore: failed to register"
            + " reference schema '" + ref.getSubject()
            + "'. Schema references must be resolvable for "
            + "correct backup/restore fidelity.", e);
      }
      remappedOut.add(new SchemaReference(
          ref.getName(), ref.getSubject(), targetVersion));
      resolvedOut.put(ref.getName(), entry.getSchema());
    }
  }

  @FunctionalInterface
  public interface ParsedSchemaFactory {
    ParsedSchema create(String rawSchema, List<SchemaReference> refs,
        Map<String, String> resolvedTexts);
  }
}
