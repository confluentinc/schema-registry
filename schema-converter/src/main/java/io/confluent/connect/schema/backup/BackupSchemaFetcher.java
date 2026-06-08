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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fetches schema metadata and builds reference trees from Schema Registry
 * for backup purposes. Thread-safe for concurrent use.
 */
public class BackupSchemaFetcher {

  private static final Logger log = LoggerFactory.getLogger(BackupSchemaFetcher.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int MAX_DEPTH = 50;

  private final SchemaRegistryClient schemaRegistry;
  private final Map<Integer, BackupSchemaInfo> cache = new ConcurrentHashMap<>();

  public BackupSchemaFetcher(SchemaRegistryClient schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  /**
   * Fetch schema info for the given schema ID, with caching.
   * Throws exceptions following the same pattern as the deserializer -
   * let RestClientException and IOException propagate to be handled by converter.
   */
  public BackupSchemaInfo fetchSchemaInfo(int schemaId)
      throws IOException, RestClientException {
    BackupSchemaInfo cached = cache.get(schemaId);
    if (cached != null) {
      return cached;
    }

    ParsedSchema fullSchema = schemaRegistry.getSchemaById(schemaId);
    String rawText = fullSchema.canonicalString();
    List<SchemaReference> directRefs = fullSchema.references();
    Map<String, RefTreeEntry> tree = new HashMap<>();
    if (directRefs != null && !directRefs.isEmpty()) {
      buildReferenceTree(directRefs, tree, 0);
    }
    String treeJson = !tree.isEmpty() ? JSON.writeValueAsString(tree) : null;
    String directRefsJson = directRefs != null && !directRefs.isEmpty()
        ? JSON.writeValueAsString(directRefs) : null;

    Map<String, Integer> versionsBySubject = new HashMap<>();
    try {
      for (SubjectVersion sv : schemaRegistry.getAllVersionsById(schemaId)) {
        versionsBySubject.put(sv.getSubject(), sv.getVersion());
      }
    } catch (IOException | RestClientException e) {
      log.warn("Could not fetch versions for schemaId={}: {}",
          schemaId, e.getMessage());
    }

    BackupSchemaInfo info = new BackupSchemaInfo(rawText, directRefs, tree,
        treeJson, directRefsJson, versionsBySubject);
    cache.put(schemaId, info);
    return info;
  }

  private void buildReferenceTree(
      List<SchemaReference> refs, Map<String, RefTreeEntry> tree,
      int depth) throws IOException, RestClientException {
    if (refs == null) {
      return;
    }
    if (depth >= MAX_DEPTH) {
      throw new IOException(
          "Reference tree depth limit (" + MAX_DEPTH + ") exceeded. "
          + "Possible circular reference chain.");
    }
    for (SchemaReference ref : refs) {
      if (tree.containsKey(ref.getName())) {
        continue;
      }
      io.confluent.kafka.schemaregistry.client.SchemaMetadata meta =
          schemaRegistry.getSchemaMetadata(ref.getSubject(), ref.getVersion());
      String schemaText = meta.getSchema();
      ParsedSchema parsed = schemaRegistry.getSchemaById(meta.getId());
      List<SchemaReference> childRefs = parsed.references();
      tree.put(ref.getName(), new RefTreeEntry(
          ref.getSubject(), ref.getVersion(), schemaText, childRefs,
          meta.getId(), meta.getSchemaType()));
      if (childRefs != null && !childRefs.isEmpty()) {
        buildReferenceTree(childRefs, tree, depth + 1);
      }
    }
  }

  public static class BackupSchemaInfo {
    private final String rawSchema;
    private final List<SchemaReference> directReferences;
    private final Map<String, RefTreeEntry> referenceTree;
    private final String referenceTreeJson;
    private final String directRefsJson;
    private final Map<String, Integer> versionsBySubject;

    public BackupSchemaInfo(
        String rawSchema,
        List<SchemaReference> directReferences,
        Map<String, RefTreeEntry> referenceTree,
        String referenceTreeJson,
        String directRefsJson,
        Map<String, Integer> versionsBySubject) {
      this.rawSchema = rawSchema;
      this.directReferences = directReferences != null
          ? directReferences : Collections.emptyList();
      this.referenceTree = referenceTree != null
          ? referenceTree : Collections.emptyMap();
      this.referenceTreeJson = referenceTreeJson;
      this.directRefsJson = directRefsJson;
      this.versionsBySubject = versionsBySubject != null
          ? versionsBySubject : Collections.emptyMap();
    }

    public String getRawSchema() {
      return rawSchema;
    }

    public List<SchemaReference> getDirectReferences() {
      return directReferences;
    }

    public Map<String, RefTreeEntry> getReferenceTree() {
      return referenceTree;
    }

    public String getReferenceTreeJson() {
      return referenceTreeJson;
    }

    public String getDirectRefsJson() {
      return directRefsJson;
    }

    public Integer getVersionForSubject(String subject) {
      return versionsBySubject.get(subject);
    }
  }

  public static class RefTreeEntry {
    private final String subject;
    private final int version;
    private final String schema;
    private final List<SchemaReference> references;
    private final int globalId;
    private final String schemaType;

    public RefTreeEntry(String subject, int version, String schema,
        List<SchemaReference> references, int globalId, String schemaType) {
      this.subject = subject;
      this.version = version;
      this.schema = schema;
      this.references = references != null ? references : Collections.emptyList();
      this.globalId = globalId;
      this.schemaType = schemaType;
    }

    public String getSubject() {
      return subject;
    }

    public int getVersion() {
      return version;
    }

    public String getSchema() {
      return schema;
    }

    public List<SchemaReference> getReferences() {
      return references;
    }

    public int getGlobalId() {
      return globalId;
    }

    public String getSchemaType() {
      return schemaType;
    }
  }
}
