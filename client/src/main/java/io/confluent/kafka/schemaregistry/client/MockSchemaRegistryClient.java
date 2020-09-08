/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.LinkedList;

/**
 * Mock implementation of SchemaRegistryClient that can be used for tests. This version is NOT
 * thread safe. Schema data is stored in memory and is not persistent or shared across instances.
 */
public class MockSchemaRegistryClient implements SchemaRegistryClient {

  private static final Logger log = LoggerFactory.getLogger(MockSchemaRegistryClient.class);

  private static final String WILDCARD = "*";

  private String defaultCompatibility = "BACKWARD";
  private final Map<String, Map<ParsedSchema, Integer>> schemaCache;
  private final Map<ParsedSchema, Integer> schemaIdCache;
  private final Map<String, Map<Integer, ParsedSchema>> idCache;
  private final Map<String, Map<ParsedSchema, Integer>> versionCache;
  private final Map<String, String> compatibilityCache;
  private final Map<String, String> modes;
  private final AtomicInteger ids;
  private final Map<String, SchemaProvider> providers;

  public MockSchemaRegistryClient() {
    this(null);
  }

  public MockSchemaRegistryClient(List<SchemaProvider> providers) {
    schemaCache = new HashMap<String, Map<ParsedSchema, Integer>>();
    schemaIdCache = new HashMap<>();
    idCache = new HashMap<String, Map<Integer, ParsedSchema>>();
    versionCache = new HashMap<String, Map<ParsedSchema, Integer>>();
    compatibilityCache = new HashMap<String, String>();
    modes = new HashMap<String, String>();
    ids = new AtomicInteger(0);
    idCache.put(null, new HashMap<Integer, ParsedSchema>());

    this.providers = providers != null && !providers.isEmpty()
                     ? providers.stream().collect(Collectors.toMap(p -> p.schemaType(), p -> p))
                     : Collections.singletonMap(AvroSchema.TYPE, new AvroSchemaProvider());
    Map<String, Object> schemaProviderConfigs = new HashMap<>();
    schemaProviderConfigs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, this);
    for (SchemaProvider provider : this.providers.values()) {
      provider.configure(schemaProviderConfigs);
    }
  }

  @Override
  public Optional<ParsedSchema> parseSchema(
      String schemaType,
      String schemaString,
      List<SchemaReference> references) {
    if (schemaType == null) {
      schemaType = AvroSchema.TYPE;
    }
    SchemaProvider schemaProvider = providers.get(schemaType);
    if (schemaProvider == null) {
      log.error("No provider found for schema type " + schemaType);
      return Optional.empty();
    }
    return schemaProvider.parseSchema(schemaString, references);
  }

  private int getIdFromRegistry(
      String subject, ParsedSchema schema, boolean registerRequest, int id)
      throws IOException, RestClientException {
    Map<Integer, ParsedSchema> idSchemaMap;
    if (idCache.containsKey(subject)) {
      idSchemaMap = idCache.get(subject);
      for (Map.Entry<Integer, ParsedSchema> entry : idSchemaMap.entrySet()) {
        if (entry.getValue().canonicalString().equals(schema.canonicalString())) {
          if (registerRequest) {
            if (id >= 0 && id != entry.getKey()) {
              throw new IllegalStateException("Schema already registered with id "
                  + entry.getKey() + " instead of input id " + id);
            }
            generateVersion(subject, schema);
          }
          return entry.getKey();
        }
      }
    } else {
      if (!registerRequest) {
        throw new RestClientException("Subject Not Found", 404, 40401);
      }
      idSchemaMap = new HashMap<Integer, ParsedSchema>();
    }
    if (registerRequest) {
      Integer schemaId = schemaIdCache.get(schema);
      if (schemaId == null) {
        schemaId = id >= 0 ? id : ids.incrementAndGet();
        schemaIdCache.put(schema, schemaId);
      } else if (id >= 0 && id != schemaId) {
        throw new IllegalStateException("Schema already registered with id "
            + schemaId + " instead of input id " + id);
      }
      idSchemaMap.put(schemaId, schema);
      idCache.put(subject, idSchemaMap);
      generateVersion(subject, schema);
      return schemaId;
    } else {
      throw new RestClientException("Schema Not Found", 404, 40403);
    }
  }

  private void generateVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    List<Integer> versions = allVersions(subject);
    Map<ParsedSchema, Integer> schemaVersionMap;
    int currentVersion;
    if (versions.isEmpty()) {
      schemaVersionMap = new HashMap<ParsedSchema, Integer>();
      currentVersion = 1;
    } else {
      schemaVersionMap = versionCache.get(subject);
      currentVersion = versions.get(versions.size() - 1) + 1;
    }
    schemaVersionMap.put(schema, currentVersion);
    versionCache.put(subject, schemaVersionMap);
  }

  @Override
  public synchronized List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    if (versionCache.containsKey(subject)) {
      return allVersions(subject);
    } else {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
  }

  private synchronized List<Integer> allVersions(String subject) {
    ArrayList<Integer> versions = new ArrayList<Integer>();
    if (versionCache.containsKey(subject)) {
      versions.addAll(versionCache.get(subject).values());
      Collections.sort(versions);
    }
    return versions;
  }

  private ParsedSchema getSchemaBySubjectAndIdFromRegistry(String subject, int id)
      throws IOException, RestClientException {
    if (idCache.containsKey(subject)) {
      Map<Integer, ParsedSchema> idSchemaMap = idCache.get(subject);
      if (idSchemaMap.containsKey(id)) {
        return idSchemaMap.get(id);
      }
    } else {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
    throw new IOException("Cannot get schema from schema registry!");
  }

  @Override
  public synchronized int register(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return register(subject, schema, 0, -1);
  }

  @Override
  public synchronized int register(String subject, ParsedSchema schema, int version, int id)
      throws IOException, RestClientException {
    Map<ParsedSchema, Integer> schemaIdMap;
    if (schemaCache.containsKey(subject)) {
      schemaIdMap = schemaCache.get(subject);
    } else {
      schemaIdMap = new HashMap<ParsedSchema, Integer>();
      schemaCache.put(subject, schemaIdMap);
    }

    if (schemaIdMap.containsKey(schema)) {
      int schemaId = schemaIdMap.get(schema);
      if (id >= 0 && id != schemaId) {
        throw new IllegalStateException("Schema already registered with id "
            + schemaId + " instead of input id " + id);
      }
      return schemaId;
    } else {
      id = getIdFromRegistry(subject, schema, true, id);
      schemaIdMap.put(schema, id);
      if (!idCache.get(null).containsKey(id)) {
        idCache.get(null).put(id, schema);
      }
      return id;
    }
  }

  @Override
  public synchronized ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    return getSchemaBySubjectAndId(null, id);
  }

  @Override
  public synchronized ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    Map<Integer, ParsedSchema> idSchemaMap;
    if (idCache.containsKey(subject)) {
      idSchemaMap = idCache.get(subject);
    } else {
      idSchemaMap = new HashMap<Integer, ParsedSchema>();
      idCache.put(subject, idSchemaMap);
    }

    if (idSchemaMap.containsKey(id)) {
      return idSchemaMap.get(id);
    } else {
      ParsedSchema schema = getSchemaBySubjectAndIdFromRegistry(subject, id);
      idSchemaMap.put(id, schema);
      return schema;
    }
  }

  @Override
  public Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException {
    return idCache.entrySet().stream()
            .filter(entry -> entry.getValue().containsKey(id))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  @Override
  public Collection<SubjectVersion> getAllVersionsById(int id) throws IOException,
      RestClientException {
    return idCache.entrySet().stream()
        .filter(entry -> entry.getValue().containsKey(id))
        .map(e -> {
          ParsedSchema schema = e.getValue().get(id);
          int version = versionCache.get(e.getKey()).get(schema);
          return new SubjectVersion(e.getKey(), version);
        }).collect(Collectors.toList());
  }

  private int getLatestVersion(String subject)
      throws IOException, RestClientException {
    List<Integer> versions = getAllVersions(subject);
    if (versions.isEmpty()) {
      throw new IOException("No schema registered under subject!");
    } else {
      return versions.get(versions.size() - 1);
    }
  }

  @Override
  public Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
    ParsedSchema schema = null;
    Map<ParsedSchema, Integer> schemaVersionMap = versionCache.get(subject);
    if (schemaVersionMap == null) {
      throw new RuntimeException(new RestClientException("Subject Not Found", 404, 40401));
    }
    for (Map.Entry<ParsedSchema, Integer> entry : schemaVersionMap.entrySet()) {
      if (entry.getValue() == version) {
        schema = entry.getKey();
      }
    }
    int id = -1;
    Map<Integer, ParsedSchema> idSchemaMap = idCache.get(subject);
    for (Map.Entry<Integer, ParsedSchema> entry : idSchemaMap.entrySet()) {
      if (entry.getValue().canonicalString().equals(schema.canonicalString())) {
        id = entry.getKey();
      }
    }
    return new Schema(subject, version, id, schema.schemaType(), schema.references(),
        schema.canonicalString());
  }

  @Override
  public synchronized SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException {
    ParsedSchema schema = null;
    Map<ParsedSchema, Integer> schemaVersionMap = versionCache.get(subject);
    if (schemaVersionMap == null) {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
    for (Map.Entry<ParsedSchema, Integer> entry : schemaVersionMap.entrySet()) {
      if (entry.getValue() == version) {
        schema = entry.getKey();
      }
    }
    int id = -1;
    Map<Integer, ParsedSchema> idSchemaMap = idCache.get(subject);
    for (Map.Entry<Integer, ParsedSchema> entry : idSchemaMap.entrySet()) {
      if (entry.getValue().canonicalString().equals(schema.canonicalString())) {
        id = entry.getKey();
      }
    }
    return new SchemaMetadata(
        id, version, schema.schemaType(), schema.references(), schema.canonicalString());
  }

  @Override
  public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    int version = getLatestVersion(subject);
    return getSchemaMetadata(subject, version);
  }

  @Override
  public synchronized int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    if (versionCache.containsKey(subject)) {
      Map<ParsedSchema, Integer> versions = versionCache.get(subject);
      return versions.get(schema);
    } else {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
  }

  @Override
  public boolean testCompatibility(String subject, ParsedSchema newSchema) throws IOException,
                                                                            RestClientException {
    String compatibility = compatibilityCache.get(subject);
    if (compatibility == null) {
      compatibility = defaultCompatibility;
    }

    CompatibilityLevel compatibilityLevel = CompatibilityLevel.forName(compatibility);
    if (compatibilityLevel == null) {
      return false;
    }

    List<ParsedSchema> schemaHistory = new ArrayList<>();
    for (int version : allVersions(subject)) {
      SchemaMetadata schemaMetadata = getSchemaMetadata(subject, version);
      schemaHistory.add(getSchemaBySubjectAndIdFromRegistry(subject,
          schemaMetadata.getId()));
    }

    return newSchema.isCompatible(compatibilityLevel, schemaHistory).isEmpty();
  }

  @Override
  public List<String> testCompatibilityVerbose(String subject, ParsedSchema newSchema)
          throws IOException, RestClientException {
    String compatibility = compatibilityCache.get(subject);
    if (compatibility == null) {
      compatibility = defaultCompatibility;
    }

    CompatibilityLevel compatibilityLevel = CompatibilityLevel.forName(compatibility);
    if (compatibilityLevel == null) {
      return new LinkedList<>(Arrays.asList("Compatibility level not specified."));
    }

    List<ParsedSchema> schemaHistory = new ArrayList<>();
    for (int version : allVersions(subject)) {
      SchemaMetadata schemaMetadata = getSchemaMetadata(subject, version);
      schemaHistory.add(getSchemaBySubjectAndIdFromRegistry(subject,
          schemaMetadata.getId()));
    }

    return newSchema.isCompatible(compatibilityLevel, schemaHistory);
  }

  @Override
  public String updateCompatibility(String subject, String compatibility)
      throws IOException,
             RestClientException {
    if (subject == null) {
      defaultCompatibility = compatibility;
      return compatibility;
    }
    compatibilityCache.put(subject, compatibility);
    return compatibility;
  }

  @Override
  public String getCompatibility(String subject) throws IOException, RestClientException {
    if (subject == null) {
      return defaultCompatibility;
    }
    String compatibility = compatibilityCache.get(subject);
    if (compatibility == null) {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
    return compatibility;
  }

  @Override
  public String setMode(String mode)
      throws IOException, RestClientException {
    modes.put(WILDCARD, mode);
    return mode;
  }

  @Override
  public String setMode(String mode, String subject)
      throws IOException, RestClientException {
    modes.put(subject, mode);
    return mode;
  }

  @Override
  public String getMode() throws IOException, RestClientException {
    return modes.getOrDefault(WILDCARD, "READWRITE");
  }

  @Override
  public String getMode(String subject) throws IOException, RestClientException {
    String mode = modes.get(subject);
    if (mode == null) {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
    return mode;
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    List<String> results = new ArrayList<>();
    results.addAll(this.schemaCache.keySet());
    Collections.sort(results, String.CASE_INSENSITIVE_ORDER);
    return results;
  }

  @Override
  public int getId(String subject, ParsedSchema schema) throws IOException, RestClientException {
    return getIdFromRegistry(subject, schema, false, -1);
  }

  @Override
  public List<Integer> deleteSubject(String subject, boolean isPermanent)
          throws IOException, RestClientException {
    return deleteSubject(null, subject, isPermanent);
  }

  @Override
  public List<Integer> deleteSubject(
      Map<String, String> requestProperties,
      String subject,
      boolean isPermanent)
      throws IOException, RestClientException {
    schemaCache.remove(subject);
    idCache.remove(subject);
    versionCache.remove(subject);
    compatibilityCache.remove(subject);
    return Arrays.asList(0);
  }

  @Override
  public Integer deleteSchemaVersion(String subject, String version, boolean isPermanent)
      throws IOException, RestClientException {
    return deleteSchemaVersion(null, subject, version, isPermanent);
  }

  @Override
  public Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version,
      boolean isPermanent)
      throws IOException, RestClientException {
    if (versionCache.containsKey(subject)) {
      versionCache.get(subject).values().remove(Integer.valueOf(version));
      return 0;
    }
    return -1;
  }

  @Override
  public void reset() {
    schemaCache.clear();
    idCache.clear();
    versionCache.clear();
    idCache.put(null, new HashMap<Integer, ParsedSchema>());
  }
}
