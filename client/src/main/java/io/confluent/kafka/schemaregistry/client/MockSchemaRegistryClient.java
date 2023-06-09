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

import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.LinkedList;
import java.util.stream.Stream;

/**
 * Mock implementation of SchemaRegistryClient that can be used for tests. This version is NOT
 * thread safe. Schema data is stored in memory and is not persistent or shared across instances.
 */
public class MockSchemaRegistryClient implements SchemaRegistryClient {

  private static final Logger log = LoggerFactory.getLogger(MockSchemaRegistryClient.class);

  private static final String WILDCARD = "*";

  private String defaultCompatibility = "BACKWARD";
  private final Map<String, Map<ParsedSchema, Integer>> schemaCache;
  private final Map<String, Map<ParsedSchema, Integer>> schemaIdCache;
  private final Map<String, Map<Integer, ParsedSchema>> idCache;
  private final Map<String, Map<ParsedSchema, Integer>> versionCache;
  private final Map<String, String> compatibilityCache;
  private final Map<String, String> modes;
  private final Map<String, AtomicInteger> ids;
  private final Map<String, SchemaProvider> providers;

  private static final String NO_SUBJECT = "";

  public MockSchemaRegistryClient() {
    this(null);
  }

  public MockSchemaRegistryClient(List<SchemaProvider> providers) {
    schemaCache = new ConcurrentHashMap<>();
    schemaIdCache = new ConcurrentHashMap<>();
    idCache = new ConcurrentHashMap<>();
    versionCache = new ConcurrentHashMap<>();
    compatibilityCache = new ConcurrentHashMap<>();
    modes = new ConcurrentHashMap<>();
    ids = new ConcurrentHashMap<>();
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
    Map<Integer, ParsedSchema> idSchemaMap =
        idCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());
    if (!idSchemaMap.isEmpty()) {
      for (Map.Entry<Integer, ParsedSchema> entry : idSchemaMap.entrySet()) {
        if (entry.getValue().canonicalString().equals(schema.canonicalString())) {
          if (registerRequest) {
            if (id < 0 || id == entry.getKey()) {
              generateVersion(subject, schema);
            } else {
              continue;
            }
          }
          return entry.getKey();
        }
      }
    } else {
      if (!registerRequest) {
        throw new RestClientException("Subject Not Found", 404, 40401);
      }
    }
    if (registerRequest) {
      String context = toQualifiedContext(subject);
      Map<ParsedSchema, Integer> schemaIdMap =
          schemaIdCache.computeIfAbsent(context, k -> new ConcurrentHashMap<>());
      int schemaId;
      if (id >= 0) {
        schemaId = id;
        schemaIdMap.put(schema, schemaId);
      } else {
        schemaId = schemaIdMap.computeIfAbsent(schema, k ->
            ids.computeIfAbsent(context, c -> new AtomicInteger(0)).incrementAndGet());
      }
      generateVersion(subject, schema);
      idSchemaMap.put(schemaId, schema);
      return schemaId;
    } else {
      throw new RestClientException("Schema Not Found", 404, 40403);
    }
  }

  private void generateVersion(String subject, ParsedSchema schema) {
    List<Integer> versions = allVersions(subject);
    int currentVersion;
    if (versions.isEmpty()) {
      currentVersion = 1;
    } else {
      currentVersion = versions.get(versions.size() - 1) + 1;
    }
    Map<ParsedSchema, Integer> schemaVersionMap =
        versionCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());
    schemaVersionMap.put(schema, currentVersion);
  }

  private ParsedSchema getSchemaBySubjectAndIdFromRegistry(String subject, int id)
      throws IOException, RestClientException {
    Map<Integer, ParsedSchema> idSchemaMap = idCache.get(subject);
    if (idSchemaMap != null) {
      ParsedSchema schema = idSchemaMap.get(id);
      if (schema != null) {
        return schema;
      }
    }
    String context = toQualifiedContext(subject);
    if (!context.equals(subject)) {
      idSchemaMap = idCache.get(context);
      if (idSchemaMap != null) {
        ParsedSchema schema = idSchemaMap.get(id);
        if (schema != null) {
          return schema;
        }
      }
    }
    throw new RestClientException("Subject Not Found", 404, 40401);
  }

  @Override
  public int register(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return register(subject, schema, 0, -1);
  }

  @Override
  public int register(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return register(subject, schema, 0, -1, normalize);
  }

  @Override
  public int register(String subject, ParsedSchema schema, int version, int id)
      throws IOException, RestClientException {
    return register(subject, schema, version, id, false);
  }

  private int register(String subject, ParsedSchema schema, int version, int id, boolean normalize)
      throws IOException, RestClientException {
    if (normalize) {
      schema = schema.normalize();
    }
    Map<ParsedSchema, Integer> schemaIdMap =
        schemaCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());

    Integer schemaId = schemaIdMap.get(schema);
    if (schemaId != null && (id < 0 || id == schemaId)) {
      return schemaId;
    }

    synchronized (this) {
      schemaId = schemaIdMap.get(schema);
      if (schemaId != null && (id < 0 || id == schemaId)) {
        return schemaId;
      }

      int retrievedId = getIdFromRegistry(subject, schema, true, id);
      schemaIdMap.put(schema, retrievedId);
      String context = toQualifiedContext(subject);
      final Map<Integer, ParsedSchema> idSchemaMap = idCache.computeIfAbsent(
          context, k -> new ConcurrentHashMap<>());
      idSchemaMap.put(retrievedId, schema);
      return retrievedId;
    }
  }

  @Override
  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    return getSchemaBySubjectAndId(NO_SUBJECT, id);
  }

  @Override
  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    if (subject == null) {
      subject = NO_SUBJECT;
    }

    final Map<Integer, ParsedSchema> idSchemaMap = idCache
        .computeIfAbsent(subject, k -> new ConcurrentHashMap<>());

    ParsedSchema schema = idSchemaMap.get(id);
    if (schema != null) {
      return schema;
    }

    synchronized (this) {
      schema = idSchemaMap.get(id);
      if (schema != null) {
        return schema;
      }

      ParsedSchema retrievedSchema = getSchemaBySubjectAndIdFromRegistry(subject, id);
      idSchemaMap.put(id, retrievedSchema);
      return retrievedSchema;
    }
  }

  private Stream<ParsedSchema> getSchemasForSubject(
      String subject,
      boolean latestOnly) {
    try {
      List<Integer> versions = getAllVersions(subject);
      if (latestOnly) {
        int length = versions.size();
        versions = versions.subList(length - 1, length);
      }

      List<SchemaMetadata> schemaMetadata = new LinkedList<>();
      for (Integer version: versions) {
        schemaMetadata.add(getSchemaMetadata(subject, version));
      }

      List<ParsedSchema> schemas = new LinkedList<>();
      for (SchemaMetadata metadata: schemaMetadata) {
        schemas.add(getSchemaBySubjectAndId(subject, metadata.getId()));
      }
      return schemas.stream();
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<ParsedSchema> getSchemas(
      String subjectPrefix,
      boolean lookupDeletedSchema,
      boolean latestOnly)
      throws IOException, RestClientException {
    Stream<String> validSubjects = getAllSubjects().stream()
        .filter(subject -> subject.startsWith(subjectPrefix));

    return validSubjects
        .flatMap(subject -> getSchemasForSubject(subject, latestOnly))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException {
    return idCache.entrySet().stream()
            .filter(entry -> entry.getValue().containsKey(id))
            .map(Map.Entry::getKey).collect(Collectors.toCollection(LinkedHashSet::new));
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
    int maxVersion = -1;
    for (Map.Entry<ParsedSchema, Integer> entry : schemaVersionMap.entrySet()) {
      if (version == -1) {
        if (entry.getValue() > maxVersion) {
          schema = entry.getKey();
          maxVersion = entry.getValue();
        }
      } else {
        if (entry.getValue() == version) {
          schema = entry.getKey();
        }
      }
    }
    if (schema == null) {
      throw new RuntimeException(new RestClientException("Subject Not Found", 404, 40401));
    }
    if (maxVersion != -1) {
      version = maxVersion;
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
  public SchemaMetadata getSchemaMetadata(String subject, int version)
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
    if (schema == null) {
      throw new RestClientException("Subject Not Found", 404, 40401);
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
  public SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    int version = getLatestVersion(subject);
    return getSchemaMetadata(subject, version);
  }

  @Override
  public int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getVersion(subject, schema, false);
  }

  @Override
  public int getVersion(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    if (normalize) {
      schema = schema.normalize();
    }
    Map<ParsedSchema, Integer> versions = versionCache.get(subject);
    if (versions != null) {
      return versions.get(schema);
    } else {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
  }

  @Override
  public List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    List<Integer> allVersions = allVersions(subject);
    if (!allVersions.isEmpty()) {
      return allVersions;
    } else {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
  }

  private List<Integer> allVersions(String subject) {
    ArrayList<Integer> allVersions = new ArrayList<>();
    Map<ParsedSchema, Integer> versions = versionCache.get(subject);
    if (versions != null) {
      allVersions.addAll(versions.values());
      Collections.sort(allVersions);
    }
    return allVersions;
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
  public int getId(String subject, ParsedSchema schema) throws IOException, RestClientException {
    return getId(subject, schema, false);
  }

  @Override
  public int getId(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    if (normalize) {
      schema = schema.normalize();
    }
    Map<ParsedSchema, Integer> schemaIdMap =
        schemaCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());

    Integer schemaId = schemaIdMap.get(schema);
    if (schemaId != null) {
      return schemaId;
    }

    synchronized (this) {
      schemaId = schemaIdMap.get(schema);
      if (schemaId != null) {
        return schemaId;
      }

      int retrievedId = getIdFromRegistry(subject, schema, false, -1);
      schemaIdMap.put(schema, retrievedId);
      String context = toQualifiedContext(subject);
      final Map<Integer, ParsedSchema> idSchemaMap = idCache.computeIfAbsent(
          context, k -> new ConcurrentHashMap<>());
      idSchemaMap.put(retrievedId, schema);
      return retrievedId;
    }
  }

  @Override
  public List<Integer> deleteSubject(String subject, boolean isPermanent)
          throws IOException, RestClientException {
    return deleteSubject(null, subject, isPermanent);
  }

  @Override
  public synchronized List<Integer> deleteSubject(
      Map<String, String> requestProperties,
      String subject,
      boolean isPermanent)
      throws IOException, RestClientException {
    schemaCache.remove(subject);
    idCache.remove(subject);
    versionCache.remove(subject);
    compatibilityCache.remove(subject);
    return Collections.singletonList(0);
  }

  @Override
  public Integer deleteSchemaVersion(String subject, String version, boolean isPermanent)
      throws IOException, RestClientException {
    return deleteSchemaVersion(null, subject, version, isPermanent);
  }

  @Override
  public synchronized Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version,
      boolean isPermanent)
      throws IOException, RestClientException {
    Map<ParsedSchema, Integer> schemaVersionMap = versionCache.get(subject);
    if (schemaVersionMap != null) {
      for (Map.Entry<ParsedSchema, Integer> entry : schemaVersionMap.entrySet()) {
        if (entry.getValue().equals(Integer.valueOf(version))) {
          schemaVersionMap.values().remove(entry.getValue());

          if (isPermanent) {
            idCache.get(subject).remove(entry.getValue());
            schemaCache.get(subject).remove(entry.getKey());
          }
          return Integer.valueOf(version);
        }
      }
    }
    return -1;
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
      return Collections.singletonList("Compatibility level not specified.");
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
      throws IOException, RestClientException {
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
  public String setMode(String mode, String subject, boolean force)
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
    List<String> results = new ArrayList<>(schemaCache.keySet());
    Collections.sort(results);
    return results;
  }

  @Override
  public Collection<String> getAllSubjectsByPrefix(String subjectPrefix)
      throws IOException, RestClientException {
    Stream<String> validSubjects = getAllSubjects().stream()
        .filter(subject -> subject.startsWith(subjectPrefix));
    return validSubjects.collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Override
  public synchronized void reset() {
    schemaCache.clear();
    idCache.clear();
    versionCache.clear();
  }

  private static String toQualifiedContext(String subject) {
    QualifiedSubject qualifiedSubject =
        QualifiedSubject.create(QualifiedSubject.DEFAULT_TENANT, subject);
    return qualifiedSubject != null ? qualifiedSubject.toQualifiedContext() : NO_SUBJECT;
  }
}
