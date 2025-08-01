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

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_TENANT;

import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.LinkedHashSet;
import java.util.SortedMap;
import java.util.TreeMap;
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

  private Config defaultConfig = new Config("BACKWARD");
  private final Map<String, Map<ParsedSchema, Integer>> schemaToIdCache;
  private final Map<String, Map<ParsedSchema, Integer>> registeredSchemaCache;
  private final Map<String, Map<Integer, ParsedSchema>> idToSchemaCache;
  private final Map<String, ParsedSchema> guidToSchemaCache;
  private final Map<String, Map<ParsedSchema, Integer>> schemaToVersionCache;
  private final Map<String, Config> configCache;
  private final Map<String, String> modes;
  private final Map<String, AtomicInteger> ids;
  private final Map<String, SchemaProvider> providers;

  private static final String NO_SUBJECT = "";

  public MockSchemaRegistryClient() {
    this(null);
  }

  public MockSchemaRegistryClient(List<SchemaProvider> providers) {
    schemaToIdCache = new ConcurrentHashMap<>();
    registeredSchemaCache = new ConcurrentHashMap<>();
    idToSchemaCache = new ConcurrentHashMap<>();
    guidToSchemaCache = new ConcurrentHashMap<>();
    schemaToVersionCache = new ConcurrentHashMap<>();
    configCache = new ConcurrentHashMap<>();
    modes = new ConcurrentHashMap<>();
    ids = new ConcurrentHashMap<>();
    this.providers = providers != null && !providers.isEmpty()
                     ? providers.stream()
                           .collect(Collectors.toMap(SchemaProvider::schemaType, p -> p))
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
      log.error("No provider found for schema type {}", schemaType);
      return Optional.empty();
    }
    return schemaProvider.parseSchema(schemaString, references);
  }

  @Override
  public Optional<ParsedSchema> parseSchema(Schema schema) {
    String schemaType = schema.getSchemaType();
    if (schemaType == null) {
      schemaType = AvroSchema.TYPE;
    }
    SchemaProvider schemaProvider = providers.get(schemaType);
    if (schemaProvider == null) {
      log.error("Invalid schema type {}", schemaType);
      return Optional.empty();
    }
    return schemaProvider.parseSchema(schema, false, false);
  }

  private int getIdFromRegistry(
      String subject, ParsedSchema schema, boolean registerRequest, int id)
      throws RestClientException {
    Map<Integer, ParsedSchema> idSchemaMap =
        idToSchemaCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());
    if (!idSchemaMap.isEmpty()) {
      for (Map.Entry<Integer, ParsedSchema> entry : idSchemaMap.entrySet()) {
        if (schemasEqual(entry.getValue(), schema)) {
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
          registeredSchemaCache.computeIfAbsent(context, k -> new ConcurrentHashMap<>());
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

  private boolean schemasEqual(ParsedSchema schema1, ParsedSchema schema2) {
    return schema1.canonicalString().equals(schema2.canonicalString())
        || schema1.canLookup(schema2, this);
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
        schemaToVersionCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());
    schemaVersionMap.put(schema, currentVersion);
  }

  private ParsedSchema getSchemaBySubjectAndIdFromRegistry(String subject, int id)
      throws RestClientException {
    Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.get(subject);
    if (idSchemaMap != null) {
      ParsedSchema schema = idSchemaMap.get(id);
      if (schema != null) {
        return schema;
      }
    }
    String context = toQualifiedContext(subject);
    if (!context.equals(subject)) {
      idSchemaMap = idToSchemaCache.get(context);
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
    return registerWithResponse(subject, schema, 0, -1, normalize, false).getId();
  }

  @Override
  public int register(String subject, ParsedSchema schema, int version, int id)
      throws IOException, RestClientException {
    return registerWithResponse(subject, schema, version, id, false, false).getId();
  }

  @Override
  public RegisterSchemaResponse registerWithResponse(
      String subject, ParsedSchema schema, boolean normalize, boolean propagateSchemaTags)
      throws RestClientException {
    return registerWithResponse(subject, schema, 0, -1, normalize, propagateSchemaTags);
  }

  private RegisterSchemaResponse registerWithResponse(
      String subject, ParsedSchema schema, int version, int id,
      boolean normalize, boolean propagateSchemaTags)
      throws RestClientException {
    if (normalize) {
      schema = schema.normalize();
    }
    Map<ParsedSchema, Integer> schemaIdMap =
        schemaToIdCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());

    Integer schemaId = schemaIdMap.get(schema);
    if (schemaId != null && (id < 0 || id == schemaId)) {
      return new RegisterSchemaResponse(new Schema(subject, version, schemaId, schema));
    }

    synchronized (this) {
      schemaId = schemaIdMap.get(schema);
      if (schemaId != null && (id < 0 || id == schemaId)) {
        return new RegisterSchemaResponse(new Schema(subject, version, schemaId, schema));
      }

      int retrievedId = getIdFromRegistry(subject, schema, true, id);
      schemaIdMap.put(schema, retrievedId);
      String context = toQualifiedContext(subject);
      final Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.computeIfAbsent(
          context, k -> new ConcurrentHashMap<>());
      idSchemaMap.put(retrievedId, schema);
      Schema schemaEntity = new Schema(subject, version, retrievedId, schema);
      guidToSchemaCache.put(schemaEntity.getGuid(), schema);
      return new RegisterSchemaResponse(schemaEntity);
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

    final Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache
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

  @Override
  public ParsedSchema getSchemaByGuid(String guid, String format)
      throws IOException, RestClientException {
    return guidToSchemaCache.get(guid);
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
  public Collection<String> getAllSubjectsById(int id) {
    return idToSchemaCache.entrySet().stream()
            .filter(entry -> entry.getValue().containsKey(id))
            .map(Map.Entry::getKey).collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Override
  public Collection<SubjectVersion> getAllVersionsById(int id) {
    return idToSchemaCache.entrySet().stream()
        .filter(entry -> entry.getValue().containsKey(id))
        .flatMap(e -> {
          ParsedSchema schema = e.getValue().get(id);
          Map<ParsedSchema, Integer> schemaVersionMap = schemaToVersionCache.get(e.getKey());
          if (schemaVersionMap != null) {
            int version = schemaVersionMap.get(schema);
            return Stream.of(new SubjectVersion(e.getKey(), version));
          } else {
            return Stream.empty();
          }
        })
        .distinct()
        .collect(Collectors.toList());
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
    Map<ParsedSchema, Integer> schemaVersionMap = schemaToVersionCache.get(subject);
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
    Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.get(subject);
    for (Map.Entry<Integer, ParsedSchema> entry : idSchemaMap.entrySet()) {
      if (schemasEqual(entry.getValue(), schema)) {
        id = entry.getKey();
      }
    }
    return new Schema(subject, version, id, schema);
  }

  @Override
  public SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException {
    return getSchemaMetadata(subject, version, false);
  }

  @Override
  public SchemaMetadata getSchemaMetadata(String subject, int version,
      boolean lookupDeletedSchema) throws RestClientException {
    ParsedSchema schema = null;
    Map<ParsedSchema, Integer> schemaVersionMap = schemaToVersionCache.get(subject);
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
    Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.get(subject);
    for (Map.Entry<Integer, ParsedSchema> entry : idSchemaMap.entrySet()) {
      if (schemasEqual(entry.getValue(), schema)) {
        id = entry.getKey();
      }
    }
    return new SchemaMetadata(new Schema(subject, version, id, schema));
  }

  @Override
  public SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    int version = getLatestVersion(subject);
    return getSchemaMetadata(subject, version);
  }

  @Override
  public SchemaMetadata getLatestWithMetadata(String subject, Map<String, String> metadata,
      boolean lookupDeletedSchema) throws IOException, RestClientException {
    Map<ParsedSchema, Integer> versions = schemaToVersionCache.get(subject);
    SortedMap<Integer, ParsedSchema> reverseMap = new TreeMap<>(Collections.reverseOrder());
    for (Map.Entry<ParsedSchema, Integer> entry : versions.entrySet()) {
      reverseMap.put(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<Integer, ParsedSchema> entry : reverseMap.entrySet()) {
      Integer version = entry.getKey();
      ParsedSchema schema = entry.getValue();
      Metadata schemaMetadata = schema.metadata();
      if (schemaMetadata != null) {
        Map<String, String> props = schemaMetadata.getProperties();
        if (props != null && props.entrySet().containsAll(metadata.entrySet())) {
          int id = -1;
          Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.get(subject);
          for (Map.Entry<Integer, ParsedSchema> e : idSchemaMap.entrySet()) {
            if (schemasEqual(e.getValue(), schema)) {
              id = e.getKey();
            }
          }
          return new SchemaMetadata(new Schema(subject, version, id, schema));
        }
      }
    }
    throw new RestClientException("Schema Not Found", 404, 40403);
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
    Map<ParsedSchema, Integer> versions = schemaToVersionCache.get(subject);
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
    Map<ParsedSchema, Integer> versions = schemaToVersionCache.get(subject);
    if (versions != null) {
      allVersions.addAll(versions.values());
      Collections.sort(allVersions);
    }
    return allVersions;
  }

  @Override
  public boolean testCompatibility(String subject, ParsedSchema newSchema) throws IOException,
                                                                            RestClientException {
    Config config = configCache.get(subject);
    if (config == null) {
      config = defaultConfig;
    }

    CompatibilityLevel compatibilityLevel =
        CompatibilityLevel.forName(config.getCompatibilityLevel());
    if (compatibilityLevel == null) {
      return false;
    }

    List<ParsedSchemaHolder> schemaHistory = new ArrayList<>();
    for (int version : allVersions(subject)) {
      SchemaMetadata schemaMetadata = getSchemaMetadata(subject, version);
      schemaHistory.add(new SimpleParsedSchemaHolder(getSchemaBySubjectAndIdFromRegistry(subject,
          schemaMetadata.getId())));
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
    return getIdWithResponse(subject, schema, normalize).getId();
  }

  public String getGuid(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getGuid(subject, schema, false);
  }

  public String getGuid(
      String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return getIdWithResponse(subject, schema, normalize).getGuid();
  }

  @Override
  public RegisterSchemaResponse getIdWithResponse(
      String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    if (normalize) {
      schema = schema.normalize();
    }
    Map<ParsedSchema, Integer> schemaIdMap =
        schemaToIdCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());

    Integer schemaId = schemaIdMap.get(schema);
    if (schemaId != null) {
      Schema schemaEntity = new Schema(subject, null, schemaId, schema);
      return new RegisterSchemaResponse(schemaEntity);
    }

    synchronized (this) {
      schemaId = schemaIdMap.get(schema);
      if (schemaId != null) {
        Schema schemaEntity = new Schema(subject, null, schemaId, schema);
        return new RegisterSchemaResponse(schemaEntity);
      }

      int retrievedId = getIdFromRegistry(subject, schema, false, -1);
      schemaIdMap.put(schema, retrievedId);
      String context = toQualifiedContext(subject);
      final Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.computeIfAbsent(
          context, k -> new ConcurrentHashMap<>());
      idSchemaMap.put(retrievedId, schema);
      Schema schemaEntity = new Schema(subject, null, retrievedId, schema);
      return new RegisterSchemaResponse(schemaEntity);
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
    schemaToIdCache.remove(subject);
    idToSchemaCache.remove(subject);
    Map<ParsedSchema, Integer> versions = schemaToVersionCache.remove(subject);
    configCache.remove(subject);
    return versions != null
        ? versions.values().stream().sorted().collect(Collectors.toList())
        : Collections.emptyList();
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
    Map<ParsedSchema, Integer> schemaVersionMap = schemaToVersionCache.get(subject);
    if (schemaVersionMap != null) {
      for (Map.Entry<ParsedSchema, Integer> entry : schemaVersionMap.entrySet()) {
        if (entry.getValue().equals(Integer.valueOf(version))) {
          schemaVersionMap.values().remove(entry.getValue());

          if (isPermanent) {
            idToSchemaCache.get(subject).remove(entry.getValue());
            schemaToIdCache.get(subject).remove(entry.getKey());
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
    Config config = configCache.get(subject);
    if (config == null) {
      config = defaultConfig;
    }

    CompatibilityLevel compatibilityLevel =
        CompatibilityLevel.forName(config.getCompatibilityLevel());
    if (compatibilityLevel == null) {
      return Lists.newArrayList("Compatibility level not specified.");
    }

    List<ParsedSchemaHolder> schemaHistory = new ArrayList<>();
    for (int version : allVersions(subject)) {
      SchemaMetadata schemaMetadata = getSchemaMetadata(subject, version);
      schemaHistory.add(new SimpleParsedSchemaHolder(getSchemaBySubjectAndIdFromRegistry(subject,
          schemaMetadata.getId())));
    }

    return newSchema.isCompatible(compatibilityLevel, schemaHistory);
  }

  @Override
  public Config updateConfig(String subject, Config config)
      throws IOException, RestClientException {
    if (subject == null) {
      defaultConfig = config;
      return config;
    }
    configCache.put(subject, config);
    return config;
  }

  @Override
  public Config getConfig(String subject) throws IOException, RestClientException {
    if (subject == null) {
      return defaultConfig;
    }
    Config config = configCache.get(subject);
    if (config == null) {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
    return config;
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
  public Collection<String> getAllContexts() throws IOException, RestClientException {
    List<String> results = new ArrayList<>(schemaToIdCache.keySet()).stream()
        .map(s -> QualifiedSubject.create(DEFAULT_TENANT, s).getContext())
        .sorted()
        .distinct()
        .collect(Collectors.toList());
    return results;
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    List<String> results = new ArrayList<>(schemaToIdCache.keySet());
    Collections.sort(results);
    return results;
  }

  @Override
  public Collection<String> getAllSubjectsByPrefix(String subjectPrefix)
      throws IOException, RestClientException {
    Stream<String> validSubjects = getAllSubjects().stream()
        .filter(subject -> subjectPrefix == null || subject.startsWith(subjectPrefix));
    return validSubjects.collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Override
  public synchronized void reset() {
    schemaToIdCache.clear();
    registeredSchemaCache.clear();
    idToSchemaCache.clear();
    guidToSchemaCache.clear();
    schemaToVersionCache.clear();
    configCache.clear();
    modes.clear();
    ids.clear();
  }

  private static String toQualifiedContext(String subject) {
    QualifiedSubject qualifiedSubject =
        QualifiedSubject.create(DEFAULT_TENANT, subject);
    return qualifiedSubject != null ? qualifiedSubject.toQualifiedContext() : NO_SUBJECT;
  }
}
