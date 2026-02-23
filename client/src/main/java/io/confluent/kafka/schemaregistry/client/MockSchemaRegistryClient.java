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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryDeployment;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;

import java.util.LinkedHashSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.Arrays;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
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

  private static final int DEFAULT_CAPACITY = 1000;
  private static final String RESOURCE_WILDCARD = "-";
  private static final String WILDCARD = "*";
  private static final String DEFAULT_RESOURCE_TYPE = "topic";
  private static final String DEFAULT_ASSOCIATION_TYPE = "value";
  private static final LifecyclePolicy DEFAULT_LIFECYCLE_POLICY = LifecyclePolicy.STRONG;
  private static final Map<String, List<String>> RESOURCE_TYPE_TO_ASSOC_TYPE_MAP =
          new HashMap<String, List<String>>() {{
            put(DEFAULT_RESOURCE_TYPE, Arrays.asList("key", DEFAULT_ASSOCIATION_TYPE));
          }};

  private Config defaultConfig = new Config("BACKWARD");
  private final Map<String, Map<ParsedSchema, RegisterSchemaResponse>> schemaToResponseCache;
  private final Map<String, Map<ParsedSchema, Integer>> registeredSchemaCache;
  private final Map<String, Map<Integer, ParsedSchema>> idToSchemaCache;
  private final Map<String, ParsedSchema> guidToSchemaCache;
  private final Map<String, Map<ParsedSchema, Integer>> schemaToVersionCache;
  private final Map<String, Config> configCache;
  private final Map<String, List<Association>> subjectToAssocCache;
  private final Map<ResourceAndAssocType, Association> resourceAndAssocTypeCache;
  private final Map<String, List<Association>> resourceIdToAssocCache;
  private final Map<String, Map<String, List<Association>>> resourceNameToAssocCache;
  private final Map<String, String> modes;
  private final Map<String, AtomicInteger> ids;
  private final LoadingCache<Schema, ParsedSchema> parsedSchemaCache;
  private final Map<String, SchemaProvider> providers = new HashMap<>();

  private static final String NO_SUBJECT = "";

  private static class ResourceAndAssocType {
    String resourceId;
    String resourceType;
    String associationType;

    public ResourceAndAssocType(String resourceId,
                                String resourceType,
                                String associationType) {
      this.resourceId = resourceId;
      this.resourceType = resourceType;
      this.associationType = associationType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(resourceId, resourceType, associationType);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      ResourceAndAssocType other = (ResourceAndAssocType) obj;
      return Objects.equals(resourceId, other.resourceId)
              && Objects.equals(resourceType, other.resourceType)
              && Objects.equals(associationType, other.associationType);
    }
  }

  public MockSchemaRegistryClient() {
    this(null);
  }

  public MockSchemaRegistryClient(List<SchemaProvider> providers) {
    schemaToResponseCache = new ConcurrentHashMap<>();
    registeredSchemaCache = new ConcurrentHashMap<>();
    idToSchemaCache = new ConcurrentHashMap<>();
    guidToSchemaCache = new ConcurrentHashMap<>();
    schemaToVersionCache = new ConcurrentHashMap<>();
    configCache = new ConcurrentHashMap<>();
    subjectToAssocCache = new ConcurrentHashMap<>();
    resourceAndAssocTypeCache = new ConcurrentHashMap<>();
    resourceIdToAssocCache = new ConcurrentHashMap<>();
    resourceNameToAssocCache = new ConcurrentHashMap<>();
    modes = new ConcurrentHashMap<>();
    ids = new ConcurrentHashMap<>();
    if (providers == null || providers.isEmpty()) {
      providers = Collections.singletonList(new AvroSchemaProvider());
    }
    addProviders(providers);

    final Map<String, SchemaProvider> schemaProviders = this.providers;
    this.parsedSchemaCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CAPACITY)
        .build(new CacheLoader<Schema, ParsedSchema>() {
          @Override
          public ParsedSchema load(Schema schema) throws Exception {
            String schemaType = schema.getSchemaType();
            if (schemaType == null) {
              schemaType = AvroSchema.TYPE;
            }
            SchemaProvider schemaProvider = schemaProviders.get(schemaType);
            if (schemaProvider == null) {
              log.error("Invalid schema type {}", schemaType);
              throw new IllegalStateException("Invalid schema type " + schemaType);
            }
            return schemaProvider.parseSchema(schema, false, false).orElseThrow(
                () -> new IOException("Invalid schema of type " + schema.getSchemaType()));
          }
        });
  }

  public Map<String, SchemaProvider> getProviders() {
    return Collections.unmodifiableMap(providers);
  }

  public void addProviders(List<SchemaProvider> providers) {
    for (SchemaProvider provider : providers) {
      this.providers.put(provider.schemaType(), provider);
      Map<String, Object> schemaProviderConfigs = new HashMap<>();
      schemaProviderConfigs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, this);
      provider.configure(schemaProviderConfigs);
    }
  }

  @Override
  public Optional<ParsedSchema> parseSchema(
      String schemaType,
      String schemaString,
      List<SchemaReference> references) {
    return parseSchema(new Schema(null, null, null, schemaType, references, schemaString));
  }

  @Override
  public Optional<ParsedSchema> parseSchema(Schema schema) {
    try {
      return Optional.of(parsedSchemaCache.get(schema));
    } catch (ExecutionException e) {
      return Optional.empty();
    }
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
    return schema1.equals(schema2) || schema2.canLookup(schema1, this);
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
    Map<ParsedSchema, RegisterSchemaResponse> schemaResponseMap =
        schemaToResponseCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());

    RegisterSchemaResponse schemaResponse = schemaResponseMap.get(schema);
    if (schemaResponse != null && (id < 0 || id == schemaResponse.getId())) {
      return schemaResponse;
    }

    synchronized (this) {
      schemaResponse = schemaResponseMap.get(schema);
      if (schemaResponse != null && (id < 0 || id == schemaResponse.getId())) {
        return schemaResponse;
      }

      int retrievedId = getIdFromRegistry(subject, schema, true, id);
      Schema schemaEntity = new Schema(subject, version, retrievedId, schema);
      schemaResponse = new RegisterSchemaResponse(schemaEntity);
      schemaResponseMap.put(schema, schemaResponse);
      String context = toQualifiedContext(subject);
      final Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.computeIfAbsent(
          context, k -> new ConcurrentHashMap<>());
      idSchemaMap.put(retrievedId, schema);
      guidToSchemaCache.put(schemaEntity.getGuid(), schema);
      return schemaResponse;
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
    if (versions != null && versions.containsKey(schema)) {
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

  private int latestVersion(String subject) {
    List<Integer> versions = allVersions(subject);
    if (versions.isEmpty()) {
      return -1;
    }
    return versions.get(versions.size() - 1);
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
    Map<ParsedSchema, RegisterSchemaResponse> schemaResponseMap =
        schemaToResponseCache.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());

    RegisterSchemaResponse schemaResponse = schemaResponseMap.get(schema);
    if (schemaResponse != null) {
      return schemaResponse;
    }

    synchronized (this) {
      schemaResponse = schemaResponseMap.get(schema);
      if (schemaResponse != null) {
        return schemaResponse;
      }

      int retrievedId = getIdFromRegistry(subject, schema, false, -1);
      Schema schemaEntity = new Schema(subject, null, retrievedId, schema);
      schemaResponse = new RegisterSchemaResponse(schemaEntity);
      schemaResponseMap.put(schema, schemaResponse);
      String context = toQualifiedContext(subject);
      final Map<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.computeIfAbsent(
          context, k -> new ConcurrentHashMap<>());
      idSchemaMap.put(retrievedId, schema);
      return schemaResponse;
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
    // make sure this subject has no associations
    List<Association> associations = getAssociationsBySubject(subject, null, null, null, 0, -1);
    if (!associations.isEmpty()) {
      throw new RestClientException("Associations found", 409, 40921);
    }
    return deleteSubjectNoAssociationsCheck(requestProperties, subject, isPermanent);
  }

  private List<Integer> deleteSubjectNoAssociationsCheck(
          Map<String, String> requestProperties,
          String subject,
          boolean isPermanent)
          throws IOException, RestClientException {
    schemaToResponseCache.remove(subject);
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
            schemaToResponseCache.get(subject).remove(entry.getKey());
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
    List<String> results = new ArrayList<>(schemaToResponseCache.keySet()).stream()
        .map(s -> QualifiedSubject.create(DEFAULT_TENANT, s).getContext())
        .sorted()
        .distinct()
        .collect(Collectors.toList());
    return results;
  }

  @Override
  public SchemaRegistryDeployment getSchemaRegistryDeployment()
      throws IOException, RestClientException {
    // For the mock client, return an empty deployment (default behavior)
    return new SchemaRegistryDeployment();
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    List<String> results = new ArrayList<>(schemaToResponseCache.keySet());
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
    schemaToResponseCache.clear();
    registeredSchemaCache.clear();
    idToSchemaCache.clear();
    guidToSchemaCache.clear();
    schemaToVersionCache.clear();
    subjectToAssocCache.clear();
    resourceAndAssocTypeCache.clear();
    resourceIdToAssocCache.clear();
    resourceNameToAssocCache.clear();
    configCache.clear();
    modes.clear();
    ids.clear();
  }

  private static String toQualifiedContext(String subject) {
    QualifiedSubject qualifiedSubject =
        QualifiedSubject.create(DEFAULT_TENANT, subject);
    return qualifiedSubject != null ? qualifiedSubject.toQualifiedContext() : NO_SUBJECT;
  }

  private boolean validateResourceTypeAndAssociationType(
          String resourceType, String associationType) {
    if (!RESOURCE_TYPE_TO_ASSOC_TYPE_MAP.containsKey(resourceType)) {
      return false;
    }
    if (!RESOURCE_TYPE_TO_ASSOC_TYPE_MAP.get(resourceType).contains(associationType)) {
      return false;
    }
    return true;
  }

  private void checkAssociationTypeUniqueness(AssociationCreateOrUpdateRequest request)
          throws RestClientException {
    Map<String, AssociationCreateOrUpdateInfo> infosByType = new HashMap<>();
    for (AssociationCreateOrUpdateInfo info : request.getAssociations()) {
      String associationType = info.getAssociationType();
      if (infosByType.containsKey(associationType)) {
        throw new RestClientException(
                String.format(
                        "The association specified an invalid value for property: %s",
                        associationType), 422, 42212);
      }
      infosByType.put(associationType, info);
    }
  }

  private void checkSubjectExists(AssociationCreateOrUpdateRequest request)
          throws RestClientException {
    for (AssociationCreateOrUpdateInfo associationInRequest : request.getAssociations()) {
      String subject = associationInRequest.getSubject();
      int latestVersion = latestVersion(subject);
      if (associationInRequest.getSchema() == null && latestVersion < 0) {
        throw new RestClientException(
                String.format("No active (non-deleted) version exists for subject '%s", subject),
                409, 40907);
      }
    }
  }

  private void checkExistingAssociationsByResourceId(AssociationCreateOrUpdateRequest request,
                                                     boolean isCreateOnly)
          throws RestClientException {
    String resourceId = request.getResourceId();
    String resourceType = request.getResourceType();

    for (AssociationCreateOrUpdateInfo associationInRequest : request.getAssociations()) {
      String subject = associationInRequest.getSubject();
      String associationType = associationInRequest.getAssociationType();
      RegisterSchemaRequest schema = associationInRequest.getSchema();

      ResourceAndAssocType key = new ResourceAndAssocType(
              resourceId, resourceType, associationType);

      Association existingAssociation = resourceAndAssocTypeCache.get(key);

      if (existingAssociation == null) {
        // If on create, frozen is set to true, ensure that a schema is being
        // passed in, and that no other schemas exist in the subject
        if (Boolean.TRUE.equals(associationInRequest.getFrozen())) {
          if (schema == null) {
            throw new RestClientException(
                "schema must be provided when creating a frozen association", 422, 42212);
          }
          if (latestVersion(subject) >= 0) {
            throw new RestClientException(
                "cannot create a frozen association when schemas already exist in the subject",
                422, 42212);
          }
        }
        continue;
      }
      if (existingAssociation.isEquivalent(associationInRequest)) {
        if (isCreateOnly && schema != null && !schemaExistsInRegistry(subject, schema)) {
          throw new RestClientException(String.format(
                  "An association of type '%s' already exists for resource '%s",
                  associationType, resourceId), 422, 42212);
        }
      } else {
        if (isCreateOnly) {
          throw new RestClientException(String.format(
                  "An association of type '%s' already exists for resource '%s",
                  associationType, resourceId), 422, 42212);
        }
        // Only lifecycle and frozen can be updated
        // frozen can only be changed from weak to strong, not the other way around
        // subject must stay the same
        if (!existingAssociation.getSubject().equals(subject)) {
          throw new RestClientException(String.format(
                  "The association specified an invalid value for property: '%s', detail: %s",
                  "subject", "subject of association cannot be changed"),
                  422, 42212);
        }
        // Don't allow the frozen attribute to be updated
        if (associationInRequest.getFrozen() != null
                && existingAssociation.isFrozen() != associationInRequest.getFrozen()) {
          throw new RestClientException(String.format(
                  "The association specified an invalid value for property: '%s', detail: %s",
                  "frozen", "frozen attribute of association cannot be changed"), 422, 42212);
        }
        if (existingAssociation.isFrozen()) {
          throw new RestClientException(String.format(
                  "The association of type '%s' is frozen for subject '%s'",
                  associationType, subject), 409, 40908);
        }
        // If existing association is weak but request is frozen, return false
        if (existingAssociation.getLifecycle() == LifecyclePolicy.WEAK
                && Boolean.TRUE.equals(associationInRequest.getFrozen())) {
          throw new RestClientException(String.format(
                  "The association specified an invalid value for property: '%s', detail: %s",
                  "frozen", "association with lifecycle of WEAK cannot be frozen"),
                  422, 42212);
        }
      }
    }
  }

  private void checkExistingAssociationsBySubject(AssociationCreateOrUpdateRequest request)
          throws RestClientException {
    String resourceId = request.getResourceId();
    String resourceType = request.getResourceType();

    for (AssociationCreateOrUpdateInfo associationInRequest : request.getAssociations()) {
      String subject = associationInRequest.getSubject();
      String associationType = associationInRequest.getAssociationType();
      // Filter out the associationInRequest
      List<Association> existingAssociations = subjectToAssocCache.get(subject);
      if (existingAssociations != null) {
        existingAssociations = existingAssociations.stream().filter(associationBySubject ->
                        !associationBySubject.getResourceType().equals(resourceType)
                     || !associationBySubject.getResourceId().equals(resourceId)
                     || !associationBySubject.getAssociationType().equals(associationType))
                .collect(Collectors.toList());
        if (!existingAssociations.isEmpty()) {
          if (associationInRequest.getLifecycle() == LifecyclePolicy.STRONG) {
            throw new RestClientException(String.format(
                    "An association of type '%s', already exists for subject '%s",
                    associationType, subject), 409, 40904);
          }
          if (existingAssociations.get(0).getLifecycle() == LifecyclePolicy.STRONG) {
            throw new RestClientException(
                    String.format(
                            "A strong association of type '%s' already exists for subject '%s",
                            associationInRequest.getAssociationType(), subject), 409, 40905);
          }
        }
      }
    }
  }

  private List<Association> writeAssociationsToCaches(AssociationCreateOrUpdateRequest request) {
    String resourceId = request.getResourceId();
    String resourceType = request.getResourceType();
    String resourceName = request.getResourceName();
    String resourceNamespace = request.getResourceNamespace();

    List<Association> results = new ArrayList<>();
    for (AssociationCreateOrUpdateInfo associationInRequest : request.getAssociations()) {
      ResourceAndAssocType key = new ResourceAndAssocType(resourceId, resourceType,
              associationInRequest.getAssociationType());

      if (resourceAndAssocTypeCache.containsKey(key)) {
        // Modify existing association in place
        Association existingAssociation = resourceAndAssocTypeCache.get(key);
        if (associationInRequest.getLifecycle() != null) {
          existingAssociation.setLifecycle(associationInRequest.getLifecycle());
        }
        if (associationInRequest.getFrozen() != null) {
          existingAssociation.setFrozen(associationInRequest.getFrozen());
        }
        results.add(resourceAndAssocTypeCache.get(key));
        continue;
      }

      Association newAssociation = new Association(
              associationInRequest.getSubject(), UUID.randomUUID().toString(),
              resourceName, resourceNamespace, resourceId, resourceType,
              associationInRequest.getAssociationType(),
              associationInRequest.getLifecycle() == LifecyclePolicy.STRONG
                      ? LifecyclePolicy.STRONG
                      : LifecyclePolicy.WEAK,
              Boolean.TRUE.equals(associationInRequest.getFrozen()));

      // Update all caches
      resourceAndAssocTypeCache.put(key, newAssociation);
      subjectToAssocCache.computeIfAbsent(newAssociation.getSubject(),
              k -> new ArrayList<>()).add(newAssociation);
      resourceIdToAssocCache.computeIfAbsent(resourceId,
              k -> new ArrayList<>()).add(newAssociation);
      resourceNameToAssocCache
              .computeIfAbsent(resourceName, k -> new ConcurrentHashMap<>())
              .computeIfAbsent(resourceNamespace,
                      k -> new CopyOnWriteArrayList<>()).add(newAssociation);
      results.add(newAssociation);
    }
    return results;
  }

  private synchronized AssociationResponse createOrUpdateAssociationHelper(
      AssociationCreateOrUpdateRequest request, boolean isCreateOnly)
      throws IOException, RestClientException {
    // Check that association types are unique
    checkAssociationTypeUniqueness(request);

    // Make sure subject exists if the schema in request is null
    // The schema compatibility check will be done through post new schema directly
    checkSubjectExists(request);

    // Check whether the resource already has an association
    checkExistingAssociationsByResourceId(request, isCreateOnly);

    // Check if subject can accept new association
    checkExistingAssociationsBySubject(request);

    // Post all schemas
    postAllSchemasFromAssociationRequest(request);

    // Write associations to caches
    List<Association> results = writeAssociationsToCaches(request);

    // Create response
    return createResponseFromAssociationCreateOrUpdateRequest(request, results);
  }

  private AssociationResponse createResponseFromAssociationCreateOrUpdateRequest(
          AssociationCreateOrUpdateRequest request, List<Association> associations) {
    String resourceId = request.getResourceId();
    String resourceType = request.getResourceType();
    String resourceName = request.getResourceName();
    String resourceNamespace = request.getResourceNamespace();
    List<AssociationInfo> infos = new ArrayList<>();
    for (int i = 0; i < associations.size(); i++) {
      Association association = associations.get(i);
      AssociationCreateOrUpdateInfo createOrUpdateInfo = request.getAssociations().get(i);
      infos.add(new AssociationInfo(association.getSubject(),
              association.getAssociationType(),
              association.getLifecycle(),
              association.isFrozen(),
              createOrUpdateInfo.getSchema() != null
                      ? new Schema(createOrUpdateInfo.getSubject(),
                      createOrUpdateInfo.getSchema())
                      : null));
    }
    return new AssociationResponse(
            resourceName, resourceNamespace, resourceId, resourceType, infos);
  }

  private void postAllSchemasFromAssociationRequest(AssociationCreateOrUpdateRequest request)
          throws RestClientException, IOException {
    for (AssociationCreateOrUpdateInfo associationInRequest : request.getAssociations()) {
      if (associationInRequest.getSchema() != null) {
        register(associationInRequest.getSubject(),
                parseSchema(new Schema(
                        associationInRequest.getSubject(),
                        associationInRequest.getSchema())).get(),
                Boolean.TRUE.equals(associationInRequest.getNormalize()));
      }
    }
  }

  private boolean schemaExistsInRegistry(String subject, RegisterSchemaRequest schema) {
    ParsedSchema parsedSchema = parseSchema(new Schema(subject, schema)).get();
    try {
      getIdFromRegistry(subject, parsedSchema, false, -1);
    } catch (RestClientException e) {
      return false;
    }
    return true;
  }

  private void validateAssociationCreateOrUpdateRequest(AssociationCreateOrUpdateRequest request) {
    request.validate(false);
    // Validate each association
    for (AssociationCreateOrUpdateInfo associationCreateInfo : request.getAssociations()) {
      // Validate resource type and association type
      if (!validateResourceTypeAndAssociationType(
              request.getResourceType(), associationCreateInfo.getAssociationType())) {
        throw new IllegalArgumentException(
                String.format("resourceType {} and associationType {} don't match",
                        request.getResourceType(), associationCreateInfo.getAssociationType()));
      }
    }
  }

  public AssociationResponse createOrUpdateAssociation(AssociationCreateOrUpdateRequest request)
          throws IOException, RestClientException {
    try {
      validateAssociationCreateOrUpdateRequest(request);
    } catch (Exception e) {
      throw new RestClientException(
              String.format(
                      "The association specified an invalid value for property, %s",
                      e.getMessage()),
              422, 42212);
    }
    AssociationResponse response = createOrUpdateAssociationHelper(request, false);
    return response;
  }

  public AssociationResponse createAssociation(AssociationCreateOrUpdateRequest request)
          throws IOException, RestClientException {
    try {
      validateAssociationCreateOrUpdateRequest(request);
    } catch (Exception e) {
      throw new RestClientException(
              String.format(
                      "The association specified an invalid value for property, %s",
                      e.getMessage()),
              422, 42212);
    }
    AssociationResponse response = createOrUpdateAssociationHelper(request, true);
    return response;
  }

  public List<Association> getAssociationsBySubject(String subject, String resourceType,
                                                    List<String> associationTypes,
                                                    String lifecycle, int offset, int limit)
          throws IOException, RestClientException {
    if (subject == null || subject.isEmpty()) {
      throw new RestClientException("Association parameters are invalid", 422, 42212);
    }
    if (lifecycle != null) {
      try {
        LifecyclePolicy.valueOf(lifecycle);
      } catch (IllegalArgumentException e) {
        throw new RestClientException("Association parameters are invalid", 422, 42212);
      }
    }

    List<Association> associations = subjectToAssocCache.get(subject);

    if (associations == null || associations.isEmpty()) {
      return new ArrayList<>();  // Return empty list
    }
    List<Association> filtered = associations.stream()
            .filter(association ->
                    resourceType == null || association.getResourceType().equals(resourceType))
            .filter(association ->
                    associationTypes == null
                            || associationTypes.isEmpty()
                            || associationTypes.contains(association.getAssociationType()))
            .filter(association ->
                    lifecycle == null || association.getLifecycle().toString().equals(lifecycle))
            .collect(Collectors.toList());

    // Apply pagination
    int start = offset;
    if (start > filtered.size()) {
      start = filtered.size();
    }

    int end = start + limit;
    if (limit <= 0 || end > filtered.size()) {
      end = filtered.size();
    }
    return filtered.subList(start, end);
  }

  public List<Association> getAssociationsByResourceId(String resourceId, String resourceType,
                                                       List<String> associationTypes,
                                                       String lifecycle, int offset, int limit)
          throws IOException, RestClientException {
    if (resourceId == null || resourceId.isEmpty()) {
      throw new RestClientException("Association parameters are invalid", 422, 42212);
    }
    List<Association> associations = resourceIdToAssocCache.get(resourceId);
    if (lifecycle != null) {
      try {
        LifecyclePolicy.valueOf(lifecycle);
      } catch (IllegalArgumentException e) {
        throw new RestClientException("Association parameters are invalid", 422, 42212);
      }
    }

    if (associations == null || associations.isEmpty()) {
      return new ArrayList<>();  // Return empty list
    }
    List<Association> filtered = associations.stream()
            .filter(association ->
                    resourceType == null || association.getResourceType().equals(resourceType))
            .filter(association ->
                    associationTypes == null
                            || associationTypes.isEmpty()
                            || associationTypes.contains(association.getAssociationType()))
            .filter(association ->
                    lifecycle == null || association.getLifecycle().toString().equals(lifecycle))
            .collect(Collectors.toList());

    // Apply pagination
    int start = offset;
    if (start > filtered.size()) {
      start = filtered.size();
    }

    int end = start + limit;
    if (limit <= 0 || end > filtered.size()) {
      end = filtered.size();
    }
    return filtered.subList(start, end);
  }

  public List<Association> getAssociationsByResourceName(String resourceName,
                                                         String resourceNamespace,
                                                         String resourceType,
                                                         List<String> associationTypes,
                                                         String lifecycle, int offset, int limit)
          throws IOException, RestClientException {
    if (resourceName == null || resourceName.isEmpty()) {
      throw new RestClientException("Association parameters are invalid", 422, 42212);
    }
    if (lifecycle != null) {
      try {
        LifecyclePolicy.valueOf(lifecycle);
      } catch (IllegalArgumentException e) {
        throw new RestClientException("Association parameters are invalid", 422, 42212);
      }
    }

    Map<String, List<Association>> namespaceMap = resourceNameToAssocCache.get(resourceName);
    if (namespaceMap == null || namespaceMap.isEmpty()) {
      return new ArrayList<>();  // Return empty list
    }

    List<Association> associations = new ArrayList<>();
    // If resourceNamespace is null or "-", collect from all namespaces
    if (resourceNamespace == null || RESOURCE_WILDCARD.equals(resourceNamespace)) {
      for (List<Association> assocList : namespaceMap.values()) {
        associations.addAll(assocList);
      }
    } else {
      // Get associations from specific namespace
      List<Association> namespaceAssociations = namespaceMap.get(resourceNamespace);
      if (namespaceAssociations != null) {
        associations.addAll(namespaceAssociations);
      }
    }

    if (associations.isEmpty()) {
      return new ArrayList<>();  // Return empty list
    }

    List<Association> filtered = associations.stream()
            .filter(association ->
                    resourceType == null || association.getResourceType().equals(resourceType))
            .filter(association ->
                    associationTypes == null
                            || associationTypes.isEmpty()
                            || associationTypes.contains(association.getAssociationType()))
            .filter(association ->
                    lifecycle == null || association.getLifecycle().toString().equals(lifecycle))
            .collect(Collectors.toList());

    // Apply pagination
    int start = offset;
    if (start > filtered.size()) {
      start = filtered.size();
    }

    int end = start + limit;
    if (limit <= 0 || end > filtered.size()) {
      end = filtered.size();
    }
    return filtered.subList(start, end);
  }

  private void checkDeleteAssociation(Association association, boolean cascadeLifecycle)
          throws RestClientException {
    if (!cascadeLifecycle && association.isFrozen()) {
      throw new RestClientException(String.format(
              "The association of type '%s' is frozen for subject '%s",
              association.getAssociationType(), association.getSubject()), 409, 40908);
    }
  }

  private void deleteAssociation(Association association, boolean cascadeLifecycle)
          throws IOException, RestClientException {
    String subject = association.getSubject();
    String resourceId = association.getResourceId();
    if (cascadeLifecycle && association.getLifecycle() == LifecyclePolicy.STRONG) {
      deleteSubjectNoAssociationsCheck(null, subject, false);
      deleteSubjectNoAssociationsCheck(null, subject, true);
    }
    resourceNameToAssocCache.computeIfPresent(association.getResourceName(), (k, map) -> {
      map.computeIfPresent(association.getResourceNamespace(), (k2, list) -> {
        list.remove(association);
        return list.isEmpty() ? null : list;
      });
      return map.isEmpty() ? null : map;
    });
    resourceIdToAssocCache.computeIfPresent(resourceId, (k, list) -> {
      list.remove(association);
      return list.isEmpty() ? null : list;
    });
    subjectToAssocCache.computeIfPresent(subject, (k, list) -> {
      list.remove(association);
      return list.isEmpty() ? null : list;
    });
    ResourceAndAssocType resourceAndAssocType = new ResourceAndAssocType(
            association.getResourceId(), association.getResourceType(),
            association.getAssociationType()
    );
    resourceAndAssocTypeCache.remove(resourceAndAssocType);
  }

  public synchronized void deleteAssociations(String resourceId, String resourceType,
                                              List<String> associationTypes,
                                              boolean cascadeLifecycle)
          throws IOException, RestClientException {
    List<Association> associationsToDelete = getAssociationsByResourceId(
            resourceId, resourceType, associationTypes, null, 0, -1);

    if (associationsToDelete == null || associationsToDelete.isEmpty()) {
      return;
    }

    for (Association associationToDelete : associationsToDelete) {
      checkDeleteAssociation(associationToDelete, cascadeLifecycle);
    }

    for (Association associationToDelete : associationsToDelete) {
      deleteAssociation(associationToDelete, cascadeLifecycle);
    }
  }
}
