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

import java.util.Collections;
import java.util.Objects;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeGetResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;


/**
 * Thread-safe Schema Registry Client with client side caching.
 */
public class CachedSchemaRegistryClient implements SchemaRegistryClient {

  private static final Logger log = LoggerFactory.getLogger(CachedSchemaRegistryClient.class);

  private final RestService restService;
  private final int identityMapCapacity;
  private final Map<String, Map<ParsedSchema, Integer>> schemaCache;
  private final Map<String, Map<Integer, ParsedSchema>> idCache;
  private final Map<String, Map<ParsedSchema, Integer>> versionCache;
  private final Map<String, SchemaProvider> providers;

  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES =
        Collections.singletonMap("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
  }

  public CachedSchemaRegistryClient(String baseUrl, int identityMapCapacity) {
    this(new RestService(baseUrl), identityMapCapacity);
  }

  public CachedSchemaRegistryClient(List<String> baseUrls, int identityMapCapacity) {
    this(new RestService(baseUrls), identityMapCapacity);
  }

  public CachedSchemaRegistryClient(RestService restService, int identityMapCapacity) {
    this(restService, identityMapCapacity, null);
  }

  public CachedSchemaRegistryClient(
      String baseUrl,
      int identityMapCapacity,
      Map<String, ?> originals) {
    this(baseUrl, identityMapCapacity, originals, null);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int identityMapCapacity,
      Map<String, ?> originals) {
    this(baseUrls, identityMapCapacity, originals, null);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int identityMapCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> originals) {
    this(new RestService(baseUrls), identityMapCapacity, providers, originals, null);
  }

  public CachedSchemaRegistryClient(
      RestService restService,
      int identityMapCapacity,
      Map<String, ?> configs) {
    this(restService, identityMapCapacity, null, configs, null);
  }

  public CachedSchemaRegistryClient(
      String baseUrl,
      int identityMapCapacity,
      Map<String, ?> originals,
      Map<String, String> httpHeaders) {
    this(new RestService(baseUrl), identityMapCapacity, null, originals, httpHeaders);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int identityMapCapacity,
      Map<String, ?> originals,
      Map<String, String> httpHeaders) {
    this(new RestService(baseUrls), identityMapCapacity, null, originals, httpHeaders);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int identityMapCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> originals,
      Map<String, String> httpHeaders) {
    this(new RestService(baseUrls), identityMapCapacity, providers, originals, httpHeaders);
  }

  public CachedSchemaRegistryClient(
      RestService restService,
      int identityMapCapacity,
      Map<String, ?> originals,
      Map<String, String> httpHeaders) {
    this(restService, identityMapCapacity, null, originals, httpHeaders);
  }

  public CachedSchemaRegistryClient(
      RestService restService,
      int identityMapCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    this.identityMapCapacity = identityMapCapacity;
    this.schemaCache = new HashMap<String, Map<ParsedSchema, Integer>>();
    this.idCache = new HashMap<String, Map<Integer, ParsedSchema>>();
    this.versionCache = new HashMap<String, Map<ParsedSchema, Integer>>();
    this.restService = restService;
    this.idCache.put(null, new HashMap<Integer, ParsedSchema>());
    this.providers = providers != null && !providers.isEmpty()
                     ? providers.stream().collect(Collectors.toMap(p -> p.schemaType(), p -> p))
                     : Collections.singletonMap(AvroSchema.TYPE, new AvroSchemaProvider());
    Map<String, Object> schemaProviderConfigs = new HashMap<>();
    schemaProviderConfigs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, this);
    for (SchemaProvider provider : this.providers.values()) {
      provider.configure(schemaProviderConfigs);
    }
    if (httpHeaders != null) {
      restService.setHttpHeaders(httpHeaders);
    }
    if (configs != null && !configs.isEmpty()) {
      restService.configure(configs);

      Map<String, Object> sslConfigs = configs.entrySet().stream()
          .filter(e -> e.getKey().startsWith(SchemaRegistryClientConfig.CLIENT_NAMESPACE))
          .collect(Collectors.toMap(
              e -> e.getKey().substring(SchemaRegistryClientConfig.CLIENT_NAMESPACE.length()),
              Map.Entry::getValue));
      SslFactory sslFactory = new SslFactory(sslConfigs);
      if (sslFactory != null && sslFactory.sslContext() != null) {
        restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
      }
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
      log.error("Invalid schema type " + schemaType);
      return Optional.empty();
    }
    return schemaProvider.parseSchema(schemaString, references);
  }

  public Map<String, SchemaProvider> getSchemaProviders() {
    return providers;
  }

  private int registerAndGetId(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return restService.registerSchema(schema.canonicalString(), schema.schemaType(),
        schema.references(), subject);
  }

  private int registerAndGetId(String subject, ParsedSchema schema, int version, int id)
      throws IOException, RestClientException {
    return restService.registerSchema(schema.canonicalString(), schema.schemaType(),
        schema.references(), subject, version, id);
  }

  protected ParsedSchema getSchemaByIdFromRegistry(int id) throws IOException, RestClientException {
    SchemaString restSchema = restService.getId(id);
    Optional<ParsedSchema> schema = parseSchema(
        restSchema.getSchemaType(), restSchema.getSchemaString(), restSchema.getReferences());
    return schema.orElseThrow(() -> new IOException("Invalid schema " + restSchema.getSchemaString()
            + " with refs " + restSchema.getReferences()
            + " of type " + restSchema.getSchemaType()));
  }

  private int getVersionFromRegistry(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response =
        restService.lookUpSubjectVersion(schema.canonicalString(),
            schema.schemaType(), schema.references(), subject, true);
    return response.getVersion();
  }

  private int getIdFromRegistry(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response =
        restService.lookUpSubjectVersion(schema.canonicalString(),
            schema.schemaType(), schema.references(), subject, false);
    return response.getId();
  }

  @Override
  public synchronized int register(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return register(subject, schema, 0, -1);
  }

  @Override
  public synchronized int register(String subject, ParsedSchema schema, int version, int id)
      throws IOException, RestClientException {
    final Map<ParsedSchema, Integer> schemaIdMap =
        schemaCache.computeIfAbsent(subject, k -> new HashMap<>());

    final Integer cachedId = schemaIdMap.get(schema);
    if (cachedId != null) {
      if (id >= 0 && id != cachedId) {
        throw new IllegalStateException("Schema already registered with id "
            + cachedId + " instead of input id " + id);
      }
      return cachedId;
    }

    if (schemaIdMap.size() >= identityMapCapacity) {
      throw new IllegalStateException("Too many schema objects created for " + subject + "!");
    }

    final int retrievedId = id >= 0
                            ? registerAndGetId(subject, schema, version, id)
                            : registerAndGetId(subject, schema);
    schemaIdMap.put(schema, retrievedId);
    idCache.get(null).put(retrievedId, schema);
    return retrievedId;
  }

  @Override
  public synchronized ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    return getSchemaBySubjectAndId(null, id);
  }

  @Override
  public synchronized ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {

    final Map<Integer, ParsedSchema> idSchemaMap = idCache
        .computeIfAbsent(subject, k -> new HashMap<>());

    final ParsedSchema cachedSchema = idSchemaMap.get(id);
    if (cachedSchema != null) {
      return cachedSchema;
    }

    final ParsedSchema retrievedSchema = getSchemaByIdFromRegistry(id);
    idSchemaMap.put(id, retrievedSchema);
    return retrievedSchema;
  }

  @Override
  public Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException {
    return restService.getAllSubjectsById(id);
  }

  @Override
  public Collection<SubjectVersion> getAllVersionsById(int id) throws IOException,
      RestClientException {
    return restService.getAllVersionsById(id);
  }

  @Override
  public Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
    try {
      return restService.getVersion(subject, version, lookupDeletedSchema);
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
        = restService.getVersion(subject, version);
    int id = response.getId();
    String schemaType = response.getSchemaType();
    String schema = response.getSchema();
    List<SchemaReference> references = response.getReferences();
    return new SchemaMetadata(id, version, schemaType, references, schema);
  }

  @Override
  public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
        = restService.getLatestVersion(subject);
    int id = response.getId();
    int version = response.getVersion();
    String schemaType = response.getSchemaType();
    String schema = response.getSchema();
    List<SchemaReference> references = response.getReferences();
    return new SchemaMetadata(id, version, schemaType, references, schema);
  }

  @Override
  public synchronized int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    final Map<ParsedSchema, Integer> schemaVersionMap =
        versionCache.computeIfAbsent(subject, k -> new HashMap<>());

    final Integer cachedVersion = schemaVersionMap.get(schema);
    if (cachedVersion != null) {
      return cachedVersion;
    }

    if (schemaVersionMap.size() >= identityMapCapacity) {
      throw new IllegalStateException("Too many schema objects created for " + subject + "!");
    }

    final int retrievedVersion = getVersionFromRegistry(subject, schema);
    schemaVersionMap.put(schema, retrievedVersion);
    return retrievedVersion;
  }

  @Override
  public List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    return restService.getAllVersions(subject);
  }

  @Override
  public synchronized int getId(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    final Map<ParsedSchema, Integer> schemaIdMap =
        schemaCache.computeIfAbsent(subject, k -> new HashMap<>());

    final Integer cachedId = schemaIdMap.get(schema);
    if (cachedId != null) {
      return cachedId;
    }

    if (schemaIdMap.size() >= identityMapCapacity) {
      throw new IllegalStateException("Too many schema objects created for " + subject + "!");
    }

    final int retrievedId = getIdFromRegistry(subject, schema);
    schemaIdMap.put(schema, retrievedId);
    idCache.get(null).put(retrievedId, schema);
    return retrievedId;
  }

  @Override
  public List<Integer> deleteSubject(String subject,
           boolean isPermanent) throws IOException, RestClientException {
    return deleteSubject(DEFAULT_REQUEST_PROPERTIES, subject, isPermanent);
  }

  @Override
  public synchronized List<Integer> deleteSubject(
      Map<String, String> requestProperties, String subject, boolean isPermanent)
      throws IOException, RestClientException {
    Objects.requireNonNull(subject, "subject");
    versionCache.remove(subject);
    idCache.remove(subject);
    schemaCache.remove(subject);
    return restService.deleteSubject(requestProperties, subject, isPermanent);
  }

  @Override
  public Integer deleteSchemaVersion(String subject, String version, boolean isPermanent)
      throws IOException, RestClientException {
    return deleteSchemaVersion(DEFAULT_REQUEST_PROPERTIES, subject, version, isPermanent);
  }

  @Override
  public synchronized Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version,
      boolean isPermanent)
      throws IOException, RestClientException {
    versionCache
        .getOrDefault(subject, Collections.emptyMap())
        .values()
        .remove(Integer.valueOf(version));
    return restService.deleteSchemaVersion(requestProperties, subject, version, isPermanent);
  }

  @Override
  public boolean testCompatibility(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return restService.testCompatibility(schema.canonicalString(), schema.schemaType(),
        schema.references(), subject, "latest", false).isEmpty();
  }

  @Override
  public List<String> testCompatibilityVerbose(String subject, ParsedSchema schema)
          throws IOException, RestClientException {
    return restService.testCompatibility(schema.canonicalString(), schema.schemaType(),
            schema.references(), subject, "latest", true);
  }

  @Override
  public String updateCompatibility(String subject, String compatibility)
      throws IOException, RestClientException {
    ConfigUpdateRequest response = restService.updateCompatibility(compatibility, subject);
    return response.getCompatibilityLevel();
  }

  @Override
  public String getCompatibility(String subject) throws IOException, RestClientException {
    Config response = restService.getConfig(subject);
    return response.getCompatibilityLevel();
  }

  @Override
  public String setMode(String mode)
      throws IOException, RestClientException {
    ModeUpdateRequest response = restService.setMode(mode);
    return response.getMode();
  }

  @Override
  public String setMode(String mode, String subject)
      throws IOException, RestClientException {
    ModeUpdateRequest response = restService.setMode(mode, subject);
    return response.getMode();
  }

  @Override
  public String getMode() throws IOException, RestClientException {
    ModeGetResponse response = restService.getMode();
    return response.getMode();
  }

  @Override
  public String getMode(String subject) throws IOException, RestClientException {
    ModeGetResponse response = restService.getMode(subject);
    return response.getMode();
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    return restService.getAllSubjects();
  }

  @Override
  public void reset() {
    schemaCache.clear();
    idCache.clear();
    versionCache.clear();
    idCache.put(null, new HashMap<Integer, ParsedSchema>());
  }
}
