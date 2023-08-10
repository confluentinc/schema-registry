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

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.config.SslConfigs;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;

import javax.net.ssl.HostnameVerifier;


/**
 * Thread-safe Schema Registry Client with client side caching.
 */
public class CachedSchemaRegistryClient implements SchemaRegistryClient {

  private static final Logger log = LoggerFactory.getLogger(CachedSchemaRegistryClient.class);

  private final RestService restService;
  private final int cacheCapacity;
  private final Map<String, Map<ParsedSchema, Integer>> schemaCache;
  private final Map<String, Map<Integer, ParsedSchema>> idCache;
  private final Map<String, Map<ParsedSchema, Integer>> versionCache;
  private final Cache<SubjectAndSchema, Long> missingSchemaCache;
  private final Cache<SubjectAndId, Long> missingIdCache;
  private final Map<String, SchemaProvider> providers;

  private static final String NO_SUBJECT = "";
  private static final int HTTP_NOT_FOUND = 404;
  private static final int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;

  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES =
        Collections.singletonMap("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
  }

  public CachedSchemaRegistryClient(String baseUrl, int cacheCapacity) {
    this(new RestService(baseUrl), cacheCapacity);
  }

  public CachedSchemaRegistryClient(List<String> baseUrls, int cacheCapacity) {
    this(new RestService(baseUrls), cacheCapacity);
  }

  public CachedSchemaRegistryClient(RestService restService, int cacheCapacity) {
    this(restService, cacheCapacity, null);
  }

  public CachedSchemaRegistryClient(
      String baseUrl,
      int cacheCapacity,
      Map<String, ?> originals) {
    this(baseUrl, cacheCapacity, originals, null);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int cacheCapacity,
      Map<String, ?> originals) {
    this(baseUrls, cacheCapacity, originals, null);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> originals) {
    this(new RestService(baseUrls), cacheCapacity, providers, originals, null);
  }

  public CachedSchemaRegistryClient(
      String baseUrls,
      int identityMapCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> originals) {
    this(new RestService(baseUrls), identityMapCapacity, providers, originals, null);
  }

  public CachedSchemaRegistryClient(
      RestService restService,
      int cacheCapacity,
      Map<String, ?> configs) {
    this(restService, cacheCapacity, null, configs, null);
  }

  public CachedSchemaRegistryClient(
      String baseUrl,
      int cacheCapacity,
      Map<String, ?> originals,
      Map<String, String> httpHeaders) {
    this(new RestService(baseUrl), cacheCapacity, null, originals, httpHeaders);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int cacheCapacity,
      Map<String, ?> originals,
      Map<String, String> httpHeaders) {
    this(new RestService(baseUrls), cacheCapacity, null, originals, httpHeaders);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> originals,
      Map<String, String> httpHeaders) {
    this(new RestService(baseUrls), cacheCapacity, providers, originals, httpHeaders);
  }

  public CachedSchemaRegistryClient(
      RestService restService,
      int cacheCapacity,
      Map<String, ?> originals,
      Map<String, String> httpHeaders) {
    this(restService, cacheCapacity, null, originals, httpHeaders);
  }

  public CachedSchemaRegistryClient(
      RestService restService,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    this(restService, cacheCapacity, providers, configs, httpHeaders, Ticker.systemTicker());
  }

  public CachedSchemaRegistryClient(
      RestService restService,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> configs,
      Map<String, String> httpHeaders,
      Ticker ticker) {
    this.cacheCapacity = cacheCapacity;
    this.schemaCache = new BoundedConcurrentHashMap<>(cacheCapacity);
    this.idCache = new BoundedConcurrentHashMap<>(cacheCapacity);
    this.versionCache = new BoundedConcurrentHashMap<>(cacheCapacity);
    this.restService = restService;

    long missingIdTTL = SchemaRegistryClientConfig.getMissingIdTTL(configs);
    long missingSchemaTTL = SchemaRegistryClientConfig.getMissingSchemaTTL(configs);
    int maxMissingCacheSize = SchemaRegistryClientConfig.getMaxMissingCacheSize(configs);

    this.missingSchemaCache = CacheBuilder.newBuilder()
        .maximumSize(maxMissingCacheSize)
        .ticker(ticker)
        .expireAfterWrite(missingSchemaTTL, TimeUnit.SECONDS)
        .build();
    this.missingIdCache = CacheBuilder.newBuilder()
        .maximumSize(maxMissingCacheSize)
        .ticker(ticker)
        .expireAfterWrite(missingIdTTL, TimeUnit.SECONDS)
        .build();

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
      Map<String, Object> restConfigs = configs.entrySet().stream()
          .collect(Collectors.toMap(
              e -> e.getKey().startsWith(SchemaRegistryClientConfig.CLIENT_NAMESPACE)
                  ? e.getKey().substring(SchemaRegistryClientConfig.CLIENT_NAMESPACE.length())
                  : e.getKey(),
              Map.Entry::getValue,
              (existing, replacement) -> replacement));
      restService.configure(restConfigs);

      Map<String, Object> sslConfigs = configs.entrySet().stream()
          .filter(e -> e.getKey().startsWith(SchemaRegistryClientConfig.CLIENT_NAMESPACE))
          .collect(Collectors.toMap(
              e -> e.getKey().substring(SchemaRegistryClientConfig.CLIENT_NAMESPACE.length()),
              Map.Entry::getValue));
      SslFactory sslFactory = new SslFactory(sslConfigs);
      if (sslFactory.sslContext() != null) {
        restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
        restService.setHostnameVerifier(getHostnameVerifier(sslConfigs));
      }
    }
  }

  private HostnameVerifier getHostnameVerifier(Map<String, Object> config) {
    String sslEndpointIdentificationAlgo =
            (String) config.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);

    if (sslEndpointIdentificationAlgo == null
            || sslEndpointIdentificationAlgo.equals("none")
            || sslEndpointIdentificationAlgo.isEmpty()) {
      return (hostname, session) -> true;
    }

    return null;
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

  private int registerAndGetId(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return restService.registerSchema(schema.canonicalString(), schema.schemaType(),
        schema.references(), subject, normalize);
  }

  private int registerAndGetId(
      String subject, ParsedSchema schema, int version, int id, boolean normalize)
      throws IOException, RestClientException {
    return restService.registerSchema(schema.canonicalString(), schema.schemaType(),
        schema.references(), subject, version, id, normalize);
  }

  protected ParsedSchema getSchemaByIdFromRegistry(int id, String subject)
      throws IOException, RestClientException {
    if (missingIdCache.getIfPresent(new SubjectAndId(subject, id)) != null) {
      throw new RestClientException("Schema " + id + " not found",
          HTTP_NOT_FOUND, SCHEMA_NOT_FOUND_ERROR_CODE);
    }

    SchemaString restSchema;
    try {
      restSchema = restService.getId(id, subject);
    } catch (RestClientException rce) {
      if (isSchemaNotFoundException(rce)) {
        missingIdCache.put(new SubjectAndId(subject, id), System.currentTimeMillis());
      }
      throw rce;
    }
    Optional<ParsedSchema> schema = parseSchema(
        restSchema.getSchemaType(), restSchema.getSchemaString(), restSchema.getReferences());
    return schema.orElseThrow(() -> new IOException("Invalid schema " + restSchema.getSchemaString()
            + " with refs " + restSchema.getReferences()
            + " of type " + restSchema.getSchemaType()));
  }

  private int getVersionFromRegistry(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    checkMissingSchemaCache(subject, schema, normalize);

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response;
    try {
      response = restService.lookUpSubjectVersion(schema.canonicalString(),
              schema.schemaType(), schema.references(), subject, normalize, true);
    } catch (RestClientException rce) {
      if (isSchemaNotFoundException(rce)) {
        missingSchemaCache.put(
            new SubjectAndSchema(subject, schema, normalize), System.currentTimeMillis());
      }
      throw rce;
    }

    return response.getVersion();
  }

  private int getIdFromRegistry(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    checkMissingSchemaCache(subject, schema, normalize);

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response;
    try {
      response = restService.lookUpSubjectVersion(schema.canonicalString(),
              schema.schemaType(), schema.references(), subject, normalize, false);
    } catch (RestClientException rce) {
      if (isSchemaNotFoundException(rce)) {
        missingSchemaCache.put(
            new SubjectAndSchema(subject, schema, normalize), System.currentTimeMillis());
      }
      throw rce;
    }
    return response.getId();
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
    final Map<ParsedSchema, Integer> schemaIdMap = schemaCache.computeIfAbsent(
        subject, k -> new BoundedConcurrentHashMap<>(cacheCapacity));

    Integer cachedId = schemaIdMap.get(schema);
    if (cachedId != null && (id < 0 || id == cachedId)) {
      return cachedId;
    }

    synchronized (this) {
      cachedId = schemaIdMap.get(schema);
      if (cachedId != null && (id < 0 || id == cachedId)) {
        return cachedId;
      }

      final int retrievedId = id >= 0
          ? registerAndGetId(subject, schema, version, id, normalize)
          : registerAndGetId(subject, schema, normalize);
      schemaIdMap.put(schema, retrievedId);
      String context = toQualifiedContext(subject);
      final Map<Integer, ParsedSchema> idSchemaMap = idCache.computeIfAbsent(
          context, k -> new BoundedConcurrentHashMap<>(cacheCapacity));
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

    final Map<Integer, ParsedSchema> idSchemaMap = idCache.computeIfAbsent(
        subject, k -> new BoundedConcurrentHashMap<>(cacheCapacity));

    ParsedSchema cachedSchema = idSchemaMap.get(id);
    if (cachedSchema != null) {
      return cachedSchema;
    }

    synchronized (this) {
      cachedSchema = idSchemaMap.get(id);
      if (cachedSchema != null) {
        return cachedSchema;
      }

      final ParsedSchema retrievedSchema = getSchemaByIdFromRegistry(id, subject);
      idSchemaMap.put(id, retrievedSchema);
      return retrievedSchema;
    }
  }

  @Override
  public List<ParsedSchema> getSchemas(
          String subjectPrefix,
          boolean lookupDeletedSchema,
          boolean latestOnly)
          throws IOException, RestClientException {
    List<Schema> restSchemas = restService.getSchemas(
            subjectPrefix,
            lookupDeletedSchema,
            latestOnly);
    return restSchemas.stream()
        .map(restSchema -> parseSchema(
                  restSchema.getSchemaType(),
                  restSchema.getSchema(),
                  restSchema.getReferences())
        )
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
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
  public SchemaMetadata getLatestSchemaMetadata(String subject)
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
  public int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getVersion(subject, schema, false);
  }

  @Override
  public int getVersion(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    final Map<ParsedSchema, Integer> schemaVersionMap = versionCache.computeIfAbsent(
        subject, k -> new BoundedConcurrentHashMap<>(cacheCapacity));

    Integer cachedVersion = schemaVersionMap.get(schema);
    if (cachedVersion != null) {
      return cachedVersion;
    }

    synchronized (this) {
      cachedVersion = schemaVersionMap.get(schema);
      if (cachedVersion != null) {
        return cachedVersion;
      }

      final int retrievedVersion = getVersionFromRegistry(subject, schema, normalize);
      schemaVersionMap.put(schema, retrievedVersion);
      return retrievedVersion;
    }
  }

  @Override
  public List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    return restService.getAllVersions(subject);
  }

  @Override
  public int getId(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getId(subject, schema, false);
  }

  @Override
  public int getId(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    final Map<ParsedSchema, Integer> schemaIdMap = schemaCache.computeIfAbsent(
        subject, k -> new BoundedConcurrentHashMap<>(cacheCapacity));

    Integer cachedId = schemaIdMap.get(schema);
    if (cachedId != null) {
      return cachedId;
    }

    synchronized (this) {
      cachedId = schemaIdMap.get(schema);
      if (cachedId != null) {
        return cachedId;
      }

      final int retrievedId = getIdFromRegistry(subject, schema, normalize);
      schemaIdMap.put(schema, retrievedId);
      String context = toQualifiedContext(subject);
      final Map<Integer, ParsedSchema> idSchemaMap = idCache.computeIfAbsent(
          context, k -> new BoundedConcurrentHashMap<>(cacheCapacity));
      idSchemaMap.put(retrievedId, schema);
      return retrievedId;
    }
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
  public void deleteCompatibility(String subject) throws IOException, RestClientException {
    restService.deleteSubjectConfig(subject);
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
  public String setMode(String mode, String subject, boolean force)
      throws IOException, RestClientException {
    ModeUpdateRequest response = restService.setMode(mode, subject, force);
    return response.getMode();
  }

  @Override
  public String getMode() throws IOException, RestClientException {
    Mode response = restService.getMode();
    return response.getMode();
  }

  @Override
  public String getMode(String subject) throws IOException, RestClientException {
    Mode response = restService.getMode(subject);
    return response.getMode();
  }

  @Override
  public void deleteMode(String subject) throws IOException, RestClientException {
    restService.deleteSubjectMode(subject);
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    return restService.getAllSubjects();
  }

  @Override
  public Collection<String> getAllSubjectsByPrefix(String subjectPrefix) throws IOException,
      RestClientException {
    return restService.getAllSubjects(subjectPrefix, false);
  }

  @Override
  public synchronized void reset() {
    schemaCache.clear();
    idCache.clear();
    versionCache.clear();
    missingSchemaCache.invalidateAll();
    missingIdCache.invalidateAll();
  }

  @Override
  public void close() throws IOException {
    if (restService != null) {
      restService.close();
    }
  }

  private void checkMissingSchemaCache(String subject, ParsedSchema schema, boolean normalize)
      throws RestClientException {
    if (missingSchemaCache.getIfPresent(
        new SubjectAndSchema(subject, schema, normalize)) != null) {
      throw new RestClientException("Schema not found",
          HTTP_NOT_FOUND, SCHEMA_NOT_FOUND_ERROR_CODE);
    }
  }

  private boolean isSchemaNotFoundException(RestClientException rce) {
    return rce.getStatus() == HTTP_NOT_FOUND && rce.getErrorCode() == SCHEMA_NOT_FOUND_ERROR_CODE;
  }

  private static String toQualifiedContext(String subject) {
    QualifiedSubject qualifiedSubject =
        QualifiedSubject.create(QualifiedSubject.DEFAULT_TENANT, subject);
    return qualifiedSubject != null ? qualifiedSubject.toQualifiedContext() : NO_SUBJECT;
  }

  static class SubjectAndSchema {
    private final String subject;
    private final ParsedSchema schema;
    private final boolean normalize;

    public SubjectAndSchema(String subject, ParsedSchema schema, boolean normalize) {
      this.subject = subject;
      this.schema = schema;
      this.normalize = normalize;
    }

    public String subject() {
      return subject;
    }

    public ParsedSchema schema() {
      return schema;
    }

    public boolean normalize() {
      return normalize;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubjectAndSchema that = (SubjectAndSchema) o;
      return Objects.equals(subject, that.subject) && schema.equals(that.schema)
          && normalize == that.normalize;
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, schema, normalize);
    }

    @Override
    public String toString() {
      return "SubjectAndSchema{" + "subject='" + subject + '\'' + ", schema=" + schema
          + ", normalize=" + normalize + '}';
    }
  }

  static class SubjectAndId {
    private final String subject;
    private final int id;

    public SubjectAndId(String subject, int id) {
      this.subject = subject;
      this.id = id;
    }

    public String subject() {
      return subject;
    }

    public int id() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubjectAndId that = (SubjectAndId) o;
      return Objects.equals(subject, that.subject) && id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, id);
    }

    @Override
    public String toString() {
      return "SubjectAndId{" + "subject='" + subject + '\'' + ", id=" + id + '}';
    }
  }
}
