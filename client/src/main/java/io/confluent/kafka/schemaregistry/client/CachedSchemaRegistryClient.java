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
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryServerVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryDeployment;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;

import javax.net.ssl.HostnameVerifier;


/**
 * Thread-safe Schema Registry Client with client side caching.
 */
public class CachedSchemaRegistryClient implements SchemaRegistryClient {

  private static final Logger log = LoggerFactory.getLogger(CachedSchemaRegistryClient.class);

  private final RestService restService;
  private final int cacheCapacity;
  private final Cache<String, Cache<ParsedSchema, RegisterSchemaResponse>> schemaToResponseCache;
  private final Cache<String, Cache<ParsedSchema, Integer>> schemaToIdCache;
  private final Cache<String, Cache<Integer, ParsedSchema>> idToSchemaCache;
  private final Cache<String, ParsedSchema> guidToSchemaCache;
  private final Cache<String, Cache<ParsedSchema, String>> schemaToGuidCache;
  private final Cache<String, Cache<ParsedSchema, Integer>> schemaToVersionCache;
  private final Cache<String, Cache<Integer, Schema>> versionToSchemaCache;
  private final Cache<String, SchemaMetadata> latestVersionCache;
  private final Cache<SubjectAndMetadata, SchemaMetadata> latestWithMetadataCache;
  private final Cache<SubjectAndSchema, Long> missingSchemaCache;
  private final Cache<SubjectAndInt, Long> missingIdCache;
  private final Cache<String, Long> missingGuidCache;
  private final Cache<SubjectAndInt, Long> missingVersionCache;
  private final LoadingCache<Schema, ParsedSchema> parsedSchemaCache;
  private final Map<String, SchemaProvider> providers;
  private final Ticker ticker;

  private static final String NO_SUBJECT = "";
  private static final int HTTP_NOT_FOUND = 404;
  private static final int VERSION_NOT_FOUND_ERROR_CODE = 40402;
  private static final int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;
  private static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES = RestService.DEFAULT_REQUEST_PROPERTIES;
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
          String baseUrls,
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
    this.schemaToResponseCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .build();
    this.schemaToIdCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .build();
    this.idToSchemaCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .build();
    this.schemaToGuidCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .build();
    this.guidToSchemaCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .build();
    this.schemaToVersionCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .build();
    this.versionToSchemaCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .build();
    this.restService = restService;
    this.ticker = ticker;

    long latestTTL = SchemaRegistryClientConfig.getLatestTTL(configs);

    CacheBuilder<Object, Object> latestVersionBuilder = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .ticker(ticker);
    if (latestTTL >= 0) {
      latestVersionBuilder = latestVersionBuilder.expireAfterWrite(
          latestTTL, TimeUnit.SECONDS);
    }
    this.latestVersionCache = latestVersionBuilder.build();
    CacheBuilder<Object, Object> latestWithMetadataBuilder = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .ticker(ticker);
    if (latestTTL >= 0) {
      latestWithMetadataBuilder = latestWithMetadataBuilder.expireAfterWrite(
          latestTTL, TimeUnit.SECONDS);
    }
    this.latestWithMetadataCache = latestWithMetadataBuilder.build();

    int maxMissingCacheSize = SchemaRegistryClientConfig.getMaxMissingCacheSize(configs);

    long missingSchemaTTL = SchemaRegistryClientConfig.getMissingSchemaTTL(configs);
    this.missingSchemaCache = CacheBuilder.newBuilder()
        .maximumSize(maxMissingCacheSize)
        .ticker(ticker)
        .expireAfterWrite(missingSchemaTTL, TimeUnit.SECONDS)
        .build();

    long missingIdTTL = SchemaRegistryClientConfig.getMissingIdTTL(configs);
    this.missingIdCache = CacheBuilder.newBuilder()
        .maximumSize(maxMissingCacheSize)
        .ticker(ticker)
        .expireAfterWrite(missingIdTTL, TimeUnit.SECONDS)
        .build();

    this.missingGuidCache = CacheBuilder.newBuilder()
        .maximumSize(maxMissingCacheSize)
        .ticker(ticker)
        .expireAfterWrite(missingIdTTL, TimeUnit.SECONDS)
        .build();

    long missingVersionTTL = SchemaRegistryClientConfig.getMissingVersionTTL(configs);
    this.missingVersionCache = CacheBuilder.newBuilder()
        .maximumSize(maxMissingCacheSize)
        .ticker(ticker)
        .expireAfterWrite(missingVersionTTL, TimeUnit.SECONDS)
        .build();

    this.providers = providers != null && !providers.isEmpty()
        ? providers.stream().collect(Collectors.toMap(SchemaProvider::schemaType, p -> p))
        : Collections.singletonMap(AvroSchema.TYPE, new AvroSchemaProvider());
    Map<String, Object> schemaProviderConfigs = new HashMap<>();
    schemaProviderConfigs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, this);
    for (SchemaProvider provider : this.providers.values()) {
      provider.configure(schemaProviderConfigs);
    }

    final Map<String, SchemaProvider> schemaProviders = this.providers;
    this.parsedSchemaCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
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
            return schemaProvider.parseSchemaOrElseThrow(schema, false, false);
          }
        });

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

  @Override
  public Ticker ticker() {
    return ticker;
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

  @Override
  public ParsedSchema parseSchemaOrElseThrow(Schema schema) throws IOException {
    try {
      return parsedSchemaCache.get(schema);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause != null) {
        throw new IOException(cause);
      }
      throw new IOException(e);
    }
  }

  public Map<String, SchemaProvider> getSchemaProviders() {
    return providers;
  }

  private RegisterSchemaResponse registerAndGetId(
      String subject, ParsedSchema schema, boolean normalize, boolean propagateSchemaTags)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);
    if (propagateSchemaTags) {
      request.setPropagateSchemaTags(true);
    }
    return restService.registerSchema(request, subject, normalize);
  }

  private RegisterSchemaResponse registerAndGetId(
      String subject, ParsedSchema schema, int version, int id,
      boolean normalize, boolean propagateSchemaTags)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);
    request.setVersion(version);
    request.setId(id);
    if (propagateSchemaTags) {
      request.setPropagateSchemaTags(true);
    }
    return restService.registerSchema(request, subject, normalize);
  }

  protected ParsedSchema getSchemaByIdFromRegistry(int id, String subject)
      throws IOException, RestClientException {
    if (missingIdCache.getIfPresent(new SubjectAndInt(subject, id)) != null) {
      throw new RestClientException("Schema " + id + " not found",
          HTTP_NOT_FOUND, SCHEMA_NOT_FOUND_ERROR_CODE);
    }

    SchemaString restSchema;
    try {
      restSchema = restService.getId(id, subject);
    } catch (RestClientException rce) {
      if (isSchemaOrSubjectNotFoundException(rce)) {
        missingIdCache.put(new SubjectAndInt(subject, id), System.currentTimeMillis());
      }
      throw rce;
    }
    return parseSchemaOrElseThrow(new Schema(null, null, null, restSchema));
  }

  protected ParsedSchema getSchemaByGuidFromRegistry(String guid, String format)
      throws IOException, RestClientException {
    String cacheKey = format != null ? guid + ":" + format : guid;
    if (missingGuidCache.getIfPresent(cacheKey) != null) {
      throw new RestClientException("Schema " + guid + " not found",
          HTTP_NOT_FOUND, SCHEMA_NOT_FOUND_ERROR_CODE);
    }

    SchemaString restSchema;
    try {
      restSchema = restService.getByGuid(guid, format);
    } catch (RestClientException rce) {
      if (isSchemaOrSubjectNotFoundException(rce)) {
        missingGuidCache.put(cacheKey, System.currentTimeMillis());
      }
      throw rce;
    }
    Optional<ParsedSchema> schema = parseSchema(new Schema(null, null, null, restSchema));
    return schema.orElseThrow(() -> new IOException("Invalid schema " + restSchema.getSchemaString()
        + " with refs " + restSchema.getReferences()
        + " of type " + restSchema.getSchemaType()));
  }

  private int getVersionFromRegistry(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return getIdWithResponseFromRegistry(subject, schema, normalize, true).getVersion();
  }

  private int getIdFromRegistry(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return getIdWithResponseFromRegistry(subject, schema, normalize, false).getId();
  }

  private String getGuidFromRegistry(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return getIdWithResponseFromRegistry(subject, schema, normalize, false).getGuid();
  }

  private RegisterSchemaResponse getIdWithResponseFromRegistry(
      String subject, ParsedSchema schema, boolean normalize, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    checkMissingSchemaCache(subject, schema, normalize);

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema schemaEntity;
    RegisterSchemaResponse response;
    try {
      RegisterSchemaRequest request = new RegisterSchemaRequest(schema);
      schemaEntity = restService.lookUpSubjectVersion(
          request, subject, normalize, lookupDeletedSchema);
      response = new RegisterSchemaResponse(schemaEntity);
    } catch (RestClientException rce) {
      if (isSchemaOrSubjectNotFoundException(rce)) {
        missingSchemaCache.put(
            new SubjectAndSchema(subject, schema, normalize), System.currentTimeMillis());
      }
      throw rce;
    }
    return response;
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
      throws IOException, RestClientException {
    return registerWithResponse(subject, schema, 0, -1, normalize, propagateSchemaTags);
  }

  private RegisterSchemaResponse registerWithResponse(
      String subject, ParsedSchema schema, int version, int id,
      boolean normalize, boolean propagateSchemaTags)
      throws IOException, RestClientException {
    try {
      final Cache<ParsedSchema, RegisterSchemaResponse> schemaResponseMap =
          schemaToResponseCache.get(subject, () -> CacheBuilder.newBuilder()
              .maximumSize(cacheCapacity)
              .build());

      RegisterSchemaResponse cachedResponse = schemaResponseMap.getIfPresent(schema);
      if (cachedResponse != null && (id < 0 || id == cachedResponse.getId())) {
        return cachedResponse;
      }

      synchronized (this) {
        cachedResponse = schemaResponseMap.getIfPresent(schema);
        if (cachedResponse != null && (id < 0 || id == cachedResponse.getId())) {
          return cachedResponse;
        }

        final RegisterSchemaResponse retrievedResponse = id >= 0
            ? registerAndGetId(subject, schema, version, id, normalize, propagateSchemaTags)
            : registerAndGetId(subject, schema, normalize, propagateSchemaTags);
        schemaResponseMap.put(schema, retrievedResponse);
        String context = toQualifiedContext(subject);
        final Cache<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.get(
            context, () -> CacheBuilder.newBuilder()
                .maximumSize(cacheCapacity)
                .build());
        idSchemaMap.put(retrievedResponse.getId(), schema);
        return retrievedResponse;
      }
    } catch (ExecutionException e) {
      throw new IOException("Error accessing cache", e);
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

    try {
      final Cache<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.get(
          subject, () -> CacheBuilder.newBuilder()
              .maximumSize(cacheCapacity)
              .build());

      ParsedSchema cachedSchema = idSchemaMap.getIfPresent(id);
      if (cachedSchema != null) {
        return cachedSchema;
      }

      synchronized (this) {
        cachedSchema = idSchemaMap.getIfPresent(id);
        if (cachedSchema != null) {
          return cachedSchema;
        }

        final ParsedSchema retrievedSchema = getSchemaByIdFromRegistry(id, subject);
        idSchemaMap.put(id, retrievedSchema);
        return retrievedSchema;
      }
    } catch (ExecutionException e) {
      throw new IOException("Error accessing cache", e);
    }
  }

  @Override
  public ParsedSchema getSchemaByGuid(String guid, String format)
      throws IOException, RestClientException {
    String cacheKey = format != null ? guid + ":" + format : guid;
    ParsedSchema cachedSchema = guidToSchemaCache.getIfPresent(cacheKey);
    if (cachedSchema != null) {
      return cachedSchema;
    }

    synchronized (this) {
      cachedSchema = guidToSchemaCache.getIfPresent(cacheKey);
      if (cachedSchema != null) {
        return cachedSchema;
      }

      final ParsedSchema retrievedSchema = getSchemaByGuidFromRegistry(guid, format);
      guidToSchemaCache.put(cacheKey, retrievedSchema);
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
        .map(this::parseSchema)
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
      return getSchemaByVersion(subject, version, lookupDeletedSchema);
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  private Schema getSchemaByVersion(String subject, int version, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    try {
      final Cache<Integer, Schema> versionSchemaMap = versionToSchemaCache.get(
          subject, () -> CacheBuilder.newBuilder()
              .maximumSize(cacheCapacity)
              .build());

      // The cache is only used when lookupDeletedSchema is true
      Schema cachedSchema = lookupDeletedSchema ? versionSchemaMap.getIfPresent(version) : null;
      if (cachedSchema != null) {
        return cachedSchema;
      }

      synchronized (this) {
        cachedSchema = lookupDeletedSchema ? versionSchemaMap.getIfPresent(version) : null;
        if (cachedSchema != null) {
          return cachedSchema;
        }

        final Schema retrievedSchema = getSchemaByVersionFromRegistry(
            subject, version, lookupDeletedSchema);
        // The cache is only used when lookupDeletedSchema is true
        if (lookupDeletedSchema) {
          versionSchemaMap.put(version, retrievedSchema);
        }
        return retrievedSchema;
      }
    } catch (ExecutionException e) {
      throw new IOException("Error accessing cache", e);
    }
  }

  private Schema getSchemaByVersionFromRegistry(
      String subject, int version, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    if (lookupDeletedSchema
        && missingVersionCache.getIfPresent(new SubjectAndInt(subject, version)) != null) {
      throw new RestClientException("Version " + version + " not found",
          HTTP_NOT_FOUND, VERSION_NOT_FOUND_ERROR_CODE);
    }

    Schema restSchema;
    try {
      restSchema = restService.getVersion(subject, version, lookupDeletedSchema);
    } catch (RestClientException rce) {
      if (lookupDeletedSchema && isVersionNotFoundException(rce)) {
        missingVersionCache.put(new SubjectAndInt(subject, version), System.currentTimeMillis());
      }
      throw rce;
    }

    return restSchema;
  }

  @Override
  public SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException {
    return getSchemaMetadata(subject, version, false);
  }

  @Override
  public SchemaMetadata getSchemaMetadata(String subject, int version, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
        = getSchemaByVersion(subject, version, lookupDeletedSchema);
    return new SchemaMetadata(response);
  }

  @Override
  public SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    SchemaMetadata schema = latestVersionCache.getIfPresent(subject);
    if (schema != null) {
      return schema;
    }

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
        = restService.getLatestVersion(subject);
    schema = new SchemaMetadata(response);
    latestVersionCache.put(subject, schema);
    return schema;
  }

  @Override
  public SchemaMetadata getLatestWithMetadata(String subject, Map<String, String> metadata,
      boolean lookupDeletedSchema) throws IOException, RestClientException {
    SubjectAndMetadata subjectAndMetadata = new SubjectAndMetadata(subject, metadata);
    SchemaMetadata schema = latestWithMetadataCache.getIfPresent(subjectAndMetadata);
    if (schema != null) {
      return schema;
    }

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
        = restService.getLatestWithMetadata(subject, metadata, lookupDeletedSchema);
    schema = new SchemaMetadata(response);
    latestWithMetadataCache.put(subjectAndMetadata, schema);
    return schema;
  }

  @Override
  public int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getVersion(subject, schema, false);
  }

  @Override
  public int getVersion(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    try {
      final Cache<ParsedSchema, Integer> schemaVersionMap = schemaToVersionCache.get(
          subject, () -> CacheBuilder.newBuilder()
              .maximumSize(cacheCapacity)
              .build());

      Integer cachedVersion = schemaVersionMap.getIfPresent(schema);
      if (cachedVersion != null) {
        return cachedVersion;
      }

      synchronized (this) {
        cachedVersion = schemaVersionMap.getIfPresent(schema);
        if (cachedVersion != null) {
          return cachedVersion;
        }

        final int retrievedVersion = getVersionFromRegistry(subject, schema, normalize);
        schemaVersionMap.put(schema, retrievedVersion);
        return retrievedVersion;
      }
    } catch (ExecutionException e) {
      throw new IOException("Error accessing cache", e);
    }
  }

  @Override
  public List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    return restService.getAllVersions(subject);
  }

  @Override
  public List<Integer> getAllVersions(String subject, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    return restService.getAllVersions(RestService.DEFAULT_REQUEST_PROPERTIES,
        subject, lookupDeletedSchema);
  }

  @Override
  public int getId(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getId(subject, schema, false);
  }

  @Override
  public int getId(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    try {
      final Cache<ParsedSchema, Integer> schemaIdMap = schemaToIdCache.get(
          subject, () -> CacheBuilder.newBuilder()
              .maximumSize(cacheCapacity)
              .build());

      Integer cachedId = schemaIdMap.getIfPresent(schema);
      if (cachedId != null) {
        return cachedId;
      }

      synchronized (this) {
        cachedId = schemaIdMap.getIfPresent(schema);
        if (cachedId != null) {
          return cachedId;
        }

        final int retrievedId = getIdFromRegistry(subject, schema, normalize);
        schemaIdMap.put(schema, retrievedId);
        String context = toQualifiedContext(subject);
        final Cache<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.get(
            context, () -> CacheBuilder.newBuilder()
                .maximumSize(cacheCapacity)
                .build());
        idSchemaMap.put(retrievedId, schema);
        return retrievedId;
      }
    } catch (ExecutionException e) {
      throw new IOException("Error accessing cache", e);
    }
  }

  public String getGuid(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getGuid(subject, schema, false);
  }

  public String getGuid(
      String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    try {
      final Cache<ParsedSchema, String> guidMap = schemaToGuidCache.get(
          subject, () -> CacheBuilder.newBuilder()
              .maximumSize(cacheCapacity)
              .build());

      String cachedGuid = guidMap.getIfPresent(schema);
      if (cachedGuid != null) {
        return cachedGuid;
      }

      synchronized (this) {
        cachedGuid = guidMap.getIfPresent(schema);
        if (cachedGuid != null) {
          return cachedGuid;
        }

        final String retrievedGuid = getGuidFromRegistry(subject, schema, normalize);
        guidMap.put(schema, retrievedGuid);
        guidToSchemaCache.put(retrievedGuid, schema);
        return retrievedGuid;
      }
    } catch (ExecutionException e) {
      throw new IOException("Error accessing cache", e);
    }
  }

  @Override
  public RegisterSchemaResponse getIdWithResponse(
      String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    try {
      final Cache<ParsedSchema, RegisterSchemaResponse> schemaResponseMap =
          schemaToResponseCache.get(subject, () -> CacheBuilder.newBuilder()
              .maximumSize(cacheCapacity)
              .build());

      RegisterSchemaResponse cachedResponse = schemaResponseMap.getIfPresent(schema);
      if (cachedResponse != null) {
        // Allow the schema to be looked up again if version is not valid
        // This is for backward compatibility with versions before CP 8.0
        if (cachedResponse.getVersion() != null && cachedResponse.getVersion() > 0) {
          return cachedResponse;
        }
      }

      synchronized (this) {
        cachedResponse = schemaResponseMap.getIfPresent(schema);
        if (cachedResponse != null) {
          // Allow the schema to be looked up again if version is not valid
          // This is for backward compatibility with versions before CP 8.0
          if (cachedResponse.getVersion() != null && cachedResponse.getVersion() > 0) {
            return cachedResponse;
          }
        }

        final RegisterSchemaResponse retrievedResponse =
            getIdWithResponseFromRegistry(subject, schema, normalize, false);
        schemaResponseMap.put(schema, retrievedResponse);
        String context = toQualifiedContext(subject);
        final Cache<Integer, ParsedSchema> idSchemaMap = idToSchemaCache.get(
            context, () -> CacheBuilder.newBuilder()
                .maximumSize(cacheCapacity)
                .build());
        idSchemaMap.put(retrievedResponse.getId(), schema);
        return retrievedResponse;
      }
    } catch (ExecutionException e) {
      throw new IOException("Error accessing cache", e);
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
    schemaToVersionCache.invalidate(subject);
    if (isPermanent) {
      versionToSchemaCache.invalidate(subject);
    }
    idToSchemaCache.invalidate(subject);
    schemaToIdCache.invalidate(subject);
    schemaToResponseCache.invalidate(subject);
    latestVersionCache.invalidate(subject);
    latestWithMetadataCache.invalidateAll();
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
    Cache<ParsedSchema, Integer> versionCache = schemaToVersionCache.getIfPresent(subject);
    if (versionCache != null) {
      versionCache.asMap().values().remove(Integer.valueOf(version));
    }
    if (isPermanent) {
      Cache<Integer, Schema> schemaCache = versionToSchemaCache.getIfPresent(subject);
      if (schemaCache != null) {
        schemaCache.invalidate(Integer.valueOf(version));
      }
    }
    latestVersionCache.invalidate(subject);
    latestWithMetadataCache.invalidateAll();
    return restService.deleteSchemaVersion(requestProperties, subject, version, isPermanent);
  }

  @Override
  public boolean testCompatibility(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);
    return restService.testCompatibility(request, subject, "latest", false, false).isEmpty();
  }

  @Override
  public List<String> testCompatibilityVerbose(String subject, ParsedSchema schema)
          throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);
    return restService.testCompatibility(request, subject, "latest", false, true);
  }

  @Override
  public List<String> testCompatibilityVerbose(
      String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);
    return restService.testCompatibility(request, subject, "latest", normalize, true);
  }

  @Override
  public Config updateConfig(String subject, Config config)
      throws IOException, RestClientException {
    ConfigUpdateRequest response = restService.updateConfig(
        new ConfigUpdateRequest(config), subject);
    return new Config(response);
  }

  @Override
  public Config getConfig(String subject) throws IOException, RestClientException {
    return restService.getConfig(subject);
  }

  @Override
  public void deleteConfig(String subject) throws IOException, RestClientException {
    restService.deleteConfig(subject);
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
  public Collection<String> getAllContexts() throws IOException, RestClientException {
    return restService.getAllContexts();
  }

  @Override
  public SchemaRegistryDeployment getSchemaRegistryDeployment() 
      throws IOException, RestClientException {
    return restService.getSchemaRegistryDeployment();
  }

  @Override
  public SchemaRegistryServerVersion getSchemaRegistryServerVersion()
      throws IOException, RestClientException {
    return restService.getSchemaRegistryServerVersion();
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    return restService.getAllSubjects();
  }

  @Override
  public Collection<String> getAllSubjects(boolean lookupDeletedSubject)
      throws IOException, RestClientException {
    return restService.getAllSubjects(lookupDeletedSubject);
  }

  @Override
  public Collection<String> getAllSubjectsByPrefix(String subjectPrefix) throws IOException,
      RestClientException {
    return restService.getAllSubjects(subjectPrefix, false);
  }

  @Override
  public synchronized void reset() {
    schemaToResponseCache.invalidateAll();
    schemaToIdCache.invalidateAll();
    idToSchemaCache.invalidateAll();
    schemaToVersionCache.invalidateAll();
    versionToSchemaCache.invalidateAll();
    schemaToGuidCache.invalidateAll();
    guidToSchemaCache.invalidateAll();
    latestVersionCache.invalidateAll();
    latestWithMetadataCache.invalidateAll();
    missingSchemaCache.invalidateAll();
    missingIdCache.invalidateAll();
    missingGuidCache.invalidateAll();
    missingVersionCache.invalidateAll();
  }

  @Override
  public void close() throws IOException {
    if (restService != null) {
      restService.close();
    }
  }

  @Override
  public AssociationResponse createAssociation(AssociationCreateOrUpdateRequest request)
      throws IOException, RestClientException {
    return restService.createAssociation(DEFAULT_REQUEST_PROPERTIES, null, false, request);
  }

  @Override
  public AssociationResponse createOrUpdateAssociation(AssociationCreateOrUpdateRequest request)
      throws IOException, RestClientException {
    return restService.createOrUpdateAssociation(DEFAULT_REQUEST_PROPERTIES, null, false, request);
  }

  @Override
  public List<Association> getAssociationsBySubject(String subject,
      String resourceType, List<String> associationTypes, String lifecycle, int offset, int limit)
      throws IOException, RestClientException {
    LifecyclePolicy lifecyclePolicy = lifecycle != null ? LifecyclePolicy.valueOf(lifecycle) : null;
    return restService.getAssociationsBySubject(
        DEFAULT_REQUEST_PROPERTIES, subject, resourceType, associationTypes, lifecyclePolicy,
        offset, limit);
  }

  @Override
  public List<Association> getAssociationsByResourceId(String resourceId,
      String resourceType, List<String> associationTypes, String lifecycle, int offset, int limit)
      throws IOException, RestClientException {
    LifecyclePolicy lifecyclePolicy = lifecycle != null ? LifecyclePolicy.valueOf(lifecycle) : null;
    return restService.getAssociationsByResourceId(
        DEFAULT_REQUEST_PROPERTIES, resourceId, resourceType, associationTypes, lifecyclePolicy,
        offset, limit);
  }

  @Override
  public List<Association> getAssociationsByResourceName(String resourceName,
      String resourceNamespace, String resourceType, List<String> associationTypes,
      String lifecycle, int offset, int limit) throws IOException, RestClientException {
    LifecyclePolicy lifecyclePolicy = lifecycle != null ? LifecyclePolicy.valueOf(lifecycle) : null;
    return restService.getAssociationsByResourceName(
        DEFAULT_REQUEST_PROPERTIES, resourceName, resourceNamespace, resourceType, associationTypes,
        lifecyclePolicy, offset, limit);
  }

  @Override
  public void deleteAssociations(String resourceId, String resourceType,
                                  List<String> associationTypes, boolean cascadeLifecycle)
      throws IOException, RestClientException {
    restService.deleteAssociations(
        DEFAULT_REQUEST_PROPERTIES, resourceId, resourceType, associationTypes, cascadeLifecycle);
  }

  private void checkMissingSchemaCache(String subject, ParsedSchema schema, boolean normalize)
      throws RestClientException {
    if (missingSchemaCache.getIfPresent(
        new SubjectAndSchema(subject, schema, normalize)) != null) {
      throw new RestClientException("Schema not found",
          HTTP_NOT_FOUND, SCHEMA_NOT_FOUND_ERROR_CODE);
    }
  }

  private boolean isVersionNotFoundException(RestClientException rce) {
    return rce.getStatus() == HTTP_NOT_FOUND && rce.getErrorCode() == VERSION_NOT_FOUND_ERROR_CODE;
  }

  private boolean isSchemaOrSubjectNotFoundException(RestClientException rce) {
    return rce.getStatus() == HTTP_NOT_FOUND
        && (rce.getErrorCode() == SCHEMA_NOT_FOUND_ERROR_CODE
        || rce.getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE);
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

  static class SubjectAndInt {
    private final String subject;
    private final int id;

    public SubjectAndInt(String subject, int id) {
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
      SubjectAndInt that = (SubjectAndInt) o;
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

  static class SubjectAndMetadata {
    private final String subject;
    private final Map<String, String> metadata;

    public SubjectAndMetadata(String subject, Map<String, String> metadata) {
      this.subject = subject;
      this.metadata = ImmutableMap.copyOf(metadata);
    }

    public String subject() {
      return subject;
    }

    public Map<String, String> metadata() {
      return metadata;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubjectAndMetadata that = (SubjectAndMetadata) o;
      return Objects.equals(subject, that.subject) && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, metadata);
    }

    @Override
    public String toString() {
      return "SubjectAndMetadata{" + "subject='" + subject + '\'' + ", metadata=" + metadata + '}';
    }
  }
}
