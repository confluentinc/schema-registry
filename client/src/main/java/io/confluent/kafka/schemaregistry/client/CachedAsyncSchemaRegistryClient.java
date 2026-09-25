/*
 * Copyright 2026 Confluent Inc.
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

import static io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.HTTP_NOT_FOUND;
import static io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.NO_SUBJECT;
import static io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.SCHEMA_NOT_FOUND_ERROR_CODE;
import static io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.VERSION_NOT_FOUND_ERROR_CODE;
import static io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.toQualifiedContext;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.SubjectAndInt;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.SubjectAndMetadata;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.SubjectAndSchema;
import io.confluent.kafka.schemaregistry.client.rest.AsyncRestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Non-blocking Schema Registry client with client-side caching, the counterpart to
 * {@link CachedSchemaRegistryClient}, whose caching behavior it follows.
 *
 * <p>The caches hold futures rather than values, so concurrent requests for the same key share one
 * registry call instead of queueing on a lock. A load that fails is evicted, so the next request
 * retries it; not-found responses are instead remembered in the missing caches, for the TTLs set
 * in {@link SchemaRegistryClientConfig}.
 *
 * <p>Futures that need a registry call complete on the executor of the supplied
 * {@link AsyncRestService}, while cache hits are already complete when returned. See
 * {@link AsyncRestService} for when to supply your own executor.
 */
public class CachedAsyncSchemaRegistryClient implements AsyncSchemaRegistryClient {

  private static final Logger log = LoggerFactory.getLogger(CachedAsyncSchemaRegistryClient.class);

  private final AsyncRestService restService;
  private final Map<String, SchemaProvider> providers;

  private final Cache<Schema, CompletableFuture<Optional<ParsedSchema>>> parsedSchemaCache;
  private final Cache<SubjectAndInt, CompletableFuture<Schema>> idToSchemaCache;
  private final Cache<String, CompletableFuture<ParsedSchema>> guidToSchemaCache;
  private final Cache<SubjectAndInt, CompletableFuture<Schema>> versionToSchemaCache;
  private final Cache<String, CompletableFuture<SchemaMetadata>> latestVersionCache;
  private final Cache<SubjectAndMetadata, CompletableFuture<SchemaMetadata>>
      latestWithMetadataCache;
  // Registrations and lookups are cached apart, so that a registration never joins an in-flight
  // lookup, which fails if the schema is new. Each reads the other's completed results.
  private final Cache<SubjectAndSchema, CompletableFuture<RegisterSchemaResponse>>
      registerResponseCache;
  private final Cache<SubjectAndSchema, CompletableFuture<RegisterSchemaResponse>>
      lookupResponseCache;
  private final Cache<SubjectAndSchema, CompletableFuture<Integer>> schemaToVersionCache;
  private final Cache<SubjectAndSchema, Long> missingSchemaCache;
  private final Cache<SubjectAndInt, Long> missingIdCache;
  private final Cache<String, Long> missingGuidCache;
  private final Cache<SubjectAndInt, Long> missingVersionCache;

  /**
   * @param restService   used for all registry calls, and closed with this client
   * @param cacheCapacity the maximum number of entries in each cache
   * @param providers     the schema types to support; Avro alone if null or empty
   * @param configs       client configs, for cache TTLs and schema provider settings
   */
  public CachedAsyncSchemaRegistryClient(
      AsyncRestService restService,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> configs) {
    this.restService = restService;
    this.providers = CachedSchemaRegistryClient.configureProviders(
        providers, configs, new PrefetchedVersionFetcher());

    this.parsedSchemaCache = newCache(cacheCapacity, -1);
    this.idToSchemaCache = newCache(cacheCapacity, -1);
    this.guidToSchemaCache = newCache(cacheCapacity, -1);
    this.versionToSchemaCache = newCache(cacheCapacity, -1);
    this.registerResponseCache = newCache(cacheCapacity, -1);
    this.lookupResponseCache = newCache(cacheCapacity, -1);
    this.schemaToVersionCache = newCache(cacheCapacity, -1);

    long latestTTL = SchemaRegistryClientConfig.getLatestTTL(configs);
    this.latestVersionCache = newCache(cacheCapacity, latestTTL);
    this.latestWithMetadataCache = newCache(cacheCapacity, latestTTL);

    int maxMissingCacheSize = SchemaRegistryClientConfig.getMaxMissingCacheSize(configs);
    long missingIdTTL = SchemaRegistryClientConfig.getMissingIdTTL(configs);
    this.missingSchemaCache = newCache(
        maxMissingCacheSize, SchemaRegistryClientConfig.getMissingSchemaTTL(configs));
    this.missingIdCache = newCache(maxMissingCacheSize, missingIdTTL);
    this.missingGuidCache = newCache(maxMissingCacheSize, missingIdTTL);
    this.missingVersionCache = newCache(
        maxMissingCacheSize, SchemaRegistryClientConfig.getMissingVersionTTL(configs));
  }

  /**
   * Completes with an empty result if the schema is invalid, and exceptionally if a schema it
   * references could not be fetched.
   */
  @Override
  public CompletableFuture<Optional<ParsedSchema>> parseSchema(Schema schema) {
    // Keyed by content, so the same schema under another subject or id is a cache hit
    Schema cacheKey = CachedSchemaRegistryClient.contentCacheKey(providers, schema);
    return cached(
        parsedSchemaCache,
        cacheKey,
        () -> prefetchReferences(
            schema.getSubject(), schema.getReferences(), ConcurrentHashMap.newKeySet())
            .thenApply(ignored -> parse(schema)),
        // As in CachedSchemaRegistryClient, schemas that fail to parse are not cached
        Optional::isPresent);
  }

  @Override
  public CompletableFuture<ParsedSchema> getSchemaBySubjectAndId(String subject, int id) {
    return getSchemaEntityBySubjectAndId(subject, id).thenCompose(this::parseSchemaOrElseThrow);
  }

  @Override
  public CompletableFuture<Schema> getSchemaEntityBySubjectAndId(String subject, int id) {
    String subjectOrNone = subject != null ? subject : NO_SUBJECT;
    SubjectAndInt key = new SubjectAndInt(subjectOrNone, id);
    return cached(idToSchemaCache, key, () -> {
      if (missingIdCache.getIfPresent(key) != null) {
        return notFound("Schema " + id + " not found", SCHEMA_NOT_FOUND_ERROR_CODE);
      }
      return rememberMissing(
          restService.getId(id, subjectOrNone),
          missingIdCache,
          key,
          CachedSchemaRegistryClient::isSchemaOrSubjectNotFoundException)
          .thenApply(restSchema -> new Schema(
              restSchema.getSubject(), restSchema.getVersion(), id, restSchema));
    });
  }

  @Override
  public CompletableFuture<ParsedSchema> getSchemaByGuid(String guid, String format) {
    String key = format != null ? guid + ":" + format : guid;
    return cached(guidToSchemaCache, key, () -> {
      if (missingGuidCache.getIfPresent(key) != null) {
        return notFound("Schema " + guid + " not found", SCHEMA_NOT_FOUND_ERROR_CODE);
      }
      return rememberMissing(
          restService.getByGuid(guid, format),
          missingGuidCache,
          key,
          CachedSchemaRegistryClient::isSchemaOrSubjectNotFoundException)
          .thenCompose(restSchema ->
              parseSchemaOrElseThrow(new Schema(null, null, null, restSchema)));
    });
  }

  @Override
  public CompletableFuture<SchemaMetadata> getSchemaMetadata(
      String subject, int version, boolean lookupDeletedSchema) {
    return getSchemaByVersion(subject, version, lookupDeletedSchema)
        .thenApply(SchemaMetadata::new);
  }

  @Override
  public CompletableFuture<SchemaMetadata> getLatestSchemaMetadata(String subject) {
    return cached(latestVersionCache, subject,
        () -> restService.getLatestVersion(subject).thenApply(SchemaMetadata::new));
  }

  @Override
  public CompletableFuture<SchemaMetadata> getLatestWithMetadata(
      String subject, Map<String, String> metadata, boolean lookupDeletedSchema) {
    return cached(latestWithMetadataCache, new SubjectAndMetadata(subject, metadata),
        () -> restService.getLatestWithMetadata(subject, metadata, lookupDeletedSchema)
            .thenApply(SchemaMetadata::new));
  }

  @Override
  public CompletableFuture<RegisterSchemaResponse> registerWithResponse(
      String subject, ParsedSchema schema, boolean normalize, boolean propagateSchemaTags) {
    SubjectAndSchema key = new SubjectAndSchema(subject, schema, normalize);
    // A schema that a lookup found is already registered
    RegisterSchemaResponse lookedUp = completedValue(lookupResponseCache, key);
    if (lookedUp != null) {
      return CompletableFuture.completedFuture(lookedUp);
    }
    return cached(registerResponseCache, key, () -> {
      RegisterSchemaRequest request = new RegisterSchemaRequest(schema);
      if (propagateSchemaTags) {
        request.setPropagateSchemaTags(true);
      }
      return restService.registerSchema(request, subject, normalize)
          .thenApply(response -> {
            cacheSchemaById(subject, response);
            // A new version makes the cached latest versions stale
            latestVersionCache.invalidate(subject);
            latestWithMetadataCache.invalidateAll();
            return response;
          });
    });
  }

  @Override
  public CompletableFuture<RegisterSchemaResponse> getIdWithResponse(
      String subject, ParsedSchema schema, boolean normalize) {
    SubjectAndSchema key = new SubjectAndSchema(subject, schema, normalize);
    RegisterSchemaResponse registered = completedValue(registerResponseCache, key);
    // Look the schema up anyway if the version is not valid, as registries before CP 8.0 did not
    // return one on registration
    if (registered != null && registered.getVersion() != null && registered.getVersion() > 0) {
      return CompletableFuture.completedFuture(registered);
    }
    return cached(lookupResponseCache, key,
        () -> lookUpFromRegistry(subject, schema, normalize, false)
            .thenApply(response -> {
              cacheSchemaById(subject, response);
              return response;
            }));
  }

  @Override
  public CompletableFuture<Integer> getVersion(
      String subject, ParsedSchema schema, boolean normalize) {
    return cached(schemaToVersionCache, new SubjectAndSchema(subject, schema, normalize),
        () -> lookUpFromRegistry(subject, schema, normalize, true)
            .thenApply(RegisterSchemaResponse::getVersion));
  }

  @Override
  public CompletableFuture<List<String>> testCompatibilityVerbose(
      String subject, ParsedSchema schema, boolean normalize) {
    return restService.testCompatibility(
        new RegisterSchemaRequest(schema), subject, "latest", normalize, true);
  }

  @Override
  public CompletableFuture<Config> getConfig(String subject, boolean defaultToGlobal) {
    return restService.getConfig(subject, defaultToGlobal);
  }

  @Override
  public void reset() {
    parsedSchemaCache.invalidateAll();
    idToSchemaCache.invalidateAll();
    guidToSchemaCache.invalidateAll();
    versionToSchemaCache.invalidateAll();
    latestVersionCache.invalidateAll();
    latestWithMetadataCache.invalidateAll();
    registerResponseCache.invalidateAll();
    lookupResponseCache.invalidateAll();
    schemaToVersionCache.invalidateAll();
    missingSchemaCache.invalidateAll();
    missingIdCache.invalidateAll();
    missingGuidCache.invalidateAll();
    missingVersionCache.invalidateAll();
  }

  @Override
  public void close() throws IOException {
    restService.close();
  }

  private Optional<ParsedSchema> parse(Schema schema) {
    String schemaType = schema.getSchemaType() != null ? schema.getSchemaType() : AvroSchema.TYPE;
    SchemaProvider provider = providers.get(schemaType);
    if (provider == null) {
      log.error("Invalid schema type {}", schemaType);
      return Optional.empty();
    }
    return provider.parseSchema(schema, false, false);
  }

  private CompletableFuture<ParsedSchema> parseSchemaOrElseThrow(Schema schema) {
    return parseSchema(schema).thenCompose(parsed -> parsed.isPresent()
        ? CompletableFuture.completedFuture(parsed.get())
        : CompletableFuture.<ParsedSchema>failedFuture(new IOException(
            "Invalid schema " + schema.getSchema()
                + " with refs " + schema.getReferences()
                + " of type " + schema.getSchemaType())));
  }

  /**
   * Loads every schema that {@code references} transitively point to into the version cache, so
   * that a provider resolving them while parsing finds them there rather than waiting on the
   * registry. Makes the same lookups as {@code AbstractSchemaProvider.resolveReferences}.
   */
  private CompletableFuture<Void> prefetchReferences(
      String subject, List<SchemaReference> references, Set<SubjectAndInt> visited) {
    if (references == null || references.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    List<CompletableFuture<Void>> fetches = new ArrayList<>();
    for (SchemaReference reference : references) {
      QualifiedSubject refSubject = reference.getSubject() != null
          ? QualifiedSubject.qualifySubjectWithParent(tenant(), subject, reference.getSubject())
          : null;
      if (refSubject == null || reference.getVersion() == null) {
        // Left for the provider to reject when it parses
        continue;
      }
      SubjectAndInt key =
          new SubjectAndInt(refSubject.toQualifiedSubject(), reference.getVersion());
      if (visited.add(key)) {
        fetches.add(getSchemaByVersion(key.subject(), key.id(), true)
            .thenCompose(s -> prefetchReferences(s.getSubject(), s.getReferences(), visited)));
      }
    }
    return CompletableFuture.allOf(fetches.toArray(new CompletableFuture<?>[0]));
  }

  private CompletableFuture<Schema> getSchemaByVersion(
      String subject, int version, boolean lookupDeletedSchema) {
    // As in CachedSchemaRegistryClient, only lookups that include deleted versions are cached
    if (!lookupDeletedSchema) {
      return restService.getVersion(subject, version, false);
    }
    SubjectAndInt key = new SubjectAndInt(subject, version);
    return cached(versionToSchemaCache, key, () -> {
      if (missingVersionCache.getIfPresent(key) != null) {
        return notFound("Version " + version + " not found", VERSION_NOT_FOUND_ERROR_CODE);
      }
      return rememberMissing(
          restService.getVersion(subject, version, true),
          missingVersionCache,
          key,
          CachedSchemaRegistryClient::isVersionNotFoundException);
    });
  }

  private CompletableFuture<RegisterSchemaResponse> lookUpFromRegistry(
      String subject, ParsedSchema schema, boolean normalize, boolean lookupDeletedSchema) {
    SubjectAndSchema key = new SubjectAndSchema(subject, schema, normalize);
    if (missingSchemaCache.getIfPresent(key) != null) {
      return notFound("Schema not found", SCHEMA_NOT_FOUND_ERROR_CODE);
    }
    return rememberMissing(
        restService.lookUpSubjectVersion(
            new RegisterSchemaRequest(schema), subject, normalize, lookupDeletedSchema),
        missingSchemaCache,
        key,
        CachedSchemaRegistryClient::isSchemaOrSubjectNotFoundException)
        .thenApply(RegisterSchemaResponse::new);
  }

  private void cacheSchemaById(String subject, RegisterSchemaResponse response) {
    if (response.getSchema() != null) {
      idToSchemaCache.put(
          new SubjectAndInt(toQualifiedContext(subject), response.getId()),
          CompletableFuture.completedFuture(new Schema(subject, response)));
    }
  }

  private static <K, V> Cache<K, V> newCache(int maximumSize, long ttlSeconds) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().maximumSize(maximumSize);
    if (ttlSeconds >= 0) {
      builder = builder.expireAfterWrite(ttlSeconds, TimeUnit.SECONDS);
    }
    return builder.build();
  }

  private static <K, V> CompletableFuture<V> cached(
      Cache<K, CompletableFuture<V>> cache, K key, Supplier<CompletableFuture<V>> loader) {
    return cached(cache, key, loader, value -> true);
  }

  /**
   * Returns the cached future for {@code key}, starting {@code loader} if there is none, so that
   * concurrent callers share one load. The entry is evicted if the load fails or its result does
   * not pass {@code keep}.
   */
  private static <K, V> CompletableFuture<V> cached(
      Cache<K, CompletableFuture<V>> cache,
      K key,
      Supplier<CompletableFuture<V>> loader,
      Predicate<V> keep) {
    CompletableFuture<V> future = new CompletableFuture<>();
    CompletableFuture<V> existing = cache.asMap().putIfAbsent(key, future);
    if (existing != null) {
      return existing.copy();
    }
    future.whenComplete((value, error) -> {
      if (error != null || !keep.test(value)) {
        cache.asMap().remove(key, future);
      }
    });
    // The loader runs only after the entry is in place, and outside any cache lock
    try {
      loader.get().whenComplete((value, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
        } else {
          future.complete(value);
        }
      });
    } catch (RuntimeException e) {
      future.completeExceptionally(e);
    }
    // A copy, so that a caller completing or cancelling it cannot affect other callers
    return future.copy();
  }

  /**
   * Returns the result of the cached future for {@code key}, or null if there is none or it has
   * not completed successfully.
   */
  private static <K, V> V completedValue(Cache<K, CompletableFuture<V>> cache, K key) {
    CompletableFuture<V> future = cache.getIfPresent(key);
    return future != null && future.isDone() && !future.isCompletedExceptionally()
        ? future.join()
        : null;
  }

  /**
   * Records {@code key} in {@code missingCache} if {@code future} fails with a not-found error.
   */
  private static <K, T> CompletableFuture<T> rememberMissing(
      CompletableFuture<T> future,
      Cache<K, Long> missingCache,
      K key,
      Predicate<RestClientException> isNotFound) {
    return future.whenComplete((value, error) -> {
      Throwable cause = error instanceof CompletionException ? error.getCause() : error;
      if (cause instanceof RestClientException && isNotFound.test((RestClientException) cause)) {
        missingCache.put(key, System.currentTimeMillis());
      }
    });
  }

  private static <T> CompletableFuture<T> notFound(String message, int errorCode) {
    return CompletableFuture.failedFuture(
        new RestClientException(message, HTTP_NOT_FOUND, errorCode));
  }

  /**
   * Given to schema providers to resolve references while parsing. {@link #parseSchema} fetches
   * every reference first, so lookups here are normally cache hits; only an entry evicted in
   * between is fetched again, blocking the parsing thread as the synchronous client would.
   */
  private class PrefetchedVersionFetcher implements SchemaVersionFetcher {

    @Override
    public String tenant() {
      return CachedAsyncSchemaRegistryClient.this.tenant();
    }

    @Override
    public Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
      return getSchemaByVersion(subject, version, lookupDeletedSchema).join();
    }
  }
}
