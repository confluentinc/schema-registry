/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.dpregistry.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.confluent.dpregistry.client.DataProductRegistryClient;
import io.confluent.dpregistry.client.rest.DekRegistryRestService;
import io.confluent.dpregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dpregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dpregistry.client.rest.entities.Dek;
import io.confluent.dpregistry.client.rest.entities.Kek;
import io.confluent.dpregistry.client.rest.entities.KeyType;
import io.confluent.dpregistry.client.rest.entities.RegisteredDataProduct;
import io.confluent.dpregistry.client.rest.entities.UpdateKekRequest;
import io.confluent.dpregistry.metrics.MetricsManager;
import io.confluent.dpregistry.storage.exceptions.AlreadyExistsException;
import io.confluent.dpregistry.storage.exceptions.DekGenerationException;
import io.confluent.dpregistry.storage.exceptions.InvalidKeyException;
import io.confluent.dpregistry.storage.exceptions.KeyNotSoftDeletedException;
import io.confluent.dpregistry.storage.exceptions.KeyReferenceExistsException;
import io.confluent.dpregistry.storage.exceptions.KeySoftDeletedException;
import io.confluent.dpregistry.storage.exceptions.TooManyKeysException;
import io.confluent.dpregistry.storage.serialization.DataProductKeySerde;
import io.confluent.dpregistry.storage.serialization.DataProductValueSerde;
import io.confluent.dpregistry.storage.serialization.EncryptionKeyIdSerde;
import io.confluent.dpregistry.storage.serialization.EncryptionKeySerde;
import io.confluent.dpregistry.storage.utils.CompositeCacheUpdateHandler;
import io.confluent.dpregistry.web.rest.handlers.EncryptionUpdateRequestHandler;
import io.confluent.dpregistry.client.rest.entities.DataProduct;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.Mode;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.RestException;
import io.kcache.Cache;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.Caches;
import io.kcache.utils.InMemoryCache;
import jakarta.ws.rs.core.UriBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DataProductRegistry implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(DataProductRegistry.class);

  public static final String KEY = "dekRegistry";

  public static final int LATEST_VERSION = DataProductRegistryClient.LATEST_VERSION;
  public static final int MIN_VERSION = 1;
  public static final byte[] EMPTY_AAD = new byte[0];
  public static final String X_FORWARD_HEADER = DataProductRegistryRestService.X_FORWARD_HEADER;

  private static final TypeReference<RegisteredDataProduct> REGISTERED_DATA_PRODUCT_TYPE =
      new TypeReference<RegisteredDataProduct>() {
      };
  private static final TypeReference<Void> VOID_TYPE =
      new TypeReference<Void>() {
      };

  private final KafkaSchemaRegistry schemaRegistry;
  private final MetricsManager metricsManager;
  private final DataProductRegistryConfig config;

  private final int dataProductSearchDefaultLimit;
  private final int dataProductSearchMaxLimit;
  // visible for testing
  final Cache<DataProductKey, DataProductValue> dataProducts;
  private final Map<String, Lock> tenantToLock = new ConcurrentHashMap<>();
  private final AtomicBoolean initialized = new AtomicBoolean();
  private final CountDownLatch initLatch = new CountDownLatch(1);

  @Inject
  public DataProductRegistry(
      SchemaRegistry schemaRegistry,
      MetricsManager metricsManager
  ) {
    try {
      this.schemaRegistry = (KafkaSchemaRegistry) schemaRegistry;
      this.schemaRegistry.properties().put(KEY, this);
      this.metricsManager = metricsManager;
      this.config = new DataProductRegistryConfig(schemaRegistry.config().originalProperties());
      this.dataProducts = createCache(new DataProductKeySerde(), new DataProductValueSerde(),
          config.topic(), getCacheUpdateHandler(config));
      this.dataProductSearchDefaultLimit =
              config.getInt(DataProductRegistryConfig.DATAPRODUCT_SEARCH_DEFAULT_LIMIT_CONFIG);
      this.dataProductSearchMaxLimit = config.getInt(DataProductRegistryConfig.DATAPRODUCT_SEARCH_MAX_LIMIT_CONFIG);
    } catch (RestConfigException e) {
      throw new IllegalArgumentException("Could not instantiate DekRegistry", e);
    }
  }

  public KafkaSchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
  }

  public MetricsManager getMetricsManager() {
    return metricsManager;
  }

  protected <K, V> Cache<K, V> createCache(
      Serde<K> keySerde,
      Serde<V> valueSerde,
      String topic,
      CacheUpdateHandler<K, V> cacheUpdateHandler) throws CacheInitializationException {
    Properties props = getKafkaCacheProperties(topic);
    KafkaCacheConfig config = new KafkaCacheConfig(props);
    Cache<K, V> kafkaCache = Caches.concurrentCache(
        new KafkaCache<>(config,
            keySerde,
            valueSerde,
            cacheUpdateHandler,
            new InMemoryCache<>()));
    getSchemaRegistry().addLeaderChangeListener(isLeader -> {
      if (isLeader) {
        // Reset the cache to remove any stale data from previous leadership
        kafkaCache.reset();
        // Ensure the new leader catches up with the offsets
        kafkaCache.sync();
      }
    });
    return kafkaCache;
  }

  private Properties getKafkaCacheProperties(String topic) {
    Properties props = new Properties();
    props.putAll(schemaRegistry.config().originalProperties());
    Set<String> keys = props.stringPropertyNames();
    for (String key : keys) {
      if (key.startsWith("kafkastore.")) {
        String newKey = key.replace("kafkastore", "kafkacache");
        if (!keys.contains(newKey)) {
          props.put(newKey, props.get(key));
        }
      }
    }
    props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
    return props;
  }

  private CacheUpdateHandler<DataProductKey, DataProductValue> getCacheUpdateHandler(
      DataProductRegistryConfig config) {
    Map<String, Object> handlerConfigs =
        config.originalsWithPrefix(DataProductRegistryConfig.DATAPRODUCT_REGISTRY_UPDATE_HANDLERS_CONFIG + ".");
    handlerConfigs.put(DataProductCacheUpdateHandler.DATA_PRODUCT_REGISTRY, this);
    List<DataProductCacheUpdateHandler> customCacheHandlers =
        config.getConfiguredInstances(DataProductRegistryConfig.DATAPRODUCT_REGISTRY_UPDATE_HANDLERS_CONFIG,
        DataProductCacheUpdateHandler.class,
        handlerConfigs);
    DataProductCacheUpdateHandler cacheHandler = new DefaultDataProductCacheUpdateHandler(this);
    for (DataProductCacheUpdateHandler customCacheHandler : customCacheHandlers) {
      log.info("Registering custom cache handler: {}",
          customCacheHandler.getClass().getName()
      );
    }
    customCacheHandlers.add(cacheHandler);
    return new CompositeCacheUpdateHandler<>(customCacheHandlers);
  }

  public Cache<DataProductKey, DataProductValue> dataProducts() {
    return dataProducts;
  }

  public DataProductRegistryConfig config() {
    return config;
  }

  @PostConstruct
  public void init() {
    if (!initialized.get()) {
      dataProducts.init();
      boolean isInitialized = initialized.compareAndSet(false, true);
      if (!isInitialized) {
        throw new IllegalStateException("DekRegistry was already initialized");
      }
      initLatch.countDown();
    }
  }

  public void waitForInit() throws InterruptedException {
    initLatch.await();
  }

  public boolean initialized() {
    return initialized.get();
  }

  public boolean isLeader() {
    return schemaRegistry.isLeader();
  }

  private boolean isLeader(Map<String, String> headerProperties) {
    String forwardHeader = headerProperties.get(X_FORWARD_HEADER);
    return isLeader() && (forwardHeader == null || Boolean.parseBoolean(forwardHeader));
  }

  protected Lock lockFor(String tenant) {
    return tenantToLock.computeIfAbsent(tenant, k -> new ReentrantLock());
  }

  private void lock(String tenant, Map<String, String> headerProperties) {
    String forwardHeader = headerProperties.get(X_FORWARD_HEADER);
    if (forwardHeader == null || Boolean.parseBoolean(forwardHeader)) {
      lockFor(tenant).lock();
    }
  }

  private void unlock(String tenant) {
    if (((ReentrantLock) lockFor(tenant)).isHeldByCurrentThread()) {
      lockFor(tenant).unlock();
    }
  }

  public List<String> getDataProductNames(boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    return getDataProducts(tenant, lookupDeleted).stream()
        .map(kv -> ((DataProductKey) kv.key).getName())
        .collect(Collectors.toList());
  }

  protected List<KeyValue<DataProductKey, DataProductValue>> getDataProducts(
      String tenant, String env, String cluster, boolean lookupDeleted) {
    List<KeyValue<DataProductKey, DataProductValue>> result = new ArrayList<>();
    DataProductKey key1 = new DataProductKey(tenant, env, cluster,
        String.valueOf(Character.MIN_VALUE), 1);
    DataProductKey key2 = new DataProductKey(tenant, env, cluster,
        String.valueOf(Character.MAX_VALUE), Integer.MAX_VALUE);
    try (KeyValueIterator<DataProductKey, DataProductValue> iter =
        dataProducts.range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        KeyValue<DataProductKey, DataProductValue> kv = iter.next();
        if (!kv.value.isDeleted() || lookupDeleted) {
          result.add(kv);
        }
      }
    }
    return result;
  }

  protected List<KeyValue<DataProductKey, DataProductValue>> getDataProducts(
      String tenant, String env, String cluster, String name, boolean lookupDeleted) {
    List<KeyValue<DataProductKey, DataProductValue>> result = new ArrayList<>();
    DataProductKey key1 = new DataProductKey(tenant, env, cluster, name, 1);
    DataProductKey key2 = new DataProductKey(tenant, env, cluster, name, Integer.MAX_VALUE);
    try (KeyValueIterator<DataProductKey, DataProductValue> iter =
        dataProducts.range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        KeyValue<DataProductKey, DataProductValue> kv = iter.next();
        if (!kv.value.isDeleted() || lookupDeleted) {
          result.add(kv);
        }
      }
    }
    return result;
  }

  public DataProductValue getDataProduct(
      String env, String cluster, String name, int version, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    DataProductKey key = new DataProductKey(tenant, env, cluster, name, version);
    DataProductValue value = dataProducts.get(key);
    if (value != null && (!value.isDeleted() || lookupDeleted)) {
      return value;
    } else {
      return null;
    }
  }

  public KeyValue<DataProductKey, DataProductValue> getLatestDataProduct(
      String env, String cluster, String name)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    List<KeyValue<DataProductKey, DataProductValue>> products =
        getDataProducts(tenant, env, cluster, name, false);
    Collections.reverse(products);
    return products.isEmpty() ? null : products.get(0);
  }

  public RegisteredDataProduct createDataProductOrForward(String env, String cluster,
      DataProduct request, Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        return createDataProduct(env, cluster, request).toEntity();
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          return forwardCreateDataProductRequestToLeader(env, cluster, request, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private RegisteredDataProduct forwardCreateDataProductRequestToLeader(
      String env, String cluster,
      DataProduct request,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/env/{env}/cluster/{cluster}/dataproducts");

    String path = builder.build(env, cluster).toString();

    log.debug(String.format("Forwarding create data product request to %s", baseUrl));
    try {
      return leaderRestService.httpRequest(
          path, "POST", toJson(request), headerProperties, REGISTERED_DATA_PRODUCT_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the create key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public DataProductValue createDataProduct(String env, String cluster, DataProduct request)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    dataProducts.sync();

    String tenant = schemaRegistry.tenant();

    KeyValue<DataProductKey, DataProductValue> latest =
        getLatestDataProduct(env, cluster, request.getInfo().getName());
    int newVersion = latest != null ? latest.key.getVersion() + 1 : MIN_VERSION;

    DataProductKey key = new DataProductKey(
        tenant, env, cluster, request.getInfo().getName(), newVersion);
    DataProductValue value = new DataProductValue(UUID.randomUUID().toString(), request);
    dataProducts.put(key, value);
    // Retrieve data product with ts set
    return dataProducts.get(key);
  }

  public void deleteKekOrForward(String name, boolean permanentDelete,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        deleteKek(name, permanentDelete);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardDeleteKekRequestToLeader(name, permanentDelete, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardDeleteKekRequestToLeader(String name, boolean permanentDelete,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}")
        .queryParam("permanent", permanentDelete);
    String path = builder.build(name).toString();

    log.debug(String.format("Forwarding delete key request to %s", baseUrl));
    try {
      leaderRestService.httpRequest(
          path, "DELETE", null, headerProperties, VOID_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the delete key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public void deleteKek(String name, boolean permanentDelete) throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    if (!getDeks(tenant, name, permanentDelete).isEmpty()) {
      throw new KeyReferenceExistsException(name);
    }
    EncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey oldKey = (KeyEncryptionKey) keys.get(keyId);
    if (oldKey == null) {
      return;
    }
    if (permanentDelete) {
      if (!oldKey.isDeleted()) {
        throw new KeyNotSoftDeletedException(name);
      }
      keys.remove(keyId);
    } else {
      if (!oldKey.isDeleted()) {
        KeyEncryptionKey newKey = new KeyEncryptionKey(name, oldKey.getKmsType(),
            oldKey.getKmsKeyId(), oldKey.getKmsProps(), oldKey.getDoc(), oldKey.isShared(), true);
        keys.put(keyId, newKey);
      }
    }
  }

  public void deleteDekOrForward(String name, String subject, DekFormat algorithm,
      boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        deleteDek(name, subject, algorithm, permanentDelete);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardDeleteDekRequestToLeader(name, subject, algorithm,
              permanentDelete, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardDeleteDekRequestToLeader(String name, String subject, DekFormat algorithm,
      boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks/{subject}")
        .queryParam("permanent", permanentDelete);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, subject).toString();

    log.debug(String.format("Forwarding delete key request to %s", baseUrl));
    try {
      leaderRestService.httpRequest(
          path, "DELETE", null, headerProperties, VOID_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the delete key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public void deleteDek(String name, String subject, DekFormat algorithm, boolean permanentDelete)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    List<KeyValue<EncryptionKeyId, EncryptionKey>> deks =
        getDeks(tenant, name, subject, algorithm, permanentDelete);

    if (permanentDelete) {
      for (KeyValue<EncryptionKeyId, EncryptionKey> dek : deks) {
        if (!dek.value.isDeleted()) {
          DataEncryptionKeyId keyId = (DataEncryptionKeyId) dek.key;
          throw new KeyNotSoftDeletedException(keyId.getSubject());
        }
      }
      for (KeyValue<EncryptionKeyId, EncryptionKey> dek : deks) {
        DataEncryptionKeyId keyId = (DataEncryptionKeyId) dek.key;
        keys.remove(keyId);
      }
    } else {
      for (KeyValue<EncryptionKeyId, EncryptionKey> dek : deks) {
        if (!dek.value.isDeleted()) {
          DataEncryptionKeyId id = (DataEncryptionKeyId) dek.key;
          DataEncryptionKey oldKey = (DataEncryptionKey) dek.value;
          DataEncryptionKey newKey = new DataEncryptionKey(name, oldKey.getSubject(),
              oldKey.getAlgorithm(), oldKey.getVersion(), oldKey.getEncryptedKeyMaterial(), true);
          keys.put(id, newKey);
        }
      }
    }
  }

  public void deleteDekVersionOrForward(String name, String subject, int version,
      DekFormat algorithm, boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        deleteDekVersion(name, subject, version, algorithm, permanentDelete);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardDeleteDekVersionRequestToLeader(name, subject, version, algorithm,
              permanentDelete, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardDeleteDekVersionRequestToLeader(String name, String subject, int version,
      DekFormat algorithm, boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath(
        "/dek-registry/v1/keks/{name}/deks/{subject}/versions/{version}")
        .queryParam("permanent", permanentDelete);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, subject, version).toString();

    log.debug(String.format("Forwarding delete key version request to %s", baseUrl));
    try {
      leaderRestService.httpRequest(
          path, "DELETE", null, headerProperties, VOID_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the delete key version request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public void deleteDekVersion(String name, String subject, int version,
      DekFormat algorithm, boolean permanentDelete)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    String tenant = schemaRegistry.tenant();
    DataEncryptionKeyId id = new DataEncryptionKeyId(tenant, name, subject, algorithm, version);
    DataEncryptionKey key = (DataEncryptionKey) keys.get(id);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      if (!key.isDeleted()) {
        throw new KeyNotSoftDeletedException(subject);
      }
      keys.remove(id);
    } else {
      if (!key.isDeleted()) {
        DataEncryptionKey newKey = new DataEncryptionKey(name, key.getSubject(),
            key.getAlgorithm(), key.getVersion(), key.getEncryptedKeyMaterial(), true);
        keys.put(id, newKey);
      }
    }
  }

  public void undeleteKekOrForward(String name, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        undeleteKek(name);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardUndeleteKekRequestToLeader(name, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardUndeleteKekRequestToLeader(String name, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/undelete");
    String path = builder.build(name).toString();

    log.debug(String.format("Forwarding undelete key request to %s", baseUrl));
    try {
      leaderRestService.httpRequest(
          path, "POST", null, headerProperties, VOID_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the undelete key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public void undeleteKek(String name) throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    EncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = (KeyEncryptionKey) keys.get(keyId);
    if (key == null) {
      return;
    }
    if (key.isDeleted()) {
      KeyEncryptionKey newKey = new KeyEncryptionKey(name, key.getKmsType(),
          key.getKmsKeyId(), key.getKmsProps(), key.getDoc(), key.isShared(), false);
      keys.put(keyId, newKey);
    }
  }

  public void undeleteDekOrForward(String name, String subject, DekFormat algorithm,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        undeleteDek(name, subject, algorithm);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardUndeleteDekRequestToLeader(name, subject, algorithm, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardUndeleteDekRequestToLeader(String name, String subject, DekFormat algorithm,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath(
        "/dek-registry/v1/keks/{name}/deks/{subject}/undelete");
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, subject).toString();

    log.debug(String.format("Forwarding undelete key request to %s", baseUrl));
    try {
      leaderRestService.httpRequest(
          path, "POST", null, headerProperties, VOID_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the undelete key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public void undeleteDek(String name, String subject, DekFormat algorithm)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    EncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = (KeyEncryptionKey) keys.get(keyId);
    if (key == null) {
      return;
    }
    if (key.isDeleted()) {
      throw new KeySoftDeletedException(name);
    }
    List<KeyValue<EncryptionKeyId, EncryptionKey>> deks =
        getDeks(tenant, name, subject, algorithm, true);

    for (KeyValue<EncryptionKeyId, EncryptionKey> dek : deks) {
      DataEncryptionKeyId id = (DataEncryptionKeyId) dek.key;
      DataEncryptionKey oldKey = (DataEncryptionKey) dek.value;
      if (oldKey.isDeleted()) {
        DataEncryptionKey newKey = new DataEncryptionKey(name, oldKey.getSubject(),
            oldKey.getAlgorithm(), oldKey.getVersion(), oldKey.getEncryptedKeyMaterial(), false);
        keys.put(id, newKey);
      }
    }
  }

  public void undeleteDekVersionOrForward(String name, String subject, int version,
      DekFormat algorithm, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        undeleteDekVersion(name, subject, version, algorithm);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardUndeleteDekVersionRequestToLeader(name, subject, version, algorithm,
              headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardUndeleteDekVersionRequestToLeader(String name, String subject, int version,
      DekFormat algorithm, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath(
            "/dek-registry/v1/keks/{name}/deks/{subject}/versions/{version}/undelete");
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, subject, version).toString();

    log.debug(String.format("Forwarding undelete key version request to %s", baseUrl));
    try {
      leaderRestService.httpRequest(
          path, "POST", null, headerProperties, VOID_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the undelete key version request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public void undeleteDekVersion(String name, String subject, int version,
      DekFormat algorithm) throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    String tenant = schemaRegistry.tenant();
    EncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = (KeyEncryptionKey) keys.get(keyId);
    if (key == null) {
      return;
    }
    if (key.isDeleted()) {
      throw new KeySoftDeletedException(name);
    }
    DataEncryptionKeyId id = new DataEncryptionKeyId(tenant, name, subject, algorithm, version);
    DataEncryptionKey oldKey = (DataEncryptionKey) keys.get(id);
    if (oldKey == null) {
      return;
    }
    if (oldKey.isDeleted()) {
      DataEncryptionKey newKey = new DataEncryptionKey(name, oldKey.getSubject(),
          oldKey.getAlgorithm(), oldKey.getVersion(), oldKey.getEncryptedKeyMaterial(), false);
      keys.put(id, newKey);
    }
  }

  public int normalizeLimit(int suppliedLimit, int defaultLimit, int maxLimit) {
    int limit = defaultLimit;
    if (suppliedLimit > 0 && suppliedLimit <= maxLimit) {
      limit = suppliedLimit;
    }
    return limit;
  }

  public int normalizeSearchLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, dataProductSearchDefaultLimit, dataProductSearchMaxLimit);
  }

  @PreDestroy
  @Override
  public void close() throws IOException {
    log.info("Shutting down dek registry");
    if (dataProducts != null) {
      dataProducts.close();
    }
  }

  private static byte[] toJson(Object o) throws JsonProcessingException {
    return JacksonMapper.INSTANCE.writeValueAsBytes(o);
  }
}
