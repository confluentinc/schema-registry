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
import io.confluent.dpregistry.client.rest.DataProductRegistryRestService;
import io.confluent.dpregistry.client.rest.entities.RegisteredDataProduct;
import io.confluent.dpregistry.metrics.MetricsManager;
import io.confluent.dpregistry.storage.exceptions.DataProductNotSoftDeletedException;
import io.confluent.dpregistry.storage.serialization.DataProductKeySerde;
import io.confluent.dpregistry.storage.serialization.DataProductValueSerde;
import io.confluent.dpregistry.storage.utils.CompositeCacheUpdateHandler;
import io.confluent.dpregistry.client.rest.entities.DataProduct;
import io.confluent.dpregistry.client.rest.entities.DataProductSchemas;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

  public static final String KEY = "dataProductRegistry";

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

  private final int nameSearchDefaultLimit;
  private final int nameSearchMaxLimit;
  private final int versionSearchDefaultLimit;
  private final int versionSearchMaxLimit;
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
      this.nameSearchDefaultLimit =
          config.getInt(DataProductRegistryConfig.DATAPRODUCT_NAME_SEARCH_DEFAULT_LIMIT_CONFIG);
      this.nameSearchMaxLimit =
          config.getInt(DataProductRegistryConfig.DATAPRODUCT_NAME_SEARCH_MAX_LIMIT_CONFIG);
      this.versionSearchDefaultLimit =
          config.getInt(DataProductRegistryConfig.DATAPRODUCT_VERSION_SEARCH_DEFAULT_LIMIT_CONFIG);
      this.versionSearchMaxLimit =
          config.getInt(DataProductRegistryConfig.DATAPRODUCT_VERSION_SEARCH_MAX_LIMIT_CONFIG);
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
    Map<String, Object> handlerConfigs = config.originalsWithPrefix(
        DataProductRegistryConfig.DATAPRODUCT_REGISTRY_UPDATE_HANDLERS_CONFIG + ".");
    handlerConfigs.put(DataProductCacheUpdateHandler.DATA_PRODUCT_REGISTRY, this);
    List<DataProductCacheUpdateHandler> customCacheHandlers = config.getConfiguredInstances(
        DataProductRegistryConfig.DATAPRODUCT_REGISTRY_UPDATE_HANDLERS_CONFIG,
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

  public List<String> getDataProductNames(String env, String cluster, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    return getDataProducts(tenant, env, cluster, lookupDeleted).stream()
        .map(kv -> ((DataProductKey) kv.key).getName())
        .collect(Collectors.toList());
  }

  public List<Integer> getDataProductVersions(
      String env, String cluster, String name, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    return getDataProducts(tenant, env, cluster, name, lookupDeleted).stream()
        .map(kv -> ((DataProductKey) kv.key).getVersion())
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
      String env, String cluster, String name, int version, boolean lookupDeleted)
      throws SchemaRegistryException {
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
    return getLatestDataProduct(env, cluster, name, false);
  }

  public KeyValue<DataProductKey, DataProductValue> getLatestDataProduct(
      String env, String cluster, String name, boolean lookupDeleted)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    List<KeyValue<DataProductKey, DataProductValue>> products =
        getDataProducts(tenant, env, cluster, name, lookupDeleted);
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
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}"
            + "/dataproducts/{name}/versions");

    String path = builder.build(env, cluster, request.getInfo().getName()).toString();

    log.debug(String.format("Forwarding create data product request to %s", baseUrl));
    try {
      return leaderRestService.httpRequest(
          path, "POST", toJson(request), headerProperties, REGISTERED_DATA_PRODUCT_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the create data product request to %s",
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
    String newGuid = UUID.randomUUID().toString();

    DataProductSchemas schemas = request.getSchemas();
    if (schemas != null) {
      if (schemas.getKey() != null) {
        int id = schemaRegistry.getIdentityGenerator().id(new SchemaValue(schemas.getKey()));
        schemas.getKey().setId(id);
      }
      if (schemas.getValue() != null) {
        int id = schemaRegistry.getIdentityGenerator().id(new SchemaValue(schemas.getValue()));
        schemas.getValue().setId(id);
      }
    }

    DataProductValue value = new DataProductValue(newVersion, newGuid, request);
    DataProductKey key = new DataProductKey(
        tenant, env, cluster, request.getInfo().getName(), newVersion);
    KeyValue<DataProductKey, DataProductValue> oldKeyValue =
        getLatestDataProduct(env, cluster, request.getInfo().getName());
    if (oldKeyValue != null
        && !oldKeyValue.value.isDeleted()
        && value.isEquivalent(oldKeyValue.value)) {
      // If the value is equivalent to the latest version, return the existing one
      return oldKeyValue.value;
    }

    dataProducts.put(key, value);
    // Retrieve data product with ts set
    return dataProducts.get(key);
  }

  public void deleteDataProductOrForward(String env, String cluster, String name,
      boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        deleteDataProduct(env, cluster, name, permanentDelete);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardDeleteDataProductRequestToLeader(
              env, cluster, name, permanentDelete, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardDeleteDataProductRequestToLeader(String env, String cluster, String name,
      boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}/dataproducts/{name}")
        .queryParam("permanent", permanentDelete);
    String path = builder.build(env, cluster, name).toString();

    log.debug(String.format("Forwarding delete data product request to %s", baseUrl));
    try {
      leaderRestService.httpRequest(
          path, "DELETE", null, headerProperties, VOID_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the delete data product request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public void deleteDataProduct(String env, String cluster, String name, boolean permanentDelete)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    dataProducts.sync();

    String tenant = schemaRegistry.tenant();
    List<KeyValue<DataProductKey, DataProductValue>> products =
        getDataProducts(tenant, env, cluster, name, permanentDelete);
    if (permanentDelete) {
      for (KeyValue<DataProductKey, DataProductValue> product : products) {
        if (!product.value.isDeleted()) {
          DataProductKey key = product.key;
          throw new DataProductNotSoftDeletedException(key.getName());
        }
      }
      for (KeyValue<DataProductKey, DataProductValue> product : products) {
        DataProductKey key = product.key;
        dataProducts.remove(key);
      }
    } else {
      for (KeyValue<DataProductKey, DataProductValue> product : products) {
        if (!product.value.isDeleted()) {
          DataProductValue oldValue = product.value;
          DataProductValue newValue = new DataProductValue(
              oldValue.getVersion(), oldValue.getGuid(), oldValue.getInfo(),
              oldValue.getSchemas(), oldValue.getConfigs(), true, null, null);
          dataProducts.put(product.key, newValue);
        }
      }
    }
  }

  public void deleteDataProductVersionOrForward(String env, String cluster, String name,
      int version, boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        deleteDataProductVersion(env, cluster, name, version, permanentDelete);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardDeleteDataProductVersionRequestToLeader(env, cluster, name, version,
              permanentDelete, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardDeleteDataProductVersionRequestToLeader(String env, String cluster,
      String name, int version, boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}/dataproducts/{name}"
            + "/versions/{version}")
        .queryParam("permanent", permanentDelete);
    String path = builder.build(env, cluster, name, version).toString();

    log.debug(String.format("Forwarding delete data product version request to %s", baseUrl));
    try {
      leaderRestService.httpRequest(
          path, "DELETE", null, headerProperties, VOID_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the delete data product version "
                  + "request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public void deleteDataProductVersion(String env, String cluster, String name, int version,
      boolean permanentDelete)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    dataProducts.sync();

    String tenant = schemaRegistry.tenant();
    DataProductKey key = new DataProductKey(tenant, env, cluster, name, version);
    DataProductValue value = dataProducts.get(key);
    if (value == null) {
      return;
    }
    if (permanentDelete) {
      if (!value.isDeleted()) {
        throw new DataProductNotSoftDeletedException(key.getName());
      }
      dataProducts.remove(key);
    } else {
      if (!value.isDeleted()) {
        DataProductValue newValue = new DataProductValue(
            value.getVersion(), value.getGuid(), value.getInfo(),
            value.getSchemas(), value.getConfigs(), true, null, null);
        dataProducts.put(key, newValue);
      }
    }
  }

  public int normalizeLimit(int suppliedLimit, int defaultLimit, int maxLimit) {
    int limit = defaultLimit;
    if (suppliedLimit > 0 && suppliedLimit <= maxLimit) {
      limit = suppliedLimit;
    }
    return limit;
  }

  public int normalizeNameSearchLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, nameSearchDefaultLimit, nameSearchMaxLimit);
  }

  public int normalizeVersionSearchLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, versionSearchDefaultLimit, versionSearchMaxLimit);
  }

  @PreDestroy
  @Override
  public void close() throws IOException {
    log.info("Shutting down data product registry");
    if (dataProducts != null) {
      dataProducts.close();
    }
  }

  private static byte[] toJson(Object o) throws JsonProcessingException {
    return JacksonMapper.INSTANCE.writeValueAsBytes(o);
  }
}
