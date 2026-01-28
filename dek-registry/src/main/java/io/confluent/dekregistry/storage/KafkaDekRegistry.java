/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.dekregistry.storage;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_DELIMITER;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_PREFIX;

import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.confluent.dekregistry.metrics.MetricsManager;
import io.confluent.dekregistry.storage.serialization.EncryptionKeyIdSerde;
import io.confluent.dekregistry.storage.serialization.EncryptionKeySerde;
import io.confluent.dekregistry.storage.utils.CompositeCacheUpdateHandler;
import io.confluent.dekregistry.web.rest.handlers.EncryptionUpdateRequestHandler;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.RestConfigException;
import io.kcache.Cache;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.Caches;
import io.kcache.utils.InMemoryCache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka/KCache-backed implementation of DEK Registry.
 */
@Singleton
public class KafkaDekRegistry extends AbstractDekRegistry {

  private static final Logger log = LoggerFactory.getLogger(KafkaDekRegistry.class);

  // visible for testing
  final Cache<EncryptionKeyId, EncryptionKey> keys;

  @Inject
  public KafkaDekRegistry(
      SchemaRegistry schemaRegistry,
      MetricsManager metricsManager
  ) {
    this(schemaRegistry, metricsManager, createConfig(schemaRegistry));
  }

  protected KafkaDekRegistry(
      SchemaRegistry schemaRegistry,
      MetricsManager metricsManager,
      DekRegistryConfig config
  ) {
    super(schemaRegistry, metricsManager, config);
    this.schemaRegistry.addUpdateRequestHandler(new EncryptionUpdateRequestHandler());
    this.keys = createCache(new EncryptionKeyIdSerde(), new EncryptionKeySerde(),
        config.topic(), getCacheUpdateHandler(config));
  }

  private static DekRegistryConfig createConfig(SchemaRegistry schemaRegistry) {
    try {
      return new DekRegistryConfig(schemaRegistry.config().originalProperties());
    } catch (RestConfigException e) {
      throw new IllegalArgumentException("Could not instantiate DekRegistryConfig", e);
    }
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

  protected CacheUpdateHandler<EncryptionKeyId, EncryptionKey> getCacheUpdateHandler(
      DekRegistryConfig config) {
    Map<String, Object> handlerConfigs =
        config.originalsWithPrefix(DekRegistryConfig.DEK_REGISTRY_UPDATE_HANDLERS_CONFIG + ".");
    handlerConfigs.put(DekCacheUpdateHandler.DEK_REGISTRY, this);
    List<DekCacheUpdateHandler> customCacheHandlers =
        config.getConfiguredInstances(DekRegistryConfig.DEK_REGISTRY_UPDATE_HANDLERS_CONFIG,
            DekCacheUpdateHandler.class,
            handlerConfigs);
    DekCacheUpdateHandler cacheHandler = new DefaultDekCacheUpdateHandler(this);
    for (DekCacheUpdateHandler customCacheHandler : customCacheHandlers) {
      log.info("Registering custom cache handler: {}",
          customCacheHandler.getClass().getName()
      );
    }
    customCacheHandlers.add(cacheHandler);
    return new CompositeCacheUpdateHandler<>(customCacheHandlers);
  }

  public Cache<EncryptionKeyId, EncryptionKey> keys() {
    return keys;
  }

  // ==================== Abstract Method Implementations ====================

  @Override
  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getKeksFromStore(
      String tenant, boolean lookupDeleted) {
    List<KeyValue<EncryptionKeyId, EncryptionKey>> result = new ArrayList<>();
    KeyEncryptionKeyId key1 = new KeyEncryptionKeyId(tenant, String.valueOf(Character.MIN_VALUE));
    KeyEncryptionKeyId key2 = new KeyEncryptionKeyId(tenant, String.valueOf(Character.MAX_VALUE));
    try (KeyValueIterator<EncryptionKeyId, EncryptionKey> iter =
             keys.range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        KeyValue<EncryptionKeyId, EncryptionKey> kv = iter.next();
        if (!kv.value.isDeleted() || lookupDeleted) {
          result.add(kv);
        }
      }
    }
    return result;
  }

  @Override
  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeksFromStore(
      String tenant, String minKekName, String maxKekName, boolean lookupDeleted) {
    List<KeyValue<EncryptionKeyId, EncryptionKey>> result = new ArrayList<>();
    DataEncryptionKeyId key1 = new DataEncryptionKeyId(
        tenant, minKekName, CONTEXT_PREFIX + CONTEXT_DELIMITER,
        DekFormat.AES128_GCM, MIN_VERSION);
    DataEncryptionKeyId key2 = new DataEncryptionKeyId(
        tenant, maxKekName, CONTEXT_PREFIX + Character.MAX_VALUE + CONTEXT_DELIMITER,
        DekFormat.AES256_SIV, Integer.MAX_VALUE);
    try (KeyValueIterator<EncryptionKeyId, EncryptionKey> iter =
             keys.range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        KeyValue<EncryptionKeyId, EncryptionKey> kv = iter.next();
        if (!kv.value.isDeleted() || lookupDeleted) {
          result.add(kv);
        }
      }
    }
    return result;
  }

  @Override
  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeksFromStore(
      String tenant, String kekName, String subject, DekFormat algorithm, boolean lookupDeleted) {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    List<KeyValue<EncryptionKeyId, EncryptionKey>> result = new ArrayList<>();
    DataEncryptionKeyId key1 = new DataEncryptionKeyId(
        tenant, kekName, subject,
        algorithm, MIN_VERSION);
    DataEncryptionKeyId key2 = new DataEncryptionKeyId(
        tenant, kekName, subject,
        algorithm, Integer.MAX_VALUE);
    try (KeyValueIterator<EncryptionKeyId, EncryptionKey> iter =
             keys.range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        KeyValue<EncryptionKeyId, EncryptionKey> kv = iter.next();
        if (!kv.value.isDeleted() || lookupDeleted) {
          result.add(kv);
        }
      }
    }
    return result;
  }

  @Override
  protected KeyEncryptionKey getKekById(KeyEncryptionKeyId keyId) {
    return (KeyEncryptionKey) keys.get(keyId);
  }

  @Override
  protected DataEncryptionKey getDekById(DataEncryptionKeyId keyId) {
    return (DataEncryptionKey) keys.get(keyId);
  }

  @Override
  protected void putKey(EncryptionKeyId id, EncryptionKey key) {
    keys.put(id, key);
  }

  @Override
  protected void removeKey(EncryptionKeyId id) {
    keys.remove(id);
  }

  @Override
  protected void syncStore() {
    keys.sync();
  }

  @Override
  protected void initStore() {
    keys.init();
  }

  // ==================== Lifecycle ====================

  @PostConstruct
  public void init() {
    super.init();
  }

  @PreDestroy
  @Override
  public void close() throws IOException {
    log.info("Shutting down KafkaDekRegistry");
    if (keys != null) {
      keys.close();
    }
  }
}
