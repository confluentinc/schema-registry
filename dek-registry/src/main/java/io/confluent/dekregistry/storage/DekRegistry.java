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

package io.confluent.dekregistry.storage;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_DELIMITER;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_PREFIX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.crypto.tink.Aead;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.rest.DekRegistryRestService;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.storage.exceptions.DekGenerationException;
import io.confluent.dekregistry.storage.exceptions.InvalidKeyException;
import io.confluent.dekregistry.storage.exceptions.KeySoftDeletedException;
import io.confluent.dekregistry.storage.utils.CompositeCacheUpdateHandler;
import io.confluent.dekregistry.web.rest.handlers.EncryptionUpdateRequestHandler;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import io.confluent.dekregistry.client.rest.entities.UpdateKekRequest;
import io.confluent.dekregistry.metrics.MetricsManager;
import io.confluent.dekregistry.storage.exceptions.AlreadyExistsException;
import io.confluent.dekregistry.storage.exceptions.KeyNotSoftDeletedException;
import io.confluent.dekregistry.storage.exceptions.KeyReferenceExistsException;
import io.confluent.dekregistry.storage.exceptions.TooManyKeysException;
import io.confluent.dekregistry.storage.serialization.EncryptionKeySerde;
import io.confluent.dekregistry.storage.serialization.EncryptionKeyIdSerde;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
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
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ws.rs.core.UriBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DekRegistry implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(DekRegistry.class);

  public static final String KEY = "dekRegistry";

  public static final int LATEST_VERSION = DekRegistryClient.LATEST_VERSION;
  public static final int MIN_VERSION = 1;
  public static final byte[] EMPTY_AAD = new byte[0];
  public static final String X_FORWARD_HEADER = DekRegistryRestService.X_FORWARD_HEADER;

  public static final String AWS_KMS = "aws-kms";
  public static final String AZURE_KMS = "azure-kms";
  public static final String GCP_KMS = "gcp-kms";

  private static final TypeReference<Kek> KEK_TYPE =
      new TypeReference<Kek>() {
      };
  private static final TypeReference<Dek> DEK_TYPE =
      new TypeReference<Dek>() {
      };
  private static final TypeReference<Void> VOID_TYPE =
      new TypeReference<Void>() {
      };

  private final KafkaSchemaRegistry schemaRegistry;
  private final MetricsManager metricsManager;
  private final DekRegistryConfig config;
  // visible for testing
  final Cache<EncryptionKeyId, EncryptionKey> keys;
  private final SetMultimap<String, KeyEncryptionKeyId> sharedKeys;
  private final Map<DekFormat, Cryptor> cryptors;
  private final Map<String, Lock> tenantToLock = new ConcurrentHashMap<>();
  private final AtomicBoolean initialized = new AtomicBoolean();
  private final CountDownLatch initLatch = new CountDownLatch(1);

  @Inject
  public DekRegistry(
      SchemaRegistry schemaRegistry,
      MetricsManager metricsManager
  ) {
    try {
      this.schemaRegistry = (KafkaSchemaRegistry) schemaRegistry;
      this.schemaRegistry.properties().put(KEY, this);
      this.schemaRegistry.addUpdateRequestHandler(new EncryptionUpdateRequestHandler());
      this.metricsManager = metricsManager;
      this.config = new DekRegistryConfig(schemaRegistry.config().originalProperties());
      this.keys = createCache(new EncryptionKeyIdSerde(), new EncryptionKeySerde(),
          config.topic(), getCacheUpdateHandler(config));
      this.sharedKeys = Multimaps.synchronizedSetMultimap(TreeMultimap.create());
      this.cryptors = new ConcurrentHashMap<>();
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

  private CacheUpdateHandler<EncryptionKeyId, EncryptionKey> getCacheUpdateHandler(
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

  public DekRegistryConfig config() {
    return config;
  }

  protected SetMultimap<String, KeyEncryptionKeyId> getSharedKeys() {
    return sharedKeys;
  }

  protected Cryptor getCryptor(DekFormat dekFormat) {
    return cryptors.computeIfAbsent(dekFormat, k -> {
      try {
        return new Cryptor(dekFormat);
      } catch (GeneralSecurityException e) {
        log.error("Invalid format {}", dekFormat);
        throw new IllegalArgumentException("Invalid format " + dekFormat, e);
      }
    });
  }

  @PostConstruct
  public void init() {
    if (!initialized.get()) {
      keys.init();
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

  public List<String> getKekNames(boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    return getKeks(tenant, lookupDeleted).stream()
        .map(kv -> ((KeyEncryptionKeyId) kv.key).getName())
        .collect(Collectors.toList());
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getKeks(
      String tenant, boolean lookupDeleted) {
    List<KeyValue<EncryptionKeyId, EncryptionKey>> result = new ArrayList<>();
    KeyEncryptionKeyId key1 = new KeyEncryptionKeyId(tenant, String.valueOf(Character.MIN_VALUE));
    KeyEncryptionKeyId key2 = new KeyEncryptionKeyId(tenant, String.valueOf(Character.MAX_VALUE));
    try (KeyValueIterator<EncryptionKeyId, EncryptionKey> iter =
        keys().range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        KeyValue<EncryptionKeyId, EncryptionKey> kv = iter.next();
        if (!kv.value.isDeleted() || lookupDeleted) {
          result.add(kv);
        }
      }
    }
    return result;
  }

  public KeyEncryptionKey getKek(String name, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = (KeyEncryptionKey) keys.get(keyId);
    if (key != null && (!key.isDeleted() || lookupDeleted)) {
      return key;
    } else {
      return null;
    }
  }

  public Kek toKekEntity(KeyEncryptionKey kek) {
    return kek.toKekEntity();
  }

  public List<String> getDekSubjects(String kekName, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    return getDeks(tenant, kekName, lookupDeleted).stream()
        .map(kv -> ((DataEncryptionKeyId) kv.key).getSubject())
        .sorted()
        .distinct()
        .collect(Collectors.toList());
  }

  public List<Integer> getDekVersions(
      String kekName, String subject, DekFormat algorithm, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    return getDeks(tenant, kekName, subject, algorithm, lookupDeleted).stream()
        .map(kv -> ((DataEncryptionKeyId) kv.key).getVersion())
        .collect(Collectors.toList());
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeks(
      String tenant, String kekName, boolean lookupDeleted) {
    List<KeyValue<EncryptionKeyId, EncryptionKey>> result = new ArrayList<>();
    DataEncryptionKeyId key1 = new DataEncryptionKeyId(
        tenant, kekName, CONTEXT_PREFIX + CONTEXT_DELIMITER,
        DekFormat.AES128_GCM, MIN_VERSION);
    DataEncryptionKeyId key2 = new DataEncryptionKeyId(
        tenant, kekName, CONTEXT_PREFIX + Character.MAX_VALUE + CONTEXT_DELIMITER,
        DekFormat.AES256_SIV, Integer.MAX_VALUE);
    try (KeyValueIterator<EncryptionKeyId, EncryptionKey> iter =
        keys().range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        KeyValue<EncryptionKeyId, EncryptionKey> kv = iter.next();
        if (!kv.value.isDeleted() || lookupDeleted) {
          result.add(kv);
        }
      }
    }
    return result;
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeks(
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
        keys().range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        KeyValue<EncryptionKeyId, EncryptionKey> kv = iter.next();
        DataEncryptionKeyId key = (DataEncryptionKeyId) kv.key;
        if (!kv.value.isDeleted() || lookupDeleted) {
          result.add(kv);
        }
      }
    }
    return result;
  }

  public DataEncryptionKey getLatestDek(String kekName, String subject, DekFormat algorithm,
      boolean lookupDeleted) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    List<KeyValue<EncryptionKeyId, EncryptionKey>> deks =
        getDeks(tenant, kekName, subject, algorithm, lookupDeleted);
    Collections.reverse(deks);
    return deks.isEmpty() ? null : (DataEncryptionKey) deks.get(0).value;
  }

  public DataEncryptionKey getDek(String kekName, String subject, int version,
      DekFormat algorithm, boolean lookupDeleted) throws SchemaRegistryException {
    if (version == LATEST_VERSION) {
      return getLatestDek(kekName, subject, algorithm, lookupDeleted);
    }
    String tenant = schemaRegistry.tenant();
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DataEncryptionKeyId keyId = new DataEncryptionKeyId(
        tenant, kekName, subject, algorithm, version);
    DataEncryptionKey key = (DataEncryptionKey) keys.get(keyId);
    if (key != null && (!key.isDeleted() || lookupDeleted)) {
      KeyEncryptionKey kek = getKek(key.getKekName(), true);
      if (kek.isShared()) {
        key = generateRawDek(kek, key);
      }
      return key;
    } else {
      return null;
    }
  }

  public Kek createKekOrForward(CreateKekRequest request,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        return toKekEntity(createKek(request));
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          return forwardCreateKekRequestToLeader(request, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private Kek forwardCreateKekRequestToLeader(CreateKekRequest request,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks");
    String path = builder.build().toString();

    log.debug(String.format("Forwarding create key request to %s", baseUrl));
    try {
      return leaderRestService.httpRequest(
          path, "POST", toJson(request), headerProperties, KEK_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the create key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public KeyEncryptionKey createKek(CreateKekRequest request)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    if (metricsManager.getKeyCount(tenant, KeyType.KEK) > config.maxKeys()) {
      throw new TooManyKeysException(KeyType.KEK.name());
    }

    String kmsType = normalizeKmsType(request.getKmsType());

    SortedMap<String, String> kmsProps = request.getKmsProps() != null
        ? new TreeMap<>(request.getKmsProps())
        : Collections.emptySortedMap();
    KeyEncryptionKey key = new KeyEncryptionKey(request.getName(), kmsType,
        request.getKmsKeyId(), kmsProps, request.getDoc(), request.isShared(), request.isDeleted());

    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, request.getName());
    KeyEncryptionKey oldKey = (KeyEncryptionKey) keys.get(keyId);
    // Allow create to act like undelete if the kek is deleted
    if (oldKey != null
        && (request.isDeleted() == oldKey.isDeleted() || !oldKey.isEquivalent(key))) {
      throw new AlreadyExistsException(request.getName());
    }
    keys.put(keyId, key);
    // Retrieve key with ts set
    key = (KeyEncryptionKey) keys.get(keyId);
    return key;
  }

  private String normalizeKmsType(String kmsType) {
    String type = kmsType.toLowerCase(Locale.ROOT);
    if (type.startsWith("aws")) {
      return AWS_KMS;
    } else if (type.startsWith("azure")) {
      return AZURE_KMS;
    } else if (type.startsWith("gcp")) {
      return GCP_KMS;
    } else {
      return kmsType;
    }
  }

  public Dek createDekOrForward(String kekName, CreateDekRequest request,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        return createDek(kekName, request).toDekEntity();
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          return forwardCreateDekRequestToLeader(kekName, request, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private Dek forwardCreateDekRequestToLeader(String kekName,
      CreateDekRequest request, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks");
    String path = builder.build(kekName).toString();

    log.debug(String.format("Forwarding create key request to %s", baseUrl));
    try {
      return leaderRestService.httpRequest(
          path, "POST", toJson(request), headerProperties, DEK_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the create key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public DataEncryptionKey createDek(String kekName, CreateDekRequest request)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    if (metricsManager.getKeyCount(tenant, KeyType.DEK) > config.maxKeys()) {
      throw new TooManyKeysException(KeyType.DEK.name());
    }
    DekFormat algorithm = request.getAlgorithm() != null
        ? request.getAlgorithm()
        : DekFormat.AES256_GCM;
    int version = request.getVersion() != null ? request.getVersion() : MIN_VERSION;
    DataEncryptionKey key = new DataEncryptionKey(kekName, request.getSubject(),
        algorithm, version, request.getEncryptedKeyMaterial(), request.isDeleted());
    KeyEncryptionKey kek = getKek(key.getKekName(), true);
    if (key.getEncryptedKeyMaterial() == null) {
      if (kek.isShared()) {
        key = generateEncryptedDek(kek, key);
      } else {
        throw new InvalidKeyException("encryptedKeyMaterial");
      }
    }
    DataEncryptionKeyId keyId = new DataEncryptionKeyId(
        tenant, kekName, request.getSubject(), algorithm, version);
    DataEncryptionKey oldKey = (DataEncryptionKey) keys.get(keyId);
    // Allow create to act like undelete if the dek is deleted
    if (oldKey != null
        && ((request.isDeleted() == oldKey.isDeleted()) || !oldKey.isEquivalent(key))) {
      throw new AlreadyExistsException(request.getSubject());
    }
    keys.put(keyId, key);
    // Retrieve key with ts set
    key = (DataEncryptionKey) keys.get(keyId);
    if (kek.isShared()) {
      Mode mode = schemaRegistry.getModeInScope(request.getSubject());
      if (mode != Mode.IMPORT) {
        key = generateRawDek(kek, key);
      }
    }
    return key;
  }

  protected DataEncryptionKey generateEncryptedDek(KeyEncryptionKey kek, DataEncryptionKey key)
      throws DekGenerationException {
    try {
      Aead aead = toKekEntity(kek).toAead(config.originals());
      // Generate new dek
      byte[] rawDek = getCryptor(key.getAlgorithm()).generateKey();
      byte[] encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
      String encryptedDekStr =
          new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
      key = new DataEncryptionKey(key.getKekName(), key.getSubject(), key.getAlgorithm(),
          key.getVersion(), encryptedDekStr, key.isDeleted());
      return key;
    } catch (GeneralSecurityException e) {
      String msg = "Could not generate encrypted dek for " + key.getSubject();
      log.error(msg, e);
      msg += ": " + e.getMessage();
      Throwable cause = e.getCause();
      if (cause != null) {
        msg += ": " + cause.getMessage();
      }
      throw new DekGenerationException(msg);
    }
  }

  protected DataEncryptionKey generateRawDek(KeyEncryptionKey kek, DataEncryptionKey key)
      throws DekGenerationException {
    try {
      // Decrypt dek
      Aead aead = toKekEntity(kek).toAead(config.originals());
      byte[] encryptedDek = Base64.getDecoder().decode(
          key.getEncryptedKeyMaterial().getBytes(StandardCharsets.UTF_8));
      byte[] rawDek = aead.decrypt(encryptedDek, EMPTY_AAD);
      String rawDekStr =
          new String(Base64.getEncoder().encode(rawDek), StandardCharsets.UTF_8);
      // Copy dek
      DataEncryptionKey newKey = new DataEncryptionKey(key.getKekName(), key.getSubject(),
          key.getAlgorithm(), key.getVersion(), key.getEncryptedKeyMaterial(), key.isDeleted());
      newKey.setKeyMaterial(rawDekStr);
      newKey.setTimestamp(key.getTimestamp());
      return newKey;
    } catch (GeneralSecurityException e) {
      String msg = "Could not generate raw dek for " + key.getSubject();
      log.error(msg, e);
      msg += ": " + e.getMessage();
      Throwable cause = e.getCause();
      if (cause != null) {
        msg += ": " + cause.getMessage();
      }
      throw new DekGenerationException(msg);
    }
  }

  public Kek putKekOrForward(String name, UpdateKekRequest request,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        KeyEncryptionKey kek = putKek(name, request);
        return kek != null ? toKekEntity(kek) : null;
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          return forwardPutKekRequestToLeader(name, request, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private Kek forwardPutKekRequestToLeader(String name,
      UpdateKekRequest request, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}");
    String path = builder.build(name).toString();

    log.debug(String.format("Forwarding put key request to %s", baseUrl));
    try {
      return leaderRestService.httpRequest(
          path, "PUT", toJson(request), headerProperties, KEK_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the put key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public KeyEncryptionKey putKek(String name, UpdateKekRequest request)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = (KeyEncryptionKey) keys.get(keyId);
    if (key == null || key.isDeleted()) {
      return null;
    }
    SortedMap<String, String> kmsProps = request.getKmsProps() != null
        ? new TreeMap<>(request.getKmsProps())
        : key.getKmsProps();
    String doc = request.getDoc() != null ? request.getDoc() : key.getDoc();
    boolean shared = request.isShared() != null ? request.isShared() : key.isShared();
    KeyEncryptionKey newKey = new KeyEncryptionKey(name, key.getKmsType(),
        key.getKmsKeyId(), kmsProps, doc, shared, false);
    if (newKey.isEquivalent(key)) {
      return key;
    }
    keys.put(keyId, newKey);
    // Retrieve key with ts set
    newKey = (KeyEncryptionKey) keys.get(keyId);
    return newKey;
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

  @PreDestroy
  @Override
  public void close() throws IOException {
    log.info("Shutting down dek registry");
    if (keys != null) {
      keys.close();
    }
  }

  private static byte[] toJson(Object o) throws JsonProcessingException {
    return JacksonMapper.INSTANCE.writeValueAsBytes(o);
  }
}
