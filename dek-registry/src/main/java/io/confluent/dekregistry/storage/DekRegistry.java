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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.crypto.tink.Aead;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.confluent.dekregistry.client.rest.DekRegistryRestService;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
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

  public static final byte[] EMPTY_AAD = new byte[0];
  public static final String X_FORWARD_HEADER = DekRegistryRestService.X_FORWARD_HEADER;

  private static final String AWS_KMS = "aws-kms";
  private static final String AZURE_KMS = "azure-kms";
  private static final String GCP_KMS = "gcp-kms";

  private static final TypeReference<KeyEncryptionKey> KEY_ENCRYPTION_KEY_TYPE =
      new TypeReference<KeyEncryptionKey>() {
      };
  private static final TypeReference<DataEncryptionKey> DATA_ENCRYPTION_KEY_TYPE =
      new TypeReference<DataEncryptionKey>() {
      };
  private static final TypeReference<Void> VOID_TYPE =
      new TypeReference<Void>() {
      };

  private final KafkaSchemaRegistry schemaRegistry;
  private final MetricsManager metricsManager;
  private final DekRegistryConfig config;
  // visible for testing
  final Cache<EncryptionKeyId, EncryptionKey> keys;
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
      this.metricsManager = metricsManager;
      this.config = new DekRegistryConfig(schemaRegistry.config().originalProperties());
      this.keys = createCache(new EncryptionKeyIdSerde(), new EncryptionKeySerde(),
          config.topic(), new DekRegistryCacheUpdateHandler(metricsManager));
      this.cryptors = new ConcurrentHashMap<>();
    } catch (RestConfigException e) {
      throw new IllegalArgumentException("Could not instantiate DekRegistry", e);
    }
  }

  public KafkaSchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
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

  public Cache<EncryptionKeyId, EncryptionKey> keys() {
    return keys;
  }

  public DekRegistryConfig config() {
    return config;
  }

  protected Cryptor getCryptor(DekFormat dekFormat) {
    return cryptors.computeIfAbsent(dekFormat, k -> {
      try {
        return new Cryptor(dekFormat);
      } catch (GeneralSecurityException e) {
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
    return getKeks(tenant).stream()
        .filter(kv -> !kv.value.isDeleted() || lookupDeleted)
        .map(kv -> ((KeyEncryptionKeyId) kv.key).getName())
        .collect(Collectors.toList());
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getKeks(String tenant) {
    List<KeyValue<EncryptionKeyId, EncryptionKey>> result = new ArrayList<>();
    KeyEncryptionKeyId key1 = new KeyEncryptionKeyId(tenant, String.valueOf(Character.MIN_VALUE));
    KeyEncryptionKeyId key2 = new KeyEncryptionKeyId(tenant, String.valueOf(Character.MAX_VALUE));
    try (KeyValueIterator<EncryptionKeyId, EncryptionKey> iter =
        keys().range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        result.add(iter.next());
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

  public List<String> getDekScopes(String kekName, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    return getDeks(tenant, kekName).stream()
        .filter(kv -> !kv.value.isDeleted() || lookupDeleted)
        .map(kv -> ((DataEncryptionKeyId) kv.key).getScope())
        .collect(Collectors.toList());
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeks(String tenant) {
    List<KeyValue<EncryptionKeyId, EncryptionKey>> result = new ArrayList<>();
    DataEncryptionKeyId key1 = new DataEncryptionKeyId(
        tenant, String.valueOf(Character.MIN_VALUE),
        String.valueOf(Character.MIN_VALUE), DekFormat.AES128_GCM);
    DataEncryptionKeyId key2 = new DataEncryptionKeyId(
        tenant, String.valueOf(Character.MAX_VALUE),
        String.valueOf(Character.MAX_VALUE), DekFormat.AES256_SIV);
    try (KeyValueIterator<EncryptionKeyId, EncryptionKey> iter =
        keys().range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        result.add(iter.next());
      }
    }
    return result;
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeks(String tenant, String kekName) {
    List<KeyValue<EncryptionKeyId, EncryptionKey>> result = new ArrayList<>();
    DataEncryptionKeyId key1 = new DataEncryptionKeyId(
        tenant, kekName, String.valueOf(Character.MIN_VALUE), DekFormat.AES128_GCM);
    DataEncryptionKeyId key2 = new DataEncryptionKeyId(
        tenant, kekName, String.valueOf(Character.MAX_VALUE), DekFormat.AES256_SIV);
    try (KeyValueIterator<EncryptionKeyId, EncryptionKey> iter =
        keys().range(key1, true, key2, false)) {
      while (iter.hasNext()) {
        result.add(iter.next());
      }
    }
    return result;
  }

  public DataEncryptionKey getDek(String kekName, String scope, DekFormat algorithm,
      boolean lookupDeleted) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DataEncryptionKeyId keyId = new DataEncryptionKeyId(tenant, kekName, scope, algorithm);
    DataEncryptionKey key = (DataEncryptionKey) keys.get(keyId);
    if (key != null && (!key.isDeleted() || lookupDeleted)) {
      key = maybeGenerateRawDek(key);
      return key;
    } else {
      return null;
    }
  }

  public KeyEncryptionKey createKekOrForward(CreateKekRequest request,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        return createKek(request);
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

  private KeyEncryptionKey forwardCreateKekRequestToLeader(CreateKekRequest request,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks");
    String path = builder.build().toString();

    log.debug(String.format("Forwarding create key request to %s", baseUrl));
    try {
      return leaderRestService.httpRequest(
          path, "POST", toJson(request), headerProperties, KEY_ENCRYPTION_KEY_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the create key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private KeyEncryptionKey createKek(CreateKekRequest request)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    if (metricsManager.getKeyCount(tenant, KeyType.KEK) > config.maxKeys()) {
      throw new TooManyKeysException(KeyType.KEK.name());
    }
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, request.getName());
    if (keys.containsKey(keyId)) {
      throw new AlreadyExistsException(request.getName());
    }

    String kmsType = normalizeKmsType(request.getKmsType());

    SortedMap<String, String> kmsProps = request.getKmsProps() != null
        ? new TreeMap<>(request.getKmsProps())
        : Collections.emptySortedMap();
    KeyEncryptionKey key = new KeyEncryptionKey(request.getName(), kmsType,
        request.getKmsKeyId(), kmsProps, request.getDoc(), request.isShared(), false);
    keys.put(keyId, key);
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

  public DataEncryptionKey createDekOrForward(String kekName, CreateDekRequest request,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        return createDek(kekName, request);
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

  private DataEncryptionKey forwardCreateDekRequestToLeader(String kekName,
      CreateDekRequest request, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks");
    String path = builder.build(kekName).toString();

    log.debug(String.format("Forwarding create key request to %s", baseUrl));
    try {
      return leaderRestService.httpRequest(
          path, "POST", toJson(request), headerProperties, DATA_ENCRYPTION_KEY_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the create key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private DataEncryptionKey createDek(String kekName, CreateDekRequest request)
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
    DataEncryptionKeyId keyId = new DataEncryptionKeyId(
        tenant, kekName, request.getScope(), algorithm);
    if (keys.containsKey(keyId)) {
      throw new AlreadyExistsException(request.getScope());
    }

    DataEncryptionKey key = new DataEncryptionKey(kekName, request.getScope(),
        request.getAlgorithm(), request.getEncryptedKeyMaterial(), false);
    key = maybeGenerateEncryptedDek(key);
    keys.put(keyId, key);
    key = maybeGenerateRawDek(key);
    return key;
  }

  protected DataEncryptionKey maybeGenerateEncryptedDek(DataEncryptionKey key)
      throws SchemaRegistryException {
    try {
      if (key.getEncryptedKeyMaterial() == null) {
        KeyEncryptionKey kek = getKek(key.getKekName(), true);
        Aead aead = kek.toKekEntity().toAead(config.originals());
        // Generate new dek
        byte[] rawDek = getCryptor(key.getAlgorithm()).generateKey();
        byte[] encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
        String encryptedDekStr =
            new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
        key = new DataEncryptionKey(key.getKekName(), key.getScope(), key.getAlgorithm(),
            encryptedDekStr, key.isDeleted());
      }
      return key;
    } catch (GeneralSecurityException e) {
      throw new SchemaRegistryException(e);
    }
  }

  protected DataEncryptionKey maybeGenerateRawDek(DataEncryptionKey key)
      throws SchemaRegistryException {
    try {
      KeyEncryptionKey kek = getKek(key.getKekName(), true);
      if (kek.isShared()) {
        // Decrypt dek
        Aead aead = kek.toKekEntity().toAead(config.originals());
        byte[] encryptedDek = Base64.getDecoder().decode(
            key.getEncryptedKeyMaterial().getBytes(StandardCharsets.UTF_8));
        byte[] rawDek = aead.decrypt(encryptedDek, EMPTY_AAD);
        String rawDekStr =
            new String(Base64.getEncoder().encode(rawDek), StandardCharsets.UTF_8);
        // Copy dek
        key = new DataEncryptionKey(key.getKekName(), key.getScope(), key.getAlgorithm(),
            key.getEncryptedKeyMaterial(), key.isDeleted());
        key.setKeyMaterial(rawDekStr);
      }
      return key;
    } catch (GeneralSecurityException e) {
      throw new SchemaRegistryException(e);
    }
  }

  public KeyEncryptionKey putKekOrForward(String name, UpdateKekRequest request,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        return putKek(name, request);
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

  private KeyEncryptionKey forwardPutKekRequestToLeader(String name,
      UpdateKekRequest request, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}");
    String path = builder.build(name).toString();

    log.debug(String.format("Forwarding put key request to %s", baseUrl));
    try {
      return leaderRestService.httpRequest(
          path, "PUT", toJson(request), headerProperties, KEY_ENCRYPTION_KEY_TYPE);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the put key request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private KeyEncryptionKey putKek(String name, UpdateKekRequest request)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = (KeyEncryptionKey) keys.get(keyId);
    if (key == null) {
      return null;
    }
    SortedMap<String, String> kmsProps = request.getKmsProps() != null
        ? new TreeMap<>(request.getKmsProps())
        : Collections.emptySortedMap();
    String doc = request.getDoc() != null ? request.getDoc() : key.getDoc();
    boolean shared = request.isShared() != null ? request.isShared() : key.isShared();
    KeyEncryptionKey newKey = new KeyEncryptionKey(name, key.getKmsType(),
        key.getKmsKeyId(), kmsProps, doc, shared, false);
    keys.put(keyId, newKey);
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

  private void deleteKek(String name, boolean permanentDelete) throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    String tenant = schemaRegistry.tenant();
    if (getDeks(tenant, name).stream().anyMatch(dek -> permanentDelete || !dek.value.isDeleted())) {
      throw new KeyReferenceExistsException(name);
    }
    EncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = (KeyEncryptionKey) keys.get(keyId);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      if (!key.isDeleted()) {
        throw new KeyNotSoftDeletedException(name);
      }
      keys.remove(keyId);
    } else {
      KeyEncryptionKey newKey = new KeyEncryptionKey(name, key.getKmsType(),
          key.getKmsKeyId(), key.getKmsProps(), key.getDoc(), key.isShared(), true);
      keys.put(keyId, newKey);
    }
  }

  public void deleteDekOrForward(String name, String scope, DekFormat algorithm,
      boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        deleteDek(name, scope, algorithm, permanentDelete);
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          forwardDeleteDekRequestToLeader(name, scope, algorithm,
              permanentDelete, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private void forwardDeleteDekRequestToLeader(String name, String scope, DekFormat algorithm,
      boolean permanentDelete, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks/{scope}")
        .queryParam("permanent", permanentDelete);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, scope).toString();

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

  private void deleteDek(String name, String scope, DekFormat algorithm, boolean permanentDelete)
      throws SchemaRegistryException {
    // Ensure cache is up-to-date
    keys.sync();

    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    String tenant = schemaRegistry.tenant();
    DataEncryptionKeyId keyId = new DataEncryptionKeyId(tenant, name, scope, algorithm);
    DataEncryptionKey key = (DataEncryptionKey) keys.get(keyId);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      if (!key.isDeleted()) {
        throw new KeyNotSoftDeletedException(scope);
      }
      keys.remove(keyId);
    } else {
      DataEncryptionKey newKey = new DataEncryptionKey(name, key.getScope(),
          key.getAlgorithm(), key.getEncryptedKeyMaterial(), true);
      keys.put(keyId, newKey);
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
